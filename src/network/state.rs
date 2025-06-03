use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sqd_messages::RangeSet;
use sqd_network_transport::PeerId;

use crate::config::Config;
use crate::datasets::Datasets;
use crate::metrics;
use crate::types::api_types::WorkerDebugInfo;
use crate::types::DatasetId;
use crate::utils::RwLock;

use super::priorities::{NoWorker, WorkersPool};

#[derive(Default, Serialize)]
pub struct DatasetState {
    worker_ranges: HashMap<PeerId, RangeSet>,
    highest_seen_block: u64,
    first_gap: u64,
    #[serde(skip)]
    height_update_subscribers: Vec<Box<dyn FnMut(u64) + Send>>,
}

impl DatasetState {
    pub fn get_workers_with_block(&self, block: u64) -> impl Iterator<Item = PeerId> + '_ {
        self.worker_ranges
            .iter()
            .filter_map(move |(peer_id, range_set)| range_set.has(block).then_some(*peer_id))
    }

    pub fn update(&mut self, peer_id: PeerId, state: RangeSet) {
        let mut could_close_gap = false;
        if let Some(range) = state.ranges.last() {
            if range.end > self.highest_seen_block {
                self.highest_seen_block = range.end;
                self.notify_height_update();
            }
            if range.end >= self.first_gap {
                could_close_gap = true;
            }
        }
        self.worker_ranges.insert(peer_id, state);
        if could_close_gap {
            self.first_gap = self.highest_indexable_block() + 1;
        }
    }

    /// The last block such that every block from the beginning to this one is owned by at least one worker
    pub fn highest_indexable_block(&self) -> u64 {
        let range_set: RangeSet = self
            .worker_ranges
            .values()
            .cloned()
            .flat_map(|r| r.ranges)
            .into();
        match range_set.ranges.first() {
            Some(range) => range.end,
            _ => 0,
        }
    }

    /// The last block known to be downloaded by at least one worker
    pub fn highest_known_block(&self) -> u64 {
        self.highest_seen_block
    }

    pub fn subscribe_height_updates(&mut self, subscriber: Box<dyn FnMut(u64) + Send>) {
        self.height_update_subscribers.push(subscriber);
    }

    pub fn unsubscribe_height_updates(&mut self) {
        self.height_update_subscribers.clear();
    }

    fn notify_height_update(&mut self) {
        let height = self.highest_seen_block;
        for subscriber in &mut self.height_update_subscribers {
            subscriber(height);
        }
    }
}

pub struct NetworkState {
    config: Arc<Config>,
    datasets: Arc<RwLock<Datasets>>,
    dataset_states: HashMap<DatasetId, DatasetState>,
    last_pings: HashMap<PeerId, Instant>,
    pool: WorkersPool,
}

impl NetworkState {
    pub fn new(config: Arc<Config>, datasets: Arc<RwLock<Datasets>>) -> Self {
        Self {
            config,
            datasets,
            dataset_states: Default::default(),
            last_pings: Default::default(),
            pool: WorkersPool::default(),
        }
    }

    // TODO: return a guard object that will automatically release the worker when dropped
    pub fn find_worker(
        &self,
        dataset_id: &DatasetId,
        start_block: u64,
    ) -> Result<PeerId, NoWorker> {
        let dataset_state = self
            .dataset_states
            .get(dataset_id)
            .ok_or(NoWorker::AllUnavailable)?;

        // Choose an active worker having the requested start_block with the top priority
        let deadline = Instant::now() - self.config.worker_inactive_threshold;
        let available = dataset_state
            .get_workers_with_block(start_block)
            .filter(|peer_id| Self::worker_active(&self.last_pings, peer_id, deadline));
        self.pool.pick(available)
    }

    pub fn get_workers(&self, dataset_id: &DatasetId, start_block: u64) -> Vec<WorkerDebugInfo> {
        let Some(dataset_state) = self.dataset_states.get(dataset_id) else {
            return vec![];
        };

        let workers = dataset_state.get_workers_with_block(start_block);
        let now = Instant::now();
        self.pool
            .get_priorities(workers)
            .into_iter()
            .map(|(peer_id, priority)| WorkerDebugInfo {
                peer_id,
                priority,
                since_last_heartbeat: self.last_pings.get(&peer_id).map(|t| (now - *t).as_secs()),
            })
            .collect()
    }

    pub fn get_all_workers(&self) -> Vec<WorkerDebugInfo> {
        let now = Instant::now();
        self.pool
            .get_priorities(self.last_pings.keys().copied())
            .into_iter()
            .map(|(peer_id, priority)| WorkerDebugInfo {
                peer_id,
                priority,
                since_last_heartbeat: self.last_pings.get(&peer_id).map(|t| (now - *t).as_secs()),
            })
            .collect()
    }

    pub fn update_dataset_states(
        &mut self,
        worker_id: PeerId,
        mut worker_state: HashMap<DatasetId, RangeSet>,
    ) {
        self.last_pings.insert(worker_id, Instant::now());
        metrics::KNOWN_WORKERS.set(self.last_pings.len() as i64);
        let datasets = self.datasets.read();
        for (dataset_id, default_name) in datasets.network_datasets() {
            let dataset_state = worker_state
                .remove(dataset_id)
                .unwrap_or_else(RangeSet::empty);
            let entry = self.dataset_states.entry(dataset_id.clone()).or_default();
            entry.update(worker_id, dataset_state);
            metrics::report_dataset_updated(
                dataset_id,
                Some(default_name.to_owned()),
                entry.highest_seen_block,
                entry.first_gap,
            );
        }
    }

    pub fn lease_worker(&mut self, worker: PeerId) {
        self.pool.lease(worker);
    }

    pub fn report_query_success(&mut self, worker: PeerId) {
        self.pool.success(worker);
    }

    pub fn report_query_error(&mut self, worker: PeerId) {
        self.pool.error(worker);
    }

    pub fn report_query_failure(&mut self, worker: PeerId) {
        self.pool.failure(worker);
    }

    pub fn report_query_outrun(&mut self, worker: PeerId) {
        self.pool.outrun(worker);
    }

    pub fn hint_backoff(&mut self, worker: PeerId, duration: Duration) {
        self.pool.hint_backoff(worker, duration);
    }

    pub fn reset_allocations(&mut self) {
        self.pool.reset_allocations();
    }

    pub fn get_height(&self, dataset_id: &DatasetId) -> Option<u64> {
        self.dataset_states
            .get(dataset_id)
            .map(|state| state.highest_known_block())
    }

    fn worker_active(
        last_pings: &HashMap<PeerId, Instant>,
        worker_id: &PeerId,
        deadline: Instant,
    ) -> bool {
        last_pings.get(worker_id).is_some_and(|t| *t > deadline)
    }

    pub fn dataset_state(&self, dataset_id: &DatasetId) -> Option<&DatasetState> {
        self.dataset_states.get(dataset_id)
    }

    pub fn subscribe_height_updates(
        &mut self,
        dataset_id: &DatasetId,
        subscriber: Box<dyn FnMut(u64) + Send>,
    ) {
        self.dataset_states
            .entry(dataset_id.clone())
            .or_default()
            .subscribe_height_updates(subscriber);
    }

    pub fn unsubscribe_all_height_updates(&mut self) {
        for state in self.dataset_states.values_mut() {
            state.unsubscribe_height_updates();
        }
    }
}
