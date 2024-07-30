use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use rand::prelude::IteratorRandom;

use crate::cli::Config;
use crate::metrics;
use crate::types::DatasetId;
use contract_client::Worker;
use subsquid_messages::RangeSet;
use subsquid_network_transport::PeerId;

#[derive(Default)]
struct DatasetState {
    worker_ranges: HashMap<PeerId, RangeSet>,
    highest_seen_block: u32,
    first_gap: u32,
}

impl DatasetState {
    pub fn get_workers_with_block(&self, block: u32) -> impl Iterator<Item = PeerId> + '_ {
        self.worker_ranges
            .iter()
            .filter_map(move |(peer_id, range_set)| range_set.has(block).then_some(*peer_id))
    }

    pub fn update(&mut self, peer_id: PeerId, state: RangeSet) {
        let mut could_close_gap = false;
        if let Some(range) = state.ranges.last() {
            self.highest_seen_block = max(self.highest_seen_block, range.end);
            if range.end >= self.first_gap {
                could_close_gap = true;
            }
        }
        self.worker_ranges.insert(peer_id, state);
        if could_close_gap {
            self.first_gap = self.highest_indexable_block() + 1;
        }
    }

    pub fn highest_indexable_block(&self) -> u32 {
        let range_set: RangeSet = self
            .worker_ranges
            .values()
            .cloned()
            .flat_map(|r| r.ranges)
            .into();
        match range_set.ranges.first() {
            Some(range) if range.begin == 0 => range.end,
            _ => 0,
        }
    }
}

pub struct NetworkState {
    config: Arc<Config>,
    dataset_states: HashMap<DatasetId, DatasetState>,
    last_pings: HashMap<PeerId, Instant>,
    worker_greylist: HashMap<PeerId, Instant>,
    workers_without_allocation: HashSet<PeerId>,
    registered_workers: HashSet<PeerId>,
}

impl NetworkState {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            dataset_states: Default::default(),
            last_pings: Default::default(),
            worker_greylist: Default::default(),
            workers_without_allocation: Default::default(),
            registered_workers: Default::default(),
        }
    }

    pub fn find_worker(&self, dataset_id: &DatasetId, start_block: u32) -> Option<PeerId> {
        // tracing::debug!("Looking for worker dataset_id={dataset_id}, start_block={start_block}");
        let dataset_state = match self.dataset_states.get(dataset_id) {
            None => return None,
            Some(state) => state,
        };

        // Choose a random active worker having the requested start_block
        let mut worker = dataset_state
            .get_workers_with_block(start_block)
            .filter(|peer_id| self.worker_available(peer_id, false))
            .choose(&mut rand::thread_rng());

        // If no worker is found, try grey-listed workers
        if worker.is_none() {
            worker = dataset_state
                .get_workers_with_block(start_block)
                .filter(|peer_id| self.worker_available(peer_id, true))
                .choose(&mut rand::thread_rng());
        }

        worker
    }

    fn worker_available(&self, worker_id: &PeerId, allow_greylisted: bool) -> bool {
        // self.registered_workers.contains(worker_id)
        self.worker_has_allocation(worker_id)
            && self.worker_active(worker_id)
            && (allow_greylisted || !self.worker_greylisted(worker_id))
    }

    fn worker_active(&self, worker_id: &PeerId) -> bool {
        let inactive_threshold = self.config.worker_inactive_threshold;
        self.last_pings
            .get(worker_id)
            .is_some_and(|t| *t + inactive_threshold > Instant::now())
    }

    fn worker_greylisted(&self, worker_id: &PeerId) -> bool {
        let greylist_time = self.config.worker_greylist_time;
        self.worker_greylist
            .get(worker_id)
            .is_some_and(|t| *t + greylist_time > Instant::now())
    }

    pub fn greylisted_workers(&self) -> Vec<PeerId> {
        let greylist_time = self.config.worker_greylist_time;
        let now = Instant::now();
        self.worker_greylist
            .iter()
            .filter_map(|(worker_id, t)| (*t + greylist_time > now).then_some(*worker_id))
            .collect()
    }

    pub fn reset_allocations_cache(&mut self) {
        self.workers_without_allocation.clear();
    }

    pub fn no_allocation_for_worker(&mut self, worker_id: PeerId) {
        self.workers_without_allocation.insert(worker_id);
    }

    pub fn worker_has_allocation(&self, worker_id: &PeerId) -> bool {
        !self.workers_without_allocation.contains(worker_id)
    }

    pub fn update_dataset_states(
        &mut self,
        worker_id: PeerId,
        mut worker_state: HashMap<DatasetId, RangeSet>,
    ) {
        self.last_pings.insert(worker_id, Instant::now());
        metrics::KNOWN_WORKERS.set(self.last_pings.len() as i64);
        for dataset_id in self.config.dataset_ids() {
            let dataset_state = worker_state
                .remove(&dataset_id)
                .unwrap_or_else(RangeSet::empty);
            let entry = self.dataset_states.entry(dataset_id.clone()).or_default();
            entry.update(worker_id, dataset_state);
            metrics::report_dataset_updated(
                &dataset_id,
                entry.highest_seen_block,
                entry.highest_seen_block,
            );
        }
    }

    pub fn update_registered_workers(&mut self, workers: Vec<Worker>) {
        tracing::debug!("Updating registered workers: {workers:?}");
        self.registered_workers = workers.into_iter().map(|w| w.peer_id).collect();
    }

    pub fn greylist_worker(&mut self, worker_id: PeerId) {
        tracing::info!("Grey-listing worker {worker_id}");
        self.worker_greylist.insert(worker_id, Instant::now());
    }

    pub fn get_height(&self, dataset_id: &DatasetId) -> Option<u32> {
        self.dataset_states
            .get(dataset_id)
            .map(|state| state.highest_indexable_block())
    }
}
