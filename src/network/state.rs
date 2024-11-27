use num_rational::Ratio;
use serde::Serialize;
use std::cmp::max;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use sqd_contract_client::Worker;
use sqd_messages::RangeSet;
use sqd_network_transport::PeerId;

use crate::cli::Config;
use crate::datasets::Datasets;
use crate::metrics;
use crate::types::DatasetId;

use super::priorities::{NoWorker, WorkersPool};

#[derive(Default, Debug, Clone, Serialize)]
pub struct DatasetState {
    worker_ranges: HashMap<PeerId, RangeSet>,
    highest_seen_block: u64,
    first_gap: u64,
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

    pub fn highest_indexable_block(&self) -> u64 {
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

#[derive(Clone, PartialEq, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    DataLoading,
    Registered,
    Unregistered,
}

pub struct NetworkState {
    config: Arc<Config>,
    datasets: Arc<Datasets>,
    dataset_states: HashMap<DatasetId, DatasetState>,
    last_pings: HashMap<PeerId, Instant>,
    pool: WorkersPool,
    contracts_state: ContractsState,
}

#[derive(Clone)]
pub struct ContractsState {
    pub sqd_locked: Ratio<u128>,
    pub status: Status,
    pub operator: String,
    pub current_epoch: u32,
    pub current_epoch_started: SystemTime,
    pub compute_units_per_epoch: u64,
    pub epoch_length: Duration,
    pub uses_default_strategy: bool,
    pub active_workers_length: u64,
}

impl NetworkState {
    pub fn new(config: Arc<Config>, datasets: Arc<Datasets>) -> Self {
        Self {
            config: config.clone(),
            datasets: datasets.clone(),
            dataset_states: Default::default(),
            last_pings: Default::default(),
            pool: WorkersPool::default(),
            contracts_state: ContractsState {
                operator: Default::default(),
                current_epoch: Default::default(),
                sqd_locked: Default::default(),
                status: Status::DataLoading,
                uses_default_strategy: Default::default(),
                epoch_length: Default::default(),
                active_workers_length: Default::default(),
                current_epoch_started: SystemTime::UNIX_EPOCH,
                compute_units_per_epoch: Default::default(),
            },
        }
    }

    pub fn find_worker(
        &mut self,
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

    pub fn update_dataset_states(
        &mut self,
        worker_id: PeerId,
        mut worker_state: HashMap<DatasetId, RangeSet>,
    ) {
        self.last_pings.insert(worker_id, Instant::now());
        metrics::KNOWN_WORKERS.set(self.last_pings.len() as i64);
        for dataset_id in self.datasets.dataset_ids() {
            let dataset_state = worker_state
                .remove(dataset_id)
                .unwrap_or_else(RangeSet::empty);
            let entry = self.dataset_states.entry(dataset_id.clone()).or_default();
            entry.update(worker_id, dataset_state);
            metrics::report_dataset_updated(dataset_id, entry.highest_seen_block, entry.first_gap);
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
            .map(|state| state.highest_indexable_block())
    }

    fn worker_active(
        last_pings: &HashMap<PeerId, Instant>,
        worker_id: &PeerId,
        deadline: Instant,
    ) -> bool {
        last_pings.get(worker_id).is_some_and(|t| *t > deadline)
    }

    pub fn dataset_state(&self, dataset_id: DatasetId) -> Option<&DatasetState> {
        self.dataset_states.get(&dataset_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn set_contracts_state(
        &mut self,
        current_epoch: u32,
        sqd_locked: Option<(String, Ratio<u128>)>,
        epoch_length: Duration,
        uses_default_strategy: bool,
        active_workers: Vec<Worker>,
        current_epoch_started: SystemTime,
        compute_units_per_epoch: u64,
    ) {
        if let Some((operator, sqd)) = sqd_locked {
            self.contracts_state.operator = operator;
            self.contracts_state.sqd_locked = sqd;
            self.contracts_state.status = Status::Registered;
        } else {
            self.contracts_state.sqd_locked = Ratio::new(0, 1);
            self.contracts_state.status = Status::Unregistered;
        }

        self.contracts_state.current_epoch = current_epoch;
        self.contracts_state.epoch_length = epoch_length;
        self.contracts_state.uses_default_strategy = uses_default_strategy;
        self.contracts_state.active_workers_length = active_workers.len() as u64;
        self.contracts_state.current_epoch_started = current_epoch_started;
        self.contracts_state.compute_units_per_epoch = compute_units_per_epoch;
    }

    pub fn get_contracts_state(&self) -> ContractsState {
        self.contracts_state.clone()
    }
}
