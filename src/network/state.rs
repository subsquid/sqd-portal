use std::cmp::max;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::cli::Config;
use crate::metrics;
use crate::types::DatasetId;
use serde::Serialize;
use serde_with::serde_as;
use sqd_contract_client::U256;
use sqd_messages::RangeSet;
use sqd_network_transport::PeerId;

use super::priorities::WorkersPool;

#[derive(Default, Debug, Clone, Serialize)]
pub struct DatasetState {
    worker_ranges: HashMap<PeerId, RangeSet>,
    highest_seen_block: u32,
    first_gap: u32,
}

pub struct PublicState {
    pub current_epoch: u32,
    pub sqd_locked: U256,
    pub status: Status,
}

impl DatasetState {
    pub fn get_workers_with_block(&self, block: u64) -> impl Iterator<Item = PeerId> + '_ {
        self.worker_ranges
            .iter()
            .filter_map(move |(peer_id, range_set)| range_set.has(block as u32).then_some(*peer_id))
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

#[derive(Clone, PartialEq, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    DataLoading,
    Registered,
    Unregistered,
}

pub struct NetworkState {
    config: Arc<Config>,
    dataset_states: HashMap<DatasetId, DatasetState>,
    last_pings: HashMap<PeerId, Instant>,
    pool: WorkersPool,
    current_epoch: u32,
    sqd_locked: U256,
    status: Status,
}

impl NetworkState {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config: config.clone(),
            dataset_states: Default::default(),
            last_pings: Default::default(),
            current_epoch: Default::default(),
            sqd_locked: Default::default(),
            pool: WorkersPool::default(),
            status: Status::DataLoading,
        }
    }

    pub fn find_worker(&mut self, dataset_id: &DatasetId, start_block: u64) -> Option<PeerId> {
        let dataset_state = self.dataset_states.get(dataset_id)?;

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
        for dataset_id in self.config.dataset_ids() {
            let dataset_state = worker_state
                .remove(&dataset_id)
                .unwrap_or_else(RangeSet::empty);
            let entry = self.dataset_states.entry(dataset_id.clone()).or_default();
            entry.update(worker_id, dataset_state);
            metrics::report_dataset_updated(&dataset_id, entry.highest_seen_block, entry.first_gap);
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

    pub fn report_query_timeout(&mut self, worker: PeerId) {
        self.pool.timeout(worker);
    }

    pub fn report_query_outrun(&mut self, worker: PeerId) {
        self.pool.outrun(worker);
    }

    pub fn report_no_allocation(&mut self, worker: PeerId) {
        self.pool.unavailable(worker);
    }

    pub fn reset_allocations(&mut self) {
        self.pool.reset_allocations();
    }

    pub fn get_height(&self, dataset_id: &DatasetId) -> Option<u32> {
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

    pub fn set_current_epoch(&mut self, epoch: u32) {
        self.current_epoch = epoch;
    }

    pub fn set_sqd_locked(&mut self, sqd: Option<(String, U256)>) {
        if let Some((_, sqd)) = sqd {
            self.sqd_locked = sqd;
            self.status = Status::Registered;
        } else {
            self.sqd_locked = U256::from(0);
            self.status = Status::Unregistered;
        }
    }

    pub fn get_public_state(&self) -> PublicState {
        PublicState {
            current_epoch: self.current_epoch,
            sqd_locked: self.sqd_locked,
            status: self.status.clone(),
        }
    }
}
