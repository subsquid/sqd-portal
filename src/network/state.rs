use sqd_contract_client::Network;
use std::sync::Arc;
use std::time::Duration;

use sqd_network_transport::PeerId;

use crate::network::priorities::PrioritiesConfig;
use crate::network::{AssignmentType, StorageClient};
use crate::types::api_types::WorkerDebugInfo;
use crate::types::DatasetId;
use crate::utils::RwLock;
use crate::{datasets::Datasets, types::api_types::DatasetState};

use super::priorities::{HolderSummary, NoWorker, WorkersPool};

pub struct WorkerLease {
    pool: Arc<RwLock<WorkersPool>>,
    worker: PeerId,
}

impl WorkerLease {
    pub fn worker(&self) -> PeerId {
        self.worker
    }

    /// Creates a standalone lease backed by a fresh pool, for tests that
    /// mock the network without a full [`NetworkState`].
    #[cfg(test)]
    pub(crate) fn for_tests(worker: PeerId) -> Self {
        TestLeasePool::new().lease(worker)
    }
}

/// A shared lease pool for tests that mock the network without a full
/// [`NetworkState`], allowing them to verify that every issued
/// [`WorkerLease`] is eventually returned.
#[cfg(test)]
pub(crate) struct TestLeasePool {
    pool: Arc<RwLock<WorkersPool>>,
}

#[cfg(test)]
impl TestLeasePool {
    pub fn new() -> Self {
        Self {
            pool: Arc::new(RwLock::new(
                WorkersPool::new(PrioritiesConfig::default()),
                "TestLeasePool::pool",
            )),
        }
    }

    pub fn lease(&self, worker: PeerId) -> WorkerLease {
        self.pool.write().lease(worker);
        WorkerLease {
            pool: self.pool.clone(),
            worker,
        }
    }

    /// The number of leases issued and not yet dropped.
    pub fn outstanding(&self) -> usize {
        self.pool.read().running_queries_total()
    }
}

impl Drop for WorkerLease {
    fn drop(&mut self) {
        self.pool.write().unlease(self.worker);
    }
}

impl std::fmt::Display for WorkerLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.worker.fmt(f)
    }
}

pub struct NetworkState {
    pool: Arc<RwLock<WorkersPool>>,
    pub dataset_storage: StorageClient,
}

impl NetworkState {
    pub fn new(
        datasets: Arc<RwLock<Datasets>>,
        network: Network,
        network_state_url: &str,
        priorities_config: PrioritiesConfig,
    ) -> Self {
        Self {
            pool: Arc::new(RwLock::new(
                WorkersPool::new(priorities_config),
                "NetworkState::pool",
            )),
            dataset_storage: StorageClient::new(datasets, network, network_state_url),
        }
    }

    pub fn ignore_deprecated_workers(&mut self) {
        self.dataset_storage.ignore_deprecated_workers();
    }

    pub fn set_assignment_source(&mut self, source: Option<AssignmentType>) {
        self.dataset_storage.set_assignment_source(source);
    }

    pub async fn try_update_assignment(&self) {
        self.dataset_storage.try_update_assignment().await;
    }

    pub fn find_worker(
        &self,
        dataset_id: &DatasetId,
        start_block: u64,
    ) -> Result<WorkerLease, NoWorker> {
        let workers = self
            .dataset_storage
            .find_workers(dataset_id, start_block)
            .map_err(|e| {
                tracing::warn!(
                    %dataset_id,
                    block = start_block,
                    error = %e,
                    "Requesting worker for non-existing chunk",
                );
                NoWorker::AllUnavailable
            })?;

        // Choose a worker having the requested start_block with the top priority
        let worker = self.pool.write().pick(workers.iter().copied())?;
        Ok(WorkerLease {
            pool: Arc::clone(&self.pool),
            worker,
        })
    }

    pub fn reserve_worker(&self, worker: PeerId) -> Option<WorkerLease> {
        if !self.dataset_storage.get_all_workers().contains(&worker) {
            return None;
        }
        self.pool.write().lease(worker);
        Some(WorkerLease {
            pool: Arc::clone(&self.pool),
            worker,
        })
    }

    pub fn get_workers(&self, dataset_id: &DatasetId, start_block: u64) -> Vec<WorkerDebugInfo> {
        let Ok(workers) = self.dataset_storage.find_workers(dataset_id, start_block) else {
            return vec![];
        };

        self.pool
            .read()
            .describe(workers.iter().copied())
            .into_iter()
            .map(|(peer_id, priority, health)| WorkerDebugInfo {
                peer_id,
                priority,
                health,
            })
            .collect()
    }

    pub fn get_all_workers(&self) -> Vec<WorkerDebugInfo> {
        let workers = self.dataset_storage.get_all_workers();
        self.pool
            .read()
            .describe(workers)
            .into_iter()
            .map(|(peer_id, priority, health)| WorkerDebugInfo {
                peer_id,
                priority,
                health,
            })
            .collect()
    }

    /// How the holders of the chunk containing `start_block` look to `pick` right now.
    pub fn summarize_holders(&self, dataset_id: &DatasetId, start_block: u64) -> HolderSummary {
        let Ok(workers) = self.dataset_storage.find_workers(dataset_id, start_block) else {
            return HolderSummary::default();
        };
        self.pool.read().summarize(workers)
    }

    pub fn report_query_success(
        &self,
        worker: PeerId,
        verdict: &'static str,
        throughput: Option<f64>,
    ) {
        self.pool.write().success(worker, verdict, throughput);
    }

    pub fn report_query_error(&self, worker: PeerId, verdict: &'static str) {
        self.pool.write().error(worker, verdict);
    }

    pub fn report_query_failure(&self, worker: PeerId, verdict: &'static str) {
        self.pool.write().failure(worker, verdict);
    }

    pub fn hint_backoff(&self, worker: PeerId, duration: Duration) {
        self.pool.write().hint_backoff(worker, duration);
    }

    pub fn reset_allocations(&self) {
        self.pool.write().reset_allocations();
    }

    pub fn get_height(&self, dataset_id: &DatasetId) -> Option<u64> {
        self.dataset_storage
            .head(dataset_id)
            .map(|block| block.number)
    }

    pub fn dataset_state(&self, dataset_id: &DatasetId) -> Option<DatasetState> {
        self.dataset_storage.get_dataset_state(dataset_id)
    }
}
