use sqd_contract_client::Network;
use std::sync::Arc;
use std::time::Duration;

use sqd_network_transport::PeerId;

use crate::network::StorageClient;
use crate::types::api_types::WorkerDebugInfo;
use crate::types::DatasetId;
use crate::utils::RwLock;
use crate::{datasets::Datasets, types::api_types::DatasetState};

use super::priorities::{NoWorker, WorkersPool};

pub struct NetworkState {
    pool: RwLock<WorkersPool>,
    pub dataset_storage: StorageClient,
}

impl NetworkState {
    pub fn new(datasets: Arc<RwLock<Datasets>>, network: Network) -> Self {
        Self {
            pool: RwLock::new(WorkersPool::default(), "NetworkState::pool"),
            dataset_storage: StorageClient::new(datasets, network),
        }
    }

    pub async fn try_update_assignment(&self) {
        self.dataset_storage.try_update_assignment().await;
    }

    // TODO: return a guard object that will automatically release the worker when dropped
    pub fn find_worker(
        &self,
        dataset_id: &DatasetId,
        start_block: u64,
    ) -> Result<PeerId, NoWorker> {
        let dataset_state = self
            .dataset_storage
            .find_chunk(dataset_id, start_block)
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
        self.pool.read().pick(dataset_state.workers.iter().copied())
    }

    pub fn get_workers(&self, dataset_id: &DatasetId, start_block: u64) -> Vec<WorkerDebugInfo> {
        let Ok(chunk) = self.dataset_storage.find_chunk(dataset_id, start_block) else {
            return vec![];
        };

        self.pool
            .read()
            .get_priorities(chunk.workers.iter().copied())
            .into_iter()
            .map(|(peer_id, priority)| WorkerDebugInfo { peer_id, priority })
            .collect()
    }

    pub fn get_all_workers(&self) -> Vec<WorkerDebugInfo> {
        let workers = self.dataset_storage.get_all_workers();
        self.pool
            .read()
            .get_priorities(workers)
            .into_iter()
            .map(|(peer_id, priority)| WorkerDebugInfo { peer_id, priority })
            .collect()
    }

    pub fn lease_worker(&self, worker: PeerId) {
        self.pool.write().lease(worker);
    }

    pub fn report_query_success(&self, worker: PeerId) {
        self.pool.write().success(worker);
    }

    pub fn report_query_error(&self, worker: PeerId) {
        self.pool.write().error(worker);
    }

    pub fn report_query_failure(&self, worker: PeerId) {
        self.pool.write().failure(worker);
    }

    pub fn report_query_outrun(&self, worker: PeerId) {
        self.pool.write().outrun(worker);
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
