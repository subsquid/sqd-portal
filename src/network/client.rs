use std::{collections::HashMap, sync::Arc, time::Duration};

use contract_client::PeerId;
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use subsquid_messages::{data_chunk::DataChunk, query_result, Ping, Query, QueryResult};
use subsquid_network_transport::{
    GatewayConfig, GatewayEvent, GatewayTransportHandle, P2PTransportBuilder, QueueFull,
    TransportArgs,
};
use tokio::{sync::oneshot, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::{
    cli::Config,
    metrics,
    types::{generate_query_id, DatasetId, QueryId},
    utils::UseOnce,
};

use super::{NetworkState, StorageClient};

lazy_static::lazy_static! {
    static ref SUPPORTED_VERSIONS: semver::VersionReq = ">=1.1.0-rc1".parse().expect("Invalid version requirement");
}

/// Tracks the network state and handles p2p communication
pub struct NetworkClient {
    incoming_events: UseOnce<Box<dyn Stream<Item = GatewayEvent> + Send + Unpin + 'static>>,
    transport_handle: GatewayTransportHandle,
    network_state: Mutex<NetworkState>,
    contract_client: Box<dyn contract_client::Client>,
    tasks: Mutex<HashMap<QueryId, QueryTask>>,
    dataset_storage: StorageClient,
    dataset_update_interval: Duration,
    chain_update_interval: Duration,
}

struct QueryTask {
    result_tx: oneshot::Sender<query_result::Result>,
    worker_id: PeerId,
}

impl NetworkClient {
    pub async fn new(
        args: TransportArgs,
        logs_collector: PeerId,
        config: Arc<Config>,
    ) -> anyhow::Result<NetworkClient> {
        let dataset_storage = StorageClient::new(config.available_datasets.values()).await?;
        let transport_builder = P2PTransportBuilder::from_cli(args).await?;
        let contract_client = transport_builder.contract_client();
        let mut gateway_config = GatewayConfig::new(logs_collector);
        gateway_config.query_config.request_timeout = config.transport_timeout;
        let (incoming_events, transport_handle) =
            transport_builder.build_gateway(gateway_config)?;
        Ok(NetworkClient {
            dataset_update_interval: config.dataset_update_interval,
            chain_update_interval: config.chain_update_interval,
            transport_handle,
            incoming_events: UseOnce::new(Box::new(incoming_events)),
            network_state: Mutex::new(NetworkState::new(config)),
            contract_client,
            tasks: Mutex::new(HashMap::new()),
            dataset_storage,
        })
    }

    pub async fn run(&self, cancellation_token: CancellationToken) {
        // TODO: run coroutines in parallel
        tokio::join!(
            self.run_event_stream(cancellation_token.clone()),
            self.run_storage_updates(cancellation_token.clone()),
            self.run_chain_updates(cancellation_token),
        );
    }

    async fn run_storage_updates(&self, cancellation_token: CancellationToken) {
        let mut interval = tokio::time::interval(self.dataset_update_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                _ = cancellation_token.cancelled() => {
                    break;
                }
            }
            self.dataset_storage.update().await;
        }
    }

    async fn run_chain_updates(&self, cancellation_token: CancellationToken) {
        let mut current_epoch = self
            .contract_client
            .current_epoch()
            .await
            .unwrap_or_else(|e| panic!("Couldn't get current epoch: {e}"));
        tracing::info!("Current epoch: {current_epoch}");
        let mut interval = tokio::time::interval_at(
            Instant::now() + self.chain_update_interval,
            self.chain_update_interval,
        );
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                _ = cancellation_token.cancelled() => {
                    break;
                }
            }
            let epoch = match self.contract_client.current_epoch().await {
                Ok(epoch) => epoch,
                Err(e) => {
                    tracing::warn!("Couldn't get current epoch: {e}");
                    continue;
                }
            };
            if epoch != current_epoch {
                tracing::info!("Epoch {epoch} started");
                current_epoch = epoch;
                self.network_state.lock().reset_cache();
            }
        }
    }

    async fn run_event_stream(&self, cancellation_token: CancellationToken) {
        let stream = self
            .incoming_events
            .take()
            .unwrap()
            .take_until(cancellation_token.cancelled_owned());
        tokio::pin!(stream);
        while let Some(event) = stream.next().await {
            match event {
                GatewayEvent::Ping { peer_id, ping } => {
                    self.handle_ping(peer_id, ping);
                }
                GatewayEvent::QueryResult { peer_id, result } => {
                    self.handle_query_result(peer_id, result)
                        .unwrap_or_else(|e| {
                            tracing::error!("Error handling query: {e:?}");
                        });
                }
            }
        }
    }

    pub fn find_chunk(&self, dataset: &DatasetId, block: u64) -> Option<DataChunk> {
        self.dataset_storage.find_chunk(dataset, block)
    }

    pub fn next_chunk(&self, dataset: &DatasetId, chunk: &DataChunk) -> Option<DataChunk> {
        self.dataset_storage.next_chunk(dataset, chunk)
    }

    pub fn find_worker(&self, dataset: &DatasetId, block: u32) -> Option<PeerId> {
        self.network_state.lock().find_worker(dataset, block)
    }

    pub fn get_height(&self, dataset: &DatasetId) -> Option<u32> {
        self.network_state.lock().get_height(dataset)
    }

    pub fn query_worker(
        &self,
        worker: &PeerId,
        dataset: &DatasetId,
        query: String,
    ) -> Result<oneshot::Receiver<query_result::Result>, QueueFull> {
        let query_id = generate_query_id();

        self.transport_handle.send_query(
            *worker,
            Query {
                dataset: Some(dataset.to_string()),
                query_id: Some(query_id.clone()),
                query: Some(query),
                client_state_json: Some("{}".to_string()), // This is a placeholder field
                profiling: Some(false),
                ..Default::default()
            },
        )?;
        tracing::trace!("Sent query {query_id} to {worker}");
        metrics::QUERIES_SENT.inc();

        let (result_tx, result_rx) = oneshot::channel();
        let task = QueryTask {
            result_tx,
            worker_id: *worker,
        };
        self.tasks.lock().insert(query_id, task);
        Ok(result_rx)
    }

    fn handle_ping(&self, peer_id: PeerId, ping: Ping) {
        if !ping.version_matches(&SUPPORTED_VERSIONS) {
            metrics::IGNORED_PINGS.inc();
            return;
        }
        tracing::trace!("Ping from {peer_id}");
        metrics::VALID_PINGS.inc();
        let worker_state = ping
            .stored_ranges
            .into_iter()
            .map(|r| (DatasetId::from_url(r.url), r.ranges.into()))
            .collect();
        self.network_state
            .lock()
            .update_dataset_states(peer_id, worker_state);
    }

    fn handle_query_result(&self, peer_id: PeerId, result: QueryResult) -> anyhow::Result<()> {
        let QueryResult { query_id, result } = result;
        let result = result.ok_or_else(|| anyhow::anyhow!("Result missing"))?;
        tracing::trace!("Got result for query {query_id}");
        metrics::report_query_result(&result);

        let (query_id, task) = self
            .tasks
            .lock()
            .remove_entry(&query_id)
            .ok_or_else(|| anyhow::anyhow!("Not expecting response for query {query_id}"))?;
        if peer_id != task.worker_id {
            tracing::error!(
                "Invalid message sender, expected {}, got {}",
                task.worker_id,
                peer_id
            );
            self.network_state.lock().report_query_error(peer_id);
        }

        match &result {
            query_result::Result::ServerError(e) => {
                tracing::warn!("Server error returned for query {query_id}: {e}");
                self.network_state.lock().report_query_error(peer_id);
            }
            query_result::Result::NoAllocation(()) => {
                self.network_state.lock().report_no_allocation(peer_id);
            }
            query_result::Result::Timeout(_) | query_result::Result::TimeoutV1(_) => {
                self.network_state.lock().report_query_timeout(peer_id);
            }
            query_result::Result::Ok(_) | query_result::Result::BadRequest(_) => {
                if task.result_tx.send(result).is_ok() {
                    self.network_state.lock().report_query_success(peer_id);
                } else {
                    // The result is no longer needed. Either another query has got the result first,
                    // or the stream has been dropped. In either case, consider the query timed out.
                    self.network_state.lock().report_query_timeout(peer_id);
                }
                return Ok(());
            }
        };

        task.result_tx.send(result).ok();

        Ok(())
    }
}
