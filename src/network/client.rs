use std::{collections::HashMap, sync::Arc, time::Duration};

use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use sqd_contract_client::{Client as ContractClient, PeerId};
use sqd_messages::{query_error, query_result, Heartbeat, Query, QueryOk};
use sqd_network_transport::{
    get_agent_info, AgentInfo, GatewayConfig, GatewayEvent, GatewayTransportHandle,
    P2PTransportBuilder, QueryFailure, QueueFull, TransportArgs,
};
use tokio::{sync::oneshot, time::Instant};
use tokio_util::sync::CancellationToken;

use super::{NetworkState, StorageClient};
use crate::network::state::DatasetState;
use crate::{
    cli::Config,
    metrics,
    types::{generate_query_id, BlockRange, DatasetId, QueryError, QueryId},
    utils::UseOnce,
};

lazy_static::lazy_static! {
    static ref SUPPORTED_VERSIONS: semver::VersionReq = "~2.0.0".parse().expect("Invalid version requirement");
}
const MAX_CONCURRENT_QUERIES: usize = 1000;

pub type QueryResult = Result<QueryOk, QueryError>;

/// Tracks the network state and handles p2p communication
pub struct NetworkClient {
    incoming_events: UseOnce<Box<dyn Stream<Item = GatewayEvent> + Send + Unpin + 'static>>,
    transport_handle: GatewayTransportHandle,
    network_state: Mutex<NetworkState>,
    contract_client: Box<dyn ContractClient>,
    tasks: Mutex<HashMap<QueryId, QueryTask>>,
    dataset_storage: StorageClient,
    dataset_update_interval: Duration,
    chain_update_interval: Duration,
    local_peer_id: PeerId,
}

struct QueryTask {
    result_tx: oneshot::Sender<QueryResult>,
    worker_id: PeerId,
}

impl NetworkClient {
    pub async fn new(
        args: TransportArgs,
        config: Arc<Config>,
    ) -> anyhow::Result<NetworkClient> {
        let dataset_storage = StorageClient::new(args.rpc.network)?;
        let agent_into = get_agent_info!();
        let transport_builder = P2PTransportBuilder::from_cli(args, agent_into).await?;
        let contract_client = transport_builder.contract_client();
        let local_peer_id = transport_builder.local_peer_id().clone();

        let mut gateway_config = GatewayConfig::new();
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
            local_peer_id,
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
                self.network_state.lock().reset_allocations();
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
                GatewayEvent::Heartbeat { peer_id, heartbeat } => {
                    self.handle_heartbeat(peer_id, heartbeat);
                }
                GatewayEvent::QueryResult {
                    peer_id,
                    query_id,
                    result,
                } => {
                    self.handle_query_result(peer_id, query_id, result)
                        .unwrap_or_else(|e| {
                            tracing::error!("Error handling query result: {e:?}");
                        });
                }
                GatewayEvent::QueryDropped { query_id } => {
                    if let Some(_) = self.tasks.lock().remove(&query_id) {
                        metrics::QUERIES_RUNNING.dec();
                        // drop result_tx
                    } else {
                        tracing::error!("Not expecting response for query {query_id}");
                    }
                }
            }
        }
    }

    pub fn find_chunk(&self, dataset: &DatasetId, block: u64) -> Option<BlockRange> {
        self.dataset_storage.find_chunk(dataset, block)
    }

    pub fn next_chunk(&self, dataset: &DatasetId, chunk: &BlockRange) -> Option<BlockRange> {
        self.dataset_storage.next_chunk(dataset, chunk)
    }

    pub fn find_worker(&self, dataset: &DatasetId, block: u64) -> Option<PeerId> {
        self.network_state.lock().find_worker(dataset, block)
    }

    pub fn get_height(&self, dataset: &DatasetId) -> Option<u64> {
        self.network_state.lock().get_height(dataset)
    }

    pub fn query_worker(
        &self,
        worker: &PeerId,
        dataset: &DatasetId,
        query: String,
    ) -> Result<oneshot::Receiver<QueryResult>, QueueFull> {
        let query_id = generate_query_id();
        tracing::trace!("Sending query {query_id} to {worker}");

        self.network_state.lock().lease_worker(*worker);

        let (result_tx, result_rx) = oneshot::channel();

        let task = QueryTask {
            result_tx,
            worker_id: *worker,
        };
        let mut tasks = self.tasks.lock();
        if tasks.len() >= MAX_CONCURRENT_QUERIES {
            return Err(QueueFull);
        }
        tasks.insert(query_id.clone(), task);
        drop(tasks);

        self.transport_handle
            .send_query(
                *worker,
                Query {
                    dataset: dataset.to_string(),
                    query_id: query_id.clone(),
                    query,
                    block_range: None,
                    chunk_id: "".to_owned(), // TODO: pass chunk id
                    timestamp_ms: timestamp_now_ms(),
                    signature: Default::default(),
                },
            )
            .map_err(|e| {
                self.tasks.lock().remove(&query_id);
                e
            })?;

        metrics::QUERIES_RUNNING.inc();
        metrics::QUERIES_SENT
            .get_or_create(&vec![("worker".to_string(), worker.to_string())])
            .inc();
        Ok(result_rx)
    }

    fn handle_heartbeat(&self, peer_id: PeerId, heartbeat: Heartbeat) {
        if !heartbeat.version_matches(&SUPPORTED_VERSIONS) {
            metrics::IGNORED_PINGS.inc();
            return;
        }
        tracing::trace!("Ping from {peer_id}");
        metrics::VALID_PINGS.inc();
        todo!();
        // let worker_state = heartbeat
        //     .stored_ranges
        //     .into_iter()
        //     .map(|r| (DatasetId::from_url(r.url), r.ranges.into()))
        //     .collect();
        // self.network_state
        //     .lock()
        //     .update_dataset_states(peer_id, worker_state);
    }

    fn handle_query_result(
        &self,
        peer_id: PeerId,
        query_id: String,
        result: Result<sqd_messages::QueryResult, QueryFailure>,
    ) -> anyhow::Result<()> {
        use query_error::Err;

        tracing::trace!("Got result for query {query_id}");

        let mut tasks = self.tasks.lock();
        let (_query_id, task) = tasks
            .remove_entry(&query_id)
            .ok_or_else(|| anyhow::anyhow!("Not expecting response for query {query_id}"))?;
        metrics::QUERIES_RUNNING.set(tasks.len() as i64);
        drop(tasks);

        assert_eq!(peer_id, task.worker_id);

        let query_result = match result {
            Ok(sqd_messages::QueryResult {
                result: Some(result),
                retry_after_ms,
                ..
            }) => match result {
                query_result::Result::Ok(ok) => {
                    metrics::report_query_result(peer_id, "ok");
                    if task.result_tx.send(Ok(ok)).is_ok() {
                        self.network_state.lock().report_query_success(peer_id);
                    } else {
                        // The result is no longer needed. Either another query has got the result first,
                        // or the stream has been dropped. In either case, consider the query outrun.
                        self.network_state.lock().report_query_outrun(peer_id);
                    }
                    return Ok(());
                }
                query_result::Result::Err(sqd_messages::QueryError { err: Some(err) }) => match err
                {
                    Err::BadRequest(s) => {
                        metrics::report_query_result(peer_id, "bad_request");
                        if task.result_tx.send(Err(QueryError::BadRequest(s))).is_ok() {
                            self.network_state.lock().report_query_success(peer_id);
                        } else {
                            self.network_state.lock().report_query_outrun(peer_id);
                        }
                        return Ok(());
                    }
                    Err::NotFound(s) => {
                        metrics::report_query_result(peer_id, "not_found");
                        self.network_state.lock().report_query_error(peer_id);
                        Err(QueryError::Retriable(s))
                    }
                    Err::ServerError(s) => {
                        metrics::report_query_result(peer_id, "server_error");
                        self.network_state.lock().report_query_error(peer_id);
                        Err(QueryError::Retriable(s))
                    }
                    Err::ServerOverloaded(()) => {
                        metrics::report_query_result(peer_id, "server_overloaded");
                        self.network_state.lock().report_backoff(
                            peer_id,
                            retry_after_ms
                                .map(|ms| Duration::from_millis(ms as u64))
                                .unwrap_or(Duration::from_secs(1)),
                        );
                        Err(QueryError::Retriable("Server overloaded".to_owned()))
                    }
                    Err::TooManyRequests(()) => {
                        metrics::report_query_result(peer_id, "too_many_requests");
                        self.network_state.lock().report_backoff(
                            peer_id,
                            retry_after_ms
                                .map(|ms| Duration::from_millis(ms as u64))
                                .unwrap_or(Duration::from_secs(1)),
                        );
                        Err(QueryError::Retriable("Too many requests".to_owned()))
                    }
                },
                query_result::Result::Err(sqd_messages::QueryError { err: None }) => {
                    metrics::report_query_result(peer_id, "invalid");
                    self.network_state.lock().report_query_error(peer_id);
                    anyhow::bail!("Unknown error message")
                }
            },
            Ok(sqd_messages::QueryResult { result: None, .. }) => {
                metrics::report_query_result(peer_id, "invalid");
                self.network_state.lock().report_query_error(peer_id);
                anyhow::bail!("Unknown error message")
            }
            Err(QueryFailure::Timeout(t)) => {
                metrics::report_query_result(peer_id, "timeout");
                self.network_state.lock().report_query_failure(peer_id);
                Err(QueryError::Retriable(t.to_string()))
            }
            Err(QueryFailure::TransportError(e)) => {
                metrics::report_query_result(peer_id, "transport_error");
                self.network_state.lock().report_query_failure(peer_id);
                Err(QueryError::Retriable(format!("Transport error: {e}")))
            }
            Err(QueryFailure::ValidationError(e)) => {
                metrics::report_query_result(peer_id, "validation_error");
                self.network_state.lock().report_query_failure(peer_id);
                Err(QueryError::Retriable(format!("Validation error: {e}")))
            }
        };

        task.result_tx.send(query_result).ok();

        Ok(())
    }

    pub fn dataset_state(&self, dataset_id: DatasetId) -> Option<DatasetState> {
        self.network_state.lock().dataset_state(dataset_id).cloned()
    }

    pub fn peer_id(&self) -> PeerId {
        self.local_peer_id
    }
}

#[inline(always)]
pub fn timestamp_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .expect("we're after 1970")
        .as_millis()
        .try_into()
        .expect("not that far in the future")
}
