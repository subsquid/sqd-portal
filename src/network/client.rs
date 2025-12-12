use std::time::SystemTime;
use std::{sync::Arc, time::Duration};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures::StreamExt;
use num_rational::Ratio;
use num_traits::ToPrimitive;
use serde::Serialize;
use sqd_primitives::BlockRef;
use tokio::task::JoinError;
use tokio::time::Instant;
use tokio::time::MissedTickBehavior;
use tokio_stream::wrappers::IntervalStream;
use tokio_util::sync::CancellationToken;

use sqd_contract_client::{Client as ContractClient, ClientError, Network, PeerId, Worker};
use sqd_messages::{query_error, query_result, Query, QueryFinished, QueryOk};
use sqd_network_transport::{
    get_agent_info, AgentInfo, Keypair, P2PTransportBuilder, PortalConfig, PortalTransportHandle,
    QueryFailure, TransportArgs,
};
use tracing::{debug_span, instrument, Instrument};

use super::contracts_state::{self, ContractsState};
use super::priorities::NoWorker;
use super::{ChunkNotFound, NetworkState};
use crate::datasets::{DatasetConfig, Datasets};
use crate::hotblocks::HotblocksHandle;
use crate::types::api_types::{DatasetState, WorkerDebugInfo};
use crate::types::{BlockNumber, BlockRange, ChunkId, DataChunk};
use crate::utils::{RwLock, UseOnce};
use crate::{
    config::Config,
    metrics,
    types::{generate_query_id, DatasetId, QueryError},
};

pub type QueryResult = Result<QueryOk, QueryError>;

const LOGS_QUEUE_SIZE: usize = 10000;
const MAX_LOGS_CHUNK_SIZE: usize = 100;
const CONCURRENT_LOGS: usize = 5;
const LOGS_SENDING_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Serialize)]
pub struct CurrentEpoch {
    pub number: u32,
    pub started_at: String,
    pub ended_at: String,
    pub duration_seconds: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Workers {
    pub active_count: u64,
    pub rate_limit_per_worker: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NetworkClientStatus {
    pub peer_id: PeerId,
    pub status: contracts_state::Status,
    pub operator: Option<String>,
    pub current_epoch: Option<CurrentEpoch>,
    pub sqd_locked: Option<String>,
    pub cu_per_epoch: Option<String>,
    pub workers: Option<Workers>,
}

/// Tracks the network state and handles p2p communication
pub struct NetworkClient {
    transport_handle: PortalTransportHandle,
    network_state: NetworkState,
    datasets: Arc<RwLock<Datasets>>,
    contract_client: Box<dyn ContractClient>,
    chain_update_interval: Duration,
    assignment_update_interval: Duration,
    local_peer_id: PeerId,
    keypair: Keypair,
    contracts_state: RwLock<ContractsState>,
    logs_tx: Option<sqd_network_transport::util::Sender<Box<dyn FnOnce() -> QueryFinished + Send>>>,
    logs_rx:
        UseOnce<sqd_network_transport::util::Receiver<Box<dyn FnOnce() -> QueryFinished + Send>>>,
    verify_responses: bool,
}

pub struct NetworkClientBuilder {
    transport_builder: P2PTransportBuilder,
    network: Network,
    config: Arc<Config>,
    datasets: Arc<RwLock<Datasets>>,
    hotblocks: Arc<HotblocksHandle>,
}

impl NetworkClientBuilder {
    pub fn peer_id(&self) -> PeerId {
        self.transport_builder.local_peer_id()
    }

    /// Builds the NetworkClient and starts network communication
    pub fn build(self) -> anyhow::Result<Arc<NetworkClient>> {
        let Self {
            network,
            config,
            datasets,
            hotblocks,
            transport_builder,
        } = self;

        let contract_client = transport_builder.contract_client();
        let local_peer_id = transport_builder.local_peer_id();
        let keypair = transport_builder.keypair();

        let mut portal_config = PortalConfig {
            log_sending_timeout: LOGS_SENDING_TIMEOUT,
            ..Default::default()
        };
        portal_config.query_config.max_concurrent_streams = None;
        portal_config.query_config.request_timeout = config.transport_timeout;
        let transport_handle = transport_builder.build_portal(portal_config)?;

        let (logs_tx, logs_rx) = if config.send_logs {
            let (tx, rx) = sqd_network_transport::util::new_queue(LOGS_QUEUE_SIZE, "query_logs");
            (Some(tx), UseOnce::new(rx))
        } else {
            (None, UseOnce::empty())
        };

        let datasets_copy = datasets.clone();

        let network_state = NetworkState::new(
            datasets.clone(),
            network,
            &config.assignments_url,
            config.priorities.clone(),
        );

        let this = Arc::new(NetworkClient {
            chain_update_interval: config.chain_update_interval,
            assignment_update_interval: config.assignments_update_interval,
            transport_handle,
            network_state,
            datasets,
            contract_client,
            local_peer_id,
            keypair,
            contracts_state: RwLock::new(Default::default(), "NetworkClient::contracts_state"),
            logs_tx,
            logs_rx,
            verify_responses: config.verify_worker_responses,
        });

        this.reset_height_updates(hotblocks.clone());

        let client_handle = this.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(config.datasets_update_interval).await;
                match Datasets::update(&datasets_copy, &config).await {
                    Ok(()) => {
                        client_handle.reset_height_updates(hotblocks.clone());
                    }
                    Err(e) => tracing::warn!("Failed to update datasets mapping: {e:?}"),
                }
            }
        });

        Ok(this)
    }
}

impl NetworkClient {
    pub async fn builder(
        args: TransportArgs,
        config: Arc<Config>,
        datasets: Arc<RwLock<Datasets>>,
        hotblocks: Arc<HotblocksHandle>,
    ) -> anyhow::Result<NetworkClientBuilder> {
        let agent_into = get_agent_info!();
        let network = args.rpc.network;
        let transport_builder = P2PTransportBuilder::from_cli(args, agent_into).await?;
        Ok(NetworkClientBuilder {
            network,
            config,
            datasets,
            hotblocks,
            transport_builder,
        })
    }

    pub async fn run(
        self: Arc<Self>,
        cancellation_token: CancellationToken,
    ) -> Result<(), JoinError> {
        let this = Arc::clone(&self);
        let token = cancellation_token.child_token();
        let chain_updates_fut = tokio::spawn(async move { this.run_chain_updates(token).await });

        let this = Arc::clone(&self);
        let token = cancellation_token.child_token();
        let assignments_loop_fut =
            tokio::spawn(async move { this.run_assignments_loop(token).await });

        let this = Arc::clone(&self);
        let token = cancellation_token.child_token();
        let logs_loop_fut = tokio::spawn(async move { this.run_logs_loop(token).await });

        tokio::try_join!(chain_updates_fut, assignments_loop_fut, logs_loop_fut)?;
        Ok(())
    }

    async fn fetch_blockchain_state(
        &self,
    ) -> Result<
        (
            u32,
            Option<(String, Ratio<u128>)>,
            Duration,
            bool,
            Vec<Worker>,
            SystemTime,
            u64,
        ),
        ClientError,
    > {
        tokio::try_join!(
            self.contract_client.current_epoch(),
            self.contract_client.portal_sqd_locked(self.local_peer_id),
            self.contract_client.epoch_length(),
            self.contract_client
                .portal_uses_default_strategy(self.local_peer_id),
            self.contract_client.active_workers(),
            self.contract_client.current_epoch_start(),
            self.contract_client
                .portal_compute_units_per_epoch(self.local_peer_id),
        )
    }

    async fn run_chain_updates(&self, cancellation_token: CancellationToken) {
        let mut interval = tokio::time::interval_at(
            Instant::now() + self.chain_update_interval,
            self.chain_update_interval,
        );

        let mut first_iteration = true; // don't wait on the first term
        let mut first_fetch = true;
        let mut current_epoch: u32 = 0;
        let mut operator;
        loop {
            if first_iteration {
                first_iteration = false;
            } else {
                tokio::select! {
                    _ = interval.tick() => {}
                    () = cancellation_token.cancelled() => {
                        break;
                    }
                }
            }

            let (
                epoch,
                sqd_locked,
                epoch_length,
                uses_default_strategy,
                active_workers,
                epoch_started,
                compute_units_per_epoch,
            ) = match self.fetch_blockchain_state().await {
                Ok(data) => data,
                Err(e) => {
                    tracing::warn!("Couldn't get blockchain data: {e}");
                    continue;
                }
            };

            if first_fetch {
                first_fetch = false;

                current_epoch = epoch;
                operator = sqd_locked.clone().map(|s| s.0);

                tracing::info!(
                    "Portal operator {}, current epoch: {}",
                    operator.unwrap_or_else(|| "unknown".to_string()),
                    current_epoch
                )
            }

            self.contracts_state.write().set(
                current_epoch,
                sqd_locked,
                epoch_length,
                uses_default_strategy,
                &active_workers,
                epoch_started,
                compute_units_per_epoch,
            );

            if epoch != current_epoch {
                tracing::info!("Epoch {epoch} started");
                current_epoch = epoch;
                self.network_state.reset_allocations();
            }
        }
    }

    async fn run_assignments_loop(&self, cancellation_token: CancellationToken) {
        let mut timer =
            tokio::time::interval_at(tokio::time::Instant::now(), self.assignment_update_interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        IntervalStream::new(timer)
            .take_until(cancellation_token.cancelled_owned())
            .for_each(|_| self.network_state.try_update_assignment())
            .await;
        tracing::info!("Assignment processing task finished");
    }

    async fn run_logs_loop(&self, cancellation_token: CancellationToken) {
        let Ok(logs_rx) = self.logs_rx.take() else {
            return;
        };
        logs_rx
            .ready_chunks(MAX_LOGS_CHUNK_SIZE)
            .take_until(cancellation_token.cancelled_owned())
            .for_each_concurrent(CONCURRENT_LOGS, |log_fns| async move {
                let msg = tokio::task::spawn_blocking(|| {
                    log_fns
                        .into_iter()
                        .map(|f| {
                            let _span = debug_span!("generate_log");
                            f()
                        })
                        .collect()
                })
                .await
                .unwrap();
                self.transport_handle.send_logs(msg).await;
            })
            .await;
    }

    pub fn dataset(&self, alias: &str) -> Option<DatasetConfig> {
        self.datasets.read().get(alias).cloned()
    }

    pub fn datasets(&self) -> &RwLock<Datasets> {
        &self.datasets
    }

    pub fn first_existing_block(&self, dataset: &DatasetId) -> Option<BlockNumber> {
        self.network_state.dataset_storage.first_block(dataset)
    }

    pub fn find_chunk(&self, dataset: &DatasetId, block: u64) -> Result<DataChunk, ChunkNotFound> {
        self.network_state
            .dataset_storage
            .find_chunk(dataset, block)
    }

    pub fn find_chunk_by_timestamp(
        &self,
        dataset: &DatasetId,
        timestamp: u64,
    ) -> Result<DataChunk, ChunkNotFound> {
        self.network_state
            .dataset_storage
            .find_chunk_by_timestamp(dataset, timestamp)
    }

    pub fn next_chunk(&self, dataset: &DatasetId, chunk: &DataChunk) -> Option<DataChunk> {
        self.network_state
            .dataset_storage
            .next_chunk(dataset, chunk)
    }

    pub fn head(&self, dataset: &DatasetId) -> Option<BlockRef> {
        self.network_state.dataset_storage.head(dataset)
    }

    pub fn find_worker(
        &self,
        dataset: &DatasetId,
        block: u64,
        lease: bool,
    ) -> Result<PeerId, NoWorker> {
        let worker = self.network_state.find_worker(dataset, block, lease);
        worker
    }

    pub fn get_workers(&self, dataset: &DatasetId, block: u64) -> Vec<WorkerDebugInfo> {
        self.network_state.get_workers(dataset, block)
    }

    pub fn get_all_workers(&self) -> Vec<WorkerDebugInfo> {
        self.network_state.get_all_workers()
    }

    pub fn get_height(&self, dataset: &DatasetId) -> Option<u64> {
        self.network_state.get_height(dataset)
    }

    #[instrument(skip_all, level = "debug", fields(query_id))]
    pub async fn query_worker(
        self: Arc<Self>,
        worker: PeerId,
        request_id: String,
        chunk_id: ChunkId,
        block_range: BlockRange,
        query: String,
        lease: bool,
    ) -> QueryResult {
        let query_id = generate_query_id();
        tracing::Span::current().record("query_id", &query_id);
        tracing::trace!("Sending query {query_id} to {worker}");

        if lease {
            self.network_state.lease_worker(worker);
        }

        let mut query = Query {
            dataset: chunk_id.dataset.to_url().to_owned(),
            query_id: query_id.clone(),
            request_id: request_id,
            query,
            block_range: Some(sqd_messages::Range {
                begin: *block_range.start(),
                end: *block_range.end(),
            }),
            chunk_id: chunk_id.chunk.to_string(),
            timestamp_ms: timestamp_now_ms(),
            signature: Default::default(),
            compression: sqd_messages::Compression::Gzip as i32,
        };
        let query = tokio::task::spawn_blocking({
            let keypair = self.keypair.clone();
            move || {
                query
                    .sign(&keypair, worker)
                    .expect("Query should be valid to sign");
                query
            }
        })
        .instrument(tracing::debug_span!("sign_query"))
        .await
        .unwrap();

        metrics::QUERIES_RUNNING.inc();
        metrics::QUERIES_SENT
            .get_or_create(&vec![("worker".to_string(), worker.to_string())])
            .inc();

        let this = self.clone();
        let guard = scopeguard::guard(worker, |worker: PeerId| {
            // The result is no longer needed. Either another query has got the result first,
            // or the stream has been dropped. In either case, consider the query outrun.
            this.network_state.report_query_outrun(worker);
            metrics::QUERIES_RUNNING.dec();
        });

        let query_start_time = Instant::now();
        let result = self
            .transport_handle
            .send_query(worker, query)
            .instrument(tracing::debug_span!("running_query"))
            .await;
        scopeguard::ScopeGuard::into_inner(guard);
        metrics::QUERIES_RUNNING.dec();
        let query_time = query_start_time.elapsed();

        if let Some(logs_tx) = &self.logs_tx {
            if let Ok(result) = result.as_ref() {
                let result = result.clone();
                let f = move || {
                    QueryFinished::new(&result, worker.to_string(), query_time.as_micros() as u32)
                };
                logs_tx.send_lossy(Box::new(f));
            }
        }

        self.parse_query_result(worker, result)
            .await
            .inspect(|_| metrics::report_query_ok(query_time))
    }

    #[instrument(skip_all, level = "debug")]
    async fn parse_query_result(
        &self,
        peer_id: PeerId,
        result: Result<sqd_messages::QueryResult, QueryFailure>,
    ) -> QueryResult {
        use query_error::Err;

        match result {
            Ok(q) if self.verify_responses && !verify_signature(&q, peer_id).await => {
                metrics::report_query_result(&peer_id, "validation_error");
                self.network_state.report_query_failure(peer_id);
                Err(QueryError::Retriable(format!(
                    "invalid worker signature from {peer_id}, result: {q:?}"
                )))
            }
            Ok(sqd_messages::QueryResult {
                result: Some(result),
                retry_after_ms,
                ..
            }) => {
                if let Some(backoff) = retry_after_ms {
                    self.network_state
                        .hint_backoff(peer_id, Duration::from_millis(backoff.into()));
                    metrics::report_backoff(&peer_id);
                };
                match result {
                    query_result::Result::Ok(ok) => {
                        metrics::report_query_result(&peer_id, "ok");
                        self.network_state.report_query_success(peer_id);
                        Ok(ok)
                    }
                    query_result::Result::Err(sqd_messages::QueryError { err: Some(err) }) => {
                        match err {
                            Err::BadRequest(s) => {
                                metrics::report_query_result(&peer_id, "bad_request");
                                self.network_state.report_query_success(peer_id);
                                Err(QueryError::BadRequest(format!(
                                    "couldn't parse request: {s}"
                                )))
                            }
                            Err::NotFound(s) => {
                                // Chunk was not found on the worker. It's probably still downloading it
                                metrics::report_query_result(&peer_id, "not_found");
                                self.network_state.report_query_error(peer_id);
                                Err(QueryError::Retriable(s))
                            }
                            Err::ServerError(s) => {
                                metrics::report_query_result(&peer_id, "server_error");
                                self.network_state.report_query_error(peer_id);
                                Err(QueryError::Failure(s))
                            }
                            Err::ServerOverloaded(()) => {
                                metrics::report_query_result(&peer_id, "server_overloaded");
                                self.network_state.report_query_error(peer_id);
                                if retry_after_ms.is_none() {
                                    self.network_state
                                        .hint_backoff(peer_id, Duration::from_millis(1000));
                                }
                                Err(QueryError::Retriable("worker overloaded".to_owned()))
                            }
                            Err::TooManyRequests(()) => {
                                metrics::report_query_result(&peer_id, "too_many_requests");
                                self.network_state.report_query_success(peer_id);
                                if retry_after_ms.is_none() {
                                    self.network_state
                                        .hint_backoff(peer_id, Duration::from_millis(100));
                                }
                                Err(QueryError::RateLimitExceeded)
                            }
                        }
                    }
                    query_result::Result::Err(sqd_messages::QueryError { err: None }) => {
                        metrics::report_query_result(&peer_id, "invalid");
                        self.network_state.report_query_error(peer_id);
                        Err(QueryError::Retriable("unknown error message".to_string()))
                    }
                }
            }
            Ok(sqd_messages::QueryResult { result: None, .. }) => {
                metrics::report_query_result(&peer_id, "invalid");
                self.network_state.report_query_error(peer_id);
                Err(QueryError::Retriable("unknown error message".to_string()))
            }
            Err(QueryFailure::InvalidRequest(e)) => {
                metrics::report_query_result(&peer_id, "invalid");
                Err(QueryError::Failure(format!(
                    "portal tried to send invalid request: {e}"
                )))
            }
            Err(QueryFailure::InvalidResponse(e)) => {
                metrics::report_query_result(&peer_id, "invalid");
                self.network_state.report_query_error(peer_id);
                Err(QueryError::Retriable(format!(
                    "couldn't decode response: {e}"
                )))
            }
            Err(QueryFailure::Timeout(t)) => {
                metrics::report_query_result(&peer_id, "timeout");
                self.network_state.report_query_failure(peer_id);
                let msg = match t {
                    sqd_network_transport::StreamClientTimeout::Connect => {
                        "timed out connecting to the peer"
                    }
                    sqd_network_transport::StreamClientTimeout::Request => {
                        "timed out reading response"
                    }
                };
                Err(QueryError::Retriable(msg.to_owned()))
            }
            Err(QueryFailure::TransportError(e)) => {
                metrics::report_query_result(&peer_id, "transport_error");
                self.network_state.report_query_failure(peer_id);
                Err(QueryError::Retriable(format!("transport error: {e}")))
            }
        }
    }

    pub fn dataset_state(&self, dataset_id: &DatasetId) -> Option<DatasetState> {
        self.network_state.dataset_state(dataset_id)
    }

    pub fn get_status(&self) -> NetworkClientStatus {
        let state = self.contracts_state.read().clone();

        let epoch_secs = state.epoch_length.as_secs();
        let started_at: DateTime<Utc> = state.current_epoch_started.into();
        let ended_at = started_at + ChronoDuration::seconds(epoch_secs as i64);

        if state.status == contracts_state::Status::DataLoading {
            NetworkClientStatus {
                peer_id: self.local_peer_id,
                status: state.status,
                operator: None,
                sqd_locked: None,
                current_epoch: None,
                cu_per_epoch: None,
                workers: None,
            }
        } else {
            NetworkClientStatus {
                peer_id: self.local_peer_id,
                status: state.status,
                operator: Some(state.operator),
                sqd_locked: state.sqd_locked.to_f32().map(|r| r.to_string()),
                cu_per_epoch: Some(state.compute_units_per_epoch.to_string()),
                current_epoch: Some(CurrentEpoch {
                    number: state.current_epoch,
                    started_at: started_at.to_rfc3339(),
                    ended_at: ended_at.to_rfc3339(),
                    duration_seconds: epoch_secs,
                }),
                workers: Some(Workers {
                    active_count: state.active_workers_length,
                    rate_limit_per_worker: if state.uses_default_strategy {
                        let rate_limit = if epoch_secs > 0 && state.active_workers_length > 0 {
                            let cu_per_worker = (state.compute_units_per_epoch
                                / state.active_workers_length)
                                as f64;

                            cu_per_worker / (epoch_secs as f64)
                        } else {
                            0.0
                        };
                        Some(rate_limit.to_string())
                    } else {
                        None
                    },
                }),
            }
        }
    }

    pub fn is_ready(&self) -> bool {
        self.network_state.dataset_storage.has_assignment()
    }

    pub fn reset_height_updates(&self, hotblocks: Arc<HotblocksHandle>) {
        let datasets = self.datasets.read();
        self.network_state
            .dataset_storage
            .unsubscribe_head_updates();
        for dataset in datasets.iter() {
            if let (Some(dataset_id), Some(_)) = (&dataset.network_id, &dataset.hotblocks) {
                let hotblocks = hotblocks.clone();
                let name = dataset.default_name.clone();
                tracing::info!(
                    "Hotblocks storage for '{name}' set to track dataset height of '{dataset_id}'"
                );
                self.network_state.dataset_storage.subscribe_head_updates(
                    dataset_id,
                    Box::new(move |block| {
                        let hotblocks = hotblocks.clone();
                        let name = name.clone();
                        tokio::spawn(async move {
                            tracing::info!(
                                "Cleaning hotblocks storage for '{name}' up to block {}",
                                block.number
                            );
                            let res = hotblocks.retain_with_retries(&name, block.number + 1).await;
                            if let Err(e) = res {
                                tracing::warn!(
                                    "Failed to clean hotblocks storage for '{name}': {e:?}"
                                );
                            }
                        });
                    }),
                );
            }
        }
    }
}

#[instrument(skip_all, level = "debug")]
async fn verify_signature(query: &sqd_messages::QueryResult, peer_id: PeerId) -> bool {
    let query = query.clone();
    tokio::task::spawn_blocking(move || query.verify_signature(peer_id))
        .await
        .unwrap()
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
