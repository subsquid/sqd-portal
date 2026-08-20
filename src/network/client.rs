use std::time::SystemTime;
use std::{sync::Arc, time::Duration};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures::{AsyncReadExt, StreamExt};
use num_rational::Ratio;
use num_traits::ToPrimitive;
use prost::Message;
use serde::Serialize;
use sqd_primitives::BlockRef;
use tokio::task::JoinError;
use tokio::time::Instant;
use tokio::time::MissedTickBehavior;
use tokio_stream::wrappers::IntervalStream;
use tokio_util::sync::CancellationToken;
use utoipa::ToSchema;

use sqd_contract_client::{Client as ContractClient, ClientError, Network, PeerId, Worker};
use sqd_messages::{query_error, query_result, Query, QueryFinished, QueryOk};
use sqd_network_transport::{
    get_agent_info, AgentInfo, Keypair, P2PTransportBuilder, PortalConfig, PortalTransportHandle,
    QueryFailure, StreamClientTimeout, TransportArgs, QUERY_RESULT_MAX_SIZE,
};
use tracing::{debug_span, instrument, Instrument};

use super::contracts_state::{ContractsState, Status};
use super::priorities::NoWorker;
use super::{AssignmentSource, ChunkNotFound, NetworkState, WorkerLease};
use crate::controller::download_scheduler::{DownloadScheduler, Outcome, Priority};
use crate::datasets::{DatasetConfig, Datasets};
use crate::types::api_types::{DatasetState, WorkerDebugInfo};
use crate::types::{BlockNumber, BlockRange, ChunkId, Compression, DataChunk};
use crate::utils::{RwLock, UseOnce};
use crate::{
    config::Config,
    metrics,
    types::{generate_query_id, DatasetId, QueryError},
};

/// The subset of [`NetworkClient`] that the stream controller depends on.
///
/// Abstracting it into a trait allows exercising the controller's chunk
/// scheduling logic in tests with a mock network.
pub trait StreamingNetwork: Send + Sync + 'static {
    fn find_chunk(&self, dataset: &DatasetId, block: u64) -> Result<DataChunk, ChunkNotFound>;

    fn next_chunk(&self, dataset: &DatasetId, chunk: &DataChunk) -> Option<DataChunk>;

    fn find_worker(&self, dataset: &DatasetId, block: u64) -> Result<WorkerLease, NoWorker>;

    #[allow(clippy::too_many_arguments)]
    fn query_worker(
        self: Arc<Self>,
        lease: WorkerLease,
        request_id: String,
        chunk_id: ChunkId,
        block_range: BlockRange,
        query: String,
        compression: Compression,
        priority: Option<u32>,
    ) -> futures::future::BoxFuture<'static, QueryResult>;

    /// A response the controller rejected as contract-violating. The transport
    /// can't catch these — only the caller knows the range it asked for — so the
    /// penalty and the OB-4 counter are raised from here instead.
    fn report_integrity_failure(&self, worker: PeerId);
}

impl StreamingNetwork for NetworkClient {
    fn find_chunk(&self, dataset: &DatasetId, block: u64) -> Result<DataChunk, ChunkNotFound> {
        NetworkClient::find_chunk(self, dataset, block)
    }

    fn next_chunk(&self, dataset: &DatasetId, chunk: &DataChunk) -> Option<DataChunk> {
        NetworkClient::next_chunk(self, dataset, chunk)
    }

    fn find_worker(&self, dataset: &DatasetId, block: u64) -> Result<WorkerLease, NoWorker> {
        NetworkClient::find_worker(self, dataset, block)
    }

    fn query_worker(
        self: Arc<Self>,
        lease: WorkerLease,
        request_id: String,
        chunk_id: ChunkId,
        block_range: BlockRange,
        query: String,
        compression: Compression,
        priority: Option<u32>,
    ) -> futures::future::BoxFuture<'static, QueryResult> {
        Box::pin(NetworkClient::query_worker(
            self,
            lease,
            request_id,
            chunk_id,
            block_range,
            query,
            compression,
            priority,
        ))
    }

    fn report_integrity_failure(&self, worker: PeerId) {
        metrics::report_query_result(&worker, "integrity");
        self.network_state.report_query_error(worker);
    }
}

/// Whether the worker stays a candidate. Only two outcomes today; the split from the
/// cooldown *class* is GAP-27.
enum Health {
    Ok,
    Error,
}

/// Matched verbatim: the wire carries no code for this verdict (GAP-25).
const STALE_ENVELOPE: &str = "timestamp out of allowed range";

/// One DC-1 row: how a worker verdict is classified, counted, and charged. The three
/// were chosen independently in each match arm — eight arms times three decisions — and
/// they drifted: the two capacity refusals shared a row in the spec while disagreeing
/// here on both the error class and the health signal.
struct Verdict {
    error: QueryError,
    label: &'static str,
    health: Health,
    backs_off: bool,
}

impl Verdict {
    fn of(err: query_error::Err) -> Self {
        use query_error::Err;
        let row = |error, label, health, backs_off| Verdict {
            error,
            label,
            health,
            backs_off,
        };
        match err {
            // Clock skew, not a bad query: another worker may accept the same bytes.
            Err::BadRequest(s) if s == STALE_ENVELOPE => {
                row(QueryError::Retriable(s), "clock_skew", Health::Error, false)
            }
            Err::BadRequest(s) => row(
                QueryError::BadRequest(format!("couldn't parse request: {s}")),
                "bad_request",
                Health::Ok,
                false,
            ),
            // Probably still downloading the chunk.
            Err::NotFound(s) => row(QueryError::Retriable(s), "not_found", Health::Error, false),
            // Split inside the arm, not by guard: a guard cannot bind what it matched, so it
            // had to parse once to test and again to use, behind an `expect`.
            Err::ServerError(s) => match parse_base_block_mismatch(&s) {
                // Input validation, not a bad response.
                Some(base_block) => row(
                    QueryError::BaseBlockMismatch(base_block),
                    "block_mismatch",
                    Health::Ok,
                    false,
                ),
                // The query covers too much data: narrowing it is the client's move, and
                // another worker would answer the same.
                None if s == "Response too large" => row(
                    QueryError::BadRequest(
                        "the response for this block exceeds the size limit; \
                         try narrowing the query to request only the necessary data"
                            .to_owned(),
                    ),
                    "response_too_large",
                    Health::Ok,
                    false,
                ),
                None => row(QueryError::Failure(s), "server_error", Health::Error, false),
            },
            // One row, two verdicts: both are capacity refusals.
            Err::ServerOverloaded(()) => row(
                QueryError::RateLimitExceeded,
                "server_overloaded",
                Health::Error,
                true,
            ),
            Err::TooManyRequests(()) => row(
                QueryError::RateLimitExceeded,
                "too_many_requests",
                Health::Ok,
                true,
            ),
        }
    }
}

/// The response buffer as `Bytes`, adopting its allocation when that is not wasteful.
///
/// `QueryOk::data` is a `bytes` field, and prost shares the input buffer for one only when the
/// input is itself `Bytes`; from a slice it allocates and copies the whole payload. Adopting the
/// read buffer instead hands the payload onwards having been copied once, off the socket.
///
/// The catch is that `Bytes::from(Vec)` takes the allocation whole, capacity included, and the
/// read buffer starts at a megabyte. A barely-filled one would pin all of it for as long as the
/// payload sits in a stream's buffer, which is bounded by response *count*, not bytes. So the
/// buffer is only adopted while it is at least half full, which caps what a payload can hold at
/// twice its own size; a smaller response is copied, where copying is cheap and the saving large.
fn share_or_copy(buf: Vec<u8>) -> bytes::Bytes {
    if buf.len().saturating_mul(2) >= buf.capacity() {
        bytes::Bytes::from(buf)
    } else {
        bytes::Bytes::copy_from_slice(&buf)
    }
}

/// The data is opaque, so a `last_block` outside the queried range cannot be trimmed to
/// it, and the continuation derived from it is inverted or re-covers delivered blocks.
fn out_of_range(ok: &QueryOk, range: &BlockRange) -> bool {
    !range.contains(&ok.last_block)
}

#[derive(Debug)]
pub struct QuerySuccess {
    pub ok: QueryOk,
    pub ttfb: Duration,
    pub transfer_time: Duration,
    pub response_size: usize,
}

pub type QueryResult = Result<QuerySuccess, QueryError>;

enum ReadError {
    TooLarge,
    Transport(String),
}

type ResponseStream = Box<dyn futures::AsyncRead + Unpin + Send>;

struct QueryGuard {}

impl QueryGuard {
    fn new() -> Self {
        metrics::QUERIES_RUNNING.inc();
        Self {}
    }
}

impl Drop for QueryGuard {
    fn drop(&mut self) {
        metrics::QUERIES_RUNNING.dec();
    }
}

const CHUNK_SIZE: usize = 1024 * 1024;
const LOGS_QUEUE_SIZE: usize = 10000;
const MAX_LOGS_CHUNK_SIZE: usize = 100;
const CONCURRENT_LOGS: usize = 5;
const LOGS_SENDING_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct CurrentEpoch {
    pub number: u32,
    pub started_at: String,
    pub ended_at: String,
    pub duration_seconds: u64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Workers {
    pub active_count: u64,
    pub rate_limit_per_worker: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct NetworkClientStatus {
    #[schema(value_type = String)]
    pub peer_id: PeerId,
    pub status: Status,
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
    read_scheduler: Option<Arc<DownloadScheduler>>,
    transport_timeout: Duration,
    default_worker_backoff: Duration,
}

pub struct NetworkClientBuilder {
    transport_builder: P2PTransportBuilder,
    network: Network,
    config: Arc<Config>,
    datasets: Arc<RwLock<Datasets>>,
    assignment_source: AssignmentSource,
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
            transport_builder,
            assignment_source,
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

        let mut network_state = NetworkState::new(
            datasets.clone(),
            network,
            &config.assignments_url,
            config.priorities.clone(),
        );
        if config.ignore_deprecated_workers {
            network_state.ignore_deprecated_workers();
        }
        network_state.set_assignment_source(assignment_source);

        let read_scheduler = if config.congestion.enabled {
            let sched = Arc::new(DownloadScheduler::new(config.congestion.clone()));
            metrics::CONGESTION_WINDOW.set(sched.window_size() as i64);
            Some(sched)
        } else {
            None
        };

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
            read_scheduler,
            transport_timeout: config.transport_timeout,
            default_worker_backoff: config.default_worker_backoff,
        });

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(config.datasets_update_interval).await;
                if let Err(e) = Datasets::update(&datasets_copy, &config).await {
                    tracing::error!("Failed to update datasets mapping: {e:?}")
                }
            }
        });

        Ok(this)
    }
}

/// Why the portal is not ready to serve requests. The variant identifies the
/// failure *category* (stable across fluctuating counts); `Display` renders the
/// detailed, human-readable explanation used in logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotReady {
    /// No workers are known for any dataset yet (still bootstrapping).
    NoWorkers,
    /// Fewer than the required fraction of worker connections are established.
    InsufficientConnections {
        active: usize,
        required: usize,
        workers: usize,
    },
}

impl std::fmt::Display for NotReady {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NotReady::NoWorkers => write!(f, "no workers known for any dataset yet"),
            NotReady::InsufficientConnections {
                active,
                required,
                workers,
            } => write!(
                f,
                "not enough active connections: {active} active < {required} required ({workers} workers known)"
            ),
        }
    }
}

impl NetworkClient {
    pub async fn builder(
        args: TransportArgs,
        config: Arc<Config>,
        datasets: Arc<RwLock<Datasets>>,
        assignment_source: AssignmentSource,
    ) -> anyhow::Result<NetworkClientBuilder> {
        let agent_into = get_agent_info!();
        let network = args.rpc.network;
        let transport_builder = P2PTransportBuilder::from_cli(args, agent_into).await?;
        Ok(NetworkClientBuilder {
            network,
            config,
            datasets,
            transport_builder,
            assignment_source,
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
            metrics::AVAILABLE_COMPUTE_UNITS.set(compute_units_per_epoch as i64);

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

    pub fn find_worker(&self, dataset: &DatasetId, block: u64) -> Result<WorkerLease, NoWorker> {
        self.network_state.find_worker(dataset, block)
    }

    pub fn reserve_worker(&self, worker: PeerId) -> Option<WorkerLease> {
        self.network_state.reserve_worker(worker)
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
        lease: WorkerLease,
        request_id: String,
        chunk_id: ChunkId,
        block_range: BlockRange,
        query: String,
        compression: Compression,
        priority: Option<u32>,
    ) -> QueryResult {
        let query_id = generate_query_id();
        let worker = lease.worker();
        tracing::Span::current().record("query_id", &query_id);
        tracing::trace!("Sending query {query_id} to {worker}");

        let _guard = QueryGuard::new();
        let query = self
            .prepare_query(
                worker,
                &query_id,
                request_id,
                chunk_id,
                &block_range,
                query,
                compression,
            )
            .await;
        let result = self
            .execute_query(worker, query, &block_range, priority)
            .await;
        result
    }

    async fn execute_query(
        &self,
        worker: PeerId,
        query: Query,
        block_range: &BlockRange,
        priority: Option<u32>,
    ) -> QueryResult {
        let mut stream = self.send_to_transport(worker, query, priority).await?;
        let network_start = Instant::now();
        let mut buf = self.receive_first_byte(worker, &mut stream).await?;
        let ttfb = network_start.elapsed();
        let transfer_time = self
            .download_body(worker, &mut stream, &mut buf, priority)
            .await?;
        let query_time = network_start.elapsed();
        self.finalize_response(worker, buf, block_range, ttfb, transfer_time, query_time)
            .await
    }

    async fn prepare_query(
        &self,
        worker: PeerId,
        query_id: &str,
        request_id: String,
        chunk_id: ChunkId,
        block_range: &BlockRange,
        query: String,
        compression: Compression,
    ) -> Query {
        let compression = match compression {
            Compression::Gzip => sqd_messages::Compression::Gzip,
            Compression::Zstd => sqd_messages::Compression::Zstd,
        } as i32;
        let mut query = Query {
            dataset: chunk_id.dataset.to_url().to_owned(),
            query_id: query_id.to_owned(),
            request_id,
            query,
            block_range: Some(sqd_messages::Range {
                begin: *block_range.start(),
                end: *block_range.end(),
            }),
            chunk_id: chunk_id.chunk.to_string(),
            timestamp_ms: timestamp_now_ms(),
            signature: Default::default(),
            compression,
            // 0 for a chunk the legacy artifact produced, which names no version: proto3 leaves
            // the default off the wire, so the field is absent exactly when there is none to send.
            chunk_version: chunk_id.chunk.version(),
            query_engine: Default::default(),
            output_format: Default::default(),
        };
        tokio::task::spawn_blocking({
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
        .unwrap()
    }

    async fn send_to_transport(
        &self,
        worker: PeerId,
        query: Query,
        priority: Option<u32>,
    ) -> Result<ResponseStream, QueryError> {
        metrics::QUERIES_SENT
            .get_or_create(&vec![("worker".to_string(), worker.to_string())])
            .inc();

        let send_permit = match (&self.read_scheduler, priority) {
            (Some(sched), Some(prio)) => Some(sched.acquire(prio).await),
            _ => None,
        };
        match self
            .transport_handle
            .send_query_request(worker, query)
            .instrument(tracing::debug_span!("send_query"))
            .await
        {
            Ok(stream) => {
                if let Some(mut permit) = send_permit {
                    permit.outcome = Outcome::Success;
                }
                Ok(stream)
            }
            Err(failure) => {
                if let Some(mut permit) = send_permit {
                    if is_congestion_failure(&failure) {
                        permit.outcome = Outcome::Congestion;
                    }
                } else if is_congestion_failure(&failure) {
                    self.signal_congestion();
                }
                Err(self.convert_query_failure(worker, failure))
            }
        }
    }

    async fn receive_first_byte(
        &self,
        worker: PeerId,
        stream: &mut ResponseStream,
    ) -> Result<Vec<u8>, QueryError> {
        // Intentionally not behind a download-scheduler permit.
        // The constrained portal-side resource is the network bandwidth,
        // so only `download_body` holds a permit.
        // This lets arbitrarily many queries be in flight while the congestion
        // window bounds only the parallel reads.
        //
        // It's not perfect because the yamux substream buffer will be filled until the
        // backpressure propagates to the worker. But this is a reasonable tradeoff.
        let mut buf = Vec::with_capacity(1024 * 1024);
        wait_for_first_byte(stream, &mut buf, self.transport_timeout)
            .instrument(tracing::debug_span!("wait_first_byte"))
            .await
            .map_err(|failure| {
                if is_congestion_failure(&failure) {
                    self.signal_congestion();
                }
                self.convert_query_failure(worker, failure)
            })?;
        Ok(buf)
    }

    async fn download_body(
        &self,
        worker: PeerId,
        stream: &mut ResponseStream,
        buf: &mut Vec<u8>,
        priority: Option<u32>,
    ) -> Result<Duration, QueryError> {
        let result = match (&self.read_scheduler, priority) {
            (Some(sched), Some(prio)) => {
                read_response_with_permits(
                    sched,
                    prio,
                    stream,
                    buf,
                    sched.read_timeout(),
                    QUERY_RESULT_MAX_SIZE,
                )
                .instrument(tracing::debug_span!("read_response"))
                .await
            }
            _ => {
                read_response_simple(stream, buf, self.transport_timeout, QUERY_RESULT_MAX_SIZE)
                    .instrument(tracing::debug_span!("read_response"))
                    .await
            }
        };
        result.map_err(|e| self.convert_read_error(worker, e))
    }

    async fn finalize_response(
        &self,
        worker: PeerId,
        buf: Vec<u8>,
        block_range: &BlockRange,
        ttfb: Duration,
        transfer_time: Duration,
        query_time: Duration,
    ) -> QueryResult {
        let response_size = buf.len();
        let result = sqd_messages::QueryResult::decode(share_or_copy(buf))
            .map_err(|e| QueryFailure::InvalidResponse(e.to_string()));

        if let Some(logs_tx) = &self.logs_tx {
            if let Ok(result) = result.as_ref() {
                let result = result.clone();
                let f = move || {
                    QueryFinished::new(&result, worker.to_string(), query_time.as_micros() as u32)
                };
                logs_tx.send_lossy(Box::new(f));
            }
        }

        let throughput = if transfer_time.as_secs_f64() > 0.0 {
            Some(response_size as f64 / transfer_time.as_secs_f64())
        } else {
            None
        };
        self.parse_query_result(worker, result, block_range, throughput)
            .await
            .inspect(|_| metrics::report_query_ok(query_time))
            .map(|ok| QuerySuccess {
                ok,
                ttfb,
                transfer_time,
                response_size,
            })
    }

    fn convert_query_failure(&self, peer_id: PeerId, failure: QueryFailure) -> QueryError {
        match failure {
            QueryFailure::InvalidRequest(e) => {
                metrics::report_query_result(&peer_id, "invalid");
                self.network_state.report_query_success(peer_id, None);
                QueryError::Failure(format!("portal tried to send invalid request: {e}"))
            }
            QueryFailure::InvalidResponse(e) => {
                metrics::report_query_result(&peer_id, "integrity");
                self.network_state.report_query_error(peer_id);
                QueryError::Integrity(format!("couldn't decode response: {e}"))
            }
            QueryFailure::Timeout(t) => {
                metrics::report_query_result(&peer_id, "timeout");
                self.network_state.report_query_failure(peer_id);
                let msg = match t {
                    StreamClientTimeout::Connect => "timed out connecting to the peer",
                    StreamClientTimeout::Request => "timed out reading response",
                };
                QueryError::Retriable(msg.to_owned())
            }
            QueryFailure::TransportError(e) => {
                metrics::report_query_result(&peer_id, "transport_error");
                self.network_state.report_query_failure(peer_id);
                QueryError::Retriable(format!("transport error: {e}"))
            }
        }
    }

    fn convert_read_error(&self, peer_id: PeerId, error: ReadError) -> QueryError {
        metrics::report_query_result(&peer_id, "transport_error");
        self.network_state.report_query_failure(peer_id);
        let msg = match error {
            ReadError::TooLarge => "response too large".to_owned(),
            ReadError::Transport(e) => format!("transport error: {e}"),
        };
        QueryError::Retriable(msg)
    }

    #[instrument(skip_all, level = "debug")]
    async fn parse_query_result(
        &self,
        peer_id: PeerId,
        result: Result<sqd_messages::QueryResult, QueryFailure>,
        block_range: &BlockRange,
        throughput: Option<f64>,
    ) -> Result<QueryOk, QueryError> {
        match result {
            Ok(q) if self.verify_responses && !verify_signature(&q, peer_id).await => {
                metrics::report_query_result(&peer_id, "integrity");
                self.network_state.report_query_failure(peer_id);
                Err(QueryError::Integrity(format!(
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
                    // Before the success is recorded: counted first, one wrong-range
                    // answer landed in both `ok` and `integrity`, and left its latency
                    // and throughput in the worker's health.
                    query_result::Result::Ok(ok) if out_of_range(&ok, block_range) => {
                        metrics::report_query_result(&peer_id, "integrity");
                        self.network_state.report_query_error(peer_id);
                        Err(QueryError::Integrity(format!(
                            "worker returned last block {} outside the queried range {}-{}",
                            ok.last_block,
                            block_range.start(),
                            block_range.end()
                        )))
                    }
                    query_result::Result::Ok(ok) => {
                        metrics::report_query_result(&peer_id, "ok");
                        self.network_state.report_query_success(peer_id, throughput);
                        Ok(ok)
                    }
                    query_result::Result::Err(sqd_messages::QueryError { err: Some(err) }) => {
                        let verdict = Verdict::of(err);
                        metrics::report_query_result(&peer_id, verdict.label);
                        match verdict.health {
                            Health::Ok => self.network_state.report_query_success(peer_id, None),
                            Health::Error => self.network_state.report_query_error(peer_id),
                        }
                        if verdict.backs_off && retry_after_ms.is_none() {
                            self.network_state
                                .hint_backoff(peer_id, self.default_worker_backoff);
                        }
                        Err(verdict.error)
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
            Err(failure) => Err(self.convert_query_failure(peer_id, failure)),
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

        if state.status == Status::DataLoading {
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
        self.readiness().is_ok()
    }

    /// Returns `Ok(())` if the portal is ready to serve requests, or `Err` with a
    /// typed reason it is not (for diagnostics / logging). The reason's `Display`
    /// renders a human-readable message, while the variant itself can be compared
    /// to detect state changes without treating fluctuating counts as new states.
    pub fn readiness(&self) -> Result<(), NotReady> {
        let workers = self.network_state.dataset_storage.num_workers();
        let active = self.transport_handle.active_connections() as usize;
        let required = workers * 3 / 4;
        if workers == 0 {
            return Err(NotReady::NoWorkers);
        }
        if active < required {
            return Err(NotReady::InsufficientConnections {
                active,
                required,
                workers,
            });
        }
        Ok(())
    }

    fn signal_congestion(&self) {
        if let Some(sched) = &self.read_scheduler {
            sched.signal_congestion();
        }
    }

    pub fn download_utilization(&self) -> Option<f64> {
        self.read_scheduler.as_ref().map(|s| s.utilization())
    }
}

fn is_congestion_failure(failure: &QueryFailure) -> bool {
    matches!(
        failure,
        QueryFailure::Timeout(_) | QueryFailure::TransportError(_)
    )
}

async fn wait_for_first_byte(
    stream: &mut (impl futures::AsyncRead + Unpin),
    buf: &mut Vec<u8>,
    timeout: Duration,
) -> Result<(), QueryFailure> {
    let fut = async {
        let mut byte = [0u8; 1];
        let n = stream
            .read(&mut byte)
            .await
            .map_err(|e| QueryFailure::TransportError(e.to_string()))?;
        if n == 0 {
            return Err(QueryFailure::InvalidResponse("Empty response".into()));
        }
        buf.push(byte[0]);
        Ok(())
    };
    tokio::time::timeout(timeout, fut)
        .await
        .unwrap_or(Err(QueryFailure::Timeout(StreamClientTimeout::Request)))
}

/// Returns the cumulative time spent in actual reads (excludes permit wait time).
async fn read_response_with_permits(
    scheduler: &Arc<DownloadScheduler>,
    priority: Priority,
    stream: &mut (impl futures::AsyncRead + Unpin),
    buf: &mut Vec<u8>,
    read_timeout: Duration,
    max_size: u64,
) -> Result<Duration, ReadError> {
    let mut chunk_buf = vec![0u8; CHUNK_SIZE];
    let mut transfer_time = Duration::ZERO;
    loop {
        let mut permit = scheduler.acquire(priority).await;
        let read_start = Instant::now();
        match tokio::time::timeout(read_timeout, stream.read(&mut chunk_buf)).await {
            Err(_) => {
                // Timeout: worker stalled, abort download
                permit.outcome = Outcome::Congestion;
                return Err(ReadError::Transport("timed out reading response".into()));
            }
            Ok(Err(e)) => {
                permit.outcome = Outcome::Congestion;
                return Err(ReadError::Transport(e.to_string()));
            }
            Ok(Ok(0)) => {
                // EOF: done reading
                permit.outcome = Outcome::Success;
                return Ok(transfer_time);
            }
            Ok(Ok(n)) => {
                transfer_time += read_start.elapsed();
                buf.extend_from_slice(&chunk_buf[..n]);
                permit.outcome = Outcome::Success;
                if buf.len() as u64 > max_size {
                    return Err(ReadError::TooLarge);
                }
            }
        }
    }
}

async fn read_response_simple(
    stream: &mut (impl futures::AsyncRead + Unpin),
    buf: &mut Vec<u8>,
    timeout: Duration,
    max_size: u64,
) -> Result<Duration, ReadError> {
    let read_start = Instant::now();
    let fut = async {
        let remaining = max_size + 1 - buf.len() as u64;
        stream
            .take(remaining)
            .read_to_end(buf)
            .await
            .map_err(|e| ReadError::Transport(e.to_string()))?;
        if buf.len() as u64 > max_size {
            return Err(ReadError::TooLarge);
        }
        Ok(())
    };
    tokio::time::timeout(timeout, fut)
        .await
        .unwrap_or_else(|_| Err(ReadError::Transport("timed out reading response".into())))?;
    Ok(read_start.elapsed())
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

/// Parses error messages like:
/// "unexpected base block: expected 0xabc..., but got 12345#0xdef..."
/// we would like to have this manually parsed till
/// https://linear.app/sqd-ai/issue/NET-248/correctly-propagate-all-errors-from-the-query-engine
/// is released to all workers
fn parse_base_block_mismatch(s: &str) -> Option<BlockRef> {
    let after_got = s
        .strip_prefix("unexpected base block: expected ")?
        .split(", but got ")
        .nth(1)?;
    let (number_str, hash) = after_got.split_once('#')?;
    let number = number_str.parse().ok()?;
    Some(BlockRef {
        number,
        hash: hash.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_base_block_mismatch_valid() {
        let msg = "unexpected base block: expected 0x8e1f85e345e0752737699a43a07713515b97287c21b30a240e91dbfbbf1006ab, but got 24799999#0x151e093f39962caed11a903e118d74712dbf7ee18e6107224f519831b5079af8";
        let result = parse_base_block_mismatch(msg).unwrap();
        assert_eq!(result.number, 24799999);
        assert_eq!(
            result.hash,
            "0x151e093f39962caed11a903e118d74712dbf7ee18e6107224f519831b5079af8"
        );
    }

    #[test]
    fn parse_base_block_mismatch_different_error() {
        assert!(parse_base_block_mismatch("some other server error").is_none());
    }

    #[test]
    fn parse_base_block_mismatch_malformed_no_hash() {
        let msg = "unexpected base block: expected 0xabc, but got 12345";
        assert!(parse_base_block_mismatch(msg).is_none());
    }

    #[test]
    fn parse_base_block_mismatch_malformed_no_number() {
        let msg = "unexpected base block: expected 0xabc, but got #0xdef";
        assert!(parse_base_block_mismatch(msg).is_none());
    }

    #[test]
    fn a_response_is_shared_when_that_does_not_pin_much_more_than_it_holds() {
        // Zero-copy is worth having only where the copy would cost something. A well-filled
        // buffer is adopted; a barely-filled one is copied, so a small payload cannot hold a
        // megabyte of read buffer open while it waits in a stream's queue.
        let mut full = Vec::with_capacity(1024);
        full.extend(std::iter::repeat_n(7u8, 1024));
        let full_ptr = full.as_ptr();

        let mut sparse = Vec::with_capacity(1024);
        sparse.extend_from_slice(&[7u8; 8]);
        let sparse_ptr = sparse.as_ptr();

        let shared = share_or_copy(full);
        let copied = share_or_copy(sparse);

        assert_eq!(shared.as_ptr(), full_ptr, "a full buffer should be adopted");
        assert_ne!(copied.as_ptr(), sparse_ptr, "a sparse one should be copied");
        assert_eq!(
            copied.as_ref(),
            &[7u8; 8],
            "the copy still carries the payload"
        );
    }

    #[test]
    fn stale_envelope_reroutes() {
        let verdict = Verdict::of(query_error::Err::BadRequest(STALE_ENVELOPE.to_owned()));
        assert!(matches!(verdict.error, QueryError::Retriable(_)));
        assert!(matches!(verdict.health, Health::Error));
        assert_eq!(verdict.label, "clock_skew");
        assert!(!verdict.backs_off);
    }

    #[test]
    fn other_bad_requests_stay_terminal() {
        let verdict = Verdict::of(query_error::Err::BadRequest(
            "invalid query signature".to_owned(),
        ));
        assert!(matches!(verdict.error, QueryError::BadRequest(_)));
        assert!(matches!(verdict.health, Health::Ok));
        assert_eq!(verdict.label, "bad_request");
    }

    #[test]
    fn server_errors_split_into_three_rows() {
        // The wire's catch-all class; only the message tells the three apart.
        let mismatch = Verdict::of(query_error::Err::ServerError(
            "unexpected base block: expected 0xabc, but got 42#0xdef".to_owned(),
        ));
        assert!(matches!(mismatch.error, QueryError::BaseBlockMismatch(_)));
        assert_eq!(mismatch.label, "block_mismatch");

        let too_large = Verdict::of(query_error::Err::ServerError(
            "Response too large".to_owned(),
        ));
        assert!(matches!(too_large.error, QueryError::BadRequest(_)));
        assert_eq!(too_large.label, "response_too_large");

        let other = Verdict::of(query_error::Err::ServerError("disk on fire".to_owned()));
        assert!(matches!(other.error, QueryError::Failure(_)));
        assert_eq!(other.label, "server_error");
        assert!(matches!(other.health, Health::Error));
    }
}
