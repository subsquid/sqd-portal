use std::collections::{BTreeMap, VecDeque};
use std::iter::zip;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use std::{collections::HashMap, sync::Arc, time::Duration};

use atomic_enum::atomic_enum;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures::{Stream, StreamExt};
use itertools::Itertools;
use num_rational::Ratio;
use num_traits::ToPrimitive;
use parking_lot::{Mutex, RwLock};
use semver::VersionReq;
use serde::Serialize;
use sqd_hotblocks::HotblocksServer;
use sqd_primitives::BlockRef;
use tokio::task::JoinError;
use tokio::time::MissedTickBehavior;
use tokio::time::{sleep, Instant};
use tokio_stream::wrappers::IntervalStream;
use tokio_util::sync::CancellationToken;

use sqd_contract_client::{Client as ContractClient, ClientError, Network, PeerId, Worker};
use sqd_messages::assignments::Assignment;
use sqd_messages::{query_error, query_result, Heartbeat, Query, QueryOk, RangeSet};
use sqd_network_transport::{
    get_agent_info, AgentInfo, GatewayConfig, GatewayEvent, GatewayTransport, Keypair,
    P2PTransportBuilder, QueryFailure, TransportArgs,
};

use super::contracts_state::{self, ContractsState};
use super::priorities::NoWorker;
use super::storage::DatasetIndex;
use super::{ChunkNotFound, NetworkState, StorageClient};
use crate::datasets::{DatasetConfig, Datasets};
use crate::types::api_types::WorkerDebugInfo;
use crate::types::{BlockNumber, BlockRange, ChunkId, DataChunk};
use crate::{
    config::Config,
    metrics,
    types::{generate_query_id, DatasetId, QueryError},
    utils::UseOnce,
};

const MAX_WAITING_PINGS: usize = 2000;

pub type QueryResult = Result<QueryOk, QueryError>;

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

#[atomic_enum]
#[derive(PartialEq)]
enum ReadinessState {
    FirstHeartbeatMissing = 0,
    Delaying = 1,
    Ready = 2,
}

/// Tracks the network state and handles p2p communication
pub struct NetworkClient {
    incoming_events: UseOnce<Box<dyn Stream<Item = GatewayEvent> + Send + Unpin + 'static>>,
    transport_handle: GatewayTransport,
    network_state: Mutex<NetworkState>,
    datasets: Arc<RwLock<Datasets>>,
    hotblocks: Option<Arc<HotblocksServer>>,
    contract_client: Box<dyn ContractClient>,
    dataset_storage: StorageClient,
    chain_update_interval: Duration,
    assignment_update_interval: Duration,
    local_peer_id: PeerId,
    keypair: Keypair,
    network_state_url: String,
    assignments: RwLock<BTreeMap<String, Arc<AssignedChunks>>>,
    assignments_stored: usize,
    heartbeat_buffer: Mutex<VecDeque<(PeerId, Heartbeat)>>,
    supported_versions: VersionReq,
    readiness: Arc<AtomicReadinessState>,
    contracts_state: RwLock<ContractsState>,
}

type AssignedChunks = HashMap<PeerId, Vec<ChunkId>>;

impl NetworkClient {
    pub async fn new(
        args: TransportArgs,
        config: Arc<Config>,
        datasets: Arc<RwLock<Datasets>>,
        hotblocks: Option<Arc<HotblocksServer>>,
    ) -> anyhow::Result<Arc<NetworkClient>> {
        let network = args.rpc.network;
        let dataset_storage = StorageClient::new(datasets.clone());
        let agent_into = get_agent_info!();
        let transport_builder = P2PTransportBuilder::from_cli(args, agent_into).await?;
        let contract_client = transport_builder.contract_client();
        let local_peer_id = transport_builder.local_peer_id();
        let keypair = transport_builder.keypair();

        let mut gateway_config = GatewayConfig::default();
        gateway_config.query_config.request_timeout = config.transport_timeout;
        gateway_config.query_config.max_concurrent_streams = None;
        gateway_config.events_queue_size = 10000;
        gateway_config.worker_status_via_gossipsub = config.worker_status_via_gossipsub;
        let (incoming_events, transport_handle) =
            transport_builder.build_gateway(gateway_config)?;

        let datasets_update_interval = config.datasets_update_interval;
        let datasets_copy = datasets.clone();

        let network_state_filename = match network {
            Network::Tethys => "network-state-tethys.json",
            Network::Mainnet => "network-state-mainnet.json",
        };
        let network_state_url =
            format!("https://metadata.sqd-datasets.io/{network_state_filename}");

        let state = NetworkState::new(config.clone(), datasets.clone());

        let this = Arc::new(NetworkClient {
            chain_update_interval: config.chain_update_interval,
            assignment_update_interval: config.assignments_update_interval,
            transport_handle,
            incoming_events: UseOnce::new(Box::new(incoming_events)),
            network_state: Mutex::new(state),
            datasets,
            hotblocks,
            contract_client,
            dataset_storage,
            local_peer_id,
            keypair,
            network_state_url,
            assignments: RwLock::default(),
            assignments_stored: config.assignments_stored,
            heartbeat_buffer: Mutex::default(),
            supported_versions: config.worker_versions.clone(),
            readiness: Arc::new(AtomicReadinessState::new(
                ReadinessState::FirstHeartbeatMissing,
            )),
            contracts_state: Default::default(),
        });

        this.reset_height_updates();

        let client_handle = this.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(datasets_update_interval).await;
                match Datasets::update(&datasets_copy, &config).await {
                    Ok(_) => {
                        client_handle.reset_height_updates();
                    }
                    Err(e) => {
                        tracing::warn!("Failed to update datasets mapping: {e:?}");
                    }
                }
            }
        });

        Ok(this)
    }

    pub async fn run(
        self: Arc<Self>,
        cancellation_token: CancellationToken,
    ) -> Result<(), JoinError> {
        let this = Arc::clone(&self);
        let token = cancellation_token.child_token();
        let events_fut = tokio::spawn(async move { this.run_event_stream(token).await });
        let this = Arc::clone(&self);
        let token = cancellation_token.child_token();
        let chain_updates_fut = tokio::spawn(async move { this.run_chain_updates(token).await });
        let this = Arc::clone(&self);
        let token = cancellation_token.child_token();
        let assignments_loop_fut =
            tokio::spawn(async move { this.run_assignments_loop(token).await });

        tokio::try_join!(events_fut, chain_updates_fut, assignments_loop_fut)?;
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
        let (
            epoch,
            sqd_locked,
            epoch_length,
            uses_default_strategy,
            active_workers,
            epoch_started,
            compute_units_per_epoch,
        ) = self
            .fetch_blockchain_state()
            .await
            .unwrap_or_else(|e| panic!("Couldn't get blockchain data: {e}"));

        let mut current_epoch: u32 = epoch;

        let operator = sqd_locked.clone().map(|s| s.0);
        tracing::info!(
            "Portal operator {}, current epoch: {}",
            operator.unwrap_or_else(|| "unknown".to_string()),
            current_epoch
        );

        self.contracts_state.write().set(
            current_epoch,
            sqd_locked,
            epoch_length,
            uses_default_strategy,
            &active_workers,
            epoch_started,
            compute_units_per_epoch,
        );

        let mut interval = tokio::time::interval_at(
            Instant::now() + self.chain_update_interval,
            self.chain_update_interval,
        );
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                () = cancellation_token.cancelled() => {
                    break;
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
                    tracing::warn!("Couldn't get current epoch: {e}");
                    continue;
                }
            };

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
                self.network_state.lock().reset_allocations();
            }
        }
    }

    async fn run_assignments_loop(&self, cancellation_token: CancellationToken) {
        let mut timer =
            tokio::time::interval_at(tokio::time::Instant::now(), self.assignment_update_interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        IntervalStream::new(timer)
            .take_until(cancellation_token.cancelled_owned())
            .for_each(|_| async move {
                tracing::debug!("Checking for new assignment");
                let latest_assignment = self
                    .assignments
                    .read()
                    .last_key_value()
                    .map(|(assignment_id, _)| assignment_id.clone());
                let assignment = match Assignment::try_download(
                    self.network_state_url.clone(),
                    latest_assignment,
                    Duration::from_secs(60),
                )
                .await
                {
                    Ok(Some(assignment)) => assignment,
                    Ok(None) => {
                        tracing::debug!("Assignment has not been changed");
                        return;
                    }
                    Err(err) => {
                        tracing::error!("Unable to get assignment: {err:?}");
                        return;
                    }
                };

                let assignment_id = assignment.id.clone();
                tracing::debug!("Got assignment {:?}", assignment_id);

                let (worker_chunks, datasets) = parse_assignment(assignment).unwrap();
                tracing::debug!("Assignment parsed");

                {
                    let mut local_assignments = self.assignments.write();
                    local_assignments.insert(assignment_id.clone(), Arc::new(worker_chunks));
                    if local_assignments.len() > self.assignments_stored {
                        local_assignments.pop_first();
                    }
                }

                self.dataset_storage.update_datasets(datasets);

                let heartbeats = {
                    let mut heartbeat_buffer = self.heartbeat_buffer.lock();
                    let (unknown, known) = heartbeat_buffer
                        .drain(..)
                        .partition(|(_, heartbeat)| heartbeat.assignment_id > assignment_id);
                    *heartbeat_buffer = unknown;
                    known
                };
                for (peer_id, heartbeat) in heartbeats {
                    tracing::trace!("Replaying heartbeat from {peer_id}");
                    self.handle_heartbeat(peer_id, heartbeat);
                }
                tracing::info!("New assignment saved");
            })
            .await;
        tracing::info!("Assignment processing task finished");
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
            }
        }
    }

    pub fn reset_height_updates(&self) {
        let Some(hotblocks) = self.hotblocks.as_ref() else {
            return;
        };
        let mut state = self.network_state.lock();
        let datasets = self.datasets.read();
        state.unsubscribe_all_height_updates();
        for dataset in datasets.iter() {
            if let (Some(dataset_id), Some(_)) = (&dataset.network_id, &dataset.hotblocks) {
                let hotblocks = hotblocks.clone();
                let name = dataset.default_name.parse().expect("Invalid dataset name");
                tracing::info!(
                    "Hotblocks storage for '{name}' set to track dataset height of '{dataset_id}'"
                );
                state.subscribe_height_updates(
                    dataset_id,
                    Box::new(move |height| {
                        tracing::info!(
                            "Cleaning hotblocks storage for '{name}' up to block {height}"
                        );
                        hotblocks.retain(
                            name,
                            sqd_hotblocks::RetentionStrategy::FromBlock(height + 1),
                        );
                    }),
                );
            }
        }
    }

    pub fn dataset(&self, alias: &str) -> Option<DatasetConfig> {
        self.datasets.read().get(alias).cloned()
    }

    pub fn datasets(&self) -> &RwLock<Datasets> {
        &self.datasets
    }

    pub fn first_existing_block(&self, dataset: &DatasetId) -> Option<BlockNumber> {
        self.dataset_storage.first_block(dataset)
    }

    pub fn find_chunk(&self, dataset: &DatasetId, block: u64) -> Result<DataChunk, ChunkNotFound> {
        self.dataset_storage.find_chunk(dataset, block)
    }

    pub fn next_chunk(&self, dataset: &DatasetId, chunk: &DataChunk) -> Option<DataChunk> {
        self.dataset_storage.next_chunk(dataset, chunk)
    }

    pub fn head(&self, dataset: &DatasetId) -> Option<BlockRef> {
        self.dataset_storage.head(dataset)
    }

    pub fn find_worker(
        &self,
        dataset: &DatasetId,
        block: u64,
        lease: bool,
    ) -> Result<PeerId, NoWorker> {
        let mut state = self.network_state.lock();
        let worker = state.find_worker(dataset, block);
        if lease {
            if let Ok(worker) = worker.as_ref() {
                state.lease_worker(*worker);
            };
        }
        worker
    }

    pub fn get_workers(&self, dataset: &DatasetId, block: u64) -> Vec<WorkerDebugInfo> {
        let state = self.network_state.lock();
        state.get_workers(dataset, block)
    }

    pub fn get_all_workers(&self) -> Vec<WorkerDebugInfo> {
        let state = self.network_state.lock();
        state.get_all_workers()
    }

    pub fn get_height(&self, dataset: &DatasetId) -> Option<u64> {
        self.network_state.lock().get_height(dataset)
    }

    pub async fn query_worker(
        self: Arc<Self>,
        worker: PeerId,
        chunk_id: ChunkId,
        block_range: BlockRange,
        query: String,
        lease: bool,
    ) -> QueryResult {
        let query_id = generate_query_id();
        tracing::trace!("Sending query {query_id} to {worker}");

        if lease {
            self.network_state.lock().lease_worker(worker);
        }

        let mut query = Query {
            dataset: chunk_id.dataset.to_url().to_owned(),
            query_id: query_id.clone(),
            query,
            block_range: Some(sqd_messages::Range {
                begin: *block_range.start(),
                end: *block_range.end(),
            }),
            chunk_id: chunk_id.chunk.to_string(),
            timestamp_ms: timestamp_now_ms(),
            signature: Default::default(),
        };
        query
            .sign(&self.keypair, worker)
            .expect("Query should be valid to sign");

        metrics::QUERIES_RUNNING.inc();
        metrics::QUERIES_SENT
            .get_or_create(&vec![("worker".to_string(), worker.to_string())])
            .inc();

        let this = self.clone();
        let guard = scopeguard::guard(worker, |worker: PeerId| {
            // The result is no longer needed. Either another query has got the result first,
            // or the stream has been dropped. In either case, consider the query outrun.
            this.network_state.lock().report_query_outrun(worker);
            metrics::QUERIES_RUNNING.dec();
        });

        let result = self.transport_handle.send_query(worker, query).await;
        scopeguard::ScopeGuard::into_inner(guard);
        metrics::QUERIES_RUNNING.dec();

        self.parse_query_result(worker, result)
    }

    fn parse_query_result(
        &self,
        peer_id: PeerId,
        result: Result<sqd_messages::QueryResult, QueryFailure>,
    ) -> QueryResult {
        use query_error::Err;

        match result {
            Ok(q) if !q.verify_signature(peer_id) => {
                metrics::report_query_result(peer_id, "validation_error");
                self.network_state.lock().report_query_failure(peer_id);
                Err(QueryError::Retriable(format!("invalid worker signature from {peer_id}, result: {q:?}")))
            }
            Ok(sqd_messages::QueryResult {
                result: Some(result),
                retry_after_ms,
                ..
            }) => {
                if let Some(backoff) = retry_after_ms {
                    self.network_state
                        .lock()
                        .hint_backoff(peer_id, Duration::from_millis(backoff.into()));
                    metrics::report_backoff(peer_id);
                };
                match result {
                    query_result::Result::Ok(ok) => {
                        metrics::report_query_result(peer_id, "ok");
                        self.network_state.lock().report_query_success(peer_id);
                        Ok(ok)
                    }
                    query_result::Result::Err(sqd_messages::QueryError { err: Some(err) }) => {
                        match err {
                            Err::BadRequest(s) => {
                                metrics::report_query_result(peer_id, "bad_request");
                                self.network_state.lock().report_query_success(peer_id);
                                Err(QueryError::BadRequest(format!("couldn't parse request: {s}")))
                            }
                            Err::NotFound(s) => {
                                metrics::report_query_result(peer_id, "not_found");
                                self.network_state.lock().report_query_error(peer_id);
                                Err(QueryError::Retriable(s))
                            }
                            Err::ServerError(s) => {
                                metrics::report_query_result(peer_id, "server_error");
                                self.network_state.lock().report_query_error(peer_id);
                                Err(QueryError::Retriable(format!("internal error: {s}")))
                            }
                            Err::ServerOverloaded(()) => {
                                metrics::report_query_result(peer_id, "server_overloaded");
                                self.network_state.lock().report_query_error(peer_id);
                                Err(QueryError::Retriable("worker overloaded".to_owned()))
                            }
                            Err::TooManyRequests(()) => {
                                metrics::report_query_result(peer_id, "too_many_requests");
                                self.network_state.lock().report_query_success(peer_id);
                                Err(QueryError::Retriable("rate limit exceeded".to_owned()))
                            }
                        }
                    }
                    query_result::Result::Err(sqd_messages::QueryError { err: None }) => {
                        metrics::report_query_result(peer_id, "invalid");
                        self.network_state.lock().report_query_error(peer_id);
                        Err(QueryError::Retriable("unknown error message".to_string()))
                    }
                }
            }
            Ok(sqd_messages::QueryResult { result: None, .. }) => {
                metrics::report_query_result(peer_id, "invalid");
                self.network_state.lock().report_query_error(peer_id);
                Err(QueryError::Retriable("unknown error message".to_string()))
            }
            Err(QueryFailure::InvalidRequest(e)) => {
                metrics::report_query_result(peer_id, "bad_request");
                Err(QueryError::BadRequest(format!("couldn't send request: {e}")))
            }
            Err(QueryFailure::InvalidResponse(e)) => {
                metrics::report_query_result(peer_id, "invalid");
                self.network_state.lock().report_query_error(peer_id);
                Err(QueryError::Retriable(format!("couldn't decode response: {e}")))
            }
            Err(QueryFailure::Timeout(t)) => {
                metrics::report_query_result(peer_id, "timeout");
                self.network_state.lock().report_query_failure(peer_id);
                let msg = match t {
                    sqd_network_transport::StreamClientTimeout::Connect => "timed out connecting to the peer",
                    sqd_network_transport::StreamClientTimeout::Request => "timed out reading response"
                };
                Err(QueryError::Retriable(msg.to_owned()))
            }
            Err(QueryFailure::TransportError(e)) => {
                metrics::report_query_result(peer_id, "transport_error");
                self.network_state.lock().report_query_failure(peer_id);
                Err(QueryError::Retriable(format!("transport error: {e}")))
            }
        }
    }

    fn handle_heartbeat(&self, peer_id: PeerId, heartbeat: Heartbeat) {
        if !heartbeat.version_matches(&self.supported_versions) {
            metrics::IGNORED_PINGS.inc();
            return;
        }
        metrics::VALID_PINGS.inc();
        tracing::trace!(
            "Heartbeat from {peer_id}, assignment: {:?}",
            heartbeat.assignment_id
        );

        self.wait_readiness();

        let assignments = self.assignments.read();
        let Some(assignment) = assignments.get(&heartbeat.assignment_id).cloned() else {
            tracing::trace!("Assignment {:?} not found", heartbeat.assignment_id);
            let latest_assignment_id = assignments
                .last_key_value()
                .map(|(assignment_id, _)| assignment_id.clone())
                .unwrap_or_default();
            if heartbeat.assignment_id > latest_assignment_id {
                tracing::debug!("Putting heartbeat into waitlist for {peer_id}");
                let Some(mut heartbeat_buffer) = self.heartbeat_buffer.try_lock() else {
                    tracing::debug!("Dropping heartbeat because the buffer is locked");
                    return;
                };
                heartbeat_buffer.push_back((peer_id, heartbeat));
                if heartbeat_buffer.len() > MAX_WAITING_PINGS {
                    heartbeat_buffer.pop_front();
                }
            } else {
                tracing::debug!("Dropping heartbeat from {peer_id}");
            }
            return;
        };
        drop(assignments);

        let worker_state = match parse_heartbeat(peer_id, &heartbeat, &assignment) {
            Ok(state) => state,
            Err(e) => {
                tracing::warn!("Couldn't parse heartbeat from {peer_id}: {e}");
                return;
            }
        };

        // TODO: consider uniting NetworkState with StorageClient
        self.network_state
            .lock()
            .update_dataset_states(peer_id, worker_state);
    }

    pub fn dataset_state(&self, dataset_id: &DatasetId) -> anyhow::Result<serde_json::Value> {
        Ok(serde_json::to_value(
            self.network_state.lock().dataset_state(dataset_id),
        )?)
    }

    pub fn get_peer_id(&self) -> PeerId {
        self.local_peer_id
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

    fn wait_readiness(&self) {
        if self
            .readiness
            .compare_exchange(
                ReadinessState::FirstHeartbeatMissing,
                ReadinessState::Delaying,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return;
        }

        tracing::info!(
            "Got the first worker heartbeat, waiting 60 seconds for portal readiness..."
        );

        let readiness = Arc::clone(&self.readiness);
        tokio::spawn(async move {
            sleep(Duration::from_secs(60)).await;

            readiness.store(ReadinessState::Ready, Ordering::SeqCst);
            tracing::info!("Portal is ready to serve traffic");
        });
    }

    pub fn is_ready(&self) -> bool {
        self.readiness.load(Ordering::SeqCst) == ReadinessState::Ready
    }
}

#[tracing::instrument(skip_all)]
fn parse_assignment(
    assignment: Assignment,
) -> anyhow::Result<(AssignedChunks, HashMap<DatasetId, DatasetIndex>)> {
    let peers = assignment.get_all_peer_ids();
    let mut worker_chunks = HashMap::new();
    for peer_id in peers {
        let mut peer_chunks = Vec::<ChunkId>::default();
        let Some(chunks) = assignment.dataset_chunks_for_peer_id(&peer_id) else {
            tracing::warn!("Couldn't get assigned chunks for {peer_id}");
            continue;
        };
        for dataset in chunks {
            let dataset_id = DatasetId::from_url(&dataset.id);
            for chunk in dataset.chunks {
                let chunk = chunk.id.parse().unwrap();
                peer_chunks.push(ChunkId::new(dataset_id.clone(), chunk));
            }
        }

        worker_chunks.insert(peer_id, peer_chunks);
    }
    let datasets = assignment
        .datasets
        .into_iter()
        .map(|dataset| {
            let mut parsed_chunks = dataset
                .chunks
                .into_iter()
                .flat_map(|chunk| {
                    chunk
                        .id
                        .parse::<DataChunk>()
                        .map_err(|e| {
                            tracing::warn!("Couldn't parse chunk id '{}': {e}", chunk.id);
                        })
                        .map(|id| (id, chunk.summary))
                })
                .collect_vec();
            parsed_chunks.sort_by_key(|(chunk, _)| chunk.first_block);
            let summary = parsed_chunks
                .last_mut()
                .and_then(|(_, summary)| summary.take());
            (
                DatasetId::from_url(dataset.id),
                DatasetIndex {
                    chunks: parsed_chunks.into_iter().map(|(chunk, _)| chunk).collect(),
                    summary,
                },
            )
        })
        .collect();

    Ok((worker_chunks, datasets))
}

fn parse_heartbeat(
    peer_id: PeerId,
    heartbeat: &Heartbeat,
    assignment: &AssignedChunks,
) -> anyhow::Result<HashMap<DatasetId, RangeSet>> {
    let Some(chunk_list) = assignment.get(&peer_id) else {
        anyhow::bail!(
            "PeerID {:?} not found in the assignment {:?}",
            peer_id,
            heartbeat.assignment_id
        );
    };
    let Some(missing_chunks) = heartbeat.missing_chunks.as_ref() else {
        anyhow::bail!(
            "PeerID {:?}: missing_chunks are missing in heartbeat",
            peer_id
        );
    };

    let unavailability_map = missing_chunks.to_bytes();
    anyhow::ensure!(
        unavailability_map.len() == chunk_list.len(),
        "Heartbeat of {:?} and assignment {:?} are inconsistent",
        peer_id,
        heartbeat.assignment_id
    );

    let worker_state = zip(unavailability_map, chunk_list)
        .filter_map(|(is_missing, val)| {
            if is_missing == 0 {
                Some(val.clone())
            } else {
                None
            }
        })
        .group_by(|chunk_id| chunk_id.dataset.clone())
        .into_iter()
        .map(|(dataset_id, vals)| {
            (
                dataset_id.clone(),
                vals.map(|chunk_id| chunk_id.chunk.range_msg())
                    .collect_vec()
                    .into(), // unites adjacent ranges
            )
        })
        .collect::<HashMap<_, _>>();
    Ok(worker_state)
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
