use std::collections::{BTreeMap, LinkedList};
use std::iter::zip;
use std::{collections::HashMap, sync::Arc, time::Duration};

use futures::{Stream, StreamExt};
use itertools::Itertools;
//use parking_lot::lock_api::Mutex;
use anyhow::anyhow;
use parking_lot::Mutex;
use regex::Regex;
use sqd_contract_client::{Client as ContractClient, Network, PeerId};
use sqd_messages::assignments::Assignment;
use sqd_messages::{query_error, query_result, Heartbeat, Query, QueryOk, Range, RangeSet};
use sqd_network_transport::{
    get_agent_info, AgentInfo, GatewayConfig, GatewayEvent, GatewayTransportHandle,
    P2PTransportBuilder, QueryFailure, QueueFull, TransportArgs,
};
use tokio::time::MissedTickBehavior;
use tokio::{sync::oneshot, time::Instant};
use tokio_stream::wrappers::IntervalStream;
use tokio_util::sync::CancellationToken;

use super::priorities::NoWorker;
use super::{NetworkState, StorageClient};
use crate::network::state::DatasetState;
use crate::{
    cli::Config,
    metrics,
    types::{generate_query_id, BlockRange, DatasetId, QueryError, QueryId},
    utils::UseOnce,
};

lazy_static::lazy_static! {
    static ref SUPPORTED_VERSIONS: semver::VersionReq = "~1.2.0".parse().expect("Invalid version requirement");
}
const MAX_CONCURRENT_QUERIES: usize = 1000;
const MAX_ASSIGNMENT_BUFFER_SIZE: usize = 5;
const MAX_WAITING_PINGS: usize = 2000;

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
    assignment_check_interval: Duration,
    local_peer_id: PeerId,
    network_state_url: String,
    assignments: Mutex<BTreeMap<String, HashMap<String, Vec<(DatasetId, Range)>>>>,
    heartbeat_buffer: Mutex<LinkedList<(PeerId, Heartbeat)>>,
}

struct QueryTask {
    result_tx: oneshot::Sender<QueryResult>,
    worker_id: PeerId,
}

pub fn range_from_chunk_id(dirname: &str) -> Result<Range, anyhow::Error> {
    lazy_static::lazy_static! {
        static ref RE: Regex = Regex::new(r"(\d{10})/(\d{10})-(\d{10})-(\w{5,8})$").unwrap();
    }
    let (beg, end) = RE
        .captures(dirname)
        .and_then(|cap| match (cap.get(2), cap.get(3)) {
            (Some(beg), Some(end)) => Some((beg.as_str(), end.as_str())),
            _ => None,
        })
        .ok_or_else(|| anyhow!("Could not parse chunk dirname '{dirname}'"))?;
    Ok(Range {
        begin: beg.parse()?,
        end: end.parse()?,
    })
}

impl NetworkClient {
    pub async fn new(args: TransportArgs, config: Arc<Config>) -> anyhow::Result<NetworkClient> {
        let network = args.rpc.network;
        let dataset_storage = StorageClient::new(network)?;
        let agent_into = get_agent_info!();
        let transport_builder = P2PTransportBuilder::from_cli(args, agent_into).await?;
        let contract_client = transport_builder.contract_client();
        let local_peer_id = transport_builder.local_peer_id();

        let mut gateway_config = GatewayConfig::new();
        gateway_config.query_config.request_timeout = config.transport_timeout;
        let (incoming_events, transport_handle) =
            transport_builder.build_gateway(gateway_config)?;

        let network_state_filename = match network {
            Network::Tethys => "network-state-tethys.json",
            Network::Mainnet => "network-state-mainnet.json",
        };
        let network_state_url =
            format!("https://metadata.sqd-datasets.io/{network_state_filename}");

        Ok(NetworkClient {
            dataset_update_interval: config.dataset_update_interval,
            chain_update_interval: config.chain_update_interval,
            assignment_check_interval: Duration::new(60, 0),
            transport_handle,
            incoming_events: UseOnce::new(Box::new(incoming_events)),
            network_state: Mutex::new(NetworkState::new(config)),
            contract_client,
            tasks: Mutex::new(HashMap::new()),
            dataset_storage,
            local_peer_id,
            network_state_url,
            assignments: Mutex::new(Default::default()),
            heartbeat_buffer: Mutex::new(Default::default()),
        })
    }

    pub async fn run(&self, cancellation_token: CancellationToken) {
        // TODO: run coroutines in parallel
        tokio::join!(
            self.run_event_stream(cancellation_token.clone()),
            self.run_storage_updates(cancellation_token.clone()),
            self.run_chain_updates(cancellation_token.clone()),
            self.run_assignments_loop(cancellation_token),
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

    async fn run_assignments_loop(&self, cancellation_token: CancellationToken) {
        let mut timer =
            tokio::time::interval_at(tokio::time::Instant::now(), self.assignment_check_interval);

        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        IntervalStream::new(timer)
            .take_until(cancellation_token.cancelled_owned())
            .for_each(|_| async move {
                let latest_assignment = self
                    .assignments
                    .lock()
                    .last_key_value()
                    .map(|(assignament_id, _)| assignament_id.clone());
                let assignment = match Assignment::try_download(
                    self.network_state_url.clone(),
                    latest_assignment,
                )
                .await
                {
                    Ok(Some(assignment)) => assignment,
                    Ok(None) => {
                        tracing::info!("Assignment has not been changed");
                        return;
                    }
                    Err(err) => {
                        tracing::error!("Unable to get assignment: {err:?}");
                        return;
                    }
                };
                let assignment_id = assignment.id.clone();
                tracing::debug!("Got assignment {:?}", assignment_id);
                let peers = assignment.get_all_peer_ids();
                let mut condensed_assignment: HashMap<String, Vec<(DatasetId, Range)>> =
                    Default::default();
                for peer_id in peers {
                    let mut peer_chunks: Vec<(DatasetId, Range)> = Default::default();
                    let chunks = assignment.dataset_chunks_for_peer_id(&peer_id).unwrap();
                    for dataset in chunks {
                        for chunk in dataset.chunks {
                            let range = range_from_chunk_id(&chunk.id).unwrap();
                            let dataset_id = DatasetId::from_url(dataset.id.clone());
                            peer_chunks.push((dataset_id, range));
                        }
                    }

                    condensed_assignment.insert(peer_id, peer_chunks);
                }
                {
                    let mut local_assignments = self.assignments.lock();
                    local_assignments.insert(assignment.id, condensed_assignment);
                    if local_assignments.len() > MAX_ASSIGNMENT_BUFFER_SIZE {
                        local_assignments.pop_first();
                    }
                }
                let heartbeats;
                {
                    let mut heartbeat_buffer = self.heartbeat_buffer.lock();
                    heartbeats = heartbeat_buffer
                        .iter()
                        .filter(|(_, heartbeat)| heartbeat.assignment_id <= assignment_id)
                        .cloned()
                        .collect::<Vec<_>>();
                    *heartbeat_buffer = heartbeat_buffer
                        .iter()
                        .filter(|(_, heartbeat)| heartbeat.assignment_id > assignment_id)
                        .cloned()
                        .collect();
                }
                for (peer_id, heartbeat) in heartbeats {
                    tracing::debug!("Replaying heartbeat from {peer_id}");
                    self.handle_heartbeat(peer_id, heartbeat);
                }
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

    pub fn find_worker(&self, dataset: &DatasetId, block: u64) -> Result<PeerId, NoWorker> {
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
        tracing::debug!("Ping from {peer_id} {:?}", heartbeat.assignment_id);
        let local_assignments = self.assignments.lock();
        let Some(assignment) = local_assignments.get(&heartbeat.assignment_id) else {
            tracing::debug!("Assignment {:?} not found", heartbeat.assignment_id);
            let latest_assignmaent_id = local_assignments
                .last_key_value()
                .map(|(assignment_id, _)| assignment_id.clone())
                .unwrap_or_default();
            if heartbeat.assignment_id > latest_assignmaent_id {
                self.heartbeat_buffer.lock().push_back((peer_id, heartbeat));
                tracing::debug!("Putting heartbeat into waitlist for {peer_id}");
                {
                    let mut heartbeat_buffer = self.heartbeat_buffer.lock();
                    if heartbeat_buffer.len() > MAX_WAITING_PINGS {
                        heartbeat_buffer.pop_front();
                    }
                }
            } else {
                tracing::error!("Dropping heartbeat from {peer_id}");
            }
            return;
        };
        let Some(chunk_list) = assignment.get(&peer_id.to_string()) else {
            tracing::error!(
                "PeerID {:?} not found in Assignment {:?}",
                peer_id,
                heartbeat.assignment_id
            );
            return;
        };
        let Some(missing_chunks) = heartbeat.missing_chunks.as_ref() else {
            tracing::error!(
                "PeerID {:?}: missing_chunks are missing in heartbeat",
                peer_id
            );
            return;
        };
        let unavailability_map = missing_chunks.to_bytes();
        if unavailability_map.len() != chunk_list.len() {
            tracing::error!(
                "Heartbeat of {:?} and assignment {:?} are inconsistent",
                peer_id,
                heartbeat.assignment_id
            );
            return;
        }
        let worker_state = zip(unavailability_map, chunk_list)
            .filter_map(|(is_missing, val)| {
                if is_missing == 0 {
                    Some(val.clone())
                } else {
                    None
                }
            })
            .group_by(|(dataset, _)| dataset.clone())
            .into_iter()
            .map(|(group, vals)| {
                (
                    group,
                    RangeSet {
                        ranges: vals.map(|(_, range)| range).sorted().collect(),
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        self.network_state
            .lock()
            .update_dataset_states(peer_id, worker_state);
        metrics::VALID_PINGS.inc();
        //tracing::error!("Payload: {:?}", heartbeat.missing_chunks.as_ref().unwrap().to_bytes().iter().map(|v| *v as usize).sum::<usize>());
        // if !heartbeat.version_matches(&SUPPORTED_VERSIONS) {
        //     metrics::IGNORED_PINGS.inc();
        //     return;
        // }
        // metrics::VALID_PINGS.inc();
        // todo!();
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
            }) => {
                if let Some(backoff) = retry_after_ms {
                    self.network_state
                        .lock()
                        .hint_backoff(peer_id, Duration::from_millis(backoff as u64));
                };
                match result {
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
                    query_result::Result::Err(sqd_messages::QueryError { err: Some(err) }) => {
                        match err {
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
                                self.network_state.lock().report_query_error(peer_id);
                                Err(QueryError::Retriable("Server overloaded".to_owned()))
                            }
                            Err::TooManyRequests(()) => {
                                metrics::report_query_result(peer_id, "too_many_requests");
                                self.network_state.lock().report_query_success(peer_id);
                                Err(QueryError::Retriable("Too many requests".to_owned()))
                            }
                        }
                    }
                    query_result::Result::Err(sqd_messages::QueryError { err: None }) => {
                        metrics::report_query_result(peer_id, "invalid");
                        self.network_state.lock().report_query_error(peer_id);
                        anyhow::bail!("Unknown error message")
                    }
                }
            }
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
