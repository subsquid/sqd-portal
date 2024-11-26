use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures::{Stream, StreamExt};
use num_rational::Ratio;
use parking_lot::Mutex;
use serde::Serialize;
use sqd_contract_client::{Client as ContractClient, ClientError, PeerId, Worker};
use sqd_messages::{query_result, Ping, Query, QueryResult};
use sqd_network_transport::{
    get_agent_info, AgentInfo, GatewayConfig, GatewayEvent, GatewayTransportHandle,
    P2PTransportBuilder, QueueFull, TransportArgs,
};
use std::time::SystemTime;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{sync::oneshot, time::Instant};
use tokio_util::sync::CancellationToken;

use super::{NetworkState, StorageClient};
use crate::datasets::Datasets;
use crate::network::state::{DatasetState, Status};
use crate::{
    cli::Config,
    metrics,
    types::{generate_query_id, BlockRange, DatasetId, QueryId},
    utils::UseOnce,
};

lazy_static::lazy_static! {
    static ref SUPPORTED_VERSIONS: semver::VersionReq = "~1.2.0".parse().expect("Invalid version requirement");
}
const MAX_CONCURRENT_QUERIES: usize = 1000;

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
    pub status: Status,
    pub operator: Option<String>,
    pub current_epoch: Option<CurrentEpoch>,
    pub sqd_locked: Option<String>,
    pub cu_per_epoch: Option<String>,
    pub workers: Option<Workers>,
}

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
    result_tx: oneshot::Sender<query_result::Result>,
    worker_id: PeerId,
}

impl NetworkClient {
    pub async fn new(
        args: TransportArgs,
        logs_collector: PeerId,
        config: Arc<Config>,
        datasets: Arc<Datasets>,
    ) -> anyhow::Result<NetworkClient> {
        let dataset_storage = StorageClient::new(args.rpc.network)?;
        let agent_into = get_agent_info!();
        let transport_builder = P2PTransportBuilder::from_cli(args, agent_into).await?;
        let contract_client = transport_builder.contract_client();
        let local_peer_id = transport_builder.local_peer_id().clone();

        let mut gateway_config = GatewayConfig::new(logs_collector);
        gateway_config.query_config.request_timeout = config.transport_timeout;
        let (incoming_events, transport_handle) =
            transport_builder.build_gateway(gateway_config)?;

        Ok(NetworkClient {
            dataset_update_interval: config.dataset_update_interval,
            chain_update_interval: config.chain_update_interval,
            transport_handle,
            incoming_events: UseOnce::new(Box::new(incoming_events)),
            network_state: Mutex::new(NetworkState::new(config, datasets)),
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

        self.network_state.lock().set_contracts_state(
            current_epoch,
            sqd_locked,
            epoch_length,
            uses_default_strategy,
            active_workers,
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
                _ = cancellation_token.cancelled() => {
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

            self.network_state.lock().set_contracts_state(
                current_epoch,
                sqd_locked,
                epoch_length,
                uses_default_strategy,
                active_workers,
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
                            tracing::error!("Error handling query result: {e:?}");
                        });
                }
                GatewayEvent::QueryDropped { query_id } => {
                    if let Some(task) = self.tasks.lock().remove(&query_id) {
                        metrics::QUERIES_RUNNING.dec();
                        task.result_tx
                            .send(query_result::Result::ServerError(
                                "Outbound queue overloaded".to_string(),
                            ))
                            .ok();
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
                    dataset: Some(dataset.to_string()),
                    query_id: Some(query_id.clone()),
                    query: Some(query),
                    client_state_json: Some("{}".to_string()), // This is a placeholder field
                    profiling: Some(false),
                    ..Default::default()
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
        metrics::report_query_result(&result, peer_id.to_string());

        let mut tasks = self.tasks.lock();
        let (_query_id, task) = tasks
            .remove_entry(&query_id)
            .ok_or_else(|| anyhow::anyhow!("Not expecting response for query {query_id}"))?;
        metrics::QUERIES_RUNNING.set(tasks.len() as i64);
        drop(tasks);

        if peer_id != task.worker_id {
            self.network_state.lock().report_query_error(peer_id);
            anyhow::bail!(
                "Invalid message sender, expected {}, got {}",
                task.worker_id,
                peer_id
            );
        }

        match &result {
            query_result::Result::ServerError(_) => {
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
                    // or the stream has been dropped. In either case, consider the query outrun.
                    self.network_state.lock().report_query_outrun(peer_id);
                }
                return Ok(());
            }
        };

        task.result_tx.send(result).ok();

        Ok(())
    }

    pub fn dataset_state(&self, dataset_id: DatasetId) -> Option<DatasetState> {
        self.network_state.lock().dataset_state(dataset_id).cloned()
    }

    pub fn get_peer_id(&self) -> PeerId {
        self.local_peer_id
    }

    pub fn get_status(&self) -> NetworkClientStatus {
        let state = self.network_state.lock().get_contracts_state();

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
                sqd_locked: Some(state.sqd_locked.to_string()),
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
}
