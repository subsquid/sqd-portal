use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::anyhow;
use sqd_assignments::{Assignment, NetworkAssignment};
use sqd_contract_client::{Network, PeerId};
use sqd_primitives::BlockRef;
use tracing::instrument;

use crate::{
    datasets::Datasets,
    metrics::{self, AssignmentRefresh},
    types::{api_types::DatasetState, BlockNumber, DataChunk, DatasetId},
    utils::RwLock,
};

pub struct StorageClient {
    assignment: RwLock<Option<Assignment>>,
    datasets_config: Arc<RwLock<Datasets>>,
    applied: RwLock<Option<Applied>>,
    network_state_url: String,
    reqwest_client: reqwest::Client,
    ignore_deprecated_workers: bool,
}

/// Applied-artifact provenance: `effective_from` orders, the id only
/// deduplicates (DEF-4, OB-6).
#[derive(Clone)]
struct Applied {
    id: String,
    effective_from: u64,
}

/// Why a refresh cycle produced no new applied artifact.
#[derive(thiserror::Error, Debug)]
enum RefreshError {
    #[error("{0:#}")]
    Fetch(#[from] anyhow::Error),
    /// Distinct from a fetch failure: a publisher serving garbage is a different
    /// alarm from an unreachable one (REQ-26, FM-2).
    #[error("assignment \"{id}\" failed verification: {reason}")]
    Invalid { id: String, reason: String },
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum ChunkNotFound {
    #[error("Unknown dataset")]
    UnknownDataset,
    #[error("Block is before the first block which is {first_block}")]
    BeforeFirst { first_block: BlockNumber },
    #[error("Block is after the last block")]
    AfterLast,
    #[error("Invalid chunk ID: {0}")]
    InvalidID(String),
}

impl StorageClient {
    pub fn new(
        datasets_config: Arc<RwLock<Datasets>>,
        network: Network,
        assignments_url: &str,
    ) -> Self {
        let network_state_filename = match network {
            Network::Tethys => "network-state-tethys.json",
            Network::Mainnet => "network-state-mainnet.json",
        };
        let network_state_url = format!("{assignments_url}/{network_state_filename}");
        Self {
            assignment: RwLock::new(None, "StorageClient::assignment"),
            datasets_config,
            applied: RwLock::new(None, "StorageClient::applied"),
            network_state_url,
            reqwest_client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .read_timeout(Duration::from_secs(5))
                .user_agent(format!("SQD Portal/{}", env!("CARGO_PKG_VERSION")))
                .build()
                .unwrap(),
            ignore_deprecated_workers: false,
        }
    }

    pub fn ignore_deprecated_workers(&mut self) {
        self.ignore_deprecated_workers = true;
    }

    pub fn num_workers(&self) -> usize {
        self.assignment
            .read()
            .as_ref()
            .map_or(0, |a| a.workers().len())
    }

    pub async fn try_update_assignment(&self) {
        let outcome = match self.update_assignment().await {
            Ok(outcome) => outcome,
            Err(err @ RefreshError::Invalid { .. }) => {
                // Not "waiting for the next one": the publisher answered, with
                // something unusable.
                tracing::error!(error = %err, "Rejected assignment, keeping the applied one");
                AssignmentRefresh::Invalid
            }
            Err(err) => {
                tracing::error!(error = %err, "Failed to update assignment, waiting for the next one");
                AssignmentRefresh::FetchFailed
            }
        };
        metrics::report_assignment_refresh(outcome);
    }

    #[instrument(skip_all)]
    async fn update_assignment(&self) -> Result<AssignmentRefresh, RefreshError> {
        tracing::debug!("Checking for new assignment");
        let network_state = self.fetch_network_state().await?;
        let visible_assignment = visible_assignment(&network_state);
        let assignment_id = visible_assignment.id.clone();
        let assignment_url = visible_assignment
            .fb_url_v1
            .clone()
            .ok_or(anyhow!("Missing assignment URL"))?;
        let effective_from = visible_assignment.effective_from;

        let applied = self.applied.read().clone();
        if let Some(applied) = &applied {
            if applied.id == assignment_id {
                tracing::debug!("Assignment has not been changed");
                return Ok(AssignmentRefresh::Unchanged);
            }
            // Before the download: no reason to pay for a rejected blob.
            // Ordering is effective-from, not arrival (DEF-4, INV-2).
            if effective_from < applied.effective_from {
                tracing::warn!(
                    "Skipping assignment \"{}\": effective from {}, older than the applied \"{}\" at {}",
                    assignment_id,
                    effective_from,
                    applied.id,
                    applied.effective_from,
                );
                return Ok(AssignmentRefresh::Regressive);
            }
        }

        let assignment = self
            .fetch_assignment(&assignment_url, &assignment_id)
            .await?;

        if applied.is_some() {
            sleep_until(effective_from).await;
        }

        self.set_assignment(assignment, &assignment_id, effective_from);

        tracing::info!("Applied assignment \"{}\"", assignment_id);
        Ok(AssignmentRefresh::Applied)
    }

    async fn fetch_network_state(&self) -> anyhow::Result<sqd_assignments::NetworkState> {
        let response = self
            .reqwest_client
            .get(&self.network_state_url)
            .send()
            .await?
            .error_for_status()?;
        let network_state = response.json().await?;
        Ok(network_state)
    }

    #[instrument(skip_all)]
    async fn fetch_assignment(&self, url: &str, id: &str) -> Result<Assignment, RefreshError> {
        let buf = self.download_assignment(url).await?;

        // Not `from_owned_unchecked`: this is remote input, and an unchecked read
        // of a corrupt buffer panics here (REQ-26, INV-36, GAP-1).
        Assignment::from_owned(buf).map_err(|e| RefreshError::Invalid {
            id: id.to_owned(),
            reason: e.to_string(),
        })
    }

    async fn download_assignment(&self, url: &str) -> anyhow::Result<Vec<u8>> {
        use async_compression::tokio::bufread::GzipDecoder;
        use futures::TryStreamExt;
        use tokio::io::AsyncReadExt;
        use tokio_util::io::StreamReader;

        let response = self
            .reqwest_client
            .get(url)
            .send()
            .await?
            .error_for_status()?;
        let stream = response.bytes_stream();
        let reader = StreamReader::new(stream.map_err(std::io::Error::other));
        let mut buf = Vec::new();
        let mut decoder = GzipDecoder::new(reader);
        decoder
            .read_to_end(&mut buf)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to decompress assignment: {}", e))?;

        tracing::debug!("Downloaded assignment from {}", url);

        Ok(buf)
    }

    #[instrument(skip_all)]
    fn set_assignment(&self, assignment: Assignment, id: &str, effective_from: u64) {
        *self.applied.write() = Some(Applied {
            id: id.to_owned(),
            effective_from,
        });

        let prev = self.assignment.read();
        for dataset in assignment.datasets().iter() {
            let dataset_id = DatasetId::from_url(dataset.id());
            let new_len = dataset.chunks().len();
            let last_block = dataset.last_block();
            let prev_ds = prev.as_ref().and_then(|p| p.get_dataset(dataset.id()));
            let old_len = prev_ds.map_or(0, |p| p.chunks().len());
            if old_len < new_len {
                tracing::info!(
                    "Got {} new chunk(s) for dataset {}",
                    new_len - old_len,
                    dataset_id,
                );
            }

            let dataset_name = self
                .datasets_config
                .read()
                .default_name(&dataset_id)
                .map(ToOwned::to_owned);
            metrics::report_chunk_list_updated(&dataset_id, dataset_name, new_len, last_block);
        }
        drop(prev);

        crate::metrics::KNOWN_WORKERS.set(assignment.workers().len() as i64);

        *self.assignment.write() = Some(assignment);
    }

    pub fn find_chunk(&self, dataset: &DatasetId, block: u64) -> Result<DataChunk, ChunkNotFound> {
        let dataset = dataset.to_url();
        let guard = self.assignment.read();
        let assignment = guard.as_ref().ok_or(ChunkNotFound::UnknownDataset)?;
        let chunk = assignment
            .find_chunk(dataset, block)
            .map_err(|e| convert_chunk_not_found(e, assignment, dataset))?;
        chunk.id().parse().map_err(|e| {
            tracing::warn!(error = %e, "Failed to parse chunk ID");
            ChunkNotFound::InvalidID(chunk.id().to_string())
        })
    }

    pub fn find_chunk_by_timestamp(
        &self,
        dataset: &DatasetId,
        ts: u64,
    ) -> Result<DataChunk, ChunkNotFound> {
        let dataset = dataset.to_url();
        let guard = self.assignment.read();
        let assignment = guard.as_ref().ok_or(ChunkNotFound::UnknownDataset)?;
        let chunk = assignment
            .find_chunk_by_timestamp(dataset, ts)
            .map_err(|e| convert_chunk_not_found(e, assignment, dataset))?;
        chunk.id().parse().map_err(|e| {
            tracing::warn!(error = %e, "Failed to parse chunk ID");
            ChunkNotFound::InvalidID(chunk.id().to_string())
        })
    }

    pub fn find_workers(
        &self,
        dataset: &DatasetId,
        block: u64,
    ) -> Result<Vec<PeerId>, ChunkNotFound> {
        let dataset = dataset.to_url();
        let guard = self.assignment.read();
        let assignment = guard.as_ref().ok_or(ChunkNotFound::UnknownDataset)?;
        let chunk = assignment
            .find_chunk(dataset, block)
            .map_err(|e| convert_chunk_not_found(e, assignment, dataset))?;
        Ok(chunk
            .worker_indexes()
            .iter()
            .filter_map(|idx| {
                let w = assignment.get_worker_by_index(idx);
                if w.status() == sqd_assignments::WorkerStatus::UnsupportedVersion {
                    return None;
                }
                if self.ignore_deprecated_workers
                    && w.status() == sqd_assignments::WorkerStatus::DeprecatedVersion
                {
                    return None;
                }
                w.peer_id()
                    .inspect_err(
                        |e| tracing::warn!(error = %e, "Failed to parse worker ID #{}", idx),
                    )
                    .ok()
            })
            .collect())
    }

    pub fn next_chunk(&self, dataset: &DatasetId, chunk: &DataChunk) -> Option<DataChunk> {
        self.find_chunk(dataset, chunk.last_block + 1).ok()
    }

    pub fn first_block(&self, dataset: &DatasetId) -> Option<BlockNumber> {
        Some(
            self.assignment
                .read()
                .as_ref()?
                .get_dataset(dataset.to_url())?
                .first_block(),
        )
    }

    pub fn head(&self, dataset: &DatasetId) -> Option<BlockRef> {
        let guard = self.assignment.read();
        let dataset = guard.as_ref()?.get_dataset(dataset.to_url())?;
        dataset.last_block_hash().map(|hash| BlockRef {
            hash: hash.to_owned(),
            number: dataset.last_block(),
        })
    }

    pub fn get_all_workers(&self) -> Vec<PeerId> {
        let guard = self.assignment.read();
        guard
            .as_ref()
            .map(|a| {
                a.workers()
                    .iter()
                    .filter_map(|w| (*w.worker_id()).try_into().ok())
                    .collect()
            })
            .unwrap_or_default()
    }

    #[instrument(skip(self))]
    pub fn get_dataset_state(&self, dataset: &DatasetId) -> Option<DatasetState> {
        let mut ranges: HashMap<_, Vec<_>> = HashMap::new();
        let guard = self.assignment.read();
        let assignment = guard.as_ref()?;
        for c in assignment.get_dataset(dataset.to_url())?.chunks().iter() {
            let range = c.id().parse::<DataChunk>().unwrap().range_msg();
            for idx in c.worker_indexes() {
                let peer_id = match assignment.get_worker_id(idx) {
                    Ok(peer_id) => peer_id,
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to parse worker ID #{}", idx);
                        continue;
                    }
                };
                ranges.entry(peer_id).or_default().push(range)
            }
        }
        Some(DatasetState {
            worker_ranges: ranges
                .into_iter()
                .map(|(peer_id, ranges)| (peer_id, sqd_messages::RangeSet::from(ranges)))
                .collect(),
        })
    }
}

/// Selects the assignment portals should use for routing.
///
/// In the first NET-683 phase, `mvcc-chunks` portals prefer the dedicated
/// portal assignment but temporarily fall back to the legacy assignment so
/// rollouts can tolerate schedulers that have not started publishing
/// `portal_assignment` yet.
fn visible_assignment(network_state: &sqd_assignments::NetworkState) -> &NetworkAssignment {
    #[cfg(feature = "mvcc-chunks")]
    {
        // First NET-683 step only splits portal discovery/publication.
        // Until MVCC visibility logic lands, fall back to the legacy assignment.
        match network_state.portal_assignment.as_ref() {
            Some(assignment) => assignment,
            None => {
                tracing::warn!(
                    "portal_assignment missing in network state; falling back to legacy assignment"
                );
                &network_state.assignment
            }
        }
    }

    #[cfg(not(feature = "mvcc-chunks"))]
    {
        &network_state.assignment
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(deprecated)]
    fn assignment(id: &str) -> NetworkAssignment {
        NetworkAssignment {
            url: None,
            fb_url: None,
            fb_url_v1: Some(format!("https://example.test/{id}.fb.gz")),
            id: id.to_string(),
            effective_from: 123,
        }
    }

    fn network_state() -> sqd_assignments::NetworkState {
        sqd_assignments::NetworkState {
            network: "testnet".to_string(),
            assignment: assignment("legacy"),
            #[cfg(feature = "mvcc-chunks")]
            worker_assignment: None,
            #[cfg(feature = "mvcc-chunks")]
            portal_assignment: None,
        }
    }

    #[test]
    fn visible_assignment_uses_legacy_assignment() {
        let state = network_state();

        assert_eq!(visible_assignment(&state).id, "legacy");
    }

    #[cfg(feature = "mvcc-chunks")]
    #[test]
    fn visible_assignment_prefers_portal_assignment() {
        let mut state = network_state();
        state.portal_assignment = Some(assignment("portal"));

        assert_eq!(visible_assignment(&state).id, "portal");
    }
}

/// The CT-2 injectors of GAP-1/GAP-20, in-crate. These drive `update_assignment`
/// rather than `try_update_assignment`, to assert the typed outcome and leave the
/// process-global refresh metrics alone.
#[cfg(test)]
mod refresh_tests {
    use std::{
        io::Write,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Mutex,
        },
    };

    use axum::{extract::State, routing::get, Router};
    use flate2::{write::GzEncoder, Compression};

    use super::*;

    const DATASET: &str = "s3://ethereum-mainnet";
    /// Any well-formed peer id.
    const WORKER: &str = "12D3KooWQZQm7z8vTeUZmCkFwsPPKKzHUDDPnzLPPCEBqrnDZDRs";
    /// Must stay in the past, or `sleep_until` stalls the tests.
    const EFFECTIVE_FROM: u64 = 1_700_000_000;

    struct Publisher {
        state: Arc<Mutex<Served>>,
        artifact_hits: Arc<AtomicUsize>,
        url: String,
    }

    struct Served {
        id: String,
        effective_from: u64,
        artifact_gz: Vec<u8>,
    }

    #[derive(Clone)]
    struct AppState {
        served: Arc<Mutex<Served>>,
        artifact_hits: Arc<AtomicUsize>,
        port: u16,
    }

    impl Publisher {
        async fn start(id: &str, artifact_gz: Vec<u8>) -> Self {
            let served = Arc::new(Mutex::new(Served {
                id: id.to_owned(),
                effective_from: EFFECTIVE_FROM,
                artifact_gz,
            }));
            let artifact_hits = Arc::new(AtomicUsize::new(0));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();

            let app = Router::new()
                .route(
                    "/network-state-tethys.json",
                    get(|State(s): State<AppState>| async move {
                        let served = s.served.lock().unwrap();
                        axum::Json(serde_json::json!({
                            "network": "tethys",
                            "assignment": {
                                "id": served.id,
                                "effective_from": served.effective_from,
                                "fb_url_v1": format!("http://127.0.0.1:{}/assignment.fb.gz", s.port),
                            }
                        }))
                    }),
                )
                .route(
                    "/assignment.fb.gz",
                    get(|State(s): State<AppState>| async move {
                        s.artifact_hits.fetch_add(1, Ordering::SeqCst);
                        s.served.lock().unwrap().artifact_gz.clone()
                    }),
                )
                .with_state(AppState {
                    served: served.clone(),
                    artifact_hits: artifact_hits.clone(),
                    port,
                });

            tokio::spawn(async move { axum::serve(listener, app).await });

            Self {
                state: served,
                artifact_hits,
                url: format!("http://127.0.0.1:{port}"),
            }
        }

        fn publish(&self, id: &str, effective_from: u64, artifact_gz: Vec<u8>) {
            let mut served = self.state.lock().unwrap();
            served.id = id.to_owned();
            served.effective_from = effective_from;
            served.artifact_gz = artifact_gz;
        }

        fn artifact_hits(&self) -> usize {
            self.artifact_hits.load(Ordering::SeqCst)
        }

        fn client(&self) -> StorageClient {
            StorageClient::new(
                Arc::new(RwLock::new(Datasets::empty(), "datasets")),
                Network::Tethys,
                &self.url,
            )
        }
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(bytes).unwrap();
        enc.finish().unwrap()
    }

    /// The smallest artifact the reader accepts.
    fn valid_artifact() -> Vec<u8> {
        let mut b = sqd_assignments::AssignmentBuilder::new("test-secret");
        b.new_chunk()
            .id("0000000000/0000000000-0000000999-abcdef12")
            .dataset_id(DATASET)
            .dataset_base_url("https://ethereum.example.invalid")
            .block_range(0..=999)
            .size(1_000)
            .worker_indexes(&[0])
            .last_block_hash("0xabc")
            .last_block_timestamp(EFFECTIVE_FROM)
            .files(&["blocks.parquet".to_string()])
            .finish()
            .unwrap();
        b.finish_dataset();
        b.add_worker_with_timestamp(
            WORKER.parse().unwrap(),
            sqd_assignments::WorkerStatus::Ok,
            &[0],
            EFFECTIVE_FROM as usize,
        );
        gzip(&b.finish())
    }

    /// Valid gzip, garbage inside: the transport succeeds, so only verification
    /// stands between the portal and an unchecked FlatBuffer read.
    fn corrupt_artifact() -> Vec<u8> {
        gzip(&[0xff; 512])
    }

    fn dataset_id() -> DatasetId {
        DatasetId::from_url(DATASET)
    }

    #[tokio::test]
    async fn corrupt_artifact_is_rejected_rather_than_adopted() {
        let publisher = Publisher::start("corrupt", corrupt_artifact()).await;
        let client = publisher.client();

        let err = client.update_assignment().await.unwrap_err();

        assert!(
            matches!(err, RefreshError::Invalid { .. }),
            "expected a verification failure, got {err:?}",
        );
        assert!(client.applied.read().is_none(), "corrupt artifact applied");
        assert_eq!(client.num_workers(), 0);
    }

    #[tokio::test]
    async fn corrupt_artifact_keeps_the_applied_one() {
        let publisher = Publisher::start("good", valid_artifact()).await;
        let client = publisher.client();
        client.update_assignment().await.unwrap();

        publisher.publish("corrupt", EFFECTIVE_FROM + 100, corrupt_artifact());
        let err = client.update_assignment().await.unwrap_err();

        assert!(matches!(err, RefreshError::Invalid { .. }));
        assert_eq!(
            client.applied.read().as_ref().map(|a| a.id.clone()),
            Some("good".to_owned()),
            "a rejected artifact must not become the applied one",
        );
        assert_eq!(client.num_workers(), 1);
        assert!(
            client.find_chunk(&dataset_id(), 0).is_ok(),
            "routing must keep working off the previous artifact",
        );
    }

    #[tokio::test]
    async fn regressive_assignment_is_not_applied() {
        let publisher = Publisher::start("current", valid_artifact()).await;
        let client = publisher.client();
        client.update_assignment().await.unwrap();
        let downloads = publisher.artifact_hits();

        // Different identifier, earlier effective-from.
        publisher.publish("stale-republish", EFFECTIVE_FROM - 100, valid_artifact());
        let outcome = client.update_assignment().await.unwrap();

        assert!(matches!(outcome, AssignmentRefresh::Regressive));
        assert_eq!(
            client.applied.read().as_ref().map(|a| a.id.clone()),
            Some("current".to_owned()),
        );
        assert_eq!(
            publisher.artifact_hits(),
            downloads,
            "the guard should reject before paying for the download",
        );
    }

    #[tokio::test]
    async fn equal_effective_from_is_still_applied() {
        // Only strictly-earlier is a regression.
        let publisher = Publisher::start("first", valid_artifact()).await;
        let client = publisher.client();
        client.update_assignment().await.unwrap();

        publisher.publish("correction", EFFECTIVE_FROM, valid_artifact());
        let outcome = client.update_assignment().await.unwrap();

        assert!(matches!(outcome, AssignmentRefresh::Applied));
        assert_eq!(
            client.applied.read().as_ref().map(|a| a.id.clone()),
            Some("correction".to_owned()),
        );
    }

    #[tokio::test]
    async fn newer_assignment_is_applied() {
        let publisher = Publisher::start("first", valid_artifact()).await;
        let client = publisher.client();
        client.update_assignment().await.unwrap();

        publisher.publish("second", EFFECTIVE_FROM + 100, valid_artifact());
        let outcome = client.update_assignment().await.unwrap();

        assert!(matches!(outcome, AssignmentRefresh::Applied));
        assert_eq!(
            client.applied.read().as_ref().map(|a| a.id.clone()),
            Some("second".to_owned()),
        );
    }

    #[tokio::test]
    async fn unchanged_assignment_is_not_refetched() {
        let publisher = Publisher::start("only", valid_artifact()).await;
        let client = publisher.client();
        client.update_assignment().await.unwrap();
        let downloads = publisher.artifact_hits();

        let outcome = client.update_assignment().await.unwrap();

        assert!(matches!(outcome, AssignmentRefresh::Unchanged));
        assert_eq!(publisher.artifact_hits(), downloads);
    }
}

fn convert_chunk_not_found(
    e: sqd_assignments::ChunkNotFound,
    assignment: &Assignment,
    dataset: &str,
) -> ChunkNotFound {
    match e {
        sqd_assignments::ChunkNotFound::AfterLast => ChunkNotFound::AfterLast,
        sqd_assignments::ChunkNotFound::BeforeFirst => ChunkNotFound::BeforeFirst {
            first_block: assignment.get_dataset(dataset).unwrap().first_block(),
        },
        sqd_assignments::ChunkNotFound::UnknownDataset => ChunkNotFound::UnknownDataset,
    }
}

async fn sleep_until(timestamp: u64) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .expect("time should be after 1970");
    let until = Duration::from_secs(timestamp);
    if let Some(delta) = until.checked_sub(now) {
        tokio::time::sleep(delta).await;
    }
}
