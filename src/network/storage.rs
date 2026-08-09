use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::anyhow;
use sqd_assignments::{Assignment, NetworkAssignment};
use sqd_contract_client::{Network, PeerId};
use sqd_primitives::BlockRef;
use tracing::instrument;

use crate::{
    datasets::Datasets,
    metrics,
    types::{api_types::DatasetState, BlockNumber, DataChunk, DatasetId},
    utils::RwLock,
};

/// The assignment currently held by the client: either the legacy shared format, or (under
/// `mvcc-chunks`) the portal-oriented split format. See docs/assignment-wire-format.md in
/// network-scheduler for the split rationale.
enum ActiveAssignment {
    Legacy(Assignment),
    #[cfg(feature = "mvcc-chunks")]
    Portal(sqd_assignments::PortalAssignment),
}

/// The last applied assignment. Carries its source because `portal_assignment` and the legacy
/// `assignment` are independent sequences: ids only order within one of them.
#[derive(Clone)]
struct AppliedAssignment {
    id: String,
    is_portal_assignment: bool,
}

pub struct StorageClient {
    assignment: RwLock<Option<ActiveAssignment>>,
    datasets_config: Arc<RwLock<Datasets>>,
    latest_assignment: RwLock<Option<AppliedAssignment>>,
    network_state_url: String,
    reqwest_client: reqwest::Client,
    ignore_deprecated_workers: bool,
    prefer_portal_assignment: bool,
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
            latest_assignment: RwLock::new(None, "StorageClient::latest_assignment"),
            network_state_url,
            reqwest_client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .read_timeout(Duration::from_secs(5))
                .user_agent(format!("SQD Portal/{}", env!("CARGO_PKG_VERSION")))
                .build()
                .unwrap(),
            ignore_deprecated_workers: false,
            prefer_portal_assignment: true,
        }
    }

    pub fn ignore_deprecated_workers(&mut self) {
        self.ignore_deprecated_workers = true;
    }

    pub fn set_prefer_portal_assignment(&mut self, prefer: bool) {
        self.prefer_portal_assignment = prefer;
    }

    pub fn num_workers(&self) -> usize {
        match self.assignment.read().as_ref() {
            Some(ActiveAssignment::Legacy(a)) => a.workers().len(),
            #[cfg(feature = "mvcc-chunks")]
            Some(ActiveAssignment::Portal(a)) => a.workers().len(),
            None => 0,
        }
    }

    pub async fn try_update_assignment(&self) {
        match self.update_assignment().await {
            Ok(_) => {}
            Err(err) => {
                tracing::error!(error = ?err, "Failed to update assignment, waiting for the next one");
            }
        }
    }

    #[instrument(skip_all)]
    async fn update_assignment(&self) -> anyhow::Result<()> {
        tracing::debug!("Checking for new assignment");
        let network_state = self.fetch_network_state().await?;
        let (visible_assignment, is_portal_assignment) =
            visible_assignment(&network_state, self.prefer_portal_assignment);
        let assignment_id = visible_assignment.id.clone();
        let assignment_url = visible_assignment
            .fb_url_v1
            .clone()
            .ok_or(anyhow!("Missing assignment URL"))?;
        let effective_from = visible_assignment.effective_from;
        let latest = self.latest_assignment.read().clone();
        if latest.as_ref().is_some_and(|l| l.id == assignment_id) {
            tracing::debug!("Assignment has not been changed");
            return Ok(());
        }

        if let Some(latest) = &latest {
            if is_stale(latest, &assignment_id, is_portal_assignment) {
                // Applying it would move the head backwards, dropping already-advertised chunks
                // and opening a range no source covers. A later poll brings the newer one back.
                tracing::warn!(
                    stale_id = %assignment_id,
                    current_id = %latest.id,
                    "Rejected an assignment older than the current one"
                );
                metrics::STALE_ASSIGNMENTS_REJECTED.inc();
                return Ok(());
            }
        }

        let assignment = self
            .fetch_assignment(&assignment_url, is_portal_assignment)
            .await?;

        if latest.is_some() {
            sleep_until(effective_from).await;
        }

        self.set_assignment(assignment, &assignment_id, is_portal_assignment);

        tracing::info!("Applied assignment \"{}\"", assignment_id);
        Ok(())
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

    #[instrument(skip(self, url))]
    #[allow(unused_variables)]
    async fn fetch_assignment(
        &self,
        url: &str,
        is_portal_assignment: bool,
    ) -> anyhow::Result<ActiveAssignment> {
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

        #[cfg(feature = "mvcc-chunks")]
        if is_portal_assignment {
            return Ok(ActiveAssignment::Portal(
                sqd_assignments::PortalAssignment::from_owned_unchecked(buf),
            ));
        }

        Ok(ActiveAssignment::Legacy(Assignment::from_owned_unchecked(
            buf,
        )))
    }

    #[instrument(skip_all)]
    fn set_assignment(&self, assignment: ActiveAssignment, id: &str, is_portal_assignment: bool) {
        *self.latest_assignment.write() = Some(AppliedAssignment {
            id: id.to_owned(),
            is_portal_assignment,
        });

        let prev = self.assignment.read();
        let workers_len = match &assignment {
            ActiveAssignment::Legacy(assignment) => {
                self.update_datasets(
                    assignment
                        .datasets()
                        .iter()
                        .map(|d| (d.id(), d.chunks().len(), d.last_block())),
                    |id| match prev.as_ref() {
                        Some(ActiveAssignment::Legacy(p)) => {
                            p.get_dataset(id).map(|d| d.chunks().len())
                        }
                        _ => None,
                    },
                );
                assignment.workers().len()
            }
            #[cfg(feature = "mvcc-chunks")]
            ActiveAssignment::Portal(assignment) => {
                self.update_datasets(
                    assignment
                        .datasets()
                        .iter()
                        .map(|d| (d.id(), d.chunks().len(), d.last_block())),
                    |id| match prev.as_ref() {
                        Some(ActiveAssignment::Portal(p)) => {
                            p.get_dataset(id).map(|d| d.chunks().len())
                        }
                        _ => None,
                    },
                );
                assignment.workers().len()
            }
        };
        drop(prev);

        crate::metrics::KNOWN_WORKERS.set(workers_len as i64);

        *self.assignment.write() = Some(assignment);
    }

    /// Reports each dataset's new chunk count against its previous one (`prev_len`, keyed by
    /// dataset id), shared across both assignment formats -- the only thing that differs between
    /// them is how `(id, new_len, last_block)` and `prev_len` are looked up.
    fn update_datasets<'a>(
        &self,
        datasets_info: impl Iterator<Item = (&'a str, usize, u64)>,
        prev_len: impl Fn(&str) -> Option<usize>,
    ) {
        for (dataset_url, new_len, last_block) in datasets_info {
            let old_len = prev_len(dataset_url).unwrap_or(0);
            let dataset_id = DatasetId::from_url(dataset_url);
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
    }

    pub fn find_chunk(&self, dataset: &DatasetId, block: u64) -> Result<DataChunk, ChunkNotFound> {
        let dataset_url = dataset.to_url();
        let guard = self.assignment.read();
        let chunk_id = match guard.as_ref().ok_or(ChunkNotFound::UnknownDataset)? {
            ActiveAssignment::Legacy(assignment) => find_chunk_with(
                || assignment.find_chunk(dataset_url, block),
                || assignment.get_dataset(dataset_url).unwrap().first_block(),
            )?
            .id()
            .to_owned(),
            #[cfg(feature = "mvcc-chunks")]
            ActiveAssignment::Portal(assignment) => find_chunk_with(
                || assignment.find_chunk(dataset_url, block),
                || assignment.get_dataset(dataset_url).unwrap().first_block(),
            )?
            .id()
            .to_owned(),
        };
        chunk_id.parse().map_err(|e| {
            tracing::warn!(error = %e, "Failed to parse chunk ID");
            ChunkNotFound::InvalidID(chunk_id)
        })
    }

    pub fn find_chunk_by_timestamp(
        &self,
        dataset: &DatasetId,
        ts: u64,
    ) -> Result<DataChunk, ChunkNotFound> {
        let dataset_url = dataset.to_url();
        let guard = self.assignment.read();
        let chunk_id = match guard.as_ref().ok_or(ChunkNotFound::UnknownDataset)? {
            ActiveAssignment::Legacy(assignment) => find_chunk_with(
                || assignment.find_chunk_by_timestamp(dataset_url, ts),
                || assignment.get_dataset(dataset_url).unwrap().first_block(),
            )?
            .id()
            .to_owned(),
            #[cfg(feature = "mvcc-chunks")]
            ActiveAssignment::Portal(assignment) => find_chunk_with(
                || assignment.find_chunk_by_timestamp(dataset_url, ts),
                || assignment.get_dataset(dataset_url).unwrap().first_block(),
            )?
            .id()
            .to_owned(),
        };
        chunk_id.parse().map_err(|e| {
            tracing::warn!(error = %e, "Failed to parse chunk ID");
            ChunkNotFound::InvalidID(chunk_id)
        })
    }

    pub fn find_workers(
        &self,
        dataset: &DatasetId,
        block: u64,
    ) -> Result<Vec<PeerId>, ChunkNotFound> {
        let dataset_url = dataset.to_url();
        let guard = self.assignment.read();
        match guard.as_ref().ok_or(ChunkNotFound::UnknownDataset)? {
            ActiveAssignment::Legacy(assignment) => {
                let chunk = find_chunk_with(
                    || assignment.find_chunk(dataset_url, block),
                    || assignment.get_dataset(dataset_url).unwrap().first_block(),
                )?;
                Ok(
                    self.filtered_worker_ids(chunk.worker_indexes().iter(), |idx| {
                        let w = assignment.get_worker_by_index(idx);
                        (w.status(), w.peer_id())
                    }),
                )
            }
            #[cfg(feature = "mvcc-chunks")]
            ActiveAssignment::Portal(assignment) => {
                let chunk = find_chunk_with(
                    || assignment.find_chunk(dataset_url, block),
                    || assignment.get_dataset(dataset_url).unwrap().first_block(),
                )?;
                Ok(
                    self.filtered_worker_ids(chunk.worker_indexes().iter(), |idx| {
                        let w = assignment.get_worker_by_index(idx);
                        (w.status(), w.peer_id())
                    }),
                )
            }
        }
    }

    fn filtered_worker_ids(
        &self,
        indexes: impl Iterator<Item = u16>,
        lookup: impl Fn(u16) -> (sqd_assignments::WorkerStatus, Result<PeerId, anyhow::Error>),
    ) -> Vec<PeerId> {
        indexes
            .filter_map(|idx| {
                let (status, peer_id) = lookup(idx);
                if status == sqd_assignments::WorkerStatus::UnsupportedVersion {
                    return None;
                }
                if self.ignore_deprecated_workers
                    && status == sqd_assignments::WorkerStatus::DeprecatedVersion
                {
                    return None;
                }
                peer_id
                    .inspect_err(
                        |e| tracing::warn!(error = %e, "Failed to parse worker ID #{}", idx),
                    )
                    .ok()
            })
            .collect()
    }

    pub fn next_chunk(&self, dataset: &DatasetId, chunk: &DataChunk) -> Option<DataChunk> {
        self.find_chunk(dataset, chunk.last_block + 1).ok()
    }

    pub fn first_block(&self, dataset: &DatasetId) -> Option<BlockNumber> {
        let dataset_url = dataset.to_url();
        match self.assignment.read().as_ref()? {
            ActiveAssignment::Legacy(a) => Some(a.get_dataset(dataset_url)?.first_block()),
            #[cfg(feature = "mvcc-chunks")]
            ActiveAssignment::Portal(a) => Some(a.get_dataset(dataset_url)?.first_block()),
        }
    }

    pub fn head(&self, dataset: &DatasetId) -> Option<BlockRef> {
        let dataset_url = dataset.to_url();
        match self.assignment.read().as_ref()? {
            ActiveAssignment::Legacy(a) => {
                let dataset = a.get_dataset(dataset_url)?;
                dataset.last_block_hash().map(|hash| BlockRef {
                    hash: hash.to_owned(),
                    number: dataset.last_block(),
                })
            }
            #[cfg(feature = "mvcc-chunks")]
            ActiveAssignment::Portal(a) => {
                let dataset = a.get_dataset(dataset_url)?;
                dataset.last_block_hash().map(|hash| BlockRef {
                    hash: hash.to_owned(),
                    number: dataset.last_block(),
                })
            }
        }
    }

    pub fn get_all_workers(&self) -> Vec<PeerId> {
        match self.assignment.read().as_ref() {
            Some(ActiveAssignment::Legacy(a)) => a
                .workers()
                .iter()
                .filter_map(|w| (*w.worker_id()).try_into().ok())
                .collect(),
            #[cfg(feature = "mvcc-chunks")]
            Some(ActiveAssignment::Portal(a)) => a
                .workers()
                .iter()
                .filter_map(|w| (*w.worker_id()).try_into().ok())
                .collect(),
            None => Vec::new(),
        }
    }

    #[instrument(skip(self))]
    pub fn get_dataset_state(&self, dataset: &DatasetId) -> Option<DatasetState> {
        let dataset_url = dataset.to_url();
        let mut ranges: HashMap<_, Vec<_>> = HashMap::new();
        match self.assignment.read().as_ref()? {
            ActiveAssignment::Legacy(assignment) => {
                for c in assignment.get_dataset(dataset_url)?.chunks().iter() {
                    accumulate_range(&mut ranges, c.id(), c.worker_indexes().iter(), |idx| {
                        assignment.get_worker_id(idx)
                    });
                }
            }
            #[cfg(feature = "mvcc-chunks")]
            ActiveAssignment::Portal(assignment) => {
                for c in assignment.get_dataset(dataset_url)?.chunks().iter() {
                    accumulate_range(&mut ranges, c.id(), c.worker_indexes().iter(), |idx| {
                        assignment.get_worker_id(idx)
                    });
                }
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

fn accumulate_range(
    ranges: &mut HashMap<PeerId, Vec<sqd_messages::Range>>,
    chunk_id: &str,
    worker_indexes: impl Iterator<Item = u16>,
    get_worker_id: impl Fn(u16) -> Result<PeerId, anyhow::Error>,
) {
    let range = chunk_id.parse::<DataChunk>().unwrap().range_msg();
    for idx in worker_indexes {
        let peer_id = match get_worker_id(idx) {
            Ok(peer_id) => peer_id,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to parse worker ID #{}", idx);
                continue;
            }
        };
        ranges.entry(peer_id).or_default().push(range);
    }
}

/// Selects the assignment portals should use for routing, and reports whether it was the
/// dedicated portal assignment (`true`) or the legacy shared assignment (`false`).
///
/// Under `mvcc-chunks`, portals prefer `portal_assignment` when `prefer_portal_assignment` is
/// set (a runtime config flag, so it can be reverted without a rebuild), but fall back to the
/// legacy assignment if it's disabled or the scheduler hasn't published `portal_assignment` yet.
fn visible_assignment(
    network_state: &sqd_assignments::NetworkState,
    prefer_portal_assignment: bool,
) -> (&NetworkAssignment, bool) {
    #[cfg(feature = "mvcc-chunks")]
    if prefer_portal_assignment {
        match network_state.portal_assignment.as_ref() {
            Some(assignment) => return (assignment, true),
            None => {
                tracing::warn!(
                    "portal_assignment missing in network state; falling back to legacy assignment"
                );
            }
        }
    }

    #[cfg(not(feature = "mvcc-chunks"))]
    let _ = prefer_portal_assignment;

    (&network_state.assignment, false)
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

        let (visible, is_portal) = visible_assignment(&state, true);
        assert_eq!(visible.id, "legacy");
        assert!(!is_portal);
    }

    #[cfg(feature = "mvcc-chunks")]
    #[test]
    fn visible_assignment_prefers_portal_assignment() {
        let mut state = network_state();
        state.portal_assignment = Some(assignment("portal"));

        let (visible, is_portal) = visible_assignment(&state, true);
        assert_eq!(visible.id, "portal");
        assert!(is_portal);
    }

    fn applied(id: &str) -> AppliedAssignment {
        AppliedAssignment {
            id: id.to_string(),
            is_portal_assignment: true,
        }
    }

    #[test]
    fn older_assignment_is_stale() {
        let current = applied("2026-08-07T19:21:53_AAAA");
        assert!(is_stale(&current, "2026-08-07T19:02:21_BBBB", true));
    }

    #[test]
    fn newer_assignment_is_not_stale() {
        let current = applied("2026-08-07T19:02:21_AAAA");
        assert!(!is_stale(&current, "2026-08-07T19:21:53_BBBB", true));
    }

    #[test]
    fn same_timestamp_is_not_stale() {
        // Only the timestamp orders, so a differing hash must still apply.
        let current = applied("2026-08-07T19:21:53_AAAA");
        assert!(!is_stale(&current, "2026-08-07T19:21:53_BBBB", true));
    }

    #[test]
    fn assignment_from_the_other_source_is_never_stale() {
        // Otherwise a fallback between the two sources would look like a regression.
        let current = applied("2026-08-07T19:21:53_AAAA");
        assert!(!is_stale(&current, "2026-08-07T19:02:21_BBBB", false));
    }

    #[test]
    fn unparseable_ids_are_never_stale() {
        let cases = [
            ("no-separator", "2026-01-01T00:00:00_A"),
            ("2026-08-07T19:21:53_AAAA", "no-separator"),
            ("_AAAA", "2026-08-07T19:21:53_BBBB"),
            ("2026-08-07T19:21:53_", "2020-01-01T00:00:00_B"),
        ];
        for (current, candidate) in cases {
            assert!(
                !is_stale(&applied(current), candidate, true),
                "{current} / {candidate}"
            );
        }
    }

    #[test]
    fn ids_from_different_timestamp_formats_are_never_stale() {
        // A format change must not make every subsequent assignment look older.
        let current = applied("2026-08-07T19:21:53_AAAA");
        assert!(!is_stale(&current, "20260807T192153_BBBB", true));
    }

    #[cfg(feature = "mvcc-chunks")]
    #[test]
    fn visible_assignment_can_be_reverted_to_legacy_via_config() {
        let mut state = network_state();
        state.portal_assignment = Some(assignment("portal"));

        let (visible, is_portal) = visible_assignment(&state, false);
        assert_eq!(visible.id, "legacy");
        assert!(!is_portal);
    }
}

/// Runs a per-format `find_chunk`/`find_chunk_by_timestamp` call and converts a not-found error,
/// generic over the chunk type so both `Assignment` and `PortalAssignment` share this instead of
/// duplicating the `map_err` wrapping in each match arm.
fn find_chunk_with<C>(
    find: impl FnOnce() -> Result<C, sqd_assignments::ChunkNotFound>,
    first_block: impl FnOnce() -> u64,
) -> Result<C, ChunkNotFound> {
    find().map_err(|e| convert_chunk_not_found(e, first_block))
}

fn convert_chunk_not_found(
    e: sqd_assignments::ChunkNotFound,
    first_block: impl FnOnce() -> u64,
) -> ChunkNotFound {
    match e {
        sqd_assignments::ChunkNotFound::AfterLast => ChunkNotFound::AfterLast,
        sqd_assignments::ChunkNotFound::BeforeFirst => ChunkNotFound::BeforeFirst {
            first_block: first_block(),
        },
        sqd_assignments::ChunkNotFound::UnknownDataset => ChunkNotFound::UnknownDataset,
    }
}

/// Whether `candidate` is older than the applied assignment, and so must not replace it.
///
/// Ids are `<timestamp>_<hash>` with a fixed-width timestamp, so prefixes order lexicographically.
/// The hash is excluded: it carries no ordering, and would order same-second ids arbitrarily.
///
/// Every uncertain case returns false. Wrongly accepting costs one poll; wrongly rejecting freezes
/// the head until restart.
fn is_stale(applied: &AppliedAssignment, candidate: &str, candidate_is_portal: bool) -> bool {
    if applied.is_portal_assignment != candidate_is_portal {
        return false;
    }
    let (Some(current), Some(new)) = (timestamp_prefix(&applied.id), timestamp_prefix(candidate))
    else {
        return false;
    };
    // Differing lengths mean differing formats, which lexicographic order cannot span.
    if current.len() != new.len() {
        return false;
    }
    new < current
}

/// The ordering-bearing prefix of an id, or `None` if it isn't shaped `<timestamp>_<hash>`.
fn timestamp_prefix(id: &str) -> Option<&str> {
    let (timestamp, hash) = id.split_once('_')?;
    (!timestamp.is_empty() && !hash.is_empty()).then_some(timestamp)
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
