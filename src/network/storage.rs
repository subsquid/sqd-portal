use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::anyhow;
use serde::Deserialize;
use sqd_assignments::{Assignment, NetworkAssignment, PortalAssignment};
use sqd_contract_client::{Network, PeerId};
use sqd_primitives::BlockRef;
use tracing::instrument;

use crate::{
    datasets::Datasets,
    metrics,
    types::{api_types::DatasetState, BlockNumber, DataChunk, DatasetId},
    utils::RwLock,
};

/// Which of the published artifacts the portal routes from.
///
/// The scheduler publishes both throughout the migration, so this is an outright choice rather
/// than a preference: only the selected artifact is ever consulted, and its absence is an error
/// rather than a reason to serve the other one. That is what keeps [`Legacy`] usable as a kill
/// switch and [`Portal`] verifiable as a canary.
///
/// [`Legacy`]: AssignmentSource::Legacy
/// [`Portal`]: AssignmentSource::Portal
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum AssignmentSource {
    // Kept short and free of Rust paths: clap renders these verbatim as the `--help` text for
    // each possible value.
    /// The combined assignment, served to workers and portals alike.
    #[default]
    Legacy,
    /// The dedicated portal assignment, carrying only what a portal reads.
    Portal,
}

impl std::fmt::Display for AssignmentSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Legacy => "legacy",
            Self::Portal => "portal",
        })
    }
}

/// The assignment currently held by the client, in whichever wire format it was published in.
/// See docs/assignment-wire-format.md in network-scheduler for the split rationale.
enum ActiveAssignment {
    Legacy(Assignment),
    Portal(PortalAssignment),
}

pub struct StorageClient {
    assignment: RwLock<Option<ActiveAssignment>>,
    datasets_config: Arc<RwLock<Datasets>>,
    /// Id of the last applied assignment. Ids only order within a single source, but the source
    /// is fixed for the process lifetime, so the id alone is enough to spot a regression.
    latest_assignment_id: RwLock<Option<String>>,
    network_state_url: String,
    reqwest_client: reqwest::Client,
    ignore_deprecated_workers: bool,
    assignment_source: AssignmentSource,
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
            latest_assignment_id: RwLock::new(None, "StorageClient::latest_assignment_id"),
            network_state_url,
            reqwest_client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .read_timeout(Duration::from_secs(5))
                .user_agent(format!("SQD Portal/{}", env!("CARGO_PKG_VERSION")))
                .build()
                .unwrap(),
            ignore_deprecated_workers: false,
            assignment_source: AssignmentSource::default(),
        }
    }

    pub fn ignore_deprecated_workers(&mut self) {
        self.ignore_deprecated_workers = true;
    }

    pub fn set_assignment_source(&mut self, source: AssignmentSource) {
        self.assignment_source = source;
    }

    pub fn num_workers(&self) -> usize {
        match self.assignment.read().as_ref() {
            Some(ActiveAssignment::Legacy(a)) => a.workers().len(),
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
        // Never falls back to the other source: that would make a portal pinned to one format
        // silently serve the other, defeating both the kill switch and the canary.
        let (selected, assignment_url) = usable_assignment(&network_state, self.assignment_source)
            .inspect_err(|_| {
                metrics::MISSING_ASSIGNMENT_SOURCE.inc();
            })?;
        let assignment_id = selected.id.clone();
        let effective_from = selected.effective_from;
        let latest = self.latest_assignment_id.read().clone();
        if latest.as_deref() == Some(assignment_id.as_str()) {
            tracing::debug!("Assignment has not been changed");
            return Ok(());
        }

        if let Some(latest) = &latest {
            if is_stale(latest, &assignment_id) {
                // Applying it would move the head backwards, dropping already-advertised chunks
                // and opening a range no source covers. A later poll brings the newer one back.
                tracing::warn!(
                    stale_id = %assignment_id,
                    current_id = %latest,
                    "Rejected an assignment older than the current one"
                );
                metrics::STALE_ASSIGNMENTS_REJECTED.inc();
                return Ok(());
            }
        }

        let assignment = self
            .fetch_assignment(&assignment_url, self.assignment_source)
            .await?;

        if latest.is_some() {
            sleep_until(effective_from).await;
        }

        self.set_assignment(assignment, &assignment_id);

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
    async fn fetch_assignment(
        &self,
        url: &str,
        source: AssignmentSource,
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

        Ok(match source {
            AssignmentSource::Legacy => {
                ActiveAssignment::Legacy(Assignment::from_owned_unchecked(buf))
            }
            AssignmentSource::Portal => {
                ActiveAssignment::Portal(PortalAssignment::from_owned_unchecked(buf))
            }
        })
    }

    #[instrument(skip_all)]
    fn set_assignment(&self, assignment: ActiveAssignment, id: &str) {
        *self.latest_assignment_id.write() = Some(id.to_owned());

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

/// The descriptor `source` names together with its download url, or why it yielded nothing this
/// poll.
///
/// A descriptor that is absent and one that carries no `fb_url_v1` are the same event to an
/// operator — the configured source is unusable — so they share a counter at the call site while
/// keeping messages that say which it was. The second shape is not hypothetical: `fb_url_v1` has
/// no `skip_serializing_if`, so a publisher serializing a descriptor that still only has the
/// deprecated urls writes it out as an explicit null.
fn usable_assignment(
    network_state: &sqd_assignments::NetworkState,
    source: AssignmentSource,
) -> anyhow::Result<(&NetworkAssignment, String)> {
    let selected = select_assignment(network_state, source)
        .ok_or_else(|| anyhow!("network state publishes no {source} assignment"))?;
    let url = selected
        .fb_url_v1
        .clone()
        .ok_or_else(|| anyhow!("the {source} assignment carries no fb_url_v1"))?;
    Ok((selected, url))
}

/// The artifact descriptor `source` names, or `None` when the publisher hasn't included it.
///
/// Every field of the network state is optional, because the migration walks it through three
/// shapes: legacy alone, both, then the split pair alone. Which of those a given network is in is
/// the publisher's business -- the portal's is to serve the format it was configured for, or
/// nothing at all.
fn select_assignment(
    network_state: &sqd_assignments::NetworkState,
    source: AssignmentSource,
) -> Option<&NetworkAssignment> {
    match source {
        AssignmentSource::Legacy => network_state.assignment.as_ref(),
        AssignmentSource::Portal => network_state.portal_assignment.as_ref(),
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

    /// A state publishing exactly the artifacts named, so a test can spell out which of the three
    /// migration shapes it is in.
    fn network_state(legacy: bool, portal: bool) -> sqd_assignments::NetworkState {
        sqd_assignments::NetworkState {
            network: "testnet".to_string(),
            assignment: legacy.then(|| assignment("legacy")),
            worker_assignment: None,
            portal_assignment: portal.then(|| assignment("portal")),
            schema_bundle: None,
        }
    }

    #[test]
    fn each_source_selects_its_own_artifact() {
        let state = network_state(true, true);

        let legacy = select_assignment(&state, AssignmentSource::Legacy);
        let portal = select_assignment(&state, AssignmentSource::Portal);

        assert_eq!(legacy.map(|a| a.id.as_str()), Some("legacy"));
        assert_eq!(portal.map(|a| a.id.as_str()), Some("portal"));
    }

    #[test]
    fn a_missing_source_never_falls_back_to_the_other() {
        // The whole point of the selector: pinning to one format must not silently serve the
        // other, in either direction or in either of the one-sided migration shapes.
        let legacy_only = network_state(true, false);
        let portal_only = network_state(false, true);

        assert!(select_assignment(&legacy_only, AssignmentSource::Portal).is_none());
        assert!(select_assignment(&portal_only, AssignmentSource::Legacy).is_none());
    }

    #[allow(deprecated)]
    fn assignment_without_url(id: &str) -> NetworkAssignment {
        NetworkAssignment {
            url: None,
            fb_url: None,
            fb_url_v1: None,
            id: id.to_string(),
            effective_from: 123,
        }
    }

    #[test]
    fn a_usable_source_yields_its_download_url() {
        let state = network_state(true, false);

        let (selected, url) = usable_assignment(&state, AssignmentSource::Legacy).unwrap();

        assert_eq!(selected.id, "legacy");
        assert_eq!(url, "https://example.test/legacy.fb.gz");
    }

    #[test]
    fn a_source_carrying_no_v1_url_is_unusable() {
        // Published but unfetchable is the same event as not published at all, and must reach
        // the same counter -- an alert cannot tell the two apart and should not have to.
        let mut state = network_state(true, true);
        state.portal_assignment = Some(assignment_without_url("portal"));

        assert!(usable_assignment(&state, AssignmentSource::Portal).is_err());
        // ...and one source's defect says nothing about the other.
        assert!(usable_assignment(&state, AssignmentSource::Legacy).is_ok());
    }

    #[test]
    fn an_absent_source_is_unusable() {
        let state = network_state(true, false);

        assert!(usable_assignment(&state, AssignmentSource::Portal).is_err());
    }

    #[test]
    fn no_published_artifact_selects_nothing() {
        let state = network_state(false, false);

        assert!(select_assignment(&state, AssignmentSource::Legacy).is_none());
        assert!(select_assignment(&state, AssignmentSource::Portal).is_none());
    }

    #[test]
    fn older_assignment_is_stale() {
        assert!(is_stale(
            "2026-08-07T19:21:53_AAAA",
            "2026-08-07T19:02:21_BBBB"
        ));
    }

    #[test]
    fn newer_assignment_is_not_stale() {
        assert!(!is_stale(
            "2026-08-07T19:02:21_AAAA",
            "2026-08-07T19:21:53_BBBB"
        ));
    }

    #[test]
    fn same_timestamp_is_not_stale() {
        // Only the timestamp orders, so a differing hash must still apply.
        assert!(!is_stale(
            "2026-08-07T19:21:53_AAAA",
            "2026-08-07T19:21:53_BBBB"
        ));
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
            assert!(!is_stale(current, candidate), "{current} / {candidate}");
        }
    }

    #[test]
    fn ids_from_different_timestamp_formats_are_never_stale() {
        // A format change must not make every subsequent assignment look older.
        assert!(!is_stale(
            "2026-08-07T19:21:53_AAAA",
            "20260807T192153_BBBB"
        ));
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
/// Both ids always come from the same source, which is fixed for the process lifetime, so they are
/// always drawn from one sequence and directly comparable.
///
/// Every uncertain case returns false. Wrongly accepting costs one poll; wrongly rejecting freezes
/// the head until restart.
fn is_stale(applied_id: &str, candidate: &str) -> bool {
    let (Some(current), Some(new)) = (timestamp_prefix(applied_id), timestamp_prefix(candidate))
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
