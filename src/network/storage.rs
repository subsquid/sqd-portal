use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::anyhow;
use sqd_assignments::{Assignment, AssignmentType, PortalAssignment, ResolvedAssignments};
use sqd_contract_client::{Network, PeerId};
use sqd_primitives::BlockRef;
use tracing::instrument;

use crate::{
    datasets::Datasets,
    metrics,
    types::{api_types::DatasetState, BlockNumber, DataChunk, DatasetId},
    utils::RwLock,
};

/// The assignment currently held by the client, in whichever wire format it was published in.
/// See docs/assignment-wire-format.md in network-scheduler for the split rationale.
enum ActiveAssignment {
    Legacy(Assignment),
    Portal(PortalAssignment),
}

pub struct StorageClient {
    assignment: RwLock<Option<ActiveAssignment>>,
    datasets_config: Arc<RwLock<Datasets>>,
    /// Id of the last applied assignment. Ids only order within a single source, and
    /// `is_stale` fails open on two it cannot compare, so a change of source costs a poll.
    latest_assignment_id: RwLock<Option<String>>,
    network_state_url: String,
    reqwest_client: reqwest::Client,
    ignore_deprecated_workers: bool,
    /// Overrides the source the network state names; `None` follows it.
    assignment_source: Option<AssignmentType>,
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum ChunkNotFound {
    #[error("Unknown dataset")]
    UnknownDataset,
    #[error("Block is before the first block which is {first_block}")]
    BeforeFirst { first_block: BlockNumber },
    #[error("Block is after the last block")]
    AfterLast,
    /// Only the portal artifact can report this: it gives each chunk its own end, so a block
    /// between two chunks is an answer rather than the preceding chunk. The legacy reader has no
    /// per-chunk end and silently attributes such a block to the chunk before it.
    #[error("Block falls in a gap between chunks")]
    InGap,
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
            assignment_source: None,
        }
    }

    pub fn ignore_deprecated_workers(&mut self) {
        self.ignore_deprecated_workers = true;
    }

    pub fn set_assignment_source(&mut self, source: Option<AssignmentType>) {
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
        let selected =
            select_assignment(network_state, self.assignment_source).inspect_err(|_| {
                metrics::MISSING_ASSIGNMENT_SOURCE.inc();
            })?;
        let latest = self.latest_assignment_id.read().clone();
        if latest.as_deref() == Some(selected.id.as_str()) {
            tracing::debug!("Assignment has not been changed");
            return Ok(());
        }

        if let Some(latest) = &latest {
            if is_stale(latest, &selected.id) {
                // Applying it would move the head backwards, dropping already-advertised chunks
                // and opening a range no source covers. A later poll brings the newer one back.
                tracing::warn!(
                    stale_id = %selected.id,
                    current_id = %latest,
                    "Rejected an assignment older than the current one"
                );
                metrics::STALE_ASSIGNMENTS_REJECTED.inc();
                return Ok(());
            }
        }

        let assignment = self
            .fetch_assignment(&selected.url, selected.source)
            .await?;

        // Only the legacy artifact declares a time to wait for.
        if let (Some(_), Some(effective_from)) = (&latest, selected.effective_from) {
            sleep_until(effective_from).await;
        }

        self.set_assignment(assignment, &selected.id);

        tracing::info!(source = %selected.source, "Applied assignment \"{}\"", selected.id);
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
        source: AssignmentType,
    ) -> anyhow::Result<ActiveAssignment> {
        use futures::TryStreamExt;
        use tokio_util::io::StreamReader;

        let response = self
            .reqwest_client
            .get(url)
            .send()
            .await?
            .error_for_status()?;
        let stream = response.bytes_stream();
        let reader = StreamReader::new(stream.map_err(std::io::Error::other));
        let buf = decompress_artifact(reader, ArtifactCodec::of(url)).await?;

        tracing::debug!("Downloaded assignment from {}", url);

        Ok(match source {
            AssignmentType::Legacy => {
                ActiveAssignment::Legacy(Assignment::from_owned_unchecked(buf))
            }
            AssignmentType::Split => {
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
                        .map(|d| (d.id(), d.chunk_count(), d.last_block())),
                    |id| match prev.as_ref() {
                        Some(ActiveAssignment::Portal(p)) => {
                            p.get_dataset(id).map(|d| d.chunk_count())
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
        match guard.as_ref().ok_or(ChunkNotFound::UnknownDataset)? {
            ActiveAssignment::Legacy(assignment) => parse_chunk_id(
                find_chunk_with(
                    || assignment.find_chunk(dataset_url, block),
                    || legacy_first_block(assignment, dataset_url).unwrap_or(0),
                )?
                .id(),
            ),
            ActiveAssignment::Portal(assignment) => portal_data_chunk(find_chunk_with(
                || find_portal_chunk(assignment, dataset_url, block),
                || portal_first_block(assignment, dataset_url).unwrap_or(0),
            )?),
        }
    }

    pub fn find_chunk_by_timestamp(
        &self,
        dataset: &DatasetId,
        ts: u64,
    ) -> Result<DataChunk, ChunkNotFound> {
        let dataset_url = dataset.to_url();
        let guard = self.assignment.read();
        match guard.as_ref().ok_or(ChunkNotFound::UnknownDataset)? {
            ActiveAssignment::Legacy(assignment) => parse_chunk_id(
                find_chunk_with(
                    || assignment.find_chunk_by_timestamp(dataset_url, ts),
                    || legacy_first_block(assignment, dataset_url).unwrap_or(0),
                )?
                .id(),
            ),
            ActiveAssignment::Portal(assignment) => portal_data_chunk(find_chunk_with(
                || assignment.find_chunk_by_timestamp(dataset_url, ts),
                || portal_first_block(assignment, dataset_url).unwrap_or(0),
            )?),
        }
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
                    || legacy_first_block(assignment, dataset_url).unwrap_or(0),
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
                    || find_portal_chunk(assignment, dataset_url, block),
                    || portal_first_block(assignment, dataset_url).unwrap_or(0),
                )?;
                Ok(self.filtered_worker_ids(chunk.worker_indexes(), |idx| {
                    let w = assignment.get_worker_by_index(idx);
                    (w.status(), w.peer_id())
                }))
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
        let next_block = chunk.last_block.checked_add(1)?;
        self.find_chunk(dataset, next_block)
            .ok()
            .filter(|next| advances_past(chunk, next))
    }

    pub fn first_block(&self, dataset: &DatasetId) -> Option<BlockNumber> {
        let dataset_url = dataset.to_url();
        match self.assignment.read().as_ref()? {
            ActiveAssignment::Legacy(a) => legacy_first_block(a, dataset_url),
            ActiveAssignment::Portal(a) => portal_first_block(a, dataset_url),
        }
    }

    pub fn head(&self, dataset: &DatasetId) -> Option<BlockRef> {
        let dataset_url = dataset.to_url();
        match self.assignment.read().as_ref()? {
            ActiveAssignment::Legacy(a) => {
                let dataset = a.get_dataset(dataset_url)?;
                // Reads the last chunk, which an empty dataset does not have -- and it has no
                // head to report either way.
                if dataset.chunks().is_empty() {
                    return None;
                }
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
                    // A legacy chunk has no end block of its own, so its id is the only place to
                    // read one from -- and ids are wire data, so a bad one must not sink the
                    // whole dataset's state.
                    let range = match c.id().parse::<DataChunk>() {
                        Ok(chunk) => chunk.range_msg(),
                        Err(e) => {
                            tracing::warn!(error = %e, "Skipped a chunk with an unparseable ID");
                            continue;
                        }
                    };
                    accumulate_range(&mut ranges, range, c.worker_indexes().iter(), |idx| {
                        assignment.get_worker_id(idx)
                    });
                }
            }
            ActiveAssignment::Portal(assignment) => {
                for c in assignment.get_dataset(dataset_url)?.chunks() {
                    // Both ends are columns here, so the range needs no id: no rebuild, no parse,
                    // and nothing to skip when a chunk's hash isn't UTF-8.
                    let range = sqd_messages::Range {
                        begin: c.first_block(),
                        end: c.last_block(),
                    };
                    accumulate_range(&mut ranges, range, c.worker_indexes(), |idx| {
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

fn advances_past(current: &DataChunk, candidate: &DataChunk) -> bool {
    candidate.first_block > current.last_block
}

fn accumulate_range(
    ranges: &mut HashMap<PeerId, Vec<sqd_messages::Range>>,
    range: sqd_messages::Range,
    worker_indexes: impl Iterator<Item = u16>,
    get_worker_id: impl Fn(u16) -> Result<PeerId, anyhow::Error>,
) {
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

/// What one poll resolved to: which reader parses the blob, and what applying it needs.
struct SelectedAssignment {
    source: AssignmentType,
    id: String,
    url: String,
    /// Legacy only: the split blob has no such field, so it applies as soon as it is fetched.
    effective_from: Option<u64>,
}

/// The assignment `source` names together with its download url, or why it yielded nothing
/// this poll.
///
/// Which blobs a state carries says nothing about which are authoritative -- the migration
/// publishes both sets at once -- so `assignment_type` picks, unless `source` overrides it, and
/// the blobs it names must be there. That and a legacy descriptor carrying no `fb_url_v1` are
/// the same event to an operator, so they share a counter at the call site.
fn select_assignment(
    network_state: sqd_assignments::NetworkState,
    source: Option<AssignmentType>,
) -> anyhow::Result<SelectedAssignment> {
    let resolved = network_state.resolve(source)?;

    match resolved {
        ResolvedAssignments::Legacy(assignment) => Ok(SelectedAssignment {
            source: AssignmentType::Legacy,
            // Not hypothetical: `fb_url_v1` has no `skip_serializing_if`, so a descriptor
            // carrying only the deprecated urls serializes it as an explicit null.
            url: assignment
                .fb_url_v1
                .ok_or_else(|| anyhow!("the legacy assignment carries no fb_url_v1"))?,
            id: assignment.id,
            effective_from: Some(assignment.effective_from),
        }),
        // The worker blob and the schema bundle come with it, unread.
        ResolvedAssignments::Split { portal, .. } => Ok(SelectedAssignment {
            source: AssignmentType::Split,
            id: portal.id,
            url: portal.fb_url,
            effective_from: None,
        }),
    }
}

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
        sqd_assignments::ChunkNotFound::InGap => ChunkNotFound::InGap,
    }
}

/// How an artifact is compressed, taken from the url the network state points at: `.zst` for
/// zstd, anything else gzip -- which is what every artifact was before zstd existed, so an
/// unsuffixed or unfamiliar url keeps working exactly as it did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArtifactCodec {
    Gzip,
    Zstd,
}

impl ArtifactCodec {
    fn of(url: &str) -> Self {
        // Object stores hand out urls carrying a query, and the suffix is on the path.
        let path = match url.split_once(['?', '#']) {
            Some((path, _)) => path,
            None => url,
        };
        if path.ends_with(".zst") {
            Self::Zstd
        } else {
            Self::Gzip
        }
    }
}

async fn decompress_artifact(
    body: impl tokio::io::AsyncBufRead + Unpin,
    codec: ArtifactCodec,
) -> anyhow::Result<Vec<u8>> {
    use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
    use tokio::io::AsyncReadExt;

    let mut buf = Vec::new();
    match codec {
        ArtifactCodec::Gzip => GzipDecoder::new(body).read_to_end(&mut buf).await,
        ArtifactCodec::Zstd => ZstdDecoder::new(body).read_to_end(&mut buf).await,
    }
    .map_err(|e| anyhow!("Failed to decompress {codec:?} assignment: {e}"))?;
    Ok(buf)
}

fn parse_chunk_id(chunk_id: &str) -> Result<DataChunk, ChunkNotFound> {
    chunk_id.parse().map_err(|e| {
        tracing::warn!(error = %e, "Failed to parse chunk ID");
        ChunkNotFound::InvalidID(chunk_id.to_owned())
    })
}

fn portal_data_chunk(chunk: sqd_assignments::PortalChunk<'_>) -> Result<DataChunk, ChunkNotFound> {
    let hash = chunk
        .hash()
        .ok_or_else(|| ChunkNotFound::InvalidID("chunk hash is not valid UTF-8".to_owned()))?;
    DataChunk::new(chunk.top(), chunk.first_block(), chunk.last_block(), hash)
        // Only this artifact states which copy of a chunk workers serve; the legacy one has no
        // such column, so a chunk from it stays at 0 and the query leaves the field off the wire.
        .map(|data_chunk| data_chunk.with_version(chunk.version()))
        .ok_or_else(|| {
            ChunkNotFound::InvalidID(format!("chunk hash {hash:?} has an unusable length"))
        })
}

/// A dataset's first block, or `None` when it holds no chunks -- `first_block()` reads chunk 0 on
/// both readers, so an empty dataset panics them. Only a malformed artifact has one, but the
/// portal parses downloaded bytes with `from_owned_unchecked`, so nothing rejects one on the way
/// in and the callers sit in the request path.
fn legacy_first_block(assignment: &Assignment, dataset_url: &str) -> Option<u64> {
    let dataset = assignment.get_dataset(dataset_url)?;
    (!dataset.chunks().is_empty()).then(|| dataset.first_block())
}

fn portal_first_block(assignment: &PortalAssignment, dataset_url: &str) -> Option<u64> {
    let dataset = assignment.get_dataset(dataset_url)?;
    (dataset.chunk_count() > 0).then(|| dataset.first_block())
}

/// The chunk holding `block`, or -- when `block` falls in a gap between two chunks -- the first
/// chunk after it.
///
/// Only the portal artifact can report a gap: it gives each chunk its own end rather than running
/// it to the next chunk's start, so a block between two chunks is an answer rather than the chunk
/// before it. A portal only ever streams forward, so the next chunk that does hold data is the
/// useful answer -- reporting nothing would end a stream at every gap, and the legacy format
/// could not express one to begin with.
fn find_portal_chunk<'a>(
    assignment: &'a PortalAssignment,
    dataset_url: &str,
    block: u64,
) -> Result<sqd_assignments::PortalChunk<'a>, sqd_assignments::ChunkNotFound> {
    match assignment.find_chunk(dataset_url, block) {
        Err(sqd_assignments::ChunkNotFound::InGap) => {
            let dataset = assignment
                .get_dataset(dataset_url)
                .expect("a gap is only reported for a dataset that was found");
            // A gap past the last chunk is still within `last_block`, which is the dataset's head
            // rather than the last chunk's end, so there is not always a chunk after one.
            first_chunk_from(dataset, block).ok_or(sqd_assignments::ChunkNotFound::AfterLast)
        }
        other => other,
    }
}

fn first_chunk_from(
    dataset: sqd_assignments::fb::PortalAssignmentDataset<'_>,
    block: u64,
) -> Option<sqd_assignments::PortalChunk<'_>> {
    let (mut low, mut high) = (0u32, dataset.chunk_count() as u32);
    while low < high {
        let mid = low + (high - low) / 2;
        if dataset.chunk(mid)?.first_block() < block {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    dataset.chunk(low)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(deprecated)]
    fn legacy_assignment(url: Option<&str>) -> sqd_assignments::NetworkAssignment {
        sqd_assignments::NetworkAssignment {
            url: None,
            fb_url: None,
            fb_url_v1: url.map(ToOwned::to_owned),
            id: "legacy".to_string(),
            effective_from: 123,
        }
    }

    fn split_blob(id: &str) -> sqd_assignments::NetworkAssignmentV2 {
        sqd_assignments::NetworkAssignmentV2 {
            id: id.to_string(),
            fb_url: format!("https://example.test/{id}.fb.gz"),
            version: "2".to_string(),
        }
    }

    /// A state carrying exactly the blob sets named, and naming whichever type it likes -- the
    /// two move independently.
    fn network_state(
        assignment_type: AssignmentType,
        legacy: bool,
        split: bool,
    ) -> sqd_assignments::NetworkState {
        sqd_assignments::NetworkState {
            network: "testnet".to_string(),
            assignment_type,
            assignment: legacy
                .then(|| legacy_assignment(Some("https://example.test/legacy.fb.gz"))),
            // The pair is published together, and `resolve` refuses it without the bundle.
            worker_assignment: split.then(|| split_blob("worker")),
            portal_assignment: split.then(|| split_blob("portal")),
            schema_bundle: Some(sqd_assignments::SchemaBundle {
                hash: "a1b2c3".to_string(),
                url: "https://example.test/schema.bundle.gz".to_string(),
            }),
        }
    }

    /// Which blobs a type requires, and that an override beats the state's own type, are
    /// upstream's `resolve` and upstream's tests. What is ours is passing the pin through at
    /// all rather than always following the state.
    #[test]
    fn the_state_names_the_source_unless_the_portal_pins_one() {
        let state = || network_state(AssignmentType::Split, true, true);

        let followed = select_assignment(state(), None).expect("the split pair is published");
        let pinned =
            select_assignment(state(), Some(AssignmentType::Legacy)).expect("legacy is published");

        assert_eq!(followed.source, AssignmentType::Split);
        assert_eq!(pinned.source, AssignmentType::Legacy);
    }

    #[test]
    fn each_shape_yields_what_applying_it_needs() {
        let legacy = select_assignment(network_state(AssignmentType::Legacy, true, false), None)
            .expect("legacy is published");
        let split = select_assignment(network_state(AssignmentType::Split, false, true), None)
            .expect("the split pair is published");

        assert_eq!(legacy.url, "https://example.test/legacy.fb.gz");
        // Legacy alone declares a cutover instant; the split blob has no field for one.
        assert_eq!(legacy.effective_from, Some(123));
        assert_eq!(split.effective_from, None);
        // The portal half, not the worker half that resolves alongside it.
        assert_eq!(split.id, "portal");
        assert_eq!(split.url, "https://example.test/portal.fb.gz");
    }

    #[test]
    fn a_source_carrying_no_v1_url_is_unusable() {
        // Published but unfetchable is the same event as not published at all: one counter,
        // because an alert cannot tell the two apart.
        let mut state = network_state(AssignmentType::Legacy, true, true);
        state.assignment = Some(legacy_assignment(None));

        assert!(select_assignment(state, None).is_err());
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

    const GAPPED_DATASET: &str = "s3://gapped-dataset";

    fn gapped_portal_assignment() -> PortalAssignment {
        let mut builder = sqd_assignments::PortalAssignmentBuilder::new().check_continuity(false);
        let mut dataset = builder.new_dataset(GAPPED_DATASET, 0);
        for (first, last) in [(0u64, 99u64), (200, 299)] {
            let staged = dataset
                .new_chunk()
                .id(&format!("0000000000/{first:010}-{last:010}-aaaaa"))
                .block_range(first..=last)
                .finish();
            // With the check off a gap is still reported, but the chunk is staged anyway. Any
            // other error means the test built something the reader would reject.
            if let Err(e) = staged {
                assert!(
                    e.to_string().contains("must be contiguous"),
                    "unexpected chunk build error: {e}"
                );
            }
        }
        dataset.finish(Some("0xhead")).unwrap();
        PortalAssignment::from_owned(builder.finish()).unwrap()
    }

    fn gapped_legacy_assignment() -> Assignment {
        let mut builder =
            sqd_assignments::AssignmentBuilder::new("test-secret").check_continuity(false);
        for (first, last) in [(0u64, 99u64), (200, 299)] {
            let staged = builder
                .new_chunk()
                .id(&format!("0000000000/{first:010}-{last:010}-aaaaa"))
                .dataset_id(GAPPED_DATASET)
                .block_range(first..=last)
                .size(1)
                .dataset_base_url("s3://gapped")
                .files(&[])
                .worker_indexes(&[])
                .finish();
            // With the check off a gap is still reported, but the chunk is staged anyway. Any
            // other error means the fixture built something the reader would reject.
            if let Err(e) = staged {
                assert!(
                    e.to_string().contains("must be contiguous"),
                    "unexpected chunk build error: {e}"
                );
            }
        }
        builder.finish_dataset();
        Assignment::from_owned(builder.finish()).unwrap()
    }

    fn versioned_portal_assignment() -> PortalAssignment {
        let mut builder = sqd_assignments::PortalAssignmentBuilder::new();
        let mut dataset = builder.new_dataset(GAPPED_DATASET, 0);
        dataset
            .new_chunk()
            .id("0000000000/0000000000-0000000099-aaaaa")
            .block_range(0..=99)
            .version(7)
            .finish()
            .unwrap();
        dataset.finish(Some("0xhead")).unwrap();
        PortalAssignment::from_owned(builder.finish()).unwrap()
    }

    #[test]
    fn only_a_portal_chunk_carries_a_version() {
        let portal = versioned_portal_assignment();
        let legacy = gapped_legacy_assignment();

        let portal_chunk =
            portal_data_chunk(find_portal_chunk(&portal, GAPPED_DATASET, 50).unwrap()).unwrap();
        let legacy_chunk =
            parse_chunk_id(legacy.find_chunk(GAPPED_DATASET, 50).unwrap().id()).unwrap();

        assert_eq!(portal_chunk.version(), 7);
        assert_eq!(legacy_chunk.version(), 0);
        assert_eq!(portal_chunk.to_string(), legacy_chunk.to_string());
    }

    #[test]
    fn the_two_formats_resolve_the_same_gap_in_opposite_directions() {
        let legacy = gapped_legacy_assignment();
        let portal = gapped_portal_assignment();

        let legacy_chunk = legacy.find_chunk(GAPPED_DATASET, 150).unwrap();
        let portal_chunk = find_portal_chunk(&portal, GAPPED_DATASET, 150).unwrap();

        assert_eq!(legacy_chunk.first_block(), 0);
        assert_eq!(legacy_chunk.id(), "0000000000/0000000000-0000000099-aaaaa");
        assert_eq!(portal_chunk.first_block(), 200);
    }

    #[test]
    fn legacy_cannot_advance_across_a_gap() {
        // The legacy reader resolves the block after the first chunk backward, to that same
        // chunk. `next_chunk` rejects this non-advancing result, so the stream stops instead of
        // querying 0-99 repeatedly.
        let legacy = gapped_legacy_assignment();
        let current = legacy.find_chunk(GAPPED_DATASET, 0).unwrap();

        let after_first = legacy.find_chunk(GAPPED_DATASET, 100).unwrap();

        assert_eq!(after_first.first_block(), current.first_block());
        let current = current.id().parse::<DataChunk>().unwrap();
        let after_first = after_first.id().parse::<DataChunk>().unwrap();
        assert!(!advances_past(&current, &after_first));
    }

    #[test]
    fn a_block_in_a_gap_resolves_to_the_next_chunk() {
        let assignment = gapped_portal_assignment();

        let chunk = find_portal_chunk(&assignment, GAPPED_DATASET, 150).unwrap();
        assert_eq!(chunk.first_block(), 200);
    }

    #[test]
    fn the_block_after_a_chunk_crosses_the_gap() {
        // The portal reader resolves the block after the first chunk to the next real chunk, so
        // `next_chunk` accepts it as strict forward progress and the stream crosses the gap.
        let assignment = gapped_portal_assignment();
        let current = find_portal_chunk(&assignment, GAPPED_DATASET, 0).unwrap();

        let chunk = find_portal_chunk(&assignment, GAPPED_DATASET, 100).unwrap();
        assert_eq!(chunk.first_block(), 200);
        let current = portal_data_chunk(current).unwrap();
        let chunk = portal_data_chunk(chunk).unwrap();
        assert!(advances_past(&current, &chunk));
    }

    #[test]
    fn a_covered_block_still_resolves_to_its_own_chunk() {
        let assignment = gapped_portal_assignment();

        for (block, expected) in [(0, 0), (50, 0), (99, 0), (200, 200), (299, 200)] {
            let chunk = find_portal_chunk(&assignment, GAPPED_DATASET, block).unwrap();
            assert_eq!(chunk.first_block(), expected, "block {block}");
        }
    }

    #[test]
    fn a_block_past_the_last_chunk_is_still_after_last() {
        let assignment = gapped_portal_assignment();

        assert_eq!(
            find_portal_chunk(&assignment, GAPPED_DATASET, 300).err(),
            Some(sqd_assignments::ChunkNotFound::AfterLast)
        );
    }

    #[test]
    fn a_portal_chunk_builds_to_what_its_id_parses_to() {
        let assignment = gapped_portal_assignment();

        for block in [0, 250] {
            let chunk = find_portal_chunk(&assignment, GAPPED_DATASET, block).unwrap();
            let id = chunk.id().unwrap();

            let built = portal_data_chunk(chunk).unwrap();

            assert_eq!(built, id.parse::<DataChunk>().unwrap(), "block {block}");
            assert_eq!(built.to_string(), id, "block {block}");
        }
    }

    async fn compress(payload: &[u8], codec: ArtifactCodec) -> Vec<u8> {
        use async_compression::tokio::write::{GzipEncoder, ZstdEncoder};
        use tokio::io::AsyncWriteExt;

        let mut out = Vec::new();
        match codec {
            ArtifactCodec::Gzip => {
                let mut enc = GzipEncoder::new(&mut out);
                enc.write_all(payload).await.unwrap();
                enc.shutdown().await.unwrap();
            }
            ArtifactCodec::Zstd => {
                let mut enc = ZstdEncoder::new(&mut out);
                enc.write_all(payload).await.unwrap();
                enc.shutdown().await.unwrap();
            }
        }
        out
    }

    #[tokio::test]
    async fn an_artifact_is_read_gzipped_or_zstd_compressed() {
        let payload = b"portal assignment bytes".repeat(64);

        for codec in [ArtifactCodec::Gzip, ArtifactCodec::Zstd] {
            let body = compress(&payload, codec).await;

            let read = decompress_artifact(std::io::Cursor::new(body), codec)
                .await
                .unwrap();

            assert_eq!(read, payload, "{codec:?}");
        }
    }

    #[tokio::test]
    async fn an_artifact_compressed_the_other_way_is_refused() {
        let body = compress(b"portal assignment bytes", ArtifactCodec::Zstd).await;

        let err = decompress_artifact(std::io::Cursor::new(body), ArtifactCodec::Gzip)
            .await
            .unwrap_err();

        assert!(
            err.to_string().contains("Failed to decompress"),
            "got {err}"
        );
    }

    #[test]
    fn the_url_suffix_names_the_codec() {
        assert_eq!(
            ArtifactCodec::of("https://e.test/portal-assignment.fb.zst"),
            ArtifactCodec::Zstd
        );
        assert_eq!(
            ArtifactCodec::of("https://e.test/assignment.fb.gz"),
            ArtifactCodec::Gzip
        );
        // A query does not hide the suffix, and anything unfamiliar stays on gzip -- which is
        // what every artifact was before zstd.
        assert_eq!(
            ArtifactCodec::of("https://e.test/a.fb.zst?versionId=7&x=1"),
            ArtifactCodec::Zstd
        );
        assert_eq!(
            ArtifactCodec::of("https://e.test/a.fb"),
            ArtifactCodec::Gzip
        );
    }

    #[test]
    fn an_unpublished_dataset_has_no_first_block() {
        // The empty-chunks half of the same guard can't be built here -- the builder rejects an
        // empty dataset -- but the portal reads downloaded bytes with `from_owned_unchecked`, so
        // nothing rejects one on the way in either.
        let assignment = gapped_portal_assignment();

        assert_eq!(portal_first_block(&assignment, GAPPED_DATASET), Some(0));
        assert_eq!(
            portal_first_block(&assignment, "s3://never-published"),
            None
        );
    }
}
