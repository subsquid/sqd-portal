use std::{collections::HashMap, sync::Arc, time::Duration};

use itertools::Itertools;
use sqd_contract_client::{Network, PeerId};
use sqd_messages::assignments::{self, Assignment, ChunkSummary};
use sqd_primitives::BlockRef;
use tracing::instrument;

use crate::{
    datasets::Datasets,
    metrics,
    types::{api_types::DatasetState, BlockNumber, DataChunk, DatasetId},
    utils::RwLock,
};

#[derive(Debug, Clone)]
pub struct AssignedChunk {
    pub chunk: DataChunk,
    pub workers: Arc<Vec<PeerId>>,
}

pub struct DatasetIndex {
    pub chunks: Vec<AssignedChunk>,
    pub summary: Option<assignments::ChunkSummary>,
}

pub struct StorageClient {
    datasets: RwLock<HashMap<DatasetId, DatasetIndex>>,
    datasets_config: Arc<RwLock<Datasets>>,
    workers: RwLock<Vec<PeerId>>,
    latest_assignment: RwLock<Option<String>>,
    network_state_url: String,
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum ChunkNotFound {
    #[error("Unknown dataset")]
    UnknownDataset,
    #[error("Block is before the first block which is {first_block}")]
    BeforeFirst { first_block: BlockNumber },
    #[error("Block falls in a gap between chunks")]
    Gap,
    #[error("Block is after the last block")]
    AfterLast,
}

impl StorageClient {
    pub fn new(datasets_config: Arc<RwLock<Datasets>>, network: Network) -> Self {
        let network_state_filename = match network {
            Network::Tethys => "network-state-tethys.json",
            Network::Mainnet => "network-state-mainnet.json",
        };
        let network_state_url =
            format!("https://metadata.sqd-datasets.io/{network_state_filename}");
        Self {
            datasets: RwLock::new(Default::default(), "StorageClient::datasets"),
            datasets_config,
            workers: RwLock::new(Vec::new(), "StorageClient::workers"),
            latest_assignment: RwLock::new(None, "StorageClient::latest_assignment"),
            network_state_url,
        }
    }

    pub fn has_assignment(&self) -> bool {
        self.latest_assignment.read().is_some()
    }

    pub async fn try_update_assignment(&self) {
        tracing::debug!("Checking for new assignment");
        let latest_assignment = self.latest_assignment.read().clone();
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
                tracing::error!(error = ?err, "Unable to get assignment");
                return;
            }
        };

        // TODO: use assignment.effective_from

        let assignment_id = assignment.id.clone();
        tracing::debug!("Got assignment {:?}", assignment_id);
        *self.latest_assignment.write() = Some(assignment_id.clone());

        let workers = assignment.get_all_peer_ids();
        let datasets = match parse_assignment(assignment) {
            Ok(datasets) => datasets,
            Err(err) => {
                tracing::error!(error = ?err, "Failed to parse assignment, waiting for the next one");
                return;
            }
        };
        tracing::debug!("Assignment parsed");

        self.update_datasets(datasets);
        crate::metrics::KNOWN_WORKERS.set(workers.len() as i64);
        *self.workers.write() = workers;
        tracing::info!("New assignment saved");
    }

    #[instrument(skip_all)]
    fn update_datasets(&self, new_datasets: HashMap<DatasetId, DatasetIndex>) {
        tracing::info!("Saving known chunks");

        debug_assert!(
            new_datasets
                .values()
                .all(|index| index.chunks.is_sorted_by_key(|c| c.chunk.first_block)),
            "Chunks in the assignment must be sorted"
        );

        let datasets = self.datasets.read();
        for (dataset, index) in &new_datasets {
            let new_len = index.chunks.len();
            let last_block = index.chunks.last().map_or(0, |r| r.chunk.last_block);
            let prev = datasets.get(dataset);
            let old_len = prev.map_or(0, |i| i.chunks.len());
            if old_len < new_len {
                tracing::info!(
                    "Got {} new chunk(s) for dataset {}",
                    new_len - old_len,
                    dataset
                );
            }
            let dataset_name = self
                .datasets_config
                .read()
                .default_name(dataset)
                .map(ToOwned::to_owned);
            metrics::report_chunk_list_updated(dataset, dataset_name, new_len, last_block);
        }
        drop(datasets);

        *self.datasets.write() = new_datasets;
    }

    pub fn find_chunk(
        &self,
        dataset: &DatasetId,
        block: u64,
    ) -> Result<AssignedChunk, ChunkNotFound> {
        let datasets = self.datasets.read();
        let chunks = &datasets
            .get(dataset)
            .ok_or(ChunkNotFound::UnknownDataset)?
            .chunks;
        let first_block = chunks.first().ok_or(ChunkNotFound::Gap)?.chunk.first_block;
        if block < first_block {
            return Err(ChunkNotFound::BeforeFirst { first_block });
        }
        let first_suspect =
            chunks.partition_point(|chunk: &AssignedChunk| (chunk.chunk.last_block) < block);
        if first_suspect >= chunks.len() {
            return Err(ChunkNotFound::AfterLast);
        }
        if chunks[first_suspect].chunk.first_block <= block {
            Ok(chunks[first_suspect].clone())
        } else {
            Err(ChunkNotFound::Gap)
        }
    }

    pub fn next_chunk(&self, dataset: &DatasetId, chunk: &DataChunk) -> Option<DataChunk> {
        self.find_chunk(dataset, chunk.last_block + 1)
            .ok()
            .map(|c| c.chunk)
    }

    pub fn first_block(&self, dataset: &DatasetId) -> Option<BlockNumber> {
        self.datasets
            .read()
            .get(dataset)
            .and_then(|index| index.chunks.first().map(|c| c.chunk.first_block))
    }

    pub fn head(&self, dataset: &DatasetId) -> Option<BlockRef> {
        self.datasets.read().get(dataset).and_then(|index| {
            let number = index.chunks.last().map(|c| c.chunk.last_block);
            match (number, index.summary.as_ref()) {
                (Some(number), Some(summary)) => Some(BlockRef {
                    number: number,
                    hash: summary.last_block_hash.clone(),
                }),
                _ => None,
            }
        })
    }

    pub fn get_all_workers(&self) -> Vec<PeerId> {
        self.workers.read().clone()
    }

    pub fn get_dataset_state(&self, dataset: &DatasetId) -> Option<DatasetState> {
        let mut ranges: HashMap<_, Vec<_>> = HashMap::new();
        for c in &self.datasets.read().get(dataset)?.chunks {
            for &worker in c.workers.iter() {
                ranges.entry(worker).or_default().push(c.chunk.range_msg())
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

#[tracing::instrument(skip_all)]
fn parse_assignment(assignment: Assignment) -> anyhow::Result<HashMap<DatasetId, DatasetIndex>> {
    let mut chunks: Vec<(DatasetId, DataChunk, Vec<PeerId>, Option<ChunkSummary>)> = assignment
        .datasets
        .into_iter()
        .flat_map(|dataset| {
            let dataset_id = DatasetId::from_url(&dataset.id);
            dataset.chunks.into_iter().flat_map(move |chunk| {
                chunk
                    .id
                    .parse::<DataChunk>()
                    .map_err(|e| {
                        tracing::warn!(error=%e, "Couldn't parse chunk id '{}'", chunk.id);
                    })
                    .map(|id| (dataset_id.clone(), id, vec![], chunk.summary))
            })
        })
        .collect_vec();
    for (peer_id, worker) in assignment.worker_assignments {
        let mut index = 0;
        for delta in worker.chunks_deltas {
            index += delta;
            chunks[index as usize].2.push(peer_id);
        }
    }
    chunks.sort_unstable_by_key(|(dataset, chunk, _, _)| (dataset.clone(), chunk.first_block));
    let mut datasets = HashMap::new();
    for (dataset_id, chunk, workers, summary) in chunks {
        let dataset_index = datasets.entry(dataset_id).or_insert_with(|| DatasetIndex {
            chunks: Vec::new(),
            summary: None,
        });
        dataset_index.chunks.push(AssignedChunk {
            chunk,
            workers: Arc::new(workers),
        });
        dataset_index.summary = summary; // taken from the last chunk
    }

    Ok(datasets)
}
