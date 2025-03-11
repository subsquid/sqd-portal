use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;
use sqd_messages::assignments;
use sqd_primitives::BlockRef;

use crate::{
    datasets::Datasets,
    metrics,
    types::{BlockNumber, DataChunk, DatasetId},
};

pub struct DatasetIndex {
    pub chunks: Vec<DataChunk>,
    pub summary: Option<assignments::ChunkSummary>,
}

pub struct StorageClient {
    datasets: RwLock<HashMap<DatasetId, DatasetIndex>>,
    datasets_config: Arc<RwLock<Datasets>>,
}

pub enum ChunkNotFound {
    UnknownDataset,
    BeforeFirst(BeforeFirstError),
    Gap,
    AfterLast,
}

pub struct BeforeFirstError {
    pub first_block: BlockNumber,
}

impl StorageClient {
    pub fn new(datasets_config: Arc<RwLock<Datasets>>) -> Self {
        Self {
            datasets: RwLock::default(),
            datasets_config,
        }
    }

    pub fn update_datasets(&self, mut new_datasets: HashMap<DatasetId, DatasetIndex>) {
        tracing::info!("Saving known chunks");

        let timer = tokio::time::Instant::now();

        for index in new_datasets.values_mut() {
            index.chunks.sort_by_key(|r| r.first_block);
        }

        let mut datasets = self.datasets.write();
        for (dataset, index) in new_datasets {
            let new_len = index.chunks.len();
            let last_block = index.chunks.last().map_or(0, |r| r.last_block);
            let prev = datasets.insert(dataset.clone(), index);
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
                .default_name(&dataset)
                .map(ToOwned::to_owned);
            metrics::report_chunk_list_updated(&dataset, dataset_name, new_len, last_block);
        }

        let elapsed = timer.elapsed().as_millis();
        tracing::debug!("Chunks parsed in {elapsed} ms");
    }

    pub fn find_chunk(&self, dataset: &DatasetId, block: u64) -> Result<DataChunk, ChunkNotFound> {
        let datasets = self.datasets.read();
        let chunks = &datasets
            .get(dataset)
            .ok_or(ChunkNotFound::UnknownDataset)?
            .chunks;
        let first_block = chunks.first().ok_or(ChunkNotFound::Gap)?.first_block;
        if block < first_block {
            return Err(ChunkNotFound::BeforeFirst(BeforeFirstError { first_block }));
        }
        let first_suspect = chunks.partition_point(|chunk: &DataChunk| (chunk.last_block) < block);
        if first_suspect >= chunks.len() {
            return Err(ChunkNotFound::AfterLast);
        }
        if chunks[first_suspect].first_block <= block {
            Ok(chunks[first_suspect])
        } else {
            Err(ChunkNotFound::Gap)
        }
    }

    pub fn next_chunk(&self, dataset: &DatasetId, chunk: &DataChunk) -> Option<DataChunk> {
        self.find_chunk(dataset, chunk.last_block + 1).ok()
    }

    pub fn first_block(&self, dataset: &DatasetId) -> Option<BlockNumber> {
        self.datasets
            .read()
            .get(dataset)
            .and_then(|index| index.chunks.first().map(|chunk| chunk.first_block))
    }

    pub fn head(&self, dataset: &DatasetId) -> Option<BlockRef> {
        self.datasets.read().get(dataset).and_then(|index| {
            let number = index.chunks.last().map(|c| c.last_block);
            match (number, index.summary.as_ref()) {
                (Some(number), Some(summary)) => Some(BlockRef {
                    number: number,
                    hash: summary.last_block_hash.clone(),
                }),
                _ => None,
            }
        })
    }
}
