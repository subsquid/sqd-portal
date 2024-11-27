use std::collections::HashMap;

use parking_lot::RwLock;

use crate::{
    metrics,
    types::{DataChunk, DatasetId},
};

pub struct StorageClient {
    datasets: RwLock<HashMap<DatasetId, Vec<DataChunk>>>,
}

impl StorageClient {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            datasets: Default::default(),
        })
    }

    pub fn update_datasets(&self, mut new_datasets: HashMap<DatasetId, Vec<DataChunk>>) {
        tracing::info!("Saving known chunks");

        let timer = tokio::time::Instant::now();

        for chunks in new_datasets.values_mut() {
            chunks.sort_by_key(|r| r.first_block);
        }

        let mut datasets = self.datasets.write();
        for (dataset, chunks) in new_datasets {
            let new_len = chunks.len();
            let last_block = chunks.last().map(|r| r.last_block).unwrap_or(0);
            let prev = datasets.insert(dataset.clone(), chunks);
            let old_len = prev.map(|v| v.len()).unwrap_or(0);
            if old_len < new_len {
                tracing::info!(
                    "Got {} new chunk(s) for dataset {}",
                    new_len - old_len,
                    dataset
                );
            }
            metrics::report_chunk_list_updated(&dataset, new_len, last_block);
        }

        let elapsed = timer.elapsed().as_millis();
        tracing::debug!("Chunks parsed in {elapsed} ms");
    }

    pub fn find_chunk(&self, dataset: &DatasetId, block: u64) -> Option<DataChunk> {
        let datasets = self.datasets.read();
        let chunks = datasets.get(dataset)?;
        if block < chunks.first()?.first_block {
            return None;
        }
        let first_suspect = chunks.partition_point(|chunk| (chunk.last_block) < block);
        (first_suspect < chunks.len()).then(|| chunks[first_suspect])
    }

    pub fn next_chunk(&self, dataset: &DatasetId, chunk: &DataChunk) -> Option<DataChunk> {
        self.find_chunk(dataset, chunk.last_block + 1)
    }
}
