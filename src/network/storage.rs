use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;

use crate::{
    datasets::DatasetsMapping,
    metrics,
    types::{DataChunk, DatasetId},
};

pub struct StorageClient {
    datasets: RwLock<HashMap<DatasetId, Vec<DataChunk>>>,
    network_datasets: Arc<DatasetsMapping>,
}

impl StorageClient {
    pub fn new(network_datasets: Arc<DatasetsMapping>) -> Self {
        Self {
            datasets: RwLock::default(),
            network_datasets,
        }
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
            let last_block = chunks.last().map_or(0, |r| r.last_block);
            let prev = datasets.insert(dataset.clone(), chunks);
            let old_len = prev.map_or(0, |v| v.len());
            if old_len < new_len {
                tracing::info!(
                    "Got {} new chunk(s) for dataset {}",
                    new_len - old_len,
                    dataset
                );
            }
            let dataset_name = self.network_datasets.dataset_default_name(&dataset);
            metrics::report_chunk_list_updated(&dataset, dataset_name, new_len, last_block);
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
        (first_suspect < chunks.len() && chunks[first_suspect].first_block <= block)
            .then(|| chunks[first_suspect])
    }

    pub fn next_chunk(&self, dataset: &DatasetId, chunk: &DataChunk) -> Option<DataChunk> {
        self.find_chunk(dataset, chunk.last_block + 1)
    }
}
