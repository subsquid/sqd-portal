use std::{collections::HashMap, str::FromStr};

use aws_sdk_s3 as s3;
use parking_lot::RwLock;
use subsquid_datasets::DatasetStorage;
use subsquid_messages::data_chunk::DataChunk;
use tokio::sync::Mutex;

use crate::types::DatasetId;

lazy_static::lazy_static! {
    static ref S3_ENDPOINT: String = std::env::var("AWS_S3_ENDPOINT").expect("AWS_S3_ENDPOINT var not set");
}

pub struct StorageClient {
    datasets: HashMap<DatasetId, Dataset>,
}

impl StorageClient {
    pub async fn new(buckets: impl IntoIterator<Item = impl AsRef<str>>) -> anyhow::Result<Self> {
        let s3_config = aws_config::from_env()
            .endpoint_url(S3_ENDPOINT.clone())
            .load()
            .await;
        let s3_client = s3::Client::new(&s3_config);
        let mut datasets = HashMap::new();
        for bucket in buckets {
            let dataset = Dataset::new(
                bucket
                    .as_ref()
                    .strip_prefix("s3://")
                    .ok_or(anyhow::anyhow!("Wrong bucket url in config"))?,
                s3_client.clone(),
            )?;
            datasets.insert(DatasetId::from_url(bucket.as_ref()), dataset);
        }
        Ok(Self { datasets })
    }

    pub async fn update(&self) {
        tracing::info!("Updating known chunks");
        futures::future::join_all(self.datasets.iter().map(|(id, dataset)| async move {
            let result = dataset.update().await;
            if let Err(e) = result {
                tracing::warn!("Failed to update dataset {}: {:?}", id, e);
            }
        }))
        .await;
    }

    pub fn find_chunk(&self, dataset: &DatasetId, block: u64) -> Option<DataChunk> {
        self.datasets.get(dataset)?.find(block)
    }

    pub fn next_chunk(&self, dataset: &DatasetId, chunk: &DataChunk) -> Option<DataChunk> {
        self.datasets
            .get(dataset)?
            .find(chunk.last_block() as u64 + 1)
    }
}

struct Dataset {
    storage: Mutex<DatasetStorage>,
    chunks: RwLock<Vec<DataChunk>>,
}

impl Dataset {
    fn new(bucket: &str, s3_client: aws_sdk_s3::Client) -> anyhow::Result<Self> {
        let storage = DatasetStorage::new(bucket, s3_client);
        Ok(Self {
            chunks: RwLock::new(Vec::new()),
            storage: Mutex::new(storage),
        })
    }

    async fn update(&self) -> anyhow::Result<()> {
        // TODO: move synchronization inside the list_all_new_chunks method
        let new_chunks = self.storage.lock().await.list_all_new_chunks().await?;
        if !new_chunks.is_empty() {
            tracing::info!(
                "Found {} new chunks for dataset {}",
                new_chunks.len(),
                self.storage.lock().await.bucket
            );
        }
        let mut chunks = self.chunks.write();
        for chunk in new_chunks {
            let chunk = DataChunk::from_str(&chunk.chunk_str)
                .unwrap_or_else(|_| panic!("Failed to parse chunk: {}", chunk.chunk_str));
            chunks.push(chunk);
        }
        Ok(())
    }

    fn find(&self, block: u64) -> Option<DataChunk> {
        let chunks = self.chunks.read();
        if block < chunks.first()?.first_block() as u64 {
            return None;
        }
        let first_suspect = chunks.partition_point(|chunk| (chunk.last_block() as u64) < block);
        (first_suspect < chunks.len()).then(|| chunks[first_suspect].clone())
    }
}
