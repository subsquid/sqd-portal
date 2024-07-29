use std::{collections::HashMap, str::FromStr};

use aws_sdk_s3 as s3;
use subsquid_datasets::DatasetStorage;
use subsquid_messages::data_chunk::DataChunk;

use crate::types::DatasetId;

// TODO
const S3_ENDPOINT: &str = "https://7a28e49ec5f4a60c66f216392792ac38.r2.cloudflarestorage.com";

pub struct StorageClient {
    datasets: HashMap<DatasetId, Dataset>,
}

impl StorageClient {
    pub async fn new(buckets: impl IntoIterator<Item = impl AsRef<str>>) -> anyhow::Result<Self> {
        let s3_config = aws_config::from_env()
            .endpoint_url(S3_ENDPOINT)
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
            )
            .await?;
            datasets.insert(DatasetId::from_url(bucket.as_ref()), dataset);
        }
        Ok(Self { datasets })
    }

    pub fn find_chunk(&self, dataset: &DatasetId, block: u64) -> Option<DataChunk> {
        self.datasets.get(dataset)?.find(block).cloned()
    }

    pub fn next_chunk(&self, dataset: &DatasetId, chunk: &DataChunk) -> Option<DataChunk> {
        self.datasets
            .get(dataset)?
            .find(chunk.last_block() as u64 + 1)
            .cloned()
    }
}

struct Dataset {
    chunks: Vec<DataChunk>,
}

impl Dataset {
    async fn new(bucket: &str, s3_client: aws_sdk_s3::Client) -> anyhow::Result<Self> {
        let mut storage = DatasetStorage::new(bucket, s3_client);
        let chunks = storage
            .list_all_new_chunks()
            .await?
            .into_iter()
            .map(|chunk| {
                DataChunk::from_str(&chunk.chunk_str)
                    .unwrap_or_else(|_| panic!("Failed to parse chunk: {}", chunk.chunk_str))
            })
            .collect();
        Ok(Self { chunks })
    }

    fn find(&self, block: u64) -> Option<&DataChunk> {
        if block < self.chunks.first()?.first_block() as u64 {
            return None;
        }
        let first_suspect = self
            .chunks
            .partition_point(|chunk| (chunk.last_block() as u64) < block);
        (first_suspect < self.chunks.len()).then(|| &self.chunks[first_suspect])
    }
}
