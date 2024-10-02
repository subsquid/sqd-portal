use std::collections::HashMap;

use parking_lot::{Mutex, RwLock};
use serde::{de::DeserializeOwned, Deserialize};
use sqd_contract_client::Network;

use crate::{
    metrics,
    types::{BlockRange, DatasetId},
};

pub struct StorageClient {
    datasets: RwLock<HashMap<DatasetId, Vec<BlockRange>>>,
    fetcher: HttpFetcher,
}

impl StorageClient {
    pub fn new(network: Network) -> anyhow::Result<Self> {
        let chunks_url = format!(
            "https://metadata.sqd-datasets.io/datasets_{}.json.gz",
            network_string(network)
        );
        Ok(Self {
            datasets: Default::default(),
            fetcher: HttpFetcher::new(chunks_url)?,
        })
    }

    pub async fn update(&self) {
        tracing::info!("Updating known chunks");
        let chunks_summary: ChunksSummary = match self.fetcher.fetch().await {
            Ok(FetchResult::Data(chunks)) => chunks,
            Ok(FetchResult::NotModified) => {
                tracing::debug!("Chunk list has not been modified");
                return;
            }
            Err(e) => {
                tracing::warn!("Failed to fetch chunks list: {e:?}");
                return;
            }
        };

        let timer = tokio::time::Instant::now();
        let new_datasets: Vec<_> = chunks_summary
            .chunks
            .into_iter()
            .map(|(dataset_url, chunks)| {
                let dataset_id = DatasetId::from_url(dataset_url);
                let mut chunks: Vec<_> = chunks
                    .into_iter()
                    .map(|chunk| chunk.begin..=chunk.end)
                    .collect();
                chunks.sort_by_key(|r| *r.start());
                (dataset_id, chunks)
            })
            .collect();

        let mut datasets = self.datasets.write();

        for (dataset, chunks) in new_datasets {
            let new_len = chunks.len();
            let last_block = chunks.last().map(|r| *r.end()).unwrap_or(0);
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

    pub fn find_chunk(&self, dataset: &DatasetId, block: u64) -> Option<BlockRange> {
        let datasets = self.datasets.read();
        let chunks = datasets.get(dataset)?;
        if block < *chunks.first()?.start() {
            return None;
        }
        let first_suspect = chunks.partition_point(|chunk| (*chunk.end()) < block);
        (first_suspect < chunks.len()).then(|| chunks[first_suspect].clone())
    }

    pub fn next_chunk(&self, dataset: &DatasetId, chunk: &BlockRange) -> Option<BlockRange> {
        self.find_chunk(dataset, chunk.end() + 1)
    }
}
struct HttpFetcher {
    url: String,
    http_client: reqwest::Client,
    last_etag: Mutex<Option<String>>,
}

enum FetchResult<T> {
    NotModified,
    Data(T),
}

impl HttpFetcher {
    pub fn new(url: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            url: url.into(),
            http_client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            last_etag: Default::default(),
        })
    }

    pub async fn fetch<T: DeserializeOwned>(&self) -> anyhow::Result<FetchResult<T>> {
        let mut request = self.http_client.get(&self.url);
        if let Some(etag) = self.last_etag.lock().as_ref() {
            request = request.header(reqwest::header::IF_NONE_MATCH, etag);
        }
        let response = request.send().await?.error_for_status()?;
        if response.status() == reqwest::StatusCode::NOT_MODIFIED {
            return Ok(FetchResult::NotModified);
        }
        let etag = response
            .headers()
            .get(reqwest::header::ETAG)
            .ok_or(anyhow::anyhow!("No etag in HTTP response"))?
            .to_str()
            .ok()
            .map(|s| s.to_owned());
        *self.last_etag.lock() = etag;

        let bytes = response.bytes().await?;
        let decoder = flate2::bufread::GzDecoder::new(&*bytes);
        let value = serde_json::from_reader(decoder)?;

        Ok(FetchResult::Data(value))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChunkStatus {
    pub begin: u64,
    pub end: u64,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct ChunksSummary {
    chunks: HashMap<String, Vec<ChunkStatus>>,
}

fn network_string(network: Network) -> &'static str {
    match network {
        Network::Mainnet => "mainnet",
        Network::Tethys => "tethys",
    }
}
