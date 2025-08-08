use std::{collections::HashMap, io::ErrorKind, sync::Arc, time::Duration};

use anyhow::anyhow;
use sqd_assignments::Assignment;
use sqd_contract_client::{Network, PeerId};
use sqd_primitives::BlockRef;
use tracing::instrument;

use crate::{
    datasets::Datasets,
    metrics,
    types::{api_types::DatasetState, BlockNumber, DataChunk, DatasetId},
    utils::{Mutex, RwLock},
};

type HeadUpdateCallback = Box<dyn FnMut(BlockRef) + Sync + Send>;

pub struct StorageClient {
    assignment: RwLock<Option<Assignment>>,
    datasets_config: Arc<RwLock<Datasets>>,
    latest_assignment_id: RwLock<Option<String>>,
    network_state_url: String,
    reqwest_client: reqwest::Client,
    head_update_subscribers: Mutex<HashMap<DatasetId, HeadUpdateCallback>>,
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
    pub fn new(datasets_config: Arc<RwLock<Datasets>>, network: Network) -> Self {
        let network_state_filename = match network {
            Network::Tethys => "network-state-tethys.json",
            Network::Mainnet => "network-state-mainnet.json",
        };
        let network_state_url =
            format!("https://metadata.sqd-datasets.io/{network_state_filename}");
        Self {
            assignment: RwLock::new(None, "StorageClient::assignment"),
            datasets_config,
            latest_assignment_id: RwLock::new(None, "StorageClient::latest_assignment"),
            network_state_url,
            reqwest_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .unwrap(),
            head_update_subscribers: Mutex::new(
                Default::default(),
                "StorageClient::head_update_subscribers",
            ),
        }
    }

    pub fn has_assignment(&self) -> bool {
        self.assignment.read().is_some()
    }

    pub async fn try_update_assignment(&self) {
        match self.update_assignment().await {
            Ok(_) => {}
            Err(err) => {
                tracing::error!(error = ?err, "Failed to update assignment, waiting for the next one");
            }
        }
    }

    async fn update_assignment(&self) -> anyhow::Result<()> {
        tracing::debug!("Checking for new assignment");
        let network_state = self.fetch_network_state().await?;
        let assignment_id = network_state.assignment.id;
        if self.latest_assignment_id.read().as_ref() == Some(&assignment_id) {
            tracing::debug!("Assignment has not been changed");
            return Ok(());
        }

        let assignment = self
            .fetch_assignment(
                &network_state
                    .assignment
                    .fb_url
                    .ok_or(anyhow!("Missing fb_url"))?,
            )
            .await?;

        sleep_until(network_state.assignment.effective_from).await;

        self.set_assignment(assignment, &assignment_id);

        tracing::info!("Applied assignment \"{}\"", assignment_id);
        Ok(())
    }

    async fn fetch_network_state(&self) -> anyhow::Result<sqd_messages::assignments::NetworkState> {
        let response = self
            .reqwest_client
            .get(&self.network_state_url)
            .send()
            .await?;
        let network_state = response.json().await?;
        Ok(network_state)
    }

    async fn fetch_assignment(&self, url: &str) -> anyhow::Result<Assignment> {
        use async_compression::tokio::bufread::GzipDecoder;
        use futures::TryStreamExt;
        use tokio::io::AsyncReadExt;
        use tokio_util::io::StreamReader;

        let response = self.reqwest_client.get(url).send().await?;
        let stream = response.bytes_stream();
        let reader =
            StreamReader::new(stream.map_err(|e| std::io::Error::new(ErrorKind::Other, e)));
        let mut buf = Vec::new();
        let mut decoder = GzipDecoder::new(reader);
        decoder
            .read_to_end(&mut buf)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to decompress assignment: {}", e))?;

        tracing::debug!("Downloaded assignment from {}", url);

        Ok(Assignment::from_owned_unchecked(buf))
    }

    #[instrument(skip_all)]
    fn set_assignment(&self, assignment: Assignment, id: &str) {
        *self.latest_assignment_id.write() = Some(id.to_owned());

        let prev = self.assignment.read();
        for dataset in assignment.datasets().iter() {
            let dataset_id = DatasetId::from_url(dataset.id());
            let new_len = dataset.chunks().len();
            let last_block = dataset.last_block();
            let prev_ds = prev.as_ref().and_then(|p| p.get_dataset(dataset.id()));
            let old_len = prev_ds.map_or(0, |p| p.chunks().len());
            if old_len < new_len {
                tracing::info!(
                    "Got {} new chunk(s) for dataset {}",
                    new_len - old_len,
                    dataset_id,
                );
            }

            if let Some(callback) = self.head_update_subscribers.lock().get_mut(&dataset_id) {
                if let Some(hash) = dataset.last_block_hash() {
                    callback(BlockRef {
                        number: dataset.last_block(),
                        hash: hash.to_owned(),
                    });
                } else {
                    tracing::warn!(
                        "Dataset {} has no last block hash, cannot update head",
                        dataset_id
                    );
                }
            }

            let dataset_name = self
                .datasets_config
                .read()
                .default_name(&dataset_id)
                .map(ToOwned::to_owned);
            metrics::report_chunk_list_updated(&dataset_id, dataset_name, new_len, last_block);
        }
        drop(prev);

        crate::metrics::KNOWN_WORKERS.set(assignment.workers().len() as i64);

        *self.assignment.write() = Some(assignment);
    }

    pub fn find_chunk(&self, dataset: &DatasetId, block: u64) -> Result<DataChunk, ChunkNotFound> {
        let dataset = dataset.to_url();
        let guard = self.assignment.read();
        let assignment = guard.as_ref().ok_or(ChunkNotFound::UnknownDataset)?;
        let chunk = assignment
            .find_chunk(dataset, block)
            .map_err(|e| convert_chunk_not_found(e, assignment, dataset))?;
        Ok(chunk.id().parse().map_err(|e| {
            tracing::warn!(error = %e, "Failed to parse chunk ID");
            ChunkNotFound::InvalidID(chunk.id().to_string())
        })?)
    }

    pub fn find_workers(
        &self,
        dataset: &DatasetId,
        block: u64,
    ) -> Result<Vec<PeerId>, ChunkNotFound> {
        let dataset = dataset.to_url();
        let guard = self.assignment.read();
        let assignment = guard.as_ref().ok_or(ChunkNotFound::UnknownDataset)?;
        let chunk = assignment
            .find_chunk(dataset, block)
            .map_err(|e| convert_chunk_not_found(e, assignment, dataset))?;
        Ok(chunk
            .worker_indexes()
            .iter()
            .filter_map(|idx| match assignment.get_worker_id(idx) {
                Ok(id) => Some(id),
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to parse worker ID #{}", idx);
                    None
                }
            })
            .collect())
    }

    pub fn next_chunk(&self, dataset: &DatasetId, chunk: &DataChunk) -> Option<DataChunk> {
        self.find_chunk(dataset, chunk.last_block + 1).ok()
    }

    pub fn first_block(&self, dataset: &DatasetId) -> Option<BlockNumber> {
        Some(
            self.assignment
                .read()
                .as_ref()?
                .get_dataset(dataset.to_url())?
                .first_block(),
        )
    }

    pub fn head(&self, dataset: &DatasetId) -> Option<BlockRef> {
        let guard = self.assignment.read();
        let dataset = guard.as_ref()?.get_dataset(dataset.to_url())?;
        dataset.last_block_hash().map(|hash| BlockRef {
            hash: hash.to_owned(),
            number: dataset.last_block(),
        })
    }

    pub fn get_all_workers(&self) -> Vec<PeerId> {
        let guard = self.assignment.read();
        guard
            .as_ref()
            .map(|a| {
                a.workers()
                    .iter()
                    .filter_map(|w| (*w.worker_id()).try_into().ok())
                    .collect()
            })
            .unwrap_or_default()
    }

    #[instrument(skip(self))]
    pub fn get_dataset_state(&self, dataset: &DatasetId) -> Option<DatasetState> {
        let mut ranges: HashMap<_, Vec<_>> = HashMap::new();
        let guard = self.assignment.read();
        let assignment = guard.as_ref()?;
        for c in assignment.get_dataset(dataset.to_url())?.chunks().iter() {
            let range = c.id().parse::<DataChunk>().unwrap().range_msg();
            for idx in c.worker_indexes() {
                let peer_id = match assignment.get_worker_id(idx) {
                    Ok(peer_id) => peer_id,
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to parse worker ID #{}", idx);
                        continue;
                    }
                };
                ranges.entry(peer_id).or_default().push(range)
            }
        }
        Some(DatasetState {
            worker_ranges: ranges
                .into_iter()
                .map(|(peer_id, ranges)| (peer_id, sqd_messages::RangeSet::from(ranges)))
                .collect(),
        })
    }

    pub fn subscribe_head_updates(&self, dataset_id: &DatasetId, callback: HeadUpdateCallback) {
        self.head_update_subscribers
            .lock()
            .insert(dataset_id.clone(), callback);
    }

    pub fn unsubscribe_head_updates(&self) {
        self.head_update_subscribers.lock().clear();
    }
}

fn convert_chunk_not_found(
    e: sqd_assignments::ChunkNotFound,
    assignment: &Assignment,
    dataset: &str,
) -> ChunkNotFound {
    match e {
        sqd_assignments::ChunkNotFound::AfterLast => ChunkNotFound::AfterLast,
        sqd_assignments::ChunkNotFound::BeforeFirst => ChunkNotFound::BeforeFirst {
            first_block: assignment.get_dataset(dataset).unwrap().first_block(),
        },
        sqd_assignments::ChunkNotFound::UnknownDataset => ChunkNotFound::UnknownDataset,
    }
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
