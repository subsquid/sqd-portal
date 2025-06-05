use serde::Serialize;
use sqd_contract_client::PeerId;

use crate::{datasets::DatasetConfig, network};

use super::{BlockNumber, DatasetId};

#[derive(serde::Serialize)]
pub(crate) struct AvailableDatasetApiResponse {
    pub dataset: String,
    pub aliases: Vec<String>,
    pub real_time: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_block: Option<BlockNumber>,
}

impl AvailableDatasetApiResponse {
    pub fn new(metadata: DatasetConfig, start_block: impl Into<Option<BlockNumber>>) -> Self {
        Self {
            dataset: metadata.default_name,
            aliases: metadata.aliases,
            real_time: metadata.hotblocks.is_some(),
            start_block: start_block.into(),
        }
    }
}

impl From<DatasetConfig> for AvailableDatasetApiResponse {
    fn from(metadata: DatasetConfig) -> Self {
        Self {
            dataset: metadata.default_name,
            aliases: metadata.aliases,
            real_time: metadata.hotblocks.is_some(),
            start_block: None,
        }
    }
}

#[derive(Serialize)]
pub struct WorkerDebugInfo {
    pub peer_id: PeerId,
    pub priority: network::Priority,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum DatasetRef {
    #[serde(rename = "dataset_name")]
    Name(String),
    #[serde(rename = "dataset_id")]
    Id(DatasetId),
}
