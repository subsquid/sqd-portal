use std::collections::HashMap;

use serde::Serialize;
use sqd_contract_client::PeerId;
use sqd_messages::RangeSet;

use crate::{datasets::DatasetConfig, network};

use super::{BlockNumber, DatasetId};

#[derive(serde::Serialize)]
pub(crate) struct AvailableDatasetApiResponse {
    pub dataset: String,
    pub aliases: Vec<String>,
    pub real_time: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_block: Option<BlockNumber>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

impl AvailableDatasetApiResponse {
    pub fn new(config: DatasetConfig, start_block: impl Into<Option<BlockNumber>>) -> Self {
        let extra = match config.metadata {
            serde_json::Value::Object(map) => map,
            _ => serde_json::Map::new(),
        };
        Self {
            dataset: config.default_name,
            aliases: config.aliases,
            real_time: config.hotblocks.is_some(),
            start_block: start_block.into(),
            extra,
        }
    }

    pub fn with_fields(mut self, fields: &[String]) -> Self {
        if fields.is_empty() {
            self.extra.clear();
        } else {
            self.extra.retain(|k, _| fields.contains(k));
        }
        self
    }
}

impl From<DatasetConfig> for AvailableDatasetApiResponse {
    fn from(config: DatasetConfig) -> Self {
        Self::new(config, None)
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

#[derive(Default, Serialize)]
pub struct DatasetState {
    pub worker_ranges: HashMap<PeerId, RangeSet>,
}
