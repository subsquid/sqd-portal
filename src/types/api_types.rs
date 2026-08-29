use std::collections::{BTreeMap, HashMap};

use serde::Serialize;
use sqd_contract_client::PeerId;
use sqd_messages::RangeSet;

use crate::{datasets::DatasetConfig, network};

use super::{BlockNumber, DatasetId};

#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct AvailableDatasetApiResponse {
    ///The default name used to reference this dataset (e.g., ethereum-mainnet).
    pub dataset: String,
    /// Alternative names for the dataset.
    pub aliases: Vec<String>,
    /// Indicates if the dataset has real-time data.
    pub real_time: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<u64>)]
    /// The block number of the first known block.
    pub start_block: Option<BlockNumber>,
    /// Additional metadata fields, present only when requested via expand[]
    #[serde(flatten)]
    #[schema(additional_properties, value_type = Object)]
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

/// One chunk's holder set as `pick` currently sees it.
#[derive(Serialize)]
pub struct ChunkHealth {
    pub chunk: String,
    pub first_block: BlockNumber,
    pub last_block: BlockNumber,
    #[serde(flatten)]
    pub holders: network::HolderSummary,
}

/// A block range's worth of [`ChunkHealth`], summarised.
///
/// The per-chunk list is deliberately not returned in full: a mainnet dataset runs to
/// tens of thousands of chunks, and the question this answers — which of them the portal
/// cannot serve — needs a histogram and the tail, not the body.
#[derive(Serialize)]
pub struct ChunkHealthReport {
    pub dataset: String,
    pub first_block: BlockNumber,
    pub last_block: BlockNumber,
    pub chunks_scanned: usize,
    /// The scan hit `limit` before reaching `to`. Resume from `last_block + 1`.
    pub truncated: bool,
    /// Chunk count keyed by how many holders `pick` could use, so a leading `"0"`
    /// or `"1"` entry is the whole finding.
    pub available_holders: BTreeMap<usize, usize>,
    /// Chunk count keyed by how many holders answered and never with an `ok` —
    /// holders that are in the assignment but not on the network.
    pub never_ok_holders: BTreeMap<usize, usize>,
    /// Fewest available holders first: the chunks a stream will stall on.
    pub worst: Vec<ChunkHealth>,
}

#[derive(Serialize)]
pub struct WorkerDebugInfo {
    pub peer_id: PeerId,
    pub priority: network::Priority,
    /// Flattened so the existing two keys keep their place in the response.
    #[serde(flatten)]
    pub health: network::WorkerHealth,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The health fields are flattened onto the existing object rather than nested, so
    /// `/debug/workers` readers and scripts that already key off `peer_id` and
    /// `priority` keep working. A nesting change would break them silently.
    #[test]
    fn worker_debug_info_keeps_its_existing_keys() {
        let info = WorkerDebugInfo {
            peer_id: PeerId::random(),
            priority: (network::PriorityGroup::Unavailable, 0, 0),
            health: network::WorkerHealth {
                blocked_by: vec!["timeouts"],
                running_queries: 0,
                backoff_secs: None,
                server_errors_cooldown_secs: None,
                timeouts_cooldown_secs: Some(742.5),
                last_throughput: None,
                last_verdict: Some("transport_error"),
                last_verdict_secs_ago: Some(157.5),
                last_ok_secs_ago: None,
                never_ok: true,
            },
        };

        let json = serde_json::to_value(&info).unwrap();
        let object = json.as_object().expect("a worker is a JSON object");

        assert!(object.contains_key("peer_id"));
        assert_eq!(object["priority"][0], "Unavailable");
        // Flattened, not nested under a "health" key.
        assert!(!object.contains_key("health"));
        assert_eq!(object["blocked_by"], serde_json::json!(["timeouts"]));
        assert_eq!(object["last_verdict"], "transport_error");
        assert_eq!(object["never_ok"], true);
    }
}
