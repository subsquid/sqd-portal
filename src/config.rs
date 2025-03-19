use serde::Deserialize;
use serde_with::serde_derive::Serialize;
use serde_with::{serde_as, DurationSeconds};
use std::collections::BTreeMap;
use std::time::Duration;
use url::Url;

use sqd_hotblocks::{self, RetentionStrategy};
use sqd_primitives::BlockNumber;

use crate::types::DatasetRef;


#[serde_as]
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(deserialize_with = "parse_hostname")]
    pub hostname: String,

    #[serde(default = "default_worker_versions")]
    pub worker_versions: semver::VersionReq,

    #[serde(default = "default_max_parallel_streams")]
    pub max_parallel_streams: usize,

    pub max_chunks_per_stream: Option<usize>,

    #[serde_as(as = "DurationSeconds")]
    #[serde(
        rename = "worker_inactive_threshold_sec",
        default = "default_worker_inactive_threshold"
    )]
    pub worker_inactive_threshold: Duration,

    #[serde_as(as = "DurationSeconds")]
    #[serde(
        rename = "transport_timeout_sec",
        default = "default_transport_timeout"
    )]
    pub transport_timeout: Duration,

    #[serde(default = "default_default_buffer_size")]
    pub default_buffer_size: usize,

    #[serde(default = "default_max_buffer_size")]
    pub max_buffer_size: usize,

    #[serde(default = "default_default_retries")]
    pub default_retries: u8,

    #[serde(default = "default_default_timeout_quantile")]
    pub default_timeout_quantile: f32,

    #[serde_as(as = "DurationSeconds")]
    #[serde(
        rename = "chain_update_interval_sec",
        default = "default_chain_update_interval"
    )]
    pub chain_update_interval: Duration,

    #[serde_as(as = "DurationSeconds")]
    #[serde(
        rename = "assignments_update_interval_sec",
        default = "default_assignments_update_interval"
    )]
    pub assignments_update_interval: Duration,

    #[serde_as(as = "DurationSeconds")]
    #[serde(
        rename = "datasets_update_interval_sec",
        default = "default_datasets_update_interval"
    )]
    pub datasets_update_interval: Duration,

    pub sqd_network: SqdNetworkConfig,

    #[serde(default)]
    pub datasets: DatasetsConfig,

    pub hotblocks_db_path: Option<String>,

    #[serde(default = "default_hotblocks_data_cache_mb")]
    pub hotblocks_data_cache_mb: usize,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqdNetworkConfig {
    #[serde(rename = "datasets")]
    pub datasets_url: String,

    #[serde(default)]
    pub serve: ServeMode,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ServeMode {
    #[default]
    All,
}

pub type DatasetsConfig = BTreeMap<String, DatasetConfigModel>;

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct DatasetConfigModel {
    pub aliases: Vec<String>,
    pub sqd_network: Option<DatasetRef>,
    pub real_time: Option<RealTimeConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RealTimeConfig {
    pub kind: sqd_hotblocks::DatasetKind,
    pub data_sources: Vec<Url>,
    #[serde(deserialize_with = "parse_retention")]
    pub retention: RetentionStrategy,
}

impl Config {
    pub fn read(config_path: &str) -> anyhow::Result<Self> {
        let file = std::fs::File::open(config_path)?;
        let buf_reader = std::io::BufReader::new(file);
        let deser = serde_yaml::Deserializer::from_reader(buf_reader);
        Ok(serde_yaml::with::singleton_map_recursive::deserialize(
            deser,
        )?)
    }
}


fn default_worker_versions() -> semver::VersionReq {
    semver::VersionReq::parse("^2.0.0").unwrap()
}

fn default_max_parallel_streams() -> usize {
    1024
}

fn default_worker_inactive_threshold() -> Duration {
    Duration::from_secs(120)
}

fn default_transport_timeout() -> Duration {
    Duration::from_secs(60)
}

fn default_default_buffer_size() -> usize {
    10
}

fn default_max_buffer_size() -> usize {
    100
}

fn default_default_retries() -> u8 {
    3
}

fn default_default_timeout_quantile() -> f32 {
    0.5
}

fn default_chain_update_interval() -> Duration {
    Duration::from_secs(60)
}

fn default_assignments_update_interval() -> Duration {
    Duration::from_secs(60)
}

fn default_datasets_update_interval() -> Duration {
    Duration::from_secs(10 * 60)
}

fn default_hotblocks_data_cache_mb() -> usize {
    4096
}


fn parse_hostname<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Ok(s.trim_end_matches('/').to_owned())
}

fn parse_retention<'de, D>(de: D) -> Result<RetentionStrategy, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum Model {
        FromBlock(BlockNumber),
        Head(BlockNumber),
    }

    let model = Model::deserialize(de)?;
    match model {
        Model::FromBlock(block) => Ok(RetentionStrategy::FromBlock(block)),
        Model::Head(block) => Ok(RetentionStrategy::Head(block)),
    }
}
