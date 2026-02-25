use serde::Deserialize;
use serde_with::serde_derive::Serialize;
use serde_with::{serde_as, DurationSeconds};
use std::collections::BTreeMap;
use std::time::Duration;
use url::Url;

use crate::network::PrioritiesConfig;
use crate::types::DatasetRef;

#[serde_as]
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(deserialize_with = "parse_hostname")]
    pub hostname: String,

    #[serde(default = "default_max_parallel_streams")]
    pub max_parallel_streams: usize,

    pub max_chunks_per_stream: Option<usize>,

    #[serde_as(as = "DurationSeconds")]
    #[serde(
        rename = "transport_timeout_sec",
        default = "default_transport_timeout"
    )]
    pub transport_timeout: Duration,

    #[serde_as(as = "DurationSeconds")]
    #[serde(
        rename = "pre_drain_grace_period_sec",
        default = "default_pre_drain_grace_period"
    )]
    pub pre_drain_grace_period: Duration,

    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "drain_timeout_sec", default = "default_drain_timeout")]
    pub drain_timeout: Duration,

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

    #[serde(default = "default_assignments_url")]
    pub assignments_url: String,

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

    #[serde(default)]
    pub priorities: PrioritiesConfig,

    #[serde(default = "default_true")]
    pub send_logs: bool,

    #[serde(default = "default_true")]
    pub verify_worker_responses: bool,

    #[serde(default)]
    pub skip_parent_hash_validation: bool,

    #[serde(default)]
    pub use_gzjoin: bool,

    #[serde(default)]
    pub ignore_deprecated_workers: bool,

    /// Please avoid overriding this value. It may eventually become unsupported.
    #[serde(default = "default_query_size_limit")]
    pub query_size_limit: u64,

    #[serde(default)]
    pub congestion: CongestionConfig,

    #[serde(default = "default_sentry_dsn")]
    pub sentry_dsn: String,

    #[serde(default = "default_sentry_sampling_rate")]
    pub sentry_sampling_rate: f32,

    #[serde(default = "default_true")]
    pub sentry_is_enabled: bool,

    pub client_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqdNetworkConfig {
    #[serde(rename = "datasets")]
    pub datasets_url: String,

    #[serde(rename = "metadata")]
    pub metadata_url: Option<String>,

    #[serde(default)]
    pub serve: ServeMode,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ServeMode {
    #[default]
    All,
    Manual,
}

pub type DatasetsConfig = BTreeMap<String, DatasetConfigModel>;

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct DatasetConfigModel {
    pub aliases: Vec<String>,
    pub sqd_network: Option<DatasetRef>,
    pub real_time: Option<RealTimeConfig>,
    pub kind: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RealTimeConfig {
    pub url: Url,
    // By default use the dataset name as in the config key
    pub dataset: Option<String>,
    // If set, queries that don't require traces or statediffs are routed here
    pub dataset_traceless: Option<String>,
    // By default use the kind in config
    pub kind: Option<String>,
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

fn default_true() -> bool {
    true
}

fn default_max_parallel_streams() -> usize {
    1024
}

fn default_transport_timeout() -> Duration {
    Duration::from_secs(60)
}

// Graceful shutdown defaults. See docs/GRACEFUL_SHUTDOWN.md for the full
// lifecycle and timing rationale.
//
// pre_drain_grace_period: window during which /ready returns 503 before we
// start refusing connections — lets upstream load balancers stop routing new
// traffic to this instance.
// drain_timeout: hard cap on waiting for in-flight requests to complete.
// Total shutdown budget = pre_drain_grace_period + drain_timeout. The
// orchestrator's kill timeout must exceed it (plus a few seconds for
// network-client wind-down and Sentry flush).
fn default_pre_drain_grace_period() -> Duration {
    Duration::from_secs(25)
}

fn default_drain_timeout() -> Duration {
    Duration::from_secs(25)
}

fn default_default_buffer_size() -> usize {
    10
}

fn default_max_buffer_size() -> usize {
    1000
}

fn default_default_retries() -> u8 {
    7
}

fn default_default_timeout_quantile() -> f32 {
    0.5
}

fn default_chain_update_interval() -> Duration {
    Duration::from_secs(60)
}

fn default_assignments_url() -> String {
    String::from("https://metadata.sqd-datasets.io")
}

fn default_assignments_update_interval() -> Duration {
    Duration::from_secs(60)
}

fn default_datasets_update_interval() -> Duration {
    Duration::from_secs(10 * 60)
}

fn default_query_size_limit() -> u64 {
    sqd_network_transport::protocol::MAX_RAW_QUERY_SIZE
}

fn default_sentry_sampling_rate() -> f32 {
    0.01
}

fn default_sentry_dsn() -> String {
    "https://b74e352d92a89dc36c3e6064284669af@o1149243.ingest.us.sentry.io/4510617125191680".into()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CongestionConfig {
    pub min_window: u32,
    pub max_window: u32,
    pub decrease_factor: f64,
    pub speculative_fraction: f64,
    pub min_shrink_interval_ms: u64,
    pub read_timeout_sec: u64,
    pub headroom_threshold: f64,
    pub priority_stride: u64,
    pub enabled: bool,
}

impl Default for CongestionConfig {
    fn default() -> Self {
        Self {
            min_window: 10,
            max_window: 500,
            decrease_factor: 0.75,
            speculative_fraction: 0.1,
            min_shrink_interval_ms: 2000,
            read_timeout_sec: 1,
            headroom_threshold: 0.95,
            priority_stride: 100,
            enabled: true,
        }
    }
}

fn parse_hostname<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Ok(s.trim_end_matches('/').to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL_YAML: &str = r#"
hostname: portal.example
sqd_network:
  datasets: https://example.invalid/datasets.yaml
"#;

    #[test]
    fn shutdown_durations_default_when_omitted() {
        let config: Config = serde_yaml::from_str(MINIMAL_YAML).expect("parse");
        assert_eq!(config.pre_drain_grace_period, Duration::from_secs(25));
        assert_eq!(config.drain_timeout, Duration::from_secs(25));
    }

    #[test]
    fn shutdown_durations_can_be_overridden() {
        let yaml = format!("{MINIMAL_YAML}pre_drain_grace_period_sec: 3\ndrain_timeout_sec: 7\n");
        let config: Config = serde_yaml::from_str(&yaml).expect("parse");
        assert_eq!(config.pre_drain_grace_period, Duration::from_secs(3));
        assert_eq!(config.drain_timeout, Duration::from_secs(7));
    }
}
