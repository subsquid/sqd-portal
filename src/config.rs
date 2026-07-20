use serde::Deserialize;
use serde_with::serde_derive::Serialize;
use serde_with::{serde_as, DurationMilliSeconds, DurationSeconds};
use std::collections::BTreeMap;
use std::time::Duration;
use url::Url;

use crate::network::PrioritiesConfig;
use crate::types::DatasetRef;

#[serde_as]
#[derive(Debug, Clone, Deserialize)]
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
        rename = "hotblocks_read_timeout_sec",
        default = "default_hotblocks_read_timeout"
    )]
    pub hotblocks_read_timeout: Duration,

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

    // Values above 1 enable eager partial continuations with bounded per-chunk buffering.
    // A value of 1 effectively keeps continuation requests lazy.
    #[serde(default = "default_max_stored_results_per_chunk")]
    pub max_stored_results_per_chunk: usize,

    #[serde(default = "default_default_retries")]
    pub default_retries: u8,

    #[serde(default = "default_default_timeout_quantile")]
    pub default_timeout_quantile: f32,

    /// Backoff applied to a worker that reports overload or rate limiting
    /// without specifying an explicit `retry_after_ms`.
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    #[serde(
        rename = "default_worker_backoff_ms",
        default = "default_default_worker_backoff"
    )]
    pub default_worker_backoff: Duration,

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

    /// P-ASSIGNMENT-MAX-AGE (ADR-013). Signalling only: past this the artifact
    /// is reported stale, but the portal keeps serving from it.
    #[serde_as(as = "DurationSeconds")]
    #[serde(
        rename = "assignment_max_age_sec",
        default = "default_assignment_max_age"
    )]
    pub assignment_max_age: Duration,

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
#[serde(default)]
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
        let mut warn_unknown = |path: serde_ignored::Path| {
            tracing::warn!("ignoring unknown config field: {path}");
        };
        let config: Self = serde_yaml::with::singleton_map_recursive::deserialize(
            serde_ignored::Deserializer::new(deser, &mut warn_unknown),
        )?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> anyhow::Result<()> {
        self.congestion.validate()?;
        Ok(())
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

// Must stay below caller-side request timeouts (SDK default: 30s), so a stalled
// upstream surfaces as our 502 rather than the caller's own timeout — a request
// still in flight has no recorded status and is invisible in metrics.
fn default_hotblocks_read_timeout() -> Duration {
    Duration::from_secs(20)
}

// Graceful shutdown defaults. See spec/decisions/ADR-005-two-phase-shutdown.md for the
// two-phase shutdown decision, lifecycle, and timing rationale.
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

fn default_max_stored_results_per_chunk() -> usize {
    2
}

fn default_default_retries() -> u8 {
    1
}

fn default_default_timeout_quantile() -> f32 {
    0.5
}

fn default_default_worker_backoff() -> Duration {
    Duration::from_millis(1000)
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

fn default_assignment_max_age() -> Duration {
    Duration::from_secs(15 * 60)
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
    pub min_shrink_interval_ms: u64,
    pub read_timeout_sec: u64,
    pub headroom_threshold: f64,
    pub priority_stride: u32,
    pub enabled: bool,
}

impl CongestionConfig {
    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.min_window >= 1,
            "congestion.min_window must be >= 1, got {}",
            self.min_window
        );
        anyhow::ensure!(
            self.min_window <= self.max_window,
            "congestion.min_window ({}) must not exceed congestion.max_window ({})",
            self.min_window,
            self.max_window
        );
        anyhow::ensure!(
            self.decrease_factor > 0.0 && self.decrease_factor < 1.0,
            "congestion.decrease_factor must be in (0.0, 1.0), got {}",
            self.decrease_factor
        );
        anyhow::ensure!(
            self.headroom_threshold > 0.0 && self.headroom_threshold <= 1.0,
            "congestion.headroom_threshold must be in (0.0, 1.0], got {}",
            self.headroom_threshold
        );
        anyhow::ensure!(
            self.read_timeout_sec >= 1,
            "congestion.read_timeout_sec must be >= 1, got {}",
            self.read_timeout_sec
        );
        anyhow::ensure!(
            self.priority_stride >= 1,
            "congestion.priority_stride must be >= 1, got {}",
            self.priority_stride
        );
        Ok(())
    }
}

impl Default for CongestionConfig {
    fn default() -> Self {
        Self {
            min_window: 10,
            max_window: 500,
            decrease_factor: 0.75,
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
    fn unknown_fields_are_reported_not_rejected() {
        let yaml = format!(
            "{MINIMAL_YAML}unknown_top_level: 123\n\
             congestion:\n  min_window: 5\n  bogus_nested: true\n"
        );
        let deser = serde_yaml::Deserializer::from_str(&yaml);
        let mut ignored = Vec::new();
        let config: Config = serde_yaml::with::singleton_map_recursive::deserialize(
            serde_ignored::Deserializer::new(deser, &mut |path: serde_ignored::Path| {
                ignored.push(path.to_string());
            }),
        )
        .expect("parse");

        // Known fields still deserialize correctly alongside the unknown ones.
        assert_eq!(config.congestion.min_window, 5);
        // Both the top-level and the nested unknown field are reported with paths.
        assert!(
            ignored.contains(&"unknown_top_level".to_string()),
            "missing top-level unknown field, got {ignored:?}"
        );
        assert!(
            ignored.contains(&"congestion.bogus_nested".to_string()),
            "missing nested unknown field, got {ignored:?}"
        );
    }

    #[test]
    fn shutdown_durations_default_when_omitted() {
        let config: Config = serde_yaml::from_str(MINIMAL_YAML).expect("parse");
        assert_eq!(config.pre_drain_grace_period, Duration::from_secs(25));
        assert_eq!(config.drain_timeout, Duration::from_secs(25));
    }

    #[test]
    fn max_stored_results_per_chunk_defaults_to_two() {
        let config: Config = serde_yaml::from_str(MINIMAL_YAML).expect("parse");
        assert_eq!(config.max_stored_results_per_chunk, 2);
    }

    #[test]
    fn shutdown_durations_can_be_overridden() {
        let yaml = format!("{MINIMAL_YAML}pre_drain_grace_period_sec: 3\ndrain_timeout_sec: 7\n");
        let config: Config = serde_yaml::from_str(&yaml).expect("parse");
        assert_eq!(config.pre_drain_grace_period, Duration::from_secs(3));
        assert_eq!(config.drain_timeout, Duration::from_secs(7));
    }

    #[test]
    fn congestion_default_is_valid() {
        assert!(CongestionConfig::default().validate().is_ok());
    }

    #[test]
    fn congestion_rejects_min_window_above_max_window() {
        let config = CongestionConfig {
            min_window: 600,
            max_window: 500,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn congestion_rejects_decrease_factor_of_one() {
        let config = CongestionConfig {
            decrease_factor: 1.0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn congestion_rejects_zero_min_window() {
        let config = CongestionConfig {
            min_window: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }
}
