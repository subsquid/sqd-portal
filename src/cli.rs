use clap::Parser;
use serde::Deserialize;
use serde_with::serde_derive::Serialize;
use serde_with::{serde_as, DurationSeconds};
use std::net::SocketAddr;
use std::time::Duration;

use sqd_network_transport::TransportArgs;

#[derive(Parser)]
#[command(version)]
pub struct Cli {
    #[command(flatten)]
    pub transport: TransportArgs,

    /// HTTP server listen addr
    #[arg(long, env = "HTTP_LISTEN_ADDR", default_value = "0.0.0.0:8000")]
    pub http_listen: SocketAddr,

    /// Path to config file
    #[arg(long, env, value_parser = Config::read)]
    pub config: Config,

    /// Whether the logs should be structured in JSON format
    #[arg(long, env)]
    pub json_log: bool,
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

fn parse_hostname<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Ok(s.trim_end_matches('/').to_owned())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqdNetworkConfig {
    #[serde(rename = "datasets")]
    pub datasets_url: String,

    #[serde(default)]
    pub serve: ServeMode,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServeMode {
    #[default]
    All,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(deserialize_with = "parse_hostname")]
    pub hostname: String,

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
}

impl Config {
    pub fn read(config_path: &str) -> anyhow::Result<Self> {
        let file = std::fs::File::open(config_path)?;
        let buf_reader = std::io::BufReader::new(file);
        Ok(serde_yaml::from_reader(buf_reader)?)
    }
}
