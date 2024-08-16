use clap::Parser;
use serde::Deserialize;
use serde_with::{serde_as, DurationSeconds};
use std::time::Duration;
use std::{collections::HashMap, net::SocketAddr};
use subsquid_network_transport::{PeerId, TransportArgs};

use crate::types::DatasetId;

#[derive(Parser)]
#[command(version)]
pub struct Cli {
    #[command(flatten)]
    pub transport: TransportArgs,

    /// HTTP server listen addr
    #[arg(long, env = "HTTP_LISTEN_ADDR", default_value = "0.0.0.0:8000")]
    pub http_listen: SocketAddr,

    /// Logs collector peer id
    #[arg(long, env)]
    pub logs_collector_id: PeerId,

    /// Path to config file
    #[arg(long, env, value_parser = Config::read)]
    pub config: Config,
}

fn default_worker_inactive_threshold() -> Duration {
    Duration::from_secs(120)
}

fn default_min_worker_priority() -> i8 {
    -5
}

fn default_max_worker_priority() -> i8 {
    3
}

fn default_transport_timeout() -> Duration {
    Duration::from_secs(60)
}

fn default_default_buffer_size() -> usize {
    10
}

fn default_default_chunk_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_default_backoff() -> Duration {
    Duration::from_secs(1)
}

fn default_default_request_multiplier() -> usize {
    1
}

fn default_default_retries() -> usize {
    3
}

fn default_default_timeout_quantile() -> f32 {
    0.5
}

fn default_dataset_update_interval() -> Duration {
    Duration::from_secs(60 * 5)
}

fn default_chain_update_interval() -> Duration {
    Duration::from_secs(60)
}

#[serde_as]
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde_as(as = "DurationSeconds")]
    #[serde(
        rename = "worker_inactive_threshold_sec",
        default = "default_worker_inactive_threshold"
    )]
    pub worker_inactive_threshold: Duration,

    #[serde(default = "default_min_worker_priority")]
    pub min_worker_priority: i8,

    #[serde(default = "default_max_worker_priority")]
    pub max_worker_priority: i8,

    #[serde_as(as = "DurationSeconds")]
    #[serde(
        rename = "transport_timeout_sec",
        default = "default_transport_timeout"
    )]
    pub transport_timeout: Duration,

    #[serde(default = "default_default_buffer_size")]
    pub default_buffer_size: usize,

    #[serde_as(as = "DurationSeconds")]
    #[serde(
        rename = "default_chunk_timeout_sec",
        default = "default_default_chunk_timeout"
    )]
    pub default_chunk_timeout: Duration,

    #[serde_as(as = "DurationSeconds")]
    #[serde(rename = "default_backoff_sec", default = "default_default_backoff")]
    pub default_backoff: Duration,

    #[serde(default = "default_default_request_multiplier")]
    pub default_request_multiplier: usize,

    #[serde(default = "default_default_retries")]
    pub default_retries: usize,

    #[serde(default = "default_default_timeout_quantile")]
    pub default_timeout_quantile: f32,

    #[serde_as(as = "DurationSeconds")]
    #[serde(
        rename = "dataset_update_interval_sec",
        default = "default_dataset_update_interval"
    )]
    pub dataset_update_interval: Duration,

    #[serde_as(as = "DurationSeconds")]
    #[serde(
        rename = "chain_update_interval_sec",
        default = "default_chain_update_interval"
    )]
    pub chain_update_interval: Duration,

    // Dataset alias -> bucket URL
    pub available_datasets: HashMap<String, String>,
}

impl Config {
    pub fn read(config_path: &str) -> anyhow::Result<Self> {
        let file_contents = std::fs::read(config_path)?;
        Ok(serde_yaml::from_slice(file_contents.as_slice())?)
    }

    pub fn dataset_id(&self, dataset: &str) -> Option<DatasetId> {
        self.available_datasets
            .get(dataset)
            .map(DatasetId::from_url)
    }

    pub fn dataset_ids(&self) -> impl Iterator<Item = DatasetId> + '_ {
        self.available_datasets.values().map(DatasetId::from_url)
    }
}
