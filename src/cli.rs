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

    #[arg(long, env, default_value_t = 10)]
    pub buffer_size: usize,

    #[arg(long, env, value_parser = Config::read)]
    pub config: Config,
}

fn parse_seconds(s: &str) -> anyhow::Result<Duration> {
    Ok(Duration::from_secs(s.parse()?))
}

fn default_worker_inactive_threshold() -> Duration {
    Duration::from_secs(120)
}

fn default_worker_greylist_time() -> Duration {
    Duration::from_secs(1800)
}

fn default_transport_timeout() -> Duration {
    Duration::from_secs(5)
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

    #[serde_as(as = "DurationSeconds")]
    #[serde(
        rename = "worker_greylist_time_sec",
        default = "default_worker_greylist_time"
    )]
    pub worker_greylist_time: Duration,

    #[serde_as(as = "DurationSeconds")]
    #[serde(
        rename = "transport_timeout_sec",
        default = "default_transport_timeout"
    )]
    pub transport_timeout: Duration,

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
