use std::path::PathBuf;

use serde::Deserialize;
use url::Url;

#[derive(Debug, Clone, Deserialize)]
pub struct CommercialConfig {
    pub control_plane_url: Url,
    pub service_token_env: String,
    pub sync_interval_secs: u64,
    pub flush_interval_secs: u64,
    pub flush_max_events: usize,
    pub usage_buffer_max_events: usize,
    pub snapshot_cache_path: PathBuf,
    pub resolve_rate_per_sec: u64,
    pub negative_cache_secs: u64,
    pub pod_count: usize,
    pub public_fallback: PublicFallbackConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PublicFallbackConfig {
    pub throughput_bytes_per_sec: u64,
    pub burst_bytes: u64,
    pub max_response_bytes: u64,
    pub volume_bytes: u64,
    pub window_secs: u64,
    pub concurrency: usize,
}

impl CommercialConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.service_token_env.trim().is_empty(),
            "commercial.service_token_env must not be empty"
        );
        let token = std::env::var(&self.service_token_env)
            .map_err(|_| anyhow::anyhow!("{} must be set", self.service_token_env))?;
        anyhow::ensure!(
            !token.trim().is_empty(),
            "{} must not be empty",
            self.service_token_env
        );
        anyhow::ensure!(
            self.pod_count >= 1,
            "commercial.pod_count must be greater than zero"
        );

        Ok(())
    }
}
