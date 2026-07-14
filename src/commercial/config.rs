use std::{path::PathBuf, time::Duration};

use serde::Deserialize;
use url::Url;

pub const DEFAULT_USAGE_MAX_RETRY_AGE_SECS: u64 = 7 * 24 * 60 * 60;
const MAX_USAGE_RETRY_AGE_SECS: u64 = 30 * 24 * 60 * 60;

#[derive(Debug, Clone, Deserialize)]
pub struct CommercialConfig {
    pub control_plane_url: Url,
    pub service_token_env: String,
    pub sync_interval_secs: u64,
    pub flush_interval_secs: u64,
    pub flush_max_events: usize,
    pub usage_buffer_max_events: usize,
    // Cross-repo contract: the portal retry cap must not exceed the SaaS
    // usage-event dedup retention, whose validated lower bound is 30 days.
    #[serde(default = "default_usage_max_retry_age_secs")]
    pub usage_max_retry_age_secs: u64,
    pub snapshot_cache_path: PathBuf,
    pub resolve_rate_per_sec: u64,
    pub negative_cache_secs: u64,
    pub pod_count: usize,
    #[serde(default = "default_client_ip_header")]
    pub client_ip_header: String,
    #[serde(default = "default_throttle_residual_secs")]
    pub throttle_residual_secs: u64,
    #[serde(default = "default_sweep_horizon_window_multiplier")]
    pub sweep_horizon_window_multiplier: u64,
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
        anyhow::ensure!(
            self.flush_max_events <= 10_000,
            "commercial.flush_max_events must be at most 10000"
        );
        anyhow::ensure!(
            self.flush_interval_secs >= 1,
            "commercial.flush_interval_secs must be at least 1"
        );
        anyhow::ensure!(
            (1..=MAX_USAGE_RETRY_AGE_SECS).contains(&self.usage_max_retry_age_secs),
            "commercial.usage_max_retry_age_secs must be between 1 and {MAX_USAGE_RETRY_AGE_SECS}"
        );
        anyhow::ensure!(
            !self.client_ip_header.trim().is_empty(),
            "commercial.client_ip_header must not be empty"
        );
        anyhow::ensure!(
            self.throttle_residual_secs > 0,
            "commercial.throttle_residual_secs must be greater than zero"
        );
        anyhow::ensure!(
            self.sweep_horizon_window_multiplier > 0,
            "commercial.sweep_horizon_window_multiplier must be greater than zero"
        );

        Ok(())
    }

    pub fn sweep_horizon(&self) -> Duration {
        Duration::from_secs(
            self.public_fallback
                .window_secs
                .max(1)
                .saturating_mul(self.sweep_horizon_window_multiplier),
        )
    }
}

fn default_client_ip_header() -> String {
    "x-forwarded-for".to_string()
}

fn default_usage_max_retry_age_secs() -> u64 {
    DEFAULT_USAGE_MAX_RETRY_AGE_SECS
}

pub fn default_throttle_residual_secs() -> u64 {
    60
}

fn default_sweep_horizon_window_multiplier() -> u64 {
    2
}
