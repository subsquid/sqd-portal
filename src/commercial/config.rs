use std::time::Duration;

use serde::Deserialize;
use url::Url;

/// Per-deployment portal identity. Every replica of a portal shares one config
/// file, so the id is normally injected per pod and overrides the file value.
const PORTAL_ID_ENV: &str = "PORTAL_ID";

/// Upper bound on concurrent authorize-on-miss calls, so an unknown-key flood
/// cannot turn into an unbounded fan-out against the control plane.
const MAX_INFLIGHT_RESOLVES: usize = 64;

/// Presence of this block turns the portal commercial: the data API then
/// requires a key. Absent, the portal behaves exactly like an OSS build.
#[derive(Debug, Clone, Deserialize)]
pub struct CommercialConfig {
    pub control_plane_url: Url,

    /// Name of the env var holding the bearer token for the internal
    /// control-plane API. Must resolve to a non-empty value at validation.
    pub service_token_env: String,

    /// Matched against a key's `portal_ids`. `PORTAL_ID` in the environment
    /// wins when set and non-empty.
    pub portal_id: String,

    #[serde(default)]
    pub enforcement: Enforcement,

    #[serde(default = "default_sync_interval_secs")]
    pub sync_interval_secs: u64,

    /// Token-bucket rate for authorize-on-miss lookups. Zero disables them, so
    /// keys absent from the snapshot are rejected until the next sync.
    #[serde(default = "default_resolve_rate_per_sec")]
    pub resolve_rate_per_sec: u64,

    #[serde(default = "default_max_inflight_resolves")]
    pub max_inflight_resolves: usize,

    /// How long a control-plane "unknown key" answer suppresses repeat lookups.
    #[serde(default = "default_negative_cache_secs")]
    pub negative_cache_secs: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Enforcement {
    #[default]
    Enforce,
    /// Evaluates the full ladder and logs the verdict, then admits regardless —
    /// including requests with no credential at all. Shadow mode for the
    /// cutover window while the Cloudflare rule is still the real gate.
    LogOnly,
}

impl CommercialConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.service_token()?;
        anyhow::ensure!(
            !self.portal_id().is_empty(),
            "commercial.portal_id must not be empty"
        );
        anyhow::ensure!(
            self.sync_interval_secs >= 1,
            "commercial.sync_interval_secs must be at least 1"
        );
        anyhow::ensure!(
            (1..=MAX_INFLIGHT_RESOLVES).contains(&self.max_inflight_resolves),
            "commercial.max_inflight_resolves must be between 1 and {MAX_INFLIGHT_RESOLVES}"
        );
        Ok(())
    }

    pub fn service_token(&self) -> anyhow::Result<String> {
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
        Ok(token)
    }

    pub fn portal_id(&self) -> String {
        std::env::var(PORTAL_ID_ENV)
            .ok()
            .map(|id| id.trim().to_owned())
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| self.portal_id.trim().to_owned())
    }

    pub fn sync_interval(&self) -> Duration {
        Duration::from_secs(self.sync_interval_secs.max(1))
    }

    pub fn negative_cache_ttl(&self) -> Duration {
        Duration::from_secs(self.negative_cache_secs)
    }
}

fn default_sync_interval_secs() -> u64 {
    10
}

fn default_resolve_rate_per_sec() -> u64 {
    20
}

fn default_max_inflight_resolves() -> usize {
    16
}

fn default_negative_cache_secs() -> u64 {
    15
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commercial::test_support::env_guard;

    const MINIMAL: &str = r#"
control_plane_url: https://cp.example/
service_token_env: PORTAL_CP_TOKEN
portal_id: portal-premium-eu
"#;

    fn parse(yaml: &str) -> CommercialConfig {
        serde_yaml::from_str(yaml).expect("commercial config should parse")
    }

    #[test]
    fn minimal_block_defaults_every_optional_field() {
        let config = parse(MINIMAL);

        assert_eq!(config.enforcement, Enforcement::Enforce);
        assert_eq!(config.sync_interval_secs, 10);
        assert_eq!(config.resolve_rate_per_sec, 20);
        assert_eq!(config.max_inflight_resolves, 16);
        assert_eq!(config.negative_cache_secs, 15);
    }

    #[test]
    fn full_block_parses_every_field() {
        let config = parse(&format!(
            "{MINIMAL}enforcement: log_only\n\
             sync_interval_secs: 3\n\
             resolve_rate_per_sec: 7\n\
             max_inflight_resolves: 5\n\
             negative_cache_secs: 30\n"
        ));

        assert_eq!(config.enforcement, Enforcement::LogOnly);
        assert_eq!(config.sync_interval_secs, 3);
        assert_eq!(config.resolve_rate_per_sec, 7);
        assert_eq!(config.max_inflight_resolves, 5);
        assert_eq!(config.negative_cache_secs, 30);
    }

    #[test]
    fn unknown_enforcement_mode_is_rejected() {
        let err = serde_yaml::from_str::<CommercialConfig>(&format!("{MINIMAL}enforcement: off\n"))
            .expect_err("unknown enforcement mode must not parse");
        assert!(err.to_string().contains("enforce"), "got {err}");
    }

    #[test]
    fn portal_id_env_overrides_config_when_non_empty() {
        let _guard = env_guard();
        let config = parse(MINIMAL);

        std::env::remove_var(PORTAL_ID_ENV);
        assert_eq!(config.portal_id(), "portal-premium-eu");

        std::env::set_var(PORTAL_ID_ENV, "portal-from-env");
        assert_eq!(config.portal_id(), "portal-from-env");

        // A blank override is an unset deployment variable, not a request for an
        // empty portal id.
        std::env::set_var(PORTAL_ID_ENV, "   ");
        assert_eq!(config.portal_id(), "portal-premium-eu");

        std::env::remove_var(PORTAL_ID_ENV);
    }

    #[test]
    fn validate_requires_a_non_empty_service_token_in_the_environment() {
        let _guard = env_guard();
        let config = parse(MINIMAL);

        std::env::remove_var("PORTAL_CP_TOKEN");
        assert!(config.validate().is_err());

        std::env::set_var("PORTAL_CP_TOKEN", "  ");
        assert!(config.validate().is_err());

        std::env::set_var("PORTAL_CP_TOKEN", "token");
        assert!(config.validate().is_ok());

        std::env::remove_var("PORTAL_CP_TOKEN");
    }

    #[test]
    fn validate_rejects_out_of_range_knobs() {
        let _guard = env_guard();
        std::env::set_var("PORTAL_CP_TOKEN", "token");

        let mut config = parse(MINIMAL);
        config.sync_interval_secs = 0;
        assert!(config.validate().is_err());

        let mut config = parse(MINIMAL);
        config.max_inflight_resolves = 0;
        assert!(config.validate().is_err());

        let mut config = parse(MINIMAL);
        config.max_inflight_resolves = MAX_INFLIGHT_RESOLVES + 1;
        assert!(config.validate().is_err());

        let mut config = parse(MINIMAL);
        config.portal_id = "  ".to_string();
        std::env::remove_var(PORTAL_ID_ENV);
        assert!(config.validate().is_err());

        std::env::remove_var("PORTAL_CP_TOKEN");
    }
}
