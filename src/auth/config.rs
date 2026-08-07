use std::time::Duration;

use serde::Deserialize;
use sqd_network_transport::Keypair;
use url::Url;

use super::signing::RequestSigner;

/// Per-deployment portal identity. Every replica of a portal shares one config
/// file, so the id is normally injected per pod and overrides the file value.
const PORTAL_ID_ENV: &str = "PORTAL_ID";

/// Presence of this block turns authorization on: the data API then
/// requires a key. Absent, the portal behaves exactly like an OSS build.
#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    pub control_plane_url: Url,

    /// What the control plane knows this portal as, and what it attributes the
    /// request signature to. `PORTAL_ID` in the environment wins when set and
    /// non-empty.
    pub portal_id: String,

    #[serde(default)]
    pub enforcement: Enforcement,

    /// Nested rather than flattened: a flattened struct absorbs every unmatched
    /// key in the block, so `Config::read`'s unknown-field warning never fires
    /// and a misspelled limit silently keeps its default.
    #[serde(default)]
    pub limits: Limits,
}

/// Everything that bounds what one credential, or a flood of them, can cost.
/// Defaulted so a minimal block stays three lines, and operator-bindable
/// because the right values depend on the credential working set a deployment
/// actually sees, which is not knowable here.
#[derive(Debug, Clone, Deserialize)]
pub struct Limits {
    /// Ceiling on the lifetime the portal will honour, whatever the control
    /// plane offers. The fleet's worst-case stale-authorization window, and the
    /// only lifetime term the portal owns (REQ-54).
    #[serde(default = "default_max_grant_lifetime_secs")]
    pub max_grant_lifetime_secs: u64,

    /// Per-exchange deadline. Must stay below the deadline callers wait on, or
    /// a request outlives the exchange it is waiting for (ADR-010).
    #[serde(default = "default_exchange_timeout_ms")]
    pub exchange_timeout_ms: u64,

    #[serde(default = "default_exchange_rate_per_sec")]
    pub exchange_rate_per_sec: u64,

    #[serde(default = "default_max_inflight_exchanges")]
    pub max_inflight_exchanges: usize,

    /// Sized by the credential working set, not by the key set — the whole
    /// point of asking on demand (HZ-13).
    #[serde(default = "default_grant_cache_capacity")]
    pub grant_cache_capacity: usize,

    /// Fingerprints are attacker-chosen, so remembered denials are capped
    /// rather than grown (HZ-10).
    #[serde(default = "default_denial_cache_capacity")]
    pub denial_cache_capacity: usize,

    /// How long a denial suppresses repeat exchanges for the same fingerprint.
    /// Short enough that a key minted moments after a refusal still works.
    #[serde(default = "default_denial_ttl_secs")]
    pub denial_ttl_secs: u64,

    /// Spread applied to renewals, as a percentage of the refresh window, so a
    /// cohort of grants issued together does not come back together (HZ-12).
    #[serde(default = "default_refresh_jitter_pct")]
    pub refresh_jitter_pct: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Enforcement {
    #[default]
    Enforce,
    /// Evaluates the full ladder and logs the verdict, then admits regardless —
    /// including requests with no credential at all. Shadow mode for the
    /// cutover window, while something upstream is still the real gate.
    LogOnly,
}

impl Enforcement {
    /// The value the log field and the OB-12 label both carry, so they cannot
    /// drift into two spellings of the same mode.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Enforce => "enforce",
            Self::LogOnly => "log_only",
        }
    }
}

impl AuthConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        let portal_id = self.portal_id();
        anyhow::ensure!(!portal_id.is_empty(), "auth.portal_id must not be empty");
        // It travels in a header and in the string the signature covers, whose
        // fields are newline-separated. Rejecting control characters here is
        // what lets the canonical form stay unambiguous (DC-8).
        anyhow::ensure!(
            portal_id
                .chars()
                .all(|c| !c.is_control() && !c.is_whitespace()),
            "auth.portal_id must not contain whitespace or control characters"
        );
        // The exchange body carries the caller's credential verbatim, so a
        // plaintext hop hands whoever can read it a key that works against the
        // public data API. Loopback keeps local dev on `http://`.
        anyhow::ensure!(
            self.control_plane_url.scheme() == "https" || is_loopback(&self.control_plane_url),
            "auth.control_plane_url must be https outside loopback: it carries credentials"
        );
        self.limits.validate()
    }

    pub fn portal_id(&self) -> String {
        std::env::var(PORTAL_ID_ENV)
            .ok()
            .map(|id| id.trim().to_owned())
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| self.portal_id.trim().to_owned())
    }

    /// The portal signs with the identity it already has. Nothing new is
    /// provisioned, and the operator configures it where they always did —
    /// `KEY_PATH` (DC-8).
    pub fn signer(&self, keypair: Keypair) -> anyhow::Result<RequestSigner> {
        self.validate()?;
        Ok(RequestSigner::new(keypair, self.portal_id()))
    }

    pub fn exchange_timeout(&self) -> Duration {
        self.limits.exchange_timeout()
    }
}

fn is_loopback(url: &Url) -> bool {
    match url.host() {
        Some(url::Host::Domain(name)) => name == "localhost",
        Some(url::Host::Ipv4(addr)) => addr.is_loopback(),
        Some(url::Host::Ipv6(addr)) => addr.is_loopback(),
        None => false,
    }
}

impl Limits {
    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.max_grant_lifetime_secs >= 1,
            "auth.max_grant_lifetime_secs must be at least 1"
        );
        anyhow::ensure!(
            self.exchange_timeout_ms >= 1,
            "auth.exchange_timeout_ms must be at least 1"
        );
        // Zero would refuse every exchange, which reads as a control-plane
        // outage rather than as the configuration mistake it is.
        anyhow::ensure!(
            self.exchange_rate_per_sec >= 1,
            "auth.exchange_rate_per_sec must be at least 1"
        );
        anyhow::ensure!(
            self.max_inflight_exchanges >= 1,
            "auth.max_inflight_exchanges must be at least 1"
        );
        anyhow::ensure!(
            self.grant_cache_capacity >= 1,
            "auth.grant_cache_capacity must be at least 1"
        );
        anyhow::ensure!(
            self.denial_cache_capacity >= 1,
            "auth.denial_cache_capacity must be at least 1"
        );
        // Zero expires a denial before the next request reads it, which turns
        // the cache off without saying so — and the exchange budget it exists
        // to protect is fleet-shared (HZ-10).
        anyhow::ensure!(
            self.denial_ttl_secs >= 1,
            "auth.denial_ttl_secs must be at least 1"
        );
        anyhow::ensure!(
            self.refresh_jitter_pct <= 100,
            "auth.refresh_jitter_pct must be a percentage"
        );
        Ok(())
    }

    pub fn exchange_timeout(&self) -> Duration {
        Duration::from_millis(self.exchange_timeout_ms.max(1))
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_grant_lifetime_secs: default_max_grant_lifetime_secs(),
            exchange_timeout_ms: default_exchange_timeout_ms(),
            exchange_rate_per_sec: default_exchange_rate_per_sec(),
            max_inflight_exchanges: default_max_inflight_exchanges(),
            grant_cache_capacity: default_grant_cache_capacity(),
            denial_cache_capacity: default_denial_cache_capacity(),
            denial_ttl_secs: default_denial_ttl_secs(),
            refresh_jitter_pct: default_refresh_jitter_pct(),
        }
    }
}

fn default_max_grant_lifetime_secs() -> u64 {
    900
}

fn default_exchange_timeout_ms() -> u64 {
    2_000
}

fn default_exchange_rate_per_sec() -> u64 {
    20
}

fn default_max_inflight_exchanges() -> usize {
    32
}

fn default_grant_cache_capacity() -> usize {
    65_536
}

fn default_denial_cache_capacity() -> usize {
    4096
}

fn default_denial_ttl_secs() -> u64 {
    15
}

fn default_refresh_jitter_pct() -> u64 {
    10
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::test_support::env_guard;

    const MINIMAL: &str = r#"
control_plane_url: https://cp.example/
portal_id: portal-premium-eu
"#;

    fn parse(yaml: &str) -> AuthConfig {
        serde_yaml::from_str(yaml).expect("auth config should parse")
    }

    /// The defaults are the ratified parameter values, and the lifetime one is
    /// the fleet's worst-case stale-authorization window — a default drifting
    /// above P-GRANT-MAX-LIFETIME widens it silently on every deployment that
    /// never set it.
    #[test]
    fn minimal_block_defaults_every_optional_field() {
        let config = parse(MINIMAL);

        assert_eq!(config.enforcement, Enforcement::Enforce);
        // P-GRANT-MAX-LIFETIME
        assert_eq!(config.limits.max_grant_lifetime_secs, 900);
        // P-GRANT-CACHE-CAPACITY
        assert_eq!(config.limits.grant_cache_capacity, 65_536);
        // P-GRANT-EXCHANGE-TIMEOUT
        assert_eq!(config.exchange_timeout(), Duration::from_millis(2_000));
        // P-GRANT-EXCHANGE-RATE / -INFLIGHT
        assert_eq!(config.limits.exchange_rate_per_sec, 20);
        assert_eq!(config.limits.max_inflight_exchanges, 32);
        // P-GRANT-NEGATIVE-TTL / -CAPACITY
        assert_eq!(config.limits.denial_ttl_secs, 15);
        assert_eq!(config.limits.denial_cache_capacity, 4096);
    }

    #[test]
    fn full_block_parses_every_field() {
        let config = parse(&format!(
            "{MINIMAL}\
             enforcement: log_only\n\
             limits:\n  \
             max_grant_lifetime_secs: 300\n  \
             exchange_timeout_ms: 500\n  \
             exchange_rate_per_sec: 5\n  \
             max_inflight_exchanges: 4\n  \
             grant_cache_capacity: 128\n  \
             denial_cache_capacity: 64\n  \
             denial_ttl_secs: 30\n  \
             refresh_jitter_pct: 25\n"
        ));

        assert_eq!(config.enforcement, Enforcement::LogOnly);
        assert_eq!(config.limits.max_grant_lifetime_secs, 300);
        assert_eq!(config.limits.exchange_rate_per_sec, 5);
        assert_eq!(config.limits.max_inflight_exchanges, 4);
        assert_eq!(config.limits.grant_cache_capacity, 128);
        assert_eq!(config.limits.denial_cache_capacity, 64);
        assert_eq!(config.limits.denial_ttl_secs, 30);
        assert_eq!(config.limits.refresh_jitter_pct, 25);
    }

    #[test]
    fn unknown_enforcement_mode_is_rejected() {
        let err = serde_yaml::from_str::<AuthConfig>(&format!("{MINIMAL}enforcement: off\n"))
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

    /// The id is a field of the string the signature covers, and that string
    /// separates its fields with newlines.
    #[test]
    fn validate_rejects_a_portal_id_that_could_forge_the_canonical_form() {
        let _guard = env_guard();
        std::env::remove_var(PORTAL_ID_ENV);

        let mut config = parse(MINIMAL);
        config.portal_id = "portal\n1800000000\nPOST".to_string();
        assert!(config.validate().is_err());

        config.portal_id = "portal premium".to_string();
        assert!(config.validate().is_err());

        config.portal_id = "  ".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_rejects_out_of_range_knobs() {
        let _guard = env_guard();
        std::env::remove_var(PORTAL_ID_ENV);

        for mutate in [
            (|l: &mut Limits| l.max_grant_lifetime_secs = 0) as fn(&mut Limits),
            |l| l.exchange_timeout_ms = 0,
            |l| l.exchange_rate_per_sec = 0,
            |l| l.max_inflight_exchanges = 0,
            |l| l.grant_cache_capacity = 0,
            |l| l.denial_cache_capacity = 0,
            |l| l.denial_ttl_secs = 0,
            |l| l.refresh_jitter_pct = 101,
        ] {
            let mut config = parse(MINIMAL);
            mutate(&mut config.limits);
            assert!(
                config.validate().is_err(),
                "{:?} should not validate",
                config
            );
        }

        assert!(parse(MINIMAL).validate().is_ok());
    }

    /// The exchange body carries the caller's credential, so the hop has to be
    /// one an observer cannot read — except where it never leaves the machine.
    #[test]
    fn a_plaintext_control_plane_is_rejected_unless_it_is_loopback() {
        let _guard = env_guard();
        std::env::remove_var(PORTAL_ID_ENV);

        let with_url = |url: &str| {
            let mut config = parse(MINIMAL);
            config.control_plane_url = url.parse().unwrap();
            config
        };

        assert!(with_url("http://cp.example/").validate().is_err());
        assert!(with_url("http://10.0.0.5:8080/").validate().is_err());

        assert!(with_url("https://cp.example/").validate().is_ok());
        assert!(with_url("http://127.0.0.1:3000/").validate().is_ok());
        assert!(with_url("http://localhost:3000/").validate().is_ok());
        assert!(with_url("http://[::1]:3000/").validate().is_ok());
    }

    /// A limit is only operator-bindable if a typo in one is audible. Flattening
    /// the struct into the block swallowed every unmatched key, so the default
    /// stayed in force and nothing said so.
    #[test]
    fn a_misspelled_limit_is_reported_rather_than_ignored() {
        let yaml = format!(
            "{MINIMAL}limits:\n  \
             max_grant_lifetime_seconds: 60\n"
        );

        let mut ignored = Vec::new();
        let deser = serde_yaml::Deserializer::from_str(&yaml);
        let config: AuthConfig = serde_ignored::deserialize(deser, |path| {
            ignored.push(path.to_string());
        })
        .expect("the block still parses");

        assert_eq!(ignored, vec!["limits.max_grant_lifetime_seconds"]);
        assert_eq!(config.limits.max_grant_lifetime_secs, 900);
    }
}
