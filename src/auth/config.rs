use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Deserializer};
use sqd_network_transport::Keypair;
use url::Url;

use super::signing::RequestSigner;

/// Per-deployment portal identity. Every replica of a portal shares one config
/// file, so the id is normally injected per pod and overrides the file value.
const PORTAL_ID_ENV: &str = "PORTAL_ID";

/// Overrides `auth.key_path`, as `PORTAL_ID` overrides the id — the two are
/// registered together and are injected together.
pub const AUTH_KEY_PATH_ENV: &str = "AUTH_KEY_PATH";

/// The key path and the knob that named it: the override wins silently, so a
/// failure blaming `auth.key_path` sends an operator to edit nothing.
#[derive(Debug, Clone)]
pub(crate) struct KeySource {
    pub path: PathBuf,
    pub knob: &'static str,
}

/// Presence of this block turns authorization on: the data API then
/// requires a key. Absent, the portal behaves exactly like an OSS build.
#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    /// Where the control plane mounts its portal API, prefix and all — the
    /// portal appends only the version and the operation. The route is the
    /// other side's to move, and this is what lets it move without a release
    /// here, and without fixing what an operator has to expose.
    pub control_plane_url: Url,

    /// What the control plane knows this portal as, and what it attributes the
    /// request signature to. `PORTAL_ID` in the environment wins when set and
    /// non-empty.
    pub portal_id: String,

    /// Ed25519 key the exchange is signed with, in the network key's format.
    /// Absent, the portal signs with its network identity; set, one machine can
    /// keep a dev and a prod registration and pick by path. `AUTH_KEY_PATH`
    /// wins when set and non-empty, so read this through
    /// [`ResolvedAuth::key`] rather than directly.
    #[serde(default, deserialize_with = "key_path_that_names_a_file")]
    pub key_path: Option<PathBuf>,

    #[serde(default)]
    pub enforcement: Enforcement,

    /// Nested, not flattened: flattening absorbs every unmatched key, so a
    /// misspelled limit would keep its default without a warning.
    #[serde(default)]
    pub limits: Limits,
}

/// What one credential, or a flood of them, can cost. Operator-bindable
/// because the right values follow the credential working set a deployment
/// sees; defaulted so a minimal block stays three lines.
#[derive(Debug, Clone, Deserialize)]
pub struct Limits {
    /// Ceiling on the lifetime the portal honours, whatever is offered: the
    /// fleet's worst-case stale-authorization window (REQ-54).
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
    /// Evaluates and logs the verdict, then admits regardless — including with
    /// no credential. For the cutover, while upstream is still the real gate.
    LogOnly,
}

impl Enforcement {
    /// Carried by both the log field and the OB-12 label, so they cannot drift
    /// into two spellings.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Enforce => "enforce",
            Self::LogOnly => "log_only",
        }
    }
}

/// The block with its deployment overrides applied, and checked. Everything
/// downstream takes this: the environment is read in [`AuthConfig::resolve`]
/// and nowhere else, so no two call sites can disagree about the identity.
#[derive(Debug, Clone)]
pub struct ResolvedAuth {
    pub control_plane_url: Url,
    pub portal_id: String,
    /// `None` signs with the network identity.
    pub(crate) key: Option<KeySource>,
    pub enforcement: Enforcement,
    pub limits: Limits,
}

impl AuthConfig {
    pub fn resolve(&self) -> anyhow::Result<ResolvedAuth> {
        let resolved = ResolvedAuth {
            control_plane_url: self.control_plane_url.clone(),
            portal_id: env_portal_id().unwrap_or_else(|| self.portal_id.trim().to_owned()),
            key: self.key_source(),
            enforcement: self.enforcement,
            limits: self.limits.clone(),
        };
        resolved.validate()?;
        Ok(resolved)
    }

    /// Where the signing key lives, or `None` for the network identity. A blank
    /// variable is an unset one; the file's own value was checked at parse, the
    /// only point where a template that rendered nothing is still
    /// distinguishable from an absent key.
    fn key_source(&self) -> Option<KeySource> {
        // `var_os`, not `var`: dropping a non-UTF-8 path as if it were never set
        // signs with the wrong identity silently.
        let from_env = std::env::var_os(AUTH_KEY_PATH_ENV)
            .and_then(|value| normalize_key_path(Path::new(&value)))
            .map(|path| KeySource {
                path,
                knob: AUTH_KEY_PATH_ENV,
            });
        from_env.or_else(|| {
            self.key_path.clone().map(|path| KeySource {
                path,
                knob: "auth.key_path",
            })
        })
    }
}

/// A key that is written at all names a file. serde folds YAML null into
/// `None`, so `key_path:` — what a template that rendered nothing produces —
/// is indistinguishable from an absent key by the time `resolve` runs, and
/// would silently sign with the network identity. Only the deserializer, which
/// runs solely because the key was written, can still tell the two apart.
fn key_path_that_names_a_file<'de, D>(deserializer: D) -> Result<Option<PathBuf>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<PathBuf>::deserialize(deserializer)?
        .as_deref()
        .and_then(normalize_key_path)
        .map(Some)
        .ok_or_else(|| {
            serde::de::Error::custom(
                "auth.key_path is present but empty. Drop the key to sign with the network \
                 identity, or point it at the key file the control plane registered.",
            )
        })
}

impl ResolvedAuth {
    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.portal_id.is_empty(),
            "auth.portal_id must not be empty"
        );
        // It travels in a header and in the string the signature covers, whose
        // fields are newline-separated. Rejecting control characters here is
        // what lets the canonical form stay unambiguous (DC-8).
        anyhow::ensure!(
            self.portal_id
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

    /// By default the portal signs with the identity it already has, so nothing
    /// is provisioned (DC-8); `key` separates the two where the registration is
    /// not the network identity.
    ///
    /// Validates rather than trusting `resolve` to have run: every field is
    /// constructible, so this is the last rung before a portal id forges the
    /// canonical form or a plaintext hop carries a customer's credential.
    pub fn signer(&self, keypair: Keypair) -> anyhow::Result<RequestSigner> {
        self.validate()?;
        Ok(RequestSigner::new(keypair, self.portal_id.clone()))
    }

    pub fn exchange_timeout(&self) -> Duration {
        self.limits.exchange_timeout()
    }
}

fn env_portal_id() -> Option<String> {
    std::env::var(PORTAL_ID_ENV)
        .ok()
        .map(|id| id.trim().to_owned())
        .filter(|id| !id.is_empty())
}

/// Trims wherever the value reads as text, and reports whitespace-only as
/// unset. A non-UTF-8 value came from the OS, no place to rewrite bytes.
fn normalize_key_path(path: &Path) -> Option<PathBuf> {
    let Some(text) = path.to_str() else {
        return Some(path.to_owned());
    };
    let trimmed = text.trim();
    (!trimmed.is_empty()).then(|| PathBuf::from(trimmed))
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
        // Zero refuses every exchange, which reads as a control-plane outage.
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
        // Zero expires a denial before the next request reads it, turning the
        // cache off silently — and the budget it protects is fleet-shared.
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
control_plane_url: https://cp.example/authority
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
        // Signs with the network identity until told otherwise.
        assert_eq!(config.key_path, None);
        // P-GRANT-MAX-LIFETIME
        assert_eq!(config.limits.max_grant_lifetime_secs, 900);
        // P-GRANT-CACHE-CAPACITY
        assert_eq!(config.limits.grant_cache_capacity, 65_536);
        // P-GRANT-EXCHANGE-TIMEOUT
        assert_eq!(
            config.limits.exchange_timeout(),
            Duration::from_millis(2_000)
        );
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
             key_path: /keys/exchange.key\n\
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

        assert_eq!(config.key_path, Some(PathBuf::from("/keys/exchange.key")));
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
    fn key_path_env_overrides_config_when_non_empty() {
        let _guard = env_guard();
        std::env::remove_var(PORTAL_ID_ENV);
        let config = parse(&format!("{MINIMAL}key_path: /keys/prod.key\n"));
        let resolved = |config: &AuthConfig| {
            config
                .resolve()
                .expect("the block validates")
                .key
                .map(|key| key.path)
        };

        std::env::remove_var(AUTH_KEY_PATH_ENV);
        assert_eq!(resolved(&config), Some(PathBuf::from("/keys/prod.key")));

        std::env::set_var(AUTH_KEY_PATH_ENV, "/keys/dev.key");
        assert_eq!(resolved(&config), Some(PathBuf::from("/keys/dev.key")));

        std::env::set_var(AUTH_KEY_PATH_ENV, "   ");
        assert_eq!(resolved(&config), Some(PathBuf::from("/keys/prod.key")));

        std::env::remove_var(AUTH_KEY_PATH_ENV);
        assert_eq!(resolved(&parse(MINIMAL)), None);
    }

    /// Told only the path, an operator edits the config file and redeploys into
    /// the same failure.
    #[test]
    fn the_resolved_key_path_names_the_knob_it_came_from() {
        let _guard = env_guard();
        std::env::remove_var(PORTAL_ID_ENV);
        let config = parse(&format!("{MINIMAL}key_path: /keys/prod.key\n"));
        let knob = |config: &AuthConfig| config.resolve().unwrap().key.unwrap().knob;

        std::env::remove_var(AUTH_KEY_PATH_ENV);
        assert_eq!(knob(&config), "auth.key_path");

        std::env::set_var(AUTH_KEY_PATH_ENV, "/keys/dev.key");
        assert_eq!(knob(&config), AUTH_KEY_PATH_ENV);

        std::env::remove_var(AUTH_KEY_PATH_ENV);
    }

    /// A trailing space is invisible in the error that would name the file.
    #[test]
    fn surrounding_whitespace_is_trimmed_from_either_source() {
        let _guard = env_guard();
        std::env::remove_var(PORTAL_ID_ENV);
        std::env::remove_var(AUTH_KEY_PATH_ENV);
        let path = |config: &AuthConfig| config.resolve().unwrap().key.unwrap().path;

        let config = parse(&format!("{MINIMAL}key_path: \"  /keys/prod.key \"\n"));
        assert_eq!(path(&config), PathBuf::from("/keys/prod.key"));

        std::env::set_var(AUTH_KEY_PATH_ENV, " /keys/dev.key\n");
        assert_eq!(path(&config), PathBuf::from("/keys/dev.key"));

        std::env::remove_var(AUTH_KEY_PATH_ENV);
    }

    /// A rendered-empty value is a broken template, not a request for the
    /// network identity — and the likeliest rendering of one is the bare
    /// `key_path:` that YAML reads as null, which serde would otherwise fold
    /// into the same `None` an absent key produces.
    #[test]
    fn an_empty_key_path_in_the_file_is_rejected() {
        for spelling in ["\"\"", "\"   \"", "", "null", "~"] {
            let yaml = format!("{MINIMAL}key_path: {spelling}\n");
            let Err(err) = serde_yaml::from_str::<AuthConfig>(&yaml) else {
                panic!("`key_path: {spelling}` read as an absent key");
            };
            assert!(err.to_string().contains("auth.key_path"), "got {err}");
        }

        assert_eq!(
            parse(&format!("{MINIMAL}key_path: /keys/prod.key\n")).key_path,
            Some(PathBuf::from("/keys/prod.key"))
        );
        assert_eq!(parse(MINIMAL).key_path, None);
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
        assert_eq!(config.resolve().unwrap().portal_id, "portal-premium-eu");

        std::env::set_var(PORTAL_ID_ENV, "portal-from-env");
        assert_eq!(config.resolve().unwrap().portal_id, "portal-from-env");

        // A blank override is an unset deployment variable, not a request for an
        // empty portal id.
        std::env::set_var(PORTAL_ID_ENV, "   ");
        assert_eq!(config.resolve().unwrap().portal_id, "portal-premium-eu");

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
        assert!(config.resolve().is_err());

        config.portal_id = "portal premium".to_string();
        assert!(config.resolve().is_err());

        config.portal_id = "  ".to_string();
        assert!(config.resolve().is_err());
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
                config.resolve().is_err(),
                "{:?} should not validate",
                config
            );
        }

        assert!(parse(MINIMAL).resolve().is_ok());
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

        assert!(with_url("http://cp.example/").resolve().is_err());
        assert!(with_url("http://10.0.0.5:8080/").resolve().is_err());

        assert!(with_url("https://cp.example/").resolve().is_ok());
        assert!(with_url("http://127.0.0.1:3000/").resolve().is_ok());
        assert!(with_url("http://localhost:3000/").resolve().is_ok());
        assert!(with_url("http://[::1]:3000/").resolve().is_ok());
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
