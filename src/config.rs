use serde::Deserialize;
use serde_with::serde_derive::Serialize;
use serde_with::{serde_as, DurationMilliSeconds, DurationSeconds};
use std::collections::BTreeMap;
use std::time::Duration;
use url::Url;

use crate::auth::AuthConfig;
use crate::network::PrioritiesConfig;
use crate::types::DatasetRef;

#[serde_as]
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(deserialize_with = "parse_hostname")]
    pub hostname: String,

    #[serde(default = "default_max_parallel_streams")]
    pub max_parallel_streams: usize,

    /// Backoff reported to clients turned away because `max_parallel_streams` is reached.
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    #[serde(
        rename = "task_limit_retry_after_ms",
        default = "default_task_limit_retry_after"
    )]
    pub task_limit_retry_after: Duration,

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

    /// Whether to prefer the portal-oriented assignment over the legacy one when both are
    /// available (`mvcc-chunks` builds only). A runtime kill switch: can be flipped back to
    /// `false` without a rebuild if `portal_assignment` needs to be reverted.
    #[serde(default = "default_true")]
    pub prefer_portal_assignment: bool,

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

    /// Absent means no authentication at all; present, the data API requires a
    /// key. A key written with nothing under it is refused rather than folded
    /// into `None` — an open portal is the one outcome it cannot have meant.
    #[serde(default, deserialize_with = "parse_auth")]
    pub auth: Option<AuthConfig>,

    /// Keys the deserializer skipped, carried so `main` can warn about them
    /// once a tracing subscriber exists — see [`Config::read`].
    #[serde(skip)]
    pub ignored_fields: Vec<String>,
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
        Self::from_reader(std::io::BufReader::new(file))
    }

    /// Unknown keys are collected onto the config rather than logged here: this
    /// runs inside clap's `value_parser`, before `setup_tracing`, so a warning
    /// emitted now goes to a subscriber that does not exist yet and a
    /// misspelled `auth.limits` knob would keep its default without a
    /// trace. `main` replays them once tracing is up.
    fn from_reader(reader: impl std::io::Read) -> anyhow::Result<Self> {
        let deser = serde_yaml::Deserializer::from_reader(reader);
        let mut ignored = Vec::new();
        let mut collect = |path: serde_ignored::Path| ignored.push(path.to_string());
        let mut config: Self = serde_yaml::with::singleton_map_recursive::deserialize(
            serde_ignored::Deserializer::new(deser, &mut collect),
        )?;
        config.ignored_fields = ignored;
        reject_unrecognized_block_without_auth(&config)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> anyhow::Result<()> {
        self.congestion.validate()?;
        if let Some(auth) = &self.auth {
            // Resolved and discarded: applying the deployment overrides is what
            // checks them, and reading the file is where a bad block should
            // fail rather than the first request that needs a key.
            auth.resolve()?;
        }
        Ok(())
    }
}

fn default_true() -> bool {
    true
}

fn default_max_parallel_streams() -> usize {
    1024
}

fn default_task_limit_retry_after() -> Duration {
    Duration::from_millis(9999)
}

fn default_transport_timeout() -> Duration {
    Duration::from_secs(60)
}

// Must stay below caller-side request timeouts (callers default to 30s), so a stalled
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

/// Reached only when the config file actually carries an `auth` key —
/// `#[serde(default)]` answers for an absent one without coming here — so a
/// null arriving at this point was written by hand.
fn parse_auth<'de, D>(deserializer: D) -> Result<Option<AuthConfig>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    match Option::<AuthConfig>::deserialize(deserializer)? {
        Some(config) => Ok(Some(config)),
        None => Err(serde::de::Error::custom(
            "auth: the block is present but empty. Remove the key to run the portal \
             without authorization, or fill the block in — an empty one would silently serve \
             the data API to anyone.",
        )),
    }
}

/// A top-level block this build does not know, on a portal that ends up with no
/// `auth:` key, has the shape of an authorization block under a wrong name:
/// serde reads an unrecognized key as absent, and absent means the data API
/// answers anyone. Refusing to start is the only fail-closed reading — the
/// alternative is a portal that was closed yesterday quietly serving today.
///
/// Nested keys elsewhere stay a warning: the worst case is one knob keeping its
/// default. Inside `auth:` that default is a different signing identity or an
/// open door, so they are fatal too — except under `limits:`, where the
/// fallback really is just a default and a build that predates a knob would
/// otherwise refuse the config a rollback hands it.
fn reject_unrecognized_block_without_auth(config: &Config) -> anyhow::Result<()> {
    let stray: Vec<&str> = config
        .ignored_fields
        .iter()
        .filter(|path| {
            if config.auth.is_some() {
                path.starts_with("auth.") && !path.contains(".limits.")
            } else {
                !path.contains('.')
            }
        })
        .map(String::as_str)
        .collect();
    anyhow::ensure!(
        stray.is_empty(),
        "unrecognized config {}: {}. Authorization is configured under `auth:` — a key this \
         build does not know is a key that is not in force, and the value it falls back to \
         either serves the data API without a credential or signs with the wrong identity, \
         so the portal refuses to start on it.",
        if stray.len() == 1 { "key" } else { "keys" },
        stray.join(", "),
    );
    Ok(())
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

    /// The kill switch: without the block the portal is byte-for-byte an OSS
    /// portal, and `run_server` installs no authorization middleware.
    #[test]
    fn auth_is_absent_unless_configured() {
        let config: Config = serde_yaml::from_str(MINIMAL_YAML).expect("parse");
        assert!(config.auth.is_none());
    }

    /// An operator who wrote the key meant to configure something. serde folds
    /// an explicit null into `None`, which is the open portal — the single
    /// outcome nobody typing `auth:` can have intended — and nothing anywhere
    /// would have said so.
    #[test]
    fn an_empty_auth_block_is_a_config_error() {
        let yaml = format!("{MINIMAL_YAML}auth:\n");

        let err = serde_yaml::from_str::<Config>(&yaml)
            .expect_err("a null auth block must not parse as absent");
        assert!(err.to_string().contains("auth"), "got {err}");

        // The production path reads through two adapters; both must agree.
        let deser = serde_yaml::Deserializer::from_str(&yaml);
        let err = serde_yaml::with::singleton_map_recursive::deserialize::<Config, _>(
            serde_ignored::Deserializer::new(deser, &mut |_: serde_ignored::Path| {}),
        )
        .expect_err("a null auth block must not parse as absent");
        assert!(err.to_string().contains("auth"), "got {err}");

        // A block that is present but has nothing usable in it fails on the
        // field it is missing, which is the message the operator needs.
        let err = serde_yaml::from_str::<Config>(&format!("{MINIMAL_YAML}auth: {{}}\n"))
            .expect_err("an empty mapping must not parse either");
        assert!(err.to_string().contains("control_plane_url"), "got {err}");
    }

    #[test]
    fn auth_block_parses_through_the_production_deserializer() {
        // `from_reader` validates, and validation reads `PORTAL_ID` — which
        // another test mutates.
        let _guard = crate::auth::test_support::env_guard();
        let yaml = format!(
            "{MINIMAL_YAML}auth:\n  \
             control_plane_url: https://cp.example/\n  \
             portal_id: portal-premium-eu\n  \
             enforcement: log_only\n"
        );
        let config = Config::from_reader(yaml.as_bytes()).expect("parse");

        let auth = config.auth.expect("auth block");
        assert_eq!(auth.portal_id, "portal-premium-eu");
        assert_eq!(auth.enforcement, crate::auth::Enforcement::LogOnly);
        // Doubles as a lint on this fixture: a stale key here would document a
        // knob that does not exist.
        assert_eq!(config.ignored_fields, Vec::<String>::new());
    }

    /// A misspelled limit is only audible if the unknown key survives the read,
    /// which happens before tracing exists, all the way to `main`.
    #[test]
    fn unknown_config_fields_are_carried_for_later_reporting() {
        let yaml = format!("{MINIMAL_YAML}congestion:\n  min_windo: 5\n");
        let config = Config::from_reader(yaml.as_bytes()).expect("parse");

        assert_eq!(config.ignored_fields, vec!["congestion.min_windo"]);
    }

    /// Under `auth:` the same key is fatal: it is a knob that is not in force,
    /// and the identity it falls back to is registered with nobody.
    #[test]
    fn a_misspelled_key_under_auth_refuses_to_start() {
        let yaml = format!(
            "{MINIMAL_YAML}auth:\n  \
             control_plane_url: https://cp.example/\n  \
             portal_id: portal-premium-eu\n  \
             key-path: /keys/exchange.key\n"
        );

        let err = Config::from_reader(yaml.as_bytes())
            .expect_err("an unknown key under `auth:` must not read as its default");
        assert!(err.to_string().contains("key-path"), "got {err}");
    }

    /// A limit is the one thing under `auth:` whose fallback is a plain default,
    /// so it stays a warning — otherwise rolling back to a build that predates a
    /// knob turns the config that build is handed into a crash loop.
    #[test]
    fn a_misspelled_limit_under_auth_is_reported_rather_than_fatal() {
        let _guard = crate::auth::test_support::env_guard();
        let yaml = format!(
            "{MINIMAL_YAML}auth:\n  \
             control_plane_url: https://cp.example/\n  \
             portal_id: portal-premium-eu\n  \
             limits:\n    \
             max_grant_lifetime_seconds: 60\n"
        );

        let config = Config::from_reader(yaml.as_bytes()).expect("a limit typo still starts");

        assert_eq!(
            config.ignored_fields.len(),
            1,
            "{:?}",
            config.ignored_fields
        );
        assert!(
            config.ignored_fields[0].ends_with("limits.max_grant_lifetime_seconds"),
            "got {:?}",
            config.ignored_fields
        );
    }

    /// An authorization block under a name this build does not know reads as no
    /// authorization at all, which is the one outcome nobody writing one can
    /// have meant. It must refuse to start rather than serve the data API to
    /// anyone.
    #[test]
    fn an_unrecognized_top_level_block_refuses_to_start_when_nothing_authorizes() {
        let _guard = crate::auth::test_support::env_guard();
        let block = "  control_plane_url: https://cp.example/\n  \
                     portal_id: portal-premium-eu\n";

        let err = Config::from_reader(format!("{MINIMAL_YAML}authorisation:\n{block}").as_bytes())
            .expect_err("a misnamed block must not read as an open portal");
        assert!(err.to_string().contains("authorisation"), "got {err}");

        // Under the name the build knows, the same block configures the gate.
        let config = Config::from_reader(format!("{MINIMAL_YAML}auth:\n{block}").as_bytes())
            .expect("the block parses under `auth:`");
        assert!(config.auth.is_some());
    }

    /// Only the fail-open shape refuses. A portal that does authorize keeps
    /// starting with a stray key, which stays the warning it always was —
    /// refusing there would turn an unknown knob into an outage.
    #[test]
    fn a_stray_key_alongside_a_real_auth_block_is_only_a_warning() {
        let _guard = crate::auth::test_support::env_guard();
        let yaml = format!(
            "{MINIMAL_YAML}stray_key: 1\nauth:\n  \
             control_plane_url: https://cp.example/\n  \
             portal_id: portal-premium-eu\n"
        );

        let config = Config::from_reader(yaml.as_bytes()).expect("parse");

        assert!(config.auth.is_some());
        assert_eq!(config.ignored_fields, vec!["stray_key"]);
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
