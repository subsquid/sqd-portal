use std::time::Duration;

use serde::Deserialize;

/// Presence of this block turns usage measurement on. Absent, the portal
/// measures nothing, opens no connection to the usage endpoint, and serves
/// byte-for-byte what it serves without it (REQ-60, REQ-56's argument applied
/// one level down).
///
/// Nested under `auth:` rather than beside it because attribution is what makes
/// a measurement worth taking: events name the key a request was served on, and
/// that key exists only where the gate resolved a grant. It is deliberately
/// *independent of* `enforcement`, so a deployment can measure through a
/// `log_only` cutover — the two knobs answer different questions.
///
/// Nested, not flattened, for the reason [`super::super::config::Limits`] is: a
/// flattened block absorbs every unmatched key, so a misspelled knob would keep
/// its default in silence.
#[derive(Debug, Clone, Deserialize)]
pub struct UsageConfig {
    /// How long a batch waits for company before it goes out (P-USAGE-FLUSH).
    /// Bounds publication lag, not correctness: a batch is sent whenever it
    /// fills, whichever comes first.
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,

    /// Events per request to the control plane (P-USAGE-BATCH-MAX). One HTTP
    /// call per event would make the sink's cost scale with the data plane's
    /// request rate.
    #[serde(default = "default_batch_max_events")]
    pub batch_max_events: usize,

    /// Events held in memory before the hot path starts dropping them
    /// (P-USAGE-QUEUE). The bound is the whole memory story: measurement must
    /// never grow with a sink that has stopped answering (HZ-14).
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: usize,

    /// How long an undelivered event may be retried before it is dropped and
    /// counted (P-USAGE-MAX-RETRY-AGE). Retrying forever is how a sink outage
    /// becomes a queue that never drains.
    #[serde(default = "default_max_retry_age_secs")]
    pub max_retry_age_secs: u64,

    /// How much measured time one delta record may cover (P-USAGE-INTERIM).
    /// Streams here run for hours, so completion-only records would be biased
    /// exactly where the money is (ADR-016).
    #[serde(default = "default_interim_interval_secs")]
    pub interim_interval_secs: u64,
}

/// Past this tokio's channel panics inside its own constructor, so a capacity
/// meant as "unbounded" would take the process down at startup with a message
/// about a semaphore rather than about a knob.
const MAX_QUEUE_CAPACITY: usize = 1_048_576;

/// The ingest contract's batch cap (DC-9). A batch above it is refused on its
/// content, forever and identically, which turns every record behind it into a
/// counted drop.
const MAX_BATCH_MAX_EVENTS: usize = 1_000;

/// An hour, a day, a week. None of the three is a working value; they are the
/// distance a misplaced unit travels — seconds typed as milliseconds, days as
/// seconds — and past them the knob has stopped meaning what it says.
const MAX_FLUSH_INTERVAL_MS: u64 = 3_600_000;
const MAX_INTERIM_INTERVAL_SECS: u64 = 86_400;
const MAX_RETRY_AGE_SECS: u64 = 604_800;

impl UsageConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        // Zero would flush in a tight loop, turning the sink into the load.
        anyhow::ensure!(
            self.flush_interval_ms >= 1,
            "auth.usage.flush_interval_ms must be at least 1"
        );
        anyhow::ensure!(
            self.flush_interval_ms <= MAX_FLUSH_INTERVAL_MS,
            "auth.usage.flush_interval_ms must be at most {MAX_FLUSH_INTERVAL_MS}"
        );
        anyhow::ensure!(
            self.batch_max_events >= 1,
            "auth.usage.batch_max_events must be at least 1"
        );
        anyhow::ensure!(
            self.batch_max_events <= MAX_BATCH_MAX_EVENTS,
            "auth.usage.batch_max_events must be at most {MAX_BATCH_MAX_EVENTS}"
        );
        // Zero drops every event on the floor while still paying for the
        // measurement — reporting off is spelled by removing the block.
        anyhow::ensure!(
            self.queue_capacity >= 1,
            "auth.usage.queue_capacity must be at least 1"
        );
        anyhow::ensure!(
            self.queue_capacity <= MAX_QUEUE_CAPACITY,
            "auth.usage.queue_capacity must be at most {MAX_QUEUE_CAPACITY}"
        );
        anyhow::ensure!(
            self.max_retry_age_secs >= 1,
            "auth.usage.max_retry_age_secs must be at least 1"
        );
        anyhow::ensure!(
            self.max_retry_age_secs <= MAX_RETRY_AGE_SECS,
            "auth.usage.max_retry_age_secs must be at most {MAX_RETRY_AGE_SECS}"
        );
        // Zero would emit a record per polled frame.
        anyhow::ensure!(
            self.interim_interval_secs >= 1,
            "auth.usage.interim_interval_secs must be at least 1"
        );
        anyhow::ensure!(
            self.interim_interval_secs <= MAX_INTERIM_INTERVAL_SECS,
            "auth.usage.interim_interval_secs must be at most {MAX_INTERIM_INTERVAL_SECS}"
        );
        Ok(())
    }

    pub fn flush_interval(&self) -> Duration {
        Duration::from_millis(self.flush_interval_ms.max(1))
    }

    pub fn max_retry_age(&self) -> Duration {
        Duration::from_secs(self.max_retry_age_secs.max(1))
    }

    pub fn interim_interval(&self) -> Duration {
        Duration::from_secs(self.interim_interval_secs.max(1))
    }
}

impl Default for UsageConfig {
    fn default() -> Self {
        Self {
            flush_interval_ms: default_flush_interval_ms(),
            batch_max_events: default_batch_max_events(),
            queue_capacity: default_queue_capacity(),
            max_retry_age_secs: default_max_retry_age_secs(),
            interim_interval_secs: default_interim_interval_secs(),
        }
    }
}

fn default_flush_interval_ms() -> u64 {
    5_000
}

fn default_batch_max_events() -> usize {
    256
}

fn default_queue_capacity() -> usize {
    16_384
}

fn default_max_retry_age_secs() -> u64 {
    300
}

fn default_interim_interval_secs() -> u64 {
    30
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AuthConfig;

    const MINIMAL: &str = r#"
control_plane_url: https://cp.example/authority
portal_id: portal-premium-eu
"#;

    fn parse(yaml: &str) -> AuthConfig {
        serde_yaml::from_str(yaml).expect("auth config should parse")
    }

    /// The kill switch, one level down from REQ-56's: an `auth:` block with no
    /// `usage:` under it measures nothing at all.
    #[test]
    fn an_auth_block_without_a_usage_block_reports_nothing() {
        assert!(parse(MINIMAL).usage.is_none());
    }

    /// The defaults are the ratified parameter values; a drift here changes what
    /// every deployment that never set them does. A block written with nothing
    /// under it — the shape a template that rendered empty produces, and the one
    /// serde would otherwise fold into "absent" — is measurement on, not off.
    #[test]
    fn an_empty_usage_block_defaults_every_knob() {
        assert!(
            parse(&format!("{MINIMAL}usage:\n")).usage.is_some(),
            "a written block is a request to measure, whatever is under it"
        );
        let config = parse(&format!("{MINIMAL}usage: {{}}\n"));

        let usage = config.usage.expect("the block is present");
        // P-USAGE-FLUSH
        assert_eq!(usage.flush_interval(), Duration::from_secs(5));
        // P-USAGE-BATCH-MAX
        assert_eq!(usage.batch_max_events, 256);
        // P-USAGE-QUEUE
        assert_eq!(usage.queue_capacity, 16_384);
        // P-USAGE-MAX-RETRY-AGE
        assert_eq!(usage.max_retry_age(), Duration::from_secs(300));
        // P-USAGE-INTERIM
        assert_eq!(usage.interim_interval(), Duration::from_secs(30));
    }

    #[test]
    fn full_block_parses_every_knob() {
        let config = parse(&format!(
            "{MINIMAL}usage:\n  \
             flush_interval_ms: 250\n  \
             batch_max_events: 16\n  \
             queue_capacity: 64\n  \
             max_retry_age_secs: 30\n  \
             interim_interval_secs: 2\n"
        ));

        let usage = config.usage.expect("the block is present");
        assert_eq!(usage.flush_interval(), Duration::from_millis(250));
        assert_eq!(usage.batch_max_events, 16);
        assert_eq!(usage.queue_capacity, 64);
        assert_eq!(usage.max_retry_age(), Duration::from_secs(30));
        assert_eq!(usage.interim_interval(), Duration::from_secs(2));
    }

    /// Measurement is not enforcement, so it must be settable during a
    /// `log_only` cutover — which is the run the data is for.
    #[test]
    fn usage_is_independent_of_the_enforcement_mode() {
        let config = parse(&format!("{MINIMAL}enforcement: log_only\nusage: {{}}\n"));

        assert_eq!(config.enforcement, crate::auth::Enforcement::LogOnly);
        assert!(config.usage.is_some());
    }

    /// Both ends of every knob. The upper bounds are not taste: `usize::MAX`
    /// panics inside tokio's channel, a batch past the ingest's cap is refused
    /// on its content forever, and a duration a unit slip wide has stopped
    /// meaning what its name says — all three are startup failures rather than
    /// something an operator discovers from a drop counter.
    #[test]
    fn validate_rejects_out_of_range_knobs() {
        let _guard = crate::auth::test_support::env_guard();
        std::env::remove_var("PORTAL_ID");

        for mutate in [
            (|u: &mut UsageConfig| u.flush_interval_ms = 0) as fn(&mut UsageConfig),
            |u| u.batch_max_events = 0,
            |u| u.queue_capacity = 0,
            |u| u.max_retry_age_secs = 0,
            |u| u.interim_interval_secs = 0,
            |u| u.flush_interval_ms = MAX_FLUSH_INTERVAL_MS + 1,
            |u| u.batch_max_events = MAX_BATCH_MAX_EVENTS + 1,
            |u| u.queue_capacity = usize::MAX,
            |u| u.max_retry_age_secs = MAX_RETRY_AGE_SECS + 1,
            |u| u.interim_interval_secs = MAX_INTERIM_INTERVAL_SECS + 1,
        ] {
            let mut config = parse(&format!("{MINIMAL}usage: {{}}\n"));
            mutate(config.usage.as_mut().expect("the block is present"));
            assert!(
                config.resolve().is_err(),
                "{:?} should not validate",
                config.usage
            );
        }

        // The bounds themselves are legal, or the message an operator reads is
        // off by one from the rule it states.
        for mutate in [
            (|u: &mut UsageConfig| u.flush_interval_ms = MAX_FLUSH_INTERVAL_MS)
                as fn(&mut UsageConfig),
            |u| u.batch_max_events = MAX_BATCH_MAX_EVENTS,
            |u| u.queue_capacity = MAX_QUEUE_CAPACITY,
            |u| u.max_retry_age_secs = MAX_RETRY_AGE_SECS,
            |u| u.interim_interval_secs = MAX_INTERIM_INTERVAL_SECS,
        ] {
            let mut config = parse(&format!("{MINIMAL}usage: {{}}\n"));
            mutate(config.usage.as_mut().expect("the block is present"));
            assert!(
                config.resolve().is_ok(),
                "{:?} should validate",
                config.usage
            );
        }

        assert!(parse(&format!("{MINIMAL}usage: {{}}\n")).resolve().is_ok());
    }

    /// A knob nobody can misspell audibly is not operator-bindable. Flattening
    /// the block into `auth:` would swallow the unmatched key and leave the
    /// default in force, exactly as it did for the limits (ADR-008).
    #[test]
    fn a_misspelled_usage_knob_is_reported_rather_than_ignored() {
        let yaml = format!("{MINIMAL}usage:\n  flush_interval_millis: 10\n");

        let mut ignored = Vec::new();
        let deser = serde_yaml::Deserializer::from_str(&yaml);
        let config: AuthConfig = serde_ignored::deserialize(deser, |path| {
            ignored.push(path.to_string());
        })
        .expect("the block still parses");

        // The `?` is serde_ignored's marker for the custom deserializer the
        // block is read through; what matters is that the knob's own name
        // survives into the warning an operator reads.
        assert_eq!(ignored.len(), 1, "{ignored:?}");
        assert!(
            ignored[0].starts_with("usage") && ignored[0].ends_with("flush_interval_millis"),
            "got {ignored:?}"
        );
        assert_eq!(
            config
                .usage
                .expect("the block is present")
                .flush_interval_ms,
            default_flush_interval_ms()
        );
    }
}
