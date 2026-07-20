use std::{
    iter,
    sync::{
        atomic::{AtomicI64, Ordering},
        LazyLock,
    },
    time::{Duration, Instant},
};

use prometheus_client::{
    collector::Collector,
    encoding::{DescriptorEncoder, EncodeMetric},
    metrics::{
        counter::Counter,
        family::Family,
        gauge::{ConstGauge, Gauge},
        histogram::{exponential_buckets, Histogram},
    },
    registry::Registry,
};
use reqwest::StatusCode;
use sqd_contract_client::PeerId;

use crate::{types::DatasetId, utils::logging::StreamStats};

pub enum MutexLockMode {
    Read,
    Write,
}

impl std::fmt::Display for MutexLockMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MutexLockMode::Read => f.write_str("read"),
            MutexLockMode::Write => f.write_str("write"),
        }
    }
}

type Labels = Vec<(String, String)>;

fn buckets(start: f64, count: usize) -> impl Iterator<Item = f64> {
    iter::successors(Some(start), |x| Some(x * 10.))
        .flat_map(|x| [x, x * 1.5, x * 2.5, x * 5.0])
        .take(count)
}

/// Monotonic: a wall-clock step must not make a stale artifact look fresh.
static ORIGIN: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Seconds after [`ORIGIN`] at which the applied assignment was installed (OB-6).
static ASSIGNMENT_APPLIED_AT: AtomicI64 = AtomicI64::new(NEVER_APPLIED);

const NEVER_APPLIED: i64 = i64::MIN;

lazy_static::lazy_static! {
    pub static ref ASSIGNMENT_REFRESHES: Family<Labels, Counter> = Default::default();

    pub static ref KNOWN_WORKERS: Gauge = Default::default();
    pub static ref AVAILABLE_COMPUTE_UNITS: Gauge = Default::default();

    pub static ref HTTP_STATUS: Family<Labels, Counter> = Default::default();
    pub static ref HTTP_TTFB: Family<Labels, Histogram> =
        Family::new_with_constructor(|| Histogram::new(buckets(0.001, 20)));

    pub static ref QUERIES_SENT: Family<Labels, Counter> = Default::default();
    pub static ref QUERIES_RUNNING: Gauge = Default::default();
    static ref QUERY_RESULTS: Family<Labels, Counter> = Default::default();
    static ref QUERY_BACKOFF: Family<Labels, Counter> = Default::default();
    static ref QUERY_DURATIONS: Histogram = Histogram::new(exponential_buckets(0.001, 2.0, 20));
    static ref WORKER_PICKED: Family<Labels, Counter> = Default::default();

    pub static ref ACTIVE_STREAMS: Gauge = Default::default();
    pub static ref COMPLETED_STREAMS: Counter = Default::default();
    pub static ref STREAM_DURATIONS: Family<Labels, Histogram> =
        Family::new_with_constructor(|| Histogram::new(exponential_buckets(0.01, 2.0, 20)));
    pub static ref STREAM_BYTES: Family<Labels, Histogram> =
        Family::new_with_constructor(|| Histogram::new(exponential_buckets(1000., 2.0, 20)));
    pub static ref STREAM_BLOCKS: Family<Labels, Histogram> =
        Family::new_with_constructor(|| Histogram::new(exponential_buckets(1., 2.0, 30)));
    pub static ref STREAM_CHUNKS: Family<Labels, Histogram> =
        Family::new_with_constructor(|| Histogram::new(buckets(1., 20)));
    pub static ref STREAM_MAX_CHUNK_PARTS: Family<Labels, Histogram> =
        Family::new_with_constructor(|| Histogram::new(buckets(1., 20)));
    pub static ref STREAM_BYTES_PER_SECOND: Histogram = Histogram::new(exponential_buckets(100., 3.0, 20));
    pub static ref STREAM_BLOCKS_PER_SECOND: Family<Labels, Histogram> =
        Family::new_with_constructor(|| Histogram::new(exponential_buckets(1., 3.0, 20)));
    pub static ref STREAM_THROTTLED_RATIO: Histogram = Histogram::new(iter::empty());

    pub static ref CONGESTION_WINDOW: Gauge = Default::default();
    pub static ref CONGESTION_IN_FLIGHT: Gauge = Default::default();
    pub static ref CONGESTION_SHRINKS: Counter = Default::default();

    static ref KNOWN_CHUNKS: Family<Labels, Gauge> = Default::default();
    static ref LAST_STORAGE_BLOCK: Family<Labels, Gauge> = Default::default();

    // TODO: add metrics for procedure durations
    static ref MUTEX_HELD_NANOS: Family<Labels, Counter> = Default::default();
    static ref MUTEXES_EXISTING: Family<Labels, Gauge> = Default::default();
}

pub fn report_query_result(worker: &PeerId, status: &str) {
    QUERY_RESULTS
        .get_or_create(&vec![
            ("worker".to_owned(), worker.to_string()),
            ("status".to_owned(), status.to_owned()),
        ])
        .inc();
}

pub fn report_query_ok(duration: std::time::Duration) {
    QUERY_DURATIONS.observe(duration.as_secs_f64());
}

pub fn report_worker_picked(worker: &PeerId, priority: &str) {
    WORKER_PICKED
        .get_or_create(&vec![
            ("worker".to_owned(), worker.to_string()),
            ("priority".to_owned(), priority.to_owned()),
        ])
        .inc();
}

pub fn report_backoff(worker: &PeerId) {
    QUERY_BACKOFF
        .get_or_create(&vec![("worker".to_owned(), worker.to_string())])
        .inc();
}

pub fn report_http_response(
    endpoint: String,
    status: StatusCode,
    data_source: String,
    seconds_to_first_byte: f64,
) {
    let labels = vec![
        ("endpoint".to_owned(), endpoint.clone()),
        ("status".to_owned(), status.as_str().to_owned()),
        ("data_source".to_owned(), data_source),
    ];
    HTTP_STATUS.get_or_create(&labels).inc();
    HTTP_TTFB
        .get_or_create(&labels)
        .observe(seconds_to_first_byte);
}

pub fn report_stream_completed(
    stats: &StreamStats,
    dataset_id: &DatasetId,
    dataset_name: Option<&str>,
) {
    let mut labels = vec![("dataset_id".to_owned(), dataset_id.to_url().to_owned())];
    if let Some(name) = dataset_name {
        labels.push(("dataset_name".to_owned(), name.to_owned()));
    }
    let duration = stats.start_time.elapsed().as_secs_f64();
    let throttled = stats.throttled_for.as_secs_f64();
    let bytes = stats.response_bytes;
    let blocks = stats.response_blocks;
    let chunks = stats.chunks_downloaded;
    let max_chunk_parts = stats.max_chunk_parts;
    STREAM_DURATIONS.get_or_create(&labels).observe(duration);
    STREAM_BYTES.get_or_create(&labels).observe(bytes as f64);
    STREAM_BLOCKS.get_or_create(&labels).observe(blocks as f64);
    STREAM_CHUNKS.get_or_create(&labels).observe(chunks as f64);
    STREAM_MAX_CHUNK_PARTS
        .get_or_create(&labels)
        .observe(max_chunk_parts as f64);
    STREAM_BYTES_PER_SECOND.observe(bytes as f64 / duration);
    STREAM_BLOCKS_PER_SECOND
        .get_or_create(&labels)
        .observe(blocks as f64 / duration);
    STREAM_THROTTLED_RATIO.observe(throttled / duration);
}

pub fn report_chunk_list_updated(
    dataset_id: &DatasetId,
    dataset_name: Option<String>,
    total_chunks: usize,
    last_block: u64,
) {
    let mut labels = vec![("dataset_id".to_owned(), dataset_id.to_url().to_owned())];
    if let Some(name) = dataset_name {
        labels.push(("dataset_name".to_owned(), name));
    }
    KNOWN_CHUNKS.get_or_create(&labels).set(total_chunks as i64);
    LAST_STORAGE_BLOCK
        .get_or_create(&labels)
        .set(last_block as i64);
}

pub fn report_mutex_created(name: &'static str) {
    MUTEXES_EXISTING
        .get_or_create(&vec![("name".to_owned(), name.to_owned())])
        .inc();
}

pub fn report_mutex_destroyed(name: &'static str) {
    MUTEXES_EXISTING
        .get_or_create(&vec![("name".to_owned(), name.to_owned())])
        .dec();
}

pub fn report_mutex_held_duration(
    name: &'static str,
    duration: std::time::Duration,
    mode: MutexLockMode,
) {
    MUTEX_HELD_NANOS
        .get_or_create(&vec![
            ("name".to_owned(), name.to_owned()),
            ("mode".to_owned(), mode.to_string()),
        ])
        .inc_by(duration.as_nanos() as u64);
}

/// The outcome of one assignment refresh cycle (OB-6 counts, OB-9 alarms).
#[derive(Debug)]
pub enum AssignmentRefresh {
    Applied,
    Unchanged,
    /// Effective-from earlier than the applied artifact's (DEF-4, INV-2).
    Regressive,
    FetchFailed,
    /// Structurally invalid — never applied (REQ-26, FM-2).
    Invalid,
}

impl AssignmentRefresh {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Unchanged => "unchanged",
            Self::Regressive => "regressive",
            Self::FetchFailed => "fetch_failed",
            Self::Invalid => "invalid",
        }
    }
}

pub fn report_assignment_refresh(outcome: AssignmentRefresh) {
    if matches!(outcome, AssignmentRefresh::Applied) {
        ASSIGNMENT_APPLIED_AT.store(ORIGIN.elapsed().as_secs() as i64, Ordering::Relaxed);
    }
    ASSIGNMENT_REFRESHES
        .get_or_create(&vec![("outcome".to_owned(), outcome.as_str().to_owned())])
        .inc();
}

fn assignment_age() -> Option<Duration> {
    let applied_at = ASSIGNMENT_APPLIED_AT.load(Ordering::Relaxed);
    if applied_at == NEVER_APPLIED {
        return None;
    }
    let now = ORIGIN.elapsed().as_secs() as i64;
    Some(Duration::from_secs(
        now.saturating_sub(applied_at).max(0) as u64
    ))
}

/// Read at scrape time: a gauge the refresh loop writes would freeze at its last
/// value, which is the failure this exists to expose (ADR-013, OB-6, GAP-2).
#[derive(Debug)]
struct AssignmentAge {
    max_age: Duration,
}

impl Collector for AssignmentAge {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        // No artifact yet: readiness already fails with no workers known.
        let Some(age) = assignment_age() else {
            return Ok(());
        };

        let seconds = ConstGauge::new(age.as_secs() as i64);
        seconds.encode(encoder.encode_descriptor(
            "assignment_age_seconds",
            "Time since the applied assignment artifact was installed",
            None,
            seconds.metric_type(),
        )?)?;

        let stale = ConstGauge::new(i64::from(age > self.max_age));
        stale.encode(encoder.encode_descriptor(
            "assignment_stale",
            "1 while the applied assignment is older than assignment_max_age_sec",
            None,
            stale.metric_type(),
        )?)?;

        Ok(())
    }
}

pub fn register_metrics(registry: &mut Registry, assignment_max_age: Duration) {
    registry.register(
        "assignment_refreshes",
        "Assignment refresh cycles by outcome",
        ASSIGNMENT_REFRESHES.clone(),
    );
    registry.register_collector(Box::new(AssignmentAge {
        max_age: assignment_max_age,
    }));
    registry.register(
        "http_status",
        "Number of sent HTTP responses",
        HTTP_STATUS.clone(),
    );
    registry.register(
        "http_seconds_to_first_byte",
        "Time to first byte of HTTP responses",
        HTTP_TTFB.clone(),
    );
    registry.register(
        "queries_sent",
        "Number of sent queries",
        QUERIES_SENT.clone(),
    );
    registry.register(
        "queries_running",
        "Number of sent queries",
        QUERIES_RUNNING.clone(),
    );
    registry.register(
        "queries_responded",
        "Number of received responses",
        QUERY_RESULTS.clone(),
    );
    registry.register(
        "queries_backoff_hints",
        "Number of times the RPS limit has been hit",
        QUERY_BACKOFF.clone(),
    );
    registry.register(
        "query_durations_seconds",
        "Durations of successful queries",
        QUERY_DURATIONS.clone(),
    );
    registry.register(
        "worker_picked",
        "Number of times a worker was picked with a given priority",
        WORKER_PICKED.clone(),
    );
    registry.register(
        "known_workers",
        "Number of workers seen in the network",
        KNOWN_WORKERS.clone(),
    );
    registry.register(
        "available_compute_units",
        "Compute units available to this portal per epoch",
        AVAILABLE_COMPUTE_UNITS.clone(),
    );
    registry.register(
        "streams_active",
        "Number of currently running streams",
        ACTIVE_STREAMS.clone(),
    );
    registry.register(
        "streams_completed",
        "Number of completed streams",
        COMPLETED_STREAMS.clone(),
    );
    registry.register(
        "stream_duration_seconds",
        "Durations of completed streams",
        STREAM_DURATIONS.clone(),
    );
    registry.register(
        "stream_bytes",
        "Numbers of bytes per stream",
        STREAM_BYTES.clone(),
    );
    registry.register(
        "stream_blocks",
        "Numbers of blocks per stream",
        STREAM_BLOCKS.clone(),
    );
    registry.register(
        "stream_chunks",
        "Numbers of chunks per stream",
        STREAM_CHUNKS.clone(),
    );
    registry.register(
        "stream_max_chunk_parts",
        "Maximum number of stored response parts for a single chunk in a completed stream",
        STREAM_MAX_CHUNK_PARTS.clone(),
    );
    registry.register(
        "stream_bytes_per_second",
        "Completed streams bandwidth",
        STREAM_BYTES_PER_SECOND.clone(),
    );
    registry.register(
        "stream_blocks_per_second",
        "Completed streams speed in blocks",
        STREAM_BLOCKS_PER_SECOND.clone(),
    );
    registry.register(
        "stream_throttled_ratio",
        "Throttled time of completed streams relative to their duration",
        STREAM_THROTTLED_RATIO.clone(),
    );

    registry.register(
        "congestion_window",
        "Current congestion control window size",
        CONGESTION_WINDOW.clone(),
    );
    registry.register(
        "congestion_in_flight",
        "Number of responses currently being read",
        CONGESTION_IN_FLIGHT.clone(),
    );
    registry.register(
        "congestion_shrinks",
        "Number of times the congestion window was shrunk",
        CONGESTION_SHRINKS.clone(),
    );

    registry.register(
        "dataset_storage_known_chunks",
        "The total chunks number in the persistent storage",
        KNOWN_CHUNKS.clone(),
    );
    registry.register(
        "dataset_storage_highest_block",
        "The highest block existing in the persistent storage",
        LAST_STORAGE_BLOCK.clone(),
    );
    registry.register(
        "mutex_held_nanos",
        "Time spent holding the mutex",
        MUTEX_HELD_NANOS.clone(),
    );
    registry.register(
        "mutexes_existing",
        "Number of existing mutexes",
        MUTEXES_EXISTING.clone(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serialised: these write `ASSIGNMENT_APPLIED_AT` directly.
    fn scrape_with_age(applied_secs_ago: Option<i64>, max_age: Duration) -> String {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());

        ASSIGNMENT_APPLIED_AT.store(
            match applied_secs_ago {
                Some(ago) => ORIGIN.elapsed().as_secs() as i64 - ago,
                None => NEVER_APPLIED,
            },
            Ordering::Relaxed,
        );

        let mut registry = Registry::default();
        registry
            .sub_registry_with_prefix("portal")
            .register_collector(Box::new(AssignmentAge { max_age }));

        let mut buffer = String::new();
        prometheus_client::encoding::text::encode(&mut buffer, &registry).unwrap();
        buffer
    }

    #[test]
    fn age_is_reported_and_not_yet_stale() {
        let scrape = scrape_with_age(Some(60), Duration::from_secs(900));

        assert!(
            scrape.contains("portal_assignment_age_seconds 60\n"),
            "{scrape}"
        );
        assert!(scrape.contains("portal_assignment_stale 0\n"), "{scrape}");
    }

    /// The degraded signal of ADR-013: `/ready` stays green, this flips.
    #[test]
    fn stale_flips_past_max_age() {
        let scrape = scrape_with_age(Some(1000), Duration::from_secs(900));

        assert!(
            scrape.contains("portal_assignment_age_seconds 1000\n"),
            "{scrape}"
        );
        assert!(scrape.contains("portal_assignment_stale 1\n"), "{scrape}");
    }

    /// An age of zero here would read as freshness.
    #[test]
    fn nothing_is_reported_before_the_first_artifact() {
        let scrape = scrape_with_age(None, Duration::from_secs(900));

        assert!(!scrape.contains("assignment_age_seconds"), "{scrape}");
        assert!(!scrape.contains("assignment_stale"), "{scrape}");
    }
}
