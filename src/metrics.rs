use std::iter;

use prometheus_client::{
    metrics::{
        counter::Counter,
        family::Family,
        gauge::Gauge,
        histogram::{exponential_buckets, Histogram},
    },
    registry::Registry,
};
use reqwest::StatusCode;
use sqd_contract_client::PeerId;

use crate::{
    commercial::{DataSource, Endpoint, UsageStatus},
    types::DatasetId,
    utils::logging::StreamStats,
};

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

lazy_static::lazy_static! {
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
    pub static ref STREAM_LOGICAL_BYTES: Family<Labels, Counter> = Default::default();
    pub static ref STREAM_WIRE_BYTES: Family<Labels, Counter> = Default::default();
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
    pub static ref COMMERCIAL_USAGE_DROPPED: Counter = Default::default();
    pub static ref COMMERCIAL_USAGE_DROPPED_AGE: Counter = Default::default();
    pub static ref COMMERCIAL_USAGE_REJECTED: Counter = Default::default();
    pub static ref COMMERCIAL_ANON_FALLBACK_BUCKET: Family<Labels, Counter> = Default::default();
    pub static ref COMMERCIAL_SNAPSHOT_AGE_SECONDS: Gauge = Default::default();
    pub static ref COMMERCIAL_SYNC_STALENESS_SECONDS: Gauge = Default::default();
    pub static ref COMMERCIAL_SNAPSHOT_RECORDS: Gauge = Default::default();
    pub static ref COMMERCIAL_SNAPSHOT_PERSIST_ERRORS: Counter = Default::default();
    pub static ref COMMERCIAL_DEFAULTS_RECOVERY_PENDING_TICKS: Counter = Default::default();
    pub static ref COMMERCIAL_SNAPSHOT_PARSE_ERRORS: Counter = Default::default();
    pub static ref COMMERCIAL_STALE_QUOTA_GRACE_ADMISSIONS: Counter = Default::default();
    pub static ref COMMERCIAL_STALE_QUOTA_GRACE_BYTES: Counter = Default::default();
    pub static ref COMMERCIAL_AUTHORIZE: Family<Labels, Counter> = Default::default();
    pub static ref COMMERCIAL_RESOLVE: Family<Labels, Counter> = Default::default();
    pub static ref COMMERCIAL_USAGE_BUFFER_LEN: Gauge = Default::default();
    pub static ref COMMERCIAL_CUTOFFS: Family<Labels, Counter> = Default::default();
    pub static ref COMMERCIAL_ZERO_VALUED_LIMITS: Family<Labels, Counter> = Default::default();
    pub static ref COMMERCIAL_PENDING_COMPRESSED_BUFFER_WARNINGS: Family<Labels, Counter> = Default::default();
    pub static ref COMMERCIAL_THROTTLE_STALL_SECONDS: Histogram =
        Histogram::new(exponential_buckets(0.01, 2.0, 20));

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

pub fn report_commercial_stream_bytes(
    endpoint: &Endpoint,
    data_source: &DataSource,
    logical: u64,
    wire: u64,
) {
    let labels = vec![
        ("endpoint".to_owned(), endpoint_label(endpoint).to_owned()),
        (
            "data_source".to_owned(),
            data_source_label(data_source).to_owned(),
        ),
    ];
    STREAM_LOGICAL_BYTES.get_or_create(&labels).inc_by(logical);
    STREAM_WIRE_BYTES.get_or_create(&labels).inc_by(wire);
}

pub fn report_commercial_usage_dropped() {
    COMMERCIAL_USAGE_DROPPED.inc();
}

pub fn report_commercial_usage_dropped_age(count: u64) {
    COMMERCIAL_USAGE_DROPPED_AGE.inc_by(count);
}

pub fn report_commercial_usage_rejected(count: u64) {
    COMMERCIAL_USAGE_REJECTED.inc_by(count);
}

pub fn report_commercial_anon_fallback_bucket(bucket: &str) {
    COMMERCIAL_ANON_FALLBACK_BUCKET
        .get_or_create(&vec![("bucket".to_owned(), bucket.to_owned())])
        .inc();
}

pub fn set_commercial_snapshot_age(seconds: i64) {
    COMMERCIAL_SNAPSHOT_AGE_SECONDS.set(seconds);
}

pub fn set_commercial_sync_staleness(seconds: i64) {
    COMMERCIAL_SYNC_STALENESS_SECONDS.set(seconds);
}

pub fn set_commercial_snapshot_records(records: i64) {
    COMMERCIAL_SNAPSHOT_RECORDS.set(records);
}

pub fn report_commercial_snapshot_persist_error() {
    COMMERCIAL_SNAPSHOT_PERSIST_ERRORS.inc();
}

pub fn report_commercial_defaults_recovery_pending_tick() {
    COMMERCIAL_DEFAULTS_RECOVERY_PENDING_TICKS.inc();
}

pub fn report_commercial_snapshot_parse_error() {
    COMMERCIAL_SNAPSHOT_PARSE_ERRORS.inc();
}

pub fn report_commercial_stale_quota_grace_admission() {
    COMMERCIAL_STALE_QUOTA_GRACE_ADMISSIONS.inc();
}

pub fn report_commercial_stale_quota_grace_bytes(bytes: u64) {
    COMMERCIAL_STALE_QUOTA_GRACE_BYTES.inc_by(bytes);
}

pub fn report_commercial_authorize(outcome: &str, reason: &str) {
    COMMERCIAL_AUTHORIZE
        .get_or_create(&vec![
            ("outcome".to_owned(), outcome.to_owned()),
            ("reason".to_owned(), reason.to_owned()),
        ])
        .inc();
}

pub fn report_commercial_resolve(result: &str) {
    COMMERCIAL_RESOLVE
        .get_or_create(&vec![("result".to_owned(), result.to_owned())])
        .inc();
}

pub fn set_commercial_usage_buffer_len(len: i64) {
    COMMERCIAL_USAGE_BUFFER_LEN.set(len);
}

pub fn report_commercial_cutoff(status: &UsageStatus) {
    COMMERCIAL_CUTOFFS
        .get_or_create(&vec![(
            "status".to_owned(),
            usage_status_label(status).to_owned(),
        )])
        .inc();
}

pub fn report_commercial_pending_compressed_buffer_warning(compression: &str) {
    COMMERCIAL_PENDING_COMPRESSED_BUFFER_WARNINGS
        .get_or_create(&vec![("compression".to_owned(), compression.to_owned())])
        .inc();
}

pub fn report_commercial_zero_valued_limit(limit: &str) {
    COMMERCIAL_ZERO_VALUED_LIMITS
        .get_or_create(&vec![("limit".to_owned(), limit.to_owned())])
        .inc();
}

pub fn observe_commercial_throttle_stall(duration: std::time::Duration) {
    COMMERCIAL_THROTTLE_STALL_SECONDS.observe(duration.as_secs_f64());
}

fn endpoint_label(endpoint: &Endpoint) -> &'static str {
    match endpoint {
        Endpoint::Stream => "stream",
        Endpoint::FinalizedStream => "finalized_stream",
        Endpoint::ArchivalStream => "archival_stream",
        Endpoint::SqlQuery => "sql_query",
        Endpoint::TsLookup => "ts_lookup",
        Endpoint::LegacyQuery => "legacy_query",
    }
}

fn data_source_label(data_source: &DataSource) -> &'static str {
    match data_source {
        DataSource::Network => "network",
        DataSource::RealTime => "real_time",
    }
}

fn usage_status_label(status: &UsageStatus) -> &'static str {
    match status {
        UsageStatus::Completed => "completed",
        UsageStatus::ClientDisconnect => "client_disconnect",
        UsageStatus::CutQuota => "cut_quota",
        UsageStatus::CutMaxBytes => "cut_max_bytes",
        UsageStatus::CutSuspended => "cut_suspended",
        UsageStatus::CutShutdown => "cut_shutdown",
        UsageStatus::Error => "error",
    }
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

pub fn register_metrics(registry: &mut Registry) {
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
        "stream_logical_bytes",
        "Logical uncompressed bytes emitted by commercial shadow metering",
        STREAM_LOGICAL_BYTES.clone(),
    );
    registry.register(
        "stream_wire_bytes",
        "Wire bytes emitted by commercial shadow metering",
        STREAM_WIRE_BYTES.clone(),
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
        "commercial_usage_dropped_total",
        "Commercial usage events dropped by the bounded reporter buffer",
        COMMERCIAL_USAGE_DROPPED.clone(),
    );
    registry.register(
        "commercial_usage_dropped_age_total",
        "Commercial usage events dropped after exceeding the reporter retry-age cap",
        COMMERCIAL_USAGE_DROPPED_AGE.clone(),
    );
    registry.register(
        "commercial_usage_rejected_total",
        "Commercial usage events rejected by control-plane validation",
        COMMERCIAL_USAGE_REJECTED.clone(),
    );
    registry.register(
        "commercial_anon_fallback_bucket_total",
        "Anonymous commercial requests attributed to fallback local/invalid IP buckets",
        COMMERCIAL_ANON_FALLBACK_BUCKET.clone(),
    );
    registry.register(
        "commercial_snapshot_age_seconds",
        "Age of the commercial snapshot disk cache currently loaded by the portal",
        COMMERCIAL_SNAPSHOT_AGE_SECONDS.clone(),
    );
    registry.register(
        "commercial_sync_staleness_seconds",
        "Seconds since the latest successful commercial snapshot sync",
        COMMERCIAL_SYNC_STALENESS_SECONDS.clone(),
    );
    registry.register(
        "commercial_snapshot_records",
        "Number of key records in the commercial snapshot store",
        COMMERCIAL_SNAPSHOT_RECORDS.clone(),
    );
    registry.register(
        "commercial_snapshot_persist_errors_total",
        "Commercial snapshot disk-cache persistence failures",
        COMMERCIAL_SNAPSHOT_PERSIST_ERRORS.clone(),
    );
    registry.register(
        "commercial_defaults_recovery_pending_ticks_total",
        "Commercial sync ticks that started with authoritative defaults recovery pending",
        COMMERCIAL_DEFAULTS_RECOVERY_PENDING_TICKS.clone(),
    );
    registry.register(
        "commercial_snapshot_parse_errors_total",
        "Commercial snapshot cache or record parse failures",
        COMMERCIAL_SNAPSHOT_PARSE_ERRORS.clone(),
    );
    registry.register(
        "commercial_stale_quota_grace_admissions_total",
        "Commercial requests admitted while an exhausted quota snapshot is stale",
        COMMERCIAL_STALE_QUOTA_GRACE_ADMISSIONS.clone(),
    );
    registry.register(
        "commercial_stale_quota_grace_bytes_total",
        "Logical bytes served under stale commercial quota grace",
        COMMERCIAL_STALE_QUOTA_GRACE_BYTES.clone(),
    );
    registry.register(
        "commercial_authorize_total",
        "Commercial authorization outcomes by reason",
        COMMERCIAL_AUTHORIZE.clone(),
    );
    registry.register(
        "commercial_resolve_total",
        "Commercial on-miss resolve outcomes",
        COMMERCIAL_RESOLVE.clone(),
    );
    registry.register(
        "commercial_usage_buffer_len",
        "Commercial usage reporter buffered event count",
        COMMERCIAL_USAGE_BUFFER_LEN.clone(),
    );
    registry.register(
        "commercial_cutoffs_total",
        "Commercial stream cutoff count by terminal status",
        COMMERCIAL_CUTOFFS.clone(),
    );
    registry.register(
        "commercial_pending_compressed_buffer_warning_total",
        "Commercial streams whose pending compressed buffer crossed the early warning threshold",
        COMMERCIAL_PENDING_COMPRESSED_BUFFER_WARNINGS.clone(),
    );
    registry.register(
        "commercial_zero_valued_limits_total",
        "Commercial grants carrying zero-valued fail-open limits",
        COMMERCIAL_ZERO_VALUED_LIMITS.clone(),
    );
    registry.register(
        "commercial_throttle_stall_seconds",
        "Commercial throttle stalls before response chunks",
        COMMERCIAL_THROTTLE_STALL_SECONDS.clone(),
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
