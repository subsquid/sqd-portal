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

use crate::{types::DatasetId, utils::logging::StreamStats};

pub enum MutexLockMode {
    Read,
    Write,
}

impl ToString for MutexLockMode {
    fn to_string(&self) -> String {
        match self {
            MutexLockMode::Read => "read".to_owned(),
            MutexLockMode::Write => "write".to_owned(),
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
    pub static ref STREAM_BYTES_PER_SECOND: Histogram = Histogram::new(exponential_buckets(100., 3.0, 20));
    pub static ref STREAM_BLOCKS_PER_SECOND: Family<Labels, Histogram> =
        Family::new_with_constructor(|| Histogram::new(exponential_buckets(1., 3.0, 20)));
    pub static ref STREAM_THROTTLED_RATIO: Histogram = Histogram::new(iter::empty());

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

pub fn report_http_response(endpoint: String, status: StatusCode, seconds_to_first_byte: f64) {
    let labels = vec![
        ("endpoint".to_owned(), endpoint.clone()),
        ("status".to_owned(), status.as_str().to_owned()),
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
    STREAM_DURATIONS.get_or_create(&labels).observe(duration);
    STREAM_BYTES.get_or_create(&labels).observe(bytes as f64);
    STREAM_BLOCKS.get_or_create(&labels).observe(blocks as f64);
    STREAM_CHUNKS.get_or_create(&labels).observe(chunks as f64);
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
