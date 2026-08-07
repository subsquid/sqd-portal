use std::{iter, time::Duration};

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
    types::{DatasetId, ErrorCode, ErrorType},
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

/// Capacity that caused an overloaded stream refusal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RefusalReason {
    /// `max_parallel_streams`: this pod's slot cap.
    TaskLimit,
    /// Download headroom, `congestion.headroom_threshold`.
    Bandwidth,
    /// Every worker is backing off beyond the stream's wait limit.
    WorkersPaused,
    /// Every attempt at a chunk was refused for capacity.
    WorkersRateLimited,
}

impl RefusalReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TaskLimit => "task_limit",
            Self::Bandwidth => "bandwidth",
            Self::WorkersPaused => "workers_paused",
            Self::WorkersRateLimited => "workers_rate_limited",
        }
    }
}

/// What one evaluation may say on the keyless scrape (OB-12). Never more than
/// the caller's own response told it (INV-39).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthDecision {
    /// Enforcing, and the request was served.
    Admit,
    /// Enforcing, and this is the code the caller received.
    Reject(ErrorCode),
    /// Shadow mode: valid, invalid and indeterminate alike, since all were served.
    ShadowEvaluated,
}

impl AuthDecision {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Admit => "admit",
            Self::Reject(_) => "reject",
            Self::ShadowEvaluated => "shadow_evaluated",
        }
    }
}

/// How one credential exchange ended (OB-13). Keyless-scrape safe: an unknown
/// key id and a wrong secret both miss and land here identically, so all a
/// caller learns is cache membership, which timing already discloses (GAP-32).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeOutcome {
    /// The control plane answered. Issued and refused are one class here: split,
    /// they let a caller bracket two scrapes and read the verdict off the counter
    /// (OB-13, INV-39).
    Answered,
    /// The local budget refused to make the call — rate or in-flight cap.
    Saturated,
    /// The call failed, timed out, or could not be read.
    Failed,
}

impl ExchangeOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Answered => "answered",
            Self::Saturated => "saturated",
            Self::Failed => "failed",
        }
    }
}

/// Final transport outcome of one logical DC-4 request (ADR-015, OB-4).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HotblocksRequestOutcome {
    /// The first attempt obtained a response head.
    Response,
    /// A replay obtained a response head.
    ReplayResponse,
    /// A replay completed with another transport fault.
    ReplayFailed,
    /// The caller went away during the first attempt.
    Canceled,
    /// The caller went away during the replay.
    ReplayCanceled,
    /// The first attempt exhausted its read-idle budget and was not replayed.
    Timeout,
    /// The first attempt failed for another non-replayable transport reason.
    TransportFailed,
}

impl HotblocksRequestOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Response => "response",
            Self::ReplayResponse => "replay_response",
            Self::ReplayFailed => "replay_failed",
            Self::Canceled => "canceled",
            Self::ReplayCanceled => "replay_canceled",
            Self::Timeout => "timeout",
            Self::TransportFailed => "transport_failed",
        }
    }
}

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
    static ref REFUSED_STREAMS: Family<Labels, Counter> = Default::default();
    static ref STREAM_SECONDS: Counter<f64> = Default::default();
    static ref SATURATED_SECONDS: Counter<f64> = Default::default();
    pub static ref STREAMS_LIMIT: Gauge = Default::default();
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

    static ref HOTBLOCKS_REQUESTS: Family<Labels, Counter> = Default::default();

    pub static ref CONGESTION_WINDOW: Gauge = Default::default();
    pub static ref CONGESTION_IN_FLIGHT: Gauge = Default::default();
    pub static ref CONGESTION_SHRINKS: Counter = Default::default();

    static ref KNOWN_CHUNKS: Family<Labels, Gauge> = Default::default();
    static ref LAST_STORAGE_BLOCK: Family<Labels, Gauge> = Default::default();

    // Authorizing deployments only: inert without an `auth:` block.
    static ref AUTH_DECISIONS: Family<Labels, Counter> = Default::default();
    static ref EXCHANGES: Family<Labels, Counter> = Default::default();
    static ref EXCHANGE_DURATION: Histogram =
        Histogram::new([0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0].into_iter());
    static ref EXCHANGE_SUCCESS_AGE: Gauge = Default::default();
    static ref GRANT_CACHE_ENTRIES: Gauge = Default::default();
    static ref GRANT_CACHE_CAPACITY: Gauge = Default::default();
    static ref GRANT_CACHE_EVICTIONS: Counter = Default::default();
    static ref GRANT_LIFETIMES_CAPPED: Counter = Default::default();
    static ref GRACE_ADMISSIONS: Counter = Default::default();
    static ref GRANTS_IN_GRACE: Gauge = Default::default();
    static ref GRACE_MIN_REMAINING: Gauge = Default::default();

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

/// Records exactly one final transport outcome for a logical DC-4 request.
pub fn report_hotblocks_request(outcome: HotblocksRequestOutcome) {
    HOTBLOCKS_REQUESTS
        .get_or_create(&hotblocks_request_labels(outcome))
        .inc();
}

fn hotblocks_request_labels(outcome: HotblocksRequestOutcome) -> Labels {
    vec![("outcome".to_owned(), outcome.as_str().to_owned())]
}

#[cfg(test)]
pub fn hotblocks_requests(outcome: HotblocksRequestOutcome) -> u64 {
    HOTBLOCKS_REQUESTS
        .get_or_create(&hotblocks_request_labels(outcome))
        .get()
}

/// Count one authorization evaluation (OB-12).
pub fn report_auth_decision(decision: AuthDecision, enforcement: &str) {
    AUTH_DECISIONS
        .get_or_create(&auth_decision_labels(decision, enforcement))
        .inc();
}

/// The whole public projection of one evaluation, in one place.
#[cfg_attr(test, allow(dead_code))]
pub(crate) fn auth_decision_labels(decision: AuthDecision, enforcement: &str) -> Labels {
    let mut labels = vec![
        ("decision".to_owned(), decision.as_str().to_owned()),
        ("enforcement".to_owned(), enforcement.to_owned()),
    ];
    // Only a refusal carries a code, and only the one the caller received.
    if let AuthDecision::Reject(code) = decision {
        labels.push(("error_code".to_owned(), code.as_str().to_owned()));
        labels.push((
            "error_type".to_owned(),
            code.error_type().as_str().to_owned(),
        ));
    }
    labels
}

#[cfg(test)]
pub fn auth_decisions(decision: AuthDecision, enforcement: &str) -> u64 {
    AUTH_DECISIONS
        .get_or_create(&auth_decision_labels(decision, enforcement))
        .get()
}

/// Count one credential exchange and how long it took (OB-13).
pub fn report_exchange(outcome: ExchangeOutcome, elapsed: Option<Duration>) {
    EXCHANGES
        .get_or_create(&vec![("outcome".to_owned(), outcome.as_str().to_owned())])
        .inc();
    if let Some(elapsed) = elapsed {
        EXCHANGE_DURATION.observe(elapsed.as_secs_f64());
    }
}

#[cfg(test)]
pub fn exchanges(outcome: ExchangeOutcome) -> u64 {
    EXCHANGES
        .get_or_create(&vec![("outcome".to_owned(), outcome.as_str().to_owned())])
        .get()
}

/// Seconds since the control plane last answered anything. Republished on
/// scrape rather than on exchange, so it climbs through an outage instead of
/// freezing at the last value it happened to reach (OB-13).
pub fn report_exchange_success_age(age_seconds: u64) {
    EXCHANGE_SUCCESS_AGE.set(age_seconds as i64);
}

/// Occupancy, and the bound it is measured against — publishing the cap keeps
/// its literal out of the alert expression (OB-11's argument, applied here).
pub fn report_grant_cache_size(entries: usize) {
    GRANT_CACHE_ENTRIES.set(entries as i64);
}

pub fn report_grant_cache_capacity(capacity: usize) {
    GRANT_CACHE_CAPACITY.set(capacity as i64);
}

/// A live grant pushed out to make room. Sustained eviction is the HZ-13
/// capacity signal: the credential working set is larger than the cache.
pub fn report_grant_eviction() {
    GRANT_CACHE_EVICTIONS.inc();
}

/// The control plane offered a lifetime past the portal's ceiling. Not a
/// failure — the grant is honoured, shortened — but a misconfiguration an
/// operator should see before it becomes an incident.
pub fn report_lifetime_capped() {
    GRANT_LIFETIMES_CAPPED.inc();
}

/// A request served on a grant whose renewal has not landed. The leading edge
/// of the `expires_at` cliff, and the only warning before it (OB-9).
pub fn report_grace_admission() {
    GRACE_ADMISSIONS.inc();
}

#[cfg(test)]
pub fn grace_admissions() -> u64 {
    GRACE_ADMISSIONS.get()
}

/// Point-in-time census of the `expires_at` cliff, republished on scrape: how
/// many grants are serving on renewal grace, and the smallest remaining life
/// among them — zero when none are. The admission rate says the condition
/// exists; the minimum names the first hard refusal (OB-9, OB-13).
pub fn report_grace_census(in_grace: usize, min_remaining_secs: u64) {
    GRANTS_IN_GRACE.set(in_grace as i64);
    GRACE_MIN_REMAINING.set(min_remaining_secs as i64);
}

/// Count a capacity-based stream refusal.
pub fn report_stream_refused(reason: RefusalReason) {
    REFUSED_STREAMS.get_or_create(&refusal_labels(reason)).inc();
}

fn refusal_labels(reason: RefusalReason) -> Labels {
    vec![("reason".to_owned(), reason.as_str().to_owned())]
}

#[cfg(test)]
pub fn refused_streams(reason: RefusalReason) -> u64 {
    REFUSED_STREAMS.get_or_create(&refusal_labels(reason)).get()
}

/// Add occupancy accumulated during one elapsed interval.
pub fn observe_stream_occupancy(running: usize, limit: usize, elapsed: Duration) {
    let (stream_seconds, saturated_seconds) = occupancy_increments(running, limit, elapsed);
    STREAM_SECONDS.inc_by(stream_seconds);
    if saturated_seconds > 0. {
        SATURATED_SECONDS.inc_by(saturated_seconds);
    }
}

#[cfg(test)]
pub fn stream_seconds() -> f64 {
    STREAM_SECONDS.get()
}

#[cfg(test)]
pub fn saturated_seconds() -> f64 {
    SATURATED_SECONDS.get()
}

/// Return `(stream_seconds, saturated_seconds)` for an interval.
fn occupancy_increments(running: usize, limit: usize, elapsed: Duration) -> (f64, f64) {
    let seconds = elapsed.as_secs_f64();
    // A limit of zero would otherwise read as permanently saturated.
    let saturated = limit > 0 && running >= limit;
    (
        running as f64 * seconds,
        if saturated { seconds } else { 0. },
    )
}

/// Carries the wire's `code`/`type`, prefixed — a bare `type` label says nothing on a
/// metric. Errors only, so success series keep their label set. An unclassified error is
/// still counted rather than dropped.
pub fn http_labels(
    endpoint: String,
    status: StatusCode,
    data_source: String,
    error_code: Option<ErrorCode>,
) -> Labels {
    let mut labels = vec![
        ("endpoint".to_owned(), endpoint),
        ("status".to_owned(), status.as_str().to_owned()),
        ("data_source".to_owned(), data_source),
    ];

    // Only failures carry the taxonomy. A 2xx is not one whatever a handler tagged it
    // with — labelling a routine 204 `availability_error` would make the steady state of
    // every polling client read as a fault (INV-30).
    if !(status.is_client_error() || status.is_server_error()) {
        return labels;
    }

    // An unclassified 4xx is a client hitting a bad route or verb; only a 5xx
    // implicates us.
    let code = error_code.unwrap_or(ErrorCode::Unclassified);
    let error_type = match code {
        ErrorCode::Unclassified if status.is_client_error() => ErrorType::InvalidRequest,
        code => code.error_type(),
    };
    labels.push(("error_code".to_owned(), code.as_str().to_owned()));
    labels.push(("error_type".to_owned(), error_type.as_str().to_owned()));

    labels
}

pub fn report_http_response(
    endpoint: String,
    status: StatusCode,
    data_source: String,
    error_code: Option<ErrorCode>,
    seconds_to_first_byte: f64,
) {
    let labels = http_labels(endpoint, status, data_source, error_code);
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
        "streams_refused",
        "Streams turned away for want of capacity, by which capacity ran out",
        REFUSED_STREAMS.clone(),
    );
    registry.register(
        "stream_seconds",
        "Cumulative stream-seconds; rate() is the mean number of active streams",
        STREAM_SECONDS.clone(),
    );
    registry.register(
        "streams_saturated_seconds",
        "Cumulative seconds with every stream slot taken; rate() is the fraction of time at the cap",
        SATURATED_SECONDS.clone(),
    );
    registry.register(
        "streams_limit",
        "Configured max_parallel_streams, so alerts need not hardcode it",
        STREAMS_LIMIT.clone(),
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
        "hotblocks_requests",
        "Logical DC-4 requests by final response-head transport outcome: response, replay_response, replay_failed, canceled, replay_canceled, timeout, or transport_failed",
        HOTBLOCKS_REQUESTS.clone(),
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
    registry.register(
        "auth_decisions",
        "Authorization evaluations by public outcome; empty unless the portal is configured with an `auth:` block",
        AUTH_DECISIONS.clone(),
    );
    registry.register(
        "auth_exchanges",
        "Credential exchanges by coarse outcome; carries no key id and no denial reason",
        EXCHANGES.clone(),
    );
    registry.register(
        "auth_exchange_duration_seconds",
        "How long a credential exchange took",
        EXCHANGE_DURATION.clone(),
    );
    registry.register(
        "auth_exchange_success_age_seconds",
        "Seconds since the control plane last answered an exchange",
        EXCHANGE_SUCCESS_AGE.clone(),
    );
    registry.register(
        "auth_grant_cache_entries",
        "Grants currently held",
        GRANT_CACHE_ENTRIES.clone(),
    );
    registry.register(
        "auth_grant_cache_capacity",
        "Bound on grants held; occupancy is meaningless without it",
        GRANT_CACHE_CAPACITY.clone(),
    );
    registry.register(
        "auth_grant_cache_evictions",
        "Live grants evicted to make room; sustained eviction means the working set exceeds the cache",
        GRANT_CACHE_EVICTIONS.clone(),
    );
    registry.register(
        "auth_grant_lifetimes_capped",
        "Grants whose offered lifetime exceeded the portal's ceiling and was shortened",
        GRANT_LIFETIMES_CAPPED.clone(),
    );
    registry.register(
        "auth_grace_admissions",
        "Requests served on a grant whose renewal has not landed",
        GRACE_ADMISSIONS.clone(),
    );
    registry.register(
        "auth_grants_in_grace",
        "Grants currently serving past refresh_after while their renewal has not landed",
        GRANTS_IN_GRACE.clone(),
    );
    registry.register(
        "auth_grace_min_remaining_seconds",
        "Smallest time to expires_at among grants in grace — the first hard refusal; zero when none are in grace",
        GRACE_MIN_REMAINING.clone(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels_for(status: u16, code: Option<ErrorCode>) -> Labels {
        http_labels(
            "/stream".to_owned(),
            StatusCode::from_u16(status).unwrap(),
            "network".to_owned(),
            code,
        )
    }

    fn get<'a>(labels: &'a Labels, key: &str) -> Option<&'a str> {
        labels
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    #[test]
    fn success_keeps_the_original_label_set() {
        let labels = labels_for(200, None);
        assert_eq!(get(&labels, "error_code"), None);
        assert_eq!(get(&labels, "error_type"), None);
    }

    /// A 204 is the steady state of every polling client. Labelling it as an error type
    /// would make normal operation dominate any availability alert (INV-30). The gate is
    /// on the status, so a stray tag on a 2xx cannot reintroduce that.
    #[test]
    fn no_2xx_is_ever_labelled_as_an_error() {
        for status in [200, 204, 206] {
            let labels = labels_for(status, Some(ErrorCode::UpstreamUnavailable));
            assert_eq!(get(&labels, "error_code"), None, "{status}");
            assert_eq!(get(&labels, "error_type"), None, "{status}");
        }
    }

    #[test]
    fn classified_errors_carry_code_and_type() {
        let labels = labels_for(529, Some(ErrorCode::Overloaded));
        assert_eq!(get(&labels, "error_code"), Some("overloaded"));
        assert_eq!(get(&labels, "error_type"), Some("rate_limit_error"));
    }

    /// The point of the catch-all: an unclassified 5xx is our bug, not nothing.
    #[test]
    fn unclassified_5xx_is_counted_as_an_api_error() {
        let labels = labels_for(500, None);
        assert_eq!(get(&labels, "error_code"), Some("unclassified"));
        assert_eq!(get(&labels, "error_type"), Some("api_error"));
    }

    /// A client hitting a bad route must not page anyone.
    #[test]
    fn unclassified_4xx_is_the_clients_fault() {
        let labels = labels_for(404, None);
        assert_eq!(get(&labels, "error_code"), Some("unclassified"));
        assert_eq!(get(&labels, "error_type"), Some("invalid_request_error"));
    }

    #[test]
    fn occupancy_integrates_to_mean_concurrency() {
        let interval = Duration::from_millis(100);
        // One second of wall clock, three of twenty slots taken throughout.
        let total: f64 = (0..10)
            .map(|_| occupancy_increments(3, 20, interval).0)
            .sum();
        assert!((total - 3.0).abs() < 1e-9, "got {total}");
    }

    #[test]
    fn saturation_is_counted_only_at_the_cap() {
        let interval = Duration::from_millis(100);
        assert_eq!(occupancy_increments(19, 20, interval).1, 0.);
        assert_eq!(
            occupancy_increments(20, 20, interval).1,
            interval.as_secs_f64()
        );
        // A limit of zero is a misconfiguration, not 100% saturation forever.
        assert_eq!(occupancy_increments(0, 0, interval).1, 0.);
    }

    /// The `reason` label is as public as a wire code: alerts match on it.
    #[test]
    fn refusal_reasons_are_distinct_and_frozen() {
        use RefusalReason::*;

        let reasons = [
            (TaskLimit, "task_limit"),
            (Bandwidth, "bandwidth"),
            (WorkersPaused, "workers_paused"),
            (WorkersRateLimited, "workers_rate_limited"),
        ];
        for (reason, wire) in reasons {
            assert_eq!(reason.as_str(), wire);
        }
        let distinct: std::collections::HashSet<_> =
            reasons.iter().map(|(_, wire)| *wire).collect();
        assert_eq!(distinct.len(), reasons.len());
    }

    /// A rolling deploy fails /ready on every pod; that must not read as an api_error.
    #[test]
    fn draining_readiness_is_an_availability_error() {
        let labels = labels_for(503, Some(ErrorCode::NotReady));
        assert_eq!(get(&labels, "error_code"), Some("not_ready"));
        assert_eq!(get(&labels, "error_type"), Some("availability_error"));
    }
}
