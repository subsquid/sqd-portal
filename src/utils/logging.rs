use axum::{
    extract::Request,
    response::{IntoResponse, Response},
    routing::MethodRouter,
};
use std::collections::BTreeMap;
use std::task::{Context, Poll};
use tokio::time::{Duration, Instant};
use tower::{Layer, Service};
use tower_http::request_id::RequestId;
use tracing::Instrument;

use crate::{
    metrics,
    types::{QueryError, StreamRequest, WorkerFailureKind},
};

const LOG_INTERVAL: Duration = Duration::from_secs(5);
const NO_DATA_SOURCE: &str = "none";

pub struct StreamStats {
    pub queries_sent: u64,
    pub chunks_downloaded: u64,
    pub response_blocks: u64,
    pub response_bytes: u64,
    pub max_chunk_parts: u64,
    pub start_time: Instant,
    pub last_log: Instant,
    pub throttled_for: Duration,
    worker_failures: WorkerFailureStats,
}

#[derive(Debug, Default)]
struct WorkerFailureStats {
    total: u64,
    by_kind: BTreeMap<&'static str, u64>,
    by_worker: BTreeMap<String, u64>,
}

impl Default for StreamStats {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamStats {
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            queries_sent: 0,
            chunks_downloaded: 0,
            response_blocks: 0,
            response_bytes: 0,
            max_chunk_parts: 1,
            start_time: now,
            last_log: now,
            throttled_for: Duration::from_secs(0),
            worker_failures: WorkerFailureStats::default(),
        }
    }

    pub fn query_sent(&mut self) {
        self.queries_sent += 1;
    }

    pub fn sent_response_chunk(&mut self, blocks: u64, bytes: usize) {
        self.chunks_downloaded += 1;
        self.response_blocks += blocks;
        self.response_bytes += bytes as u64;
    }

    pub fn observe_chunk_parts(&mut self, parts: usize) {
        self.max_chunk_parts = self.max_chunk_parts.max(parts as u64);
    }

    pub fn throttled(&mut self, duration: Duration) {
        self.throttled_for += duration;
    }

    pub fn worker_query_failed(&mut self, worker: impl ToString, error: &QueryError) {
        if let QueryError::WorkerFailure { kind, .. } = error {
            self.worker_failures.record(worker.to_string(), *kind);
        }
    }

    pub fn maybe_write_log(&mut self) {
        if self.last_log.elapsed() >= LOG_INTERVAL {
            let (top_failure_worker, top_failure_worker_failures) =
                self.worker_failures.top_worker();
            tracing::info!(
                queries_sent = self.queries_sent,
                chunks_downloaded = self.chunks_downloaded,
                max_chunk_parts = self.max_chunk_parts,
                blocks_streamed = self.response_blocks,
                bytes_streamed = self.response_bytes,
                worker_failures = self.worker_failures.total,
                worker_failure_counts = %self.worker_failures.counts_summary(),
                top_failure_worker = %top_failure_worker,
                top_failure_worker_failures,
                "Streaming..."
            );
            self.last_log = Instant::now();
        }
    }

    pub fn write_summary(&self, request: &StreamRequest, error: Option<String>) {
        // tracing::debug!(
        //     dataset = %request.dataset_id,
        //     query = request.query.to_string(),
        //     "Query processed"
        // );
        let (top_failure_worker, top_failure_worker_failures) = self.worker_failures.top_worker();
        tracing::info!(
            dataset = %request.dataset_id,
            // The summary is written on Drop, outside the `http_request` span —
            // without an explicit field it cannot be joined with the access log.
            request_id = %request.request_id,
            first_block = request.query.first_block(),
            last_block = request.query.last_block(),
            queries_sent = self.queries_sent,
            chunks_downloaded = self.chunks_downloaded,
            max_chunk_parts = self.max_chunk_parts,
            blocks_streamed = self.response_blocks,
            bytes_streamed = self.response_bytes,
            total_time = ?self.start_time.elapsed(),
            throttled_for = ?self.throttled_for,
            worker_failures = self.worker_failures.total,
            worker_failure_counts = %self.worker_failures.counts_summary(),
            top_failure_worker = %top_failure_worker,
            top_failure_worker_failures,
            error = error.unwrap_or_else(|| "-".to_string()),
            "Stream finished"
        );
        metrics::report_stream_completed(self, &request.dataset_id, Some(&request.dataset_name));
    }
}

impl WorkerFailureStats {
    fn record(&mut self, worker: String, kind: WorkerFailureKind) {
        self.total += 1;
        *self.by_kind.entry(kind.as_str()).or_default() += 1;
        *self.by_worker.entry(worker).or_default() += 1;
    }

    fn counts_summary(&self) -> String {
        if self.total == 0 {
            return "-".to_string();
        }
        let mut counts = self.by_kind.iter().collect::<Vec<_>>();
        counts.sort_by(|(left_kind, left_count), (right_kind, right_count)| {
            right_count
                .cmp(left_count)
                .then_with(|| left_kind.cmp(right_kind))
        });
        counts
            .iter()
            .map(|(kind, count)| format!("{kind}={count}"))
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn top_worker(&self) -> (String, u64) {
        self.by_worker
            .iter()
            .max_by(|(left_worker, left_count), (right_worker, right_count)| {
                left_count
                    .cmp(right_count)
                    .then_with(|| right_worker.cmp(left_worker))
            })
            .map(|(worker, count)| (worker.clone(), *count))
            .unwrap_or_else(|| ("-".to_string(), 0))
    }
}

pub async fn middleware(req: Request, next: axum::middleware::Next) -> impl IntoResponse {
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let version = req.version();
    let start = Instant::now();
    let request_id = req
        .extensions()
        .get::<RequestId>()
        .expect("RequestId should be set by SetRequestIdLayer")
        .header_value()
        .to_str()
        .expect("Request ID should be a valid string");

    let span = tracing::span!(tracing::Level::INFO, "http_request", request_id);

    let response = next.run(req).instrument(span.clone()).await;

    let latency = start.elapsed();

    let endpoint = response
        .extensions()
        .get::<EndpointName>()
        .map(|e| e.0.clone())
        .unwrap_or_else(|| path.clone());
    let data_source = response
        .headers()
        .get(crate::endpoints::stream::DATA_SOURCE_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(data_source_metric_label)
        .unwrap_or(NO_DATA_SOURCE)
        .to_owned();

    span.in_scope(|| {
        tracing::info!(
            target: "http_request",
            method,
            path,
            ?version,
            status = %response.status(),
            ?latency,
            "HTTP request processed"
        );
    });

    metrics::report_http_response(
        endpoint,
        response.status(),
        data_source,
        latency.as_secs_f64(),
    );

    response
}

fn data_source_metric_label(data_source: &str) -> &str {
    match data_source {
        crate::endpoints::stream::DATA_SOURCE_REALTIME_METRIC => "hotblocks",
        crate::endpoints::stream::DATA_SOURCE_NETWORK_METRIC => "network",
        other => other,
    }
}

pub trait MethodRouterExt {
    fn endpoint(self, endpoint: impl Into<String>) -> Self;
}

impl<S> MethodRouterExt for MethodRouter<S>
where
    S: Clone + Send + Sync + 'static,
{
    fn endpoint(self, endpoint: impl Into<String>) -> Self {
        self.layer(EndpointAnnotationLayer::new(endpoint))
    }
}

#[cfg(test)]
mod tests {
    use super::{data_source_metric_label, StreamStats};
    use crate::endpoints::stream::{DATA_SOURCE_NETWORK_METRIC, DATA_SOURCE_REALTIME_METRIC};
    use crate::types::{QueryError, WorkerFailureKind};

    #[test]
    fn worker_failure_stats_aggregate_by_kind_and_top_worker() {
        let mut stats = StreamStats::new();

        stats.worker_query_failed(
            "worker-a",
            &QueryError::WorkerFailure {
                kind: WorkerFailureKind::Timeout,
                message: "timed out reading response".to_string(),
            },
        );
        stats.worker_query_failed(
            "worker-b",
            &QueryError::WorkerFailure {
                kind: WorkerFailureKind::TransportError,
                message: "transport error: closed".to_string(),
            },
        );
        stats.worker_query_failed(
            "worker-a",
            &QueryError::WorkerFailure {
                kind: WorkerFailureKind::Timeout,
                message: "timed out connecting to the peer".to_string(),
            },
        );
        stats.worker_query_failed(
            "worker-a",
            &QueryError::Retriable("not counted".to_string()),
        );
        stats.worker_query_failed(
            "worker-c",
            &QueryError::WorkerFailure {
                kind: WorkerFailureKind::InvalidResponse,
                message: "couldn't decode response".to_string(),
            },
        );

        assert_eq!(stats.worker_failures.total, 4);
        assert_eq!(
            stats.worker_failures.counts_summary(),
            "timeout=2 invalid_response=1 transport_error=1"
        );
        assert_eq!(
            stats.worker_failures.top_worker(),
            ("worker-a".to_string(), 2)
        );
    }

    #[test]
    fn data_source_metric_label_keeps_network() {
        assert_eq!(
            data_source_metric_label(DATA_SOURCE_NETWORK_METRIC),
            "network"
        );
    }

    #[test]
    fn data_source_metric_label_maps_real_time_to_hotblocks() {
        assert_eq!(
            data_source_metric_label(DATA_SOURCE_REALTIME_METRIC),
            "hotblocks"
        );
    }

    #[test]
    fn data_source_metric_label_preserves_unrecognized_values() {
        assert_eq!(data_source_metric_label("custom"), "custom");
    }
}

#[derive(Clone)]
pub struct EndpointName(pub String);

#[derive(Clone)]
pub struct EndpointAnnotationLayer {
    endpoint: String,
}

impl EndpointAnnotationLayer {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }
}

impl<S> Layer<S> for EndpointAnnotationLayer {
    type Service = EndpointAnnotationService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        EndpointAnnotationService {
            inner,
            endpoint: self.endpoint.clone(),
        }
    }
}

#[derive(Clone)]
pub struct EndpointAnnotationService<S> {
    inner: S,
    endpoint: String,
}

impl<S> Service<Request> for EndpointAnnotationService<S>
where
    S: Service<Request, Response = Response> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let endpoint = self.endpoint.clone();
        let fut = self.inner.call(req);

        Box::pin(async move {
            let mut response = fut.await?;
            // Store the endpoint name in the response extensions for the middleware to use
            response.extensions_mut().insert(EndpointName(endpoint));
            Ok(response)
        })
    }
}
