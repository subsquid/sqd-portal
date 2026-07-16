use axum::{
    body::Body,
    extract::Request,
    response::{IntoResponse, Response},
    routing::MethodRouter,
};
use std::task::{Context, Poll};
use tokio::time::{Duration, Instant};
use tower::{Layer, Service};
use tower_http::request_id::RequestId;
use tracing::Instrument;

use crate::{
    metrics,
    types::{ErrorCode, StreamRequest},
};

const LOG_INTERVAL: Duration = Duration::from_secs(5);
const NO_DATA_SOURCE: &str = "none";
/// Error envelopes are small; this only bounds a malformed upstream body.
const MAX_ERROR_BODY: usize = 64 * 1024;

pub struct StreamStats {
    pub queries_sent: u64,
    pub chunks_downloaded: u64,
    pub response_blocks: u64,
    pub response_bytes: u64,
    pub max_chunk_parts: usize,
    pub start_time: Instant,
    pub last_log: Instant,
    pub throttled_for: Duration,
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
            max_chunk_parts: 0,
            start_time: now,
            last_log: now,
            throttled_for: Duration::from_secs(0),
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

    pub fn throttled(&mut self, duration: Duration) {
        self.throttled_for += duration;
    }

    pub fn observe_chunk_parts(&mut self, parts: usize) {
        self.max_chunk_parts = self.max_chunk_parts.max(parts);
    }

    pub fn maybe_write_log(&mut self) {
        if self.last_log.elapsed() >= LOG_INTERVAL {
            tracing::info!(
                queries_sent = self.queries_sent,
                chunks_downloaded = self.chunks_downloaded,
                max_chunk_parts = self.max_chunk_parts,
                blocks_streamed = self.response_blocks,
                bytes_streamed = self.response_bytes,
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
        tracing::info!(
            dataset = %request.dataset_id,
            first_block = request.query.first_block(),
            last_block = request.query.last_block(),
            queries_sent = self.queries_sent,
            chunks_downloaded = self.chunks_downloaded,
            max_chunk_parts = self.max_chunk_parts,
            blocks_streamed = self.response_blocks,
            bytes_streamed = self.response_bytes,
            total_time = ?self.start_time.elapsed(),
            throttled_for = ?self.throttled_for,
            error = error.unwrap_or_else(|| "-".to_string()),
            "Stream finished"
        );
        metrics::report_stream_completed(self, &request.dataset_id, Some(&request.dataset_name));
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
        // The request id may be a client-supplied `x-request-id` header, which is
        // preserved verbatim and can contain non-visible-ASCII bytes. Fall back to
        // an empty id rather than panicking the request task on such input.
        .unwrap_or_default()
        .to_owned();

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
    let error_code = response.extensions().get::<ErrorCode>().copied();

    span.in_scope(|| {
        tracing::info!(
            target: "http_request",
            method,
            path,
            ?version,
            status = %response.status(),
            error_code = error_code.map(ErrorCode::as_str),
            ?latency,
            "HTTP request processed"
        );
    });

    metrics::report_http_response(
        endpoint,
        response.status(),
        data_source,
        error_code,
        latency.as_secs_f64(),
    );

    inject_request_id(response, error_code, &request_id).await
}

/// Stamp `error.request_id` into the body: the id is only known here. Touches only
/// responses tagged with an [`ErrorCode`], so a data stream is never buffered.
async fn inject_request_id(
    response: Response,
    error_code: Option<ErrorCode>,
    request_id: &str,
) -> Response {
    if error_code.is_none() || request_id.is_empty() {
        return response;
    }
    if response.status() == axum::http::StatusCode::NO_CONTENT {
        return response;
    }

    let (mut parts, body) = response.into_parts();
    let Ok(bytes) = axum::body::to_bytes(body, MAX_ERROR_BODY).await else {
        return Response::from_parts(parts, Body::empty());
    };
    let Ok(mut json) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return Response::from_parts(parts, Body::from(bytes));
    };

    match json.get_mut("error").and_then(|e| e.as_object_mut()) {
        Some(error) => {
            error.insert("request_id".to_owned(), request_id.into());
        }
        None => return Response::from_parts(parts, Body::from(bytes)),
    }

    let body = serde_json::to_vec(&json).unwrap_or_else(|_| bytes.to_vec());
    parts.headers.remove(axum::http::header::CONTENT_LENGTH);
    Response::from_parts(parts, Body::from(body))
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
    use super::data_source_metric_label;
    use crate::endpoints::stream::{DATA_SOURCE_NETWORK_METRIC, DATA_SOURCE_REALTIME_METRIC};

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

    /// Unique endpoint labels keep the shared global metric family isolated from
    /// tests running concurrently.
    #[tokio::test]
    async fn handler_error_code_reaches_the_metric() {
        use crate::metrics::{http_labels, HTTP_STATUS};
        use crate::types::{ErrorCode, RequestError};
        use axum::{
            body::Body, http::Request, middleware::from_fn, response::IntoResponse, routing::get,
            Router,
        };
        use tower::ServiceExt;
        use tower_http::request_id::{MakeRequestUuid, SetRequestIdLayer};

        use super::NO_DATA_SOURCE;

        let app = Router::new()
            .route(
                "/boom",
                get(|| async { RequestError::RateLimitExceeded.into_response() }),
            )
            .route("/fine", get(|| async { "ok" }))
            .route_layer(from_fn(super::middleware))
            .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid));

        let count = |path: &str, status: u16, code: Option<ErrorCode>| {
            HTTP_STATUS
                .get_or_create(&http_labels(
                    path.to_owned(),
                    axum::http::StatusCode::from_u16(status).unwrap(),
                    NO_DATA_SOURCE.to_owned(),
                    code,
                ))
                .get()
        };

        let before = count("/boom", 529, Some(ErrorCode::Overloaded));
        let resp = app
            .clone()
            .oneshot(Request::builder().uri("/boom").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), 529);
        assert_eq!(
            count("/boom", 529, Some(ErrorCode::Overloaded)),
            before + 1,
            "RateLimitExceeded must be counted as overloaded"
        );

        let before = count("/fine", 200, None);
        app.oneshot(Request::builder().uri("/fine").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            count("/fine", 200, None),
            before + 1,
            "success must stay on the unlabelled series"
        );
    }

    /// The id exists only at the middleware, so it must be stamped in on the way out.
    #[tokio::test]
    async fn error_bodies_echo_the_request_id() {
        use crate::types::RequestError;
        use axum::{
            body::Body, http::Request, middleware::from_fn, response::IntoResponse, routing::get,
            Router,
        };
        use tower::ServiceExt;
        use tower_http::request_id::{MakeRequestUuid, SetRequestIdLayer};

        let app = Router::new()
            .route(
                "/rid-boom",
                get(|| async { RequestError::Unavailable.into_response() }),
            )
            .route("/rid-fine", get(|| async { "streamed body" }))
            .route_layer(from_fn(super::middleware))
            .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid));

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rid-boom")
                    .header("x-request-id", "req-abc123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["request_id"], "req-abc123");
        assert_eq!(body["error"]["code"], "no_workers");

        // A success body must pass through untouched, not be buffered and rewritten.
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/rid-fine")
                    .header("x-request-id", "req-abc123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&bytes[..], b"streamed body");
    }

    // Regression test: a client-supplied `x-request-id` header is preserved verbatim
    // by SetRequestIdLayer and may contain non-visible-ASCII bytes (obs-text, 0x80-0xFF),
    // which are valid in an HTTP header value but make `HeaderValue::to_str()` fail.
    // The logging middleware must not panic on such input (previously a per-connection DoS).
    #[tokio::test]
    async fn middleware_does_not_panic_on_non_ascii_request_id() {
        use axum::{
            body::Body,
            http::{header::HeaderValue, Request, StatusCode},
            middleware::from_fn,
            routing::get,
            Router,
        };
        use tower::ServiceExt;
        use tower_http::request_id::{MakeRequestUuid, SetRequestIdLayer};

        // Mirror the real layering: the logging middleware runs inside SetRequestIdLayer,
        // so the RequestId extension is populated (here, from the client header) before it.
        let app = Router::new()
            .route("/status", get(|| async { "ok" }))
            .route_layer(from_fn(super::middleware))
            .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid));

        let req = Request::builder()
            .uri("/status")
            .header("x-request-id", HeaderValue::from_bytes(&[0x80]).unwrap())
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.expect("router should respond");
        assert_eq!(resp.status(), StatusCode::OK);
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
