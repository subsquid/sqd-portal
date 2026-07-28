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
    types::{coded_response, error_response, ErrorBody, ErrorCode, StreamRequest},
};

const LOG_INTERVAL: Duration = Duration::from_secs(5);
const NO_DATA_SOURCE: &str = "none";

/// Endpoint label for a request that never reached a route. Constant because the path is
/// client-supplied there, and labelling with it would mint a series per request.
const UNROUTED_ENDPOINT: &str = "unrouted";

/// Marks a response [`middleware`] already logged and counted.
#[derive(Clone, Copy)]
struct Observed;

/// `SetRequestIdLayer`'s header, which it does not export.
pub(crate) const X_REQUEST_ID: axum::http::HeaderName =
    axum::http::HeaderName::from_static("x-request-id");

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

/// Reject a non-ASCII client `x-request-id` with a generated correlation id.
///
/// HTTP permits obs-text in a header value, but REQ-9 restricts this application header to
/// ASCII. This runs outside `SetRequestIdLayer` and therefore records the early response
/// itself; no downstream request-id, CORS, observability, or routing layer has run yet.
pub async fn reject_non_ascii_request_id(req: Request, next: axum::middleware::Next) -> Response {
    let is_non_ascii = req
        .headers()
        .get(&X_REQUEST_ID)
        .is_some_and(|value| value.to_str().is_err());
    if !is_non_ascii {
        return next.run(req).await;
    }

    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let start = Instant::now();
    let request_id = uuid::Uuid::new_v4().to_string();
    let request_id_header: axum::http::HeaderValue = request_id
        .parse()
        .expect("a UUID is always a valid header value");
    let mut response = coded_response(
        ErrorCode::MalformedRequest,
        "x-request-id must contain only ASCII characters",
    );
    response
        .headers_mut()
        .insert(X_REQUEST_ID, request_id_header.clone());
    response
        .extensions_mut()
        .insert(RequestId::new(request_id_header));

    let latency = start.elapsed();
    tracing::info!(
        target: "http_request",
        request_id,
        method,
        path,
        status = %response.status(),
        error_code = ErrorCode::MalformedRequest.as_str(),
        ?latency,
        "HTTP request rejected before routing"
    );
    metrics::report_http_response(
        UNROUTED_ENDPOINT.to_owned(),
        response.status(),
        NO_DATA_SOURCE.to_owned(),
        Some(ErrorCode::MalformedRequest),
        latency.as_secs_f64(),
    );
    response.extensions_mut().insert(Observed);
    response
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
        // [`reject_non_ascii_request_id`] rejects a non-ASCII id before routing; this only
        // guards a stack missing that layer, where an empty id still beats a task panic.
        .unwrap_or_default()
        .to_owned();

    let span = tracing::span!(tracing::Level::INFO, "http_request", request_id);

    let response =
        normalize_framework_rejection(next.run(req).instrument(span.clone()).await).await;

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

    let mut response = stamp_request_id(response, &request_id);
    response.extensions_mut().insert(Observed);
    response
}

/// Normalize, log and count a response [`middleware`] never saw.
///
/// That middleware is a `route_layer`, so it misses both a path the router never matched and
/// a request refused by a layer outside it — today a `RequestDecompressionLayer` 415, which
/// only `gzip` satisfies here, so `deflate`, `br` and `zstd` all answered a bodyless 415 with
/// no envelope, no code and no metric (INV-26, GAP-16). Must sit outside that layer.
pub async fn observe_bypassed(req: Request, next: axum::middleware::Next) -> Response {
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    // Read softly: this path exists for degenerate requests, so a stack without
    // SetRequestIdLayer must not panic here.
    let request_id = req
        .extensions()
        .get::<RequestId>()
        .and_then(|id| id.header_value().to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let start = Instant::now();

    let response = next.run(req).await;
    if response.extensions().get::<Observed>().is_some() {
        return response;
    }

    let response = normalize_framework_rejection(response).await;
    let latency = start.elapsed();
    let error_code = response.extensions().get::<ErrorCode>().copied();

    tracing::info!(
        target: "http_request",
        // On the field, not on a span: nesting a second `http_request` span around a
        // routed request would record it twice. [`middleware`] gets it from its span,
        // and without it here REQ-9 fails on exactly the requests this layer exists for —
        // an unmatched route and a refused content-encoding.
        request_id,
        method,
        path,
        status = %response.status(),
        error_code = error_code.map(ErrorCode::as_str),
        ?latency,
        "HTTP request processed outside the router"
    );
    metrics::report_http_response(
        UNROUTED_ENDPOINT.to_owned(),
        response.status(),
        NO_DATA_SOURCE.to_owned(),
        error_code,
        latency.as_secs_f64(),
    );

    stamp_request_id(response, &request_id)
}

/// Rebuild an error the router raised before any handler ran — a path segment that will
/// not parse, an unreadable query string, an over-limit body — into the envelope.
///
/// Those rejections come from axum, not from our code, so they answer with plain text and
/// no [`ErrorCode`]: on the most common client mistake of all, a bad path parameter, the
/// endpoint emits exactly the shape this taxonomy replaced. Converting each extractor
/// call site would leave the next one to be found by a client, so the normalization lives
/// here, where every routed response passes.
///
/// Only a body of known, small size is touched. A rejection is a short in-memory buffer,
/// so an exact size hint separates it from anything streamed without polling the body —
/// no stream is buffered, or consumed, to find out.
async fn normalize_framework_rejection(response: Response) -> Response {
    use http_body::Body as _;

    const MAX_REJECTION_BODY: u64 = 8 * 1024;

    let status = response.status();
    if !(status.is_client_error() || status.is_server_error())
        || response.extensions().get::<ErrorCode>().is_some()
    {
        return response;
    }
    if response
        .body()
        .size_hint()
        .exact()
        .is_none_or(|size| size > MAX_REJECTION_BODY)
    {
        return response;
    }

    // IB-5 is a closed code→status mapping, so the status moves with the code rather than
    // keeping whatever the extractor picked: a 413 or a 415 answering `malformed_request`
    // would contradict the binding it claims to follow. The distinction is not lost —
    // axum's own prose carries it in `message`.
    //
    // 405 gets its own code instead of collapsing: the request is well-formed and the
    // fault is the verb, which `Allow` names and a 400 could not. It is also the one
    // rejection axum answers with an empty body, so the generic message would be all the
    // client got.
    //
    // A 5xx keeps `unclassified` and its status, which is an api_error and pages: the
    // router failing on its own is not something to name as the client's fault.
    use axum::http::StatusCode;
    let (code, public_status) = match status {
        StatusCode::NOT_FOUND => (ErrorCode::NotFound, status),
        StatusCode::METHOD_NOT_ALLOWED => (ErrorCode::MethodNotAllowed, status),
        s if s.is_client_error() => (
            ErrorCode::MalformedRequest,
            ErrorCode::MalformedRequest.status(),
        ),
        // Unclassified is the one code whose status is contextual: whatever the router
        // failed with is more informative than the floor.
        _ => (ErrorCode::Unclassified, status),
    };

    let (parts, body) = response.into_parts();
    // axum's rejection prose is worth keeping — "Cannot parse `abc` to a `u64`" names the
    // fault better than a generic message can.
    let message = match axum::body::to_bytes(body, MAX_REJECTION_BODY as usize).await {
        Ok(bytes) if !bytes.is_empty() => String::from_utf8_lossy(&bytes).into_owned(),
        _ => code.default_message().to_owned(),
    };

    let mut rebuilt = error_response(public_status, code, message);
    for (key, value) in parts.headers.iter() {
        if key == axum::http::header::CONTENT_TYPE || key == axum::http::header::CONTENT_LENGTH {
            continue;
        }
        rebuilt.headers_mut().insert(key, value.clone());
    }
    // Carry the extensions across, not just the headers. `EndpointName` lives here, and
    // without it the metric falls back to the raw request path — which on a rejection is
    // client-supplied, so a malformed dynamic path would mint an `endpoint` label per
    // request. The rejection had no `ErrorCode`, so nothing here overwrites the new one.
    rebuilt.extensions_mut().extend(parts.extensions);
    rebuilt
}

/// Stamp `error.request_id` into the body. Re-renders the [`ErrorBody`] the handler left
/// in extensions rather than parsing the bytes back out, so there is no size cap, no
/// chance of mangling a body that isn't the envelope, and nothing to buffer — a data
/// stream carries no `ErrorBody` and is returned untouched.
///
/// 5xx only. The id is on `x-request-id` either way (REQ-9); the body copy exists for the
/// support flow, where a user pastes JSON and loses headers. A 4xx is the client's own
/// fault and is handled programmatically — a 409 in particular is a routine reorg the
/// client resolves from `previousBlocks` — so the copy buys nothing and costs a
/// re-render of the whole body, siblings included.
fn stamp_request_id(response: Response, request_id: &str) -> Response {
    if request_id.is_empty() || !response.status().is_server_error() {
        return response;
    }
    let Some(mut body) = response.extensions().get::<ErrorBody>().cloned() else {
        return response;
    };
    body.set_request_id(request_id);

    let rendered = serde_json::to_vec(&body.to_json()).expect("ErrorBody is serializable");
    let (mut parts, _) = response.into_parts();
    parts.headers.remove(axum::http::header::CONTENT_LENGTH);
    Response::from_parts(parts, Body::from(rendered))
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

    /// A rejection the router raises before any handler runs — the commonest of them a
    /// path segment that will not parse — used to answer with axum's plain text and no
    /// code, which is the shape this taxonomy replaced. It must arrive as the envelope
    /// like everything else, and be classified so the metric can see it.
    #[tokio::test]
    async fn a_router_rejection_arrives_as_the_envelope() {
        use axum::{body::Body, extract::Path, http::Request, middleware::from_fn, routing::get};
        use tower::ServiceExt;
        use tower_http::request_id::{MakeRequestUuid, SetRequestIdLayer};

        let app = axum::Router::new()
            .route(
                "/block/:number",
                get(|Path(n): Path<u64>| async move { n.to_string() }),
            )
            .route_layer(from_fn(super::middleware))
            .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid));

        let call = |uri: &'static str| {
            let app = app.clone();
            async move {
                let resp = app
                    .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                    .await
                    .unwrap();
                let status = resp.status();
                let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                    .await
                    .unwrap();
                (status, bytes)
            }
        };

        let (status, bytes) = call("/block/not-a-number").await;
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
        let body: serde_json::Value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|e| panic!("a rejection must be the envelope, got {bytes:?}: {e}"));
        assert_eq!(body["error"]["code"], "malformed_request");
        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert!(
            body["error"]["message"]
                .as_str()
                .is_some_and(|m| m.contains("not-a-number")),
            "axum's prose names the fault better than a generic message: {body}"
        );

        // A success is not an error shape and must pass through byte for byte.
        let (status, bytes) = call("/block/42").await;
        assert_eq!(status, axum::http::StatusCode::OK);
        assert_eq!(&bytes[..], b"42");
    }

    /// IB-5 is a closed code→status mapping, so a rejection axum answered with 413 or 415
    /// cannot keep that status while calling itself `malformed_request`. The detail is not
    /// lost — it stays in the message.
    #[tokio::test]
    async fn a_rejection_status_follows_its_code() {
        use axum::{body::Body, http::Request, middleware::from_fn, routing::post, Router};
        use tower::ServiceExt;
        use tower_http::request_id::{MakeRequestUuid, SetRequestIdLayer};

        let app = Router::new()
            .route(
                "/limited",
                post(|_: axum::extract::Json<serde_json::Value>| async { "ok" }),
            )
            .route_layer(from_fn(super::middleware))
            .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid));

        // No content-type: axum rejects with 415 Unsupported Media Type.
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/limited")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            axum::http::StatusCode::BAD_REQUEST,
            "malformed_request is bound to 400 by IB-5"
        );
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["code"], "malformed_request");
        assert!(
            body["error"]["message"]
                .as_str()
                .is_some_and(|m| !m.is_empty()),
            "the status is normalized, so the specifics must survive in the message: {body}"
        );
    }

    /// 405 is the exception to that normalization. A wrong verb on a real route is not a
    /// malformed request, `Allow` is the answer and a 400 has nowhere to put it, and it is
    /// the one rejection axum leaves bodyless — collapsed to 400 the client was told only
    /// "Bad request".
    #[tokio::test]
    async fn a_wrong_verb_keeps_its_status_and_says_so() {
        use axum::{body::Body, http::Request, middleware::from_fn, routing::get, Router};
        use tower::ServiceExt;
        use tower_http::request_id::{MakeRequestUuid, SetRequestIdLayer};

        let app = Router::new()
            .route("/only-get", get(|| async { "ok" }))
            .route_layer(from_fn(super::middleware))
            .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid));

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/only-get")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), axum::http::StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(
            resp.headers()
                .get(axum::http::header::ALLOW)
                .map(|v| v.to_str().unwrap().to_owned()),
            Some("GET,HEAD".to_owned()),
            "the recovery hint must survive the rebuild"
        );
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["code"], "method_not_allowed");
        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert!(
            body["error"]["message"]
                .as_str()
                .is_some_and(|m| m.to_lowercase().contains("method")),
            "axum leaves 405 bodyless, so the default message has to name the fault: {body}"
        );
    }

    /// The endpoint label is annotated onto the response, so rebuilding a rejection must
    /// carry the extensions across. Without it the metric falls back to the raw request
    /// path — which on a rejection is client-supplied, so every malformed dynamic path
    /// would mint its own Prometheus series.
    #[tokio::test]
    async fn a_rebuilt_rejection_keeps_its_endpoint_label() {
        use crate::metrics::{http_labels, HTTP_STATUS};
        use crate::types::ErrorCode;
        use axum::{body::Body, extract::Path, http::Request, middleware::from_fn, routing::get};
        use tower::ServiceExt;
        use tower_http::request_id::{MakeRequestUuid, SetRequestIdLayer};

        use super::{MethodRouterExt, NO_DATA_SOURCE};

        let app = axum::Router::new()
            .route(
                "/labelled/:number",
                get(|Path(n): Path<u64>| async move { n.to_string() }).endpoint("/labelled"),
            )
            .route_layer(from_fn(super::middleware))
            .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid));

        let count = |endpoint: &str| {
            HTTP_STATUS
                .get_or_create(&http_labels(
                    endpoint.to_owned(),
                    axum::http::StatusCode::BAD_REQUEST,
                    NO_DATA_SOURCE.to_owned(),
                    Some(ErrorCode::MalformedRequest),
                ))
                .get()
        };

        let before = count("/labelled");
        app.oneshot(
            Request::builder()
                .uri("/labelled/attacker-controlled-value")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(
            count("/labelled"),
            before + 1,
            "the rejection must be counted under the route, not the requested path"
        );
        assert_eq!(
            count("/labelled/attacker-controlled-value"),
            0,
            "a client-supplied path must never become an endpoint label"
        );
    }

    /// The id exists only at the middleware, so a 5xx must have it stamped in on the way
    /// out. A 4xx must not: it is the client's own fault, handled programmatically rather
    /// than pasted into a ticket, and `x-request-id` carries the id either way (REQ-9).
    #[tokio::test]
    async fn only_server_errors_echo_the_request_id_in_the_body() {
        use crate::types::RequestError;
        use axum::{
            body::Body, http::Request, middleware::from_fn, response::IntoResponse, routing::get,
            Router,
        };
        use tower::ServiceExt;
        use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};

        let app = Router::new()
            .route(
                "/rid-boom",
                get(|| async { RequestError::Unavailable.into_response() }),
            )
            .route(
                "/rid-client-fault",
                get(|| async { RequestError::BadRequest("nope".into()).into_response() }),
            )
            .route("/rid-fine", get(|| async { "streamed body" }))
            .route_layer(from_fn(super::middleware))
            // Mirrors the production stack: Propagate sits inside Set so it sees the id.
            .layer(PropagateRequestIdLayer::x_request_id())
            .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid));

        let get_body = |path: &'static str| {
            let app = app.clone();
            async move {
                let resp = app
                    .oneshot(
                        Request::builder()
                            .uri(path)
                            .header("x-request-id", "req-abc123")
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                let echoed = resp.headers().get("x-request-id").cloned();
                let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                    .await
                    .unwrap();
                (echoed, bytes)
            }
        };

        let (echoed, bytes) = get_body("/rid-boom").await;
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["request_id"], "req-abc123");
        assert_eq!(body["error"]["code"], "no_workers");
        assert_eq!(echoed.unwrap(), "req-abc123");

        let (echoed, bytes) = get_body("/rid-client-fault").await;
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(
            !body["error"]
                .as_object()
                .unwrap()
                .contains_key("request_id"),
            "a 4xx must not carry the id in its body: {body}"
        );
        assert_eq!(body["error"]["code"], "malformed_request");
        assert_eq!(
            echoed.unwrap(),
            "req-abc123",
            "the header is how a 4xx conveys the id"
        );

        // A success body must pass through untouched, not be buffered and rewritten.
        let (_, bytes) = get_body("/rid-fine").await;
        assert_eq!(&bytes[..], b"streamed body");
    }

    /// Only `gzip` is compiled in, so `deflate`, `br` and `zstd` are refused by the
    /// decompression layer — outside the routed middleware, which used to leave them a
    /// bodyless 415 with no envelope and no code. `accept-encoding` must survive the
    /// rebuild: it is the only thing telling the client which encoding to use instead.
    #[tokio::test]
    async fn a_refused_content_encoding_answers_the_envelope() {
        use axum::{
            body::Body,
            http::{Request, StatusCode},
            middleware::from_fn,
            routing::post,
            Router,
        };
        use tower::ServiceExt;
        use tower_http::decompression::RequestDecompressionLayer;
        use tower_http::request_id::{MakeRequestUuid, SetRequestIdLayer};

        // Production order: the decompression layer inside `observe_bypassed`.
        let app = Router::new()
            .route("/echo", post(|| async { "ok" }))
            .route_layer(from_fn(super::middleware))
            .layer(RequestDecompressionLayer::new())
            .layer(from_fn(super::observe_bypassed))
            .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid));

        for encoding in ["deflate", "br", "zstd"] {
            let resp = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/echo")
                        .header("content-encoding", encoding)
                        .body(Body::from("payload"))
                        .unwrap(),
                )
                .await
                .unwrap();

            // IB-5 moves the status to the code's; the detail survives in `accept-encoding`.
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "{encoding}");
            assert_eq!(
                resp.headers().get("accept-encoding").unwrap(),
                "gzip",
                "{encoding} must be told what is accepted"
            );
            let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["error"]["code"], "malformed_request", "{encoding}");
            assert_eq!(body["error"]["type"], "invalid_request_error", "{encoding}");
        }

        // gzip still routes, and a routed response must not be rewritten or double-counted.
        //
        // Both middlewares see every routed response — `middleware` is a `route_layer`,
        // `observe_bypassed` an outer one — so only the `Observed` marker stops the second
        // from counting it again. Asserting the status alone left that marker unguarded:
        // deleting its early return would count every request twice and grow a phantom
        // `unrouted` series at full traffic rate, with the suite still green. The routed
        // series is +1 either way, so the `unrouted` one is the only witness.
        use super::{NO_DATA_SOURCE, UNROUTED_ENDPOINT};
        use crate::metrics::{http_labels, HTTP_STATUS};

        let count = |endpoint: &str| {
            HTTP_STATUS
                .get_or_create(&http_labels(
                    endpoint.to_owned(),
                    StatusCode::OK,
                    NO_DATA_SOURCE.to_owned(),
                    None,
                ))
                .get()
        };
        let routed_before = count("/echo");
        let unrouted_before = count(UNROUTED_ENDPOINT);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .body(Body::from("payload"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            count("/echo"),
            routed_before + 1,
            "the routed middleware must count it once"
        );
        assert_eq!(
            count(UNROUTED_ENDPOINT),
            unrouted_before,
            "a response the router did route must not also be counted as unrouted"
        );
    }

    /// A path the router never matched also bypasses the routed middleware (GAP-16).
    #[tokio::test]
    async fn an_unmatched_route_answers_the_envelope() {
        use axum::{
            body::Body,
            http::{Request, StatusCode},
            middleware::from_fn,
            routing::get,
            Router,
        };
        use tower::ServiceExt;

        let app = Router::new()
            .route("/known", get(|| async { "ok" }))
            .route_layer(from_fn(super::middleware))
            .layer(from_fn(super::observe_bypassed));

        let resp = app
            .oneshot(Request::builder().uri("/nope").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["code"], "not_found");
        assert_eq!(body["error"]["type"], "invalid_request_error");
    }

    /// An id that cannot be represented as ASCII is malformed input. It must not reach the
    /// handler, and the 400 still needs a generated correlation id in its response header.
    #[tokio::test]
    async fn a_non_ascii_request_id_is_rejected_with_a_generated_id() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };

        use crate::types::RequestError;
        use axum::{
            body::Body,
            http::{header::HeaderValue, Request, StatusCode},
            middleware::from_fn,
            response::IntoResponse,
            routing::get,
            Router,
        };
        use tower::ServiceExt;
        use tower_http::{
            cors::CorsLayer,
            request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
        };

        // Production order: reject outside Set and CORS, and generate the rejection's id
        // directly. Otherwise CORS can answer a preflight before validation runs.
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let calls = handler_calls.clone();
        let app = Router::new()
            .route(
                "/boom",
                get(move || {
                    let calls = calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::Relaxed);
                        RequestError::Unavailable.into_response()
                    }
                }),
            )
            .route_layer(from_fn(super::middleware))
            .layer(from_fn(super::observe_bypassed))
            .layer(CorsLayer::permissive())
            .layer(PropagateRequestIdLayer::x_request_id())
            .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
            .layer(from_fn(super::reject_non_ascii_request_id));

        let obs_text = HeaderValue::from_bytes(&[0x80, 0x81]).unwrap();
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/boom")
                    .header("x-request-id", obs_text.clone())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let status = resp.status();
        let echoed = resp.headers().get("x-request-id").cloned().unwrap();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_ne!(echoed, obs_text, "the malformed id must not be echoed");
        let echoed = echoed.to_str().expect("the generated id must be text");
        assert!(
            uuid::Uuid::parse_str(echoed).is_ok(),
            "the validator should have supplied a UUID, got {echoed}"
        );
        assert_eq!(body["error"]["code"], "malformed_request");
        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert_eq!(handler_calls.load(Ordering::Relaxed), 0);
        assert!(
            !body["error"]
                .as_object()
                .unwrap()
                .contains_key("request_id"),
            "4xx bodies do not duplicate the response header: {body}"
        );

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/boom")
                    .header("origin", "https://example.com")
                    .header("access-control-request-method", "GET")
                    .header("x-request-id", obs_text)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert!(
            resp.headers()
                .get("x-request-id")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| uuid::Uuid::parse_str(value).is_ok()),
            "a malformed preflight must be rejected before CORS"
        );
        assert_eq!(handler_calls.load(Ordering::Relaxed), 0);

        // A usable client id is still preserved verbatim — the support flow depends on it.
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/boom")
                    .header("x-request-id", "req-abc123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.headers()["x-request-id"], "req-abc123");
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["request_id"], "req-abc123");
        assert_eq!(handler_calls.load(Ordering::Relaxed), 1);
    }

    // Defense in depth: production rejects this request before routing, but the logging
    // middleware must still not panic if embedded in a stack without that validator.
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
