//! The one place bytes are counted (REQ-60, INV-32).
//!
//! **What is counted.** Encoded response-body bytes, as frames are yielded to
//! the transport. That is one number for every gated route, whatever the route
//! did to produce it — network fan-out, real-time proxy, SQL plan — because it
//! is measured after all of them, at the only point they have in common.
//!
//! **What is not.** No decompression, anywhere: phase 2 adds no codec tap and
//! counts nothing that would require inflating a byte. Logical size is
//! estimated at read time from (dataset family, encoding), where it can be
//! recalibrated and applied retroactively (ADR-016). Response headers and HTTP
//! framing are outside the body and so outside the count.
//!
//! **How accurate.** A frame is counted when the body yields it, not when the
//! socket drains it, so a connection that dies with data buffered leaves the
//! count above what the client received. Per-event bytes are therefore
//! approximate in both directions; only *event loss* makes a total a lower
//! bound (D5). Both are stated rather than fixed: making the count exact means
//! measuring at the socket, which is a layer the portal does not own.
//!
//! **What it costs.** One `Instant::now()` and one add per data frame, plus a
//! `try_send` per record. Frames here are chunk-sized, so the per-byte cost is
//! nil, but the per-frame cost still needs to be measured (CT-6).

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, SystemTime},
};

use axum::{
    body::Body,
    extract::{Request, State},
    http::{header, HeaderMap, Method, StatusCode},
    middleware::Next,
    response::Response,
};
use bytes::Buf;
use http_body::{Body as HttpBody, Frame, SizeHint};
use tokio::time::Instant;

use super::{
    event::{unix_seconds, Encoding, Status, UsageEvent, Window},
    Attribution, UsageSink,
};
use crate::endpoints::stream::DataSource;

/// Runs outside the router and response-rewriting middleware, so the tap sees
/// the final body, including stamped errors and HEAD's empty response. Only
/// responses attributed by the gate are measured.
pub(crate) async fn tap_middleware(
    State(sink): State<Arc<UsageSink>>,
    req: Request,
    next: Next,
) -> Response {
    let method = req.method().clone();
    let mut response = next.run(req).await;
    match response.extensions_mut().remove::<Attribution>() {
        Some(attribution) => measure(sink, attribution, &method, response),
        None => response,
    }
}

/// How this response's body is delimited, and therefore what counts as having
/// delivered all of it (RFC 9112 §6). Decided once, from the same three inputs
/// hyper decides it from, before a byte moves.
///
/// This is deliberately *not* `Body::is_end_stream`. That is an optional hint
/// whose trait default is `false`, so a body is free to never admit it has
/// ended — `axum::body::Body::from_stream` never does. Asking it turns every
/// fixed-length proxied response into an apparent hang-up. Framing is a
/// property of the message, and no body type can be wrong about it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Framing {
    /// The message cannot carry a body at all, whatever its headers say.
    Empty,
    /// Delimited by a byte count, from `Content-Length` or from an exact size
    /// hint — the latter being what hyper would put in the header itself.
    Length(u64),
    /// Delimited by the end of the stream, so only the stream ending proves it.
    Chunked,
}

impl Framing {
    fn of(method: &Method, status: StatusCode, headers: &HeaderMap, hint: &SizeHint) -> Self {
        // A response to HEAD is bodiless however it is framed, and 204/304 may
        // not carry one. Axum has already dropped the body by the time the tap
        // sees it, so the bytes are genuinely zero and zero is the whole of it.
        if method == Method::HEAD
            || status == StatusCode::NO_CONTENT
            || status == StatusCode::NOT_MODIFIED
        {
            return Self::Empty;
        }
        // Chunked wins over a length, so a message carrying both is delimited
        // by its stream and nothing may be concluded from the count.
        if headers.contains_key(header::TRANSFER_ENCODING) {
            return Self::Chunked;
        }
        headers
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .or_else(|| hint.exact())
            .map_or(Self::Chunked, Self::Length)
    }
}

/// Why the body stopped yielding — the other half of the completion question.
#[derive(Debug, Clone, Copy)]
enum Stopped {
    /// The stream ended of its own accord.
    Eof,
    /// The body failed mid-flight.
    Error,
    /// The transport let the body go without polling it to either. Whether that
    /// is delivery or a hang-up is what [`Framing`] answers.
    Dropped,
}

/// Wraps the response body so its frames are counted on the way out. The status
/// line, the headers and the body's own bytes are untouched: the wrapper
/// forwards every frame, error and end-of-stream signal exactly as it found it,
/// including the size hint — a lost hint would let the transport re-frame a
/// fixed-length response as chunked, which is a client-visible change no
/// measurement is allowed to make.
fn measure(
    sink: Arc<UsageSink>,
    attribution: Attribution,
    method: &Method,
    response: Response,
) -> Response {
    let (parts, body) = response.into_parts();
    // The hint is read before wrapping because that is what hyper will read: a
    // body with an exact size and no `Content-Length` gets one from hyper, and
    // is then length-delimited on the wire.
    let framing = Framing::of(method, parts.status, &parts.headers, &body.size_hint());
    let meter = Meter::new(
        sink,
        attribution,
        Encoding::of(&parts.headers),
        DataSource::of(&parts.extensions, &parts.headers),
        framing,
    );
    Response::from_parts(parts, Body::new(MeasuredBody { inner: body, meter }))
}

/// The running count for one response, and the clock that cuts it into records.
struct Meter {
    sink: Arc<UsageSink>,
    attribution: Attribution,
    encoding: Encoding,
    /// Read once, like the encoding: a response is served by one source (INV-13),
    /// chosen before its first byte.
    data_source: Option<DataSource>,
    interim: Duration,
    /// One wall read per response; every record's `started_at` is this plus a
    /// monotone offset, so a clock step cannot make two records of the same
    /// response overlap or run backwards.
    started_at: f64,
    started: Instant,
    /// When the open record's window began.
    window: Instant,
    /// Bytes yielded since the last record.
    pending: u64,
    /// Bytes yielded by this response in total. Distinct from `pending`, which
    /// an interim record resets — a length-delimited response that outlives
    /// `P-USAGE-INTERIM` would otherwise never be seen to reach its length.
    yielded: u64,
    framing: Framing,
    finished: bool,
}

impl Meter {
    fn new(
        sink: Arc<UsageSink>,
        attribution: Attribution,
        encoding: Encoding,
        data_source: Option<DataSource>,
        framing: Framing,
    ) -> Self {
        let interim = sink.interim();
        let started = Instant::now();
        Self {
            sink,
            attribution,
            encoding,
            data_source,
            interim,
            started_at: unix_seconds(SystemTime::now()),
            started,
            window: started,
            pending: 0,
            yielded: 0,
            framing,
            finished: false,
        }
    }

    /// One yielded frame. When the open window has already run longer than
    /// `P-USAGE-INTERIM`, it is cut *before* this frame is counted, and the
    /// frame opens the next window. Counting first and cutting after would date
    /// the frame to the old window's start — after an idle hour, an hour before
    /// it was sent, which the control plane's hourly rollup of `started_at`
    /// then shows as usage in the wrong hour. Cut first, every byte is dated
    /// less than one interim before it was yielded.
    ///
    /// The cut is on a frame boundary, so a record is never made while a frame
    /// is half-counted. That boundary is also the caveat REQ-60 states: bytes
    /// counted before a stream went idle stay unreported until its next frame
    /// or its end. They are delayed, not misdated — the record that finally
    /// carries them still starts where their window did, and its duration runs
    /// to the cut, idle time included.
    ///
    /// An expired window with nothing in it is cut like any other, as an empty
    /// `open` record, rather than silently moved forward. Records tile the
    /// response — each starts at the previous one's cut, so their durations
    /// account for the response's whole life as a run of rate observations —
    /// and a moved window would leave a hole exactly where the response sat
    /// silent, and drop the response's own start from the record whenever its
    /// first byte came late. The price is an extra event, and it is bounded at one per
    /// response: every window after a cut opens with the frame that caused it,
    /// so only the first window can expire empty. Empty frames are what would
    /// break that bound, which is why they are not frames here: they carry
    /// nothing to date, so they neither count nor cut.
    fn observed(&mut self, bytes: usize) {
        // The terminal record is the last word on a response. Armor, not a live
        // case: a frame arriving after it would open a window the completion
        // already closed, and cut a delta nothing would ever terminate. An
        // empty frame is turned away for the reason above.
        if self.finished || bytes == 0 {
            return;
        }
        let now = Instant::now();
        if now.duration_since(self.window) >= self.interim {
            self.cut(now, Status::Open);
        }
        self.pending = self.pending.saturating_add(bytes as u64);
        self.yielded = self.yielded.saturating_add(bytes as u64);
    }

    /// The end, however it came: exactly one terminal record per measured
    /// response, even at zero residual bytes. That record is what says the
    /// response ended and how — and on a request that served nothing it is
    /// still the measurement, because "this key made this request" is a fact
    /// the table needs.
    ///
    /// Idempotent: a body that reaches EOF and is then dropped — the ordinary
    /// case — must not report its tail twice.
    ///
    /// The ending is derived, not asserted by the caller: only the body knows
    /// *how* it stopped, and only the framing knows what stopping there means.
    fn finish(&mut self, stopped: Stopped) {
        if self.finished {
            return;
        }
        self.finished = true;
        let status = match (stopped, self.framing) {
            // Ending is not the same as delivering: a body that runs out before
            // the length its own message declared leaves the client an
            // incomplete message, however cleanly the stream finished. Asking
            // the framing here too, rather than taking the end of stream as the
            // answer, is the mirror of the case this table exists for — and the
            // quieter one, since it errs toward calling a truncation a success.
            (Stopped::Eof, Framing::Length(length)) if self.yielded < length => {
                Status::Disconnected
            }
            // A stream that ended, or failed, said so itself.
            (Stopped::Eof, _) => Status::Completed,
            (Stopped::Error, _) => Status::Disconnected,
            // Everything below is a body the transport dropped without polling
            // to either, which is the ordinary end of every response hyper can
            // frame in advance — not a hang-up.
            (Stopped::Dropped, Framing::Empty) => Status::Completed,
            (Stopped::Dropped, Framing::Length(length)) if self.yielded >= length => {
                Status::Completed
            }
            // Short of its declared length, or delimited by a stream that never
            // ended: the client did not get an ending.
            (Stopped::Dropped, _) => Status::Disconnected,
        };
        self.cut(Instant::now(), status);
    }

    fn cut(&mut self, now: Instant, status: Status) {
        let window = Window {
            started_at: self.started_at + offset(self.started, self.window),
            duration: now.duration_since(self.window),
        };
        let event = UsageEvent::new(
            &self.attribution,
            self.encoding,
            self.data_source,
            self.pending,
            window,
            status,
        );
        self.pending = 0;
        self.window = now;
        self.sink.record(event, now);
    }
}

fn offset(started: Instant, at: Instant) -> f64 {
    at.duration_since(started).as_secs_f64()
}

/// A body that counts what it passes through and reports what it counted.
///
/// `Drop` is load-bearing, not a safety net: a client that hangs up mid-stream
/// is the normal end of a long stream, and a wrapper that only reported at EOF
/// would systematically under-report exactly the responses that carry the most
/// bytes.
///
/// How a dropped body ended is answered from the response's framing, fixed at
/// wrap time, so `Drop` never has to interrogate the body it is dropping — and
/// the bound stays on the impls that actually need it.
struct MeasuredBody<B> {
    inner: B,
    meter: Meter,
}

impl<B> HttpBody for MeasuredBody<B>
where
    B: HttpBody + Unpin,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = &mut *self;
        let polled = Pin::new(&mut this.inner).poll_frame(cx);
        match &polled {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    this.meter.observed(data.remaining());
                }
            }
            // The body failed mid-flight. The response is already committed, so
            // this is a truncation (INV-25) — the bytes that did go out still
            // happened, and the record says the client did not get an ending.
            Poll::Ready(Some(Err(_))) => this.meter.finish(Stopped::Error),
            Poll::Ready(None) => this.meter.finish(Stopped::Eof),
            Poll::Pending => {}
        }
        polled
    }

    /// Forwarded, not re-derived. See [`measure`].
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl<B> Drop for MeasuredBody<B> {
    fn drop(&mut self) {
        // Reached by every response hyper can frame in advance, because it stops
        // polling the moment its encoder is satisfied and drops the body: a
        // 204, a HEAD, an empty stream, and every `Content-Length` response
        // including the error envelopes. Only the framing separates those from
        // a client that went away, and it is the framing that is asked.
        self.meter.finish(Stopped::Dropped);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{
        body::Body,
        http::{header, HeaderValue, StatusCode},
        routing::{get, MethodRouter},
        Router,
    };
    use futures::StreamExt;
    use tokio::sync::mpsc;
    use tower::{ServiceBuilder, ServiceExt};

    use super::*;
    use crate::auth::{cache::CachedGrant, usage::Queued};

    const INTERIM: Duration = Duration::from_secs(30);

    fn attribution() -> Attribution {
        Attribution::new(
            Arc::new(CachedGrant {
                key_id: "k1".to_owned(),
                datasets: None,
                organization_id: Some("org-7".to_owned()),
                refresh_after: 1,
                expires_at: 2,
            }),
            Some("ethereum-mainnet".to_owned()),
            Arc::from("/stream"),
        )
    }

    /// A response with the given body, wrapped exactly as the layer wraps one.
    fn measured(
        body: Body,
        encoding: Option<&str>,
        interim: Duration,
    ) -> (Response, mpsc::Receiver<Queued>) {
        let (sink, events) = UsageSink::for_test(64, interim);
        let mut response = Response::new(body);
        if let Some(encoding) = encoding {
            response.headers_mut().insert(
                header::CONTENT_ENCODING,
                HeaderValue::from_str(encoding).unwrap(),
            );
        }
        (measure(sink, attribution(), &Method::GET, response), events)
    }

    async fn read(body: Body) -> usize {
        axum::body::to_bytes(body, usize::MAX).await.unwrap().len()
    }

    fn drain(events: &mut mpsc::Receiver<Queued>) -> Vec<UsageEvent> {
        let mut drained = Vec::new();
        while let Ok(queued) = events.try_recv() {
            drained.push(queued.event);
        }
        drained
    }

    fn total(events: &[UsageEvent]) -> u64 {
        events.iter().map(|event| event.wire_bytes).sum()
    }

    /// Same boundary as the HTTP server: attribution inside the method router,
    /// normalization and request-ID stamping, then a tap around the whole service.
    async fn serve(
        route: MethodRouter,
        sink: Option<Arc<UsageSink>>,
        method: &str,
        uri: &str,
    ) -> Response {
        use tower_http::request_id::{MakeRequestUuid, SetRequestIdLayer};

        let app = Router::new()
            .route(
                "/probe",
                route.layer(axum::middleware::from_fn(
                    |req: Request, next: Next| async move {
                        let mut response = next.run(req).await;
                        response.extensions_mut().insert(attribution());
                        response
                    },
                )),
            )
            .route_layer(axum::middleware::from_fn(crate::utils::logging::middleware))
            .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid));
        ServiceBuilder::new()
            .option_layer(
                sink.map(|sink| axum::middleware::from_fn_with_state(sink, tap_middleware)),
            )
            .service(app)
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .header("x-request-id", "usage-test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn rewritten_errors_report_the_bytes_actually_served() {
        use crate::types::{coded_response, ErrorCode};
        use axum::extract::Query;

        for (route, status) in [
            (
                get(|| async { coded_response(ErrorCode::UpstreamUnavailable, "upstream failed") }),
                StatusCode::BAD_GATEWAY,
            ),
            (
                get(|Query(_): Query<std::collections::HashMap<String, u64>>| async { "unused" }),
                StatusCode::BAD_REQUEST,
            ),
        ] {
            let (sink, mut events) = UsageSink::for_test(8, INTERIM);
            let response = serve(route, Some(sink), "GET", "/probe?count=invalid").await;
            assert_eq!(response.status(), status);
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert!(body["error"].is_object());
            if status.is_server_error() {
                assert_eq!(body["error"]["request_id"], "usage-test");
            }
            let events = drain(&mut events);
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].wire_bytes, bytes.len() as u64);
            assert_eq!(events[0].status, Status::Completed);
        }
    }

    #[tokio::test]
    async fn a_head_request_reports_a_completed_empty_response() {
        let (sink, mut events) = UsageSink::for_test(8, INTERIM);
        let response = serve(get(|| async { "served" }), Some(sink), "HEAD", "/probe").await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CONTENT_LENGTH], "6");
        assert_eq!(read(response.into_body()).await, 0);
        let events = drain(&mut events);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].wire_bytes, 0);
        assert_eq!(events[0].status, Status::Completed);
    }

    /// hyper never polls a fixed-length body to `None`: it stops the moment its
    /// encoder is satisfied. Caught live — a fully delivered 400 envelope
    /// recorded as `disconnected` — because every prior test polled to EOF by
    /// hand, which only chunked bodies experience.
    #[tokio::test]
    async fn a_fixed_length_body_completes_when_hyper_stops_at_end_stream() {
        let (response, mut events) = measured(Body::from("0123456789"), None, INTERIM);
        let mut body = response.into_body();

        let frame = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
            .await
            .expect("one frame")
            .expect("no error");
        assert_eq!(frame.data_ref().expect("data").remaining(), 10);
        assert!(body.is_end_stream(), "the premise: hyper would stop here");
        drop(body);

        let events = drain(&mut events);
        assert_eq!(events.len(), 1, "one terminal record, not one per path");
        assert_eq!(events[0].status, Status::Completed);
        assert_eq!(events[0].wire_bytes, 10);
    }

    /// A response shaped like `stream_response`'s: a stream body carrying
    /// whatever framing headers the upstream sent, forwarded verbatim.
    fn proxied(
        status: StatusCode,
        chunks: Vec<&'static str>,
        content_length: Option<usize>,
    ) -> (Response, mpsc::Receiver<Queued>) {
        let (sink, events) = UsageSink::for_test(64, INTERIM);
        let body =
            Body::from_stream(futures::stream::iter(chunks.into_iter().map(|chunk| {
                Ok::<_, std::io::Error>(bytes::Bytes::from_static(chunk.as_bytes()))
            })));
        let mut builder = Response::builder().status(status);
        if let Some(length) = content_length {
            builder = builder.header(header::CONTENT_LENGTH, length.to_string());
        }
        let response = builder.body(body).unwrap();
        (measure(sink, attribution(), &Method::GET, response), events)
    }

    async fn poll_one(body: &mut Body) -> usize {
        std::future::poll_fn(|cx| Pin::new(&mut *body).poll_frame(cx))
            .await
            .expect("one frame")
            .expect("no error")
            .data_ref()
            .expect("data")
            .remaining()
    }

    /// The defect this framing exists to remove. `Body::from_stream` never
    /// admits end of stream — the trait default is `false` and axum's
    /// `StreamBody` does not override it — so every `Content-Length` response
    /// on the proxied real-time path reached `Drop` looking like a hang-up.
    #[tokio::test]
    async fn a_length_delimited_stream_completes_though_it_never_admits_end_of_stream() {
        let (response, mut events) = proxied(StatusCode::OK, vec!["0123456789"], Some(10));
        let mut body = response.into_body();

        assert_eq!(poll_one(&mut body).await, 10);
        assert!(
            !body.is_end_stream(),
            "the premise: a stream body never admits it ended"
        );
        drop(body);

        let events = drain(&mut events);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].status, Status::Completed);
        assert_eq!(events[0].wire_bytes, 10);
    }

    /// The head-tailing steady state: a client polling past the head gets a 204
    /// whose body hyper never polls at all.
    #[tokio::test]
    async fn a_bodiless_proxied_response_completes_without_ever_being_polled() {
        let (response, mut events) = proxied(StatusCode::NO_CONTENT, vec![], None);
        drop(response.into_body());

        let events = drain(&mut events);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].status, Status::Completed);
        assert_eq!(events[0].wire_bytes, 0);
    }

    /// The other half, and the one that stops the fix from being "call
    /// everything completed": a body delimited by its stream proves delivery
    /// only by ending, so one that stops early is a hang-up.
    #[tokio::test]
    async fn a_chunked_stream_dropped_early_is_still_a_disconnect() {
        let (response, mut events) = proxied(StatusCode::OK, vec!["0123456789", "rest"], None);
        let mut body = response.into_body();

        assert_eq!(poll_one(&mut body).await, 10);
        drop(body);

        let events = drain(&mut events);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].status, Status::Disconnected);
        assert_eq!(events[0].wire_bytes, 10);
    }

    /// Short of its declared length is a hang-up too — the count is what
    /// separates the two, not the mere presence of a length.
    #[tokio::test]
    async fn a_length_delimited_stream_dropped_short_is_a_disconnect() {
        let (response, mut events) = proxied(StatusCode::OK, vec!["0123456789", "rest"], Some(14));
        let mut body = response.into_body();

        assert_eq!(poll_one(&mut body).await, 10);
        drop(body);

        let events = drain(&mut events);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].status, Status::Disconnected);
        assert_eq!(events[0].wire_bytes, 10);
    }

    /// The quiet half of the same question. A stream that ends is normally the
    /// strongest proof of delivery there is, except where the message already
    /// said how much it would carry: ending short of that is a truncation the
    /// client sees, and recording it as a delivery is the one error that never
    /// looks like one on a graph.
    #[tokio::test]
    async fn a_length_delimited_stream_that_ends_short_is_a_disconnect() {
        let (response, mut events) = proxied(StatusCode::OK, vec!["0123456789"], Some(20));
        let mut body = response.into_body();

        assert_eq!(poll_one(&mut body).await, 10);
        // Polled to the end, unlike every other short case: the stream really
        // did finish, it just had less in it than the header promised.
        assert!(
            std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
                .await
                .is_none(),
            "the premise: the stream ended of its own accord"
        );

        let events = drain(&mut events);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].status, Status::Disconnected);
        assert_eq!(events[0].wire_bytes, 10);
    }

    /// The taxonomy itself (RFC 9112 §6), including the two cases no body type
    /// can report: a HEAD response, and a message framed both ways at once.
    #[test]
    fn framing_follows_the_message_not_the_body() {
        let headers = |pairs: &[(&str, &str)]| {
            let mut headers = HeaderMap::new();
            for (name, value) in pairs {
                headers.insert(
                    axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                    HeaderValue::from_str(value).unwrap(),
                );
            }
            headers
        };
        let none = SizeHint::default();

        for (method, status, pairs, hint, expected) in [
            // A response to HEAD is bodiless however it is framed.
            (
                Method::HEAD,
                StatusCode::OK,
                vec![("content-length", "6")],
                &none,
                Framing::Empty,
            ),
            (
                Method::GET,
                StatusCode::NO_CONTENT,
                vec![],
                &none,
                Framing::Empty,
            ),
            (
                Method::GET,
                StatusCode::NOT_MODIFIED,
                vec![],
                &none,
                Framing::Empty,
            ),
            // Chunked wins, so nothing may be concluded from the count.
            (
                Method::GET,
                StatusCode::OK,
                vec![("content-length", "10"), ("transfer-encoding", "chunked")],
                &none,
                Framing::Chunked,
            ),
            (
                Method::GET,
                StatusCode::OK,
                vec![("content-length", "10")],
                &none,
                Framing::Length(10),
            ),
            // No header, but hyper would write one from the hint.
            (
                Method::GET,
                StatusCode::OK,
                vec![],
                &SizeHint::with_exact(10),
                Framing::Length(10),
            ),
            (Method::GET, StatusCode::OK, vec![], &none, Framing::Chunked),
            // Unreadable framing is no framing: fall back to the stream.
            (
                Method::GET,
                StatusCode::OK,
                vec![("content-length", "not-a-number")],
                &none,
                Framing::Chunked,
            ),
        ] {
            assert_eq!(
                Framing::of(&method, status, &headers(&pairs), hint),
                expected,
                "{method} {status} {pairs:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_completed_response_reports_the_bytes_it_yielded() {
        let (response, mut events) = measured(Body::from("0123456789"), Some("gzip"), INTERIM);

        assert_eq!(read(response.into_body()).await, 10, "body unchanged");

        let events = drain(&mut events);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].wire_bytes, 10);
        assert_eq!(events[0].status, Status::Completed);
        assert_eq!(events[0].encoding, Encoding::Gzip);
        assert_eq!(events[0].key_id, "k1");
        assert_eq!(events[0].organization_id.as_deref(), Some("org-7"));
        assert_eq!(events[0].dataset.as_deref(), Some("ethereum-mainnet"));
        assert_eq!(events[0].endpoint, "/stream");
    }

    /// D2's whole claim: interim records are *deltas*, so the sum over a
    /// response is its total with nothing counted twice and nothing missing.
    #[tokio::test(start_paused = true)]
    async fn interim_deltas_and_the_residual_sum_to_the_total() {
        let chunks = futures::stream::unfold(0usize, |sent| async move {
            if sent == 4 {
                return None;
            }
            // Two frames per window, so the cut lands on a frame boundary
            // rather than on the first frame of the stream.
            tokio::time::sleep(Duration::from_secs(20)).await;
            Some((
                Ok::<_, std::io::Error>(bytes::Bytes::from(vec![b'x'; 1000])),
                sent + 1,
            ))
        });
        let (response, mut events) = measured(
            Body::from_stream(chunks),
            Some("zstd"),
            Duration::from_secs(30),
        );

        assert_eq!(read(response.into_body()).await, 4000);

        let events = drain(&mut events);
        assert!(
            events.len() >= 2,
            "a stream running past the interim must report before it ends: {events:?}"
        );
        assert_eq!(
            total(&events),
            4000,
            "deltas must sum to the bytes served, exactly once each"
        );
        let (last, interims) = events.split_last().expect("at least one record");
        assert_eq!(last.status, Status::Completed);
        assert!(
            interims.iter().all(|event| event.status == Status::Open),
            "only the final record ends the response: {events:?}"
        );
        assert!(
            interims
                .iter()
                .all(|event| event.duration_ms >= 30_000 && event.wire_bytes > 0),
            "an interim covers at least P-USAGE-INTERIM of measured time: {events:?}"
        );
        assert!(
            interims
                .windows(2)
                .all(|pair| pair[0].started_at < pair[1].started_at),
            "records must partition the response in order: {events:?}"
        );
    }

    /// A meter driven by hand, so a test can place each frame on the clock
    /// exactly — including frames no real body would bother to send.
    fn meter(interim: Duration) -> (Meter, mpsc::Receiver<Queued>) {
        let (sink, events) = UsageSink::for_test(64, interim);
        let meter = Meter::new(
            sink,
            attribution(),
            Encoding::Identity,
            None,
            Framing::Chunked,
        );
        (meter, events)
    }

    /// Seconds between two records' starts. Both are one wall read plus a
    /// monotone offset, so under paused time the difference is the offset.
    fn apart(earlier: &UsageEvent, later: &UsageEvent) -> f64 {
        later.started_at - earlier.started_at
    }

    /// The defect this ordering exists to remove: a frame that ends a long idle
    /// was counted into the window the idle began in, so bytes sent now were
    /// dated hours ago — into the wrong hour of the control plane's rollup.
    #[tokio::test(start_paused = true)]
    async fn a_frame_after_a_long_idle_is_dated_to_its_arrival() {
        let (mut meter, mut events) = meter(INTERIM);

        meter.observed(100);
        tokio::time::advance(Duration::from_secs(2 * 3600)).await;
        meter.observed(50);
        tokio::time::advance(Duration::from_secs(5)).await;
        meter.finish(Stopped::Eof);

        let events = drain(&mut events);
        assert_eq!(events.len(), 2, "{events:?}");
        let (before, after) = (&events[0], &events[1]);
        assert_eq!(before.status, Status::Open);
        assert_eq!(
            before.wire_bytes, 100,
            "the old window carries only what it held before the idle"
        );
        assert_eq!(before.duration_ms, 2 * 3600 * 1000, "and runs to the cut");
        assert_eq!(after.status, Status::Completed);
        assert_eq!(after.wire_bytes, 50);
        assert!(
            (apart(before, after) - 7200.0).abs() < 1e-3,
            "the late frame opens a window at its own arrival: {events:?}"
        );
        assert_eq!(after.duration_ms, 5_000);
    }

    /// The other side of the same reordering: a steady stream still reports
    /// once per interim, the windows still tile the response, and moving the
    /// cut ahead of the count neither loses a byte nor counts one twice.
    #[tokio::test(start_paused = true)]
    async fn a_steady_stream_cuts_one_record_per_interim_and_loses_nothing() {
        let chunks = futures::stream::unfold(0usize, |sent| async move {
            if sent == 10 {
                return None;
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
            Some((
                Ok::<_, std::io::Error>(bytes::Bytes::from(vec![b'x'; 1000])),
                sent + 1,
            ))
        });
        let (response, mut events) =
            measured(Body::from_stream(chunks), None, Duration::from_secs(30));

        assert_eq!(read(response.into_body()).await, 10_000);

        let events = drain(&mut events);
        assert_eq!(
            total(&events),
            10_000,
            "every byte exactly once: {events:?}"
        );
        let (last, interims) = events.split_last().expect("at least one record");
        assert_eq!(last.status, Status::Completed);
        // Frames at 10 s .. 100 s: the ones at 30, 60 and 90 find the window
        // expired, cut it, and open the next one.
        assert_eq!(interims.len(), 3, "one record per interim: {events:?}");
        assert!(interims
            .iter()
            .all(|event| event.status == Status::Open && event.duration_ms == 30_000));
        assert_eq!(
            events
                .iter()
                .map(|event| event.wire_bytes)
                .collect::<Vec<_>>(),
            vec![2000, 3000, 3000, 2000],
            "each cut frame lands in the window it opens: {events:?}"
        );
        assert!(
            events.windows(2).all(|pair| {
                let gap = apart(&pair[0], &pair[1]) - pair[0].duration_ms as f64 / 1000.0;
                gap.abs() < 1e-3
            }),
            "each record starts where the previous one was cut: {events:?}"
        );
    }

    /// A window that expires before anything was counted in it is still cut,
    /// as an empty `open` record, so the response's start stays on record and
    /// the windows keep tiling it. Only the first window can expire empty.
    #[tokio::test(start_paused = true)]
    async fn a_late_first_frame_cuts_one_empty_window_ahead_of_it() {
        let (mut meter, mut events) = meter(INTERIM);

        tokio::time::advance(Duration::from_secs(90)).await;
        meter.observed(100);
        tokio::time::advance(Duration::from_secs(40)).await;
        meter.observed(10);
        meter.finish(Stopped::Eof);

        let events = drain(&mut events);
        assert_eq!(events.len(), 3, "{events:?}");
        assert_eq!(events[0].status, Status::Open);
        assert_eq!(events[0].wire_bytes, 0);
        assert_eq!(events[0].duration_ms, 90_000, "from the response's start");
        assert_eq!(events[1].status, Status::Open);
        assert_eq!(events[1].wire_bytes, 100, "the late frame's own window");
        assert!((apart(&events[0], &events[1]) - 90.0).abs() < 1e-3);
        assert_eq!(events[2].status, Status::Completed);
        assert_eq!(events[2].wire_bytes, 10);
        assert_eq!(total(&events), 110);
    }

    /// Empty frames carry nothing to date, so they neither count nor cut. Were
    /// they frames here, a body yielding them on an idle stream would mint an
    /// empty record every interim.
    #[tokio::test(start_paused = true)]
    async fn empty_frames_never_cut_a_window() {
        let (mut meter, mut events) = meter(INTERIM);

        for _ in 0..4 {
            tokio::time::advance(Duration::from_secs(40)).await;
            meter.observed(0);
        }
        meter.finish(Stopped::Eof);

        let events = drain(&mut events);
        assert_eq!(events.len(), 1, "only the terminal record: {events:?}");
        assert_eq!(events[0].status, Status::Completed);
        assert_eq!(events[0].wire_bytes, 0);
        assert_eq!(events[0].duration_ms, 160_000);
    }

    /// The end that matters most on a streaming product: nobody sends EOF, the
    /// client just leaves. Reporting only at EOF would lose the whole response.
    #[tokio::test]
    async fn a_body_dropped_mid_stream_reports_what_it_served() {
        let (response, mut events) = measured(
            Body::from_stream(futures::stream::iter(vec![
                Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"first")),
                Ok(bytes::Bytes::from_static(b"second")),
            ])),
            None,
            INTERIM,
        );

        let mut frames = response.into_body().into_data_stream();
        let first = frames.next().await.expect("a frame").unwrap();
        assert_eq!(first.len(), 5);
        drop(frames);

        let events = drain(&mut events);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].wire_bytes, 5, "only what was yielded is counted");
        assert_eq!(events[0].status, Status::Disconnected);
        assert_eq!(events[0].encoding, Encoding::Identity);
    }

    /// The ordinary lifecycle is EOF *then* drop. Counting the tail on both
    /// would inflate every completed response by its last window.
    #[tokio::test]
    async fn a_completed_body_is_not_reported_again_when_it_is_dropped() {
        let (response, mut events) = measured(Body::from("0123456789"), None, INTERIM);

        assert_eq!(read(response.into_body()).await, 10);

        let events = drain(&mut events);
        assert_eq!(events.len(), 1, "{events:?}");
        assert_eq!(total(&events), 10);
    }

    /// A 204 carries no bytes and still says a key made a request. Without this
    /// the table would show nothing at all for the polling clients that are the
    /// steady state of the product.
    ///
    /// Dropped without a poll, because that is the only life hyper gives such a
    /// body: it reads `is_end_stream` off the response and never polls at all.
    /// Driving it to `None` by hand — which is what this test used to do —
    /// exercises a path the transport never takes and would pass with `Drop`
    /// mislabelling every empty response as a hang-up. CT-11 witnesses the same
    /// claim against a real server.
    #[tokio::test]
    async fn a_response_with_no_body_still_reports_the_request() {
        let (response, mut events) = measured(Body::empty(), None, INTERIM);
        let body = response.into_body();

        assert!(
            body.is_end_stream(),
            "the premise: hyper would never poll this body"
        );
        drop(body);

        let events = drain(&mut events);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].wire_bytes, 0);
        assert_eq!(events[0].status, Status::Completed);
    }

    /// Measurement may not change what the transport does with the response. A
    /// dropped size hint re-frames a fixed-length body as chunked, which is
    /// visible to every client and to every proxy in between.
    #[tokio::test]
    async fn the_wrapper_preserves_the_size_hint_and_end_of_stream_signal() {
        let plain = Body::from("0123456789");
        let expected = (plain.size_hint().exact(), plain.is_end_stream());
        let (response, _events) = measured(Body::from("0123456789"), None, INTERIM);

        let measured = response.into_body();

        assert_eq!(expected.0, Some(10), "the fixture must have an exact hint");
        assert_eq!(measured.size_hint().exact(), expected.0);
        assert_eq!(measured.is_end_stream(), expected.1);
    }

    /// The response the client gets is the response the handler wrote — the
    /// property the whole design exists to keep (INV-32).
    #[tokio::test]
    async fn a_measured_response_is_indistinguishable_from_an_unmeasured_one() {
        async fn handler() -> impl axum::response::IntoResponse {
            (
                StatusCode::PARTIAL_CONTENT,
                [(header::CONTENT_TYPE, "application/jsonl")],
                "one\ntwo\n",
            )
        }
        let (sink, _events) = UsageSink::for_test(8, INTERIM);
        let served = |response: Response| async move {
            let status = response.status();
            let headers = response.headers().clone();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            (status, headers, body)
        };

        let measured = serve(get(handler), Some(sink), "GET", "/probe").await;
        let plain = serve(get(handler), None, "GET", "/probe").await;
        assert_eq!(served(measured).await, served(plain).await);
    }

    /// Shadow mode admits requests that presented no credential at all. There is
    /// nobody to attribute those bytes to, and inventing an owner would put
    /// unattributable volume in the table the pricing work reads.
    /// The record names the layer the response does (DEF-6), read the way the HTTP
    /// metrics read it: a handler's extension, a route's `served_by`, or the
    /// `x-sqd-data-source` header that `/stream` sets.
    #[tokio::test]
    async fn the_record_names_the_source_the_response_was_served_by() {
        use crate::{
            endpoints::stream::DATA_SOURCE_HEADER, utils::logging::EndpointAnnotationLayer,
        };

        let cases: [(MethodRouter, Option<DataSource>); 4] = [
            (
                get(|| async { (DataSource::RealTime, "served") }),
                Some(DataSource::RealTime),
            ),
            (
                get(|| async { ([(DATA_SOURCE_HEADER, "network")], "served") }),
                Some(DataSource::Network),
            ),
            (
                get(|| async { "served" })
                    .layer(EndpointAnnotationLayer::new("/fixed").served_by(DataSource::Network)),
                Some(DataSource::Network),
            ),
            (get(|| async { "served" }), None),
        ];
        for (route, expected) in cases {
            let (sink, mut events) = UsageSink::for_test(8, INTERIM);
            let response = serve(route, Some(sink), "GET", "/probe").await;
            assert_eq!(read(response.into_body()).await, 6);
            let events = drain(&mut events);
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].data_source, expected);
        }
    }

    /// Read once per response, so an interim record carries it as the last one does.
    #[tokio::test(start_paused = true)]
    async fn every_record_of_a_response_carries_its_source() {
        let chunks = futures::stream::unfold(0usize, |sent| async move {
            if sent == 3 {
                return None;
            }
            tokio::time::sleep(Duration::from_secs(40)).await;
            Some((
                Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"block")),
                sent + 1,
            ))
        });
        let (sink, mut events) = UsageSink::for_test(64, INTERIM);
        let response = Response::builder()
            .header(crate::endpoints::stream::DATA_SOURCE_HEADER, "real_time")
            .body(Body::from_stream(chunks))
            .unwrap();
        let response = measure(sink, attribution(), &Method::GET, response);

        assert_eq!(read(response.into_body()).await, 15);
        let events = drain(&mut events);
        assert!(
            events.len() >= 2,
            "the premise: interim records: {events:?}"
        );
        assert!(
            events
                .iter()
                .all(|event| event.data_source == Some(DataSource::RealTime)),
            "{events:?}"
        );
    }

    #[tokio::test]
    async fn a_request_the_gate_did_not_attribute_is_not_measured() {
        let (sink, mut events) = UsageSink::for_test(8, INTERIM);
        let app = ServiceBuilder::new()
            .layer(axum::middleware::from_fn_with_state(sink, tap_middleware))
            .service(Router::new().route("/probe", get(|| async { "served" })));

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/probe")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(read(response.into_body()).await, 6);
        assert!(drain(&mut events).is_empty());
    }

    /// The sink is the only thing the serving path touches, so its failure mode
    /// is the one that matters: a full queue drops the record and serves the
    /// response, rather than waiting for room.
    #[tokio::test]
    async fn a_full_queue_drops_records_without_touching_the_response() {
        let (sink, events) = UsageSink::for_test(1, INTERIM);
        let mut served = Vec::new();
        for _ in 0..8 {
            let response = measure(
                sink.clone(),
                attribution(),
                &Method::GET,
                Response::new(Body::from("0123456789")),
            );
            served.push(read(response.into_body()).await);
        }

        assert_eq!(served, vec![10; 8], "every response was served in full");
        assert_eq!(events.len(), 1, "the queue never grew past its bound");
    }
}
