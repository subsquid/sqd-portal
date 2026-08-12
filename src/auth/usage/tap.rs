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
//! nil, but the per-frame cost is real and is the budget CT-11 asserts against.

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, SystemTime},
};

use axum::{body::Body, extract::Request, response::Response};
use bytes::Buf;
use http_body::{Body as HttpBody, Frame, SizeHint};
use tokio::time::Instant;
use tower::{Layer, Service};

use super::{
    event::{unix_seconds, Encoding, Status, UsageEvent, Window},
    Attribution, UsageSink,
};

/// Installs the tap on one gated route. Applied *inside* the gate, which is the
/// only position that can see the attribution the gate deposits — a layer
/// wrapping the gate sees the request as it arrived, before any of it existed.
pub(in crate::auth) fn tap_layer(sink: Arc<UsageSink>) -> TapLayer {
    TapLayer { sink }
}

#[derive(Clone)]
pub struct TapLayer {
    sink: Arc<UsageSink>,
}

impl<S> Layer<S> for TapLayer {
    type Service = TapService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TapService {
            inner,
            sink: self.sink.clone(),
        }
    }
}

#[derive(Clone)]
pub struct TapService<S> {
    inner: S,
    sink: Arc<UsageSink>,
}

impl<S> Service<Request> for TapService<S>
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
        // Cloned out here rather than carried through: the response no longer
        // has the request's extensions to read it from.
        let attribution = req.extensions().get::<Attribution>().cloned();
        let sink = self.sink.clone();
        let fut = self.inner.call(req);

        Box::pin(async move {
            let response = fut.await?;
            // No grant, no record (D3): an unattributed request is one shadow
            // mode admitted without a credential, and inventing an owner for it
            // would put unattributable bytes in a table used to price keys.
            let Some(attribution) = attribution else {
                return Ok(response);
            };
            Ok(measure(sink, attribution, response))
        })
    }
}

/// Wraps the response body so its frames are counted on the way out. The status
/// line, the headers and the body's own bytes are untouched: the wrapper
/// forwards every frame, error and end-of-stream signal exactly as it found it,
/// including the size hint — a lost hint would let the transport re-frame a
/// fixed-length response as chunked, which is a client-visible change no
/// measurement is allowed to make.
fn measure(sink: Arc<UsageSink>, attribution: Attribution, response: Response) -> Response {
    let (parts, body) = response.into_parts();
    let meter = Meter::new(sink, attribution, Encoding::of(&parts.headers));
    Response::from_parts(parts, Body::new(MeasuredBody { inner: body, meter }))
}

/// The running count for one response, and the clock that cuts it into records.
struct Meter {
    sink: Arc<UsageSink>,
    attribution: Attribution,
    encoding: Encoding,
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
    finished: bool,
}

impl Meter {
    fn new(sink: Arc<UsageSink>, attribution: Attribution, encoding: Encoding) -> Self {
        let interim = sink.interim();
        let started = Instant::now();
        Self {
            sink,
            attribution,
            encoding,
            interim,
            started_at: unix_seconds(SystemTime::now()),
            started,
            window: started,
            pending: 0,
            finished: false,
        }
    }

    /// One yielded frame. Cuts an interim record when the open window has run
    /// longer than `P-USAGE-INTERIM` — on the frame boundary, so a record is
    /// never made while a frame is half-counted. That boundary is also the
    /// caveat REQ-60 states: an idle stream's counted-but-unreported bytes wait
    /// for its next frame or its end.
    fn observed(&mut self, bytes: usize) {
        // The terminal record is the last word on a response. Armor, not a live
        // case: a frame arriving after it would open a window the completion
        // already closed, and cut a delta nothing would ever terminate.
        if self.finished {
            return;
        }
        self.pending = self.pending.saturating_add(bytes as u64);
        let now = Instant::now();
        if now.duration_since(self.window) >= self.interim {
            self.cut(now, Status::Open);
        }
    }

    /// The end, however it came: exactly one terminal record per measured
    /// response, even at zero residual bytes. That record is what says the
    /// response ended and how — and on a request that served nothing it is
    /// still the measurement, because "this key made this request" is a fact
    /// the table needs.
    ///
    /// Idempotent: a body that reaches EOF and is then dropped — the ordinary
    /// case — must not report its tail twice.
    fn finish(&mut self, status: Status) {
        if self.finished {
            return;
        }
        self.finished = true;
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
/// The bound is on the struct rather than the impls because `Drop` needs it:
/// deciding how a dropped body ended means asking the inner body, and a `Drop`
/// impl may not require more than the type it drops.
struct MeasuredBody<B: HttpBody> {
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
                // A fixed-length body is never polled to `None`: hyper reads
                // `is_end_stream` after the final frame and stops, so waiting
                // for EOF would hand every Content-Length response — error
                // envelopes included — to `Drop`, which cannot tell delivery
                // from a hang-up and would label it disconnected.
                if this.inner.is_end_stream() {
                    this.meter.finish(Status::Completed);
                }
            }
            // The body failed mid-flight. The response is already committed, so
            // this is a truncation (INV-25) — the bytes that did go out still
            // happened, and the record says the client did not get an ending.
            Poll::Ready(Some(Err(_))) => this.meter.finish(Status::Disconnected),
            Poll::Ready(None) => this.meter.finish(Status::Completed),
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

impl<B: HttpBody> Drop for MeasuredBody<B> {
    fn drop(&mut self) {
        // The same question `poll_frame` asks after a frame, asked again for the
        // body that never gets one: a body already at end of stream when the
        // head is written — a 204, a HEAD, an empty stream — is never polled at
        // all, hyper drops it straight away. An unconditional `Disconnected`
        // here would label every fully delivered empty response a hang-up, and
        // on the head-tailing clients that poll into an empty range that is the
        // steady state rather than an edge.
        self.meter.finish(if self.inner.is_end_stream() {
            Status::Completed
        } else {
            Status::Disconnected
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{
        body::Body,
        http::{header, HeaderValue, StatusCode},
        routing::get,
        Router,
    };
    use futures::StreamExt;
    use tokio::sync::mpsc;
    use tower::ServiceExt;

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
        (measure(sink, attribution(), response), events)
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

    /// hyper never polls a fixed-length body to `None`: it reads
    /// `is_end_stream` after the final frame and stops. Caught live — a fully
    /// delivered 400 envelope recorded as `disconnected` — because every prior
    /// test polled to EOF by hand, which only chunked bodies experience.
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
        // Mirrors the mount: the tap sits inside the layer that attributes.
        let attributed = Router::new()
            .route("/probe", get(handler))
            .layer(tap_layer(sink))
            .layer(axum::middleware::from_fn(
                |mut req: Request, next: axum::middleware::Next| async move {
                    req.extensions_mut().insert(attribution());
                    next.run(req).await
                },
            ));
        let bare = Router::new().route("/probe", get(handler));

        let served = |app: Router| async move {
            let response = app
                .oneshot(
                    axum::http::Request::builder()
                        .uri("/probe")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let status = response.status();
            let headers = response.headers().clone();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            (status, headers, body)
        };

        assert_eq!(served(attributed).await, served(bare).await);
    }

    /// Shadow mode admits requests that presented no credential at all. There is
    /// nobody to attribute those bytes to, and inventing an owner would put
    /// unattributable volume in the table the pricing work reads.
    #[tokio::test]
    async fn a_request_the_gate_did_not_attribute_is_not_measured() {
        let (sink, mut events) = UsageSink::for_test(8, INTERIM);
        let app = Router::new()
            .route("/probe", get(|| async { "served" }))
            .layer(tap_layer(sink));

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
                Response::new(Body::from("0123456789")),
            );
            served.push(read(response.into_body()).await);
        }

        assert_eq!(served, vec![10; 8], "every response was served in full");
        assert_eq!(events.len(), 1, "the queue never grew past its bound");
    }
}
