//! Usage pacing (REQ-70..REQ-75, ADR-018): each response is held to the rate
//! of the grant it was admitted on, with no state shared between responses or
//! replicas.
//!
//! The control plane decides the rate; the pod tallies nothing. Admission fixes
//! everything a response gets — the rate, the read-ahead cap, the age limit and
//! the headers — and the pacer reads nothing after it: no cache lookup, no
//! exchange, no secret (DEF-16, INV-38).
//!
//! **What it costs.** No decompression. Per frame of a wrapped response: a
//! clock read, an add on the byte counter and, above one second of the rate, a
//! zero-copy split. A response held back by its rate wakes at most twice in any
//! second; one that never waits has no timer (HZ-16).

use std::{
    future::Future,
    pin::Pin,
    task::{ready, Context, Poll},
    time::Duration,
};

use axum::{
    body::{Body, Bytes},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::Response,
};
use http_body::{Body as HttpBody, Frame, SizeHint};
use tokio::time::{Instant, Sleep};

use super::{
    cache::CachedGrant,
    config::{Pacing, PacingMode},
    types::{UsageClaim, UsageState},
};
use crate::{
    endpoints::stream::DataSource,
    metrics::{self, PacingCounters},
    types::AgeLimit,
};

/// P-PACED-STREAM-MAX-AGE. No shorter than the control plane's refresh
/// interval, which it matches (REQ-75).
const MAX_PACED_STREAM_AGE: Duration = Duration::from_secs(300);

/// P-FLOOR-READ-AHEAD, in chunk slots (REQ-72).
const FLOOR_READ_AHEAD: usize = 1;

const NANOS_PER_SEC: u128 = 1_000_000_000;

const STATE_HEADER: &str = "x-sqd-usage-state";
const LIMIT_HEADER: &str = "x-sqd-usage-limit-bytes";
const USED_HEADER: &str = "x-sqd-usage-used-bytes";
const RESET_HEADER: &str = "x-sqd-usage-reset";
const FLOOR_HEADER: &str = "x-sqd-usage-floor-bytes-per-sec";
const AS_OF_HEADER: &str = "x-sqd-usage-as-of";

/// The usage headers (IB-10), added in enforce only. Every one is exposed to
/// browsers (IB-1).
pub(crate) const USAGE_HEADERS: [&str; 6] = [
    STATE_HEADER,
    LIMIT_HEADER,
    USED_HEADER,
    RESET_HEADER,
    FLOOR_HEADER,
    AS_OF_HEADER,
];

/// What a paced network stream is admitted with (REQ-72, REQ-75): a request
/// extension the stream request reads. Set only in enforce, on a grant with a
/// rate.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PacedStream {
    pub age_limit: AgeLimit,
    /// `Some` caps `buffer_size`: the grant is over its allowance.
    pub read_ahead: Option<usize>,
}

/// What admission fixed for one response.
pub(super) struct Terms {
    usage: UsageClaim,
    mode: PacingMode,
    pace_real_time: bool,
    admitted: Instant,
}

impl Terms {
    /// `None` with pacing off and on a grant whose usage claim was not read:
    /// a version-1 grant, or a bad claim read as absent (REQ-70). The caller
    /// asks only where authorization enforces (REQ-55).
    pub(super) fn at_admission(pacing: Pacing, grant: &CachedGrant) -> Option<Self> {
        if pacing.mode == PacingMode::Off {
            return None;
        }
        Some(Self {
            usage: grant.usage.clone()?,
            mode: pacing.mode,
            pace_real_time: pacing.pace_real_time,
            admitted: Instant::now(),
        })
    }

    pub(super) fn paced_stream(&self) -> Option<PacedStream> {
        if self.mode != PacingMode::Enforce || self.usage.stream_bytes_per_sec.is_none() {
            return None;
        }
        Some(PacedStream {
            age_limit: AgeLimit {
                deadline: self.admitted + MAX_PACED_STREAM_AGE,
                state: self.usage.state.as_str(),
            },
            read_ahead: (self.usage.state == UsageState::Over).then_some(FLOOR_READ_AHEAD),
        })
    }

    /// Adds the headers in enforce, and wraps a successful body admitted with a
    /// rate.
    pub(super) fn apply(self, response: Response) -> Response {
        let (mut parts, body) = response.into_parts();
        if self.mode == PacingMode::Enforce {
            insert_headers(&mut parts.headers, &self.usage);
        }
        // The stream routes stamp their source inside the gate; the routes that
        // do not are served by the network (ADR-018).
        let source =
            DataSource::of(&parts.extensions, &parts.headers).unwrap_or(DataSource::Network);
        let Some((rate, enforce)) = self.pacer(parts.status, source) else {
            return Response::from_parts(parts, body);
        };
        let counters = metrics::pacing_counters(self.usage.state.as_str(), source.as_str());
        let body = PacedBody::new(body, rate, enforce, counters);
        Response::from_parts(parts, Body::new(body))
    }

    /// The rate a response is wrapped at, and whether its waits are taken.
    /// Error bodies carry no chain data, and outer layers re-render some of
    /// them, so they are not wrapped (REQ-71). Real-time responses are paced
    /// in shadow unless `pace_real_time`.
    fn pacer(&self, status: StatusCode, source: DataSource) -> Option<(u64, bool)> {
        let rate = self.usage.stream_bytes_per_sec?;
        if !status.is_success() {
            return None;
        }
        let enforce = self.mode == PacingMode::Enforce
            && (source == DataSource::Network || self.pace_real_time);
        Some((rate, enforce))
    }
}

fn insert_headers(headers: &mut HeaderMap, usage: &UsageClaim) {
    let mut insert = |name: &'static str, value: Option<String>| {
        if let Some(value) = value.and_then(|value| HeaderValue::try_from(value).ok()) {
            headers.insert(HeaderName::from_static(name), value);
        }
    };
    insert(STATE_HEADER, Some(usage.state.as_str().to_owned()));
    insert(LIMIT_HEADER, usage.allowance_bytes.map(|b| b.to_string()));
    insert(USED_HEADER, Some(usage.used_bytes.to_string()));
    insert(RESET_HEADER, rfc3339(usage.period_end));
    insert(
        FLOOR_HEADER,
        usage.floor_bytes_per_sec.map(|b| b.to_string()),
    );
    insert(AS_OF_HEADER, usage.as_of.and_then(rfc3339));
}

fn rfc3339(unix_secs: u64) -> Option<String> {
    let secs = i64::try_from(unix_secs).ok()?;
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|at| at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
}

/// A token bucket on encoded bytes, one second of the rate deep, kept in
/// byte-nanoseconds so refills and waits are exact (INV-16).
struct Bucket {
    rate: u64,
    capacity: u128,
    level: u128,
    at: Instant,
}

impl Bucket {
    /// Starts full: every response may send one burst at once.
    fn new(rate: u64, now: Instant) -> Self {
        let capacity = u128::from(rate) * NANOS_PER_SEC;
        Self {
            rate,
            capacity,
            level: capacity,
            at: now,
        }
    }

    /// P-PACING-BURST, the largest piece a frame is released in.
    fn burst(&self) -> usize {
        usize::try_from(self.rate).unwrap_or(usize::MAX)
    }

    /// Takes `bytes` and returns zero, or takes nothing and returns how long
    /// until the bucket is full. Waiting for full rather than for enough is
    /// what bounds wakes to two a second whatever the frame sizes (REQ-71).
    fn take(&mut self, bytes: usize, now: Instant) -> Duration {
        let elapsed = now.saturating_duration_since(self.at).as_nanos();
        self.level = self
            .level
            .saturating_add(elapsed.saturating_mul(u128::from(self.rate)))
            .min(self.capacity);
        self.at = self.at.max(now);
        let needed = bytes as u128 * NANOS_PER_SEC;
        if self.level >= needed {
            self.level -= needed;
            return Duration::ZERO;
        }
        let nanos = (self.capacity - self.level).div_ceil(u128::from(self.rate));
        Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
    }
}

/// A body released no faster than its bucket allows, or, in shadow, released
/// at once while the waits it would have taken are counted (REQ-74).
struct PacedBody {
    inner: Body,
    bucket: Bucket,
    /// Shadow takes no wait and splits no frame, so the client receives
    /// exactly what pacing off serves.
    enforce: bool,
    /// The rest of the frame being released, at most a burst at a time.
    held: Bytes,
    sleep: Option<Pin<Box<Sleep>>>,
    /// How far ahead of real time the response would run had it slept: the
    /// clock shadow replays its waits on, so each counts once.
    skew: Duration,
    waited: bool,
    counters: PacingCounters,
}

impl PacedBody {
    fn new(inner: Body, rate: u64, enforce: bool, counters: PacingCounters) -> Self {
        Self {
            inner,
            bucket: Bucket::new(rate, Instant::now()),
            enforce,
            held: Bytes::new(),
            sleep: None,
            skew: Duration::ZERO,
            waited: false,
            counters,
        }
    }

    fn count_wait(&mut self, wait: Duration) {
        self.counters.seconds.inc_by(wait.as_secs_f64());
        if !self.waited {
            self.waited = true;
            self.counters.responses.inc();
        }
    }

    /// The sleeps enforce would take on a frame of `len`, on the clock it
    /// would have kept.
    fn shadow(&mut self, mut len: usize) {
        let burst = self.bucket.burst();
        while len > 0 {
            let piece = len.min(burst);
            let mut now = Instant::now() + self.skew;
            loop {
                let wait = self.bucket.take(piece, now);
                if wait.is_zero() {
                    break;
                }
                self.count_wait(wait);
                self.skew += wait;
                now += wait;
            }
            len -= piece;
        }
    }
}

impl HttpBody for PacedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        let this = &mut *self;
        loop {
            if let Some(sleep) = this.sleep.as_mut() {
                ready!(sleep.as_mut().poll(cx));
                this.sleep = None;
            }
            if this.held.is_empty() {
                let frame = match ready!(Pin::new(&mut this.inner).poll_frame(cx)) {
                    Some(Ok(frame)) => frame,
                    other => return Poll::Ready(other),
                };
                let data = match frame.into_data() {
                    Ok(data) => data,
                    Err(frame) => return Poll::Ready(Some(Ok(frame))),
                };
                if !this.enforce {
                    this.shadow(data.len());
                    this.counters.bytes.inc_by(data.len() as u64);
                    return Poll::Ready(Some(Ok(Frame::data(data))));
                }
                if data.is_empty() {
                    return Poll::Ready(Some(Ok(Frame::data(data))));
                }
                this.held = data;
            }
            let piece = this.held.len().min(this.bucket.burst());
            let wait = this.bucket.take(piece, Instant::now());
            if wait.is_zero() {
                this.counters.bytes.inc_by(piece as u64);
                return Poll::Ready(Some(Ok(Frame::data(this.held.split_to(piece)))));
            }
            this.count_wait(wait);
            // Taken again on waking, at the real clock: a late timer costs the
            // stream its lateness rather than bending the bound.
            this.sleep = Some(Box::pin(tokio::time::sleep(wait)));
        }
    }

    /// Ends only once nothing is held, so the transport cannot drop a piece.
    fn is_end_stream(&self) -> bool {
        self.held.is_empty() && self.inner.is_end_stream()
    }

    /// Counts the held bytes, so a length-delimited body keeps its framing.
    fn size_hint(&self) -> SizeHint {
        let inner = self.inner.size_hint();
        let held = self.held.len() as u64;
        let mut hint = SizeHint::new();
        hint.set_lower(inner.lower() + held);
        if let Some(upper) = inner.upper() {
            hint.set_upper(upper + held);
        }
        hint
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;

    use axum::{routing::get, Router};

    use super::*;
    use crate::auth::types::MIN_PACING_RATE;

    const RATE: u64 = MIN_PACING_RATE;
    const BURST: usize = RATE as usize;

    fn counters() -> PacingCounters {
        PacingCounters {
            seconds: Default::default(),
            responses: Default::default(),
            bytes: Default::default(),
        }
    }

    /// A body whose frames are all ready at once, so only the pacer holds it.
    fn body_of(frames: &[usize]) -> Body {
        let frames: Vec<Result<Bytes, std::io::Error>> = frames
            .iter()
            .map(|len| Ok(Bytes::from(vec![7u8; *len])))
            .collect();
        Body::from_stream(futures::stream::iter(frames))
    }

    /// Each released frame, with when it left.
    async fn run(frames: &[usize], enforce: bool) -> (Vec<(Duration, usize)>, PacingCounters) {
        let counters = counters();
        let mut body = PacedBody::new(body_of(frames), RATE, enforce, counters.clone());
        let start = Instant::now();
        let mut released = Vec::new();
        while let Some(frame) = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
            let data = frame.unwrap().into_data().unwrap();
            released.push((start.elapsed(), data.len()));
        }
        (released, counters)
    }

    /// Many small frames; one frame ten bursts long; small frames then a huge
    /// final one — the zstd case, where a frame is a whole worker result.
    fn shapes() -> Vec<(&'static str, Vec<usize>)> {
        let mut tail = vec![1_000; 20];
        tail.push(5 * BURST);
        vec![
            ("many small frames", vec![4_000; 100]),
            ("one huge frame", vec![10 * BURST]),
            ("a huge final frame", tail),
        ]
    }

    /// INV-16: over every interval the bytes released are at most a burst plus
    /// the rate times its length, through the last byte, and all of them go out.
    #[tokio::test(start_paused = true)]
    async fn the_byte_curve_stays_within_the_bound() {
        for (shape, frames) in shapes() {
            let (released, counters) = run(&frames, true).await;
            let total: usize = frames.iter().sum();

            assert_eq!(
                released.iter().map(|(_, len)| len).sum::<usize>(),
                total,
                "{shape}"
            );
            assert_eq!(counters.bytes.get(), total as u64, "{shape}");
            assert!(
                released.iter().all(|(_, len)| *len <= BURST),
                "{shape}: a piece over a burst"
            );
            for i in 0..released.len() {
                let mut sent = 0u128;
                for j in i..released.len() {
                    sent += released[j].1 as u128;
                    let span = (released[j].0 - released[i].0).as_nanos();
                    assert!(
                        sent * NANOS_PER_SEC <= RATE as u128 * (NANOS_PER_SEC + span),
                        "{shape}: {sent} bytes in {span} ns"
                    );
                }
            }
            // Paced, not just bounded: done within a second of the rate's pace.
            let end = released.last().unwrap().0;
            assert!(
                end <= Duration::from_secs_f64(total as f64 / RATE as f64),
                "{shape}: {end:?}"
            );
            assert_eq!(counters.responses.get(), 1, "{shape}");
            // The timer is late by at most its millisecond resolution per wake,
            // and the stream loses that lateness rather than recovering it.
            let mut wakes: Vec<_> = released.iter().map(|(at, _)| *at).collect();
            wakes.dedup();
            let late = end.as_secs_f64() - counters.seconds.get();
            assert!(
                late >= 0.0 && late <= wakes.len() as f64 * 1e-3,
                "{shape}: {late}"
            );
        }
    }

    /// REQ-71: a response held back by its rate wakes at most twice in the time
    /// its rate takes to fill a burst, whatever its frame sizes.
    #[tokio::test(start_paused = true)]
    async fn a_held_response_wakes_at_most_twice_a_second() {
        let mut mixed = Vec::new();
        for i in 0..40 {
            mixed.push(if i % 2 == 0 { 10 } else { BURST });
        }
        for (shape, frames) in shapes().into_iter().chain([("alternating", mixed)]) {
            let (released, _) = run(&frames, true).await;
            let mut wakes: Vec<Duration> = released.iter().map(|(at, _)| *at).collect();
            wakes.dedup();
            wakes.retain(|at| !at.is_zero());
            for pair in wakes.windows(3) {
                assert!(
                    pair[2] - pair[0] > Duration::from_secs(1),
                    "{shape}: {pair:?}"
                );
            }
        }
    }

    /// REQ-74: shadow releases every frame as it comes, unsplit, and counts
    /// the sleeps enforce takes on the same frames, each once.
    #[tokio::test(start_paused = true)]
    async fn shadow_counts_what_enforce_sleeps() {
        for (shape, frames) in shapes() {
            let (enforced, enforce) = run(&frames, true).await;
            let (shadowed, shadow) = run(&frames, false).await;

            let expected: Vec<_> = frames.iter().map(|len| (Duration::ZERO, *len)).collect();
            assert_eq!(
                shadowed, expected,
                "{shape}: shadow must serve what off serves"
            );
            assert!(
                enforced.last().unwrap().0 > Duration::ZERO,
                "{shape}: enforce slept"
            );
            assert_eq!(shadow.seconds.get(), enforce.seconds.get(), "{shape}");
            assert_eq!(shadow.responses.get(), enforce.responses.get(), "{shape}");
            assert_eq!(shadow.bytes.get(), enforce.bytes.get(), "{shape}");
        }
    }

    /// A response within its burst never waits and has no timer.
    #[tokio::test(start_paused = true)]
    async fn a_response_within_its_burst_never_waits() {
        let (released, counters) = run(&[BURST / 2, BURST / 2], true).await;
        assert!(released.iter().all(|(at, _)| at.is_zero()));
        assert_eq!(counters.responses.get(), 0);
        assert_eq!(counters.seconds.get(), 0.0);
    }

    /// OB-16: a response dropped mid-frame, at a piece or waiting for the next,
    /// counts the bytes it sent, not the ones still held.
    #[tokio::test(start_paused = true)]
    async fn a_dropped_response_counts_only_what_it_sent() {
        let counters = counters();
        let mut body = PacedBody::new(body_of(&[10 * BURST]), RATE, true, counters.clone());
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());

        let first = Pin::new(&mut body).poll_frame(&mut cx);
        assert!(matches!(first, Poll::Ready(Some(Ok(_)))));
        assert_eq!(counters.bytes.get(), BURST as u64);

        assert!(Pin::new(&mut body).poll_frame(&mut cx).is_pending());
        drop(body);
        assert_eq!(counters.bytes.get(), BURST as u64);
    }

    /// A single buffered body keeps its exact length through hyper, which stops
    /// polling once the length is written: the held pieces count in the hint,
    /// and the body does not end while one is held (CT-12).
    #[tokio::test]
    async fn a_buffered_body_arrives_whole_under_its_exact_length() {
        let len = 2 * BURST + 1_000;
        let body = PacedBody::new(Body::from(vec![7u8; len]), RATE, true, counters());
        assert_eq!(body.size_hint().exact(), Some(len as u64));

        let app = Router::new().route(
            "/",
            get(move || async move {
                Body::new(PacedBody::new(
                    Body::from(vec![7u8; len]),
                    RATE,
                    true,
                    counters(),
                ))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let response = reqwest::get(format!("http://{addr}/")).await.unwrap();
        assert_eq!(response.content_length(), Some(len as u64));
        assert_eq!(response.bytes().await.unwrap().len(), len);
    }

    /// HZ-16's budget, for CT-6 to measure by hand under `/usr/bin/time`:
    /// P-MAX-STREAMS responses of whole-worker-result frames, each paced for
    /// about ten seconds at `PACING_RATE`. `PACING_BENCH=shadow` and `off`
    /// drain the same bytes unpaced, as log_only and an unwrapped body do.
    /// `PACING_FRAME` and `PACING_FRAMES` (per stream) override the shape, for
    /// the per-frame cost of a wrapper that never waits.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "CT-6 cost check, run by hand"]
    async fn ct6_pacing_cost() {
        const STREAMS: usize = 1024;
        const SECS: u64 = 10;
        let var = |name: &str| std::env::var(name).ok().map(|v| v.parse::<u64>().unwrap());
        let mode = std::env::var("PACING_BENCH").unwrap_or_else(|_| "enforce".into());
        let rate = var("PACING_RATE").unwrap_or(500_000);
        let frame_len = var("PACING_FRAME").unwrap_or(4_000_000) as usize;
        let frames =
            var("PACING_FRAMES").unwrap_or((rate * SECS).div_ceil(frame_len as u64)) as usize;
        let frame = Bytes::from(vec![7u8; frame_len]);
        // One set for every stream, as the families are in production.
        let shared = counters();

        let started = std::time::Instant::now();
        let tasks: Vec<_> = (0..STREAMS)
            .map(|_| {
                let source = futures::stream::iter(
                    std::iter::repeat_n(frame.clone(), frames).map(Ok::<_, std::io::Error>),
                );
                let mut body = match mode.as_str() {
                    "off" => Body::from_stream(source),
                    shadow => Body::new(PacedBody::new(
                        Body::from_stream(source),
                        rate,
                        shadow != "shadow",
                        shared.clone(),
                    )),
                };
                tokio::spawn(async move {
                    let mut released = 0usize;
                    while let Some(frame) = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
                        frame.unwrap();
                        released += 1;
                    }
                    released
                })
            })
            .collect();
        let mut released = 0;
        for task in tasks {
            released += task.await.unwrap();
        }
        println!(
            "mode={mode} rate={rate} streams={STREAMS} bytes={} frames_in={} frames_out={released} wall={:.2}s",
            STREAMS * frames * frame_len,
            STREAMS * frames,
            started.elapsed().as_secs_f64(),
        );
    }

    fn usage(state: UsageState, rate: Option<u64>) -> UsageClaim {
        UsageClaim {
            state,
            stream_bytes_per_sec: rate,
            floor_bytes_per_sec: Some(500_000),
            allowance_bytes: Some(400_000_000_000),
            used_bytes: 7,
            period_end: 1_800_000_000,
            as_of: Some(1_799_999_000),
        }
    }

    fn terms(mode: PacingMode, pace_real_time: bool, usage: UsageClaim) -> Terms {
        Terms {
            usage,
            mode,
            pace_real_time,
            admitted: Instant::now(),
        }
    }

    /// Only successful responses with a rate are wrapped; real-time ones are
    /// shadowed unless `pace_real_time`; log_only shadows everything.
    #[test]
    fn which_responses_are_paced_and_how() {
        use DataSource::{Network, RealTime};
        use PacingMode::{Enforce, LogOnly};
        let ok = StatusCode::OK;
        let rate = Some(1_000_000);
        let cases = [
            (Enforce, false, rate, ok, Network, Some(true)),
            (Enforce, false, rate, ok, RealTime, Some(false)),
            (Enforce, true, rate, ok, RealTime, Some(true)),
            (LogOnly, true, rate, ok, Network, Some(false)),
            (
                Enforce,
                false,
                rate,
                StatusCode::SERVICE_UNAVAILABLE,
                Network,
                None,
            ),
            (
                Enforce,
                false,
                rate,
                StatusCode::NO_CONTENT,
                Network,
                Some(true),
            ),
            (Enforce, false, None, ok, Network, None),
        ];
        for (mode, real_time, rate, status, source, enforce) in cases {
            let terms = terms(mode, real_time, usage(UsageState::Within, rate));
            assert_eq!(
                terms.pacer(status, source).map(|(_, enforce)| enforce),
                enforce,
                "{mode:?} pace_real_time={real_time} rate={rate:?} {status} {source:?}"
            );
        }
    }

    /// REQ-72, REQ-75: enforce with a rate gets the age limit, and the floor
    /// read-ahead when over; nothing else gets either.
    #[test]
    fn a_paced_stream_is_limited_at_admission() {
        let over = terms(
            PacingMode::Enforce,
            false,
            usage(UsageState::Over, Some(RATE)),
        );
        let paced = over.paced_stream().unwrap();
        assert_eq!(paced.read_ahead, Some(1));
        assert_eq!(paced.age_limit.state, "over");
        assert_eq!(
            paced.age_limit.deadline,
            over.admitted + Duration::from_secs(300)
        );

        let within = terms(
            PacingMode::Enforce,
            false,
            usage(UsageState::Within, Some(RATE)),
        );
        assert_eq!(within.paced_stream().unwrap().read_ahead, None);

        for unlimited in [
            terms(
                PacingMode::LogOnly,
                false,
                usage(UsageState::Over, Some(RATE)),
            ),
            terms(
                PacingMode::Enforce,
                false,
                usage(UsageState::Unmetered, None),
            ),
        ] {
            assert!(unlimited.paced_stream().is_none());
        }
    }

    /// IB-10: in enforce, every header with a value, from the grant; none in
    /// log_only.
    #[test]
    fn the_usage_headers_are_the_grant_s_in_enforce_only() {
        let headers = |terms: Terms| {
            let response = terms.apply(Response::new(Body::empty()));
            let mut headers: Vec<_> = response
                .headers()
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_str().unwrap().to_owned()))
                .collect();
            headers.sort();
            headers
        };
        let pair = |name: &str, value: &str| (name.to_owned(), value.to_owned());

        assert_eq!(
            headers(terms(
                PacingMode::Enforce,
                false,
                usage(UsageState::Over, Some(RATE))
            )),
            [
                pair(AS_OF_HEADER, "2027-01-15T07:43:20Z"),
                pair(FLOOR_HEADER, "500000"),
                pair(LIMIT_HEADER, "400000000000"),
                pair(RESET_HEADER, "2027-01-15T08:00:00Z"),
                pair(STATE_HEADER, "over"),
                pair(USED_HEADER, "7"),
            ]
        );
        let unmetered = UsageClaim {
            floor_bytes_per_sec: None,
            allowance_bytes: None,
            as_of: None,
            ..usage(UsageState::Unmetered, None)
        };
        assert_eq!(
            headers(terms(PacingMode::Enforce, false, unmetered)),
            [
                pair(RESET_HEADER, "2027-01-15T08:00:00Z"),
                pair(STATE_HEADER, "unmetered"),
                pair(USED_HEADER, "7"),
            ]
        );
        assert!(headers(terms(
            PacingMode::LogOnly,
            false,
            usage(UsageState::Over, Some(RATE))
        ))
        .is_empty());
    }
}
