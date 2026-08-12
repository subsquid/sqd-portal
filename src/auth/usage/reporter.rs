//! Everything slow, on a task of its own (DC-9, OB-14).
//!
//! The reporter batches what the tap queued, signs it with the identity the
//! control plane already knows from the exchange (DC-8), and posts it. It
//! retries with backoff, drops what has aged out, and flushes once, briefly, on
//! shutdown.
//!
//! Nothing here is awaited by serving code, and nothing here returns an error to
//! anyone: the only way a failure leaves this module is as a counter and a log
//! line. That is deliberate — a reporter whose failure could propagate would be
//! a dependency of the data path, which is the one thing usage measurement must
//! never become (INV-32, FM's usage-sink row).

use std::time::Duration;

use serde::Serialize;
use tokio::{sync::mpsc, time::Instant};
use tokio_util::sync::CancellationToken;
use url::Url;

use super::{config::UsageConfig, event::UsageEvent, Queued};
use crate::{
    auth::{client::endpoint_url, config::ResolvedAuth, now_secs, signing::RequestSigner},
    metrics::{UsageDrop, UsageSignals},
};

/// Beside the exchange, and mounted by the same config value: where the control
/// plane puts its portal API is its own routing (DC-8).
const USAGE_PATH: [&str; 3] = ["v1", "auth", "usage"];

/// Per-attempt deadline. Not operator-bindable: it bounds a call nothing waits
/// on, so the only thing a knob could tune is how fast a dead sink is noticed.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);

/// First retry wait, doubled per attempt up to [`MAX_BACKOFF`], spread by
/// [`JITTER`] so a fleet that lost the sink together does not come back in step
/// (the HZ-12 argument, applied to the sink).
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const JITTER: f64 = 0.25;

/// How long the final flush may run — the *whole* of it, including a post that
/// was already on the wire when the stop arrived. Shutdown is not the time to
/// wait out an outage: what does not go out in this window is dropped and
/// counted, which is the same trade the queue makes every other second of the
/// process's life.
const FINALIZE_BUDGET: Duration = Duration::from_secs(5);

/// How much of `batch_max` is allocated before a single record has arrived. The
/// batch still grows to whatever the knob allows; this only stops a large
/// `batch_max_events` from costing a reporter that never fills one.
const EAGER_BATCH_CAPACITY: usize = 1024;

pub(super) struct Reporter {
    http: reqwest::Client,
    usage_url: Url,
    signer: RequestSigner,
    batch_max: usize,
    flush_interval: Duration,
    max_retry_age: Duration,
    signals: UsageSignals,
}

/// The wire shape of one delivery. An object rather than a bare array so the
/// ingest can grow a field without a new endpoint.
#[derive(Serialize)]
struct Batch<'a> {
    events: Vec<&'a UsageEvent>,
}

/// Whether another attempt could ever succeed. The split matters: retrying a
/// batch the control plane has already refused on its content burns the queue's
/// whole budget on events that will never land.
enum Delivery {
    Retry(String),
    Rejected(String),
}

impl Reporter {
    pub(super) fn new(
        config: &ResolvedAuth,
        usage: &UsageConfig,
        signer: RequestSigner,
        signals: UsageSignals,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(DELIVERY_TIMEOUT)
                // Same reasoning as the exchange: a redirect sends signed
                // customer attribution somewhere the operator did not configure.
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            usage_url: endpoint_url(&config.control_plane_url, &USAGE_PATH)?,
            signer,
            batch_max: usage.batch_max_events.max(1),
            flush_interval: usage.flush_interval(),
            max_retry_age: usage.max_retry_age(),
            signals,
        })
    }

    pub(super) fn endpoint(&self) -> &Url {
        &self.usage_url
    }

    /// Fills a batch until it is full or the flush interval elapses, delivers
    /// it, and repeats. The stop ends the loop through the bounded final flush
    /// rather than by dropping what is in hand.
    ///
    /// `stop` is the reporter's own signal, fired by [`super::Reporting::finish`]
    /// and by nothing else — deliberately *not* the process's cancellation
    /// token. That token fires when the HTTP drain begins, up to the drain's
    /// whole length before serving actually stops, so a reporter listening to it
    /// would finalize while responses were still ending: every terminal record
    /// cut during the drain — which is most of them, a drain being exactly where
    /// long streams end — would arrive at a queue nobody was reading. `finish()`
    /// runs after the drain returns (ADR-005's second phase), which is the first
    /// moment there is nothing left to measure.
    pub(super) async fn run(self, mut events: mpsc::Receiver<Queued>, stop: CancellationToken) {
        let mut batch: Vec<Queued> = Vec::with_capacity(self.batch_max.min(EAGER_BATCH_CAPACITY));
        loop {
            let deadline = Instant::now() + self.flush_interval;
            loop {
                tokio::select! {
                    biased;
                    () = stop.cancelled() => {
                        self.finalize(&mut events, &mut batch).await;
                        return;
                    }
                    received = events.recv() => match received {
                        Some(queued) => {
                            batch.push(queued);
                            if batch.len() >= self.batch_max {
                                break;
                            }
                        }
                        // Every sink handle is gone, which on this process means
                        // the gate itself is gone.
                        None => {
                            self.finalize(&mut events, &mut batch).await;
                            return;
                        }
                    },
                    () = tokio::time::sleep_until(deadline) => break,
                }
            }
            // Read where the reader is, not where the writers are: the hot path
            // must not pay for a gauge (OB-14).
            self.signals.queue_depth.set(events.len() as i64);
            if !batch.is_empty() {
                // A stop during this returns with the batch still in hand; the
                // loop above takes its biased stop branch on the next turn and
                // finalizes it.
                self.deliver(&mut batch, &stop).await;
            }
        }
    }

    /// Delivers one batch, retrying transient failures until they succeed, the
    /// events age out, or the reporter is asked to stop. The age bound is what
    /// keeps a dead sink from turning into an unbounded retry loop — and it is
    /// checked before every attempt, so a batch that spent its life in backoff
    /// dies there rather than on the next one.
    async fn deliver(&self, batch: &mut Vec<Queued>, stop: &CancellationToken) {
        let mut backoff = INITIAL_BACKOFF;
        loop {
            self.expire(batch);
            if batch.is_empty() {
                return;
            }
            let started = Instant::now();
            // Raced, not merely awaited. A post to a sink that accepts the
            // connection and then says nothing holds a whole DELIVERY_TIMEOUT,
            // and FINALIZE_BUDGET is the budget for the *whole* stop — not for
            // whatever is left after the request already in flight gives up.
            // The batch is untouched by the race, so nothing is lost by losing
            // it: the caller finalizes what is still in hand.
            let posted = tokio::select! {
                biased;
                () = stop.cancelled() => return,
                posted = self.post(batch) => posted,
            };
            match posted {
                Ok(()) => {
                    self.signals.delivered.inc_by(batch.len() as u64);
                    self.signals
                        .delivery_seconds
                        .observe(started.elapsed().as_secs_f64());
                    batch.clear();
                    return;
                }
                Err(Delivery::Rejected(why)) => {
                    self.signals
                        .dropped_by(UsageDrop::Rejected, batch.len() as u64);
                    tracing::error!(
                        events = batch.len(),
                        reason = why,
                        "usage batch refused on its content; dropping it"
                    );
                    batch.clear();
                    return;
                }
                Err(Delivery::Retry(why)) => {
                    self.signals.flush_failures.inc();
                    // Per batch, never per event: the queue is what absorbs a
                    // sink outage, and a log line per record would just move the
                    // flood somewhere else.
                    tracing::warn!(
                        events = batch.len(),
                        reason = why,
                        backoff = ?backoff,
                        "usage batch delivery failed; retrying"
                    );
                    tokio::select! {
                        () = stop.cancelled() => return,
                        () = tokio::time::sleep(spread(backoff)) => {}
                    }
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
            }
        }
    }

    /// One bounded pass at whatever is left, off the serving path and after the
    /// listener has stopped taking new work. A response still draining past this
    /// loses its residual record — loss is acceptable and counted (D5).
    async fn finalize(&self, events: &mut mpsc::Receiver<Queued>, batch: &mut Vec<Queued>) {
        // Closed first, so what is left is a fixed set this can account for in
        // full rather than a moving one. A response still draining finds the
        // sink shut from here on and is counted at the hand-off instead.
        events.close();

        let flush = async {
            loop {
                while batch.len() < self.batch_max {
                    match events.try_recv() {
                        Ok(queued) => batch.push(queued),
                        Err(_) => break,
                    }
                }
                self.expire(batch);
                if batch.is_empty() {
                    return None;
                }
                // One attempt each: a shutdown that waits out a retry schedule
                // is a shutdown that misses its deadline.
                match self.post(batch).await {
                    Ok(()) => {
                        self.signals.delivered.inc_by(batch.len() as u64);
                        batch.clear();
                    }
                    Err(why) => return Some(why),
                }
            }
        };
        let refused = match tokio::time::timeout(FINALIZE_BUDGET, flush).await {
            Ok(outcome) => outcome,
            Err(_) => {
                tracing::warn!(
                    budget = ?FINALIZE_BUDGET,
                    "final usage flush did not finish within its budget"
                );
                None
            }
        };
        self.abandon(events, batch, refused);
    }

    /// Everything the final flush did not place, counted before it goes. Every
    /// exit from [`Self::finalize`] lands here — a refusal, a transport failure,
    /// the budget running out — and each takes both the batch still in hand and
    /// whatever the closed queue still holds, which nothing after this will ever
    /// read. DC-9's bargain is that what does not go out is dropped *and*
    /// counted; a remnant nobody counted would make the loss largest exactly
    /// where it is least visible.
    ///
    /// The reason is the cause rather than a default. A batch the control plane
    /// refused on its content is `rejected` and reads as a contract break;
    /// everything else — a transport failure, the budget, the stop arriving
    /// mid-post — is `stopped` and reads as shutdown loss. The two page
    /// different people, which is the whole point of OB-14's reason axis.
    /// Records still in the queue were never offered to the sink at all, so they
    /// are `stopped` whatever became of the batch.
    fn abandon(
        &self,
        events: &mut mpsc::Receiver<Queued>,
        batch: &mut Vec<Queued>,
        refused: Option<Delivery>,
    ) {
        let in_hand = batch.len() as u64;
        batch.clear();
        let mut unread = 0u64;
        while events.try_recv().is_ok() {
            unread += 1;
        }
        if in_hand > 0 {
            let reason = match &refused {
                Some(Delivery::Rejected(_)) => UsageDrop::Rejected,
                _ => UsageDrop::Stopped,
            };
            self.signals.dropped_by(reason, in_hand);
        }
        if unread > 0 {
            self.signals.dropped_by(UsageDrop::Stopped, unread);
        }
        if in_hand + unread > 0 {
            tracing::warn!(
                undelivered = in_hand,
                abandoned = unread,
                reason = refused.map(|why| why.to_string()).unwrap_or_default(),
                "final usage flush left records behind; dropping and counting them"
            );
        }
    }

    /// Drops what has waited longer than `P-USAGE-MAX-RETRY-AGE`, counted by
    /// reason so the loss is a number rather than a suspicion (HZ-14).
    fn expire(&self, batch: &mut Vec<Queued>) {
        let before = batch.len();
        batch.retain(|queued| queued.queued_at.elapsed() <= self.max_retry_age);
        let dropped = before - batch.len();
        if dropped > 0 {
            self.signals.dropped_by(UsageDrop::Expired, dropped as u64);
            tracing::warn!(
                events = dropped,
                max_retry_age = ?self.max_retry_age,
                "dropping usage events older than the retry-age bound"
            );
        }
    }

    async fn post(&self, batch: &[Queued]) -> Result<(), Delivery> {
        let events = Batch {
            events: batch.iter().map(|queued| &queued.event).collect(),
        };
        // Serialized once: the signature covers the bytes actually sent.
        let body = serde_json::to_vec(&events)
            .map_err(|err| Delivery::Rejected(format!("batch does not serialize: {err}")))?;
        let headers = self
            .signer
            .headers("POST", self.usage_url.path(), &body, now_secs())
            .map_err(|err| Delivery::Rejected(format!("batch cannot be signed: {err}")))?;

        let mut request = self
            .http
            .post(self.usage_url.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/json");
        for (name, value) in headers {
            request = request.header(name, value);
        }

        let response = request
            .body(body)
            .send()
            .await
            .map_err(|err| Delivery::Retry(err.to_string()))?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        // A validation refusal is about the batch and will be about it forever;
        // everything else — including the ones that look permanent, like a 401
        // during a key rotation — is worth another attempt inside the age bound.
        //
        // 413 belongs with them: it is an ingress size cap answering this
        // batch's size, so the next attempt is the same batch against the same
        // cap. Retried, it never lands and head-of-line-blocks every record
        // behind it until the whole queue ages out — a size limit turned into a
        // total outage.
        if matches!(
            status,
            reqwest::StatusCode::BAD_REQUEST
                | reqwest::StatusCode::PAYLOAD_TOO_LARGE
                | reqwest::StatusCode::UNPROCESSABLE_ENTITY
        ) {
            return Err(Delivery::Rejected(format!("control plane says {status}")));
        }
        Err(Delivery::Retry(format!("control plane says {status}")))
    }
}

impl std::fmt::Display for Delivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Retry(why) | Self::Rejected(why) => f.write_str(why),
        }
    }
}

/// Backoff, spread by up to [`JITTER`] either way.
fn spread(backoff: Duration) -> Duration {
    let span = backoff.as_secs_f64() * JITTER;
    let drawn = backoff.as_secs_f64() + rand::random_range(-span..=span);
    Duration::from_secs_f64(drawn.max(0.0))
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };

    use axum::{extract::State, http::HeaderMap, http::StatusCode, routing::post, Json, Router};
    use serde_json::Value;

    use super::*;
    use crate::auth::{signing, usage::event::Encoding, Enforcement};

    /// A control plane that records the batches it was posted and answers with
    /// whatever statuses a test queued.
    #[derive(Default)]
    struct Sink {
        batches: Mutex<Vec<Vec<Value>>>,
        signatures: Mutex<Vec<(String, String, String)>>,
        statuses: Mutex<Vec<u16>>,
        attempts: AtomicUsize,
    }

    impl Sink {
        async fn spawn(statuses: Vec<u16>) -> (Arc<Self>, ResolvedAuth) {
            let state = Arc::new(Self {
                statuses: Mutex::new(statuses),
                ..Self::default()
            });
            let app = Router::new()
                .route("/authority/v1/auth/usage", post(usage))
                .with_state(state.clone());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

            let config = ResolvedAuth {
                control_plane_url: format!("http://{addr}/authority").parse().unwrap(),
                portal_id: "portal-premium-eu".to_string(),
                key: None,
                enforcement: Enforcement::Enforce,
                limits: crate::auth::config::Limits::default(),
                usage: Some(UsageConfig::default()),
            };
            (state, config)
        }

        fn batches(&self) -> Vec<Vec<Value>> {
            self.batches.lock().unwrap().clone()
        }

        fn events(&self) -> Vec<Value> {
            self.batches().into_iter().flatten().collect()
        }

        fn ids(&self) -> Vec<String> {
            self.events()
                .iter()
                .map(|event| event["event_id"].as_str().unwrap_or_default().to_owned())
                .collect()
        }

        fn attempts(&self) -> usize {
            self.attempts.load(Ordering::SeqCst)
        }
    }

    async fn usage(
        State(sink): State<Arc<Sink>>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> StatusCode {
        let attempt = sink.attempts.fetch_add(1, Ordering::SeqCst);
        let header = |name: &str| {
            headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_owned()
        };
        sink.signatures.lock().unwrap().push((
            header(signing::PORTAL_ID_HEADER),
            header(signing::TIMESTAMP_HEADER),
            header(signing::SIGNATURE_HEADER),
        ));
        let status = sink
            .statuses
            .lock()
            .unwrap()
            .get(attempt)
            .copied()
            .unwrap_or(202);
        if StatusCode::from_u16(status).unwrap().is_success() {
            let events = body["events"].as_array().cloned().unwrap_or_default();
            sink.batches.lock().unwrap().push(events);
        }
        StatusCode::from_u16(status).unwrap()
    }

    fn reporter(config: &ResolvedAuth, usage: UsageConfig) -> Reporter {
        reporter_with(config, usage, UsageSignals::bind())
    }

    fn reporter_with(config: &ResolvedAuth, usage: UsageConfig, signals: UsageSignals) -> Reporter {
        Reporter::new(
            config,
            &usage,
            config.signer(signing::test_keypair()).unwrap(),
            signals,
        )
        .expect("the reporter should build")
    }

    /// A control plane at a port nothing listens on: every attempt fails
    /// immediately, which is the shape an outage takes for this reporter.
    fn dead_sink() -> ResolvedAuth {
        ResolvedAuth {
            control_plane_url: "http://127.0.0.1:1/authority".parse().unwrap(),
            portal_id: "portal-premium-eu".to_string(),
            key: None,
            enforcement: Enforcement::Enforce,
            limits: crate::auth::config::Limits::default(),
            usage: Some(UsageConfig::default()),
        }
    }

    /// The worse outage: the connection is accepted and the answer never comes,
    /// so a post sits there until its own deadline rather than failing at once.
    async fn stalling_sink() -> ResolvedAuth {
        let app = Router::new().route(
            "/authority/v1/auth/usage",
            post(|| async {
                tokio::time::sleep(Duration::from_secs(600)).await;
                StatusCode::ACCEPTED
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        ResolvedAuth {
            control_plane_url: format!("http://{addr}/authority").parse().unwrap(),
            portal_id: "portal-premium-eu".to_string(),
            key: None,
            enforcement: Enforcement::Enforce,
            limits: crate::auth::config::Limits::default(),
            usage: Some(UsageConfig::default()),
        }
    }

    fn event(id: &str) -> UsageEvent {
        UsageEvent {
            event_id: id.to_owned(),
            key_id: "k1".to_owned(),
            organization_id: Some("org-7".to_owned()),
            dataset: Some("ethereum-mainnet".to_owned()),
            endpoint: "/stream".to_owned(),
            encoding: Encoding::Gzip,
            wire_bytes: 1024,
            started_at: 1_800_000_000.0,
            duration_ms: 30_000,
            status: crate::auth::usage::event::Status::Completed,
        }
    }

    fn queued(id: &str) -> Queued {
        Queued {
            event: event(id),
            queued_at: Instant::now(),
        }
    }

    /// The control plane authenticates a portal by signature and stamps the
    /// portal identity from it, which is why no event carries one.
    #[tokio::test]
    async fn a_batch_arrives_whole_and_signed() {
        let (sink, config) = Sink::spawn(Vec::new()).await;
        let reporter = reporter(&config, UsageConfig::default());
        let mut batch = vec![queued("one"), queued("two")];

        reporter
            .deliver(&mut batch, &CancellationToken::new())
            .await;

        assert!(batch.is_empty(), "a delivered batch is not held");
        assert_eq!(sink.ids(), ["one", "two"]);
        assert_eq!(sink.batches().len(), 1, "one call, not one per event");
        let (portal_id, _, signature) = sink.signatures.lock().unwrap()[0].clone();
        assert_eq!(portal_id, "portal-premium-eu");
        assert!(!signature.is_empty(), "the batch must be attributable");
        // The claims travel; the portal's own identity does not.
        let event = &sink.events()[0];
        assert_eq!(event["key_id"], "k1");
        assert_eq!(event["organization_id"], "org-7");
        assert!(event.get("pod").is_none());
    }

    /// The batching claim, from the queue side: events arriving separately
    /// leave together, so the sink's call rate does not follow the request rate.
    #[tokio::test]
    async fn events_leave_in_batches_rather_than_one_call_each() {
        let (sink, config) = Sink::spawn(Vec::new()).await;
        let usage = UsageConfig {
            batch_max_events: 3,
            flush_interval_ms: 50,
            ..UsageConfig::default()
        };
        let (tx, rx) = mpsc::channel(16);
        let stop = CancellationToken::new();
        let task = tokio::spawn(reporter(&config, usage).run(rx, stop.clone()));

        for id in ["a", "b", "c", "d"] {
            tx.send(queued(id)).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
        stop.cancel();
        task.await.unwrap();

        assert_eq!(sink.ids().len(), 4, "{:?}", sink.ids());
        assert!(
            sink.batches().len() < 4,
            "each event bought its own call: {:?}",
            sink.batches()
        );
    }

    /// A transient failure is the sink being down, which is the case the queue
    /// exists for; the batch survives it.
    ///
    /// Real time, deliberately: a paused clock races the client's own deadline
    /// against the backoff and decides which fires, which is a property of the
    /// test harness rather than of the reporter. One retry at the initial
    /// backoff is what that costs.
    #[tokio::test]
    async fn a_transient_failure_is_retried_until_it_lands() {
        let (sink, config) = Sink::spawn(vec![503]).await;
        let reporter = reporter(&config, UsageConfig::default());
        let mut batch = vec![queued("survivor")];

        reporter
            .deliver(&mut batch, &CancellationToken::new())
            .await;

        assert_eq!(sink.attempts(), 2);
        assert_eq!(sink.ids(), ["survivor"]);
        assert!(batch.is_empty());
    }

    /// A batch the control plane refuses on its content will be refused
    /// identically forever, and retrying it spends the whole queue's budget on
    /// events that can never land. 413 is in the set because an ingress size cap
    /// is a refusal about this batch's size: retried, it head-of-line-blocks
    /// every record behind it until the queue ages out.
    #[tokio::test]
    async fn a_batch_refused_on_its_content_is_dropped_rather_than_retried() {
        for status in [400, 413, 422] {
            let (sink, config) = Sink::spawn(vec![status]).await;
            let reporter = reporter(&config, UsageConfig::default());
            let mut batch = vec![queued("malformed")];

            reporter
                .deliver(&mut batch, &CancellationToken::new())
                .await;

            assert_eq!(sink.attempts(), 1, "{status} must not be retried");
            assert!(batch.is_empty());
        }
    }

    /// The retry loop's bound. Without it a sink that never answers is a task
    /// that never stops trying, holding events that are long past useful — and
    /// the loss has to be a number rather than a suspicion (HZ-14).
    ///
    /// Driven against a port nothing listens on, so every attempt fails at once
    /// and only the age bound can end the loop.
    #[tokio::test(start_paused = true)]
    async fn events_past_the_retry_age_are_dropped_and_the_loop_ends() {
        let config = dead_sink();
        let usage = UsageConfig {
            max_retry_age_secs: 5,
            ..UsageConfig::default()
        };
        let signals = UsageSignals::bind();
        let reporter = reporter_with(&config, usage, signals.clone());
        let mut batch = vec![queued("too-old")];
        // The families are process-global, so only the direction is assertable.
        let dropped = signals.drops(UsageDrop::Expired);

        tokio::time::timeout(
            Duration::from_secs(600),
            reporter.deliver(&mut batch, &CancellationToken::new()),
        )
        .await
        .expect("the retry loop must be bounded by the age of what it holds");

        assert!(batch.is_empty(), "an aged-out batch is not held forever");
        assert!(
            signals.drops(UsageDrop::Expired) > dropped,
            "a dropped record must be counted"
        );
    }

    /// Shutdown takes one pass at what is queued and then gets out of the way.
    #[tokio::test]
    async fn stopping_flushes_what_is_queued_and_returns() {
        let (sink, config) = Sink::spawn(Vec::new()).await;
        let usage = UsageConfig {
            // Long enough that nothing is delivered by the interval: the flush
            // under test is the shutdown one.
            flush_interval_ms: 60_000,
            ..UsageConfig::default()
        };
        let (tx, rx) = mpsc::channel(16);
        let stop = CancellationToken::new();
        let task = tokio::spawn(reporter(&config, usage).run(rx, stop.clone()));
        tx.send(queued("last-words")).await.unwrap();
        // Let the loop take it off the channel before the stop fires.
        tokio::time::sleep(Duration::from_millis(50)).await;

        stop.cancel();
        tokio::time::timeout(FINALIZE_BUDGET * 2, task)
            .await
            .expect("the reporter must stop inside its own budget")
            .expect("the reporter task must not panic");

        assert_eq!(sink.ids(), ["last-words"]);
    }

    /// A sink that is down at shutdown must not hold the process: the flush is
    /// bounded whether or not anything is listening.
    #[tokio::test]
    async fn a_dead_sink_does_not_hold_up_shutdown() {
        let config = dead_sink();
        let (tx, rx) = mpsc::channel(16);
        let stop = CancellationToken::new();
        let task = tokio::spawn(reporter(&config, UsageConfig::default()).run(rx, stop.clone()));
        tx.send(queued("into-the-void")).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        stop.cancel();
        tokio::time::timeout(FINALIZE_BUDGET * 2, task)
            .await
            .expect("shutdown must not wait on a sink that is not answering")
            .expect("the reporter task must not panic");
    }

    /// FINALIZE_BUDGET is the budget for the *whole* stop, not for whatever is
    /// left after a request already on the wire has run out its own deadline. A
    /// sink that accepts the connection and then says nothing is the case that
    /// tells the two apart: unraced, the stop costs DELIVERY_TIMEOUT before the
    /// finalize budget even starts, overshooting the advertised bound.
    #[tokio::test]
    async fn a_stalled_post_is_raced_by_the_stop_rather_than_waited_out() {
        let config = stalling_sink().await;
        let usage = UsageConfig {
            flush_interval_ms: 10,
            ..UsageConfig::default()
        };
        let (tx, rx) = mpsc::channel(16);
        let stop = CancellationToken::new();
        let task = tokio::spawn(reporter(&config, usage).run(rx, stop.clone()));
        tx.send(queued("in-flight")).await.unwrap();
        // Long enough that the post is on the wire, waiting for an answer that
        // is not coming.
        tokio::time::sleep(Duration::from_millis(300)).await;

        let stopping = Instant::now();
        stop.cancel();
        tokio::time::timeout(DELIVERY_TIMEOUT + FINALIZE_BUDGET, task)
            .await
            .expect("the stop must not wait out the per-attempt delivery timeout")
            .expect("the reporter task must not panic");

        assert!(
            stopping.elapsed() < DELIVERY_TIMEOUT,
            "stopping took {:?}, which is the in-flight post's own deadline rather \
             than the finalize budget",
            stopping.elapsed(),
        );
    }

    /// DC-9's bargain, on the one path where it used to be broken: what does not
    /// go out is dropped *and counted*. A finalize that abandoned the queue
    /// would hide the loss exactly where it is largest — everything still in
    /// flight when the process stops.
    #[tokio::test]
    async fn what_the_final_flush_cannot_place_is_counted_rather_than_abandoned() {
        let config = dead_sink();
        let usage = UsageConfig {
            // One per batch, so the remnants stay in the queue rather than being
            // swept into the batch the failing post holds.
            batch_max_events: 1,
            ..UsageConfig::default()
        };
        let signals = UsageSignals::bind();
        let reporter = reporter_with(&config, usage, signals.clone());
        let (tx, mut events) = mpsc::channel(16);
        for id in ["queued-1", "queued-2"] {
            tx.send(queued(id)).await.unwrap();
        }
        let mut batch = vec![queued("in-hand")];
        // The families are process-global, so only a lower bound is assertable —
        // but three is more than every other test in this file can contribute.
        let stopped = signals.drops(UsageDrop::Stopped);

        reporter.finalize(&mut events, &mut batch).await;

        assert!(batch.is_empty(), "nothing is held past the finalize");
        assert!(
            events.try_recv().is_err(),
            "the queue was drained, not left holding records nobody will read"
        );
        assert!(
            signals.drops(UsageDrop::Stopped) >= stopped + 3,
            "the batch in hand and both queue remnants must each be counted"
        );
        assert!(
            tx.send(queued("too-late")).await.is_err(),
            "the queue is closed to new records once the finalize has run"
        );
    }

    /// The reason axis earns its keep only if it separates a contract break from
    /// shutdown loss: a final batch the control plane refused on its content is
    /// `rejected`, and everything the dead-sink case above loses is `stopped`.
    #[tokio::test]
    async fn a_final_batch_refused_on_its_content_is_counted_as_rejected() {
        let (_sink, config) = Sink::spawn(vec![422]).await;
        let signals = UsageSignals::bind();
        let reporter = reporter_with(&config, UsageConfig::default(), signals.clone());
        let (_tx, mut events) = mpsc::channel(16);
        let mut batch = vec![queued("malformed")];
        let rejected = signals.drops(UsageDrop::Rejected);

        reporter.finalize(&mut events, &mut batch).await;

        assert!(batch.is_empty());
        assert!(
            signals.drops(UsageDrop::Rejected) > rejected,
            "a content refusal at shutdown is still a content refusal"
        );
    }

    #[test]
    fn the_usage_endpoint_sits_beside_the_exchange() {
        let base: Url = "https://cp.example/authority".parse().unwrap();

        assert_eq!(
            endpoint_url(&base, &USAGE_PATH).unwrap().as_str(),
            "https://cp.example/authority/v1/auth/usage"
        );
    }

    /// A fleet that lost the sink together retries together unless something
    /// spreads it (the HZ-12 argument).
    #[test]
    fn backoff_is_spread_but_stays_close_to_its_nominal_wait() {
        let waits: Vec<Duration> = (0..64).map(|_| spread(Duration::from_secs(4))).collect();

        assert!(waits
            .iter()
            .all(|wait| *wait >= Duration::from_secs(3) && *wait <= Duration::from_secs(5)));
        assert!(
            waits.iter().collect::<std::collections::HashSet<_>>().len() > 1,
            "a fixed wait is not a spread one"
        );
    }
}
