//! The only authorization state a replica holds: grants it was handed for
//! credentials it has actually served (DEF-18).
//!
//! Keyed on a fingerprint of the *whole* credential, never on the key id: an
//! entry reachable by id alone would admit the next caller to name that id
//! without proving it holds the secret.

use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use lru::LruCache;
use tokio::sync::Semaphore;

use super::{
    client::{self, ControlPlaneClient, Exchanged},
    config::{Enforcement, Limits},
    extractor::Credential,
    now_secs,
    singleflight::KeyedLocks,
};
use crate::metrics::{self, ExchangeOutcome};

/// A grant as held, after the portal's own cap has been applied to what the
/// control plane offered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedGrant {
    pub key_id: String,
    pub datasets: Option<Vec<String>>,
    /// Carried for attribution only; nothing in the ladder reads it (REQ-60).
    pub organization_id: Option<String>,
    /// A request arriving past this is still served.
    pub refresh_after: u64,
    /// The hard expiry: past it the grant answers only while the control
    /// plane is failing to answer, and only for the outage grace on top
    /// (REQ-54). While the control plane answers, this is the bound on stale
    /// authorization.
    pub expires_at: u64,
}

/// What the scrape publishes about the cliff: how many grants are serving past
/// `refresh_after`, how many past `expires_at`, and the least life left before
/// the first of them is dropped (zero when none are).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraceCensus {
    pub in_grace: usize,
    pub stale: usize,
    pub min_remaining: u64,
}

/// What resolving a credential established. Only the first two are the control
/// plane speaking; the last two are the portal failing to ask, and reporting
/// them as a verdict would tell the holder of a valid key to stop retrying
/// (REQ-54).
#[derive(Debug, Clone)]
pub enum Resolved {
    Grant(Arc<CachedGrant>),
    Denied(String),
    /// The local exchange budget is spent — rate or in-flight cap.
    Saturated,
    /// The exchange itself failed, timed out, or could not be read.
    Unavailable,
}

struct Denial {
    reason: String,
    until: u64,
}

/// An attempt that established nothing about the credential. Held so a failing
/// control plane is not paid for once per waiter and once per request.
struct Failure {
    resolved: Resolved,
    /// A waiter that queued before this is answered by it — its own call could
    /// not come back fresher.
    completed_at: Instant,
    retry_after: Instant,
    /// Wall-clock second the exchange failed to answer at all (DC-8 outage,
    /// not an unusable answer or a spent budget). The only evidence that lets
    /// this credential's grant answer past `expires_at` (REQ-54); it is per
    /// credential because attempts on one fingerprint are serialized, so the
    /// record is always its latest word.
    outage_at: Option<u64>,
}

pub struct GrantCache {
    client: ControlPlaneClient,
    limits: Limits,
    grants: Mutex<LruCache<String, Arc<CachedGrant>>>,
    /// Capped apart from the grants: the fingerprint is attacker-chosen, so a
    /// flood must not evict what a paying key is served on (HZ-10).
    denials: Mutex<LruCache<String, Denial>>,
    /// Capped for the same reason, and kept apart from the denials: both are
    /// filled at the attacker's rate, so sharing one bound would let a flood of
    /// either evict the other — and a remembered denial is what keeps a revoked
    /// key from spending an exchange per request.
    failures: Mutex<LruCache<String, Failure>>,
    /// One exchange in flight per fingerprint. Without it a burst on one
    /// uncached credential is one control-plane call per request.
    inflight: KeyedLocks,
    permits: Semaphore,
    limiter: Mutex<RateLimiter>,
    /// Wall-clock second of the last exchange the control plane answered, or 0
    /// for a replica it has never answered at all. The gauge derived from it
    /// climbs through an outage, which is the operator's distance to the
    /// `expires_at` cliff (OB-9, OB-13).
    last_success: AtomicU64,
    /// Falls back for the gauge while `last_success` is still 0, so a replica
    /// that has never been served counts up from boot instead of reading as
    /// freshly successful.
    started_at: u64,
    /// Shadow mode publishes none of the OB-13 signals: both answers serve
    /// there, so a counter moving is the verdict the response withheld (OB-12).
    enforcement: Enforcement,
}

impl GrantCache {
    pub fn new(client: ControlPlaneClient, limits: Limits, enforcement: Enforcement) -> Arc<Self> {
        let cache = Arc::new(Self {
            client,
            enforcement,
            grants: Mutex::new(LruCache::new(nonzero(limits.grant_cache_capacity))),
            denials: Mutex::new(LruCache::new(nonzero(limits.denial_cache_capacity))),
            failures: Mutex::new(LruCache::new(nonzero(limits.denial_cache_capacity))),
            inflight: KeyedLocks::default(),
            permits: Semaphore::new(limits.max_inflight_exchanges),
            limiter: Mutex::new(RateLimiter::new(limits.exchange_rate_per_sec)),
            last_success: AtomicU64::new(0),
            started_at: now_secs(),
            limits,
        });
        cache.publish_gauges();
        cache
    }

    /// Answers from the cache where it can, and asks the control plane where it
    /// cannot. A grant past `refresh_after` still answers while its renewal
    /// runs; only a credential with nothing usable waits for an exchange.
    pub async fn resolve(self: &Arc<Self>, credential: &Credential, now: u64) -> Resolved {
        if let Some(grant) = self.held_grant(&credential.fingerprint, now) {
            if grant.refresh_after > now {
                return Resolved::Grant(grant);
            }
            if grant.expires_at > now {
                // Serving on a grant whose renewal has not landed is the
                // renewal grace, and the first warning an operator gets.
                self.report(metrics::report_grace_admission);
                self.spawn_refresh(credential, now);
                return Resolved::Grant(grant);
            }
            // Past the hard expiry the grant answers only while the authority
            // is failing to answer this credential, witnessed since the grant
            // expired. Without that the request revalidates below.
            if self.outage_since(&credential.fingerprint, grant.expires_at) {
                self.report(metrics::report_stale_admission);
                self.spawn_refresh(credential, now);
                return Resolved::Grant(grant);
            }
        }
        if let Some(reason) = self.live_denial(&credential.fingerprint, now) {
            return Resolved::Denied(reason);
        }

        // Before queueing: it separates an answer produced on our behalf from
        // one that predates us.
        let arrived = Instant::now();
        let _held = self.inflight.acquire(&credential.fingerprint).await;
        self.resolve_exclusively(credential, now, arrived).await
    }

    /// Runs under the fingerprint's lock. Whoever held it before has answered by
    /// now, so this re-checks before paying for a second call: that is what
    /// makes a burst on one credential cost one exchange (INV-14).
    async fn resolve_exclusively(
        &self,
        credential: &Credential,
        now: u64,
        arrived: Instant,
    ) -> Resolved {
        let held = self.held_grant(&credential.fingerprint, now);
        if let Some(grant) = &held {
            if grant.refresh_after > now {
                return Resolved::Grant(grant.clone());
            }
        }
        if let Some(reason) = self.live_denial(&credential.fingerprint, now) {
            return Resolved::Denied(reason);
        }
        // A failure answers the queue too; without it a burst against a failing
        // control plane costs one full timeout per request, serially.
        if let Some(resolved) = self.failure_since(&credential.fingerprint, arrived) {
            return self.or_grace(
                &credential.fingerprint,
                resolved,
                held,
                latest_second_reached(now, arrived.elapsed()),
            );
        }
        let resolved = self.exchange(credential, now).await;
        self.or_grace(
            &credential.fingerprint,
            resolved,
            held,
            latest_second_reached(now, arrived.elapsed()),
        )
    }

    /// An exchange that established nothing must not discard a grant still
    /// held, or the grace would depend on which side of the lock the request
    /// arrived (REQ-54). `now` is the admission second, not the arrival: the
    /// wait plus a failed exchange can outlast what the fast path saw.
    fn or_grace(
        &self,
        fingerprint: &str,
        resolved: Resolved,
        held: Option<Arc<CachedGrant>>,
        now: u64,
    ) -> Resolved {
        match (resolved, held) {
            (Resolved::Saturated | Resolved::Unavailable, Some(grant))
                if grant.expires_at > now =>
            {
                self.report(metrics::report_grace_admission);
                Resolved::Grant(grant)
            }
            // Past the expiry only an outage of this credential's exchange
            // serves: a spent budget never asked, and an unusable answer is
            // the authority speaking.
            (Resolved::Unavailable, Some(grant))
                if self.discard_at(&grant) > now
                    && self.outage_since(fingerprint, grant.expires_at) =>
            {
                self.report(metrics::report_stale_admission);
                Resolved::Grant(grant)
            }
            (resolved, _) => resolved,
        }
    }

    /// When a grant is dropped rather than held for an outage: its hard expiry
    /// plus the outage grace. Zero grace makes this the hard expiry itself.
    fn discard_at(&self, grant: &CachedGrant) -> u64 {
        grant
            .expires_at
            .saturating_add(self.limits.outage_grace_secs)
    }

    /// Whether this credential's latest exchange failed to answer at or after
    /// `since`. A failure older than `since` says nothing about the present.
    fn outage_since(&self, fingerprint: &str, since: u64) -> bool {
        self.failures
            .lock()
            .unwrap()
            .peek(fingerprint)
            .and_then(|failure| failure.outage_at)
            .is_some_and(|at| at >= since)
    }

    /// The renewal a request past `refresh_after` triggers without waiting for
    /// it. Skipped outright when one is already running for this fingerprint.
    fn spawn_refresh(self: &Arc<Self>, credential: &Credential, now: u64) {
        // `refresh_after` cannot advance while exchanges fail, so without a
        // cooldown the whole data-plane rate lands on a control plane already
        // down.
        if self.cooling_down(&credential.fingerprint) {
            return;
        }
        // Cloned only past the cooldown: in an outage every grace-served
        // request lands here and returns above.
        let credential = credential.clone();
        let cache = self.clone();
        tokio::spawn(async move {
            let Some(_held) = cache.inflight.try_acquire(&credential.fingerprint) else {
                return;
            };
            // A refresh may have landed while this task waited for the lock.
            if cache
                .held_grant(&credential.fingerprint, now)
                .is_some_and(|grant| grant.refresh_after > now)
            {
                return;
            }
            // …or it may have come back a denial, which pops the grant, so the
            // check above no longer sees anything. Every task queued behind
            // that one would re-ask about a credential the authority has just
            // refused, at the cost of the fleet-shared budget (HZ-10).
            if cache.live_denial(&credential.fingerprint, now).is_some()
                || cache.cooling_down(&credential.fingerprint)
            {
                return;
            }
            cache.exchange(&credential, now).await;
        });
    }

    /// One call to the authority, under the budgets that keep an unauthenticated
    /// flood from becoming a cost amplifier pointed at it (HZ-10).
    async fn exchange(&self, credential: &Credential, now: u64) -> Resolved {
        let Ok(_permit) = self.permits.try_acquire() else {
            self.report(|| metrics::report_exchange(ExchangeOutcome::Saturated, None));
            tracing::warn!(
                key_id = credential.key_id,
                outcome = "over_inflight_cap",
                "credential exchange skipped: too many in flight"
            );
            return self.remember_failure(&credential.fingerprint, Resolved::Saturated);
        };
        if !self.limiter.lock().unwrap().take() {
            self.report(|| metrics::report_exchange(ExchangeOutcome::Saturated, None));
            tracing::warn!(
                key_id = credential.key_id,
                outcome = "rate_limited",
                "credential exchange skipped: rate limit"
            );
            return self.remember_failure(&credential.fingerprint, Resolved::Saturated);
        }

        let started = Instant::now();
        let answer = self.client.exchange(credential, now).await;
        let elapsed = started.elapsed();
        // Judging the answer by the request's start would admit a grant that
        // expired while it was in flight.
        let settled = second_reached(now, elapsed);
        let expired_by = latest_second_reached(now, elapsed);

        match answer {
            Ok(Exchanged::Granted(grant)) => {
                // Established nothing, so it is a failed exchange like any
                // other unusable answer (DC-8). Tested before anything is
                // published: counting it answered would refresh the OB-9
                // freshness gauge.
                if grant.expires_at <= expired_by {
                    self.report(|| {
                        metrics::report_exchange(ExchangeOutcome::Failed, Some(elapsed))
                    });
                    tracing::warn!(
                        key_id = grant.key_id,
                        "credential exchange returned an already-expired grant"
                    );
                    return self.remember_failure(&credential.fingerprint, Resolved::Unavailable);
                }
                self.last_success.store(settled, Ordering::Release);
                self.report(|| metrics::report_exchange(ExchangeOutcome::Answered, Some(elapsed)));
                let cached = self.store(&credential.fingerprint, grant, settled);
                Resolved::Grant(cached)
            }
            Ok(Exchanged::Denied(reason)) => {
                self.last_success.store(settled, Ordering::Release);
                self.report(|| metrics::report_exchange(ExchangeOutcome::Answered, Some(elapsed)));
                // A denial outranks the grant's remaining lifetime: the
                // authority has spoken since (INV-6).
                self.grants.lock().unwrap().pop(&credential.fingerprint);
                self.remember_denial(&credential.fingerprint, reason.clone(), settled);
                self.publish_gauges();
                Resolved::Denied(reason)
            }
            Err(err) => {
                self.report(|| metrics::report_exchange(ExchangeOutcome::Failed, Some(elapsed)));
                tracing::warn!(
                    key_id = credential.key_id,
                    outcome = "failed",
                    error = %err,
                    "credential exchange failed"
                );
                let outage_at = client::is_outage(&err).then_some(settled);
                self.remember(&credential.fingerprint, Resolved::Unavailable, outage_at)
            }
        }
    }

    /// Applies the portal's ceiling to the offered lifetime, so an upstream
    /// misconfiguration cannot hand the fleet a month-long grant (REQ-54).
    fn store(&self, fingerprint: &str, grant: super::types::Grant, now: u64) -> Arc<CachedGrant> {
        let ceiling = now.saturating_add(self.limits.max_grant_lifetime_secs);
        let expires_at = grant.expires_at.min(ceiling);
        if grant.expires_at > ceiling {
            self.report(metrics::report_lifetime_capped);
            tracing::warn!(
                key_id = grant.key_id,
                offered = grant.expires_at.saturating_sub(now),
                cap = self.limits.max_grant_lifetime_secs,
                "grant lifetime capped"
            );
        }
        // Clamped before the jitter is sized, so spreading a cohort's renewals
        // cannot extend how long one serves unrenewed (HZ-12). Floored after
        // the jitter: a grant landing already due would cache nothing.
        let renew_at = grant.refresh_after.min(expires_at);
        let refresh_after = renew_at
            .saturating_sub(self.jitter(renew_at.saturating_sub(now)))
            .max(now.saturating_add(1));

        let cached = Arc::new(CachedGrant {
            key_id: grant.key_id,
            datasets: grant.datasets,
            organization_id: grant.organization_id,
            refresh_after,
            expires_at,
        });
        let evicted = self
            .grants
            .lock()
            .unwrap()
            .push(fingerprint.to_owned(), cached.clone());
        if evicted.is_some_and(|(key, _)| key != fingerprint) {
            self.report(metrics::report_grant_eviction);
        }
        // A grant supersedes whatever the credential earned earlier.
        self.denials.lock().unwrap().pop(fingerprint);
        self.failures.lock().unwrap().pop(fingerprint);
        self.publish_gauges();
        cached
    }

    fn jitter(&self, window_secs: u64) -> u64 {
        let span = window_secs.saturating_mul(self.limits.refresh_jitter_pct) / 100;
        if span == 0 {
            return 0;
        }
        rand::random_range(0..=span)
    }

    fn remember_denial(&self, fingerprint: &str, reason: String, now: u64) {
        self.denials.lock().unwrap().put(
            fingerprint.to_owned(),
            Denial {
                reason,
                until: now.saturating_add(self.limits.denial_ttl_secs),
            },
        );
    }

    fn remember_failure(&self, fingerprint: &str, resolved: Resolved) -> Resolved {
        self.remember(fingerprint, resolved, None)
    }

    fn remember(&self, fingerprint: &str, resolved: Resolved, outage_at: Option<u64>) -> Resolved {
        let completed_at = Instant::now();
        self.failures.lock().unwrap().put(
            fingerprint.to_owned(),
            Failure {
                resolved: resolved.clone(),
                completed_at,
                // Retrying faster than one call takes cannot learn anything.
                retry_after: completed_at + self.limits.exchange_timeout(),
                outage_at,
            },
        );
        resolved
    }

    /// The outcome of an attempt that finished after `arrived`, if there was
    /// one — the answer this caller queued for, already paid.
    fn failure_since(&self, fingerprint: &str, arrived: Instant) -> Option<Resolved> {
        let mut failures = self.failures.lock().unwrap();
        let failure = failures.get(fingerprint)?;
        (failure.completed_at >= arrived).then(|| failure.resolved.clone())
    }

    /// Reads without removing: the record is also the credential's outage
    /// evidence, and dropping it with the cooldown would send every request
    /// past the expiry back to a synchronous exchange. A later attempt
    /// overwrites it and a grant clears it.
    fn cooling_down(&self, fingerprint: &str) -> bool {
        self.failures
            .lock()
            .unwrap()
            .peek(fingerprint)
            .is_some_and(|failure| failure.retry_after > Instant::now())
    }

    /// A grant not yet discarded: inside its hard expiry, or past it and held
    /// for the outage grace — whether it may answer past `expires_at` is the
    /// caller's to decide. Past the discard point it is dropped: it can never
    /// be served on again.
    fn held_grant(&self, fingerprint: &str, now: u64) -> Option<Arc<CachedGrant>> {
        let mut grants = self.grants.lock().unwrap();
        let grant = grants.get(fingerprint)?;
        if self.discard_at(grant) > now {
            return Some(grant.clone());
        }
        grants.pop(fingerprint);
        // Also republished here: in an outage nothing is stored, and occupancy
        // would sit at its pre-outage peak while the cache drains.
        let entries = grants.len();
        drop(grants);
        self.report(|| metrics::report_grant_cache_size(entries));
        None
    }

    /// How many grants are past `refresh_after` but inside `expires_at`, how
    /// many are past `expires_at` and held for the outage, and the least life
    /// left among all of them before the first is dropped (zero when none
    /// are). The admission rates say the condition exists; the minimum says
    /// when the first hard refusal lands if the outage persists (OB-9, OB-13).
    /// One walk per scrape, bounded by the capacity.
    pub fn grace_census(&self, now: u64) -> GraceCensus {
        let grants = self.grants.lock().unwrap();
        let failures = self.failures.lock().unwrap();
        let mut census = GraceCensus {
            in_grace: 0,
            stale: 0,
            min_remaining: 0,
        };
        for (fingerprint, grant) in grants.iter() {
            if grant.refresh_after > now {
                continue;
            }
            let discard_at = self.discard_at(grant);
            if discard_at <= now {
                continue;
            }
            if grant.expires_at > now {
                census.in_grace += 1;
            } else if failures
                .peek(fingerprint)
                .and_then(|failure| failure.outage_at)
                .is_some_and(|at| at >= grant.expires_at)
            {
                census.stale += 1;
            } else {
                // Expired and idle, or expired on a healthy control plane:
                // nothing is being served on it, so it names no cliff.
                continue;
            }
            let remaining = discard_at - now;
            census.min_remaining = if census.in_grace + census.stale == 1 {
                remaining
            } else {
                census.min_remaining.min(remaining)
            };
        }
        census
    }

    fn live_denial(&self, fingerprint: &str, now: u64) -> Option<String> {
        let mut denials = self.denials.lock().unwrap();
        let denial = denials.get(fingerprint)?;
        if denial.until > now {
            return Some(denial.reason.clone());
        }
        denials.pop(fingerprint);
        None
    }

    fn publish_gauges(&self) {
        self.report(|| metrics::report_grant_cache_size(self.grants.lock().unwrap().len()));
    }

    /// Shadow mode admits either way, so a moving counter would publish the
    /// verdict the response withheld (OB-12, INV-39).
    fn silent(&self) -> bool {
        self.enforcement != Enforcement::Enforce
    }

    /// Every OB-13 signal goes out through here, so no call site can forget the
    /// shadow-mode guard.
    fn report(&self, publish: impl FnOnce()) {
        if !self.silent() {
            publish();
        }
    }

    /// Seconds since the control plane last answered, counting from boot on a
    /// replica it never has. Read per scrape, so it climbs through an outage
    /// rather than freezing at the last value (OB-13).
    pub fn last_exchange_success_age(&self) -> u64 {
        let since = match self.last_success.load(Ordering::Acquire) {
            0 => self.started_at,
            at => at,
        };
        now_secs().saturating_sub(since)
    }

    pub fn capacity(&self) -> usize {
        self.limits.grant_cache_capacity
    }

    #[cfg(test)]
    pub(crate) fn insert_for_test(&self, fingerprint: &str, grant: CachedGrant) {
        self.grants
            .lock()
            .unwrap()
            .put(fingerprint.to_owned(), Arc::new(grant));
    }

    /// What a burst of misses does to the token bucket, without the burst.
    #[cfg(test)]
    pub(crate) fn exhaust_budget_for_test(&self) {
        let mut limiter = self.limiter.lock().unwrap();
        limiter.tokens = 0.0;
        limiter.last = Instant::now();
    }
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or(NonZeroUsize::MIN)
}

/// The second `now` has become, `elapsed` later. Advanced rather than reread,
/// so the clock the caller passed in stays authoritative.
fn second_reached(now: u64, elapsed: Duration) -> u64 {
    now.saturating_add(elapsed.as_secs())
}

/// The same second rounded up, which every expiry check uses: `now` is floored
/// and `elapsed` is fractional, so the true second can be one later — and late
/// is the only direction REQ-54 allows erring in.
fn latest_second_reached(now: u64, elapsed: Duration) -> u64 {
    second_reached(now, elapsed).saturating_add(1)
}

struct RateLimiter {
    rate_per_sec: f64,
    tokens: f64,
    last: Instant,
}

impl RateLimiter {
    fn new(rate_per_sec: u64) -> Self {
        Self {
            rate_per_sec: rate_per_sec as f64,
            tokens: rate_per_sec as f64,
            last: Instant::now(),
        }
    }

    fn take(&mut self) -> bool {
        if self.rate_per_sec <= 0.0 {
            return false;
        }
        let now = Instant::now();
        let elapsed = now.duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + elapsed * self.rate_per_sec).min(self.rate_per_sec);
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::auth::test_support::{
        cache_for, cache_with_limits, credential, MockControlPlane, KEY_ID,
    };

    const NOW: u64 = 1_800_000_000;

    fn granted(resolved: &Resolved) -> &CachedGrant {
        match resolved {
            Resolved::Grant(grant) => grant,
            other => panic!("expected a grant, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn the_first_request_exchanges_and_the_next_one_does_not() {
        let cp = MockControlPlane::spawn().await;
        cp.grant(KEY_ID, None, NOW + 300, NOW + 900);
        let cache = cache_for(&cp).await;

        let first = cache.resolve(&credential(), NOW).await;
        assert_eq!(granted(&first).key_id, KEY_ID);
        assert_eq!(cp.exchanges(), 1);

        cache.resolve(&credential(), NOW).await;
        assert_eq!(cp.exchanges(), 1, "a fresh grant answers locally");
    }

    /// INV-14: concurrent requests for one credential cost one call, whatever
    /// the request rate.
    #[tokio::test]
    async fn a_burst_on_one_credential_makes_one_exchange() {
        let cp = MockControlPlane::spawn().await;
        cp.grant(KEY_ID, None, NOW + 300, NOW + 900);
        cp.delay(Duration::from_millis(50));
        let cache = cache_for(&cp).await;

        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..16 {
            let cache = cache.clone();
            tasks.spawn(async move { cache.resolve(&credential(), NOW).await });
        }
        while let Some(result) = tasks.join_next().await {
            granted(&result.unwrap());
        }

        assert_eq!(cp.exchanges(), 1);
    }

    /// The burst rule has to survive the answer being a failure. Without it
    /// each waiter re-enters the exchange and pays the full per-exchange
    /// deadline in turn, so the last one is held for N times a timeout that is
    /// specified to stay under the deadline callers wait on.
    #[tokio::test]
    async fn a_burst_against_a_failing_control_plane_makes_one_exchange() {
        let cp = MockControlPlane::spawn().await;
        cp.status(KEY_ID, 503);
        cp.delay(Duration::from_millis(50));
        let cache = cache_for(&cp).await;

        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..16 {
            let cache = cache.clone();
            tasks.spawn(async move { cache.resolve(&credential(), NOW).await });
        }
        while let Some(result) = tasks.join_next().await {
            let resolved = result.unwrap();
            assert!(
                matches!(resolved, Resolved::Unavailable),
                "got {resolved:?}"
            );
        }

        assert_eq!(cp.exchanges(), 1);
    }

    /// A control plane that renews on issue — or whose clock trails the
    /// portal's — must not produce a grant that is due the second it is stored.
    /// Such a grant is never really cached: every request goes back to the
    /// exchange path and spends the fleet-shared budget.
    #[tokio::test]
    async fn a_grant_that_arrives_already_due_is_still_cached() {
        let cp = MockControlPlane::spawn().await;
        cp.grant(KEY_ID, None, NOW, NOW + 900);
        let cache = cache_for(&cp).await;

        let first = cache.resolve(&credential(), NOW).await;
        assert!(
            granted(&first).refresh_after > NOW,
            "a grant stored due at the current second re-exchanges forever"
        );

        cache.resolve(&credential(), NOW).await;
        assert_eq!(cp.exchanges(), 1);
    }

    /// The jitter can span the whole refresh window, so the already-due floor
    /// has to be applied after it — a maximal draw would otherwise store the
    /// grant due at the current second, the exact state the floor exists to
    /// prevent, and every request until renewal would ride the grace path.
    #[tokio::test]
    async fn a_maximal_jitter_draw_cannot_store_a_grant_already_due() {
        let cp = MockControlPlane::spawn().await;
        // A one-second refresh window, so half of all draws are maximal.
        cp.grant(KEY_ID, None, NOW + 1, NOW + 900);
        let cache = {
            let config = crate::auth::ResolvedAuth {
                limits: Limits {
                    refresh_jitter_pct: 100,
                    // The loop below re-exchanges faster than the default
                    // budget refills.
                    exchange_rate_per_sec: 100_000,
                    ..Limits::default()
                },
                ..cp.config()
            };
            let signer = config
                .signer(sqd_network_transport::Keypair::generate_ed25519())
                .unwrap();
            GrantCache::new(
                ControlPlaneClient::new(&config, signer).unwrap(),
                config.limits.clone(),
                config.enforcement,
            )
        };

        // The draw is random, so a single pass proves nothing: pin the floor
        // across enough draws that a maximal one is all but certain.
        for _ in 0..64 {
            let resolved = cache.resolve(&credential(), NOW).await;
            assert!(
                granted(&resolved).refresh_after > NOW,
                "a maximal draw stored a grant already due"
            );
            cache.grants.lock().unwrap().pop(&credential().fingerprint);
        }
    }

    fn census(in_grace: usize, stale: usize, min_remaining: u64) -> GraceCensus {
        GraceCensus {
            in_grace,
            stale,
            min_remaining,
        }
    }

    /// The census the scrape republishes: the admission rates say the cliff is
    /// coming, the counts say how wide it is and which side of the hard expiry
    /// it is on, and the minimum names when the first hard refusal lands if
    /// the outage persists (OB-9).
    #[tokio::test]
    async fn the_grace_census_names_the_first_hard_refusal() {
        let cp = MockControlPlane::spawn().await;
        let cache = cache_with_limits(
            &cp,
            Limits {
                outage_grace_secs: 1000,
                ..cp.config().limits
            },
        )
        .await;
        for (fingerprint, refresh_after, expires_at) in [
            // Fresh: not in grace.
            ("fresh", NOW + 300, NOW + 900),
            // In grace, the nearest cliff.
            ("closest", NOW, NOW + 500),
            ("further", NOW, NOW + 700),
        ] {
            cache.insert_for_test(
                fingerprint,
                CachedGrant {
                    key_id: fingerprint.to_owned(),
                    datasets: None,
                    organization_id: None,
                    refresh_after,
                    expires_at,
                },
            );
        }
        // Outages witnessed after each expiry; "fresh" never sees one.
        cache.remember("closest", Resolved::Unavailable, Some(NOW + 550));
        cache.remember("further", Resolved::Unavailable, Some(NOW + 750));

        // The first refusal is the nearest expiry plus the outage grace.
        assert_eq!(cache.grace_census(NOW), census(2, 0, 1500));
        // Past its hard expiry a grant riding an outage is stale rather than
        // gone, and still names the cliff; the fresh one has aged into grace.
        assert_eq!(cache.grace_census(NOW + 600), census(2, 1, 900));
        // Past its discard point a grant stops counting, and an expired grant
        // with no outage behind it is idle and names nothing.
        assert_eq!(cache.grace_census(NOW + 1500), census(0, 1, 200));
        // Nothing held reads as zero, not as a stale minimum.
        assert_eq!(cache.grace_census(NOW + 1900), census(0, 0, 0));
    }

    /// `refresh_after` cannot advance while the renewal keeps failing, so a
    /// grace window without a cooldown turns every request into an attempt and
    /// points the portal's whole data-plane rate at a control plane that is
    /// already down.
    #[tokio::test]
    async fn grace_does_not_re_ask_the_control_plane_on_every_request() {
        let cp = MockControlPlane::spawn().await;
        cp.grant(KEY_ID, None, NOW + 300, NOW + 900);
        let cache = cache_for(&cp).await;
        cache.resolve(&credential(), NOW).await;
        assert_eq!(cp.exchanges(), 1);

        cp.stop();
        for _ in 0..25 {
            granted(&cache.resolve(&credential(), NOW + 400).await);
            // Long enough for the spawned renewal to land, so the loop measures
            // the cooldown rather than the in-flight guard.
            tokio::time::sleep(Duration::from_millis(2)).await;
        }

        assert!(
            cp.exchanges() <= 2,
            "25 grace-served requests made {} exchanges",
            cp.exchanges()
        );
    }

    /// The fast path serves through an outage on a grant that is due but not
    /// expired. The path under the lock reads the same grant, so it has to
    /// reach the same verdict — otherwise the grace depends on which side of a
    /// contended lock the request happened to arrive (REQ-54).
    #[tokio::test]
    async fn a_non_authoritative_refusal_does_not_discard_a_live_grant() {
        let cp = MockControlPlane::spawn().await;
        let cache = cache_for(&cp).await;
        let held = Arc::new(CachedGrant {
            key_id: KEY_ID.to_owned(),
            datasets: None,
            organization_id: None,
            refresh_after: NOW,
            expires_at: NOW + 900,
        });

        let fp = &credential().fingerprint;
        for refusal in [Resolved::Unavailable, Resolved::Saturated] {
            let graced = cache.or_grace(fp, refusal, Some(held.clone()), NOW + 1);
            assert_eq!(granted(&graced).key_id, KEY_ID);
        }

        // With nothing to fall back on the refusal stands, and stays retryable
        // rather than becoming a verdict about the credential.
        let bare = cache.or_grace(fp, Resolved::Unavailable, None, NOW + 1);
        assert!(matches!(bare, Resolved::Unavailable), "got {bare:?}");

        // The deadline is judged as of the admission, not the arrival: a lock
        // wait plus a failed exchange can outlast the grant (REQ-54).
        let served = cache.or_grace(fp, Resolved::Unavailable, Some(held.clone()), NOW + 899);
        assert_eq!(granted(&served).key_id, KEY_ID);

        // Past `expires_at` only an outage of this credential's exchange,
        // witnessed since the expiry, serves — up to the discard point.
        let unwitnessed = cache.or_grace(fp, Resolved::Unavailable, Some(held.clone()), NOW + 900);
        assert!(
            matches!(unwitnessed, Resolved::Unavailable),
            "got {unwitnessed:?}"
        );
        cache.remember(fp, Resolved::Unavailable, Some(NOW + 900));
        let stale = cache.or_grace(fp, Resolved::Unavailable, Some(held.clone()), NOW + 900);
        assert_eq!(granted(&stale).key_id, KEY_ID);
        let saturated = cache.or_grace(fp, Resolved::Saturated, Some(held.clone()), NOW + 900);
        assert!(
            matches!(saturated, Resolved::Saturated),
            "got {saturated:?}"
        );
        let discard_at = NOW + 900 + cache.limits.outage_grace_secs;
        let last = cache.or_grace(
            fp,
            Resolved::Unavailable,
            Some(held.clone()),
            discard_at - 1,
        );
        assert_eq!(granted(&last).key_id, KEY_ID);
        let discarded = cache.or_grace(fp, Resolved::Unavailable, Some(held), discard_at);
        assert!(
            matches!(discarded, Resolved::Unavailable),
            "got {discarded:?}"
        );
    }

    /// The keyed-lock map has no capacity bound, so an entry left behind leaks
    /// for the process lifetime. Drives the contended path and pins that it
    /// drains.
    #[tokio::test]
    async fn contended_fingerprints_do_not_accumulate_locks() {
        let cp = MockControlPlane::spawn().await;
        // Due on arrival, so every request lands in grace and spawns a renewal
        // that has to race the request holding the lock.
        cp.grant(KEY_ID, None, NOW, NOW + 100_000);
        cp.delay(Duration::from_millis(2));
        let cache = cache_for(&cp).await;
        cache.resolve(&credential(), NOW).await;

        for round in 0..24 {
            let mut tasks = tokio::task::JoinSet::new();
            for _ in 0..8 {
                let cache = cache.clone();
                tasks.spawn(async move { cache.resolve(&credential(), NOW + 2 + round).await });
            }
            while let Some(result) = tasks.join_next().await {
                result.unwrap();
            }
        }
        // Let any renewal still racing the last request check its entry back in.
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(
            cache.inflight.len(),
            0,
            "a losing renewal orphaned its lock entry"
        );
    }

    /// REQ-54's grace, and its end. The control plane stops answering: a grant
    /// past `refresh_after` keeps serving, the same grant past `expires_at`
    /// keeps serving once this replica has seen the authority fail since the
    /// expiry, and past the outage grace it does not — retryably, and never as
    /// a claim about the credential.
    #[tokio::test]
    async fn an_outage_is_survived_to_the_outage_grace_and_no_further() {
        let cp = MockControlPlane::spawn().await;
        cp.grant(KEY_ID, None, NOW + 300, NOW + 900);
        let cache = cache_with_limits(
            &cp,
            Limits {
                outage_grace_secs: 600,
                ..cp.config().limits
            },
        )
        .await;
        cache.resolve(&credential(), NOW).await;

        cp.stop();

        let graced = cache.resolve(&credential(), NOW + 400).await;
        assert_eq!(
            granted(&graced).key_id,
            KEY_ID,
            "a grant past refresh_after still serves while renewal fails"
        );
        // Let the renewal that request spawned fail, so the exchanges counted
        // below are the ones the expired grant costs.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let before = cp.exchanges();
        let stale_before = metrics::stale_admissions();

        // The last failure this replica saw predates the expiry, so the first
        // request past it revalidates rather than trusting old news — and is
        // served on the grant when that revalidation cannot run either.
        let stale = cache.resolve(&credential(), NOW + 901).await;
        assert_eq!(granted(&stale).key_id, KEY_ID);
        assert_eq!(
            cp.exchanges(),
            before + 1,
            "the expired grant is revalidated first"
        );
        assert!(metrics::stale_admissions() > stale_before);

        // With the authority now known silent since the expiry, the next
        // request is served without waiting on another attempt.
        let again = cache.resolve(&credential(), NOW + 902).await;
        assert_eq!(granted(&again).key_id, KEY_ID);
        assert_eq!(
            cp.exchanges(),
            before + 1,
            "served stale without a new exchange"
        );

        let discarded = cache.resolve(&credential(), NOW + 1501).await;
        assert!(
            matches!(discarded, Resolved::Unavailable),
            "past the outage grace the outage is a dependency failure, got {discarded:?}"
        );
    }

    fn other_credential() -> Credential {
        crate::auth::extractor::parse_token_for_test("sqd_portal_k2_anothersecretvalue")
            .expect("the second test token parses")
    }

    /// Past the expiry the grant needs the authority to have failed; a spent
    /// local budget never asked it. Serving here is what let a flood of junk
    /// keys keep a revoked key working (HZ-10).
    #[tokio::test]
    async fn a_spent_budget_past_the_expiry_refuses() {
        let cp = MockControlPlane::spawn().await;
        cp.grant(KEY_ID, None, NOW + 300, NOW + 900);
        let cache = cache_for(&cp).await;
        cache.resolve(&credential(), NOW).await;
        cache.exhaust_budget_for_test();

        let refused = cache.resolve(&credential(), NOW + 901).await;

        assert!(matches!(refused, Resolved::Saturated), "got {refused:?}");
        assert_eq!(cp.exchanges(), 1, "the control plane was never asked");
    }

    /// An answer this build cannot use is the control plane speaking: a newer
    /// claims version may carry a restriction the old grant lacks.
    #[tokio::test]
    async fn an_unusable_answer_past_the_expiry_refuses() {
        let cp = MockControlPlane::spawn().await;
        cp.grant(KEY_ID, None, NOW + 300, NOW + 900);
        let cache = cache_for(&cp).await;
        cache.resolve(&credential(), NOW).await;

        cp.raw(
            KEY_ID,
            serde_json::json!({
                "result": "granted",
                "grant": {
                    "claims_version": crate::auth::types::CLAIMS_VERSION + 1,
                    "key_id": KEY_ID,
                    "refresh_after": NOW + 1300,
                    "expires_at": NOW + 1900,
                },
            }),
        );
        let refused = cache.resolve(&credential(), NOW + 901).await;

        assert!(matches!(refused, Resolved::Unavailable), "got {refused:?}");
    }

    /// Outage evidence is per credential. One key's failing exchange does not
    /// let another key's expired grant skip revalidation, and another key's
    /// answer does not send the failing key back to a synchronous exchange.
    #[tokio::test]
    async fn one_keys_outage_neither_opens_nor_closes_another_keys_grace() {
        let cp = MockControlPlane::spawn().await;
        cp.grant(KEY_ID, None, NOW + 300, NOW + 900);
        cp.grant("k2", None, NOW + 300, NOW + 900);
        let cache = cache_for(&cp).await;
        cache.resolve(&credential(), NOW).await;
        cache.resolve(&other_credential(), NOW).await;

        cp.status(KEY_ID, 503);
        granted(&cache.resolve(&credential(), NOW + 901).await);

        cp.deny("k2", "revoked");
        let other = cache.resolve(&other_credential(), NOW + 901).await;
        assert!(matches!(other, Resolved::Denied(_)), "got {other:?}");

        let before = cp.exchanges();
        granted(&cache.resolve(&credential(), NOW + 902).await);
        assert_eq!(
            cp.exchanges(),
            before,
            "served stale without a new exchange"
        );
    }

    /// The knob at zero is the behaviour before it existed: nothing serves past
    /// `expires_at`, whatever the authority's state.
    #[tokio::test]
    async fn zero_outage_grace_stops_at_the_hard_expiry() {
        let cp = MockControlPlane::spawn().await;
        cp.grant(KEY_ID, None, NOW + 300, NOW + 900);
        let cache = cache_with_limits(
            &cp,
            Limits {
                outage_grace_secs: 0,
                ..cp.config().limits
            },
        )
        .await;
        cache.resolve(&credential(), NOW).await;

        cp.stop();

        granted(&cache.resolve(&credential(), NOW + 899).await);
        let expired = cache.resolve(&credential(), NOW + 900).await;
        assert!(matches!(expired, Resolved::Unavailable), "got {expired:?}");
    }

    /// An expired grant is not a licence to skip the authority: a key idle
    /// across its expiry while the control plane was healthy is exchanged
    /// before it is served, and served on what the exchange returned.
    #[tokio::test]
    async fn an_expired_grant_is_revalidated_while_the_authority_answers() {
        let cp = MockControlPlane::spawn().await;
        cp.grant(KEY_ID, None, NOW + 300, NOW + 900);
        let cache = cache_for(&cp).await;
        cache.resolve(&credential(), NOW).await;

        cp.grant(KEY_ID, None, NOW + 1300, NOW + 1900);
        let renewed = cache.resolve(&credential(), NOW + 1000).await;

        assert_eq!(cp.exchanges(), 2);
        assert_eq!(
            granted(&renewed).expires_at,
            NOW + 1900,
            "the request is answered on the fresh grant, not the expired one"
        );
    }

    /// INV-6 holds through the outage grace: the moment the authority answers
    /// again, its answer outranks the stale grant — a denial evicts, a grant
    /// replaces.
    #[tokio::test]
    async fn the_authority_returning_outranks_a_stale_grant_at_once() {
        for revoked in [true, false] {
            let cp = MockControlPlane::spawn().await;
            cp.grant(KEY_ID, None, NOW + 300, NOW + 900);
            let cache = cache_with_limits(
                &cp,
                Limits {
                    // The cooldown is the exchange timeout; a short one lets
                    // the refresh below run within the test.
                    exchange_timeout_ms: 20,
                    ..cp.config().limits
                },
            )
            .await;
            cache.resolve(&credential(), NOW).await;

            cp.status(KEY_ID, 503);
            granted(&cache.resolve(&credential(), NOW + 901).await);

            cp.clear_status(KEY_ID);
            if revoked {
                cp.deny(KEY_ID, "revoked");
            } else {
                // Inside the lifetime cap as of the refresh, so the stored
                // expiry is the offered one.
                cp.grant(KEY_ID, None, NOW + 1300, NOW + 1700);
            }

            let settled = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    match cache.resolve(&credential(), NOW + 902).await {
                        Resolved::Denied(reason) => return Err(reason),
                        Resolved::Grant(grant) if grant.expires_at == NOW + 1700 => return Ok(()),
                        _ => tokio::time::sleep(Duration::from_millis(5)).await,
                    }
                }
            })
            .await
            .expect("the refresh must land");

            if revoked {
                assert_eq!(settled, Err("revoked".to_owned()));
            } else {
                assert_eq!(settled, Ok(()));
            }
        }
    }

    /// INV-6: the authority has spoken since, so the remaining lifetime does not
    /// outrank it.
    #[tokio::test]
    async fn a_denial_evicts_a_live_grant_at_once() {
        let cp = MockControlPlane::spawn().await;
        cp.grant(KEY_ID, None, NOW + 300, NOW + 900);
        let cache = cache_for(&cp).await;
        cache.resolve(&credential(), NOW).await;

        cp.deny(KEY_ID, "revoked");
        // The renewal is due, so this request triggers one and is still served.
        let served = cache.resolve(&credential(), NOW + 400).await;
        granted(&served);

        // …but the denial it fetched replaces the grant, so the next one is not.
        let refused = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match cache.resolve(&credential(), NOW + 401).await {
                    Resolved::Denied(reason) => return reason,
                    _ => tokio::task::yield_now().await,
                }
            }
        })
        .await
        .expect("the refresh must land");

        assert_eq!(refused, "revoked");
    }

    #[tokio::test]
    async fn a_denial_is_remembered_briefly_rather_than_re_asked_every_request() {
        let cp = MockControlPlane::spawn().await;
        cp.deny(KEY_ID, "unknown_key");
        let cache = cache_for(&cp).await;

        for _ in 0..5 {
            assert!(matches!(
                cache.resolve(&credential(), NOW).await,
                Resolved::Denied(_)
            ));
        }
        assert_eq!(cp.exchanges(), 1);

        // Past the TTL the authority is asked again: a key minted moments after
        // a refusal has to be able to start working.
        cp.grant(KEY_ID, None, NOW + 3900, NOW + 4500);
        let resolved = cache.resolve(&credential(), NOW + 3600).await;
        granted(&resolved);
        assert_eq!(cp.exchanges(), 2);
    }

    /// A denial and a failure are alternatives for one fingerprint, but sharing
    /// a cache entry between them let a failed renewal erase a live denial. The
    /// revoked key it belonged to was then told to retry, and every retry
    /// inside the TTL is another exchange from the fleet-shared budget the
    /// denial cache exists to protect (HZ-10).
    #[tokio::test]
    async fn a_failed_exchange_does_not_erase_a_live_denial() {
        let cp = MockControlPlane::spawn().await;
        let cache = cache_for(&cp).await;
        let fingerprint = &credential().fingerprint;

        cache.remember_denial(fingerprint, "revoked".to_owned(), NOW);
        cache.remember_failure(fingerprint, Resolved::Unavailable);

        assert_eq!(
            cache.live_denial(fingerprint, NOW).as_deref(),
            Some("revoked")
        );
    }

    /// Both negatives are filled at whatever rate well-formed tokens arrive, so
    /// one bound shared between them is one an attacker spends on either side.
    #[tokio::test]
    async fn a_flood_of_failures_does_not_evict_remembered_denials() {
        let cp = MockControlPlane::spawn().await;
        let cache = cache_for(&cp).await;
        cache.remember_denial("revoked-key", "revoked".to_owned(), NOW);

        for index in 0..=cache.limits.denial_cache_capacity {
            cache.remember_failure(&format!("flood-{index}"), Resolved::Saturated);
        }

        assert_eq!(
            cache.live_denial("revoked-key", NOW).as_deref(),
            Some("revoked")
        );
    }

    /// HZ-10: the budget is fail-closed, and what it refuses is retryable
    /// overload rather than a verdict.
    #[tokio::test]
    async fn a_spent_budget_refuses_without_calling_the_control_plane() {
        let cp = MockControlPlane::spawn().await;
        cp.grant(KEY_ID, None, NOW + 300, NOW + 900);
        let cache = cache_for(&cp).await;
        cache.exhaust_budget_for_test();

        let resolved = cache.resolve(&credential(), NOW).await;

        assert!(matches!(resolved, Resolved::Saturated), "got {resolved:?}");
        assert_eq!(cp.exchanges(), 0);
    }

    /// The control plane sets the lifetime; the portal owns the ceiling.
    #[tokio::test]
    async fn an_over_long_lifetime_is_capped() {
        let cp = MockControlPlane::spawn().await;
        cp.grant(KEY_ID, None, NOW + 300, NOW + 30 * 24 * 3600);
        let cache = cache_for(&cp).await;

        let resolved = cache.resolve(&credential(), NOW).await;

        let cap = cache.limits.max_grant_lifetime_secs;
        assert_eq!(granted(&resolved).expires_at, NOW + cap);
    }

    /// The eviction hook recursed into itself instead of reporting, so pushing a
    /// live grant out of a full cache overflowed the stack and took the process
    /// with it. Reachable from the ordinary HZ-13 path — a credential working set
    /// larger than the cache — where the cap is supposed to be a capacity signal,
    /// not a crash.
    #[tokio::test]
    async fn evicting_a_live_grant_reports_it_rather_than_recursing() {
        let cp = MockControlPlane::spawn().await;
        cp.grant(KEY_ID, None, NOW + 300, NOW + 900);
        let cache = cache_for(&cp).await;

        // Filled directly: `insert_for_test` bypasses `store`, so the one
        // eviction under test is the one the real path performs below.
        for i in 0..cache.capacity() {
            cache.insert_for_test(
                &format!("fingerprint-{i}"),
                CachedGrant {
                    key_id: format!("k{i}"),
                    datasets: None,
                    organization_id: None,
                    refresh_after: NOW + 300,
                    expires_at: NOW + 900,
                },
            );
        }

        let resolved = cache.resolve(&credential(), NOW).await;

        assert_eq!(granted(&resolved).key_id, KEY_ID);
        assert_eq!(cache.grants.lock().unwrap().len(), cache.capacity());
    }

    #[tokio::test]
    async fn renewal_is_never_scheduled_after_the_hard_expiry() {
        let cp = MockControlPlane::spawn().await;
        // A control plane that got the two the wrong way round.
        cp.grant(KEY_ID, None, NOW + 900, NOW + 300);
        let cache = cache_for(&cp).await;

        let grant = cache.resolve(&credential(), NOW).await;
        let grant = granted(&grant);

        assert!(grant.refresh_after <= grant.expires_at);
        assert!(grant.refresh_after >= NOW);
    }

    #[tokio::test]
    async fn the_cache_evicts_rather_than_grows() {
        let cp = MockControlPlane::spawn().await;
        let cache = cache_for(&cp).await;
        let capacity = cache.capacity();

        for index in 0..capacity + 10 {
            cache.insert_for_test(
                &format!("fingerprint-{index}"),
                CachedGrant {
                    key_id: format!("k{index}"),
                    datasets: None,
                    organization_id: None,
                    refresh_after: NOW + 300,
                    expires_at: NOW + 900,
                },
            );
        }

        assert_eq!(cache.grants.lock().unwrap().len(), capacity);
        assert!(
            cache.held_grant("fingerprint-0", NOW).is_none(),
            "the least recently used entry goes first"
        );
        assert!(cache
            .held_grant(&format!("fingerprint-{}", capacity + 9), NOW)
            .is_some());
    }

    /// A request cancelled while it waits on the exchange must still check its
    /// entry back in. `inflight` is keyed by a fingerprint the caller chooses and
    /// bounded by nothing, so a leak here is one an attacker drives with nothing
    /// but well-formed tokens and a hang-up.
    #[tokio::test]
    async fn a_cancelled_request_leaves_no_inflight_entry() {
        let cp = MockControlPlane::spawn().await;
        cp.grant(KEY_ID, None, NOW + 300, NOW + 900);
        cp.delay(Duration::from_secs(30));
        let cache = cache_for(&cp).await;

        for _ in 0..8 {
            let cancelled =
                tokio::time::timeout(Duration::from_millis(10), cache.resolve(&credential(), NOW))
                    .await;
            assert!(cancelled.is_err(), "the exchange should still be running");
        }

        assert!(
            cache.inflight.is_empty(),
            "a cancelled exchange stranded its fingerprint"
        );
    }

    #[tokio::test]
    async fn the_keyed_lock_map_does_not_grow_with_every_credential_seen() {
        let cp = MockControlPlane::spawn().await;
        cp.deny(KEY_ID, "unknown_key");
        let cache = cache_for(&cp).await;

        for _ in 0..8 {
            cache.resolve(&credential(), NOW).await;
        }

        assert!(cache.inflight.is_empty());
    }
    /// OB-12: shadow mode admits either way, so nothing about the exchange may
    /// reach the keyless scrape — a counter that moves only for a valid key hands
    /// a caller the verdict its 200 withheld. Asserted on the decision rather
    /// than the counters: those are process-global, so a concurrent test's
    /// exchange would answer for this one.
    #[tokio::test]
    async fn shadow_mode_publishes_nothing_about_the_exchange() {
        let cp = MockControlPlane::spawn().await;
        cp.grant(KEY_ID, None, NOW + 300, NOW + 900);

        let shadow = super::super::test_support::cache_with(&cp, Enforcement::LogOnly).await;
        assert!(shadow.silent());

        let enforcing = super::super::test_support::cache_with(&cp, Enforcement::Enforce).await;
        assert!(!enforcing.silent());

        // The enforcing direction is race-tolerant: other tests only ever add.
        let before = metrics::exchanges(ExchangeOutcome::Answered);
        enforcing.resolve(&credential(), NOW).await;
        assert!(metrics::exchanges(ExchangeOutcome::Answered) > before);
    }

    /// A grant whose whole life fits inside the exchange that fetched it is not
    /// an authorization, and the exchange that returned one is not a success.
    /// Publishing it as one refreshes the freshness gauge OB-9 reads as the
    /// distance to the hard-expiry cliff — the operator's only warning.
    #[tokio::test]
    async fn a_grant_that_expired_in_flight_is_a_failed_exchange_not_an_answered_one() {
        let cp = MockControlPlane::spawn().await;
        // Inside `expires_at > now`, so the client accepts it on arrival; the
        // cache is the only rung that sees it has no life left by then.
        cp.grant(KEY_ID, None, NOW + 1, NOW + 1);
        let cache = cache_for(&cp).await;

        // The counters are process-global, so only the monotone direction is
        // race-tolerant; the claim about *this* exchange is read off the cache.
        let failed_before = metrics::exchanges(ExchangeOutcome::Failed);

        let resolved = cache.resolve(&credential(), NOW).await;

        assert!(
            matches!(resolved, Resolved::Unavailable),
            "an expired grant is a dependency failure, not a verdict: got {resolved:?}"
        );
        assert!(metrics::exchanges(ExchangeOutcome::Failed) > failed_before);
        assert_eq!(
            cache.last_success.load(Ordering::Acquire),
            0,
            "an unusable answer must not stamp the freshness gauge OB-9 reads"
        );
    }
}
