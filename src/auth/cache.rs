//! The only authorization state a replica holds: grants it was handed for
//! credentials it has actually served (DEF-18).
//!
//! Keyed on a fingerprint of the *whole* credential, never on the key id: an
//! entry reachable by id alone would admit the next caller to name that id
//! without proving it holds the secret.

use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};

use lru::LruCache;
use tokio::sync::Semaphore;

use super::{
    client::{ControlPlaneClient, Exchanged},
    config::{Enforcement, Limits},
    extractor::Credential,
    now_secs,
};
use crate::metrics::{self, ExchangeOutcome};

/// A grant as held, after the portal's own cap has been applied to what the
/// control plane offered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedGrant {
    pub key_id: String,
    pub datasets: Option<Vec<String>>,
    /// A request arriving past this is still served.
    pub refresh_after: u64,
    /// When to stop. Nothing serves on this grant afterwards, whatever the
    /// control plane's state — the hard bound on stale authorization (REQ-54).
    pub expires_at: u64,
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

/// An attempt that established nothing about the credential. Held because the
/// alternative is paying for it again: once per waiter queued behind it, and
/// once per request for as long as a renewal keeps failing.
struct Failure {
    resolved: Resolved,
    /// When the attempt finished. A waiter that queued before this is answered
    /// by it — its own call could not come back fresher.
    completed_at: Instant,
    retry_after: Instant,
}

pub struct GrantCache {
    client: ControlPlaneClient,
    limits: Limits,
    grants: Mutex<LruCache<String, Arc<CachedGrant>>>,
    denials: Mutex<LruCache<String, Denial>>,
    /// Capped for the same reason denials are: the fingerprint is chosen by
    /// whoever sent the credential (HZ-10).
    failures: Mutex<LruCache<String, Failure>>,
    /// One exchange in flight per fingerprint. Without it a burst on one
    /// uncached credential is one control-plane call per request.
    inflight: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
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
            inflight: Mutex::new(HashMap::new()),
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
        if let Some(grant) = self.usable_grant(&credential.fingerprint, now) {
            if grant.refresh_after > now {
                return Resolved::Grant(grant);
            }
            // Serving on a grant whose renewal has not landed is the outage
            // grace, and the only warning an operator gets before the cliff.
            self.report(metrics::report_grace_admission);
            self.spawn_refresh(credential, now);
            return Resolved::Grant(grant);
        }
        if let Some(reason) = self.live_denial(&credential.fingerprint, now) {
            return Resolved::Denied(reason);
        }

        // Taken before queueing: it is what distinguishes an answer the holder
        // of the lock produced on our behalf from one that predates us.
        let arrived = Instant::now();
        // Declared before the mutex guard so it drops after it — the entry can
        // only be retired once nothing holds the lock.
        let (_entry, lock) = InflightEntry::take(self, &credential.fingerprint);
        let _held = lock.lock_owned().await;
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
        let held = self.usable_grant(&credential.fingerprint, now);
        if let Some(grant) = &held {
            if grant.refresh_after > now {
                return Resolved::Grant(grant.clone());
            }
        }
        if let Some(reason) = self.live_denial(&credential.fingerprint, now) {
            return Resolved::Denied(reason);
        }
        // A failure is an answer too, for whoever was already waiting on it.
        // Without this the re-check above catches only the successful case and
        // a burst against a failing control plane costs one full timeout per
        // request, serially.
        if let Some(resolved) = self.failure_since(&credential.fingerprint, arrived) {
            return self.or_grace(resolved, held, admission_second(now, arrived));
        }
        let resolved = self.exchange(credential, now).await;
        self.or_grace(resolved, held, admission_second(now, arrived))
    }

    /// Keeps a grant that is still inside its hard expiry from being discarded
    /// by an exchange that established nothing. The fast path already serves
    /// through an outage on such a grant; refusing here instead would make the
    /// grace depend on which side of the lock the request arrived (REQ-54).
    ///
    /// The expiry is checked against `now` as of the admission, not the
    /// arrival: the wait for the lock plus a failed exchange can outlast the
    /// grant the fast path saw, and past `expires_at` is the one direction
    /// REQ-54 does not allow.
    fn or_grace(&self, resolved: Resolved, held: Option<Arc<CachedGrant>>, now: u64) -> Resolved {
        match (resolved, held) {
            (Resolved::Saturated | Resolved::Unavailable, Some(grant))
                if grant.expires_at > now =>
            {
                self.report(metrics::report_grace_admission);
                Resolved::Grant(grant)
            }
            (resolved, _) => resolved,
        }
    }

    /// The renewal a request past `refresh_after` triggers without waiting for
    /// it. Skipped outright when one is already running for this fingerprint.
    fn spawn_refresh(self: &Arc<Self>, credential: &Credential, now: u64) {
        // `refresh_after` cannot advance while the exchange keeps failing, so
        // without a cooldown every request served on grace spawns another
        // attempt and the whole data-plane rate lands on a control plane that
        // is already down.
        if self.cooling_down(&credential.fingerprint) {
            return;
        }
        // Cloned only once the cooldown lets an attempt through: during an
        // outage every grace-served request lands here, and the common case is
        // this returning above.
        let credential = credential.clone();
        let cache = self.clone();
        tokio::spawn(async move {
            let (_entry, lock) = InflightEntry::take(&cache, &credential.fingerprint);
            let Ok(_held) = lock.try_lock_owned() else {
                return;
            };
            // Another refresh may have landed between the request's read and
            // this task acquiring the lock.
            if cache
                .usable_grant(&credential.fingerprint, now)
                .is_some_and(|grant| grant.refresh_after > now)
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
        // Advanced by the call's own duration rather than reread from the wall
        // clock, so the fake clock a test drives stays authoritative. Judging the
        // answer by the request's start would admit a grant that expired while it
        // was in flight.
        let settled = now.saturating_add(elapsed.as_secs());
        // `now` is a floored wall-clock second and `elapsed` is fractional, so
        // the true second at completion can be one past `settled`. The hard
        // expiry rounds up to that worst case: serving a grant a fraction past
        // its bound is the one direction REQ-54 does not allow, and a grant with
        // under a second left is not worth the coin flip.
        let expired_by = settled.saturating_add(1);

        match answer {
            Ok(Exchanged::Granted(grant)) => {
                // Tested before anything is published. A grant that expired
                // while its exchange was in flight established nothing, so it
                // is a failed exchange like any other unusable answer (DC-8) —
                // counting it answered would also refresh the freshness gauge
                // OB-9 reads as the operator's distance to the cliff.
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
                self.failures.lock().unwrap().pop(&credential.fingerprint);
                let cached = self.store(&credential.fingerprint, grant, settled);
                Resolved::Grant(cached)
            }
            Ok(Exchanged::Denied(reason)) => {
                self.last_success.store(settled, Ordering::Release);
                self.report(|| metrics::report_exchange(ExchangeOutcome::Answered, Some(elapsed)));
                // A denial outranks whatever lifetime the grant it replaces had
                // left: the authority has spoken since (INV-6).
                self.grants.lock().unwrap().pop(&credential.fingerprint);
                self.failures.lock().unwrap().pop(&credential.fingerprint);
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
                self.remember_failure(&credential.fingerprint, Resolved::Unavailable)
            }
        }
    }

    /// Applies the portal's own bound to what the control plane offered. The
    /// lifetime is the control plane's call; the ceiling is not, so a
    /// misconfiguration upstream cannot hand the fleet a month-long
    /// authorization (REQ-54).
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
        // Clamped before the jitter is sized, so it spreads a cohort's renewals
        // without extending how long any one of them serves unrenewed (HZ-12).
        // Floored at the next second *after* the jitter, which can span the
        // whole window: a grant landing already due would put every later
        // request back on the exchange path and cache nothing.
        let renew_at = grant.refresh_after.min(expires_at);
        let refresh_after = renew_at
            .saturating_sub(self.jitter(renew_at.saturating_sub(now)))
            .max(now.saturating_add(1));

        let cached = Arc::new(CachedGrant {
            key_id: grant.key_id,
            datasets: grant.datasets,
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
        // A grant supersedes any denial the same credential earned earlier.
        self.denials.lock().unwrap().pop(fingerprint);
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
        let completed_at = Instant::now();
        self.failures.lock().unwrap().put(
            fingerprint.to_owned(),
            Failure {
                resolved: resolved.clone(),
                completed_at,
                // One attempt's own deadline: retrying a fingerprint faster than
                // a call to the authority takes cannot learn anything new.
                retry_after: completed_at + self.limits.exchange_timeout(),
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

    fn cooling_down(&self, fingerprint: &str) -> bool {
        let mut failures = self.failures.lock().unwrap();
        let Some(failure) = failures.get(fingerprint) else {
            return false;
        };
        if failure.retry_after > Instant::now() {
            return true;
        }
        failures.pop(fingerprint);
        false
    }

    /// A grant that has not passed its hard expiry, whether or not it is due for
    /// renewal. An expired one is removed rather than returned: it can never be
    /// served on again, so keeping it only costs capacity.
    fn usable_grant(&self, fingerprint: &str, now: u64) -> Option<Arc<CachedGrant>> {
        let mut grants = self.grants.lock().unwrap();
        let grant = grants.get(fingerprint)?;
        if grant.expires_at > now {
            return Some(grant.clone());
        }
        grants.pop(fingerprint);
        // Republished here as well as on store: during an outage nothing is
        // stored, and occupancy would otherwise sit at its pre-outage peak
        // while the cache actually drains to empty.
        let entries = grants.len();
        drop(grants);
        self.report(|| metrics::report_grant_cache_size(entries));
        None
    }

    /// One walk of the cache under its lock, recomputed per scrape and bounded
    /// by the cache capacity: how many grants are past `refresh_after` but
    /// inside `expires_at`, and the smallest remaining life among them (zero
    /// when none are). The grace-admission rate says the condition exists;
    /// only the minimum names when the first hard refusal lands (OB-9, OB-13).
    pub fn grace_census(&self, now: u64) -> (usize, u64) {
        let grants = self.grants.lock().unwrap();
        let mut in_grace = 0;
        let mut min_remaining = 0;
        for (_, grant) in grants.iter() {
            if grant.refresh_after <= now && grant.expires_at > now {
                in_grace += 1;
                let remaining = grant.expires_at - now;
                min_remaining = if in_grace == 1 {
                    remaining
                } else {
                    min_remaining.min(remaining)
                };
            }
        }
        (in_grace, min_remaining)
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

    fn lock_for(&self, fingerprint: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.inflight
            .lock()
            .unwrap()
            .entry(fingerprint.to_owned())
            .or_default()
            .clone()
    }

    /// Dropping the caller's reference *under* the map lock is what makes this
    /// correct: whoever releases last then sees the map holding the only one
    /// left. Counting without dropping first leaves the entry to nobody.
    fn drop_lock_if_idle(&self, fingerprint: &str, lock: Arc<tokio::sync::Mutex<()>>) {
        let mut inflight = self.inflight.lock().unwrap();
        drop(lock);
        if inflight
            .get(fingerprint)
            .is_some_and(|held| Arc::strong_count(held) == 1)
        {
            inflight.remove(fingerprint);
        }
    }

    fn publish_gauges(&self) {
        self.report(|| metrics::report_grant_cache_size(self.grants.lock().unwrap().len()));
    }

    /// Shadow mode admits either way, so any of these moving would publish the
    /// verdict the response deliberately withheld (OB-12, INV-39).
    fn silent(&self) -> bool {
        self.enforcement != Enforcement::Enforce
    }

    /// Every OB-13 signal goes out through here, so a call site cannot forget
    /// the shadow-mode guard: publishing from a silent cache is exactly the
    /// disclosure INV-39 forbids.
    fn report(&self, publish: impl FnOnce()) {
        if !self.silent() {
            publish();
        }
    }

    /// Seconds since the control plane last answered anything, counting from
    /// boot on a replica it has never answered. Republished on scrape rather
    /// than on exchange, so it climbs through an outage instead of freezing at
    /// the last value (OB-13).
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

/// The wall-clock second an admission decided now takes effect: the request's
/// arrival second advanced by the real time it spent queued and exchanging,
/// rounded up the way `exchange` rounds `expired_by` — `now` is floored and the
/// elapsed time is fractional, so the estimate may only ever err late.
fn admission_second(now: u64, arrived: Instant) -> u64 {
    now.saturating_add(arrived.elapsed().as_secs())
        .saturating_add(1)
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

/// Checks a fingerprint's entry back in however its holder leaves. Returning is
/// the easy case; a request cancelled while it waits on the lock or on the
/// exchange is the one that matters, because `inflight` is keyed by
/// attacker-chosen fingerprints and bounded by nothing.
struct InflightEntry<'a> {
    cache: &'a GrantCache,
    fingerprint: &'a str,
    lock: Option<Arc<tokio::sync::Mutex<()>>>,
}

impl<'a> InflightEntry<'a> {
    fn take(cache: &'a GrantCache, fingerprint: &'a str) -> (Self, Arc<tokio::sync::Mutex<()>>) {
        let lock = cache.lock_for(fingerprint);
        let entry = Self {
            cache,
            fingerprint,
            lock: Some(lock.clone()),
        };
        (entry, lock)
    }
}

impl Drop for InflightEntry<'_> {
    fn drop(&mut self) {
        if let Some(lock) = self.lock.take() {
            self.cache.drop_lock_if_idle(self.fingerprint, lock);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::auth::test_support::{cache_for, credential, MockControlPlane, KEY_ID};

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
            let config = crate::auth::AuthConfig {
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

    /// The census the scrape republishes: the grace rate says the cliff is
    /// coming, the count says how wide it is, and the minimum names when the
    /// first hard refusal lands (OB-9).
    #[tokio::test]
    async fn the_grace_census_names_the_first_hard_refusal() {
        let cp = MockControlPlane::spawn().await;
        let cache = cache_for(&cp).await;
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
                    refresh_after,
                    expires_at,
                },
            );
        }

        assert_eq!(cache.grace_census(NOW), (2, 500));
        // Past its cliff a grant stops counting; the fresh one has aged into
        // grace by then and names the next refusal.
        assert_eq!(cache.grace_census(NOW + 600), (2, 100));
        // Nothing in grace reads as zero, not as a stale minimum.
        assert_eq!(cache.grace_census(NOW + 900), (0, 0));
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
            refresh_after: NOW,
            expires_at: NOW + 900,
        });

        for refusal in [Resolved::Unavailable, Resolved::Saturated] {
            let graced = cache.or_grace(refusal, Some(held.clone()), NOW + 1);
            assert_eq!(granted(&graced).key_id, KEY_ID);
        }

        // With nothing to fall back on the refusal stands, and stays retryable
        // rather than becoming a verdict about the credential.
        let bare = cache.or_grace(Resolved::Unavailable, None, NOW + 1);
        assert!(matches!(bare, Resolved::Unavailable), "got {bare:?}");

        // The expiry is judged as of the admission, not the arrival: a lock
        // wait plus a failed exchange can outlast the grant, and past
        // `expires_at` the refusal stands however live the grant looked when
        // the request came in (REQ-54).
        let served = cache.or_grace(Resolved::Unavailable, Some(held.clone()), NOW + 899);
        assert_eq!(granted(&served).key_id, KEY_ID);
        let expired = cache.or_grace(Resolved::Unavailable, Some(held), NOW + 900);
        assert!(matches!(expired, Resolved::Unavailable), "got {expired:?}");
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
            cache.inflight.lock().unwrap().len(),
            0,
            "a losing renewal orphaned its lock entry"
        );
    }

    /// REQ-54's grace, and its end. The control plane stops answering: a grant
    /// past `refresh_after` keeps serving, and the same grant past `expires_at`
    /// does not — retryably, and never as a claim about the credential.
    #[tokio::test]
    async fn an_outage_is_survived_to_the_hard_expiry_and_no_further() {
        let cp = MockControlPlane::spawn().await;
        cp.grant(KEY_ID, None, NOW + 300, NOW + 900);
        let cache = cache_for(&cp).await;
        cache.resolve(&credential(), NOW).await;

        cp.stop();

        let stale = cache.resolve(&credential(), NOW + 400).await;
        assert_eq!(
            granted(&stale).key_id,
            KEY_ID,
            "a grant past refresh_after still serves while renewal fails"
        );

        let expired = cache.resolve(&credential(), NOW + 901).await;
        assert!(
            matches!(expired, Resolved::Unavailable),
            "past the hard expiry the outage is a dependency failure, got {expired:?}"
        );
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
                    refresh_after: NOW + 300,
                    expires_at: NOW + 900,
                },
            );
        }

        assert_eq!(cache.grants.lock().unwrap().len(), capacity);
        assert!(
            cache.usable_grant("fingerprint-0", NOW).is_none(),
            "the least recently used entry goes first"
        );
        assert!(cache
            .usable_grant(&format!("fingerprint-{}", capacity + 9), NOW)
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
            cache.inflight.lock().unwrap().is_empty(),
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

        assert!(cache.inflight.lock().unwrap().is_empty());
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
