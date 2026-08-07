//! The only authorization state a replica holds: grants it was handed for
//! credentials it has actually served (DEF-18).
//!
//! Keyed on a fingerprint of the *whole* credential, never on the key id — an
//! entry reachable by id alone would admit the next caller to name that id
//! without proving it holds the secret. Nothing fills it in the background;
//! there is no bootstrap and no feed, so a cold replica is not a degraded one.

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
    config::Limits,
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
    /// When to renew. A request arriving past this is still served.
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
    /// The control plane's denial reason, verbatim.
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

pub struct GrantCache {
    client: ControlPlaneClient,
    limits: Limits,
    grants: Mutex<LruCache<String, Arc<CachedGrant>>>,
    denials: Mutex<LruCache<String, Denial>>,
    /// One exchange in flight per fingerprint. Without it a burst on one
    /// uncached credential is one control-plane call per request.
    inflight: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    permits: Semaphore,
    limiter: Mutex<RateLimiter>,
    /// Wall-clock second of the last exchange the control plane answered. The
    /// gauge derived from it climbs through an outage, which is the operator's
    /// distance to the `expires_at` cliff (OB-9, OB-13).
    last_success: AtomicU64,
}

impl GrantCache {
    pub fn new(client: ControlPlaneClient, limits: Limits) -> Arc<Self> {
        let cache = Arc::new(Self {
            client,
            grants: Mutex::new(LruCache::new(nonzero(limits.grant_cache_capacity))),
            denials: Mutex::new(LruCache::new(nonzero(limits.denial_cache_capacity))),
            inflight: Mutex::new(HashMap::new()),
            permits: Semaphore::new(limits.max_inflight_exchanges),
            limiter: Mutex::new(RateLimiter::new(limits.exchange_rate_per_sec)),
            last_success: AtomicU64::new(now_secs()),
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
            metrics::report_grace_admission();
            self.spawn_refresh(credential.clone(), now);
            return Resolved::Grant(grant);
        }
        if let Some(reason) = self.live_denial(&credential.fingerprint, now) {
            return Resolved::Denied(reason);
        }

        let lock = self.lock_for(&credential.fingerprint);
        let guard = lock.clone().lock_owned().await;
        let resolved = self.resolve_exclusively(credential, now).await;
        drop(guard);
        self.drop_lock_if_idle(&credential.fingerprint, lock);
        resolved
    }

    /// Runs under the fingerprint's lock. Whoever held it before has answered by
    /// now, so this re-checks before paying for a second call: that is what
    /// makes a burst on one credential cost one exchange (INV-14).
    async fn resolve_exclusively(&self, credential: &Credential, now: u64) -> Resolved {
        if let Some(grant) = self.usable_grant(&credential.fingerprint, now) {
            if grant.refresh_after > now {
                return Resolved::Grant(grant);
            }
        }
        if let Some(reason) = self.live_denial(&credential.fingerprint, now) {
            return Resolved::Denied(reason);
        }
        self.exchange(credential, now).await
    }

    /// The renewal a request past `refresh_after` triggers without waiting for
    /// it. Skipped outright when one is already running for this fingerprint.
    fn spawn_refresh(self: &Arc<Self>, credential: Credential, now: u64) {
        let cache = self.clone();
        tokio::spawn(async move {
            let lock = cache.lock_for(&credential.fingerprint);
            let Ok(guard) = lock.clone().try_lock_owned() else {
                return;
            };
            // Another refresh may have landed between the request's read and
            // this task acquiring the lock.
            if cache
                .usable_grant(&credential.fingerprint, now)
                .is_some_and(|grant| grant.refresh_after > now)
            {
                drop(guard);
                cache.drop_lock_if_idle(&credential.fingerprint, lock);
                return;
            }
            cache.exchange(&credential, now).await;
            drop(guard);
            cache.drop_lock_if_idle(&credential.fingerprint, lock);
        });
    }

    /// One call to the authority, under the budgets that keep an unauthenticated
    /// flood from becoming a cost amplifier pointed at it (HZ-10).
    async fn exchange(&self, credential: &Credential, now: u64) -> Resolved {
        let Ok(_permit) = self.permits.try_acquire() else {
            metrics::report_exchange(ExchangeOutcome::Saturated, None);
            tracing::warn!(
                key_id = credential.key_id,
                outcome = "over_inflight_cap",
                "commercial exchange skipped: too many in flight"
            );
            return Resolved::Saturated;
        };
        if !self.limiter.lock().unwrap().take() {
            metrics::report_exchange(ExchangeOutcome::Saturated, None);
            tracing::warn!(
                key_id = credential.key_id,
                outcome = "rate_limited",
                "commercial exchange skipped: rate limit"
            );
            return Resolved::Saturated;
        }

        let started = Instant::now();
        let answer = self.client.exchange(credential, now).await;
        let elapsed = started.elapsed();

        match answer {
            Ok(Exchanged::Granted(grant)) => {
                self.last_success.store(now_secs(), Ordering::Release);
                metrics::report_exchange(ExchangeOutcome::Granted, Some(elapsed));
                let cached = self.store(&credential.fingerprint, grant, now);
                Resolved::Grant(cached)
            }
            Ok(Exchanged::Denied(reason)) => {
                self.last_success.store(now_secs(), Ordering::Release);
                metrics::report_exchange(ExchangeOutcome::Denied, Some(elapsed));
                // A denial outranks whatever lifetime the grant it replaces had
                // left: the authority has spoken since (INV-6).
                self.grants.lock().unwrap().pop(&credential.fingerprint);
                self.remember_denial(&credential.fingerprint, reason.clone(), now);
                self.publish_gauges();
                Resolved::Denied(reason)
            }
            Err(err) => {
                metrics::report_exchange(ExchangeOutcome::Failed, Some(elapsed));
                tracing::warn!(
                    key_id = credential.key_id,
                    outcome = "failed",
                    error = %err,
                    "commercial exchange failed"
                );
                Resolved::Unavailable
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
            metrics::report_lifetime_capped();
            tracing::warn!(
                key_id = grant.key_id,
                offered = grant.expires_at.saturating_sub(now),
                cap = self.limits.max_grant_lifetime_secs,
                "commercial grant lifetime capped"
            );
        }
        // Clamped before the jitter is sized, so the spread is a fraction of the
        // window actually served rather than of whatever was offered. Early
        // rather than late: jitter must spread a cohort's renewals, never extend
        // how long any one of them serves unrenewed (HZ-12).
        let renew_at = grant.refresh_after.min(expires_at).max(now);
        let refresh_after = renew_at.saturating_sub(self.jitter(renew_at - now));

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
            metrics::report_grant_eviction();
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
        None
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

    /// Keeps the keyed-lock map from growing with every credential ever seen.
    /// Called only once the guard is dropped: under the map lock no clone can be
    /// taken, so a count of two — the map's and the caller's — means nobody else
    /// is waiting on it.
    fn drop_lock_if_idle(&self, fingerprint: &str, lock: Arc<tokio::sync::Mutex<()>>) {
        let mut inflight = self.inflight.lock().unwrap();
        if Arc::strong_count(&lock) <= 2 {
            inflight.remove(fingerprint);
        }
    }

    fn publish_gauges(&self) {
        metrics::report_grant_cache_size(self.grants.lock().unwrap().len());
    }

    /// Seconds since the control plane last answered anything. Republished on
    /// scrape rather than on exchange, so it climbs through an outage instead of
    /// freezing at the last value (OB-13).
    pub fn last_exchange_success_age(&self) -> u64 {
        now_secs().saturating_sub(self.last_success.load(Ordering::Acquire))
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
    use crate::commercial::test_support::{cache_for, credential, MockControlPlane, KEY_ID};

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
}
