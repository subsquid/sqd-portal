use std::{
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use dashmap::{mapref::entry::Entry, DashMap};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug)]
pub struct ConcurrencyLimiter {
    pod_count: usize,
    semaphores: DashMap<String, Arc<LimitSemaphore>>,
}

#[derive(Debug)]
struct LimitSemaphore {
    permits: u64,
    semaphore: Arc<Semaphore>,
    last_touched: Mutex<Instant>,
}

#[derive(Clone)]
pub struct ConcurrencyPermit {
    _permit: Arc<OwnedSemaphorePermit>,
}

impl ConcurrencyLimiter {
    pub fn new(pod_count: usize) -> Self {
        Self {
            pod_count: pod_count.max(1),
            semaphores: DashMap::new(),
        }
    }

    pub fn try_acquire(&self, account_id: &str, global_limit: u64) -> Option<ConcurrencyPermit> {
        let permits = per_pod_permits(global_limit, self.pod_count);
        let semaphore = self.semaphore(account_id, permits);
        semaphore
            .try_acquire_owned()
            .ok()
            .map(|permit| ConcurrencyPermit {
                _permit: Arc::new(permit),
            })
    }

    fn semaphore(&self, account_id: &str, permits: u64) -> Arc<Semaphore> {
        match self.semaphores.entry(account_id.to_string()) {
            Entry::Occupied(mut entry) => {
                let existing = entry.get();
                existing.touch();
                if existing.permits == permits {
                    return existing.semaphore.clone();
                }
                if !existing.all_permits_available() {
                    tracing::debug!(
                        account_id,
                        old_permits = existing.permits,
                        new_permits = permits,
                        "commercial concurrency limit change deferred until active permits drain"
                    );
                    return existing.semaphore.clone();
                }
                let replacement = Arc::new(LimitSemaphore::new(permits));
                entry.insert(replacement.clone());
                replacement.semaphore.clone()
            }
            Entry::Vacant(entry) => {
                let inserted = entry.insert(Arc::new(LimitSemaphore::new(permits)));
                inserted.semaphore.clone()
            }
        }
    }

    pub fn sweep_idle(&self, horizon: Duration) -> usize {
        let now = Instant::now();
        let mut removed = 0;
        self.semaphores.retain(|_, entry| {
            let evict = entry.is_idle(now, horizon);
            if evict {
                removed += 1;
            }
            !evict
        });
        removed
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.semaphores.len()
    }
}

impl LimitSemaphore {
    fn new(permits: u64) -> Self {
        Self {
            permits,
            semaphore: Arc::new(Semaphore::new(permits as usize)),
            last_touched: Mutex::new(Instant::now()),
        }
    }

    fn touch(&self) {
        *self.last_touched.lock().unwrap() = Instant::now();
    }

    fn all_permits_available(&self) -> bool {
        self.semaphore.available_permits() == self.permits as usize
    }

    fn is_idle(&self, now: Instant, horizon: Duration) -> bool {
        self.all_permits_available()
            && now.duration_since(*self.last_touched.lock().unwrap()) >= horizon
    }
}

fn per_pod_permits(global_limit: u64, pod_count: usize) -> u64 {
    global_limit.div_ceil(pod_count as u64).saturating_add(1)
}

impl fmt::Debug for ConcurrencyPermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ConcurrencyPermit(<held>)")
    }
}

impl PartialEq for ConcurrencyPermit {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for ConcurrencyPermit {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Barrier, thread};

    #[test]
    fn per_pod_limit_rounds_up_and_adds_guardrail() {
        assert_eq!(per_pod_permits(4, 4), 2);
        assert_eq!(per_pod_permits(5, 4), 3);
    }

    #[test]
    fn permit_exhaustion_and_release() {
        let limiter = ConcurrencyLimiter::new(4);
        let first = limiter.try_acquire("account", 4).unwrap();
        let second = limiter.try_acquire("account", 4).unwrap();
        assert!(limiter.try_acquire("account", 4).is_none());

        drop(first);
        assert!(limiter.try_acquire("account", 4).is_some());
        drop(second);
    }

    #[test]
    fn concurrent_first_access_shares_one_semaphore() {
        const ATTEMPTS: usize = 64;
        let limiter = Arc::new(ConcurrencyLimiter::new(1));
        let barrier = Arc::new(Barrier::new(ATTEMPTS));
        let handles = (0..ATTEMPTS)
            .map(|_| {
                let limiter = limiter.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    limiter.try_acquire("account", 1)
                })
            })
            .collect::<Vec<_>>();

        let permits = handles
            .into_iter()
            .filter_map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();

        assert!(
            permits.len() <= per_pod_permits(1, 1) as usize,
            "got {} permits from one account semaphore",
            permits.len()
        );
    }

    #[test]
    fn changed_limit_rebuilds_after_active_permits_drain() {
        let limiter = ConcurrencyLimiter::new(1);
        let first = limiter.try_acquire("account", 1).unwrap();
        let second = limiter.try_acquire("account", 3).unwrap();
        assert!(limiter.try_acquire("account", 3).is_none());

        drop(first);
        drop(second);

        let _first = limiter.try_acquire("account", 3).unwrap();
        let _second = limiter.try_acquire("account", 3).unwrap();
        let _third = limiter.try_acquire("account", 3).unwrap();
        let _fourth = limiter.try_acquire("account", 3).unwrap();
        assert!(limiter.try_acquire("account", 3).is_none());
    }

    #[test]
    fn sweep_evicts_idle_entries_after_horizon() {
        let limiter = ConcurrencyLimiter::new(1);
        let permit = limiter.try_acquire("anon:bucket", 1).unwrap();
        drop(permit);

        assert_eq!(limiter.len(), 1);
        assert_eq!(limiter.sweep_idle(Duration::from_secs(60)), 0);
        assert_eq!(limiter.len(), 1);
        assert_eq!(limiter.sweep_idle(Duration::ZERO), 1);
        assert_eq!(limiter.len(), 0);
    }

    #[test]
    fn sweep_keeps_entries_with_held_permits() {
        let limiter = ConcurrencyLimiter::new(1);
        let permit = limiter.try_acquire("anon:bucket", 1).unwrap();

        assert_eq!(limiter.sweep_idle(Duration::ZERO), 0);
        assert_eq!(limiter.len(), 1);

        drop(permit);
        assert_eq!(limiter.sweep_idle(Duration::ZERO), 1);
        assert_eq!(limiter.len(), 0);
    }
}
