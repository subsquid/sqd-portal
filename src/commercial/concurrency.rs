use std::{fmt, sync::Arc};

use dashmap::DashMap;
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
        if permits == 0 {
            return None;
        }
        let semaphore = self.semaphore(account_id, permits);
        semaphore
            .try_acquire_owned()
            .ok()
            .map(|permit| ConcurrencyPermit {
                _permit: Arc::new(permit),
            })
    }

    fn semaphore(&self, account_id: &str, permits: u64) -> Arc<Semaphore> {
        if let Some(existing) = self.semaphores.get(account_id) {
            if existing.permits == permits {
                return existing.semaphore.clone();
            }
        }
        let replacement = Arc::new(LimitSemaphore {
            permits,
            semaphore: Arc::new(Semaphore::new(permits as usize)),
        });
        self.semaphores
            .insert(account_id.to_string(), replacement.clone());
        replacement.semaphore.clone()
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
    fn changed_limit_rebuilds_future_semaphore() {
        let limiter = ConcurrencyLimiter::new(1);
        let _first = limiter.try_acquire("account", 1).unwrap();
        let _second = limiter.try_acquire("account", 3).unwrap();
        let _third = limiter.try_acquire("account", 3).unwrap();
        let _fourth = limiter.try_acquire("account", 3).unwrap();
        let _fifth = limiter.try_acquire("account", 3).unwrap();
        assert!(limiter.try_acquire("account", 3).is_none());
    }
}
