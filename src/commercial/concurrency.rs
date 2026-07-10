use std::{
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use dashmap::{mapref::entry::Entry, DashMap};
use tokio::{
    runtime::Handle,
    sync::{Notify, OwnedSemaphorePermit, Semaphore},
};

#[derive(Debug)]
pub struct ConcurrencyLimiter {
    pod_count: usize,
    semaphores: DashMap<String, Arc<LimitSemaphore>>,
}

#[derive(Debug)]
struct LimitSemaphore {
    state: Arc<Mutex<LimitState>>,
    semaphore: Arc<Semaphore>,
    reclaim_notify: Arc<Notify>,
    last_touched: Mutex<Instant>,
}

#[derive(Debug)]
struct LimitState {
    issued: u64,
    target: u64,
    reclaimer_running: bool,
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
            Entry::Occupied(entry) => {
                let existing = entry.get();
                existing.touch();
                existing.sync_target(account_id, permits);
                existing.semaphore.clone()
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
            state: Arc::new(Mutex::new(LimitState {
                issued: permits,
                target: permits,
                reclaimer_running: false,
            })),
            semaphore: Arc::new(Semaphore::new(permits as usize)),
            reclaim_notify: Arc::new(Notify::new()),
            last_touched: Mutex::new(Instant::now()),
        }
    }

    fn touch(&self) {
        *self.last_touched.lock().unwrap() = Instant::now();
    }

    fn sync_target(&self, account_id: &str, permits: u64) {
        let mut permits_to_add = 0;
        let mut spawn_reclaimer = false;
        {
            let mut state = self.state.lock().unwrap();
            if state.target != permits {
                tracing::debug!(
                    account_id,
                    old_permits = state.target,
                    new_permits = permits,
                    issued_permits = state.issued,
                    "commercial concurrency limit changed"
                );
            }
            state.target = permits;
            if state.target > state.issued {
                permits_to_add = state.target - state.issued;
                state.issued = state.target;
            } else {
                forget_available_surplus(&self.semaphore, &mut state);
                if state.issued > state.target && !state.reclaimer_running {
                    state.reclaimer_running = true;
                    spawn_reclaimer = true;
                }
            }
        }

        if permits_to_add > 0 {
            self.semaphore.add_permits(permits_to_add as usize);
        }
        if spawn_reclaimer {
            self.spawn_reclaimer(account_id.to_string());
        } else {
            self.reclaim_notify.notify_waiters();
        }
    }

    fn spawn_reclaimer(&self, account_id: String) {
        let semaphore = self.semaphore.clone();
        let state = self.state.clone();
        let notify = self.reclaim_notify.clone();

        let Ok(handle) = Handle::try_current() else {
            state.lock().unwrap().reclaimer_running = false;
            tracing::debug!(
                account_id,
                "commercial concurrency surplus reclamation deferred without a tokio runtime"
            );
            return;
        };

        handle.spawn(async move {
            loop {
                {
                    let mut state = state.lock().unwrap();
                    forget_available_surplus(&semaphore, &mut state);
                    if state.issued <= state.target {
                        state.reclaimer_running = false;
                        return;
                    }
                }

                tokio::select! {
                    permit = semaphore.clone().acquire_owned() => {
                        let Ok(permit) = permit else {
                            state.lock().unwrap().reclaimer_running = false;
                            return;
                        };
                        let forget = {
                            let mut state = state.lock().unwrap();
                            if state.issued > state.target {
                                state.issued -= 1;
                                true
                            } else {
                                state.reclaimer_running = false;
                                false
                            }
                        };
                        if forget {
                            permit.forget();
                        } else {
                            drop(permit);
                            return;
                        }
                    }
                    _ = notify.notified() => {}
                }
            }
        });
    }

    fn all_permits_available(&self) -> bool {
        let state = self.state.lock().unwrap();
        !state.reclaimer_running
            && state.issued == state.target
            && self.semaphore.available_permits() == state.issued as usize
    }

    fn is_idle(&self, now: Instant, horizon: Duration) -> bool {
        self.all_permits_available()
            && now.duration_since(*self.last_touched.lock().unwrap()) >= horizon
    }
}

fn forget_available_surplus(semaphore: &Semaphore, state: &mut LimitState) {
    let surplus = state.issued.saturating_sub(state.target);
    if surplus == 0 {
        return;
    }
    let forgotten = semaphore.forget_permits(surplus as usize) as u64;
    state.issued -= forgotten;
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
    fn limit_increase_adds_capacity_while_permits_are_held() {
        let limiter = ConcurrencyLimiter::new(1);
        let first = limiter.try_acquire("account", 1).unwrap();
        let second = limiter.try_acquire("account", 1).unwrap();
        assert!(limiter.try_acquire("account", 1).is_none());

        let third = limiter.try_acquire("account", 3).unwrap();
        let fourth = limiter.try_acquire("account", 3).unwrap();
        assert!(limiter.try_acquire("account", 3).is_none());

        drop(first);
        drop(second);
        drop(third);
        drop(fourth);
    }

    #[tokio::test]
    async fn limit_decrease_reclaims_surplus_as_permits_finish() {
        let limiter = ConcurrencyLimiter::new(1);
        let mut permits = (0..4)
            .map(|_| limiter.try_acquire("account", 3).unwrap())
            .collect::<Vec<_>>();
        assert!(limiter.try_acquire("account", 3).is_none());

        assert!(limiter.try_acquire("account", 1).is_none());

        drop(permits.pop());
        yield_reclaimer().await;
        assert!(limiter.try_acquire("account", 1).is_none());

        drop(permits.pop());
        yield_reclaimer().await;
        assert!(limiter.try_acquire("account", 1).is_none());

        drop(permits.pop());
        yield_reclaimer().await;
        let replacement = limiter.try_acquire("account", 1).unwrap();
        assert!(limiter.try_acquire("account", 1).is_none());

        drop(replacement);
        drop(permits);
        yield_reclaimer().await;
        let first = limiter.try_acquire("account", 1).unwrap();
        let second = limiter.try_acquire("account", 1).unwrap();
        assert!(limiter.try_acquire("account", 1).is_none());
        drop(first);
        drop(second);
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

    async fn yield_reclaimer() {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }
}
