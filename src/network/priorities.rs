use serde::Serialize;
use std::{collections::HashMap, time::Duration};
use tokio::time::Instant;

use sqd_contract_client::PeerId;

pub type Priority = (PriorityGroup, u8, i64);

const MAX_QUERIES_PER_WORKER: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum PriorityGroup {
    Best = 0,
    Slow = 1,
    Backoff = 2,
    Unavailable = 3,
}

#[derive(Debug)]
pub enum NoWorker {
    AllUnavailable,
    Backoff(Instant),
}

#[derive(Default, Debug, Clone)]
pub struct WorkersPool {
    workers: HashMap<PeerId, WorkerStats>,
}

#[derive(Debug, Clone)]
struct WorkerStats {
    last_query: Instant,
    running_queries: u8,
    paused_until: Option<Instant>,
    ok: EventCounter<3>,
    slow: EventCounter<3>,
    server_errors: Cooldown<30>,
    // Timeouts are especially painful because they cause a 60s delay.
    timeouts: Cooldown<300>,
}

impl Default for WorkerStats {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            last_query: now,
            running_queries: 0,
            paused_until: None,
            ok: EventCounter::new(now),
            slow: EventCounter::new(now),
            server_errors: Cooldown::default(),
            timeouts: Cooldown::default(),
        }
    }
}

// Less is better
fn priority(worker: &WorkerStats, now: Instant) -> (PriorityGroup, u8, Instant) {
    if let Some(paused_until) = worker.paused_until {
        if now < paused_until {
            return (PriorityGroup::Backoff, worker.running_queries, paused_until);
        }
    }
    let penalty = if worker.server_errors.observed(now)
        || worker.timeouts.observed(now)
        || worker.running_queries >= MAX_QUERIES_PER_WORKER
    {
        PriorityGroup::Unavailable
    } else if worker.slow.estimate(now) > worker.ok.estimate(now) {
        PriorityGroup::Slow
    } else {
        PriorityGroup::Best
    };
    (penalty, worker.running_queries, worker.last_query)
}

fn serializable_priority(worker: &WorkerStats, now: Instant) -> Priority {
    let (group, queries, time) = priority(worker, now);
    (group, queries, -((now - time).as_secs() as i64))
}

impl WorkersPool {
    pub fn pick(
        &mut self,
        workers: impl IntoIterator<Item = PeerId>,
        lease: bool,
    ) -> Result<PeerId, NoWorker> {
        let now = Instant::now();
        let default_priority = WorkerStats::default();
        let (best, best_priority) = workers
            .into_iter()
            .map(|peer_id| {
                (
                    peer_id,
                    priority(self.workers.get(&peer_id).unwrap_or(&default_priority), now),
                )
            })
            .min_by_key(|&(_, priority)| priority)
            .ok_or(NoWorker::AllUnavailable)?;
        tracing::trace!("Picked worker {:?} with priority {:?}", best, best_priority);
        let worker = match best_priority.0 {
            PriorityGroup::Unavailable => return Err(NoWorker::AllUnavailable),
            PriorityGroup::Backoff => return Err(NoWorker::Backoff(best_priority.2)),
            _ => best,
        };

        if lease {
            self.lease(worker);
        }
        Ok(worker)
    }

    pub fn get_priorities(
        &self,
        workers: impl IntoIterator<Item = PeerId>,
    ) -> Vec<(PeerId, Priority)> {
        let now = Instant::now();
        let default_priority = WorkerStats::default();
        let mut v: Vec<_> = workers
            .into_iter()
            .map(|peer_id| {
                (
                    peer_id,
                    serializable_priority(
                        self.workers.get(&peer_id).unwrap_or(&default_priority),
                        now,
                    ),
                )
            })
            .collect();
        v.sort();
        v
    }

    pub fn lease(&mut self, worker: PeerId) {
        self.modify(worker, |stats| {
            stats.running_queries += 1;
            stats.last_query = Instant::now();
        });
    }

    pub fn success(&mut self, worker: PeerId) {
        self.modify(worker, |stats| {
            stats.running_queries -= 1;
            stats.ok.observe(Instant::now());
        });
    }

    // Query error has been returned from the worker
    pub fn error(&mut self, worker: PeerId) {
        self.modify(worker, |stats| {
            stats.running_queries -= 1;
            stats.server_errors.observe(Instant::now());
        });
    }

    // Query could not be processed, e.g. because the worker couldn't be reached
    pub fn failure(&mut self, worker: PeerId) {
        self.modify(worker, |stats| {
            stats.running_queries -= 1;
            stats.timeouts.observe(Instant::now());
        });
    }

    pub fn outrun(&mut self, worker: PeerId) {
        self.modify(worker, |stats| {
            stats.running_queries -= 1;
            stats.slow.observe(Instant::now());
        });
    }

    pub fn hint_backoff(&mut self, worker: PeerId, backoff: Duration) {
        self.modify(worker, |stats| {
            stats.paused_until = Some(Instant::now() + backoff);
        });
    }

    pub fn reset_allocations(&mut self) {}

    fn modify(&mut self, worker: PeerId, f: impl FnOnce(&mut WorkerStats)) {
        f(self.workers.entry(worker).or_default());
    }
}

#[derive(Default, Debug, Clone)]
struct Cooldown<const S: u64> {
    last_observed: Option<Instant>,
}

impl<const S: u64> Cooldown<S> {
    fn observe(&mut self, now: Instant) {
        self.last_observed = Some(now);
    }

    fn observed(&self, now: Instant) -> bool {
        self.last_observed
            .map_or(false, |last| (now - last).as_secs() < S)
    }
}

/// A counter for the approximate number of events in the last `window` with constant memory usage.
/// If `estimate` returned `n`, then the number of events observed in
/// the last `2 * S` seconds is not more than `2 * n`.
#[derive(Debug, Clone)]
struct EventCounter<const S: u64> {
    last_time: Instant,
    count: u32,
    prev_count: u32,
}

impl<const S: u64> EventCounter<S> {
    fn new(now: Instant) -> Self {
        Self {
            last_time: now,
            count: 0,
            prev_count: 0,
        }
    }

    fn observe(&mut self, now: Instant) {
        if (now - self.last_time).as_secs() > 2 * S {
            self.prev_count = 0;
            self.count = 0;
            self.last_time = now;
        } else if (now - self.last_time).as_secs() > S {
            self.prev_count = self.count;
            self.count = 0;
            self.last_time = now;
        }
        self.count += 1;
    }

    fn estimate(&self, now: Instant) -> u32 {
        if (now - self.last_time).as_secs() > 2 * S {
            0
        } else if (now - self.last_time).as_secs() > S {
            self.count
        } else {
            std::cmp::max(self.prev_count, self.count)
        }
    }
}
