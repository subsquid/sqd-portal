use std::collections::HashMap;
use tokio::time::Instant;

use sqd_contract_client::PeerId;

#[derive(Default)]
pub struct WorkersPool {
    workers: HashMap<PeerId, WorkerStats>,
}

struct WorkerStats {
    last_query: Instant,
    running_queries: u8,
    no_allocation: bool,
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
            no_allocation: false,
            ok: EventCounter::new(now),
            slow: EventCounter::new(now),
            server_errors: Default::default(),
            timeouts: Default::default(),
        }
    }
}

// Less is better
fn priority(worker: &WorkerStats, now: Instant) -> (u8, u8, Instant) {
    let penalty = if worker.no_allocation
        || worker.server_errors.observed(now)
        || worker.timeouts.observed(now)
    {
        2
    } else if worker.slow.estimate(now) > worker.ok.estimate(now) {
        1
    } else {
        0
    };
    (penalty, worker.running_queries, worker.last_query)
}

impl WorkersPool {
    pub fn pick(&mut self, workers: impl IntoIterator<Item = PeerId>) -> Option<PeerId> {
        let now = Instant::now();
        let default_priority = Default::default();
        let (best, best_priority) = workers
            .into_iter()
            .map(|peer_id| {
                (
                    peer_id,
                    priority(self.workers.get(&peer_id).unwrap_or(&default_priority), now),
                )
            })
            .min_by_key(|&(_, priority)| priority)?;
        tracing::trace!("Picked worker {:?} with priority {:?}", best, best_priority);
        (best_priority.0 < 2).then_some(best)
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

    pub fn error(&mut self, worker: PeerId) {
        self.modify(worker, |stats| {
            stats.running_queries -= 1;
            stats.server_errors.observe(Instant::now());
        });
    }

    pub fn timeout(&mut self, worker: PeerId) {
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

    pub fn unavailable(&mut self, worker: PeerId) {
        self.modify(worker, |stats| {
            stats.running_queries -= 1;
            stats.no_allocation = true;
        });
    }

    pub fn reset_allocations(&mut self) {
        for worker in self.workers.values_mut() {
            worker.no_allocation = false;
        }
    }

    fn modify(&mut self, worker: PeerId, f: impl FnOnce(&mut WorkerStats)) {
        f(self.workers.entry(worker).or_default());
    }
}

#[derive(Default)]
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
/// the last `2 * window` is not more than `2 * n`.
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
