use rand::seq::IndexedRandom;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, time::Duration};
use tokio::time::Instant;

use sqd_contract_client::PeerId;

use crate::metrics;

pub type Priority = (PriorityGroup, u8, i64);

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

#[derive(Debug, Clone)]
pub struct WorkersPool {
    config: PrioritiesConfig,
    workers: HashMap<PeerId, WorkerStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PrioritiesConfig {
    pub max_queries_per_worker: u8,
    pub window_ok_secs: u32,
    pub window_slow_secs: u32,
    pub window_errors_secs: u32,
    pub window_timeouts_secs: u32,
}

impl Default for PrioritiesConfig {
    fn default() -> Self {
        Self {
            max_queries_per_worker: 1,
            window_ok_secs: 3,
            window_slow_secs: 3,
            window_errors_secs: 30,
            // Timeouts are especially painful because they cause a 60s delay.
            window_timeouts_secs: 300,
        }
    }
}

#[derive(Debug, Clone)]
struct WorkerStats {
    last_query: Instant,
    running_queries: u8,
    paused_until: Option<Instant>,
    ok: EventCounter,
    slow: EventCounter,
    server_errors: Cooldown,
    timeouts: Cooldown,
}

impl WorkerStats {
    fn new(config: &PrioritiesConfig) -> Self {
        let now = Instant::now();
        Self {
            last_query: now,
            running_queries: 0,
            paused_until: None,
            ok: EventCounter::new(config.window_ok_secs, now),
            slow: EventCounter::new(config.window_slow_secs, now),
            server_errors: Cooldown::new(config.window_errors_secs),
            timeouts: Cooldown::new(config.window_timeouts_secs),
        }
    }
}

impl WorkersPool {
    pub fn new(config: PrioritiesConfig) -> Self {
        Self {
            config,
            workers: HashMap::new(),
        }
    }

    pub fn pick(
        &mut self,
        workers: impl IntoIterator<Item = PeerId>,
        lease: bool,
    ) -> Result<PeerId, NoWorker> {
        let now = Instant::now();
        let mut unknown_workers = Vec::new();
        let best_known = workers
            .into_iter()
            .filter_map(|peer_id| match self.workers.get(&peer_id) {
                None => {
                    unknown_workers.push(peer_id);
                    None
                }
                Some(stats) => Some((peer_id, self.priority(stats, now))),
            })
            .min_by_key(|&(_, priority)| priority);

        let (worker, best_priority) = unknown_workers
            // prefer new workers, randomly choosing one
            .choose(&mut rand::rng())
            .map(|peer_id| (*peer_id, (PriorityGroup::Best, 0, Instant::now())))
            .or(best_known)
            .ok_or(NoWorker::AllUnavailable)?;

        tracing::trace!(
            "Picked worker {:?} with priority {:?}",
            worker,
            best_priority
        );
        metrics::report_worker_picked(&worker, &format!("{:?}", best_priority.0));

        match best_priority.0 {
            PriorityGroup::Unavailable => Err(NoWorker::AllUnavailable),
            PriorityGroup::Backoff => Err(NoWorker::Backoff(best_priority.2)),
            _ => {
                if lease {
                    self.lease(worker);
                }
                Ok(worker)
            }
        }
    }

    pub fn get_priorities(
        &self,
        workers: impl IntoIterator<Item = PeerId>,
    ) -> Vec<(PeerId, Priority)> {
        let now = Instant::now();
        let default_priority = Self::default_serializable_priority();
        let mut v: Vec<_> = workers
            .into_iter()
            .map(|peer_id| {
                (
                    peer_id,
                    self.workers
                        .get(&peer_id)
                        .map_or(default_priority, |stats| {
                            self.serializable_priority(stats, now)
                        }),
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
            stats.paused_until = stats.paused_until.max(Some(Instant::now() + backoff));
        });
    }

    pub fn reset_allocations(&mut self) {}

    fn modify(&mut self, worker: PeerId, f: impl FnOnce(&mut WorkerStats)) {
        f(self
            .workers
            .entry(worker)
            .or_insert_with(|| WorkerStats::new(&self.config)));
    }

    // Less is better
    fn priority(&self, worker: &WorkerStats, now: Instant) -> (PriorityGroup, u8, Instant) {
        if let Some(paused_until) = worker.paused_until {
            if now < paused_until {
                return (PriorityGroup::Backoff, worker.running_queries, paused_until);
            }
        }
        let penalty = if worker.server_errors.observed(now)
            || worker.timeouts.observed(now)
            || worker.running_queries >= self.config.max_queries_per_worker
        {
            PriorityGroup::Unavailable
        } else if worker.slow.estimate(now) > worker.ok.estimate(now) {
            PriorityGroup::Slow
        } else {
            PriorityGroup::Best
        };
        (penalty, worker.running_queries, worker.last_query)
    }

    fn serializable_priority(&self, worker: &WorkerStats, now: Instant) -> Priority {
        let (group, queries, time) = self.priority(worker, now);
        (group, queries, -((now - time).as_secs() as i64))
    }

    fn default_serializable_priority() -> Priority {
        (PriorityGroup::Best, 0, 0)
    }
}

#[derive(Debug, Clone)]
struct Cooldown {
    seconds: u32,
    last_observed: Option<Instant>,
}

impl Cooldown {
    fn new(seconds: u32) -> Self {
        Self {
            seconds,
            last_observed: None,
        }
    }

    fn observe(&mut self, now: Instant) {
        self.last_observed = Some(now);
    }

    fn observed(&self, now: Instant) -> bool {
        self.last_observed
            .map_or(false, |last| (now - last).as_secs() < self.seconds as u64)
    }
}

/// A counter for the approximate number of events in the last `window` with constant memory usage.
/// If `estimate` returned `n`, then the number of events observed in
/// the last `2 * window` seconds is not more than `2 * n`.
#[derive(Debug, Clone)]
struct EventCounter {
    window: u32,
    last_time: Instant,
    count: u32,
    prev_count: u32,
}

impl EventCounter {
    fn new(window: u32, now: Instant) -> Self {
        Self {
            window,
            last_time: now,
            count: 0,
            prev_count: 0,
        }
    }

    fn observe(&mut self, now: Instant) {
        if (now - self.last_time).as_secs() > 2 * self.window as u64 {
            self.prev_count = 0;
            self.count = 0;
            self.last_time = now;
        } else if (now - self.last_time).as_secs() > self.window as u64 {
            self.prev_count = self.count;
            self.count = 0;
            self.last_time = now;
        }
        self.count += 1;
    }

    fn estimate(&self, now: Instant) -> u32 {
        if (now - self.last_time).as_secs() > 2 * self.window as u64 {
            0
        } else if (now - self.last_time).as_secs() > self.window as u64 {
            self.count
        } else {
            std::cmp::max(self.prev_count, self.count)
        }
    }
}
