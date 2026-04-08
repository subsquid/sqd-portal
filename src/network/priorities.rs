use serde::{Deserialize, Serialize};
use std::{collections::HashMap, time::Duration};
use tokio::time::Instant;

use sqd_contract_client::PeerId;

use crate::metrics;

pub type Priority = (PriorityGroup, u8, i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum PriorityGroup {
    Best = 0,
    Backoff = 1,
    Unavailable = 2,
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
    pub window_errors_secs: u32,
    pub window_timeouts_secs: u32,
}

impl Default for PrioritiesConfig {
    fn default() -> Self {
        Self {
            max_queries_per_worker: 1,
            window_errors_secs: 30,
            // Timeouts are especially painful because they cause a 60s delay.
            window_timeouts_secs: 300,
        }
    }
}

#[derive(Debug, Clone)]
struct WorkerStats {
    running_queries: u8,
    paused_until: Option<Instant>,
    last_throughput: Option<f64>,
    server_errors: Cooldown,
    timeouts: Cooldown,
}

impl WorkerStats {
    fn new(config: &PrioritiesConfig) -> Self {
        Self {
            running_queries: 0,
            paused_until: None,
            last_throughput: None,
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

    pub fn pick(&mut self, workers: impl IntoIterator<Item = PeerId>) -> Result<PeerId, NoWorker> {
        let now = Instant::now();
        let (worker, best_priority) = workers
            .into_iter()
            .map(|peer_id| {
                let priority = self
                    .workers
                    .get(&peer_id)
                    .map_or_else(Self::default_priority, |stats| self.priority(stats, now));
                (peer_id, priority)
            })
            .min_by_key(|&(_, priority)| priority)
            .ok_or(NoWorker::AllUnavailable)?;

        tracing::trace!(
            "Picked worker {:?} with priority {:?}",
            worker,
            best_priority
        );
        metrics::report_worker_picked(&worker, &format!("{:?}", best_priority.0));

        match best_priority.0 {
            PriorityGroup::Unavailable => Err(NoWorker::AllUnavailable),
            PriorityGroup::Backoff => {
                let until = self
                    .workers
                    .get(&worker)
                    .and_then(|s| s.paused_until)
                    .unwrap_or_else(Instant::now);
                Err(NoWorker::Backoff(until))
            }
            PriorityGroup::Best => {
                self.lease(worker);
                Ok(worker)
            }
        }
    }

    pub fn get_priorities(
        &self,
        workers: impl IntoIterator<Item = PeerId>,
    ) -> Vec<(PeerId, Priority)> {
        let now = Instant::now();
        let default_priority = Self::default_priority();
        let mut v: Vec<_> = workers
            .into_iter()
            .map(|peer_id| {
                (
                    peer_id,
                    self.workers
                        .get(&peer_id)
                        .map_or(default_priority, |stats| self.priority(stats, now)),
                )
            })
            .collect();
        v.sort();
        v
    }

    pub fn lease(&mut self, worker: PeerId) {
        self.modify(worker, |stats| {
            stats.running_queries += 1;
        });
    }

    pub fn unlease(&mut self, worker: PeerId) {
        self.modify(worker, |stats| {
            stats.running_queries -= 1;
        });
    }

    pub fn success(&mut self, worker: PeerId, throughput: Option<f64>) {
        self.modify(worker, |stats| {
            if let Some(t) = throughput {
                stats.last_throughput = Some(t);
            }
        });
    }

    // Query error has been returned from the worker
    pub fn error(&mut self, worker: PeerId) {
        self.modify(worker, |stats| {
            stats.server_errors.observe(Instant::now());
        });
    }

    // Query could not be processed, e.g. because the worker couldn't be reached
    pub fn failure(&mut self, worker: PeerId) {
        self.modify(worker, |stats| {
            stats.timeouts.observe(Instant::now());
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
    fn priority(&self, worker: &WorkerStats, now: Instant) -> Priority {
        if let Some(paused_until) = worker.paused_until {
            if now < paused_until {
                // shorter remaining backoff = lower key = picked first among Backoff workers
                let remaining_ms = (paused_until - now).as_millis() as i64;
                return (PriorityGroup::Backoff, worker.running_queries, remaining_ms);
            }
        }
        if worker.server_errors.observed(now)
            || worker.timeouts.observed(now)
            || worker.running_queries >= self.config.max_queries_per_worker
        {
            return (PriorityGroup::Unavailable, worker.running_queries, 0);
        }
        // Higher throughput = more negative key = picked first among Best workers.
        // None = no data yet = -inf, so unknown workers rank above any measured worker.
        let throughput_key = match worker.last_throughput {
            None => i64::MIN,
            Some(t) => -(t as i64),
        };
        (PriorityGroup::Best, worker.running_queries, throughput_key)
    }

    fn default_priority() -> Priority {
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
            .is_some_and(|last| (now - last).as_secs() < self.seconds as u64)
    }
}
