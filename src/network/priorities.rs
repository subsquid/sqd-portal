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

/// The one verdict label that means the worker actually served the query.
pub const VERDICT_OK: &str = "ok";

/// Why `pick` ranks a worker where it does, for the debug routes.
///
/// The monotonic instants behind these fields are rendered as seconds relative to
/// the reading, since a wall-clock rendering of an `Instant` would be a guess.
#[derive(Debug, Clone, Serialize)]
pub struct WorkerHealth {
    /// Every reason that ranks this worker below `Best`, in the order `priority`
    /// tests them. Empty for a worker `pick` would take right now.
    pub blocked_by: Vec<&'static str>,
    pub running_queries: u8,
    pub backoff_secs: Option<f64>,
    pub server_errors_cooldown_secs: Option<f64>,
    /// Also covers transport errors: unreachable and slow share one cooldown.
    pub timeouts_cooldown_secs: Option<f64>,
    pub last_throughput: Option<f64>,
    pub last_verdict: Option<&'static str>,
    pub last_verdict_secs_ago: Option<f64>,
    pub last_ok_secs_ago: Option<f64>,
    /// Answered at least once, never with an `ok`.
    pub never_ok: bool,
}

/// One chunk's holder set, counted the way `pick` sees it. `holders` is what the
/// assignment promises; `available` is what the portal can actually reach for the
/// next query, and the two diverge badly after a fleet-wide outage.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct HolderSummary {
    pub holders: usize,
    pub available: usize,
    pub backoff: usize,
    pub unavailable: usize,
    /// Holders that answered at least once and never with an `ok`.
    pub never_ok: usize,
    /// Holders this portal has never queried, so it has no opinion on them.
    pub unqueried: usize,
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
    /// The label of the last verdict recorded for this worker, and when. Kept only
    /// for the debug routes: a `PriorityGroup` alone cannot tell an unreachable
    /// holder from a busy one, and that distinction is the whole question when a
    /// chunk stops being servable.
    last_verdict: Option<(&'static str, Instant)>,
    last_ok: Option<Instant>,
}

impl WorkerStats {
    fn new(config: &PrioritiesConfig) -> Self {
        Self {
            running_queries: 0,
            paused_until: None,
            last_throughput: None,
            server_errors: Cooldown::new(config.window_errors_secs),
            timeouts: Cooldown::new(config.window_timeouts_secs),
            last_verdict: None,
            last_ok: None,
        }
    }

    /// Answered at least once, never with an `ok`. The signature of a holder that
    /// is in the assignment but not reachable on the network.
    fn never_ok(&self) -> bool {
        self.last_verdict.is_some() && self.last_ok.is_none()
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

    /// The debug view of a worker set, in the order `pick` would consider it.
    pub fn describe(
        &self,
        workers: impl IntoIterator<Item = PeerId>,
    ) -> Vec<(PeerId, Priority, WorkerHealth)> {
        let now = Instant::now();
        let mut v: Vec<_> = workers
            .into_iter()
            .map(|peer_id| match self.workers.get(&peer_id) {
                Some(stats) => (peer_id, self.priority(stats, now), self.health(stats, now)),
                None => (peer_id, Self::default_priority(), Self::unqueried_health()),
            })
            .collect();

        // Pick order, not peer order: which worker gets the next query is the
        // question this view exists to answer.
        v.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        v
    }

    /// The same verdicts as [`Self::describe`], counted instead of listed. Cheap
    /// enough to run over every chunk of a dataset.
    pub fn summarize(&self, workers: impl IntoIterator<Item = PeerId>) -> HolderSummary {
        let now = Instant::now();
        let mut summary = HolderSummary::default();

        for peer_id in workers {
            summary.holders += 1;

            let Some(stats) = self.workers.get(&peer_id) else {
                summary.available += 1;
                summary.unqueried += 1;
                continue;
            };

            match self.priority(stats, now).0 {
                PriorityGroup::Best => summary.available += 1,
                PriorityGroup::Backoff => summary.backoff += 1,
                PriorityGroup::Unavailable => summary.unavailable += 1,
            }
            if stats.never_ok() {
                summary.never_ok += 1;
            }
            if stats.last_verdict.is_none() {
                summary.unqueried += 1;
            }
        }

        summary
    }

    pub fn lease(&mut self, worker: PeerId) {
        self.modify(worker, |stats| {
            stats.running_queries += 1;
        });
    }

    pub fn unlease(&mut self, worker: PeerId) {
        self.modify(worker, |stats| {
            // Normally, this should never underflow because unleasing the worker
            // requires that it had been leased before. In case of a bug,
            // an underflow should happen in the release mode.
            stats.running_queries -= 1;
        });
    }

    /// The total number of currently leased query slots across all workers.
    #[cfg(test)]
    pub(crate) fn running_queries_total(&self) -> usize {
        self.workers
            .values()
            .map(|stats| stats.running_queries as usize)
            .sum()
    }

    pub fn success(&mut self, worker: PeerId, verdict: &'static str, throughput: Option<f64>) {
        let now = Instant::now();
        self.modify(worker, |stats| {
            if let Some(t) = throughput {
                stats.last_throughput = Some(t);
            }
            stats.last_verdict = Some((verdict, now));
            // Not every `success` is an answer: a request the portal itself malformed
            // is charged to the portal, and must not count as the worker replying.
            if verdict == VERDICT_OK {
                stats.last_ok = Some(now);
            }
        });
    }

    // Query error has been returned from the worker
    pub fn error(&mut self, worker: PeerId, verdict: &'static str) {
        let now = Instant::now();
        self.modify(worker, |stats| {
            stats.server_errors.observe(now);
            stats.last_verdict = Some((verdict, now));
        });
    }

    // Query could not be processed, e.g. because the worker couldn't be reached
    pub fn failure(&mut self, worker: PeerId, verdict: &'static str) {
        let now = Instant::now();
        self.modify(worker, |stats| {
            stats.timeouts.observe(now);
            stats.last_verdict = Some((verdict, now));
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
        (PriorityGroup::Best, 0, i64::MIN)
    }

    fn health(&self, worker: &WorkerStats, now: Instant) -> WorkerHealth {
        let backoff_secs = worker
            .paused_until
            .and_then(|until| until.checked_duration_since(now))
            .map(|d| d.as_secs_f64());
        let server_errors_cooldown_secs = worker.server_errors.remaining(now).map(secs);
        let timeouts_cooldown_secs = worker.timeouts.remaining(now).map(secs);
        let at_query_limit = worker.running_queries >= self.config.max_queries_per_worker;

        let mut blocked_by = Vec::new();
        if backoff_secs.is_some() {
            blocked_by.push("backoff");
        }
        if server_errors_cooldown_secs.is_some() {
            blocked_by.push("server_errors");
        }
        if timeouts_cooldown_secs.is_some() {
            blocked_by.push("timeouts");
        }
        if at_query_limit {
            blocked_by.push("at_query_limit");
        }

        WorkerHealth {
            blocked_by,
            running_queries: worker.running_queries,
            backoff_secs,
            server_errors_cooldown_secs,
            timeouts_cooldown_secs,
            last_throughput: worker.last_throughput,
            last_verdict: worker.last_verdict.map(|(label, _)| label),
            last_verdict_secs_ago: worker
                .last_verdict
                .map(|(_, at)| secs(now.saturating_duration_since(at))),
            last_ok_secs_ago: worker
                .last_ok
                .map(|at| secs(now.saturating_duration_since(at))),
            never_ok: worker.never_ok(),
        }
    }

    /// A worker the pool has no entry for: it ranks `Best` by default, and every
    /// field below is genuinely unknown rather than zero.
    fn unqueried_health() -> WorkerHealth {
        WorkerHealth {
            blocked_by: Vec::new(),
            running_queries: 0,
            backoff_secs: None,
            server_errors_cooldown_secs: None,
            timeouts_cooldown_secs: None,
            last_throughput: None,
            last_verdict: None,
            last_verdict_secs_ago: None,
            last_ok_secs_ago: None,
            never_ok: false,
        }
    }
}

fn secs(d: Duration) -> f64 {
    d.as_secs_f64()
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
        self.remaining(now).is_some()
    }

    /// How long the cooldown still bars this worker, or `None` once it has lapsed.
    fn remaining(&self, now: Instant) -> Option<Duration> {
        let window = Duration::from_secs(self.seconds as u64);
        let elapsed = now.saturating_duration_since(self.last_observed?);
        (elapsed.as_secs() < self.seconds as u64).then(|| window.saturating_sub(elapsed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool() -> WorkersPool {
        WorkersPool::new(PrioritiesConfig {
            max_queries_per_worker: 1,
            window_errors_secs: 30,
            window_timeouts_secs: 300,
        })
    }

    fn health_of(pool: &WorkersPool, worker: PeerId) -> WorkerHealth {
        let (_, _, health) = pool
            .describe([worker])
            .into_iter()
            .next()
            .expect("describe returns one row per worker");
        health
    }

    /// The verdict is what separates a holder that is offline from one that is merely
    /// busy. `PriorityGroup::Unavailable` says both, which is why reading it alone sent
    /// an investigation after the wrong layer.
    #[test]
    fn an_unreachable_holder_is_told_apart_from_a_busy_one() {
        let mut pool = pool();
        let (dead, busy) = (PeerId::random(), PeerId::random());

        pool.failure(dead, "transport_error");
        pool.lease(busy);

        let dead = health_of(&pool, dead);
        assert_eq!(dead.blocked_by, ["timeouts"]);
        assert_eq!(dead.last_verdict, Some("transport_error"));
        assert!(dead.never_ok);

        let busy = health_of(&pool, busy);
        assert_eq!(busy.blocked_by, ["at_query_limit"]);
        assert_eq!(busy.last_verdict, None);
        assert!(
            !busy.never_ok,
            "a worker that was never asked has not failed"
        );
    }

    /// A worker the pool has no entry for ranks `Best`, and must not be reported as
    /// having answered — `never_ok` is a claim about verdicts, not about silence.
    #[test]
    fn an_unqueried_worker_reports_nothing_it_does_not_know() {
        let health = health_of(&pool(), PeerId::random());

        assert!(health.blocked_by.is_empty());
        assert_eq!(health.last_verdict, None);
        assert_eq!(health.last_ok_secs_ago, None);
        assert!(!health.never_ok);
    }

    /// One `ok` clears the flag, and a later failure does not bring it back: the
    /// question it answers is "has this worker ever served us", not "is it failing now".
    #[test]
    fn never_ok_survives_only_until_the_first_ok() {
        let mut pool = pool();
        let worker = PeerId::random();

        pool.failure(worker, "transport_error");
        assert!(health_of(&pool, worker).never_ok);

        pool.success(worker, VERDICT_OK, Some(1.0));
        assert!(!health_of(&pool, worker).never_ok);

        pool.error(worker, "server_overloaded");
        let health = health_of(&pool, worker);
        assert!(!health.never_ok);
        assert_eq!(health.last_verdict, Some("server_overloaded"));
    }

    /// A request the portal itself malformed is charged to the portal, not answered by
    /// the worker, so it must not count as the worker having served us.
    #[test]
    fn a_portal_side_invalid_request_is_not_an_ok() {
        let mut pool = pool();
        let worker = PeerId::random();

        pool.success(worker, "invalid", None);

        let health = health_of(&pool, worker);
        assert!(health.never_ok);
        assert_eq!(health.last_ok_secs_ago, None);
    }

    /// The count `summarize` reports is the count `pick` would act on — a chunk with ten
    /// holders and one reachable is one available holder, not ten.
    #[test]
    fn summarize_counts_holders_the_way_pick_sees_them() {
        let mut pool = pool();
        let live = PeerId::random();
        let dead: Vec<_> = (0..8).map(|_| PeerId::random()).collect();
        let paused = PeerId::random();

        pool.success(live, VERDICT_OK, Some(1.0));
        for &worker in &dead {
            pool.failure(worker, "transport_error");
        }
        // As production reaches Backoff: a verdict, then the worker's own retry hint.
        pool.error(paused, "too_many_requests");
        pool.hint_backoff(paused, Duration::from_secs(5));

        let holders: Vec<_> = std::iter::once(live)
            .chain(dead.iter().copied())
            .chain(std::iter::once(paused))
            .collect();
        let summary = pool.summarize(holders.iter().copied());

        assert_eq!(summary.holders, 10);
        assert_eq!(summary.available, 1);
        assert_eq!(summary.backoff, 1);
        assert_eq!(summary.unavailable, 8);
        assert_eq!(summary.never_ok, 9, "the paused one never answered either");
        assert_eq!(summary.unqueried, 0);

        // And `pick` agrees: the one live holder is the only one it will take.
        assert_eq!(pool.pick(holders).unwrap(), live);
    }

    /// `describe` orders by priority because "who gets the next query" is the question
    /// it exists to answer; peer order hides it.
    #[test]
    fn describe_lists_workers_in_pick_order() {
        let mut pool = pool();
        let (dead, live) = (PeerId::random(), PeerId::random());

        pool.failure(dead, "transport_error");
        pool.success(live, VERDICT_OK, Some(1.0));

        let order: Vec<_> = pool
            .describe([dead, live])
            .into_iter()
            .map(|(peer_id, _, _)| peer_id)
            .collect();
        assert_eq!(order, [live, dead]);
    }

    /// A cooldown that has lapsed is no longer a reason, and must stop being reported as
    /// one — a stale `blocked_by` would read as an outage that is already over.
    #[tokio::test(start_paused = true)]
    async fn a_lapsed_cooldown_stops_being_a_reason() {
        let mut pool = pool();
        let worker = PeerId::random();

        pool.failure(worker, "transport_error");
        assert_eq!(health_of(&pool, worker).blocked_by, ["timeouts"]);

        tokio::time::advance(Duration::from_secs(301)).await;

        let health = health_of(&pool, worker);
        assert!(health.blocked_by.is_empty());
        assert_eq!(health.timeouts_cooldown_secs, None);
        // The verdict itself outlives the cooldown: this worker is still the one that
        // has never answered, and still outranks every worker that has.
        assert!(health.never_ok);
        assert_eq!(health.last_verdict, Some("transport_error"));
    }
}
