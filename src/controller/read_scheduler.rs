use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::config::CongestionConfig;
use crate::metrics;
use crate::utils::Mutex;

pub type Priority = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Success,
    Congestion,
    Neutral,
}

pub struct DownloadScheduler {
    state: Mutex<SchedulerState>,
    config: CongestionConfig,
}

struct SchedulerState {
    active_reads: u32,
    max_reads: u32,
    successes_since_shrink: u32,
    last_shrink: Instant,
    shrink_count: u64,
    waiters: BTreeMap<(u32, u64), oneshot::Sender<()>>,
    next_seq: u64,
}

pub struct DownloadPermit {
    scheduler: Arc<DownloadScheduler>,
    pub outcome: Outcome,
}

impl DownloadScheduler {
    pub fn new(config: CongestionConfig, initial_workers: u32) -> Self {
        let max_reads = initial_workers.clamp(config.min_window, config.max_window);
        Self {
            state: Mutex::new(SchedulerState {
                active_reads: 0,
                max_reads,
                successes_since_shrink: 0,
                last_shrink: Instant::now(),
                shrink_count: 0,
                waiters: BTreeMap::new(),
                next_seq: 0,
            }, "DownloadScheduler::state"),
            config,
        }
    }

    pub async fn acquire(self: &Arc<Self>, priority: Priority) -> DownloadPermit {
        let rx = {
            let mut state = self.state.lock();
            if state.active_reads < state.max_reads {
                state.active_reads += 1;
                metrics::CONGESTION_IN_FLIGHT.set(state.active_reads as i64);
                return DownloadPermit {
                    scheduler: Arc::clone(self),
                    outcome: Outcome::Neutral,
                };
            }
            let priority_key = (priority.min(u32::MAX as u64) as u32, state.next_seq);
            state.next_seq += 1;
            let (tx, rx) = oneshot::channel();
            state.waiters.insert(priority_key, tx);
            rx
        };
        let _ = rx.await;
        DownloadPermit {
            scheduler: Arc::clone(self),
            outcome: Outcome::Neutral,
        }
    }

    pub fn has_speculative_capacity(&self) -> bool {
        let state = self.state.lock();
        let threshold = (state.max_reads as f64 * (1.0 - self.config.speculative_fraction)) as u32;
        state.active_reads < threshold
    }

    pub fn utilization(&self) -> f64 {
        let state = self.state.lock();
        if state.max_reads == 0 {
            return 1.0;
        }
        state.active_reads as f64 / state.max_reads as f64
    }

    pub fn window_size(&self) -> u32 {
        self.state.lock().max_reads
    }

    fn release(&self, outcome: Outcome) {
        let mut state = self.state.lock();
        state.active_reads = state.active_reads.saturating_sub(1);

        match outcome {
            Outcome::Success => {
                state.successes_since_shrink += 1;
                if state.successes_since_shrink >= state.max_reads {
                    state.max_reads = (state.max_reads + 1).min(self.config.max_window);
                    state.successes_since_shrink = 0;
                    metrics::CONGESTION_WINDOW.set(state.max_reads as i64);
                }
            }
            Outcome::Congestion => {
                let now = Instant::now();
                let min_interval =
                    Duration::from_millis(self.config.min_shrink_interval_ms);
                if now.duration_since(state.last_shrink) >= min_interval {
                    let new_max = (state.max_reads as f64 * self.config.decrease_factor) as u32;
                    state.max_reads = new_max.max(self.config.min_window);
                    state.successes_since_shrink = 0;
                    state.last_shrink = now;
                    state.shrink_count += 1;
                    metrics::CONGESTION_WINDOW.set(state.max_reads as i64);
                    metrics::CONGESTION_SHRINKS.inc();
                    tracing::debug!(
                        "Congestion window shrunk to {} (shrink #{})",
                        state.max_reads,
                        state.shrink_count
                    );
                }
            }
            Outcome::Neutral => {}
        }

        while state.active_reads < state.max_reads {
            let Some((_key, sender)) = state.waiters.pop_first() else {
                break;
            };
            if sender.send(()).is_ok() {
                state.active_reads += 1;
            }
        }

        metrics::CONGESTION_IN_FLIGHT.set(state.active_reads as i64);
    }
}

impl Drop for DownloadPermit {
    fn drop(&mut self) {
        self.scheduler.release(self.outcome);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> CongestionConfig {
        CongestionConfig {
            min_window: 2,
            max_window: 100,
            decrease_factor: 0.75,
            speculative_fraction: 0.1,
            min_shrink_interval_ms: 0,
            read_timeout_sec: 1,
            headroom_threshold: 0.95,
            priority_stride: 100,
            enabled: true,
        }
    }

    #[tokio::test]
    async fn immediate_grant_under_window() {
        let sched = Arc::new(DownloadScheduler::new(test_config(), 10));
        let permit = sched.acquire(0).await;
        assert_eq!(sched.state.lock().active_reads, 1);
        drop(permit);
        assert_eq!(sched.state.lock().active_reads, 0);
    }

    #[tokio::test]
    async fn window_grows_after_enough_successes() {
        let config = CongestionConfig {
            min_window: 5,
            max_window: 100,
            ..test_config()
        };
        let sched = Arc::new(DownloadScheduler::new(config, 5));
        let initial = sched.window_size();
        for _ in 0..initial {
            let mut permit = sched.acquire(0).await;
            permit.outcome = Outcome::Success;
            drop(permit);
        }
        assert_eq!(sched.window_size(), initial + 1);
    }

    #[tokio::test]
    async fn window_shrinks_on_congestion() {
        let sched = Arc::new(DownloadScheduler::new(test_config(), 10));
        let initial = sched.window_size();
        let mut permit = sched.acquire(0).await;
        permit.outcome = Outcome::Congestion;
        drop(permit);
        let expected = ((initial as f64 * 0.75) as u32).max(2);
        assert_eq!(sched.window_size(), expected);
    }

    #[tokio::test]
    async fn utilization_correct() {
        let config = CongestionConfig {
            min_window: 10,
            max_window: 10,
            ..test_config()
        };
        let sched = Arc::new(DownloadScheduler::new(config, 10));
        assert!((sched.utilization() - 0.0).abs() < f64::EPSILON);
        let _p1 = sched.acquire(0).await;
        assert!((sched.utilization() - 0.1).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn priority_ordering() {
        let config = CongestionConfig {
            min_window: 1,
            max_window: 1,
            ..test_config()
        };
        let sched = Arc::new(DownloadScheduler::new(config, 1));

        let blocker = sched.acquire(0).await;

        let sched2 = Arc::clone(&sched);
        let mut high_priority = tokio::spawn(async move {
            let _permit = sched2.acquire(10).await;
            "high"
        });

        let sched3 = Arc::clone(&sched);
        let low_priority = tokio::spawn(async move {
            let _permit = sched3.acquire(100).await;
            "low"
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        drop(blocker);
        let first = tokio::select! {
            r = &mut high_priority => r.unwrap(),
            _ = tokio::time::sleep(Duration::from_millis(100)) => "timeout",
        };
        assert_eq!(first, "high");

        let second = tokio::time::timeout(Duration::from_millis(100), low_priority)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second, "low");
    }

    #[tokio::test]
    async fn speculative_capacity_check() {
        let config = CongestionConfig {
            min_window: 10,
            max_window: 10,
            speculative_fraction: 0.2, // threshold = 80% of 10 = 8
            ..test_config()
        };
        let sched = Arc::new(DownloadScheduler::new(config, 10));

        // With 0 active reads, there's capacity
        assert!(sched.has_speculative_capacity());

        // Fill up to 7 — still under threshold of 8
        let mut permits = Vec::new();
        for _ in 0..7 {
            permits.push(sched.acquire(0).await);
        }
        assert!(sched.has_speculative_capacity());

        // 8th read hits the threshold
        permits.push(sched.acquire(0).await);
        assert!(!sched.has_speculative_capacity());

        // Drop one — back below threshold
        permits.pop();
        assert!(sched.has_speculative_capacity());
    }
}
