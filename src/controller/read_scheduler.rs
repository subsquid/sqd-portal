use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
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
    pub fn new(config: CongestionConfig) -> Self {
        let max_reads = config.max_window;
        Self {
            state: Mutex::new(
                SchedulerState {
                    active_reads: 0,
                    max_reads,
                    successes_since_shrink: 0,
                    last_shrink: Instant::now(),
                    shrink_count: 0,
                    waiters: BTreeMap::new(),
                    next_seq: 0,
                },
                "DownloadScheduler::state",
            ),
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
        // Use cancellation-safe future: if this task is aborted after release()
        // woke us (pre-incrementing active_reads) but before we create the permit,
        // WaiterFuture::drop releases the reserved slot.
        WaiterFuture {
            rx: Some(rx),
            scheduler: Arc::clone(self),
        }
        .await;
        DownloadPermit {
            scheduler: Arc::clone(self),
            outcome: Outcome::Neutral,
        }
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

    pub fn read_timeout(&self) -> Duration {
        Duration::from_secs(self.config.read_timeout_sec)
    }

    /// Signal congestion without holding a permit (e.g. transport errors before permit acquisition).
    pub fn signal_congestion(&self) {
        let mut state = self.state.lock();
        Self::apply_congestion(&self.config, &mut state);
    }

    fn apply_outcome(config: &CongestionConfig, state: &mut SchedulerState, outcome: Outcome) {
        match outcome {
            Outcome::Success => {
                state.successes_since_shrink += 1;
                if state.successes_since_shrink >= state.max_reads {
                    state.max_reads = (state.max_reads + 1).min(config.max_window);
                    state.successes_since_shrink = 0;
                    metrics::CONGESTION_WINDOW.set(state.max_reads as i64);
                }
            }
            Outcome::Congestion => {
                Self::apply_congestion(config, state);
            }
            Outcome::Neutral => {}
        }
    }

    fn apply_congestion(config: &CongestionConfig, state: &mut SchedulerState) {
        let now = Instant::now();
        let min_interval = Duration::from_millis(config.min_shrink_interval_ms);
        if now.duration_since(state.last_shrink) >= min_interval {
            let new_max = (state.max_reads as f64 * config.decrease_factor) as u32;
            state.max_reads = new_max.max(config.min_window);
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

    fn release(&self, outcome: Outcome) {
        let mut state = self.state.lock();
        state.active_reads = state.active_reads.saturating_sub(1);

        Self::apply_outcome(&self.config, &mut state, outcome);

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

/// Cancellation-safe wrapper around `oneshot::Receiver`.
///
/// When `release()` wakes a waiter, it pre-increments `active_reads` and sends
/// on the channel. If the task is aborted between the send and the permit
/// creation, this future's `Drop` detects the buffered value via `try_recv`
/// and releases the reserved slot.
struct WaiterFuture {
    rx: Option<oneshot::Receiver<()>>,
    scheduler: Arc<DownloadScheduler>,
}

impl Future for WaiterFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let rx = self.rx.as_mut().expect("polled after completion");
        match Pin::new(rx).poll(cx) {
            Poll::Ready(_) => {
                self.rx = None;
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for WaiterFuture {
    fn drop(&mut self) {
        if let Some(mut rx) = self.rx.take() {
            // Value was sent (slot reserved) but we never created the permit.
            if rx.try_recv().is_ok() {
                self.scheduler.release(Outcome::Neutral);
            }
        }
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
        let sched = Arc::new(DownloadScheduler::new(test_config()));
        let permit = sched.acquire(0).await;
        assert_eq!(sched.state.lock().active_reads, 1);
        drop(permit);
        assert_eq!(sched.state.lock().active_reads, 0);
    }

    #[tokio::test]
    async fn window_grows_after_enough_successes() {
        let config = CongestionConfig {
            min_window: 2,
            max_window: 100,
            ..test_config()
        };
        let sched = Arc::new(DownloadScheduler::new(config));

        // Shrink the window first so there's room to grow
        let mut permit = sched.acquire(0).await;
        permit.outcome = Outcome::Congestion;
        drop(permit);

        let shrunk = sched.window_size();
        assert!(shrunk < 100);

        for _ in 0..shrunk {
            let mut permit = sched.acquire(0).await;
            permit.outcome = Outcome::Success;
            drop(permit);
        }
        assert_eq!(sched.window_size(), shrunk + 1);
    }

    #[tokio::test]
    async fn window_shrinks_on_congestion() {
        let sched = Arc::new(DownloadScheduler::new(test_config()));
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
        let sched = Arc::new(DownloadScheduler::new(config));
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
        let sched = Arc::new(DownloadScheduler::new(config));

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
}
