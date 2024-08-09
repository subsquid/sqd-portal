use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use parking_lot::Mutex;
use tokio::sync::watch;

/// Keeps track of sliding percentile of request durations.
/// Calculates a timeout so that (1-q) fraction of the slowest requests are retried.
/// Running and timed out requests count as "infinite" duration.
pub struct TimeoutManager {
    quantile: f32,
    durations: Mutex<Vec<Duration>>,
    num_infs: AtomicUsize,
    current_timeout: watch::Sender<Option<Duration>>,
}

impl TimeoutManager {
    pub fn new(quantile: f32) -> Self {
        let (tx, _) = watch::channel(None);
        Self {
            quantile,
            durations: Mutex::new(Vec::new()),
            num_infs: AtomicUsize::new(0),
            current_timeout: tx,
        }
    }

    pub fn complete_ok(&self, duration: Duration) {
        let mut durations = self.durations.lock();
        durations.push(duration);
        let num_infs = self.num_infs.load(Ordering::Relaxed);

        // TODO: optimize time complexity
        let kth = ((durations.len() + num_infs) as f32 * self.quantile).floor() as usize;
        let new_timeout = if kth >= durations.len() {
            None
        } else {
            durations.sort();
            Some(durations[kth])
        };
        tracing::trace!(
            "Current timeout: {:?}, among {}+{} samples",
            new_timeout,
            durations.len(),
            num_infs
        );
        self.current_timeout.send_if_modified(|timeout| {
            if *timeout != new_timeout {
                *timeout = new_timeout;
                true
            } else {
                false
            }
        });
    }

    pub fn complete_err(&self) {
        // Ignore any requests that didn't lead to a result or timeout
    }

    pub fn complete_timeout(&self) {
        self.num_infs.fetch_add(1, Ordering::Relaxed);
    }

    pub async fn sleep(&self) {
        let start = tokio::time::Instant::now();
        self.num_infs.fetch_add(1, Ordering::Relaxed);
        let guard = scopeguard::guard((), |_| {
            let before = self.num_infs.fetch_sub(1, Ordering::Relaxed);
            assert!(before > 0);
        });
        let mut current_timeout = self.current_timeout.subscribe();
        loop {
            let timeout = *current_timeout.borrow();
            if let Some(timeout) = timeout {
                tokio::select! {
                    _ = tokio::time::sleep_until(start + timeout) => break,
                    recv = current_timeout.changed() => {
                        recv.expect("Timeout publisher dropped");
                        continue;
                    },
                }
            } else {
                current_timeout
                    .changed()
                    .await
                    .expect("Timeout publisher dropped");
            }
        }
        // Defuse the guard to keep the num_infs in case of timeout
        scopeguard::ScopeGuard::into_inner(guard);
    }
}
