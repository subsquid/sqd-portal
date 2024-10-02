use std::{collections::VecDeque, time::Duration};

use parking_lot::Mutex;

const WINDOW_SIZE: usize = 100;
const DEFAULT_TIMEOUT: Duration = Duration::from_millis(1000);

/// Keeps track of the sliding percentile of request durations.
pub struct TimeoutManager {
    quantile: f32,
    durations: Mutex<VecDeque<Duration>>,
}

impl TimeoutManager {
    pub fn new(quantile: f32) -> Self {
        Self {
            quantile,
            durations: Mutex::new(VecDeque::with_capacity(WINDOW_SIZE)),
        }
    }

    pub fn observe(&self, duration: Duration) {
        let mut durations = self.durations.lock();
        if durations.len() >= WINDOW_SIZE {
            durations.pop_front();
        }
        durations.push_back(duration);
    }

    pub async fn sleep(&self) {
        let start = tokio::time::Instant::now();
        let timeout = self.current_timeout();
        tracing::trace!("Current timeout: {:?}", timeout);
        tokio::time::sleep_until(start + timeout).await;
    }

    pub fn current_timeout(&self) -> Duration {
        let mut durations = self.durations.lock().iter().copied().collect::<Vec<_>>();
        if durations.is_empty() {
            return DEFAULT_TIMEOUT;
        }
        let kth = (durations.len() as f32 * self.quantile).floor() as usize;
        if kth >= durations.len() {
            // sorted[sorted.len() - 1] * 2
            return DEFAULT_TIMEOUT;
        }
        // TODO: optimize time complexity
        durations.sort();
        durations[kth]
    }
}
