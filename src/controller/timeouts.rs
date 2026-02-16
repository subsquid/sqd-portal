use std::{collections::VecDeque, time::Duration};

use crate::utils::Mutex;

const WINDOW_SIZE: usize = 20;
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
            durations: Mutex::new(
                VecDeque::with_capacity(WINDOW_SIZE),
                "TimeoutManager::durations",
            ),
        }
    }

    pub fn observe(&self, duration: Duration) {
        let mut durations = self.durations.lock();
        if durations.len() >= WINDOW_SIZE {
            durations.pop_front();
        }
        durations.push_back(duration);
    }

    pub fn current_timeout(&self) -> Duration {
        let mut durations = self.durations.lock().iter().copied().collect::<Vec<_>>();
        if durations.len() < WINDOW_SIZE {
            return DEFAULT_TIMEOUT;
        }
        let kth = ((durations.len() as f32 * self.quantile).floor() as usize)
            .min(durations.len() - 1);
        // TODO: optimize time complexity
        durations.sort();
        durations[kth]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fill_window(manager: &TimeoutManager, durations: &[u64]) {
        for &ms in durations {
            manager.observe(Duration::from_millis(ms));
        }
    }

    #[test]
    fn returns_default_timeout_when_window_not_full() {
        let manager = TimeoutManager::new(0.5);
        fill_window(&manager, &[100; WINDOW_SIZE - 1]);
        assert_eq!(manager.current_timeout(), DEFAULT_TIMEOUT);
    }

    #[test]
    fn returns_median_for_half_quantile() {
        let manager = TimeoutManager::new(0.5);
        // 1..=20 sorted: [1,2,...,20], kth = floor(20*0.5) = 10 → 11ms
        let durations: Vec<u64> = (1..=WINDOW_SIZE as u64).collect();
        fill_window(&manager, &durations);
        assert_eq!(
            manager.current_timeout(),
            Duration::from_millis(WINDOW_SIZE as u64 / 2 + 1)
        );
    }

    #[test]
    fn quantile_one_returns_max() {
        let manager = TimeoutManager::new(1.0);
        let durations: Vec<u64> = (1..=WINDOW_SIZE as u64).collect();
        fill_window(&manager, &durations);
        assert_eq!(
            manager.current_timeout(),
            Duration::from_millis(WINDOW_SIZE as u64)
        );
    }

    #[test]
    fn quantile_zero_returns_min() {
        let manager = TimeoutManager::new(0.0);
        let durations: Vec<u64> = (1..=WINDOW_SIZE as u64).collect();
        fill_window(&manager, &durations);
        assert_eq!(manager.current_timeout(), Duration::from_millis(1));
    }

    #[test]
    fn sliding_window_evicts_old_entries() {
        let manager = TimeoutManager::new(0.5);
        // Fill with 1000ms
        fill_window(&manager, &[1000; WINDOW_SIZE]);
        assert_eq!(manager.current_timeout(), Duration::from_millis(1000));
        // Push 20 more values of 500ms, fully replacing the window
        fill_window(&manager, &[500; WINDOW_SIZE]);
        assert_eq!(manager.current_timeout(), Duration::from_millis(500));
    }
}
