//! One worker query a stream dispatches, and what became of it (OB-16).
//!
//! The [`Attempt`] stays with the stream controller, which knows the query's fate. The
//! [`AttemptMeter`] goes into the query task, which knows whether the query reached the
//! transport and how many bytes came back. Either can finish last: a superseded hedge
//! is still reading, a finished task waits to be polled. Each half swaps its mark into
//! one shared state and whichever finishes second publishes, so every query is counted
//! once, with final values.

use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering},
    Arc,
};

use crate::metrics::{AttemptKind, AttemptOutcome, AttemptSignals};

const OPEN: u8 = 0;
/// The task finished first and waits for the controller's verdict.
const READ_DONE: u8 = 1;

/// The state word for a verdict given while the task was still running.
const fn settled(outcome: AttemptOutcome) -> u8 {
    match outcome {
        AttemptOutcome::Delivered => 2,
        AttemptOutcome::Failed => 3,
        AttemptOutcome::Superseded => 4,
        AttemptOutcome::Discarded => 5,
        AttemptOutcome::Abandoned => 6,
    }
}

struct Shared {
    kind: AttemptKind,
    sent: AtomicBool,
    bytes: AtomicU64,
    state: AtomicU8,
    signals: Arc<AttemptSignals>,
}

impl Shared {
    fn publish(&self, outcome: AttemptOutcome) {
        if self.sent.load(Ordering::Relaxed) {
            self.signals.settled(self.kind, outcome);
            self.signals
                .bytes(outcome, self.bytes.load(Ordering::Relaxed));
        } else {
            self.signals.withdrawn(self.kind);
        }
    }
}

/// The controller's half. Dropped unsettled, it is abandoned: that is the fate of
/// whatever a stream still holds when it ends.
pub struct Attempt {
    shared: Arc<Shared>,
    settled: bool,
}

/// The query task's half.
pub struct AttemptMeter {
    shared: Arc<Shared>,
}

impl Attempt {
    #[must_use = "an attempt dropped at once is counted as a withdrawn query"]
    pub fn start(kind: AttemptKind, signals: &Arc<AttemptSignals>) -> (Attempt, AttemptMeter) {
        let shared = Arc::new(Shared {
            kind,
            sent: AtomicBool::new(false),
            bytes: AtomicU64::new(0),
            state: AtomicU8::new(OPEN),
            signals: signals.clone(),
        });
        let meter = AttemptMeter {
            shared: shared.clone(),
        };
        let attempt = Attempt {
            shared,
            settled: false,
        };
        (attempt, meter)
    }

    #[cfg(test)]
    pub fn detached() -> Attempt {
        Self::start(AttemptKind::First, &Arc::default()).0
    }

    pub fn settle(mut self, outcome: AttemptOutcome) {
        self.settle_once(outcome);
    }

    fn settle_once(&mut self, outcome: AttemptOutcome) {
        if std::mem::replace(&mut self.settled, true) {
            return;
        }
        // Acquire the task's `sent` and `bytes` if it finished first.
        if self.shared.state.swap(settled(outcome), Ordering::Acquire) == READ_DONE {
            // A lost race whose answer had already arrived was never read.
            let outcome = match outcome {
                AttemptOutcome::Superseded => AttemptOutcome::Discarded,
                outcome => outcome,
            };
            self.shared.publish(outcome);
        }
    }
}

impl Drop for Attempt {
    fn drop(&mut self) {
        self.settle_once(AttemptOutcome::Abandoned);
    }
}

impl AttemptMeter {
    /// The query is being handed to the transport, so a worker may see it.
    pub fn mark_sent(&self) {
        self.shared.sent.store(true, Ordering::Relaxed);
        self.shared.signals.sent(self.shared.kind);
    }

    pub fn record(&self, bytes: usize) {
        self.shared.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

impl Drop for AttemptMeter {
    /// Runs when the task's future is dropped: on completion, or when an abort lands.
    fn drop(&mut self) {
        // Release `sent` and `bytes` to a controller that settles after this.
        let previous = self.shared.state.swap(READ_DONE, Ordering::Release);
        let verdict = AttemptOutcome::ALL
            .into_iter()
            .find(|&outcome| settled(outcome) == previous);
        if let Some(outcome) = verdict {
            self.shared.publish(outcome);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn start(kind: AttemptKind) -> (Arc<AttemptSignals>, Attempt, AttemptMeter) {
        let signals = Arc::new(AttemptSignals::default());
        let (attempt, meter) = Attempt::start(kind, &signals);
        (signals, attempt, meter)
    }

    #[test]
    fn a_task_finishing_first_leaves_the_controller_to_publish() {
        let (signals, attempt, meter) = start(AttemptKind::First);
        meter.mark_sent();
        meter.record(100);
        drop(meter);
        assert_eq!(signals.byte_count(AttemptOutcome::Delivered), 0);

        attempt.settle(AttemptOutcome::Delivered);
        assert_eq!(signals.byte_count(AttemptOutcome::Delivered), 100);
        let delivered = signals.settled_count(AttemptKind::First, AttemptOutcome::Delivered);
        assert_eq!(delivered, 1);
    }

    /// Bytes read after the controller gave up still belong to the verdict.
    #[test]
    fn a_controller_settling_first_leaves_the_task_to_publish_its_final_count() {
        let (signals, attempt, meter) = start(AttemptKind::Hedge);
        meter.mark_sent();
        meter.record(10);
        attempt.settle(AttemptOutcome::Superseded);
        meter.record(15);
        drop(meter);

        assert_eq!(signals.byte_count(AttemptOutcome::Superseded), 25);
    }

    #[test]
    fn a_loser_that_had_already_answered_is_discarded() {
        let (signals, attempt, meter) = start(AttemptKind::Hedge);
        meter.mark_sent();
        meter.record(40);
        drop(meter);
        attempt.settle(AttemptOutcome::Superseded);

        let discarded = signals.settled_count(AttemptKind::Hedge, AttemptOutcome::Discarded);
        assert_eq!(discarded, 1);
        assert_eq!(signals.byte_count(AttemptOutcome::Discarded), 40);
    }

    /// Cancelled while still queued for the transport: no worker saw it.
    #[test]
    fn a_query_that_never_reached_the_transport_is_withdrawn() {
        let (signals, attempt, meter) = start(AttemptKind::Hedge);
        attempt.settle(AttemptOutcome::Superseded);
        drop(meter);

        assert_eq!(signals.sent_count(AttemptKind::Hedge), 0);
        assert_eq!(signals.withdrawn_count(AttemptKind::Hedge), 1);
        let superseded = signals.settled_count(AttemptKind::Hedge, AttemptOutcome::Superseded);
        assert_eq!(superseded, 0);
    }

    #[test]
    fn an_attempt_dropped_unsettled_is_abandoned() {
        let (signals, attempt, meter) = start(AttemptKind::First);
        meter.mark_sent();
        meter.record(7);
        drop(attempt);
        drop(meter);

        let abandoned = signals.settled_count(AttemptKind::First, AttemptOutcome::Abandoned);
        assert_eq!(abandoned, 1);
        assert_eq!(signals.byte_count(AttemptOutcome::Abandoned), 7);
    }

    /// The halves finish on different threads in production: whichever wins, the
    /// query lands in exactly one outcome with every byte.
    #[test]
    fn concurrent_halves_publish_exactly_once() {
        for _ in 0..500 {
            let (signals, attempt, meter) = start(AttemptKind::Hedge);
            let reader = std::thread::spawn(move || {
                meter.mark_sent();
                for _ in 0..50 {
                    meter.record(2);
                }
            });
            attempt.settle(AttemptOutcome::Superseded);
            reader.join().unwrap();

            let lost = [AttemptOutcome::Superseded, AttemptOutcome::Discarded];
            let settled: u64 = lost
                .iter()
                .map(|&o| signals.settled_count(AttemptKind::Hedge, o))
                .sum();
            let bytes: u64 = lost.iter().map(|&o| signals.byte_count(o)).sum();
            assert_eq!((settled, bytes), (1, 100));
        }
    }
}
