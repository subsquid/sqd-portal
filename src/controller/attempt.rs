//! One worker query a stream dispatches, and what became of it (OB-16).
//!
//! Three facts are recorded about every query, by the two parties that know them:
//!
//! - The **outcome** is why the controller let go of the query: delivered, failed,
//!   superseded by another attempt's answer, cancelled by another attempt's terminal
//!   error, or abandoned when the stream ended. The [`Attempt`] stays with the stream
//!   controller and carries it.
//! - The **stage** is whether the controller had taken the task's result by then. Also
//!   the controller's: it is set the moment the controller polls the task and takes
//!   what it returned.
//! - The **completion** is what the query task had done by then: returned an answer,
//!   returned an error, or neither. The [`AttemptMeter`] goes into the query task and
//!   records it, along with whether the query reached the transport and how many bytes
//!   came back.
//!
//! Keeping them apart is what makes waste legible. A hedge that lost the race is
//! `superseded` either way; whether its answer had arrived and went unread (`ok`) or it
//! had already failed (`error`) or it was cut off mid-body (`incomplete`) is the
//! completion. Folding them into one word, as an earlier version did with "discarded",
//! misfiled a hedge that had failed as an answer thrown away. And the completion alone
//! cannot say whether an abandoned answer had been buffered: a task can answer after
//! the controller has let its query go, and before the abort lands. Only the stage says
//! whether the controller ever held it.
//!
//! Either half can finish last: a superseded hedge is still reading, a finished task
//! waits to be polled. Each half swaps its mark into one shared state and whichever
//! finishes second publishes, so every query is counted once, with final values.

use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering},
    Arc,
};

use crate::metrics::{
    AttemptCompletion, AttemptKind, AttemptOutcome, AttemptSignals, AttemptStage, StreamEnd,
};

const OPEN: u8 = 0;
/// The task finished first and waits for the controller's verdict.
const READ_DONE: u8 = 1;

/// The state word for a verdict given while the task was still running.
const fn settled(outcome: AttemptOutcome) -> u8 {
    match outcome {
        AttemptOutcome::Delivered => 2,
        AttemptOutcome::Failed => 3,
        AttemptOutcome::Superseded => 4,
        AttemptOutcome::Cancelled => 5,
        AttemptOutcome::Abandoned(StreamEnd::Error) => 6,
        AttemptOutcome::Abandoned(StreamEnd::Unknown) => 7,
    }
}

const fn completion_word(completion: AttemptCompletion) -> u8 {
    match completion {
        AttemptCompletion::Incomplete => 0,
        AttemptCompletion::Ok => 1,
        AttemptCompletion::Error => 2,
    }
}

const fn stage_word(stage: AttemptStage) -> u8 {
    match stage {
        AttemptStage::InFlight => 0,
        AttemptStage::Read => 1,
    }
}

struct Shared {
    dataset: Arc<str>,
    kind: AttemptKind,
    sent: AtomicBool,
    /// Written by the task, before it releases the state.
    completion: AtomicU8,
    /// Written by the controller, before it releases the state.
    stage: AtomicU8,
    bytes: AtomicU64,
    state: AtomicU8,
    signals: Arc<AttemptSignals>,
}

impl Shared {
    fn publish(&self, outcome: AttemptOutcome) {
        if !self.sent.load(Ordering::Relaxed) {
            self.signals.withdrawn(&self.dataset, self.kind);
            return;
        }
        let word = self.completion.load(Ordering::Relaxed);
        let completion = AttemptCompletion::ALL
            .into_iter()
            .find(|&c| completion_word(c) == word)
            .expect("every completion word is written by `AttemptMeter::complete`");
        let word = self.stage.load(Ordering::Relaxed);
        let stage = AttemptStage::ALL
            .into_iter()
            .find(|&s| stage_word(s) == word)
            .expect("every stage word is written by `Attempt::mark_read`");
        self.signals
            .settled(&self.dataset, self.kind, outcome, stage, completion);
        let bytes = self.bytes.load(Ordering::Relaxed);
        self.signals
            .bytes(&self.dataset, self.kind, outcome, stage, completion, bytes);
    }
}

/// The controller's half. Dropped unsettled, it is abandoned with no known reason: the
/// controller's own drop abandons everything it holds first, with the reason it knows.
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
    pub fn start(
        kind: AttemptKind,
        dataset: &Arc<str>,
        signals: &Arc<AttemptSignals>,
    ) -> (Attempt, AttemptMeter) {
        let shared = Arc::new(Shared {
            dataset: dataset.clone(),
            kind,
            sent: AtomicBool::new(false),
            completion: AtomicU8::new(completion_word(AttemptCompletion::Incomplete)),
            stage: AtomicU8::new(stage_word(AttemptStage::InFlight)),
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
        Self::start(AttemptKind::First, &Arc::from("detached"), &Arc::default()).0
    }

    /// The controller has polled the task and taken its result. From here the query is
    /// something the controller holds, not something it waits for.
    pub fn mark_read(&self) {
        self.shared
            .stage
            .store(stage_word(AttemptStage::Read), Ordering::Relaxed);
    }

    pub fn settle(mut self, outcome: AttemptOutcome) {
        self.settle_once(outcome);
    }

    /// The stream ended with this attempt in hand. In place, so the controller's drop
    /// can settle attempts still embedded in its buffer before the buffer goes.
    pub fn abandon(&mut self, end: StreamEnd) {
        self.settle_once(AttemptOutcome::Abandoned(end));
    }

    fn settle_once(&mut self, outcome: AttemptOutcome) {
        if std::mem::replace(&mut self.settled, true) {
            return;
        }
        // Release `stage` to a task that publishes after; acquire the task's `sent`,
        // `completion` and `bytes` if it finished first.
        if self.shared.state.swap(settled(outcome), Ordering::AcqRel) == READ_DONE {
            self.shared.publish(outcome);
        }
    }
}

impl Drop for Attempt {
    fn drop(&mut self) {
        self.settle_once(AttemptOutcome::Abandoned(StreamEnd::Unknown));
    }
}

impl AttemptMeter {
    /// The query is being handed to the transport, so a worker may see it. Submission
    /// only: nothing here proves the worker received or executed it.
    pub fn mark_sent(&self) {
        self.shared.sent.store(true, Ordering::Relaxed);
        self.shared
            .signals
            .sent(&self.shared.dataset, self.shared.kind);
    }

    /// Bytes the application read off the response stream.
    pub fn record(&self, bytes: usize) {
        self.shared.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// The task is returning its result. Not called on a task that is aborted, so an
    /// attempt cut off mid-body stays incomplete.
    pub fn complete(&self, ok: bool) {
        let completion = if ok {
            AttemptCompletion::Ok
        } else {
            AttemptCompletion::Error
        };
        self.shared
            .completion
            .store(completion_word(completion), Ordering::Relaxed);
    }
}

impl Drop for AttemptMeter {
    /// Runs when the task's future is dropped: on completion, or when an abort lands.
    fn drop(&mut self) {
        // Release `sent`, `completion` and `bytes` to a controller that settles after;
        // acquire the controller's `stage` if it settled first.
        let previous = self.shared.state.swap(READ_DONE, Ordering::AcqRel);
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

    const DATASET: &str = "test-dataset";

    fn start(kind: AttemptKind) -> (Arc<AttemptSignals>, Attempt, AttemptMeter) {
        let signals = Arc::new(AttemptSignals::default());
        let (attempt, meter) = Attempt::start(kind, &Arc::from(DATASET), &signals);
        (signals, attempt, meter)
    }

    fn settled_count(
        signals: &AttemptSignals,
        kind: AttemptKind,
        outcome: AttemptOutcome,
        stage: AttemptStage,
        completion: AttemptCompletion,
    ) -> u64 {
        signals.settled_count(DATASET, kind, outcome, stage, completion)
    }

    fn byte_count(
        signals: &AttemptSignals,
        kind: AttemptKind,
        outcome: AttemptOutcome,
        stage: AttemptStage,
        completion: AttemptCompletion,
    ) -> u64 {
        signals.byte_count(DATASET, kind, outcome, stage, completion)
    }

    #[test]
    fn a_task_finishing_first_leaves_the_controller_to_publish() {
        use AttemptCompletion::Ok;
        use AttemptKind::First;
        use AttemptOutcome::Delivered;
        use AttemptStage::Read;
        let (signals, attempt, meter) = start(First);
        meter.mark_sent();
        meter.record(100);
        meter.complete(true);
        drop(meter);
        assert_eq!(byte_count(&signals, First, Delivered, Read, Ok), 0);

        attempt.mark_read();
        attempt.settle(Delivered);
        assert_eq!(byte_count(&signals, First, Delivered, Read, Ok), 100);
        assert_eq!(settled_count(&signals, First, Delivered, Read, Ok), 1);
    }

    /// Bytes read after the controller gave up still belong to the verdict, and a task
    /// that never returned is incomplete however many it read.
    #[test]
    fn a_controller_settling_first_leaves_the_task_to_publish_its_final_count() {
        use AttemptCompletion::Incomplete;
        use AttemptKind::Hedge;
        use AttemptOutcome::Superseded;
        use AttemptStage::InFlight;
        let (signals, attempt, meter) = start(Hedge);
        meter.mark_sent();
        meter.record(10);
        attempt.settle(Superseded);
        meter.record(15);
        drop(meter);

        let bytes = byte_count(&signals, Hedge, Superseded, InFlight, Incomplete);
        assert_eq!(bytes, 25);
        let count = settled_count(&signals, Hedge, Superseded, InFlight, Incomplete);
        assert_eq!(count, 1);
    }

    /// The answer had arrived and went unread: superseded, in flight from where the
    /// controller stood, with the task's `ok`.
    #[test]
    fn a_loser_that_had_already_answered_is_superseded_with_its_answer_in() {
        use AttemptCompletion::Ok;
        use AttemptKind::Hedge;
        use AttemptOutcome::Superseded;
        use AttemptStage::InFlight;
        let (signals, attempt, meter) = start(Hedge);
        meter.mark_sent();
        meter.record(40);
        meter.complete(true);
        drop(meter);
        attempt.settle(Superseded);

        assert_eq!(settled_count(&signals, Hedge, Superseded, InFlight, Ok), 1);
        assert_eq!(byte_count(&signals, Hedge, Superseded, InFlight, Ok), 40);
    }

    /// A hedge that had failed before it lost is not an answer thrown away.
    #[test]
    fn a_loser_that_had_already_failed_is_superseded_with_its_error_in() {
        use AttemptCompletion::{Error, Ok};
        use AttemptKind::Hedge;
        use AttemptOutcome::Superseded;
        use AttemptStage::InFlight;
        let (signals, attempt, meter) = start(Hedge);
        meter.mark_sent();
        meter.record(3);
        meter.complete(false);
        drop(meter);
        attempt.settle(Superseded);

        assert_eq!(
            settled_count(&signals, Hedge, Superseded, InFlight, Error),
            1
        );
        assert_eq!(settled_count(&signals, Hedge, Superseded, InFlight, Ok), 0);
        assert_eq!(byte_count(&signals, Hedge, Superseded, InFlight, Error), 3);
    }

    /// Cancelled while still queued for the transport: no worker saw it.
    #[test]
    fn a_query_that_never_reached_the_transport_is_withdrawn() {
        use AttemptKind::Hedge;
        use AttemptOutcome::Superseded;
        let (signals, attempt, meter) = start(Hedge);
        attempt.settle(Superseded);
        drop(meter);

        assert_eq!(signals.sent_count(DATASET, Hedge), 0);
        assert_eq!(signals.withdrawn_count(DATASET, Hedge), 1);
        assert_eq!(signals.settled_by_outcome(DATASET, Hedge, Superseded), 0);
    }

    /// Abandoned in place by a controller that knows how its stream ended, holding an
    /// answer it had read into its buffer.
    #[test]
    fn an_attempt_abandoned_by_the_controller_carries_the_streams_end() {
        use AttemptCompletion::Ok;
        use AttemptKind::First;
        use AttemptOutcome::Abandoned;
        use AttemptStage::Read;
        let (signals, mut attempt, meter) = start(First);
        meter.mark_sent();
        meter.record(7);
        meter.complete(true);
        drop(meter);
        attempt.mark_read();
        attempt.abandon(StreamEnd::Error);
        drop(attempt);

        let outcome = Abandoned(StreamEnd::Error);
        assert_eq!(settled_count(&signals, First, outcome, Read, Ok), 1);
        assert_eq!(byte_count(&signals, First, outcome, Read, Ok), 7);
    }

    /// The controller abandons a query it had not read, and the task then answers
    /// before the abort lands. The task's `ok` is true, but the controller never held
    /// that answer: the stage says so, and this is not a buffered answer.
    #[test]
    fn an_answer_landing_after_the_controller_let_go_is_abandoned_in_flight() {
        use AttemptCompletion::Ok;
        use AttemptKind::First;
        use AttemptOutcome::Abandoned;
        use AttemptStage::{InFlight, Read};
        let (signals, mut attempt, meter) = start(First);
        meter.mark_sent();
        meter.record(4);
        attempt.abandon(StreamEnd::Unknown);
        meter.record(5);
        meter.complete(true);
        drop(meter);

        let outcome = Abandoned(StreamEnd::Unknown);
        assert_eq!(settled_count(&signals, First, outcome, InFlight, Ok), 1);
        assert_eq!(settled_count(&signals, First, outcome, Read, Ok), 0);
        assert_eq!(byte_count(&signals, First, outcome, InFlight, Ok), 9);
    }

    /// Dropped with no verdict at all: abandoned, reason unknown.
    #[test]
    fn an_attempt_dropped_unsettled_is_abandoned_for_no_known_reason() {
        use AttemptCompletion::Incomplete;
        use AttemptKind::First;
        use AttemptOutcome::Abandoned;
        use AttemptStage::InFlight;
        let (signals, attempt, meter) = start(First);
        meter.mark_sent();
        meter.record(7);
        drop(attempt);
        drop(meter);

        let outcome = Abandoned(StreamEnd::Unknown);
        assert_eq!(
            settled_count(&signals, First, outcome, InFlight, Incomplete),
            1
        );
        assert_eq!(
            byte_count(&signals, First, outcome, InFlight, Incomplete),
            7
        );
    }

    /// The halves finish on different threads in production: whichever wins, the
    /// query lands in exactly one outcome, stage and completion, with every byte.
    #[test]
    fn concurrent_halves_publish_exactly_once() {
        use AttemptKind::Hedge;
        use AttemptOutcome::Superseded;
        for _ in 0..500 {
            let (signals, attempt, meter) = start(Hedge);
            let reader = std::thread::spawn(move || {
                meter.mark_sent();
                for _ in 0..50 {
                    meter.record(2);
                }
                meter.complete(true);
            });
            attempt.settle(Superseded);
            reader.join().unwrap();

            let settled = signals.settled_by_outcome(DATASET, Hedge, Superseded);
            let bytes = signals.bytes_by_outcome(DATASET, Superseded);
            assert_eq!((settled, bytes), (1, 100));
        }
    }
}
