use std::sync::{
    atomic::{AtomicU32, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use async_stream::stream;
use futures::{Stream, StreamExt};
use tracing_futures::Instrument;

use crate::{
    metrics,
    network::NetworkClient,
    types::{RequestError, ResponseChunk, StreamRequest},
};

use super::stream::StreamController;

/// Admission slots for concurrent streams. Split out of [`TaskManager`] so the
/// accounting is testable without a `NetworkClient`.
struct Slots {
    running: AtomicUsize,
    max: usize,
}

impl Slots {
    /// Check and increment in one CAS: the count never reads above `max`, and a
    /// rejection is a load and a compare — no counter churn, no `Arc` clone.
    fn try_acquire(self: &Arc<Self>) -> Option<StreamPermit> {
        self.running
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |running| {
                (running < self.max).then_some(running + 1)
            })
            .ok()?;
        metrics::ACTIVE_STREAMS.inc();
        Some(StreamPermit {
            slots: self.clone(),
        })
    }
}

/// One admitted stream's slot, released on drop.
pub struct StreamPermit {
    slots: Arc<Slots>,
}

impl Drop for StreamPermit {
    fn drop(&mut self) {
        self.slots.running.fetch_sub(1, Ordering::Relaxed);
        metrics::ACTIVE_STREAMS.dec();
    }
}

/// Tracks all existing streams
pub struct TaskManager {
    network_client: Arc<NetworkClient>,
    slots: Arc<Slots>,
    next_stream_index: AtomicU32,
    bandwidth_utilization_threshold: f64,
    priority_stride: u32,
}

impl TaskManager {
    pub fn new(
        network_client: Arc<NetworkClient>,
        max_parallel_streams: usize,
        bandwidth_utilization_threshold: f64,
        priority_stride: u32,
    ) -> TaskManager {
        TaskManager {
            network_client,
            slots: Arc::new(Slots {
                running: 0.into(),
                max: max_parallel_streams,
            }),
            next_stream_index: AtomicU32::new(0),
            bandwidth_utilization_threshold,
            priority_stride,
        }
    }

    /// Admission check, deliberately cheap: run it before any work a rejection
    /// would throw away — parsing, head fetches, spawned tasks.
    pub fn try_reserve(&self) -> Result<StreamPermit, RequestError> {
        // Cap first: it's one CAS, while download_utilization takes the download
        // scheduler's lock. A storm rejects here, so it must not contend on that.
        let permit = self
            .slots
            .try_acquire()
            .ok_or(RequestError::TooManyStreams)?;

        if let Some(util) = self.network_client.download_utilization() {
            if util > self.bandwidth_utilization_threshold {
                return Err(RequestError::BusyFor(Duration::from_secs(1)));
            }
        }

        Ok(permit)
    }

    pub async fn spawn_stream(
        self: Arc<Self>,
        permit: StreamPermit,
        mut request: StreamRequest,
    ) -> Result<impl Stream<Item = ResponseChunk>, RequestError> {
        // ACTIVE_STREAMS.dec() rides on the permit's Drop, inside this guard.
        let guard = scopeguard::guard(permit, |_permit| {
            metrics::COMPLETED_STREAMS.inc();
        });

        if request.skip_parent_hash_validation {
            request.query.remove_parent_hash();
        }

        let stream_index = self.next_stream_index.fetch_add(1, Ordering::Relaxed);
        let mut streamer = StreamController::new(
            request,
            self.network_client.clone(),
            stream_index,
            self.priority_stride,
        )?;
        let first_chunk = streamer
            .next()
            .instrument(tracing::debug_span!("stream_next"))
            .await
            .expect("First chunk missing from the stream")?;
        Ok(stream! {
            let _guard = guard;
            yield first_chunk;
            loop {
                match streamer.next().instrument(tracing::debug_span!("stream_next")).await {
                    None => break,
                    Some(Ok(chunk)) => yield chunk,
                    Some(Err(e)) => {
                        tracing::warn!("Stream got interrupted: {:?}", e);
                        // There is no way to pass the error to the client
                        break;
                    }
                }
            }
        }
        .in_current_span())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slots(max: usize) -> Arc<Slots> {
        Arc::new(Slots {
            running: 0.into(),
            max,
        })
    }

    #[test]
    fn admits_up_to_max_then_rejects() {
        let slots = slots(2);

        let _first = slots.try_acquire().expect("first");
        let _second = slots.try_acquire().expect("second");

        assert!(slots.try_acquire().is_none());
    }

    #[test]
    fn dropping_a_permit_frees_its_slot() {
        let slots = slots(1);

        let permit = slots.try_acquire().expect("first");
        assert!(slots.try_acquire().is_none());

        drop(permit);
        assert!(slots.try_acquire().is_some());
    }

    /// The slot used to be released by hand on each rejection path; one missed
    /// decrement would wedge the portal at its cap until restarted.
    #[test]
    fn rejections_do_not_consume_slots() {
        let slots = slots(1);
        let permit = slots.try_acquire().expect("first");

        for _ in 0..100 {
            assert!(slots.try_acquire().is_none());
        }

        drop(permit);
        assert!(slots.try_acquire().is_some(), "rejections leaked the slot");
    }

    /// The tests above would pass against a plain `Cell`. This is the one the
    /// atomics are actually for.
    #[test]
    fn never_admits_more_than_max_under_contention() {
        const MAX: usize = 4;
        const THREADS: usize = 16;
        const ATTEMPTS: usize = 500;

        let slots = slots(MAX);
        let live = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let admitted = AtomicUsize::new(0);

        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                scope.spawn(|| {
                    for _ in 0..ATTEMPTS {
                        let Some(permit) = slots.try_acquire() else {
                            continue;
                        };
                        admitted.fetch_add(1, Ordering::SeqCst);
                        let now = live.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(now, Ordering::SeqCst);
                        std::thread::yield_now();
                        live.fetch_sub(1, Ordering::SeqCst);
                        drop(permit);
                    }
                });
            }
        });

        let peak = peak.load(Ordering::SeqCst);
        assert!(admitted.load(Ordering::SeqCst) > 0, "nothing was admitted");
        assert!(
            peak <= MAX,
            "{peak} streams held a slot at once, cap is {MAX}"
        );
        assert_eq!(
            slots.running.load(Ordering::SeqCst),
            0,
            "contention leaked slots",
        );
    }
}
