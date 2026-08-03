use std::sync::{
    atomic::{AtomicU32, AtomicUsize, Ordering},
    Arc, Mutex, PoisonError,
};
use std::time::Duration;

use async_stream::stream;
use futures::{Stream, StreamExt};
use tracing_futures::Instrument;

// The runtime clock, so paused-time tests drive the sampler the same way real time does.
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::{
    config::Config,
    metrics::{self, RefusalReason},
    network::NetworkClient,
    types::{RequestError, ResponseChunk, StreamRequest},
};

use super::stream::StreamController;

/// Flush interval for unchanged occupancy.
const OCCUPANCY_TICK: Duration = Duration::from_millis(100);

#[derive(Clone, Copy, Debug)]
enum OccupancyEvent {
    Started,
    Finished,
    Flush,
}

#[derive(Clone, Copy, Debug)]
struct OccupancyInterval {
    running: usize,
    limit: usize,
    elapsed: Duration,
}

/// Occupancy level serialized across transitions and flushes.
struct OccupancyState {
    running: usize,
    limit: usize,
    observed_at: Instant,
}

impl OccupancyState {
    fn new(limit: usize) -> Self {
        Self::at(limit, Instant::now())
    }

    fn at(limit: usize, observed_at: Instant) -> Self {
        Self {
            running: 0,
            limit,
            observed_at,
        }
    }

    fn record(&mut self, event: OccupancyEvent, now: Instant) -> OccupancyInterval {
        let interval = OccupancyInterval {
            running: self.running,
            limit: self.limit,
            elapsed: now.saturating_duration_since(self.observed_at),
        };
        self.observed_at = now;

        // Debug-only: an imbalance is an accounting bug, but this runs on the admission
        // path, and a panic here would poison the mutex and refuse every later stream.
        // Tests catch it; production degrades to a wrong counter.
        match event {
            OccupancyEvent::Started => {
                debug_assert!(
                    self.running < self.limit,
                    "admitted stream occupancy must stay below its limit"
                );
                self.running += 1;
            }
            OccupancyEvent::Finished => {
                debug_assert!(
                    self.running > 0,
                    "a finished stream must have been admitted"
                );
                self.running = self.running.saturating_sub(1);
            }
            OccupancyEvent::Flush => {}
        }

        interval
    }
}

/// Occupancy meter: every transition and flush closes an interval under one clock.
struct Occupancy(Mutex<OccupancyState>);

impl Occupancy {
    fn new(limit: usize) -> Self {
        Self(Mutex::new(OccupancyState::new(limit)))
    }

    fn record(&self, event: OccupancyEvent) {
        let interval = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .record(event, Instant::now());
        metrics::observe_stream_occupancy(interval.running, interval.limit, interval.elapsed);
    }

    /// Publish the standing level every tick, so a window that never transitions still
    /// accumulates — the case a saturated fleet turning everything away produces.
    async fn sample(&self, cancel: CancellationToken) {
        let mut ticker = tokio::time::interval(OCCUPANCY_TICK);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    self.record(OccupancyEvent::Flush);
                    return;
                },
                _ = ticker.tick() => {
                    self.record(OccupancyEvent::Flush);
                }
            }
        }
    }
}

/// Tracks all existing streams
pub struct TaskManager {
    network_client: Arc<NetworkClient>,
    running_tasks: AtomicUsize,
    max_tasks: usize,
    occupancy: Occupancy,
    task_limit_retry_after: Duration,
    next_stream_index: AtomicU32,
    bandwidth_utilization_threshold: f64,
    priority_stride: u32,
}

impl TaskManager {
    pub fn new(network_client: Arc<NetworkClient>, config: &Config) -> TaskManager {
        metrics::STREAMS_LIMIT.set(config.max_parallel_streams as i64);
        TaskManager {
            network_client,
            running_tasks: 0.into(),
            max_tasks: config.max_parallel_streams,
            occupancy: Occupancy::new(config.max_parallel_streams),
            task_limit_retry_after: config.task_limit_retry_after,
            next_stream_index: AtomicU32::new(0),
            bandwidth_utilization_threshold: config.congestion.headroom_threshold,
            priority_stride: config.congestion.priority_stride,
        }
    }

    pub async fn spawn_stream(
        self: Arc<Self>,
        mut request: StreamRequest,
    ) -> Result<impl Stream<Item = ResponseChunk>, RequestError> {
        let running_tasks = self.running_tasks.fetch_add(1, Ordering::Relaxed);
        if running_tasks >= self.max_tasks {
            self.running_tasks.fetch_sub(1, Ordering::Relaxed);
            metrics::report_stream_refused(RefusalReason::TaskLimit);
            return Err(RequestError::BusyFor(self.task_limit_retry_after));
        }

        if let Some(util) = self.network_client.download_utilization() {
            if util > self.bandwidth_utilization_threshold {
                self.running_tasks.fetch_sub(1, Ordering::Relaxed);
                metrics::report_stream_refused(RefusalReason::Bandwidth);
                return Err(RequestError::BusyFor(Duration::from_secs(1)));
            }
        }

        self.occupancy.record(OccupancyEvent::Started);
        metrics::ACTIVE_STREAMS.inc();

        let self_clone = self.clone();
        let guard = scopeguard::guard((), move |()| {
            // Record the finish before releasing the admission slot.
            self_clone.occupancy.record(OccupancyEvent::Finished);
            metrics::ACTIVE_STREAMS.dec();
            self_clone.running_tasks.fetch_sub(1, Ordering::Relaxed);
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

    /// Periodically flush unchanged occupancy until cancelled.
    pub async fn observe_occupancy(self: Arc<Self>, cancel: CancellationToken) {
        self.occupancy.sample(cancel).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn occupancy_intervals_follow_admitted_stream_transitions() {
        let start = Instant::now();
        let mut state = OccupancyState::at(2, start);

        let idle = state.record(OccupancyEvent::Started, start + Duration::from_millis(100));
        let one = state.record(OccupancyEvent::Started, start + Duration::from_millis(300));
        let saturated = state.record(OccupancyEvent::Finished, start + Duration::from_millis(450));
        let draining = state.record(OccupancyEvent::Flush, start + Duration::from_secs(1));

        assert_eq!(
            (idle.running, idle.elapsed),
            (0, Duration::from_millis(100))
        );
        assert_eq!((one.running, one.elapsed), (1, Duration::from_millis(200)));
        assert_eq!(
            (saturated.running, saturated.elapsed),
            (2, Duration::from_millis(150))
        );
        assert_eq!(
            (draining.running, draining.elapsed),
            (1, Duration::from_millis(550))
        );
    }

    #[test]
    fn repeated_flushes_cannot_replay_a_missed_interval() {
        let start = Instant::now();
        let delayed = start + Duration::from_secs(5);
        let mut state = OccupancyState::at(1, start);
        state.record(OccupancyEvent::Started, start);

        let first = state.record(OccupancyEvent::Flush, delayed);
        let replay = state.record(OccupancyEvent::Flush, delayed);

        assert_eq!((first.running, first.elapsed), (1, Duration::from_secs(5)));
        assert_eq!(replay.elapsed, Duration::ZERO);
    }

    /// The one thing the state machine alone cannot show. A window pinned at the cap has
    /// no transition to close its interval — that is the shape of the incident, every
    /// slot taken and every arrival refused — so only the sampler can put it on the
    /// counter, and it has to do so *while the window is still open*. Asserting after the
    /// fact proves nothing: the flush on cancel would settle the whole interval at once.
    #[tokio::test(start_paused = true)]
    async fn a_saturated_window_is_visible_before_it_ends() {
        let occupancy = Arc::new(Occupancy::new(1));
        occupancy.record(OccupancyEvent::Started);

        let streams_before = metrics::stream_seconds();
        let saturated_before = metrics::saturated_seconds();

        let cancel = CancellationToken::new();
        let sampler = tokio::spawn({
            let (occupancy, cancel) = (occupancy.clone(), cancel.clone());
            async move { occupancy.sample(cancel).await }
        });

        // A scrape lands ten seconds in, with nothing having transitioned and nothing
        // about to. Publication lag is bounded by one tick.
        tokio::time::sleep(Duration::from_secs(10)).await;
        let unpublished = 10. - (metrics::saturated_seconds() - saturated_before);
        assert!(
            unpublished <= OCCUPANCY_TICK.as_secs_f64() + 1e-6,
            "a scrape during the incident must witness it; {unpublished}s went unpublished"
        );

        cancel.cancel();
        sampler.await.unwrap();

        // And the integral is exact once the window closes, tick boundaries regardless.
        let elapsed = metrics::stream_seconds() - streams_before;
        assert!(
            (elapsed - 10.).abs() < 1e-6,
            "one stream held for ten seconds is ten stream-seconds, got {elapsed}"
        );
    }
}
