//! Streaming query controller.
//!
//! [`StreamController`] produces an ordered stream of response chunks for a
//! client query that may span many dataset chunks. Internally it keeps a
//! sliding buffer of in-flight per-chunk requests, drains the front in chunk
//! order, and refills the back speculatively.
//!
//! ```text
//! poll_next(ctx)
//!   |- try_fill_slots: append new chunk slots while buffer capacity allows it
//!   |- poll_chunk_slot: advance each active worker request
//!   `- pop_response: drain the front chunk slot in order
//!
//! buffer: SlidingArray<ChunkSlot>
//! ChunkSlot {
//!     buffered: VecDeque<BufferedResponse>, // ready parts for this chunk
//!     active: Option<Slot>,                 // pending request or held ready part
//! }
//!
//! Slot.state:
//! NoWorkers / Paused -> Pending -> Done
//!                           `-> Partial
//!
//! Partial results are buffered and continued eagerly only while the per-chunk stored-result cap
//! leaves room for the next active response. Otherwise the active partial is held until the client
//! drains a buffered part.
//! ```
//!
//! Every worker query is an [`Attempt`], settled once by why the controller let go of it
//! (OB-16). A winning answer carries its attempt until handed on, so read-ahead the client
//! never takes is abandoned; the controller's drop abandons everything it still holds with
//! the one thing it knows about the stream's end, whether an error had been yielded.

#![allow(unstable_name_collisions)]

use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures::FutureExt;
use sqd_contract_client::PeerId;
use tokio::time::Instant;
use tracing::{instrument, Instrument};

use crate::{
    controller::{attempt::Attempt, timeouts::TimeoutManager},
    metrics::{
        self, AttemptKind, AttemptOutcome, AttemptSignals, ChunkOutcome, RefusalReason, StreamEnd,
    },
    network::{ChunkNotFound, NetworkClient, NoWorker, QueryResult, StreamingNetwork, WorkerLease},
    types::{
        BlockRange, ChunkId, DataChunk, ErrorCode, ExhaustionClass, QueryError, RequestError,
        ResponseChunk, SendQueryError, StreamRequest,
    },
    utils::{logging::StreamStats, SlidingArray},
};

const MAX_IDLE_TIME: Duration = Duration::from_millis(1000);

pub struct StreamController<N: StreamingNetwork = NetworkClient> {
    request: StreamRequest,
    network: Arc<N>,
    buffer: SlidingArray<ChunkSlot>,
    next_chunk: Option<DataChunk>,
    timeouts: TimeoutManager,
    stats: StreamStats,
    span: tracing::Span,
    last_error: Option<String>,
    /// An error was yielded; consumers stop at the first one, so the stream is over and
    /// whatever is still held is abandoned because of it.
    ended_in_error: bool,
    stream_index: u32,
    priority_stride: u32,
    /// The configured dataset name every attempt is charged to.
    dataset: Arc<str>,
    signals: Arc<AttemptSignals>,
}

#[derive(Clone)]
pub struct DataRange {
    pub range: BlockRange,
    pub chunk: DataChunk,
    pub chunk_index: usize,
    /// The rest of a range a worker answered in part.
    pub continuation: bool,
}

struct Slot {
    data_range: DataRange,
    state: RequestState,
}

struct ChunkSlot {
    active: Option<Slot>,
    buffered: VecDeque<BufferedResponse>,
    /// A query was dispatched for this chunk, so it settles as an OB-16 chunk outcome.
    dispatched: bool,
}

struct BufferedResponse {
    chunk_index: usize,
    read_range: BlockRange,
    result: Result<Payload, RequestError>,
}

/// A worker's answer, with the attempt that fetched it.
struct Payload {
    data: ResponseChunk,
    attempt: Attempt,
}

enum RequestState {
    /// Couldn't find enough workers now.
    /// Retry whenever the stream is polled, or give up if this is the first slot.
    NoWorkers,
    /// It's known how long to wait until the workers will become available.
    /// If this is the first slot, it may be worth to wait.
    Paused(PausedState),
    /// Workers are allocated and queries to some of them are running. Waiting for the results.
    Pending(PendingRequests),
    /// We've got a successful result from one of the workers but it didn't cover the whole chunk
    /// range. A continuation request can be sent once buffering it won't exceed the per-chunk cap.
    Partial(PartialResult),
    /// Either a successful result has been received, or all the attempts have failed.
    Done(Result<Payload, RequestError>),
}

struct PausedState {
    until: Instant,
    timeout: Pin<Box<tokio::time::Sleep>>,
}

struct PendingRequests {
    requests: Vec<WorkerRequest>,
    timeout: Pin<Box<tokio::time::Sleep>>,
    timeout_duration: Duration,
}

struct PartialResult {
    data: Payload,
    next_range: BlockRange,
}

enum WorkerRequest {
    NotStarted(ReservedWorker),
    Running(RunningWorkerRequest),
    Finished(FinishedWorkerRequest),
}

struct ReservedWorker {
    lease: Option<WorkerLease>,
}

struct RunningWorkerRequest {
    resp: tokio::task::JoinHandle<QueryResult>,
    start_time: tokio::time::Instant,
    worker: PeerId,
    /// Taken when the request finishes or loses; if still here on drop, abandoned.
    attempt: Option<Attempt>,
}

struct FinishedWorkerRequest {
    result: QueryResult,
    worker: PeerId,
}

impl Drop for RunningWorkerRequest {
    fn drop(&mut self) {
        self.resp.abort();
    }
}

enum UpdateStatus {
    Updated,
    NotUpdated,
}

struct PendingPollSummary {
    not_started: usize,
    running: usize,
    newly_finished: usize,
}

enum PendingSlotPoll {
    Updated(RequestState),
    NotUpdated,
}

impl UpdateStatus {
    fn updated(&self) -> bool {
        matches!(self, UpdateStatus::Updated)
    }
}

impl<N: StreamingNetwork> StreamController<N> {
    pub fn new(
        request: StreamRequest,
        network: Arc<N>,
        stream_index: u32,
        priority_stride: u32,
    ) -> Result<Self, RequestError> {
        let first_block = request.query.first_block();

        let first_chunk = match network.find_chunk(&request.dataset_id, first_block) {
            Ok(first_chunk) => first_chunk,
            Err(ChunkNotFound::BeforeFirst { first_block }) => {
                return Err(RequestError::BadRequest(format!(
                    "dataset starts from block {}",
                    first_block
                )))
            }
            Err(ChunkNotFound::AfterLast) => {
                return Err(RequestError::NoData);
            }
            Err(e) => {
                // Should not be the case under normal operation
                return Err(RequestError::Internal(format!(
                    "block {} could not be found in dataset {} ({e}), please report this to the developers",
                    first_block, request.dataset_id
                )));
            }
        };

        // Gap resolution can return a chunk on either side of the requested range.
        if request
            .query
            .intersect_with(&first_chunk.block_range())
            .is_none()
        {
            return Err(RequestError::NoData);
        }

        let signals = AttemptSignals::bind();
        signals.prime(&request.dataset_name);
        Ok(Self {
            network,
            buffer: SlidingArray::with_capacity(request.buffer_size),
            next_chunk: Some(first_chunk),
            timeouts: TimeoutManager::new(request.timeout_quantile),
            dataset: Arc::from(request.dataset_name.as_str()),
            request,
            stats: StreamStats::new(),
            span: tracing::Span::current(),
            last_error: None,
            ended_in_error: false,
            stream_index,
            priority_stride,
            signals: Arc::new(signals),
        })
    }

    #[cfg(test)]
    fn with_signals(mut self, signals: Arc<AttemptSignals>) -> Self {
        self.signals = signals;
        self
    }

    pub fn poll_next(
        &mut self,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Result<ResponseChunk, RequestError>>> {
        self.last_error = None;

        if self.buffer.next_index() == 0 {
            self.try_fill_slots(ctx);
        }

        // extract this field to be able to pass both its values and `&mut self` to the method
        let mut buffer = std::mem::take(&mut self.buffer);
        let mut updated = false;
        for chunk_slot in buffer.iter_mut() {
            updated |= self.poll_chunk_slot(chunk_slot, ctx).updated();
        }
        self.buffer = buffer;

        if updated {
            tracing::debug!(
                "Buffer: [{}]",
                self.buffer
                    .data()
                    .iter()
                    .map(ChunkSlot::debug_symbol)
                    .collect::<String>()
            );
        }
        self.observe_max_chunk_parts();
        self.stats.maybe_write_log();

        let result = self.pop_response(ctx);

        self.try_fill_slots(ctx);

        if let Poll::Ready(Some(Err(e))) = &result {
            self.last_error = Some(e.to_string());
            self.ended_in_error = true;
        }

        result
    }

    fn poll_chunk_slot(
        &mut self,
        chunk_slot: &mut ChunkSlot,
        ctx: &mut Context<'_>,
    ) -> UpdateStatus {
        let mut updated = false;

        if let Some(mut slot) = chunk_slot.active.take() {
            updated |= self.poll_slot(&mut slot, ctx).updated();
            chunk_slot.dispatched |= slot.is_querying();
            chunk_slot.active = Some(slot);
        }

        updated |= self.buffer_active_response(chunk_slot, ctx);

        if updated {
            UpdateStatus::Updated
        } else {
            UpdateStatus::NotUpdated
        }
    }

    #[instrument(skip_all, level="debug", fields(chunk_index = slot.data_range.chunk_index))]
    fn poll_slot(&mut self, slot: &mut Slot, ctx: &mut Context<'_>) -> UpdateStatus {
        match &mut slot.state {
            RequestState::Pending(pending) => {
                match self.poll_pending_slot(&slot.data_range, pending, ctx) {
                    PendingSlotPoll::Updated(state) => {
                        slot.state = state;
                        UpdateStatus::Updated
                    }
                    PendingSlotPoll::NotUpdated => UpdateStatus::NotUpdated,
                }
            }
            _ => self.poll_deferred_slot(slot, ctx),
        }
    }

    fn poll_deferred_slot(&mut self, slot: &mut Slot, ctx: &mut Context<'_>) -> UpdateStatus {
        match &mut slot.state {
            RequestState::NoWorkers => {}
            RequestState::Paused(state) => match state.timeout.as_mut().poll_unpin(ctx) {
                Poll::Pending => return UpdateStatus::NotUpdated,
                Poll::Ready(_) => {}
            },
            _ => return UpdateStatus::NotUpdated,
        }

        match self.start_querying_chunk(slot.data_range.clone(), ctx) {
            Ok(s) => {
                *slot = s;
                UpdateStatus::Updated
            }
            Err(err) => {
                let (s, e) = *err;
                *slot = s;
                self.last_error = Some(e.to_string());
                if matches!(slot.state, RequestState::NoWorkers) {
                    UpdateStatus::NotUpdated
                } else {
                    UpdateStatus::Updated
                }
            }
        }
    }

    fn poll_pending_slot(
        &mut self,
        data_range: &DataRange,
        pending: &mut PendingRequests,
        ctx: &mut Context<'_>,
    ) -> PendingSlotPoll {
        match self.poll_worker_requests(data_range, pending, ctx) {
            Ok(summary) => self.advance_pending_slot(data_range, pending, summary, ctx),
            Err(state) => PendingSlotPoll::Updated(state),
        }
    }

    fn poll_worker_requests(
        &mut self,
        data_range: &DataRange,
        pending: &mut PendingRequests,
        ctx: &mut Context<'_>,
    ) -> Result<PendingPollSummary, RequestState> {
        let mut summary = PendingPollSummary {
            not_started: 0,
            running: 0,
            newly_finished: 0,
        };

        let polled = pending.requests.iter_mut().try_for_each(|request| {
            self.poll_worker_request(data_range, request, &mut summary, ctx)
        });
        if let Err(state) = polled {
            // The range is settled, and whatever else is still running is let go. Only an
            // answer wins a race; a terminal error ends the range with no winner, and
            // the rest are cancelled, not beaten.
            let outcome = match &state {
                RequestState::Done(Err(_)) => AttemptOutcome::Cancelled,
                _ => AttemptOutcome::Superseded,
            };
            pending.settle_running(outcome);
            return Err(state);
        }

        Ok(summary)
    }

    fn poll_worker_request(
        &mut self,
        data_range: &DataRange,
        request: &mut WorkerRequest,
        summary: &mut PendingPollSummary,
        ctx: &mut Context<'_>,
    ) -> Result<(), RequestState> {
        match request {
            WorkerRequest::NotStarted(_) => {
                summary.not_started += 1;
            }
            WorkerRequest::Finished(_) => {}
            WorkerRequest::Running(running) => {
                let Poll::Ready(response) = running.resp.poll_unpin(ctx) else {
                    summary.running += 1;
                    return Ok(());
                };
                summary.newly_finished += 1;
                let attempt = running.attempt.take().expect("taken only once finished");
                // From here the controller holds the result; a task can also answer
                // after its query is let go, which the stage keeps apart from this.
                attempt.mark_read();

                // This is intentionally measured when the result has been polled, not when it's ready.
                // If the stream is consumed slower than generated, this duration may get
                // significantly higher than the response time.
                // This way the extra "follow up" queries won't be sent, saving on the number of queries.
                let duration = running.start_time.elapsed();
                self.timeouts.observe(duration);

                let response = match response {
                    Ok(res) => res,
                    Err(join_err) => {
                        tracing::error!(
                            "Worker query task failed in {}ms for {}: {}",
                            duration.as_millis(),
                            running.worker,
                            join_err,
                        );
                        attempt.settle(AttemptOutcome::Failed);
                        return Err(RequestState::Done(Err(RequestError::Internal(format!(
                            "worker query task failed: {join_err}"
                        )))));
                    }
                };
                // Only what this layer rejects: the network reports its own, so
                // reporting every `Integrity` counted a bad signature twice.
                let response = match wrong_range(&response, &data_range.range) {
                    Some(reason) => {
                        tracing::warn!(
                            "Discarding invalid response for range {}-{} from worker {}: {}",
                            data_range.range.start(),
                            data_range.range.end(),
                            running.worker,
                            reason,
                        );
                        self.network.report_integrity_failure(running.worker);
                        Err(QueryError::Integrity(reason))
                    }
                    None => response,
                };

                if retriable(&response) {
                    tracing::debug!(
                        "Got retriable error in {}ms from {}: {}",
                        duration.as_millis(),
                        running.worker,
                        response.as_ref().unwrap_err().to_string(),
                    );
                    attempt.settle(AttemptOutcome::Failed);
                    *request = WorkerRequest::Finished(FinishedWorkerRequest {
                        result: response,
                        worker: running.worker,
                    });
                } else {
                    // Work is over for this slot. All the remaining requests will be cancelled.
                    return Err(parse_response(
                        response,
                        &data_range.range,
                        running.worker,
                        duration,
                        attempt,
                    ));
                }
            }
        }

        Ok(())
    }

    fn advance_pending_slot(
        &mut self,
        data_range: &DataRange,
        pending: &mut PendingRequests,
        summary: PendingPollSummary,
        ctx: &mut Context<'_>,
    ) -> PendingSlotPoll {
        if summary.running == 0 && summary.not_started == 0 {
            return PendingSlotPoll::Updated(Self::all_attempts_failed(pending));
        }

        let timed_out = pending.timeout.as_mut().poll(ctx).is_ready();
        let should_retry = summary.newly_finished > 0 || timed_out || summary.running == 0;

        if should_retry && summary.not_started > 0 {
            let kind = match (
                summary.not_started == pending.requests.len(),
                summary.running,
            ) {
                (true, _) if data_range.continuation => AttemptKind::Continuation,
                (true, _) => AttemptKind::First,
                (false, 0) => AttemptKind::Retry,
                (false, _) => AttemptKind::Hedge,
            };
            self.start_next_attempt(data_range, pending, timed_out, kind, ctx);
            return PendingSlotPoll::NotUpdated;
        }

        if should_retry {
            // The last query attempt timed out, wait for the rest to complete
            assert!(summary.running > 0);
        }

        PendingSlotPoll::NotUpdated
    }

    fn all_attempts_failed(pending: &mut PendingRequests) -> RequestState {
        let mut errors = Vec::with_capacity(pending.requests.len());
        let mut classes = Vec::with_capacity(pending.requests.len());
        for request in pending.requests.drain(..) {
            let WorkerRequest::Finished(f) = request else {
                unreachable!("all worker requests should be finished")
            };
            let error = f.result.unwrap_err();
            classes.push(error.exhaustion_class());
            // Format from the QueryError directly so every attempt is labeled with its
            // worker. Going through RequestError would drop the peer id for variants whose
            // Display is a fixed string (e.g. RateLimitExceeded, BaseBlockMismatch).
            errors.push(format!("worker {}: {}", f.worker, error));
        }
        // A class is claimed only when every attempt agrees; a mixed run stays
        // transient, and an empty one claims nothing.
        let unanimous = classes
            .first()
            .copied()
            .filter(|first| classes.iter().all(|c| c == first));
        let message = errors.join("; ");

        RequestState::Done(Err(match unanimous {
            Some(ExhaustionClass::Integrity) => {
                RequestError::Failure(format!("All query attempts failed: {message}"))
            }
            Some(ExhaustionClass::Capacity) => {
                tracing::debug!("All query attempts refused for capacity: {message}");
                RequestError::RateLimitExceeded
            }
            // RetriesExhausted prefixes the message itself.
            _ => RequestError::RetriesExhausted(message),
        }))
    }

    fn start_next_attempt(
        &mut self,
        data_range: &DataRange,
        pending: &mut PendingRequests,
        timed_out: bool,
        kind: AttemptKind,
        ctx: &mut Context<'_>,
    ) {
        if timed_out {
            tracing::trace!(
                "Request didn't complete in {}ms, sending one more query",
                pending.timeout_duration.as_millis()
            );
        }

        for req in &mut pending.requests {
            if let WorkerRequest::NotStarted(worker) = req {
                let lease = worker
                    .lease
                    .take()
                    .expect("worker lease should only be used once");
                let request = self.send_query(data_range, lease, kind);
                *req = WorkerRequest::Running(request);
                pending.set_timeout(self.timeouts.current_timeout(), ctx);
                // A task wakes only for what it has polled, and this query has not been
                // polled yet: ask for another pass, so its answer wakes the stream.
                ctx.waker().wake_by_ref();
                break;
            }
        }
    }

    fn pop_response(
        &mut self,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Result<ResponseChunk, RequestError>>> {
        let Some(mut chunk_slot) = self.buffer.pop_front() else {
            return Poll::Ready(None);
        };

        if let Some(response) = chunk_slot.buffered.pop_front() {
            self.buffer_active_response(&mut chunk_slot, ctx);
            if chunk_slot.is_empty() {
                self.settle_chunk(&chunk_slot, chunk_outcome(&response.result));
            } else {
                self.buffer.push_front(chunk_slot);
            }
            return self.ready_response(response);
        }

        let Some(slot) = chunk_slot.active.take() else {
            return Poll::Pending;
        };

        let chunk_index = slot.data_range.chunk_index;
        let (result, read_range, next_slot) = match slot.state {
            RequestState::Done(result) => {
                self.settle_chunk(&chunk_slot, chunk_outcome(&result));
                let result = result.map(Payload::deliver);
                (Poll::Ready(Some(result)), slot.data_range.range, None)
            }
            RequestState::NoWorkers => {
                self.settle_chunk(&chunk_slot, ChunkOutcome::Failed);
                // We don't know how long we'll have to wait, so give up immediately
                return Poll::Ready(Some(Err(RequestError::Unavailable)));
            }
            RequestState::Paused(ref s) => {
                // All workers are rate-limited, try to pause and continue streaming
                let duration = s.until.duration_since(Instant::now());
                if duration > MAX_IDLE_TIME {
                    metrics::report_stream_refused(RefusalReason::WorkersPaused);
                    self.settle_chunk(&chunk_slot, ChunkOutcome::Failed);
                    return Poll::Ready(Some(Err(RequestError::BusyFor(duration))));
                } else {
                    // TODO: fix calculation in case we're polling the same paused slot multiple times
                    self.stats.throttled(duration);
                }
                chunk_slot.active = Some(slot);
                self.buffer.push_front(chunk_slot);
                return Poll::Pending;
            }
            RequestState::Pending(_) => {
                // The query is still running, keep waiting
                chunk_slot.active = Some(slot);
                self.buffer.push_front(chunk_slot);
                return Poll::Pending;
            }
            RequestState::Partial(partial) => {
                // Return the partial result and schedule the continuation query
                let (response, next_data_range) =
                    into_partial_continuation(slot.data_range, partial);
                let BufferedResponse {
                    result,
                    read_range,
                    chunk_index: _,
                } = response;
                let slot = match self.start_querying_chunk(next_data_range, ctx) {
                    Ok(slot) => slot,
                    Err(err) => {
                        let (slot, e) = *err;
                        tracing::debug!("Couldn't schedule continuation request: {e:?}");
                        slot
                    }
                };
                let result = result.map(Payload::deliver);
                (Poll::Ready(Some(result)), read_range, Some(slot))
            }
        };

        if let Some(slot) = next_slot {
            chunk_slot.active = Some(slot);
            self.buffer.push_front(chunk_slot);
        }

        self.observe_response(chunk_index, &read_range, &result);
        result
    }

    fn ready_response(
        &mut self,
        response: BufferedResponse,
    ) -> Poll<Option<Result<ResponseChunk, RequestError>>> {
        let result = Poll::Ready(Some(response.result.map(Payload::deliver)));
        self.observe_response(response.chunk_index, &response.read_range, &result);
        result
    }

    fn settle_chunk(&self, chunk_slot: &ChunkSlot, outcome: ChunkOutcome) {
        if chunk_slot.dispatched {
            self.signals.chunk(&self.dataset, outcome);
        }
    }

    fn observe_response(
        &mut self,
        chunk_index: usize,
        read_range: &BlockRange,
        result: &Poll<Option<Result<ResponseChunk, RequestError>>>,
    ) {
        match result {
            Poll::Ready(Some(Ok(bytes))) => {
                self.stats
                    .sent_response_chunk(*read_range.end() - *read_range.start() + 1, bytes.len());
                tracing::trace!(
                    chunk_index,
                    "Writing response blocks {}-{} ({} bytes)",
                    *read_range.start(),
                    *read_range.end(),
                    bytes.len()
                );
            }
            // Once per stream, not once per failed chunk: both consumers — `spawn_stream`
            // and the test collector — stop at the first `Err`, so no later chunk's
            // refusal is ever surfaced.
            Poll::Ready(Some(Err(RequestError::RateLimitExceeded))) => {
                metrics::report_stream_refused(RefusalReason::WorkersRateLimited);
            }
            _ => {}
        }
    }

    fn buffer_active_response(
        &mut self,
        chunk_slot: &mut ChunkSlot,
        ctx: &mut Context<'_>,
    ) -> bool {
        let Some(slot) = chunk_slot.active.take() else {
            return false;
        };

        match slot.state {
            RequestState::Done(result)
                if chunk_slot
                    .can_buffer_terminal_response(self.request.max_stored_results_per_chunk) =>
            {
                chunk_slot.buffered.push_back(BufferedResponse {
                    chunk_index: slot.data_range.chunk_index,
                    read_range: slot.data_range.range,
                    result,
                });
                true
            }
            RequestState::Partial(partial)
                if chunk_slot.has_capacity_for_eager_continuation(
                    self.request.max_stored_results_per_chunk,
                ) =>
            {
                let (response, next_data_range) =
                    into_partial_continuation(slot.data_range, partial);
                chunk_slot.buffered.push_back(response);
                let next_slot = match self.start_querying_chunk(next_data_range, ctx) {
                    Ok(slot) => slot,
                    Err(err) => {
                        let (slot, e) = *err;
                        tracing::debug!("Couldn't schedule continuation request: {e:?}");
                        slot
                    }
                };
                chunk_slot.active = Some(next_slot);
                true
            }
            state => {
                chunk_slot.active = Some(Slot {
                    data_range: slot.data_range,
                    state,
                });
                false
            }
        }
    }

    fn try_fill_slots(&mut self, ctx: &mut Context<'_>) {
        if self.buffer.back().is_some_and(ChunkSlot::is_paused) {
            // Either the amount of compute units is low or the network is overloaded.
            // Don't send new queries until the existing ones complete.
            return;
        }
        while self.buffer.len() < self.request.buffer_size
            && self
                .request
                .max_chunks
                .is_none_or(|limit| self.buffer.total_size() < limit)
        {
            let Some(chunk) = self.next_chunk.take() else {
                break;
            };
            let next_index = self.buffer.total_size();
            match self.start_querying_chunk(
                DataRange {
                    range: chunk.block_range(),
                    chunk,
                    chunk_index: next_index,
                    continuation: false,
                },
                ctx,
            ) {
                Ok(slot) => {
                    let paused = slot.is_paused();
                    self.buffer.push_back(ChunkSlot::new(slot));
                    self.next_chunk = self.get_next_chunk(&chunk);
                    if paused {
                        break;
                    }
                }
                Err(err) => {
                    let (slot, e) = *err;
                    if !matches!(e, SendQueryError::NoWorkers) {
                        tracing::debug!("Couldn't schedule request: {e:?}");
                    }
                    if self.buffer.len() == 0 {
                        // Couldn't schedule a new request with no ongoing requests
                        // Return the error immediately
                        let chunk_slot = ChunkSlot::new(slot);
                        self.stats
                            .observe_chunk_parts(chunk_slot.stored_result_count());
                        self.buffer.push_back(chunk_slot);
                        // The pushed slot now owns this chunk. If the stream survives
                        // (`pop_response` keeps slots paused for less than MAX_IDLE_TIME
                        // alive), the slot is retried in place by `poll_deferred_slot`.
                        // Re-queueing the chunk into `next_chunk` as well would schedule
                        // a second slot for the same chunk once the backoff expires,
                        // duplicating the chunk's data in the response.
                        self.next_chunk = self.get_next_chunk(&chunk);
                    } else {
                        self.next_chunk = Some(chunk);
                    }
                    self.last_error = Some(e.to_string());
                    break;
                }
            }
        }
    }

    fn get_next_chunk(&self, chunk: &DataChunk) -> Option<DataChunk> {
        let next_chunk = self.network.next_chunk(&self.request.dataset_id, chunk);

        if let Some(next_chunk) = &next_chunk {
            if self
                .request
                .query
                .last_block()
                .is_some_and(|last_block| last_block < next_chunk.first_block)
            {
                tracing::debug!("The end of the requested range reached");
                return None;
            }
        } else {
            tracing::debug!("No more chunks available");
        }
        next_chunk
    }

    fn observe_max_chunk_parts(&mut self) {
        let max_chunk_parts = self
            .buffer
            .data()
            .iter()
            .map(ChunkSlot::stored_result_count)
            .max()
            .unwrap_or_default();
        self.stats.observe_chunk_parts(max_chunk_parts);
    }

    fn start_querying_chunk(
        &mut self,
        range: DataRange,
        ctx: &mut Context<'_>,
    ) -> Result<Slot, Box<(Slot, SendQueryError)>> {
        let block_range = self
            .request
            .query
            .intersect_with(&range.range)
            .expect("Chunk doesn't contain requested data");
        let data_range = range.with_range(block_range);

        let attempts = 1 + self.request.retries as usize;
        match self.pre_lease_workers(&data_range, attempts) {
            Ok(leases) => {
                let mut slot = Slot {
                    data_range,
                    state: RequestState::Pending(PendingRequests::new(
                        leases,
                        self.timeouts.current_timeout(),
                    )),
                };
                self.poll_slot(&mut slot, ctx);
                Ok(slot)
            }
            Err(err @ SendQueryError::NoWorkers) => {
                let slot = Slot {
                    data_range,
                    state: RequestState::NoWorkers,
                };
                Err(Box::new((slot, err)))
            }
            Err(err @ SendQueryError::Backoff(until)) => {
                let mut slot = Slot {
                    data_range,
                    state: RequestState::Paused(PausedState::new(until)),
                };
                self.poll_slot(&mut slot, ctx);
                Err(Box::new((slot, err)))
            }
        }
    }

    /// Leases `count` distinct workers for the given range, releasing all on any failure.
    ///
    /// Note that if enough distinct workers can't be found, some duplicates may be returned
    /// leading to equal queries sent in parallel to the same worker. It's not a problem now,
    /// but can be improved in the future.
    fn pre_lease_workers(
        &mut self,
        range: &DataRange,
        count: usize,
    ) -> Result<Vec<WorkerLease>, SendQueryError> {
        let mut workers = Vec::with_capacity(count);
        for _ in 0..count {
            match self
                .network
                .find_worker(&self.request.dataset_id, *range.range.start())
            {
                Ok(w) => workers.push(w),
                Err(e) => {
                    return Err(match e {
                        NoWorker::AllUnavailable => SendQueryError::NoWorkers,
                        NoWorker::Backoff(until) => SendQueryError::Backoff(until),
                    });
                }
            }
        }
        Ok(workers)
    }

    /// Sends a query to an already-leased worker.
    fn send_query(
        &mut self,
        range: &DataRange,
        lease: WorkerLease,
        kind: AttemptKind,
    ) -> RunningWorkerRequest {
        tracing::debug!(
            "Sending {} query for chunk {} ({}-{}) to worker {}",
            kind.as_str(),
            range.chunk_index,
            range.range.start(),
            range.range.end(),
            lease,
        );
        let query = if *range.range.start() == self.request.query.first_block() {
            self.request.query.to_string()
        } else {
            self.request.query.without_parent_hash()
        };
        let start_time = tokio::time::Instant::now();

        let priority = self.stream_index * self.priority_stride + range.chunk_index as u32;

        let worker = lease.worker();
        let (attempt, meter) = Attempt::start(kind, &self.dataset, &self.signals);
        let fut = self
            .network
            .clone()
            .query_worker(
                lease,
                self.request.request_id.to_string(),
                ChunkId::new(self.request.dataset_id.clone(), range.chunk),
                range.range.clone(),
                query,
                self.request.compression,
                Some(priority),
                meter,
            )
            .in_current_span();

        self.stats.query_sent();
        RunningWorkerRequest {
            resp: tokio::spawn(fut),
            start_time,
            worker,
            attempt: Some(attempt),
        }
    }
}

impl<N: StreamingNetwork> Drop for StreamController<N> {
    fn drop(&mut self) {
        let _enter = self.span.enter();
        // Everything still held is abandoned, with the one thing known about why: an
        // error had been yielded, or the consumer let go of a stream that had not ended.
        // Settled here, in place, so each attempt carries that reason rather than the
        // unknown its own drop would fall back to once the buffer goes.
        let end = if self.ended_in_error {
            StreamEnd::Error
        } else {
            StreamEnd::Unknown
        };
        for chunk_slot in self.buffer.iter_mut() {
            if chunk_slot.dispatched {
                self.signals
                    .chunk(&self.dataset, ChunkOutcome::Abandoned(end));
            }
            chunk_slot.abandon(end);
        }
        self.stats
            .write_summary(&self.request, self.last_error.take());
    }
}

impl<N: StreamingNetwork> futures::Stream for StreamController<N> {
    type Item = Result<ResponseChunk, RequestError>;

    fn poll_next(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Self::poll_next(Pin::into_inner(self), ctx)
    }
}

impl RequestState {
    fn debug_symbol(&self) -> char {
        match &self {
            RequestState::Pending(_) => '.',
            RequestState::Partial(_) => '+',
            RequestState::NoWorkers => '?',
            RequestState::Paused(_) => 'z',
            RequestState::Done(Ok(_)) => '#',
            RequestState::Done(Err(_)) => '!',
        }
    }
}

impl DataRange {
    fn with_range(self, range: BlockRange) -> Self {
        Self {
            range,
            chunk: self.chunk,
            chunk_index: self.chunk_index,
            continuation: self.continuation,
        }
    }
}

impl ChunkSlot {
    fn new(active: Slot) -> Self {
        Self {
            dispatched: active.is_querying(),
            active: Some(active),
            buffered: VecDeque::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.active.is_none() && self.buffered.is_empty()
    }

    fn is_paused(&self) -> bool {
        self.active.as_ref().is_some_and(Slot::is_paused)
    }

    fn stored_result_count(&self) -> usize {
        self.buffered.len()
            + usize::from(
                self.active
                    .as_ref()
                    .is_some_and(|slot| slot.state.has_stored_response()),
            )
    }

    fn can_buffer_terminal_response(&self, max_stored_results: usize) -> bool {
        self.buffered.len() < max_stored_results
    }

    // Starting a continuation can create one more ready result before the client drains this
    // chunk. Keep one slot in the cap for that active result, so the total stored results per
    // chunk (buffered responses plus active Partial/Done) stays within max_stored_results.
    fn has_capacity_for_eager_continuation(&self, max_stored_results: usize) -> bool {
        self.buffered.len() + 1 < max_stored_results
    }

    /// The stream ended with this chunk in hand: every attempt it still holds, answered
    /// and buffered or still in flight, is abandoned for that reason.
    fn abandon(&mut self, end: StreamEnd) {
        if let Some(slot) = &mut self.active {
            slot.state.abandon(end);
        }
        for response in &mut self.buffered {
            if let Ok(payload) = &mut response.result {
                payload.attempt.abandon(end);
            }
        }
    }

    fn debug_symbol(&self) -> char {
        if !self.buffered.is_empty() && self.active.is_some() {
            '+'
        } else if let Some(response) = self.buffered.front() {
            if response.result.is_ok() {
                '#'
            } else {
                '!'
            }
        } else if let Some(active) = &self.active {
            active.state.debug_symbol()
        } else {
            '-'
        }
    }
}

impl Slot {
    fn is_paused(&self) -> bool {
        matches!(&self.state, RequestState::Paused(_))
    }

    /// Pending is entered only by dispatching the range's first query.
    fn is_querying(&self) -> bool {
        matches!(&self.state, RequestState::Pending(_))
    }
}

impl Payload {
    fn deliver(self) -> ResponseChunk {
        self.attempt.settle(AttemptOutcome::Delivered);
        self.data
    }
}

fn chunk_outcome<T>(result: &Result<T, RequestError>) -> ChunkOutcome {
    match result {
        Ok(_) => ChunkOutcome::Delivered,
        Err(_) => ChunkOutcome::Failed,
    }
}

fn into_partial_continuation(
    data_range: DataRange,
    partial: PartialResult,
) -> (BufferedResponse, DataRange) {
    let PartialResult { data, next_range } = partial;
    let read_range = BlockRange::new(*data_range.range.start(), *next_range.start() - 1);
    let next_data_range = DataRange {
        continuation: true,
        ..data_range.with_range(next_range)
    };
    let response = BufferedResponse {
        chunk_index: next_data_range.chunk_index,
        read_range,
        result: Ok(data),
    };
    (response, next_data_range)
}

impl RequestState {
    fn has_stored_response(&self) -> bool {
        matches!(self, RequestState::Partial(_) | RequestState::Done(_))
    }

    fn abandon(&mut self, end: StreamEnd) {
        match self {
            RequestState::Pending(pending) => pending.abandon(end),
            RequestState::Partial(partial) => partial.data.attempt.abandon(end),
            RequestState::Done(Ok(payload)) => payload.attempt.abandon(end),
            RequestState::Done(Err(_)) | RequestState::NoWorkers | RequestState::Paused(_) => {}
        }
    }
}

impl PausedState {
    fn new(until: Instant) -> Self {
        Self {
            until,
            timeout: Box::pin(tokio::time::sleep_until(until)),
        }
    }
}

impl PendingRequests {
    fn new(leases: impl IntoIterator<Item = WorkerLease>, timeout: Duration) -> Self {
        Self {
            requests: leases
                .into_iter()
                .map(|lease| {
                    let peer_id = lease.worker();
                    let result = WorkerRequest::NotStarted(ReservedWorker { lease: Some(lease) });
                    tracing::trace!("Reserved worker {}", peer_id);
                    result
                })
                .collect(),
            timeout: Box::pin(tokio::time::sleep(timeout)),
            timeout_duration: timeout,
        }
    }

    fn set_timeout(&mut self, timeout: Duration, ctx: &mut Context<'_>) {
        self.timeout = Box::pin(tokio::time::sleep(timeout));
        self.timeout_duration = timeout;
        assert!(self.timeout.poll_unpin(ctx).is_pending()); // pass the context to wake
    }

    /// Settles every attempt still running, once the range no longer needs any of them.
    fn settle_running(&mut self, outcome: AttemptOutcome) {
        for request in &mut self.requests {
            if let WorkerRequest::Running(running) = request {
                if let Some(attempt) = running.attempt.take() {
                    attempt.settle(outcome);
                }
            }
        }
    }

    fn abandon(&mut self, end: StreamEnd) {
        self.settle_running(AttemptOutcome::Abandoned(end));
    }
}

fn parse_response(
    response: QueryResult,
    range: &BlockRange,
    worker: PeerId,
    duration: Duration,
    attempt: Attempt,
) -> RequestState {
    let s = match response {
        Ok(success) => success,
        Err(e) => {
            attempt.settle(AttemptOutcome::Failed);
            let error = RequestError::from_query_error(e, worker);
            tracing::debug!(
                "Got error in {}ms from {}: {}",
                duration.as_millis(),
                worker,
                error.to_string(),
            );
            return RequestState::Done(Err(error));
        }
    };
    let result = s.ok;

    let throughput = if s.transfer_time.is_zero() {
        0.0
    } else {
        (s.response_size as f64 / (1024.0 * 1024.0)) / s.transfer_time.as_secs_f64()
    };

    let last_block = result.last_block;

    let state = if last_block == *range.end() {
        RequestState::Done(Ok(Payload {
            data: result.data,
            attempt,
        }))
    } else if last_block < *range.start() {
        // Unreachable: `check_response_range` rejects this before the response
        // gets here. Kept because falling through would emit blocks below the
        // requested start (INV-21) instead of failing.
        attempt.settle(AttemptOutcome::Failed);
        RequestState::Done(Err(RequestError::Failure(format!(
            "worker {worker} returned last block {last_block} below the first queried block {}",
            range.start(),
        ))))
    } else {
        RequestState::Partial(PartialResult {
            data: Payload {
                data: result.data,
                attempt,
            },
            next_range: BlockRange::new(last_block + 1, *range.end()),
        })
    };

    tracing::debug!(
        "Got result ({}) in {}ms from {}, {:.1} KB, ttfb={:.1?}, \
            transfer={:.1?}, throughput={throughput:.2} MB/s",
        short_code(&state),
        duration.as_millis(),
        worker,
        s.response_size as f64 / 1024.0,
        s.ttfb,
        s.transfer_time
    );

    state
}

/// Why a response falls outside the range this slot asked for, if it does.
///
/// The network rejects these at the source. This is the delivery boundary refusing to
/// emit them (INV-21) whatever a [`StreamingNetwork`] hands it — unreachable in
/// production, which is the point.
fn wrong_range(response: &QueryResult, range: &BlockRange) -> Option<String> {
    let last_block = response.as_ref().ok()?.ok.last_block;
    let bound = if last_block > *range.end() {
        format!("beyond the queried range end {}", range.end())
    } else if last_block < *range.start() {
        format!("below the first queried block {}", range.start())
    } else {
        return None;
    };
    Some(format!("worker returned last block {last_block} {bound}"))
}

fn retriable(result: &QueryResult) -> bool {
    match result {
        Ok(_) => false,
        Err(QueryError::BadRequest(_)) => false,
        Err(QueryError::Retriable(_)) => true,
        // Rerouted like a transient failure; only the exhaustion class differs.
        Err(QueryError::Integrity(_)) => true,
        Err(QueryError::Failure(_)) => false,
        Err(QueryError::RateLimitExceeded) => true,
        Err(QueryError::BaseBlockMismatch(_)) => false,
    }
}

fn short_code(result: &RequestState) -> &'static str {
    match result {
        RequestState::Done(Ok(_)) => "ok",
        // NoData is the one variant with no code: a 204 is not a failure.
        RequestState::Done(Err(RequestError::NoData)) => "no_data",
        RequestState::Done(Err(e)) => e.code().map_or("-", ErrorCode::as_str),
        RequestState::Partial(_) => "partial",
        RequestState::Pending(_) | RequestState::Paused(_) | RequestState::NoWorkers => "-",
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    /// The configured name every test stream carries.
    const DATASET: &str = "test-dataset";

    fn test_chunk() -> DataChunk {
        DataChunk::from_str("0000000000/0000000100-0000000200-abcde").unwrap()
    }

    fn data_range(start: u64, end: u64) -> DataRange {
        DataRange {
            range: BlockRange::new(start, end),
            chunk: test_chunk(),
            chunk_index: 0,
            continuation: false,
        }
    }

    fn payload(data: &[u8]) -> Payload {
        Payload {
            data: data.to_vec().into(),
            attempt: Attempt::detached(),
        }
    }

    fn slot(state: RequestState) -> Slot {
        Slot {
            data_range: data_range(100, 199),
            state,
        }
    }

    fn partial_result(end: u64, last_returned: u64, data: &[u8]) -> PartialResult {
        PartialResult {
            data: payload(data),
            next_range: BlockRange::new(last_returned + 1, end),
        }
    }

    fn buffered_response() -> BufferedResponse {
        BufferedResponse {
            chunk_index: 0,
            read_range: 100..=149,
            result: Ok(payload(&[1, 2, 3])),
        }
    }

    #[test]
    fn partial_slot_is_split_into_done_part_and_continuation_range() {
        let data_range = data_range(100, 200);
        let partial = partial_result(200, 120, b"first");

        let (response, continuation) = into_partial_continuation(data_range, partial);

        assert_eq!(response.read_range, BlockRange::new(100, 120));
        assert_eq!(response.chunk_index, 0);
        match response.result {
            Ok(payload) => assert_eq!(payload.data.as_ref(), b"first"),
            Err(_) => panic!("partial data should become an emit-ready response"),
        }
        assert_eq!(continuation.range, BlockRange::new(121, 200));
        assert_eq!(continuation.chunk, test_chunk());
        assert_eq!(continuation.chunk_index, 0);
    }

    #[test]
    fn chunk_slot_keeps_completed_response_before_active_continuation() {
        let (response, continuation) =
            into_partial_continuation(data_range(100, 200), partial_result(200, 120, b"first"));
        let mut chunk_slot = ChunkSlot {
            active: None,
            buffered: VecDeque::new(),
            dispatched: true,
        };
        chunk_slot.buffered.push_back(response);
        chunk_slot.active = Some(Slot {
            data_range: continuation,
            state: RequestState::NoWorkers,
        });

        assert_eq!(chunk_slot.buffered.len(), 1);
        assert_eq!(
            chunk_slot.buffered.front().unwrap().read_range,
            BlockRange::new(100, 120)
        );
        assert_eq!(
            chunk_slot.active.as_ref().unwrap().data_range.range,
            BlockRange::new(121, 200)
        );
        assert!(matches!(
            chunk_slot.active.as_ref().unwrap().state,
            RequestState::NoWorkers
        ));
    }

    #[test]
    fn active_partial_counts_as_stored_result() {
        let chunk_slot = ChunkSlot {
            active: Some(slot(RequestState::Partial(PartialResult {
                data: payload(&[1]),
                next_range: 150..=199,
            }))),
            buffered: VecDeque::new(),
            dispatched: true,
        };

        assert_eq!(chunk_slot.stored_result_count(), 1);
    }

    #[test]
    fn partial_continuation_requires_capacity_for_next_active_result() {
        let mut chunk_slot = ChunkSlot {
            active: Some(slot(RequestState::Partial(PartialResult {
                data: payload(&[1]),
                next_range: 150..=199,
            }))),
            buffered: VecDeque::new(),
            dispatched: true,
        };

        assert!(!chunk_slot.has_capacity_for_eager_continuation(1));
        assert!(chunk_slot.has_capacity_for_eager_continuation(2));

        chunk_slot.buffered.push_back(buffered_response());

        assert!(!chunk_slot.has_capacity_for_eager_continuation(2));
    }

    #[test]
    fn terminal_response_can_use_last_capacity_slot() {
        let mut chunk_slot = ChunkSlot {
            active: Some(slot(RequestState::Done(Ok(payload(&[4, 5, 6]))))),
            buffered: VecDeque::new(),
            dispatched: true,
        };

        assert!(chunk_slot.can_buffer_terminal_response(1));

        chunk_slot.buffered.push_back(buffered_response());

        assert!(!chunk_slot.can_buffer_terminal_response(1));
    }

    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::{future::BoxFuture, StreamExt};

    use crate::controller::attempt::AttemptMeter;
    use crate::metrics::{AttemptCompletion, AttemptStage};
    use crate::network::QuerySuccess;
    use crate::types::{Compression, DatasetId, ParsedQuery, StreamRequest};

    /// A single-chunk dataset whose workers are all in a backoff until
    /// `backoff_until`; afterwards every query succeeds and returns the
    /// queried range's full data.
    struct MockNetwork {
        chunk: DataChunk,
        backoff_until: Instant,
        queries_sent: AtomicUsize,
    }

    impl StreamingNetwork for MockNetwork {
        fn find_chunk(
            &self,
            _dataset: &DatasetId,
            _block: u64,
        ) -> Result<DataChunk, ChunkNotFound> {
            Ok(self.chunk)
        }

        fn next_chunk(&self, _dataset: &DatasetId, _chunk: &DataChunk) -> Option<DataChunk> {
            None
        }

        fn find_worker(&self, _dataset: &DatasetId, _block: u64) -> Result<WorkerLease, NoWorker> {
            if Instant::now() < self.backoff_until {
                return Err(NoWorker::Backoff(self.backoff_until));
            }
            Ok(WorkerLease::for_tests(PeerId::random()))
        }

        fn query_worker(
            self: Arc<Self>,
            _lease: WorkerLease,
            _request_id: String,
            _chunk_id: ChunkId,
            block_range: BlockRange,
            _query: String,
            _compression: Compression,
            _priority: Option<u32>,
            _meter: AttemptMeter,
        ) -> BoxFuture<'static, QueryResult> {
            self.queries_sent.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                Ok(QuerySuccess {
                    ok: sqd_messages::QueryOk {
                        data: format!("data-{}-{}", block_range.start(), block_range.end())
                            .into_bytes()
                            .into(),
                        last_block: *block_range.end(),
                    },
                    ttfb: Duration::from_millis(1),
                    transfer_time: Duration::from_millis(1),
                    response_size: 10,
                })
            })
        }

        fn report_integrity_failure(&self, _worker: PeerId) {}
    }

    fn stream_request() -> StreamRequest {
        let query_json = r#"{
            "type": "evm",
            "fromBlock": 100,
            "toBlock": 150,
            "fields": {"block": {"number": true}},
            "includeAllBlocks": true
        }"#;
        StreamRequest {
            dataset_id: DatasetId::from_url("test-dataset"),
            dataset_name: "test-dataset".to_owned(),
            query: ParsedQuery::try_from(query_json.to_owned()).unwrap(),
            request_id: "test-request".to_owned(),
            buffer_size: 10,
            max_stored_results_per_chunk: 2,
            max_chunks: None,
            timeout_quantile: 0.5,
            retries: 1,
            compression: Compression::Gzip,
            skip_parent_hash_validation: false,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_chunk_sharing_no_block_with_the_query_is_no_data() {
        for chunk in [
            DataChunk::new(0, 200, 299, "aaaaa").unwrap(), // portal: the gap resolved forward
            DataChunk::new(0, 0, 99, "aaaaa").unwrap(),    // legacy: the chunk before the gap
        ] {
            let network = Arc::new(MockNetwork {
                chunk,
                backoff_until: Instant::now(),
                queries_sent: AtomicUsize::new(0),
            });

            let result = StreamController::new(stream_request(), network, 0, 1);

            let Err(err) = result else {
                panic!("a range holding no data is not a stream ({chunk})");
            };
            assert!(matches!(err, RequestError::NoData), "{chunk}: got {err:?}");
        }
    }

    /// Regression test for the duplicated-response incident: a request whose
    /// first chunk hits a short worker backoff (all workers rate-limited)
    /// must still serve the chunk's data exactly once.
    ///
    /// Before the fix, the failed scheduling attempt left the chunk owned
    /// both by the error slot in the buffer (retried in place once the
    /// backoff expired) and by `next_chunk` (scheduled a second time by
    /// `try_fill_slots`), so the chunk's full data was queried and emitted
    /// twice within one stream.
    #[tokio::test(start_paused = true)]
    async fn chunk_hitting_short_backoff_is_served_exactly_once() {
        let network = Arc::new(MockNetwork {
            chunk: test_chunk(),
            backoff_until: Instant::now() + Duration::from_millis(100),
            queries_sent: AtomicUsize::new(0),
        });
        let mut controller =
            StreamController::new(stream_request(), network.clone(), 0, 1).unwrap();

        let mut responses = Vec::new();
        while let Some(item) = controller.next().await {
            responses.push(item.expect("stream should not fail"));
        }

        assert_eq!(
            responses,
            vec![b"data-100-150".to_vec()],
            "the chunk's data must be served exactly once",
        );
        assert_eq!(
            network.queries_sent.load(Ordering::SeqCst),
            1,
            "the chunk must be queried exactly once",
        );
    }

    // ------------------------------------------------------------------
    // Property-based tests: random scheduling adversity (worker backoffs,
    // partial responses, retriable failures) must never break the stream's
    // core invariants.
    // ------------------------------------------------------------------

    use std::collections::HashMap;
    use std::sync::atomic::{AtomicI64, AtomicU64};
    use std::sync::Mutex as StdMutex;

    use proptest::prelude::*;

    use crate::network::TestLeasePool;

    /// Scripted outcome of one `find_worker` call.
    #[derive(Debug, Clone)]
    enum FindWorkerEvent {
        Lease,
        /// Expires while the stream is still allowed to wait (< MAX_IDLE_TIME).
        ShortBackoff(u64),
        /// Longer than MAX_IDLE_TIME: the stream gives up with `BusyFor`.
        LongBackoff,
        /// No workers at all: the stream gives up with `Unavailable`.
        Unavailable,
    }

    /// What a scripted error response reads off the wire.
    const ERROR_BYTES: usize = 3;

    /// Scripted outcome of one worker query.
    #[derive(Debug, Clone)]
    enum QueryEvent {
        Full,
        /// Serve only this percentage of the queried range, forcing a
        /// continuation request for the remainder.
        Partial(u8),
        /// Misbehave: report this many blocks beyond the queried range end.
        Overshoot(u64),
        /// Misbehave in the other direction: report this many blocks below the
        /// queried range start.
        Undershoot(u64),
        /// Serve the full range, but only after `ms` of (virtual) time. When
        /// this exceeds the request timeout the controller fires a speculative
        /// (hedged) query to another reserved worker while this one is still in
        /// flight — the only way to drive the `is_speculative` path, since every
        /// other event resolves instantly under the paused clock.
        Slow(u64),
        Retriable,
        /// Fail with a retriable error, but only after `ms` of (virtual) time: a query
        /// whose failure can land after another attempt has already settled the range.
        FailAfter(u64),
        RateLimited,
        /// Fail with a terminal error: the range ends with it, and so does the stream.
        Fatal,
        /// Never respond. Only used by the cancellation test; including it in
        /// the random generator would (correctly) fail the liveness property,
        /// because a slot whose every attempt hangs waits forever — in
        /// production the transport read timeout, modeled here as `Retriable`,
        /// breaks that wait.
        Hang,
    }

    /// Decrements the live-query counter when the query future is dropped,
    /// whether it completed or was aborted.
    struct LiveQueryGuard(Arc<AtomicI64>);

    impl LiveQueryGuard {
        fn new(counter: &Arc<AtomicI64>) -> Self {
            counter.fetch_add(1, Ordering::SeqCst);
            Self(counter.clone())
        }
    }

    impl Drop for LiveQueryGuard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// A network where every `find_worker` / `query_worker` call consumes the
    /// next scripted event; exhausted scripts default to success.
    struct ScriptedNetwork {
        chunks: Vec<DataChunk>,
        lease_pool: TestLeasePool,
        find_worker_script: StdMutex<VecDeque<FindWorkerEvent>>,
        query_script: StdMutex<VecDeque<QueryEvent>>,
        /// `(request_id, range)` of every in-range successful worker
        /// execution (rejected overshoots don't count: their data is never
        /// delivered, so re-executing their range is legal).
        ok_ranges: StdMutex<Vec<(String, (u64, u64))>>,
        /// Query futures currently alive (running or queued, not yet
        /// completed nor aborted).
        live_queries: Arc<AtomicI64>,
        /// Workers the controller reported as returning contract-violating data.
        integrity_failures: StdMutex<Vec<PeerId>>,
        /// Every byte any query read, wherever it ended up: OB-16's ledger truth.
        bytes_read: Arc<AtomicU64>,
    }

    impl ScriptedNetwork {
        fn new(
            chunks: Vec<DataChunk>,
            find_worker_script: Vec<FindWorkerEvent>,
            query_script: Vec<QueryEvent>,
        ) -> Arc<Self> {
            Arc::new(Self {
                chunks,
                lease_pool: TestLeasePool::new(),
                find_worker_script: StdMutex::new(find_worker_script.into()),
                query_script: StdMutex::new(query_script.into()),
                ok_ranges: StdMutex::new(Vec::new()),
                live_queries: Arc::new(AtomicI64::new(0)),
                integrity_failures: StdMutex::new(Vec::new()),
                bytes_read: Arc::default(),
            })
        }
    }

    impl StreamingNetwork for ScriptedNetwork {
        fn find_chunk(&self, _dataset: &DatasetId, block: u64) -> Result<DataChunk, ChunkNotFound> {
            if block < *self.chunks[0].block_range().start() {
                return Err(ChunkNotFound::BeforeFirst {
                    first_block: *self.chunks[0].block_range().start(),
                });
            }
            self.chunks
                .iter()
                .find(|c| c.block_range().contains(&block))
                .cloned()
                .ok_or(ChunkNotFound::AfterLast)
        }

        fn next_chunk(&self, _dataset: &DatasetId, chunk: &DataChunk) -> Option<DataChunk> {
            let pos = self.chunks.iter().position(|c| c == chunk)?;
            self.chunks.get(pos + 1).cloned()
        }

        fn find_worker(&self, _dataset: &DatasetId, _block: u64) -> Result<WorkerLease, NoWorker> {
            let event = self
                .find_worker_script
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(FindWorkerEvent::Lease);
            match event {
                FindWorkerEvent::Lease => Ok(self.lease_pool.lease(PeerId::random())),
                FindWorkerEvent::ShortBackoff(ms) => Err(NoWorker::Backoff(
                    Instant::now() + Duration::from_millis(ms),
                )),
                FindWorkerEvent::LongBackoff => Err(NoWorker::Backoff(
                    // Strictly past MAX_IDLE_TIME so the stream always gives up
                    // with `BusyFor`, however that threshold is tuned.
                    Instant::now() + MAX_IDLE_TIME + Duration::from_secs(1),
                )),
                FindWorkerEvent::Unavailable => Err(NoWorker::AllUnavailable),
            }
        }

        fn query_worker(
            self: Arc<Self>,
            _lease: WorkerLease,
            request_id: String,
            _chunk_id: ChunkId,
            block_range: BlockRange,
            _query: String,
            _compression: Compression,
            _priority: Option<u32>,
            meter: AttemptMeter,
        ) -> BoxFuture<'static, QueryResult> {
            let event = self
                .query_script
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(QueryEvent::Full);
            let guard = LiveQueryGuard::new(&self.live_queries);
            let ledger = self.bytes_read.clone();
            Box::pin(async move {
                let _guard = guard;
                // Sent once the task runs; a task aborted before it runs is withdrawn.
                meter.mark_sent();
                let read = |bytes: usize| {
                    meter.record(bytes);
                    ledger.fetch_add(bytes as u64, Ordering::SeqCst);
                };
                let result: QueryResult = async {
                    let (start, end) = (*block_range.start(), *block_range.end());
                    let last = match event {
                        QueryEvent::Hang => {
                            read(1);
                            futures::future::pending::<()>().await;
                            unreachable!("pending future never resolves")
                        }
                        QueryEvent::Retriable => {
                            read(ERROR_BYTES);
                            return Err(QueryError::Retriable("scripted failure".to_owned()));
                        }
                        QueryEvent::FailAfter(ms) => {
                            read(1);
                            tokio::time::sleep(Duration::from_millis(ms)).await;
                            read(ERROR_BYTES);
                            return Err(QueryError::Retriable("scripted late failure".to_owned()));
                        }
                        QueryEvent::RateLimited => {
                            read(ERROR_BYTES);
                            return Err(QueryError::RateLimitExceeded);
                        }
                        QueryEvent::Fatal => {
                            read(ERROR_BYTES);
                            return Err(QueryError::Failure(
                                "scripted terminal failure".to_owned(),
                            ));
                        }
                        QueryEvent::Full => end,
                        QueryEvent::Slow(ms) => {
                            read(1); // part of the body, so a cancelled one has read something
                            tokio::time::sleep(Duration::from_millis(ms)).await;
                            end
                        }
                        QueryEvent::Overshoot(extra) => end + extra,
                        QueryEvent::Undershoot(below) => start.saturating_sub(below.max(1)),
                        QueryEvent::Partial(_) if start == end => end,
                        QueryEvent::Partial(pct) => start + (end - start) * (pct as u64) / 100,
                    };
                    if (start..=end).contains(&last) {
                        self.ok_ranges
                            .lock()
                            .unwrap()
                            .push((request_id, (start, last)));
                    }
                    let data = format!("{start}:{last}").into_bytes();
                    read(data.len() - usize::from(matches!(event, QueryEvent::Slow(_))));
                    Ok(QuerySuccess {
                        ok: sqd_messages::QueryOk {
                            data: data.into(),
                            last_block: last,
                        },
                        ttfb: Duration::from_millis(1),
                        transfer_time: Duration::from_millis(1),
                        response_size: 10,
                    })
                }
                .await;
                // Reached only by a task that returns, as in production: an aborted one
                // stays incomplete.
                meter.complete(result.is_ok());
                result
            })
        }

        fn report_integrity_failure(&self, worker: PeerId) {
            self.integrity_failures.lock().unwrap().push(worker);
        }
    }

    /// The block range requested by one of the concurrent streams.
    #[derive(Debug, Clone)]
    struct StreamSpec {
        from: u64,
        to: u64,
    }

    #[derive(Debug, Clone)]
    struct Scenario {
        n_chunks: usize,
        streams: Vec<StreamSpec>,
        find_worker_script: Vec<FindWorkerEvent>,
        query_script: Vec<QueryEvent>,
        buffer_size: usize,
        retries: u8,
        max_stored_results_per_chunk: usize,
        max_chunks: Option<usize>,
    }

    impl Scenario {
        const CHUNK_SIZE: u64 = 100;
        const FIRST_BLOCK: u64 = 100;

        fn dataset_last_block(&self) -> u64 {
            Self::FIRST_BLOCK + (self.n_chunks as u64) * Self::CHUNK_SIZE - 1
        }

        /// The last block a cleanly completed stream is expected to serve:
        /// the requested end, capped by the dataset end and by `max_chunks`.
        fn expected_end(&self, spec: &StreamSpec) -> u64 {
            let mut end = spec.to.min(self.dataset_last_block());
            if let Some(limit) = self.max_chunks {
                let first_chunk = (spec.from - Self::FIRST_BLOCK) / Self::CHUNK_SIZE;
                let limit_end =
                    Self::FIRST_BLOCK + (first_chunk + limit as u64) * Self::CHUNK_SIZE - 1;
                end = end.min(limit_end);
            }
            end
        }

        fn build_chunks(&self) -> Vec<DataChunk> {
            (0..self.n_chunks)
                .map(|i| {
                    let first = Self::FIRST_BLOCK + (i as u64) * Self::CHUNK_SIZE;
                    let last = first + Self::CHUNK_SIZE - 1;
                    DataChunk::from_str(&format!("{first:010}/{first:010}-{last:010}-abcde"))
                        .unwrap()
                })
                .collect()
        }

        fn request(&self, spec: &StreamSpec, stream_index: usize) -> StreamRequest {
            let query_json = format!(
                r#"{{
                    "type": "evm",
                    "fromBlock": {},
                    "toBlock": {},
                    "fields": {{"block": {{"number": true}}}},
                    "includeAllBlocks": true
                }}"#,
                spec.from, spec.to
            );
            StreamRequest {
                dataset_id: DatasetId::from_url("test-dataset"),
                dataset_name: "test-dataset".to_owned(),
                query: ParsedQuery::try_from(query_json).unwrap(),
                request_id: format!("stream-{stream_index}"),
                buffer_size: self.buffer_size,
                max_stored_results_per_chunk: self.max_stored_results_per_chunk,
                max_chunks: self.max_chunks,
                timeout_quantile: 0.5,
                retries: self.retries,
                compression: Compression::Gzip,
                skip_parent_hash_validation: false,
            }
        }
    }

    fn scenario_strategy() -> impl Strategy<Value = Scenario> {
        let find_worker_event = prop_oneof![
            3 => Just(FindWorkerEvent::Lease),
            3 => (10u64..=300).prop_map(FindWorkerEvent::ShortBackoff),
            1 => Just(FindWorkerEvent::LongBackoff),
            1 => Just(FindWorkerEvent::Unavailable),
        ];
        let query_event = prop_oneof![
            4 => Just(QueryEvent::Full),
            2 => (10u8..=90).prop_map(QueryEvent::Partial),
            2 => (200u64..=2500).prop_map(QueryEvent::Slow),
            1 => (1u64..=20).prop_map(QueryEvent::Overshoot),
            1 => (1u64..=20).prop_map(QueryEvent::Undershoot),
            1 => Just(QueryEvent::Retriable),
            1 => (200u64..=2500).prop_map(QueryEvent::FailAfter),
        ];
        (1usize..=4)
            .prop_flat_map(move |n_chunks| {
                let total_blocks = (n_chunks as u64) * Scenario::CHUNK_SIZE;
                (
                    Just(n_chunks),
                    prop::collection::vec((0..total_blocks, 0u64..300), 1..=3),
                    prop::collection::vec(find_worker_event.clone(), 0..6),
                    prop::collection::vec(query_event.clone(), 0..10),
                    1usize..=10,
                    0u8..=2,
                    1usize..=4,
                    prop::option::of(1usize..=4),
                )
            })
            .prop_map(
                |(n_chunks, streams, fw, q, buffer_size, retries, max_stored, max_chunks)| {
                    Scenario {
                        n_chunks,
                        streams: streams
                            .into_iter()
                            .map(|(offset, span)| StreamSpec {
                                from: Scenario::FIRST_BLOCK + offset,
                                to: Scenario::FIRST_BLOCK + offset + span,
                            })
                            .collect(),
                        find_worker_script: fw,
                        query_script: q,
                        buffer_size,
                        retries,
                        max_stored_results_per_chunk: max_stored,
                        max_chunks,
                    }
                },
            )
    }

    struct StreamOutcome {
        /// (first_block, last_block) of every response chunk, in emission order.
        emissions: Vec<(u64, u64)>,
        emitted_bytes: u64,
        error: Option<String>,
        /// What the client is actually told. The message is prose and carries a random
        /// PeerId, so two runs differ whatever they were classified as — only the code
        /// distinguishes an integrity exhaustion from a transient one.
        code: Option<ErrorCode>,
    }

    struct ScenarioOutcome {
        streams: Vec<StreamOutcome>,
        timed_out: bool,
        ok_ranges: Vec<(String, (u64, u64))>,
        leases_outstanding: usize,
        signals: Arc<AttemptSignals>,
        bytes_read: u64,
    }

    async fn collect_stream<N: StreamingNetwork>(
        request: StreamRequest,
        network: Arc<N>,
        stream_index: u32,
    ) -> StreamOutcome {
        collect_stream_with(request, network, stream_index, Arc::default()).await
    }

    async fn collect_stream_with<N: StreamingNetwork>(
        request: StreamRequest,
        network: Arc<N>,
        stream_index: u32,
        signals: Arc<AttemptSignals>,
    ) -> StreamOutcome {
        let mut controller = StreamController::new(request, network, stream_index, 1)
            .unwrap()
            .with_signals(signals);
        let mut emitted_bytes = 0;
        let mut emissions = Vec::new();
        let mut error = None;
        let mut code = None;
        while let Some(item) = controller.next().await {
            match item {
                Ok(bytes) => {
                    emitted_bytes += bytes.len() as u64;
                    let text = String::from_utf8(bytes.to_vec()).unwrap();
                    let (start, last) = text.split_once(':').unwrap();
                    emissions.push((start.parse().unwrap(), last.parse().unwrap()));
                }
                Err(e) => {
                    error = Some(e.to_string());
                    code = e.code();
                    break;
                }
            }
        }
        StreamOutcome {
            emissions,
            emitted_bytes,
            error,
            code,
        }
    }

    fn run_scenario(scenario: &Scenario) -> ScenarioOutcome {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        runtime.block_on(async {
            let network = ScriptedNetwork::new(
                scenario.build_chunks(),
                scenario.find_worker_script.clone(),
                scenario.query_script.clone(),
            );
            let signals = Arc::<AttemptSignals>::default();

            let collect_all =
                futures::future::join_all(scenario.streams.iter().enumerate().map(|(i, spec)| {
                    let request = scenario.request(spec, i);
                    collect_stream_with(request, network.clone(), i as u32, signals.clone())
                }));

            // Virtual-time deadline: liveness. A stream stuck without timers
            // would otherwise hang the auto-advancing paused clock forever.
            match tokio::time::timeout(Duration::from_secs(3600), collect_all).await {
                Ok(streams) => {
                    // The controllers are dropped; give the runtime a few
                    // turns to process the aborted query tasks so their
                    // leases get returned.
                    for _ in 0..8 {
                        tokio::task::yield_now().await;
                    }
                    ScenarioOutcome {
                        streams,
                        timed_out: false,
                        ok_ranges: network.ok_ranges.lock().unwrap().clone(),
                        leases_outstanding: network.lease_pool.outstanding(),
                        signals,
                        bytes_read: network.bytes_read.load(Ordering::SeqCst),
                    }
                }
                Err(_) => ScenarioOutcome {
                    streams: Vec::new(),
                    timed_out: true,
                    ok_ranges: network.ok_ranges.lock().unwrap().clone(),
                    leases_outstanding: network.lease_pool.outstanding(),
                    signals,
                    bytes_read: network.bytes_read.load(Ordering::SeqCst),
                },
            }
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 128, ..ProptestConfig::default() })]

        /// Whatever adversity the network throws at the controller(s) —
        /// backoffs, unavailability, partial / out-of-range / slow / failing
        /// worker responses, chunk limits, several streams sharing the network
        /// — every stream must emit a gapless, strictly increasing prefix of
        /// its requested range, fully covering it on clean completion;
        /// redundant successful execution of a range (via hedging) stays
        /// bounded by the reserved fan-out; and no worker lease may leak.
        #[test]
        fn stream_invariants_hold_under_scheduling_adversity(scenario in scenario_strategy()) {
            let outcome = run_scenario(&scenario);

            // Liveness: every stream terminates (with data or an error).
            prop_assert!(!outcome.timed_out, "streams did not terminate");

            for (i, (spec, stream)) in scenario.streams.iter().zip(&outcome.streams).enumerate() {
                // No duplicates, monotonically increasing, no gaps: each
                // emission continues exactly where the previous one ended.
                let mut expected_next = spec.from;
                let bound = scenario.expected_end(spec);
                for &(start, last) in &stream.emissions {
                    prop_assert_eq!(
                        start,
                        expected_next,
                        "stream {} emission must continue the stream exactly (emissions: {:?})",
                        i,
                        &stream.emissions
                    );
                    prop_assert!(last >= start, "emission range must not be inverted");
                    // Bounds respect, asserted on the error path too: a stream
                    // that overshoots and *then* fails would satisfy the
                    // completeness check below vacuously.
                    prop_assert!(
                        last <= bound,
                        "stream {} emitted block {} past its bound {} (emissions: {:?})",
                        i,
                        last,
                        bound,
                        &stream.emissions
                    );
                    expected_next = last + 1;
                }

                // Completeness: a stream that ended without an error must
                // have served the whole requested range (capped by the
                // dataset end and the max_chunks limit).
                //
                // Note what this cannot see: truncation is a legal outcome
                // (ADR-001), so a controller that gives up while it still had
                // reserved attempts left satisfies every assertion here. That
                // property is carried by the deterministic skip-and-retry tests
                // and by the CT-2 worker-fault matrix in `harness/`.
                if stream.error.is_none() {
                    prop_assert_eq!(
                        expected_next,
                        bound + 1,
                        "stream {} clean completion must cover the full range (emissions: {:?})",
                        i,
                        &stream.emissions
                    );
                }
            }

            // Bounded redundant execution. Hedging sends the *same* range to
            // more than one healthy worker at once (a speculative query races
            // an in-flight one — it is not a retry of a *failed* attempt), so a
            // range legitimately can be executed successfully more than once.
            // What must hold is that this redundancy stays bounded by the
            // reserved fan-out (`retries + 1`) and never runs away: the stream
            // reserves at most that many workers per range and enters the
            // query-sending phase for a range at most once. Single *delivery*
            // to the client is guaranteed separately by the no-duplicate /
            // gapless emission invariant above — the losing hedged responses
            // are discarded, never emitted.
            let max_executions = scenario.retries as usize + 1;
            let mut ok_count = HashMap::new();
            for executed in &outcome.ok_ranges {
                *ok_count.entry(executed).or_insert(0usize) += 1;
            }
            for (executed, count) in ok_count {
                prop_assert!(
                    count <= max_executions,
                    "{:?} was successfully executed {} times, over the hedging \
                     fan-out of {} (all: {:?})",
                    executed,
                    count,
                    max_executions,
                    &outcome.ok_ranges
                );
            }

            // No lease leaks: every worker slot taken is eventually returned.
            prop_assert_eq!(
                outcome.leases_outstanding,
                0,
                "worker leases leaked after all streams finished"
            );

            // OB-16 against ledger truth: every sent query settles once, nothing lands
            // outside the reachable label set (a delivered or failed query was read, a
            // superseded or cancelled one was not, a query read and held had answered),
            // delivered is exactly what clients got, every byte read has one outcome,
            // and every chunk a query was dispatched for settles once.
            let signals = &outcome.signals;
            let settled =
                |kind| AttemptOutcome::ALL.map(|o| signals.settled_by_outcome(DATASET, kind, o));
            for kind in AttemptKind::ALL {
                let sent = signals.sent_count(DATASET, kind);
                prop_assert_eq!(settled(kind).iter().sum::<u64>(), sent);
                for outcome in AttemptOutcome::ALL {
                    for stage in AttemptStage::ALL {
                        for completion in AttemptCompletion::ALL {
                            let reachable = outcome.stages().contains(&stage)
                                && outcome.completions(stage).contains(&completion);
                            if reachable {
                                continue;
                            }
                            let count =
                                signals.settled_count(DATASET, kind, outcome, stage, completion);
                            prop_assert_eq!(
                                count,
                                0,
                                "{:?}/{:?}/{:?}/{:?} is unreachable",
                                kind,
                                outcome,
                                stage,
                                completion
                            );
                        }
                    }
                }
            }
            let delivered: u64 = AttemptKind::ALL.map(|k| settled(k)[0]).iter().sum();
            let emissions: usize = outcome.streams.iter().map(|s| s.emissions.len()).sum();
            prop_assert_eq!(delivered, emissions as u64);
            let emitted: u64 = outcome.streams.iter().map(|s| s.emitted_bytes).sum();
            let delivered_bytes = signals.bytes_by_outcome(DATASET, AttemptOutcome::Delivered);
            prop_assert_eq!(delivered_bytes, emitted);
            let read: u64 = AttemptOutcome::ALL
                .map(|o| signals.bytes_by_outcome(DATASET, o))
                .iter()
                .sum();
            prop_assert_eq!(read, outcome.bytes_read);
            let chunks: u64 = ChunkOutcome::ALL.map(|o| signals.chunk_count(DATASET, o)).iter().sum();
            let first = signals.sent_count(DATASET, AttemptKind::First)
                + signals.withdrawn_count(DATASET, AttemptKind::First);
            prop_assert_eq!(chunks, first);
        }
    }

    /// A wrong-range worker response — in either direction — must be rejected
    /// as an integrity failure: discarded and rerouted, not trusted, and not
    /// fatal to the stream on its own.
    #[test]
    fn wrong_range_response_is_rejected_as_integrity_failure() {
        let success = |last_block| {
            Ok(QuerySuccess {
                ok: sqd_messages::QueryOk {
                    data: b"data".to_vec().into(),
                    last_block,
                },
                ttfb: Duration::from_millis(1),
                transfer_time: Duration::from_millis(1),
                response_size: 4,
            })
        };
        let range = BlockRange::new(100, 150);

        for (last_block, case) in [(151, "overshoot"), (99, "undershoot")] {
            let reason = wrong_range(&success(last_block), &range);
            assert!(reason.is_some(), "{case} must be rejected");
            let rejected: QueryResult = Err(QueryError::Integrity(reason.unwrap()));
            assert!(retriable(&rejected), "{case} must be rerouted");
        }

        for last_block in [100, 125, 150] {
            assert!(
                wrong_range(&success(last_block), &range).is_none(),
                "in-range response {last_block} must pass through"
            );
        }

        // A failure the network already classified is not re-reported here.
        let from_network: QueryResult = Err(QueryError::Integrity("bad signature".into()));
        assert!(wrong_range(&from_network, &range).is_none());
    }

    /// Exhaustion class: transient failures report the data as temporarily
    /// unavailable, but attempts that all equivocate page instead (DC-1).
    #[tokio::test(start_paused = true)]
    async fn all_attempts_equivocating_pages_instead_of_reporting_an_outage() {
        let scenario = |query_script: Vec<QueryEvent>| Scenario {
            n_chunks: 1,
            streams: vec![StreamSpec { from: 100, to: 150 }],
            find_worker_script: Vec::new(),
            query_script,
            buffer_size: 10,
            retries: 1,
            max_stored_results_per_chunk: 1,
            max_chunks: None,
        };

        // Both reserved workers overshoot: integrity exhaustion.
        let all_bad = scenario(vec![QueryEvent::Overshoot(5), QueryEvent::Undershoot(5)]);
        let network = ScriptedNetwork::new(
            all_bad.build_chunks(),
            Vec::new(),
            all_bad.query_script.clone(),
        );
        let outcome =
            collect_stream(all_bad.request(&all_bad.streams[0], 0), network.clone(), 0).await;
        assert!(
            outcome.emissions.is_empty(),
            "equivocated data must never be delivered"
        );
        assert_eq!(
            network.integrity_failures.lock().unwrap().len(),
            2,
            "every discarded response must be counted against its worker"
        );
        assert_eq!(
            outcome.code,
            Some(ErrorCode::WorkerFailure),
            "an all-equivocating run is our bug to page on, not an outage to retry: {:?}",
            outcome.error
        );

        // One transient failure in the mix keeps the outcome transient.
        let mixed = scenario(vec![QueryEvent::Retriable, QueryEvent::Overshoot(5)]);
        let network =
            ScriptedNetwork::new(mixed.build_chunks(), Vec::new(), mixed.query_script.clone());
        let mixed_outcome =
            collect_stream(mixed.request(&mixed.streams[0], 0), network.clone(), 0).await;
        assert!(mixed_outcome.emissions.is_empty());
        assert_eq!(
            mixed_outcome.code,
            Some(ErrorCode::RetriesExhausted),
            "one transient attempt means a later retry can still succeed: {:?}",
            mixed_outcome.error
        );
    }

    /// A refusal is counted once even when all read-ahead slots fail.
    #[tokio::test]
    async fn buffered_rate_limits_count_one_refused_stream() {
        let scenario = Scenario {
            n_chunks: 3,
            streams: vec![StreamSpec { from: 100, to: 399 }],
            find_worker_script: Vec::new(),
            query_script: vec![QueryEvent::RateLimited; 3],
            buffer_size: 3,
            retries: 0,
            max_stored_results_per_chunk: 1,
            max_chunks: None,
        };
        let network = ScriptedNetwork::new(
            scenario.build_chunks(),
            Vec::new(),
            scenario.query_script.clone(),
        );
        let before = metrics::refused_streams(RefusalReason::WorkersRateLimited);

        let outcome = collect_stream(scenario.request(&scenario.streams[0], 0), network, 0).await;

        assert_eq!(outcome.code, Some(ErrorCode::Overloaded));
        assert_eq!(
            metrics::refused_streams(RefusalReason::WorkersRateLimited),
            before + 1,
            "three buffered chunk failures still interrupt only one stream"
        );
    }

    /// The class is chosen here; every other taxonomy test starts from a `RequestError`
    /// that already exists. Capacity refusals used to land in `retries_exhausted`, so a
    /// rate-limited fleet answered a bare 503 — the 2026-07 storm's shape (ADR-012).
    #[tokio::test]
    async fn the_exhaustion_class_follows_what_every_attempt_agreed_on() {
        let exhaust = |errors: Vec<QueryError>| {
            let mut pending = PendingRequests::new(
                errors
                    .iter()
                    .map(|_| WorkerLease::for_tests(PeerId::random())),
                Duration::from_secs(1),
            );
            pending.requests = errors
                .into_iter()
                .map(|e| {
                    WorkerRequest::Finished(FinishedWorkerRequest {
                        result: Err(e),
                        worker: PeerId::random(),
                    })
                })
                .collect();
            match StreamController::<ScriptedNetwork>::all_attempts_failed(&mut pending) {
                RequestState::Done(Err(e)) => e,
                _ => panic!("exhaustion must produce an error"),
            }
        };
        let integrity = || QueryError::Integrity("equivocated".into());
        let transient = || QueryError::Retriable("timed out".into());

        let cases = [
            (
                vec![QueryError::RateLimitExceeded, QueryError::RateLimitExceeded],
                ErrorCode::Overloaded,
                "capacity refusals",
            ),
            (
                vec![integrity(), integrity()],
                ErrorCode::WorkerFailure,
                "equivocation",
            ),
            (
                vec![transient(), transient()],
                ErrorCode::RetriesExhausted,
                "transient failures",
            ),
            // No class is claimed unless every attempt agrees on it.
            (
                vec![QueryError::RateLimitExceeded, transient()],
                ErrorCode::RetriesExhausted,
                "a mix with a transient attempt",
            ),
            (
                vec![QueryError::RateLimitExceeded, integrity()],
                ErrorCode::RetriesExhausted,
                "a mix of refusal and equivocation",
            ),
        ];

        for (errors, want, case) in cases {
            assert_eq!(exhaust(errors).code(), Some(want), "{case}");
        }

        // The hint INV-26 owes an overload has to survive to the wire.
        use axum::response::IntoResponse;
        let response = exhaust(vec![QueryError::RateLimitExceeded]).into_response();
        assert_eq!(response.status().as_u16(), 529);
        assert_eq!(response.headers()[axum::http::header::RETRY_AFTER], "1");
    }

    /// The undershoot half of the wrong-range contract: a worker reporting a
    /// last block below the queried start used to fail the whole stream.
    #[tokio::test(start_paused = true)]
    async fn undershooting_worker_response_is_skipped_and_retried() {
        let scenario = Scenario {
            n_chunks: 1,
            streams: vec![StreamSpec { from: 100, to: 150 }],
            find_worker_script: Vec::new(),
            query_script: vec![QueryEvent::Undershoot(10)],
            buffer_size: 10,
            retries: 1,
            max_stored_results_per_chunk: 1,
            max_chunks: None,
        };
        let network = ScriptedNetwork::new(
            scenario.build_chunks(),
            Vec::new(),
            scenario.query_script.clone(),
        );

        let outcome = collect_stream(
            scenario.request(&scenario.streams[0], 0),
            network.clone(),
            0,
        )
        .await;

        assert_eq!(
            outcome.error, None,
            "one bad worker must not fail the stream"
        );
        assert_eq!(
            outcome.emissions,
            vec![(100, 150)],
            "the range must be served exactly once by the retry"
        );
        assert_eq!(
            network.integrity_failures.lock().unwrap().len(),
            1,
            "the equivocating worker must be penalized"
        );
    }

    /// A single misbehaving worker must not fail the stream: its response is
    /// skipped and the range is served by the next reserved worker.
    #[tokio::test(start_paused = true)]
    async fn overshooting_worker_response_is_skipped_and_retried() {
        let scenario = Scenario {
            n_chunks: 1,
            streams: vec![StreamSpec { from: 100, to: 150 }],
            find_worker_script: Vec::new(),
            query_script: vec![QueryEvent::Overshoot(10)],
            buffer_size: 10,
            retries: 1,
            max_stored_results_per_chunk: 1,
            max_chunks: None,
        };
        let network = ScriptedNetwork::new(
            scenario.build_chunks(),
            Vec::new(),
            scenario.query_script.clone(),
        );

        let outcome = collect_stream(
            scenario.request(&scenario.streams[0], 0),
            network.clone(),
            0,
        )
        .await;

        assert_eq!(
            outcome.error, None,
            "one bad worker must not fail the stream"
        );
        assert_eq!(
            outcome.emissions,
            vec![(100, 150)],
            "the range must be served exactly once by the retry"
        );
    }

    /// Hedging (speculative queries) sends the *same* range to more than one
    /// healthy worker at once, so a range can be executed successfully more
    /// than once — that is by design, not a bug. What the stream must still
    /// guarantee is that the client is *delivered* the range exactly once; the
    /// losing hedged response is discarded, never emitted.
    ///
    /// Two workers are scripted to finish the same range at the same virtual
    /// instant: the first (non-speculative) query lasts 1500ms, longer than
    /// the 1000ms request timeout, so at t=1000 a speculative query is sent to
    /// the second reserved worker; sleeping 500ms, it also completes at t=1500.
    /// Both record an in-range success, so the range is executed twice.
    #[test]
    fn hedged_query_executes_twice_but_is_delivered_once() {
        let scenario = Scenario {
            n_chunks: 1,
            streams: vec![StreamSpec { from: 100, to: 199 }],
            find_worker_script: Vec::new(),
            query_script: vec![QueryEvent::Slow(1500), QueryEvent::Slow(500)],
            buffer_size: 1,
            retries: 1,
            max_stored_results_per_chunk: 1,
            max_chunks: None,
        };

        let outcome = run_scenario(&scenario);

        assert!(!outcome.timed_out, "stream must terminate");

        // The range was genuinely executed by *both* healthy workers.
        let executions = outcome
            .ok_ranges
            .iter()
            .filter(|entry| entry.0 == "stream-0" && entry.1 == (100, 199))
            .count();
        assert_eq!(
            executions, 2,
            "hedging should have executed the range on both workers (all: {:?})",
            outcome.ok_ranges
        );

        // Yet the client sees it exactly once, gapless and complete.
        assert_eq!(
            outcome.streams[0].error, None,
            "hedging must not fail the stream"
        );
        assert_eq!(
            outcome.streams[0].emissions,
            vec![(100, 199)],
            "the hedged range must be delivered exactly once"
        );

        // The aborted loser still returns its lease.
        assert_eq!(outcome.leases_outstanding, 0, "worker leases leaked");
    }

    /// Dropping the stream (client disconnect) must abort the in-flight
    /// worker queries and return their leases.
    #[tokio::test(start_paused = true)]
    async fn dropping_the_stream_aborts_in_flight_queries() {
        let scenario = Scenario {
            n_chunks: 2,
            streams: vec![StreamSpec { from: 100, to: 299 }],
            find_worker_script: Vec::new(),
            query_script: vec![QueryEvent::Hang; 8],
            buffer_size: 4,
            retries: 1,
            max_stored_results_per_chunk: 1,
            max_chunks: None,
        };
        let network = ScriptedNetwork::new(
            scenario.build_chunks(),
            Vec::new(),
            vec![QueryEvent::Hang; 8],
        );
        let mut controller = StreamController::new(
            scenario.request(&scenario.streams[0], 0),
            network.clone(),
            0,
            1,
        )
        .unwrap();

        // One poll is enough to schedule the chunk queries; they all hang.
        futures::future::poll_fn(|ctx| {
            let _ = Pin::new(&mut controller).poll_next(ctx);
            Poll::Ready(())
        })
        .await;
        assert!(
            network.live_queries.load(Ordering::SeqCst) > 0,
            "queries should be in flight"
        );

        drop(controller);
        // Aborts are processed by the runtime; give it a few turns.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }

        assert_eq!(
            network.live_queries.load(Ordering::SeqCst),
            0,
            "dropping the stream must abort all in-flight worker queries"
        );
        assert_eq!(
            network.lease_pool.outstanding(),
            0,
            "all worker leases must be returned"
        );
    }

    /// Each kind of query, each outcome and each completion, from a script that
    /// produces it.
    #[test]
    fn queries_are_counted_by_kind_outcome_and_completion() {
        use AttemptCompletion::*;
        use AttemptKind::*;
        use AttemptOutcome::*;
        let cases = [
            // Too slow: a hedge answers at t=1100 and wins; the first is cut off
            // mid-body. Were new queries left unpolled, the hedge would be read only
            // once the first finished at t=1500, and lose to it.
            (
                vec![QueryEvent::Slow(1500), QueryEvent::Slow(100)],
                1,
                vec![(First, Superseded, Incomplete), (Hedge, Delivered, Ok)],
            ),
            // A tie: both answers are in when the stream looks and it reads the first.
            // The hedge's answer had arrived and goes unread: a download wasted.
            (
                vec![QueryEvent::Slow(1500), QueryEvent::Slow(500)],
                1,
                vec![(First, Delivered, Ok), (Hedge, Superseded, Ok)],
            ),
            // A hedge that had failed by the time the first answered is let go the same
            // way, but no answer was thrown away: its cost is a failure's.
            (
                vec![QueryEvent::Slow(1500), QueryEvent::FailAfter(500)],
                1,
                vec![(First, Delivered, Ok), (Hedge, Superseded, Error)],
            ),
            // A hedge that fails while the first is still running is read as soon as it
            // fails; the first goes on to deliver.
            (
                vec![QueryEvent::Slow(1500), QueryEvent::FailAfter(100)],
                1,
                vec![(First, Delivered, Ok), (Hedge, Failed, Error)],
            ),
            // A terminal error, read as soon as it lands, ends the range with no winner:
            // the first, still running, is cancelled, not beaten.
            (
                vec![QueryEvent::Slow(1500), QueryEvent::Fatal],
                1,
                vec![(First, Cancelled, Incomplete), (Hedge, Failed, Error)],
            ),
            (
                vec![QueryEvent::Retriable, QueryEvent::Full],
                1,
                vec![(First, Failed, Error), (Retry, Delivered, Ok)],
            ),
            // An answer the controller rejects: the task returned one, the controller
            // failed it.
            (
                vec![QueryEvent::Overshoot(5), QueryEvent::Full],
                1,
                vec![(First, Failed, Ok), (Retry, Delivered, Ok)],
            ),
            (
                vec![QueryEvent::Partial(50), QueryEvent::Full],
                0,
                vec![(First, Delivered, Ok), (Continuation, Delivered, Ok)],
            ),
        ];
        for (script, retries, want) in cases {
            let scenario = Scenario {
                n_chunks: 1,
                streams: vec![StreamSpec { from: 100, to: 199 }],
                find_worker_script: Vec::new(),
                query_script: script.clone(),
                buffer_size: 1,
                retries,
                max_stored_results_per_chunk: 1,
                max_chunks: None,
            };
            let signals = run_scenario(&scenario).signals;
            for kind in AttemptKind::ALL {
                for outcome in AttemptOutcome::ALL {
                    for stage in AttemptStage::ALL {
                        for completion in AttemptCompletion::ALL {
                            // None of these scripts abandons anything, so every
                            // outcome here has exactly one stage it can be at.
                            let expected = u64::from(
                                want.contains(&(kind, outcome, completion))
                                    && outcome.stages() == [stage],
                            );
                            let got =
                                signals.settled_count(DATASET, kind, outcome, stage, completion);
                            assert_eq!(
                                got, expected,
                                "{script:?}: {kind:?}/{outcome:?}/{stage:?}/{completion:?}"
                            );
                        }
                    }
                }
            }
        }
    }

    fn read_ahead_scenario(script: Vec<QueryEvent>) -> Scenario {
        Scenario {
            n_chunks: 3,
            streams: vec![StreamSpec { from: 100, to: 399 }],
            find_worker_script: Vec::new(),
            query_script: script,
            buffer_size: 3,
            retries: 0,
            max_stored_results_per_chunk: 1,
            max_chunks: None,
        }
    }

    /// Takes the first of three read-ahead chunks and leaves, having let the others'
    /// queries run or not.
    async fn leave_after_first_chunk(
        queries_run: bool,
        script: Vec<QueryEvent>,
    ) -> (Arc<AttemptSignals>, u64) {
        let scenario = read_ahead_scenario(script);
        let network = ScriptedNetwork::new(
            scenario.build_chunks(),
            Vec::new(),
            scenario.query_script.clone(),
        );
        let signals = Arc::<AttemptSignals>::default();
        let request = scenario.request(&scenario.streams[0], 0);
        let mut controller = StreamController::new(request, network.clone(), 0, 1)
            .unwrap()
            .with_signals(signals.clone());
        if queries_run {
            controller.next().await.unwrap().unwrap();
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
        } else {
            futures::future::poll_fn(|ctx| {
                let _ = Pin::new(&mut controller).poll_next(ctx);
                Poll::Ready(())
            })
            .await;
        }
        drop(controller);
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        (signals, network.bytes_read.load(Ordering::SeqCst))
    }

    /// Read-ahead fetched for a client that left: requested, downloaded, read into the
    /// buffer, never sent. The reason is unknown, since all the controller saw was a
    /// healthy stream let go of.
    #[tokio::test(start_paused = true)]
    async fn read_ahead_the_client_never_takes_is_abandoned() {
        let (signals, _) = leave_after_first_chunk(true, Vec::new()).await;

        let abandoned = AttemptOutcome::Abandoned(StreamEnd::Unknown);
        let chunk = ChunkOutcome::Abandoned(StreamEnd::Unknown);
        assert_eq!(signals.chunk_count(DATASET, chunk), 2);
        let (read, ok) = (AttemptStage::Read, AttemptCompletion::Ok);
        let count = signals.settled_count(DATASET, AttemptKind::First, abandoned, read, ok);
        assert_eq!(count, 2);
        let unsent = "200:299".len() + "300:399".len();
        let bytes = signals.byte_count(DATASET, AttemptKind::First, abandoned, read, ok);
        assert_eq!(bytes, unsent as u64);
    }

    /// Queries still in flight when the client left are abandoned incomplete, charged
    /// only what they had read: outstanding work, not a buffered answer thrown away.
    #[tokio::test(start_paused = true)]
    async fn queries_outstanding_when_the_client_leaves_are_abandoned_incomplete() {
        let script = vec![QueryEvent::Full, QueryEvent::Hang, QueryEvent::Hang];
        let (signals, _) = leave_after_first_chunk(true, script).await;

        let abandoned = AttemptOutcome::Abandoned(StreamEnd::Unknown);
        let count = |stage, completion| {
            signals.settled_count(DATASET, AttemptKind::First, abandoned, stage, completion)
        };
        let (in_flight, incomplete) = (AttemptStage::InFlight, AttemptCompletion::Incomplete);
        assert_eq!(count(in_flight, incomplete), 2);
        assert_eq!(count(AttemptStage::Read, AttemptCompletion::Ok), 0);
        // Each hanging query had read one byte.
        let bytes = signals.byte_count(
            DATASET,
            AttemptKind::First,
            abandoned,
            in_flight,
            incomplete,
        );
        assert_eq!(bytes, 2);
    }

    /// An answer that lands after the controller last looked. The task had answered,
    /// so its completion is `ok`, but the controller never took the answer: by its
    /// stage the query was still in flight. Bandwidth spent, and not a buffered answer,
    /// which the completion alone would have called it.
    #[tokio::test(start_paused = true)]
    async fn an_answer_landing_after_the_controller_last_looked_is_abandoned_in_flight() {
        let script = vec![QueryEvent::Full, QueryEvent::Slow(100), QueryEvent::Hang];
        let scenario = read_ahead_scenario(script);
        let network = ScriptedNetwork::new(
            scenario.build_chunks(),
            Vec::new(),
            scenario.query_script.clone(),
        );
        let signals = Arc::<AttemptSignals>::default();
        let request = scenario.request(&scenario.streams[0], 0);
        let mut controller = StreamController::new(request, network.clone(), 0, 1)
            .unwrap()
            .with_signals(signals.clone());
        controller.next().await.unwrap().unwrap();
        // The second chunk's answer lands while nobody polls the stream.
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(controller);
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }

        let abandoned = AttemptOutcome::Abandoned(StreamEnd::Unknown);
        let count = |stage, completion| {
            signals.settled_count(DATASET, AttemptKind::First, abandoned, stage, completion)
        };
        let (in_flight, ok) = (AttemptStage::InFlight, AttemptCompletion::Ok);
        assert_eq!(count(in_flight, ok), 1);
        assert_eq!(count(AttemptStage::Read, ok), 0);
        assert_eq!(count(in_flight, AttemptCompletion::Incomplete), 1);
        let bytes = signals.byte_count(DATASET, AttemptKind::First, abandoned, in_flight, ok);
        assert_eq!(bytes, "200:299".len() as u64);
    }

    /// A client gone before any query reached the transport, as when queued behind a
    /// full congestion window: the chunks are abandoned, but no worker saw a query.
    #[tokio::test(start_paused = true)]
    async fn queries_cancelled_before_the_transport_are_withdrawn_not_sent() {
        let (signals, bytes_read) = leave_after_first_chunk(false, Vec::new()).await;

        assert_eq!(signals.withdrawn_count(DATASET, AttemptKind::First), 3);
        assert_eq!(signals.sent_count(DATASET, AttemptKind::First), 0);
        assert_eq!(bytes_read, 0);
        let chunk = ChunkOutcome::Abandoned(StreamEnd::Unknown);
        assert_eq!(signals.chunk_count(DATASET, chunk), 3);
    }

    /// A stream that ends on an error abandons its read-ahead for that reason, so this
    /// waste can be told from a client that left.
    #[tokio::test(start_paused = true)]
    async fn read_ahead_behind_a_stream_error_is_abandoned_for_the_error() {
        let scenario = read_ahead_scenario(vec![QueryEvent::Fatal]);
        let network = ScriptedNetwork::new(
            scenario.build_chunks(),
            Vec::new(),
            scenario.query_script.clone(),
        );
        let signals = Arc::<AttemptSignals>::default();
        let request = scenario.request(&scenario.streams[0], 0);
        let outcome = collect_stream_with(request, network.clone(), 0, signals.clone()).await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }

        assert!(outcome.error.is_some());
        assert!(outcome.emissions.is_empty());
        use AttemptKind::First;
        let (failed, read) = (AttemptOutcome::Failed, AttemptStage::Read);
        let count = signals.settled_count(DATASET, First, failed, read, AttemptCompletion::Error);
        assert_eq!(count, 1);
        assert_eq!(signals.chunk_count(DATASET, ChunkOutcome::Failed), 1);
        // The other two had answered and were buffered behind the failure.
        let abandoned = AttemptOutcome::Abandoned(StreamEnd::Error);
        let count = signals.settled_count(DATASET, First, abandoned, read, AttemptCompletion::Ok);
        assert_eq!(count, 2);
        let chunk = ChunkOutcome::Abandoned(StreamEnd::Error);
        assert_eq!(signals.chunk_count(DATASET, chunk), 2);
        let unknown = ChunkOutcome::Abandoned(StreamEnd::Unknown);
        assert_eq!(signals.chunk_count(DATASET, unknown), 0);
    }
}
