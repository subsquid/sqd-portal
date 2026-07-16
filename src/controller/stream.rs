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
    controller::timeouts::TimeoutManager,
    network::{ChunkNotFound, NetworkClient, NoWorker, QueryResult, StreamingNetwork, WorkerLease},
    types::{
        BlockRange, ChunkId, DataChunk, QueryError, RequestError, ResponseChunk, SendQueryError,
        StreamRequest,
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
    stream_index: u32,
    priority_stride: u32,
}

#[derive(Clone)]
pub struct DataRange {
    pub range: BlockRange,
    pub chunk: DataChunk,
    pub chunk_index: usize,
}

struct Slot {
    data_range: DataRange,
    state: RequestState,
}

struct ChunkSlot {
    active: Option<Slot>,
    buffered: VecDeque<BufferedResponse>,
}

struct BufferedResponse {
    chunk_index: usize,
    read_range: BlockRange,
    result: Result<ResponseChunk, RequestError>,
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
    Done(Result<ResponseChunk, RequestError>),
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
    data: ResponseChunk,
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

        Ok(Self {
            network,
            buffer: SlidingArray::with_capacity(request.buffer_size),
            next_chunk: Some(first_chunk),
            timeouts: TimeoutManager::new(request.timeout_quantile),
            request,
            stats: StreamStats::new(),
            span: tracing::Span::current(),
            last_error: None,
            stream_index,
            priority_stride,
        })
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

        for request in pending.requests.iter_mut() {
            self.poll_worker_request(data_range, request, &mut summary, ctx)?;
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
                        return Err(RequestState::Done(Err(RequestError::Internal(format!(
                            "worker query task failed: {join_err}"
                        )))));
                    }
                };

                if retriable(&response) {
                    tracing::debug!(
                        "Got retriable error in {}ms from {}: {}",
                        duration.as_millis(),
                        running.worker,
                        response.as_ref().unwrap_err().to_string(),
                    );
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
            self.start_next_attempt(data_range, pending, timed_out, summary.running, ctx);
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
        for request in pending.requests.drain(..) {
            let WorkerRequest::Finished(f) = request else {
                unreachable!("all worker requests should be finished")
            };
            // Format from the QueryError directly so every attempt is labeled with its
            // worker. Going through RequestError would drop the peer id for variants whose
            // Display is a fixed string (e.g. RateLimitExceeded, BaseBlockMismatch).
            let error = format!("worker {}: {}", f.worker, f.result.unwrap_err());
            errors.push(error);
        }
        RequestState::Done(Err(RequestError::RetriesExhausted(errors.join("; "))))
    }

    fn start_next_attempt(
        &mut self,
        data_range: &DataRange,
        pending: &mut PendingRequests,
        timed_out: bool,
        running: usize,
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
                let is_speculative = running > 0;
                let lease = worker
                    .lease
                    .take()
                    .expect("worker lease should only be used once");
                let request = self.send_query(data_range, lease, is_speculative);
                *req = WorkerRequest::Running(request);
                pending.set_timeout(self.timeouts.current_timeout(), ctx);
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
            if !chunk_slot.is_empty() {
                self.buffer.push_front(chunk_slot);
            }
            return self.ready_response(response);
        }

        let Some(slot) = chunk_slot.active.take() else {
            return Poll::Pending;
        };

        let chunk_index = slot.data_range.chunk_index;
        let (result, read_range, next_slot) = match slot.state {
            RequestState::Done(result) => (Poll::Ready(Some(result)), slot.data_range.range, None),
            RequestState::NoWorkers => {
                // We don't know how long we'll have to wait, so give up immediately
                return Poll::Ready(Some(Err(RequestError::Unavailable)));
            }
            RequestState::Paused(ref s) => {
                // All workers are rate-limited, try to pause and continue streaming
                let duration = s.until.duration_since(Instant::now());
                if duration > MAX_IDLE_TIME {
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
                (Poll::Ready(Some(result)), read_range, Some(slot))
            }
        };

        if let Some(slot) = next_slot {
            chunk_slot.active = Some(slot);
            self.buffer.push_front(chunk_slot);
        }

        self.log_response(chunk_index, &read_range, &result);
        result
    }

    fn ready_response(
        &mut self,
        response: BufferedResponse,
    ) -> Poll<Option<Result<ResponseChunk, RequestError>>> {
        let result = Poll::Ready(Some(response.result));
        self.log_response(response.chunk_index, &response.read_range, &result);
        result
    }

    fn log_response(
        &mut self,
        chunk_index: usize,
        read_range: &BlockRange,
        result: &Poll<Option<Result<ResponseChunk, RequestError>>>,
    ) {
        if let Poll::Ready(Some(Ok(bytes))) = result {
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
        is_speculative: bool,
    ) -> RunningWorkerRequest {
        tracing::debug!(
            "Sending {}query for chunk {} ({}-{}) to worker {}",
            if is_speculative { "another " } else { "" },
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
            )
            .in_current_span();

        self.stats.query_sent();
        RunningWorkerRequest {
            resp: tokio::spawn(fut),
            start_time,
            worker,
        }
    }
}

impl<N: StreamingNetwork> Drop for StreamController<N> {
    fn drop(&mut self) {
        let _enter = self.span.enter();
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
        }
    }
}

impl ChunkSlot {
    fn new(active: Slot) -> Self {
        Self {
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
}

fn into_partial_continuation(
    data_range: DataRange,
    partial: PartialResult,
) -> (BufferedResponse, DataRange) {
    let PartialResult { data, next_range } = partial;
    let read_range = BlockRange::new(*data_range.range.start(), *next_range.start() - 1);
    let next_data_range = data_range.with_range(next_range);
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
}

fn parse_response(
    response: QueryResult,
    range: &BlockRange,
    worker: PeerId,
    duration: Duration,
) -> RequestState {
    let s = match response {
        Ok(success) => success,
        Err(e) => {
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
        RequestState::Done(Ok(result.data))
    } else if last_block < *range.start() {
        tracing::warn!(
            "Got empty response for range {}-{} from worker {}",
            range.start(),
            range.end(),
            worker,
        );
        RequestState::Done(Err(RequestError::Failure(format!(
            "worker {} returned block {} which is below the first queried block {}",
            worker,
            last_block,
            range.start(),
        ))))
    } else {
        RequestState::Partial(PartialResult {
            data: result.data,
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

fn retriable(result: &QueryResult) -> bool {
    match result {
        Ok(_) => false,
        Err(QueryError::BadRequest(_)) => false,
        Err(QueryError::Retriable(_)) => true,
        Err(QueryError::Failure(_)) => false,
        Err(QueryError::RateLimitExceeded) => true,
        Err(QueryError::BaseBlockMismatch(_)) => false,
    }
}

fn short_code(result: &RequestState) -> &'static str {
    match result {
        RequestState::Done(Ok(_)) => "ok",
        RequestState::Done(Err(e)) => e.code().as_str(),
        RequestState::Partial(_) => "partial",
        RequestState::Pending(_) | RequestState::Paused(_) | RequestState::NoWorkers => "-",
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn test_chunk() -> DataChunk {
        DataChunk::from_str("0000000000/0000000100-0000000200-abcde").unwrap()
    }

    fn data_range(start: u64, end: u64) -> DataRange {
        DataRange {
            range: BlockRange::new(start, end),
            chunk: test_chunk(),
            chunk_index: 0,
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
            data: data.to_vec(),
            next_range: BlockRange::new(last_returned + 1, end),
        }
    }

    fn buffered_response() -> BufferedResponse {
        BufferedResponse {
            chunk_index: 0,
            read_range: 100..=149,
            result: Ok(vec![1, 2, 3]),
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
            Ok(data) => assert_eq!(data, b"first"),
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
                data: vec![1],
                next_range: 150..=199,
            }))),
            buffered: VecDeque::new(),
        };

        assert_eq!(chunk_slot.stored_result_count(), 1);
    }

    #[test]
    fn partial_continuation_requires_capacity_for_next_active_result() {
        let mut chunk_slot = ChunkSlot {
            active: Some(slot(RequestState::Partial(PartialResult {
                data: vec![1],
                next_range: 150..=199,
            }))),
            buffered: VecDeque::new(),
        };

        assert!(!chunk_slot.has_capacity_for_eager_continuation(1));
        assert!(chunk_slot.has_capacity_for_eager_continuation(2));

        chunk_slot.buffered.push_back(buffered_response());

        assert!(!chunk_slot.has_capacity_for_eager_continuation(2));
    }

    #[test]
    fn terminal_response_can_use_last_capacity_slot() {
        let mut chunk_slot = ChunkSlot {
            active: Some(slot(RequestState::Done(Ok(vec![4, 5, 6])))),
            buffered: VecDeque::new(),
        };

        assert!(chunk_slot.can_buffer_terminal_response(1));

        chunk_slot.buffered.push_back(buffered_response());

        assert!(!chunk_slot.can_buffer_terminal_response(1));
    }

    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::{future::BoxFuture, StreamExt};

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
        ) -> BoxFuture<'static, QueryResult> {
            self.queries_sent.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                Ok(QuerySuccess {
                    ok: sqd_messages::QueryOk {
                        data: format!("data-{}-{}", block_range.start(), block_range.end())
                            .into_bytes(),
                        last_block: *block_range.end(),
                    },
                    ttfb: Duration::from_millis(1),
                    transfer_time: Duration::from_millis(1),
                    response_size: 10,
                })
            })
        }
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
}
