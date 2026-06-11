//! Streaming query controller.
//!
//! [`StreamController`] produces an ordered stream of response chunks for a
//! client query that may span many dataset chunks. Internally it keeps a
//! sliding buffer of in-flight per-chunk requests, drains the front in chunk
//! order, and refills the back speculatively.
//!
//! ```text
//!                        StreamController (impl futures::Stream)
//!                                        │
//!                                        ▼
//!                               poll_next(ctx)  ◄────── HTTP handler awaits items
//!                                        │
//!             ┌──────────────────────────┼──────────────────────────┐
//!             ▼                          ▼                          ▼
//!      try_fill_slots          poll_slot (every slot)         pop_response
//!      (FILL the back)         (ADVANCE state)                (DRAIN the front)
//!             │                          │                          │
//!             │                          │                          │
//!             │   ┌──────────────────────┘                          │
//!             │   │                                                 │
//!             ▼   ▼                                                 ▼
//!
//!   ┌─────────────────────────────────────────────────────────┐    output to
//!   │                  buffer: SlidingArray<Slot>             │    client (in
//!   │                                                         │    chunk order)
//!   │   front                                          back   │
//!   │  ┌──────┐   ┌──────┐   ┌──────┐   ┌──────┐   ┌──────┐   │
//!   │  │ Slot │ → │ Slot │ → │ Slot │ → │ Slot │ → │ Slot │   │
//!   │  │chunk0│   │chunk1│   │chunk2│   │chunk3│   │chunk4│   │
//!   │  └──────┘   └──────┘   └──────┘   └──────┘   └──────┘   │
//!   │     ▲                                            ▲      │
//!   │     │ pop_front                       push_back  │      │
//!   │     │ (deliver this one next)        (start next │      │
//!   │     │                                 chunk's    │      │
//!   │     │                                 query)     │      │
//!   └─────┴────────────────────────────────────────────┴──────┘
//!                   ▲
//!                   │
//!            Slot.state evolves through:
//!
//!            ┌───────────┐
//!            │ NoWorkers │──┐
//!            └───────────┘  │     ┌─────────┐     ┌──────┐
//!                           ├────▶│ Pending │────▶│ Done │ ─ Ok(bytes) / Err
//!            ┌───────────┐  │     └─────────┘     └──────┘
//!            │  Paused   │──┘          │
//!            └───────────┘             ▼
//!                ▲                ┌─────────┐
//!                │                │ Partial │ ─ has bytes for
//!                └────────────────│         │   prefix; rest
//!                  continuation   └─────────┘   re-queried
//!                  request fires
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

struct BufferedResponse {
    chunk_index: usize,
    read_range: BlockRange,
    result: Result<ResponseChunk, RequestError>,
}

struct ChunkSlot {
    pending_responses: VecDeque<BufferedResponse>,
    active: Option<Slot>,
}

enum BufferedActiveSlot {
    Nothing,
    Completed,
    Partial(DataRange),
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
    /// range. Depending on request settings, a continuation request is either scheduled eagerly
    /// while polling or later when this result is fed to the client.
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

type StartQueryingChunkError = (Box<Slot>, SendQueryError);

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
                return Err(RequestError::InternalError(format!(
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

        while let Some(slot) = chunk_slot.active.as_mut() {
            updated |= self.poll_slot(slot, ctx).updated();

            match chunk_slot.buffer_active_response(self.request.eager_continuations) {
                BufferedActiveSlot::Nothing => break,
                BufferedActiveSlot::Completed => {
                    updated = true;
                    break;
                }
                BufferedActiveSlot::Partial(next_data_range) => {
                    self.schedule_continuation(chunk_slot, next_data_range, ctx);
                    updated = true;
                    continue;
                }
            }
        }

        if updated {
            UpdateStatus::Updated
        } else {
            UpdateStatus::NotUpdated
        }
    }

    fn schedule_continuation(
        &mut self,
        chunk_slot: &mut ChunkSlot,
        next_data_range: DataRange,
        ctx: &mut Context<'_>,
    ) {
        let next_slot = match self.start_querying_chunk(next_data_range, ctx) {
            Ok(slot) => slot,
            Err((slot, e)) => {
                tracing::debug!("Couldn't schedule continuation request: {e:?}");
                *slot
            }
        };
        chunk_slot.active = Some(next_slot);
        self.stats.observe_chunk_parts(chunk_slot.num_parts());
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
            Err((s, e)) => {
                *slot = *s;
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
                        return Err(RequestState::Done(Err(RequestError::InternalError(
                            format!("worker query task failed: {join_err}"),
                        ))));
                    }
                };
                let response = reject_out_of_range(response, &data_range.range, running.worker);

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
            let error =
                RequestError::from_query_error(f.result.unwrap_err().clone(), f.worker).to_string();
            errors.push(error);
        }
        let message = format!("All query attempts failed: {}", errors.join("; "));
        RequestState::Done(Err(RequestError::InternalError(message)))
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
        let chunk_slot = loop {
            let Some(chunk_slot) = self.buffer.pop_front() else {
                return Poll::Ready(None);
            };
            if !chunk_slot.is_empty() {
                break chunk_slot;
            }
        };

        let mut chunk_slot = chunk_slot;
        if let Some(response) = self.pop_buffered_response(&mut chunk_slot) {
            if !chunk_slot.is_empty() {
                self.buffer.push_front(chunk_slot);
            }
            return response;
        }

        match chunk_slot.buffer_active_response(true) {
            BufferedActiveSlot::Nothing => {}
            BufferedActiveSlot::Completed => {
                let response = self
                    .pop_buffered_response(&mut chunk_slot)
                    .expect("completed active slot should have produced a buffered response");
                if !chunk_slot.is_empty() {
                    self.buffer.push_front(chunk_slot);
                }
                return response;
            }
            BufferedActiveSlot::Partial(next_data_range) => {
                self.schedule_continuation(&mut chunk_slot, next_data_range, ctx);
                let response = self
                    .pop_buffered_response(&mut chunk_slot)
                    .expect("partial active slot should have produced a buffered response");
                if !chunk_slot.is_empty() {
                    self.buffer.push_front(chunk_slot);
                }
                return response;
            }
        }

        let Some(slot) = chunk_slot.active.take() else {
            return Poll::Ready(None);
        };

        match slot.state {
            RequestState::NoWorkers => {
                // We don't know how long we'll have to wait, so give up immediately
                Poll::Ready(Some(Err(RequestError::Unavailable)))
            }
            RequestState::Paused(ref s) => {
                // All workers are rate-limited, try to pause and continue streaming
                let duration = s.until.duration_since(Instant::now());
                if duration > MAX_IDLE_TIME {
                    Poll::Ready(Some(Err(RequestError::BusyFor(duration))))
                } else {
                    // TODO: fix calculation in case we're polling the same paused slot multiple times
                    self.stats.throttled(duration);
                    chunk_slot.active = Some(slot);
                    self.buffer.push_front(chunk_slot);
                    Poll::Pending
                }
            }
            RequestState::Pending(_) => {
                // The query is still running, keep waiting
                chunk_slot.active = Some(slot);
                self.buffer.push_front(chunk_slot);
                Poll::Pending
            }
            RequestState::Done(_) | RequestState::Partial(_) => {
                unreachable!("completed active slots should be buffered before emission")
            }
        }
    }

    fn pop_buffered_response(
        &mut self,
        chunk_slot: &mut ChunkSlot,
    ) -> Option<Poll<Option<Result<ResponseChunk, RequestError>>>> {
        let response = chunk_slot.pending_responses.pop_front()?;
        Some(self.emit_buffered_response(response))
    }

    fn emit_buffered_response(
        &mut self,
        response: BufferedResponse,
    ) -> Poll<Option<Result<ResponseChunk, RequestError>>> {
        let result = Poll::Ready(Some(response.result));

        if let Poll::Ready(Some(Ok(bytes))) = &result {
            self.stats.sent_response_chunk(
                *response.read_range.end() - *response.read_range.start() + 1,
                bytes.len(),
            );
            tracing::trace!(
                chunk_index = response.chunk_index,
                "Writing response blocks {}-{} ({} bytes)",
                *response.read_range.start(),
                *response.read_range.end(),
                bytes.len()
            );
        }

        result
    }

    fn try_fill_slots(&mut self, ctx: &mut Context<'_>) {
        if let Some(Slot {
            state: RequestState::Paused(_),
            ..
        }) = self.buffer.back().and_then(ChunkSlot::active)
        {
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
                    let chunk_slot = ChunkSlot::new(slot);
                    self.stats.observe_chunk_parts(chunk_slot.num_parts());
                    self.buffer.push_back(chunk_slot);
                    self.next_chunk = self.get_next_chunk(&chunk);
                    if paused {
                        break;
                    }
                }
                Err((slot, e)) => {
                    if !matches!(e, SendQueryError::NoWorkers) {
                        tracing::debug!("Couldn't schedule request: {e:?}");
                    }
                    if self.buffer.len() == 0 {
                        // Couldn't schedule a new request with no ongoing requests
                        // Return the error immediately
                        let chunk_slot = ChunkSlot::new(*slot);
                        self.stats.observe_chunk_parts(chunk_slot.num_parts());
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

    fn start_querying_chunk(
        &mut self,
        range: DataRange,
        ctx: &mut Context<'_>,
    ) -> Result<Slot, StartQueryingChunkError> {
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
                Err((Box::new(slot), err))
            }
            Err(err @ SendQueryError::Backoff(until)) => {
                let mut slot = Slot {
                    data_range,
                    state: RequestState::Paused(PausedState::new(until)),
                };
                self.poll_slot(&mut slot, ctx);
                Err((Box::new(slot), err))
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
        let query = build_query_for_range(&mut self.request.query, &range.range);
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

impl Slot {
    fn is_paused(&self) -> bool {
        matches!(&self.state, RequestState::Paused(_))
    }

    fn take_completed_response(&mut self) -> Option<BufferedResponse> {
        let RequestState::Done(_) = self.state else {
            return None;
        };

        // This slot is consumed by `ChunkSlot::buffer_active_response` immediately after this
        // returns; `NoWorkers` is only a temporary placeholder for moving the partial state out.
        let state = std::mem::replace(&mut self.state, RequestState::NoWorkers);
        let RequestState::Done(result) = state else {
            unreachable!("slot state was checked to be done")
        };

        Some(BufferedResponse {
            chunk_index: self.data_range.chunk_index,
            read_range: self.data_range.range.clone(),
            result,
        })
    }

    fn take_partial_continuation(&mut self) -> Option<(BufferedResponse, DataRange)> {
        let RequestState::Partial(_) = self.state else {
            return None;
        };

        let state = std::mem::replace(&mut self.state, RequestState::NoWorkers);
        let RequestState::Partial(PartialResult { data, next_range }) = state else {
            unreachable!("slot state was checked to be partial")
        };

        // Clone the current data_range once and compute both the read_range and the next_data_range from it.
        let old = self.data_range.clone();
        let read_range = BlockRange::new(*old.range.start(), *next_range.start() - 1);
        let next_data_range = DataRange {
            range: next_range,
            chunk: old.chunk,
            chunk_index: old.chunk_index,
        };

        let response = BufferedResponse {
            chunk_index: old.chunk_index,
            read_range,
            result: Ok(data),
        };
        Some((response, next_data_range))
    }
}

impl ChunkSlot {
    fn new(slot: Slot) -> Self {
        Self {
            pending_responses: VecDeque::new(),
            active: Some(slot),
        }
    }

    fn active(&self) -> Option<&Slot> {
        self.active.as_ref()
    }

    fn is_empty(&self) -> bool {
        self.pending_responses.is_empty() && self.active.is_none()
    }

    fn num_parts(&self) -> usize {
        self.pending_responses.len() + usize::from(self.active.is_some())
    }

    fn buffer_active_response(&mut self, include_partial: bool) -> BufferedActiveSlot {
        let Some(slot) = self.active.as_mut() else {
            return BufferedActiveSlot::Nothing;
        };

        if let Some(response) = slot.take_completed_response() {
            self.pending_responses.push_back(response);
            self.active = None;
            return BufferedActiveSlot::Completed;
        }

        if include_partial {
            if let Some((response, next_data_range)) = slot.take_partial_continuation() {
                self.pending_responses.push_back(response);
                self.active = None;
                return BufferedActiveSlot::Partial(next_data_range);
            }
        }

        BufferedActiveSlot::Nothing
    }

    fn debug_symbol(&self) -> char {
        if !self.pending_responses.is_empty() && self.active.is_some() {
            return '+';
        }
        if !self.pending_responses.is_empty() {
            return '#';
        }
        self.active
            .as_ref()
            .map(|slot| slot.state.debug_symbol())
            .unwrap_or('_')
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
        RequestState::Done(Err(RequestError::InternalError(format!(
            "the last returned block is {} which is below the first queried block {} from {}",
            last_block,
            range.start(),
            worker,
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

fn build_query_for_range(query: &mut crate::types::ParsedQuery, range: &BlockRange) -> String {
    if *range.start() == query.first_block() {
        query.to_string()
    } else {
        query.without_parent_hash()
    }
}

/// A misbehaving worker may report (and include) data beyond the queried
/// range. The data is an opaque compressed blob, so it can't be trimmed to
/// the range without decompressing and re-encoding it; and a continuation
/// range computed from such a `last_block` would be inverted. Reject the
/// whole response as a retriable failure so another worker can serve the
/// range instead.
fn reject_out_of_range(response: QueryResult, range: &BlockRange, worker: PeerId) -> QueryResult {
    match response {
        Ok(success) if success.ok.last_block > *range.end() => {
            tracing::warn!(
                "Got response beyond the queried range {}-{} from worker {}: last block {}",
                range.start(),
                range.end(),
                worker,
                success.ok.last_block,
            );
            Err(QueryError::Retriable(format!(
                "worker returned last block {} beyond the queried range end {}",
                success.ok.last_block,
                range.end(),
            )))
        }
        response => response,
    }
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
        RequestState::Done(Err(e)) => e.short_code(),
        RequestState::Partial(_) => "partial",
        RequestState::Pending(_) | RequestState::Paused(_) | RequestState::NoWorkers => "-",
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::types::ParsedQuery;

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

    fn partial_slot(start: u64, end: u64, last_returned: u64, data: &[u8]) -> Slot {
        Slot {
            data_range: data_range(start, end),
            state: RequestState::Partial(PartialResult {
                data: data.to_vec(),
                next_range: BlockRange::new(last_returned + 1, end),
            }),
        }
    }

    #[test]
    fn partial_slot_is_split_into_done_part_and_continuation_range() {
        let mut slot = partial_slot(100, 200, 120, b"first");

        let (response, continuation) = slot.take_partial_continuation().unwrap();

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
    fn non_partial_slot_does_not_schedule_continuation() {
        let mut slot = Slot {
            data_range: data_range(100, 200),
            state: RequestState::Done(Ok(b"complete".to_vec())),
        };

        assert!(slot.take_partial_continuation().is_none());
        assert_eq!(slot.data_range.range, BlockRange::new(100, 200));
        match slot.state {
            RequestState::Done(Ok(data)) => assert_eq!(data, b"complete"),
            _ => panic!("non-partial state should be preserved"),
        }
    }

    #[test]
    fn completed_slot_can_be_unwrapped_for_output() {
        let mut slot = Slot {
            data_range: data_range(100, 200),
            state: RequestState::Done(Ok(b"complete".to_vec())),
        };

        let response = slot.take_completed_response().unwrap();

        assert_eq!(response.read_range, BlockRange::new(100, 200));
        assert_eq!(response.chunk_index, 0);
        match response.result {
            Ok(data) => assert_eq!(data, b"complete"),
            Err(_) => panic!("completed slot should unwrap its result"),
        }
    }

    #[test]
    fn chunk_slot_keeps_completed_response_before_active_continuation() {
        let mut chunk_slot = ChunkSlot::new(partial_slot(100, 200, 120, b"first"));
        let (response, continuation) = chunk_slot
            .active
            .as_mut()
            .unwrap()
            .take_partial_continuation()
            .unwrap();
        chunk_slot.pending_responses.push_back(response);
        chunk_slot.active = Some(Slot {
            data_range: continuation,
            state: RequestState::NoWorkers,
        });

        assert_eq!(chunk_slot.num_parts(), 2);
        assert_eq!(
            chunk_slot.pending_responses.front().unwrap().read_range,
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
    fn first_range_uses_original_query_and_continuation_drops_parent_hash() {
        let query_json = r#"{
            "type": "evm",
            "fromBlock": 100,
            "parentBlockHash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "fields": {
                "block": {
                    "number": true
                }
            },
            "includeAllBlocks": true
        }"#;
        let mut query = ParsedQuery::try_from(query_json.to_owned()).unwrap();

        let first_query = build_query_for_range(&mut query, &BlockRange::new(100, 120));
        let continuation_query = build_query_for_range(&mut query, &BlockRange::new(121, 200));

        assert!(first_query.contains("parentBlockHash"));
        assert!(!continuation_query.contains("parentBlockHash"));
    }

    #[test]
    fn chunk_slot_keeps_pending_responses_before_active_slot() {
        let mut chunk_slot = ChunkSlot::new(Slot {
            data_range: data_range(141, 150),
            state: RequestState::NoWorkers,
        });
        for i in 0..3 {
            let start = 100 + i * 10;
            chunk_slot.pending_responses.push_back(BufferedResponse {
                chunk_index: 0,
                read_range: BlockRange::new(start, start + 9),
                result: Ok(Vec::new()),
            });
        }

        let mut ranges: Vec<_> = chunk_slot
            .pending_responses
            .iter()
            .map(|response| response.read_range.clone())
            .collect();
        ranges.push(chunk_slot.active.as_ref().unwrap().data_range.range.clone());
        assert_eq!(
            ranges,
            vec![
                BlockRange::new(100, 109),
                BlockRange::new(110, 119),
                BlockRange::new(120, 129),
                BlockRange::new(141, 150),
            ]
        );
        assert_eq!(chunk_slot.num_parts(), 4);
        assert_eq!(
            chunk_slot.active.as_ref().unwrap().data_range.chunk,
            test_chunk()
        );
        assert_eq!(
            chunk_slot.active.as_ref().unwrap().data_range.chunk_index,
            0
        );
    }

    #[test]
    fn chunk_slot_debug_symbol_distinguishes_partial_and_completed_multi_part_slots() {
        let mut chunk_slot = ChunkSlot::new(Slot {
            data_range: data_range(121, 200),
            state: RequestState::NoWorkers,
        });
        chunk_slot.pending_responses.push_back(BufferedResponse {
            chunk_index: 0,
            read_range: BlockRange::new(100, 120),
            result: Ok(Vec::new()),
        });

        assert_eq!(chunk_slot.debug_symbol(), '+');

        chunk_slot.active = None;

        assert_eq!(chunk_slot.debug_symbol(), '#');
    }

    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::{future::BoxFuture, StreamExt};

    use crate::network::QuerySuccess;
    use crate::types::{Compression, DatasetId, StreamRequest};

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
            Ok(self.chunk.clone())
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
            max_chunks: None,
            timeout_quantile: 0.5,
            retries: 1,
            compression: Compression::Gzip,
            skip_parent_hash_validation: false,
            eager_continuations: false,
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
    use std::sync::atomic::AtomicI64;
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

    /// Scripted outcome of one worker query.
    #[derive(Debug, Clone)]
    enum QueryEvent {
        Full,
        /// Serve only this percentage of the queried range, forcing a
        /// continuation request for the remainder.
        Partial(u8),
        /// Misbehave: report this many blocks beyond the queried range end.
        Overshoot(u64),
        Retriable,
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
                FindWorkerEvent::LongBackoff => {
                    Err(NoWorker::Backoff(Instant::now() + Duration::from_secs(60)))
                }
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
        ) -> BoxFuture<'static, QueryResult> {
            let event = self
                .query_script
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(QueryEvent::Full);
            let guard = LiveQueryGuard::new(&self.live_queries);
            Box::pin(async move {
                let _guard = guard;
                let (start, end) = (*block_range.start(), *block_range.end());
                let last = match event {
                    QueryEvent::Hang => {
                        futures::future::pending::<()>().await;
                        unreachable!("pending future never resolves")
                    }
                    QueryEvent::Retriable => {
                        return Err(QueryError::Retriable("scripted failure".to_owned()))
                    }
                    QueryEvent::Full => end,
                    QueryEvent::Overshoot(extra) => end + extra,
                    QueryEvent::Partial(_) if start == end => end,
                    QueryEvent::Partial(pct) => start + (end - start) * (pct as u64) / 100,
                };
                if last <= end {
                    self.ok_ranges
                        .lock()
                        .unwrap()
                        .push((request_id, (start, last)));
                }
                Ok(QuerySuccess {
                    ok: sqd_messages::QueryOk {
                        data: format!("{start}:{last}").into_bytes(),
                        last_block: last,
                    },
                    ttfb: Duration::from_millis(1),
                    transfer_time: Duration::from_millis(1),
                    response_size: 10,
                })
            })
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
        eager_continuations: bool,
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
                max_chunks: self.max_chunks,
                timeout_quantile: 0.5,
                retries: self.retries,
                compression: Compression::Gzip,
                skip_parent_hash_validation: false,
                eager_continuations: self.eager_continuations,
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
            1 => (1u64..=20).prop_map(QueryEvent::Overshoot),
            1 => Just(QueryEvent::Retriable),
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
                    any::<bool>(),
                    prop::option::of(1usize..=4),
                )
            })
            .prop_map(
                |(n_chunks, streams, fw, q, buffer_size, retries, eager, max_chunks)| Scenario {
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
                    eager_continuations: eager,
                    max_chunks,
                },
            )
    }

    struct StreamOutcome {
        /// (first_block, last_block) of every response chunk, in emission order.
        emissions: Vec<(u64, u64)>,
        error: Option<String>,
    }

    struct ScenarioOutcome {
        streams: Vec<StreamOutcome>,
        timed_out: bool,
        ok_ranges: Vec<(String, (u64, u64))>,
        leases_outstanding: usize,
    }

    async fn collect_stream<N: StreamingNetwork>(
        request: StreamRequest,
        network: Arc<N>,
        stream_index: u32,
    ) -> StreamOutcome {
        let mut controller = StreamController::new(request, network, stream_index, 1).unwrap();
        let mut emissions = Vec::new();
        let mut error = None;
        while let Some(item) = controller.next().await {
            match item {
                Ok(bytes) => {
                    let text = String::from_utf8(bytes).unwrap();
                    let (start, last) = text.split_once(':').unwrap();
                    emissions.push((start.parse().unwrap(), last.parse().unwrap()));
                }
                Err(e) => {
                    error = Some(e.to_string());
                    break;
                }
            }
        }
        StreamOutcome { emissions, error }
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

            let collect_all =
                futures::future::join_all(scenario.streams.iter().enumerate().map(|(i, spec)| {
                    collect_stream(scenario.request(spec, i), network.clone(), i as u32)
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
                    }
                }
                Err(_) => ScenarioOutcome {
                    streams: Vec::new(),
                    timed_out: true,
                    ok_ranges: network.ok_ranges.lock().unwrap().clone(),
                    leases_outstanding: network.lease_pool.outstanding(),
                },
            }
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 128, ..ProptestConfig::default() })]

        /// Whatever adversity the network throws at the controller(s) —
        /// backoffs, unavailability, partial / out-of-range / failing worker
        /// responses, chunk limits, several streams sharing the network —
        /// every stream must emit a gapless, strictly increasing prefix of
        /// its requested range, fully covering it on clean completion; no
        /// block range may be executed twice for the same stream; and no
        /// worker lease may leak.
        #[test]
        fn stream_invariants_hold_under_scheduling_adversity(scenario in scenario_strategy()) {
            let outcome = run_scenario(&scenario);

            // Liveness: every stream terminates (with data or an error).
            prop_assert!(!outcome.timed_out, "streams did not terminate");

            for (i, (spec, stream)) in scenario.streams.iter().zip(&outcome.streams).enumerate() {
                // No duplicates, monotonically increasing, no gaps: each
                // emission continues exactly where the previous one ended.
                let mut expected_next = spec.from;
                for &(start, last) in &stream.emissions {
                    prop_assert_eq!(
                        start,
                        expected_next,
                        "stream {} emission must continue the stream exactly (emissions: {:?})",
                        i,
                        &stream.emissions
                    );
                    prop_assert!(last >= start, "emission range must not be inverted");
                    expected_next = last + 1;
                }

                // Completeness: a stream that ended without an error must
                // have served the whole requested range (capped by the
                // dataset end and the max_chunks limit).
                if stream.error.is_none() {
                    prop_assert_eq!(
                        expected_next,
                        scenario.expected_end(spec) + 1,
                        "stream {} clean completion must cover the full range (emissions: {:?})",
                        i,
                        &stream.emissions
                    );
                }
            }

            // At-most-once execution: no block range is successfully served
            // by workers more than once for the same stream (retries of
            // *failed* attempts are fine; re-executing delivered work is not).
            let mut ok_count = HashMap::new();
            for executed in &outcome.ok_ranges {
                *ok_count.entry(executed).or_insert(0usize) += 1;
            }
            for (executed, count) in ok_count {
                prop_assert!(
                    count <= 1,
                    "{:?} was successfully executed {} times (all: {:?})",
                    executed,
                    count,
                    &outcome.ok_ranges
                );
            }

            // No lease leaks: every worker slot taken is eventually returned.
            prop_assert_eq!(
                outcome.leases_outstanding,
                0,
                "worker leases leaked after all streams finished"
            );
        }
    }

    /// An out-of-range worker response must be rejected and converted into a
    /// retriable failure rather than trusted or treated as fatal.
    #[test]
    fn out_of_range_response_is_rejected_as_retriable() {
        let success = |last_block| {
            Ok(QuerySuccess {
                ok: sqd_messages::QueryOk {
                    data: b"data".to_vec(),
                    last_block,
                },
                ttfb: Duration::from_millis(1),
                transfer_time: Duration::from_millis(1),
                response_size: 4,
            })
        };
        let range = BlockRange::new(100, 150);

        let rejected = reject_out_of_range(success(151), &range, PeerId::random());
        assert!(
            matches!(rejected, Err(QueryError::Retriable(_))),
            "overshooting response must become a retriable error"
        );
        assert!(retriable(&rejected));

        let in_range = reject_out_of_range(success(150), &range, PeerId::random());
        assert!(in_range.is_ok(), "in-range response must pass through");
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
            eager_continuations: false,
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
            eager_continuations: false,
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
}
