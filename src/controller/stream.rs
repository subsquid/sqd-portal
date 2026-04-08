#![allow(unstable_name_collisions)]

use std::{
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
    network::{ChunkNotFound, NetworkClient, NoWorker, QueryResult, WorkerLease},
    types::{
        BlockRange, ChunkId, DataChunk, QueryError, RequestError, ResponseChunk, SendQueryError,
        StreamRequest,
    },
    utils::{logging::StreamStats, SlidingArray},
};

const MAX_IDLE_TIME: Duration = Duration::from_millis(1000);

pub struct StreamController {
    request: StreamRequest,
    network: Arc<NetworkClient>,
    buffer: SlidingArray<Slot>,
    next_chunk: Option<DataChunk>,
    timeouts: TimeoutManager,
    stats: StreamStats,
    span: tracing::Span,
    last_error: Option<String>,
    stream_index: u64,
    priority_stride: u64,
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

enum RequestState {
    /// Couldn't find enough workers now.
    /// Retry whenever the stream is polled, or give up if this is the first slot.
    NoWorkers,
    /// It's known how long to wait until the workers will become available.
    /// If this is the first slot, it may be worth to wait.
    Paused(PausedState),
    /// Workers are allocated and queries to some of them are running. Waiting for the results.
    Pending(PendingRequests),
    /// We've got a successful result from one of the workers but it didn't cover the whole chunk range.
    /// A continuation request should be sent once we feed this result to the client.
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

impl UpdateStatus {
    fn updated(&self) -> bool {
        matches!(self, UpdateStatus::Updated)
    }
}

impl StreamController {
    pub fn new(
        request: StreamRequest,
        network: Arc<NetworkClient>,
        stream_index: u64,
        priority_stride: u64,
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
        for slot in buffer.iter_mut() {
            updated |= self.poll_slot(slot, ctx).updated();
        }
        self.buffer = buffer;

        if updated {
            tracing::debug!(
                "Buffer: [{}]",
                self.buffer
                    .data()
                    .iter()
                    .map(|s| s.state.debug_symbol())
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

    #[instrument(skip_all, level="debug", fields(chunk_index = slot.data_range.chunk_index))]
    fn poll_slot(&mut self, slot: &mut Slot, ctx: &mut Context<'_>) -> UpdateStatus {
        let pending = match &mut slot.state {
            RequestState::Pending(pending) => pending,
            RequestState::NoWorkers => {
                match self.start_querying_chunk(slot.data_range.clone(), ctx) {
                    Ok(s) => {
                        *slot = s;
                        return UpdateStatus::Updated;
                    }
                    Err((s, e)) => {
                        *slot = s;
                        self.last_error = Some(e.to_string());
                        if matches!(slot.state, RequestState::NoWorkers) {
                            return UpdateStatus::NotUpdated;
                        } else {
                            return UpdateStatus::Updated;
                        }
                    }
                }
            }
            RequestState::Paused(state) => match state.timeout.as_mut().poll_unpin(ctx) {
                Poll::Pending => return UpdateStatus::NotUpdated,
                Poll::Ready(_) => match self.start_querying_chunk(slot.data_range.clone(), ctx) {
                    Ok(s) => {
                        *slot = s;
                        return UpdateStatus::Updated;
                    }
                    Err((s, e)) => {
                        *slot = s;
                        self.last_error = Some(e.to_string());
                        return UpdateStatus::Updated;
                    }
                },
            },
            _ => return UpdateStatus::NotUpdated,
        };

        let mut num_not_started = 0;
        let mut num_running = 0;
        let mut num_new_finished = 0;
        for request in pending.requests.iter_mut() {
            match request {
                WorkerRequest::NotStarted(_) => num_not_started += 1,
                WorkerRequest::Finished(_) => {}
                WorkerRequest::Running(running) => {
                    let Poll::Ready(response) = running.resp.poll_unpin(ctx) else {
                        num_running += 1;
                        continue;
                    };
                    num_new_finished += 1;

                    let response = response.expect("Worker query task shouldn't panic");

                    // This is intentionally measured when the result has been polled, not when it's ready.
                    // If the stream is consumed slower than generated, this duration may get significantly higher than the response time.
                    // This way the extra "follow up" queries won't be sent, saving on the number of queries.
                    let duration = running.start_time.elapsed();
                    self.timeouts.observe(duration);

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
                        slot.state = parse_response(
                            response,
                            &slot.data_range.range,
                            running.worker,
                            duration,
                        );
                        return UpdateStatus::Updated;
                    }
                }
            }
        }

        if num_running == 0 && num_not_started == 0 {
            // All query attempts failed
            let mut errors = Vec::with_capacity(pending.requests.len());
            for request in pending.requests.drain(..) {
                let WorkerRequest::Finished(f) = request else {
                    unreachable!("all worker requests should be finished")
                };
                let error = RequestError::from_query_error(f.result.unwrap_err().clone(), f.worker)
                    .to_string();
                errors.push(error);
            }
            let message = format!("All query attempts failed: {}", errors.join("; "));
            slot.state = RequestState::Done(Err(RequestError::InternalError(message)));
            return UpdateStatus::Updated;
        }

        let timed_out = pending.timeout.as_mut().poll(ctx).is_ready();

        let should_retry = num_new_finished > 0 || timed_out || num_running == 0;
        if should_retry && num_not_started > 0 {
            // Issue a retry
            if timed_out {
                tracing::trace!(
                    "Request didn't complete in {}ms, sending one more query",
                    pending.timeout_duration.as_millis()
                );
            }
            for req in &mut pending.requests {
                if let WorkerRequest::NotStarted(worker) = req {
                    let is_speculative = num_running > 0;
                    let lease = worker
                        .lease
                        .take()
                        .expect("worker lease should only be used once");
                    let request = self.send_query(&slot.data_range, lease, is_speculative);
                    *req = WorkerRequest::Running(request);
                    pending.set_timeout(self.timeouts.current_timeout(), ctx);
                    break;
                }
            }
            return UpdateStatus::NotUpdated;
        }

        if should_retry {
            // The last query attempt timed out, give up
            let mut errors = Vec::with_capacity(pending.requests.len());
            for request in pending.requests.drain(..) {
                let error = match request {
                    WorkerRequest::NotStarted(_) => unreachable!("all queries are already running"),
                    WorkerRequest::Finished(f) => {
                        RequestError::from_query_error(f.result.unwrap_err().clone(), f.worker)
                            .to_string()
                    }
                    WorkerRequest::Running(r) => {
                        format!(
                            "query to worker {} timed out after {}ms",
                            r.worker,
                            r.start_time.elapsed().as_millis()
                        )
                    }
                };
                errors.push(error);
            }
            let message = format!("All query attempts failed: {}", errors.join("; "));
            slot.state = RequestState::Done(Err(RequestError::InternalError(message)));
            return UpdateStatus::Updated;
        }

        UpdateStatus::NotUpdated
    }

    fn pop_response(
        &mut self,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Result<ResponseChunk, RequestError>>> {
        let Some(slot) = self.buffer.pop_front() else {
            return Poll::Ready(None);
        };
        let chunk_index = slot.data_range.chunk_index;
        let (result, read_range) = match slot.state {
            RequestState::Done(result) => (Poll::Ready(Some(result)), slot.data_range.range),
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
                let range = slot.data_range.range.clone();
                self.buffer.push_front(slot);
                (Poll::Pending, range)
            }
            RequestState::Pending(_) => {
                // The query is still running, keep waiting
                let range = slot.data_range.range.clone();
                self.buffer.push_front(slot);
                (Poll::Pending, range)
            }
            RequestState::Partial(PartialResult { data, next_range }) => {
                // Return the partial result and schedule the continuation query
                let read_range =
                    BlockRange::new(*slot.data_range.range.start(), *next_range.start() - 1);
                let next_data_range = slot.data_range.with_range(next_range);
                let slot = match self.start_querying_chunk(next_data_range, ctx) {
                    Ok(slot) => slot,
                    Err((slot, e)) => {
                        tracing::debug!("Couldn't schedule continuation request: {e:?}");
                        slot
                    }
                };
                self.buffer.push_front(slot);
                (Poll::Ready(Some(Ok(data))), read_range)
            }
        };

        if let Poll::Ready(Some(Ok(bytes))) = &result {
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

        result
    }

    fn try_fill_slots(&mut self, ctx: &mut Context<'_>) {
        if let Some(Slot {
            state: RequestState::Paused(_),
            ..
        }) = self.buffer.back()
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
                    self.buffer.push_back(slot);
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
                        self.buffer.push_back(slot);
                    }
                    self.last_error = Some(e.to_string());
                    self.next_chunk = Some(chunk);
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
    ) -> Result<Slot, (Slot, SendQueryError)> {
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
                Err((slot, err))
            }
            Err(err @ SendQueryError::Backoff(until)) => {
                let mut slot = Slot {
                    data_range,
                    state: RequestState::Paused(PausedState::new(until)),
                };
                self.poll_slot(&mut slot, ctx);
                Err((slot, err))
            }
        }
    }

    /// Leases `count` distinct workers for the given range, releasing all on any failure.
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
        let query = if range.chunk_index == 0 {
            self.request.query.to_string()
        } else {
            self.request.query.without_parent_hash()
        };
        let start_time = tokio::time::Instant::now();

        let priority = self.stream_index * self.priority_stride + range.chunk_index as u64;

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

impl Drop for StreamController {
    fn drop(&mut self) {
        let _enter = self.span.enter();
        self.stats
            .write_summary(&self.request, self.last_error.take());
    }
}

impl futures::Stream for StreamController {
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

fn retriable(result: &QueryResult) -> bool {
    match result {
        Ok(_) => false,
        Err(QueryError::BadRequest(_)) => false,
        Err(QueryError::Retriable(_)) => true,
        Err(QueryError::Failure(_)) => false,
        Err(QueryError::RateLimitExceeded) => true,
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
