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
use tracing::instrument;

use crate::{
    controller::timeouts::TimeoutManager,
    network::{NetworkClient, NoWorker, QueryResult},
    types::{
        BlockRange, ChunkId, ClientRequest, DataChunk, QueryError, RequestError, ResponseChunk,
        SendQueryError,
    },
    utils::{logging::StreamStats, SlidingArray},
};

const MAX_IDLE_TIME: Duration = Duration::from_millis(1000);

pub struct StreamController {
    request: ClientRequest,
    network: Arc<NetworkClient>,
    buffer: SlidingArray<Slot>,
    next_chunk: Option<DataChunk>,
    timeouts: TimeoutManager,
    stats: StreamStats,
    span: tracing::Span,
    last_error: Option<String>,
}

pub struct DataRange {
    pub range: BlockRange,
    pub chunk: DataChunk,
    pub chunk_index: usize,
}

struct Slot {
    data_range: DataRange,
    state: RequestState,
}

// The size of this structure never exceeds the maximum response size
enum RequestState {
    Pending(PendingRequests),
    Partial(PartialResult),
    Done(Result<ResponseChunk, RequestError>),
}

struct PendingRequests {
    requests: Vec<WorkerRequest>,
    tries_left: u8,
    last_error: Option<String>,
    timeout: Pin<Box<tokio::time::Sleep>>,
    timeout_duration: Duration,
}

struct PartialResult {
    data: ResponseChunk,
    next_range: BlockRange,
}

struct WorkerRequest {
    resp: tokio::sync::oneshot::Receiver<QueryResult>,
    start_time: tokio::time::Instant,
    worker: PeerId,
}

impl StreamController {
    pub fn new(request: ClientRequest, network: Arc<NetworkClient>) -> Result<Self, RequestError> {
        let first_chunk = network.find_chunk(&request.dataset_id, request.query.first_block());
        let Some(first_chunk) = first_chunk else {
            tracing::info!(
                "No chunk found for block {}, dataset {}",
                request.query.first_block(),
                request.dataset_id
            );
            return Err(RequestError::NoData(format!(
                "No chunk found for block {}",
                request.query.first_block()
            )));
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
            updated |= self.poll_slot(slot, ctx);
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

    #[instrument(skip_all, fields(chunk_index = slot.data_range.chunk_index))]
    fn poll_slot(&mut self, slot: &mut Slot, ctx: &mut Context<'_>) -> bool {
        let RequestState::Pending(pending) = &mut slot.state else {
            return false;
        };

        let mut result = None;
        let mut retry = false;
        pending.requests.retain_mut(|request| {
            let response = match request.resp.poll_unpin(ctx) {
                Poll::Pending => return true,
                Poll::Ready(res) => res.expect("Query result sender dropped"),
            };
            // This is intentionally measured when the result has been polled, not when it's ready.
            // If the stream is consumed slower than generated, this duration may get significantly higher than the response time.
            // This way the extra "follow up" queries won't be sent, saving on the number of queries.
            let duration = request.start_time.elapsed();

            if better_result(result.as_ref().map(|(response, _, _)| response), &response) {
                result = Some((response, duration, request.worker));
            }
            false
        });

        if result
            .as_ref()
            .is_some_and(|(response, _, _)| retriable(response) && pending.tries_left > 0)
        {
            let (response, _, _) = result.as_ref().unwrap();
            pending.last_error = Some(response.as_ref().unwrap_err().to_string());
            tracing::debug!("Retrying request: {:?}", response);
            retry = true;
        } else if let Some((response, duration, worker)) = result {
            self.timeouts.observe(duration);
            slot.state = parse_response(response, &slot.data_range.range);
            tracing::debug!(
                "Got result ({}) in {}ms from {}",
                short_code(&slot.state),
                duration.as_millis(),
                worker
            );
            return true;
        }

        #[allow(clippy::collapsible_else_if)]
        if pending.timeout.as_mut().poll(ctx).is_ready() {
            if pending.tries_left > 0 {
                retry = true;
                if !pending.requests.is_empty() {
                    tracing::debug!(
                        "Request didn't complete in {}ms, sending one more query",
                        pending.timeout_duration.as_millis()
                    );
                }
            } else {
                if pending.requests.is_empty() {
                    slot.state =
                        RequestState::Done(Err(RequestError::InternalError("Timeout".to_string())));
                    return true;
                } else {
                    // wait for other requests to complete
                }
            }
        }

        if retry {
            assert!(pending.tries_left > 0);
            pending.tries_left -= 1;
            match self.send_query(&slot.data_range, ctx) {
                Ok(worker_request) => {
                    pending.set_timeout(self.timeouts.current_timeout());
                    pending.requests.push(worker_request);
                }
                Err(SendQueryError::Backoff(until)) if pending.tries_left > 0 => {
                    pending.set_deadline(until);
                    tracing::debug!(
                        "Pausing for {}ms before retrying request",
                        pending.timeout_duration.as_millis()
                    );
                }
                Err(e) if !pending.requests.is_empty() => {
                    // wait for other requests to complete
                    tracing::debug!("Couldn't schedule retry: {e:?}");
                    if pending.last_error.is_none() {
                        pending.last_error = Some(e.to_string());
                    }
                    pending.set_timeout(self.timeouts.current_timeout());
                }
                Err(e) => {
                    let last_error = pending.last_error.take().unwrap_or_else(|| e.to_string());
                    slot.state = RequestState::Done(Err(RequestError::InternalError(last_error)));
                    return true;
                }
            }
            assert!(pending.timeout.poll_unpin(ctx).is_pending()); // pass the context to wake
        }
        false
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
            RequestState::Pending(ref pending) => {
                if pending.is_paused() {
                    let duration = pending.time_left();
                    if duration > MAX_IDLE_TIME {
                        return Poll::Ready(Some(Err(RequestError::BusyFor(duration))));
                    } else {
                        self.stats.throttled(duration);
                    }
                }
                let range = slot.data_range.range.clone();
                self.buffer.push_front(slot);
                (Poll::Pending, range)
            }
            RequestState::Partial(PartialResult { data, next_range }) => {
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
            tracing::debug!(
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
        if self.buffer.back().is_some_and(Slot::is_paused) {
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
                Err((_slot, e)) => {
                    tracing::debug!("Couldn't schedule request: {e:?}");
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
        let range = range.with_range(block_range);
        let mut pending = match self.send_query(&range, ctx) {
            Ok(request) => PendingRequests::new(
                request,
                self.timeouts.current_timeout(),
                self.request.retries,
            ),
            Err(SendQueryError::Backoff(until)) => {
                let request = PendingRequests::paused(until, self.request.retries);
                tracing::debug!(
                    "Pausing for {}ms before sending request",
                    request.timeout_duration.as_millis()
                );
                request
            }
            Err(err) => {
                return Err((
                    Slot {
                        data_range: range,
                        state: RequestState::Done(Err(RequestError::InternalError(
                            err.to_string(),
                        ))),
                    },
                    err,
                ))
            }
        };
        assert!(pending.timeout.poll_unpin(ctx).is_pending());
        Ok(Slot {
            data_range: range,
            state: RequestState::Pending(pending),
        })
    }

    fn send_query(
        &mut self,
        range: &DataRange,
        ctx: &mut Context<'_>,
    ) -> Result<WorkerRequest, SendQueryError> {
        let worker =
            match self
                .network
                .find_worker(&self.request.dataset_id, *range.range.start(), true)
            {
                Ok(worker) => worker,
                Err(NoWorker::AllUnavailable) => return Err(SendQueryError::NoWorkers),
                Err(NoWorker::Backoff(until)) => return Err(SendQueryError::Backoff(until)),
            };
        tracing::debug!(
            "Sending query for chunk {} ({}-{}) to worker {}",
            range.chunk_index,
            range.range.start(),
            range.range.end(),
            worker,
        );
        let start_time = tokio::time::Instant::now();
        let mut receiver = self
            .network
            .query_worker(
                &worker,
                &ChunkId::new(self.request.dataset_id.clone(), range.chunk),
                &range.range,
                self.request.query.to_string(),
                false,
            )
            .map_err(|_| SendQueryError::TransportQueueFull)?;
        assert!(receiver.poll_unpin(ctx).is_pending());
        self.stats.query_sent();
        Ok(WorkerRequest {
            resp: receiver,
            start_time,
            worker,
        })
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
        match &self.state {
            RequestState::Pending(pending) => pending.is_paused(),
            _ => false,
        }
    }
}

impl PendingRequests {
    fn new(request: WorkerRequest, timeout: Duration, retries: u8) -> Self {
        Self {
            requests: vec![request],
            tries_left: retries,
            last_error: None,
            timeout: Box::pin(tokio::time::sleep(timeout)),
            timeout_duration: timeout,
        }
    }

    fn paused(until: Instant, retries: u8) -> Self {
        let timeout = until.duration_since(Instant::now());
        Self {
            requests: vec![],
            tries_left: retries,
            last_error: None,
            timeout: Box::pin(tokio::time::sleep_until(until)),
            timeout_duration: timeout,
        }
    }

    // Paused state means that all workers are busy now and asked to backoff the next request
    fn is_paused(&self) -> bool {
        self.requests.is_empty()
    }

    fn time_left(&self) -> Duration {
        self.timeout.deadline().duration_since(Instant::now())
    }

    fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = Box::pin(tokio::time::sleep(timeout));
        self.timeout_duration = timeout;
    }

    fn set_deadline(&mut self, deadline: Instant) {
        self.timeout = Box::pin(tokio::time::sleep_until(deadline));
        self.timeout_duration = deadline.duration_since(Instant::now());
    }
}

fn parse_response(response: QueryResult, range: &BlockRange) -> RequestState {
    let result = match response {
        Ok(result) => result,
        Err(e) => return RequestState::Done(Err(RequestError::from(e))),
    };

    let last_block = result.last_block;
    if last_block == *range.end() {
        RequestState::Done(Ok(result.data))
    } else if last_block < *range.start() {
        tracing::warn!(
            "Got empty response for range {}-{}",
            range.start(),
            range.end()
        );
        RequestState::Done(Err(RequestError::InternalError(
            "Got empty response".to_string(),
        )))
    } else {
        RequestState::Partial(PartialResult {
            data: result.data,
            next_range: BlockRange::new(last_block + 1, *range.end()),
        })
    }
}

fn better_result(prev: Option<&QueryResult>, new: &QueryResult) -> bool {
    let Some(prev) = prev else {
        return true;
    };
    if retriable(prev) {
        return true;
    }
    new.is_ok()
}

fn retriable(result: &QueryResult) -> bool {
    matches!(result, Err(QueryError::Retriable(_)))
}

fn short_code(result: &RequestState) -> &'static str {
    match result {
        RequestState::Done(Ok(_)) => "ok",
        RequestState::Done(Err(e)) => e.short_code(),
        RequestState::Partial(_) => "partial",
        RequestState::Pending(_) => "-",
    }
}
