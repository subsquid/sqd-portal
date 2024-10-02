use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures::FutureExt;
use sqd_contract_client::PeerId;
use sqd_messages::query_result;
use tracing::instrument;

use crate::{
    controller::timeouts::TimeoutManager,
    network::NetworkClient,
    types::{BlockRange, ClientRequest, RequestError, ResponseChunk, SendQueryError},
    utils::{logging::StreamStats, SlidingArray},
};

pub struct StreamController {
    request: ClientRequest,
    network: Arc<NetworkClient>,
    buffer: SlidingArray<Slot>,
    next_chunk: Option<BlockRange>,
    timeouts: Arc<TimeoutManager>,
    stats: Arc<StreamStats>,
    span: tracing::Span,
}

struct Slot {
    range: BlockRange,
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
    timeout: Pin<Box<tokio::time::Sleep>>,
    timeout_duration: Duration,
}

struct PartialResult {
    data: ResponseChunk,
    next_range: BlockRange,
    // last_block: BlockNumber,
}

struct WorkerRequest {
    resp: tokio::sync::oneshot::Receiver<query_result::Result>,
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
            return Err(RequestError::NotFound(format!(
                "No chunk found for block {}",
                request.query.first_block()
            )));
        };

        let buffer = SlidingArray::with_capacity(request.buffer_size);
        let timeout_quantile = request.timeout_quantile;
        let mut controller = Self {
            request,
            network,
            buffer,
            next_chunk: Some(first_chunk),
            timeouts: Arc::new(TimeoutManager::new(timeout_quantile)),
            stats: Arc::new(StreamStats::new()),
            span: tracing::Span::current(),
        };
        controller.try_fill_slots();
        if controller.buffer.total_size() == 0 {
            return Err(RequestError::Busy);
        }
        Ok(controller)
    }

    pub fn poll_next(
        &mut self,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Result<ResponseChunk, RequestError>>> {
        // extract this field to be able to pass both its values and `&mut self` to the method
        let mut buffer = std::mem::take(&mut self.buffer);
        let mut updated = false;
        for (index, slot) in buffer.enumerate_mut() {
            updated |= self.poll_slot(index, slot, ctx);
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

        let result = self.pop_response();

        self.try_fill_slots();

        return result;
    }

    #[instrument(skip_all, fields(chunk_index = index))]
    fn poll_slot(&mut self, index: usize, slot: &mut Slot, ctx: &mut Context<'_>) -> bool {
        let RequestState::Pending(pending) = &mut slot.state else {
            return false;
        };

        assert!(!pending.requests.is_empty());
        let mut result = None;
        let mut retry = false;
        pending.requests.retain_mut(|request| {
            let response = match request.resp.poll_unpin(ctx) {
                Poll::Pending => return true,
                Poll::Ready(res) => res.expect("Query result sender dropped"),
            };
            let duration = request.start_time.elapsed();

            if better_result(result.as_ref().map(|(response, _, _)| response), &response) {
                result = Some((response, duration, request.worker));
            }
            false
        });

        if result
            .as_ref()
            .is_some_and(|(response, _, _)| retriable(&response) && pending.tries_left > 0)
        {
            tracing::debug!("Retrying request: {:?}", result.as_ref().unwrap().0);
            retry = true;
        } else if let Some((response, duration, worker)) = result {
            self.timeouts.observe(duration);
            slot.state = parse_response(response, &slot.range);
            tracing::debug!(
                "Got result ({}) in {}ms from {}",
                short_code(&slot.state),
                duration.as_millis(),
                worker
            );
            return true;
        }

        match pending.timeout.as_mut().poll(ctx) {
            Poll::Pending => {}
            Poll::Ready(()) => {
                if pending.tries_left > 0 {
                    tracing::debug!(
                        "Request didn't complete in {}ms, sending one more query",
                        pending.timeout_duration.as_millis()
                    );
                    retry = true;
                } else {
                    assert!(!pending.requests.is_empty());
                    // wait for some request to complete
                }
            }
        }

        if retry {
            assert!(pending.tries_left > 0);
            pending.tries_left -= 1;
            pending.set_timeout(self.timeouts.current_timeout());
            match self.send_query(&slot.range, index) {
                Ok(worker_request) => {
                    pending.requests.push(worker_request);
                }
                Err(e) => {
                    tracing::debug!("Couldn't schedule request: {e:?}");
                    if pending.requests.is_empty() {
                        let (response, _, _) =
                            result.expect("if there are no requests left, there must be a result");
                        // use latest error as the result
                        slot.state =
                            RequestState::Done(Err(RequestError::try_from(response).unwrap()));
                        return true;
                    } else {
                        // wait for other requests to complete
                    }
                }
            }
        }
        false
    }

    fn pop_response(&mut self) -> Poll<Option<Result<ResponseChunk, RequestError>>> {
        let chunk_index = self.buffer.first_index();
        let Some(slot) = self.buffer.pop_front() else {
            return Poll::Ready(None);
        };
        let slot_range = slot.range.clone();
        let (result, range) = match slot.state {
            RequestState::Done(result) => (Poll::Ready(Some(result)), slot_range),
            RequestState::Pending(_) => {
                self.buffer.push_front(slot);
                (Poll::Pending, slot_range)
            }
            RequestState::Partial(PartialResult { data, next_range }) => {
                let state = match self.send_query(&next_range, chunk_index) {
                    Ok(request) => RequestState::Pending(PendingRequests::new(
                        request,
                        self.timeouts.current_timeout(),
                        self.request.retries as u8,
                    )),
                    Err(e) => {
                        tracing::debug!("Couldn't schedule continuation request: {e:?}");
                        RequestState::Done(Err(RequestError::Busy))
                    }
                };
                let range = BlockRange::new(*slot.range.start(), *next_range.start() - 1);
                self.buffer.push_front(Slot {
                    range: next_range,
                    state,
                });

                (Poll::Ready(Some(Ok(data))), range)
            }
        };

        if let Poll::Ready(Some(Ok(bytes))) = &result {
            self.stats
                .sent_response_chunk(*range.end() - *range.start() + 1, bytes.len());
            tracing::debug!(
                chunk_index,
                "Writing response blocks {}-{} ({} bytes)",
                *range.start(),
                *range.end(),
                bytes.len()
            );
        }

        result
    }

    fn try_fill_slots(&mut self) {
        while self.buffer.len() < self.request.buffer_size
            && !self
                .request
                .max_chunks
                .is_some_and(|limit| self.buffer.total_size() >= limit)
        {
            let Some(chunk) = self.next_chunk.as_ref() else {
                break;
            };
            let next_index = self.buffer.total_size();
            match self.start_querying_chunk(chunk, next_index) {
                Ok(slot) => {
                    self.buffer.push_back(slot);
                    self.next_chunk = self.get_next_chunk(chunk);
                }
                Err(e) => {
                    tracing::debug!("Couldn't schedule request: {e:?}");
                    break;
                }
            }
        }
    }

    fn get_next_chunk(&self, chunk: &BlockRange) -> Option<BlockRange> {
        let next_chunk = self.network.next_chunk(&self.request.dataset_id, &chunk);

        if let Some(next_chunk) = &next_chunk {
            if self
                .request
                .query
                .last_block()
                .is_some_and(|last_block| last_block < *next_chunk.start())
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
        &self,
        chunk: &BlockRange,
        index: usize,
    ) -> Result<Slot, SendQueryError> {
        let range = self
            .request
            .query
            .intersect_with(chunk)
            .expect("Chunk doesn't contain requested data");
        let request = self.send_query(&range, index)?;
        Ok(Slot {
            range,
            state: RequestState::Pending(PendingRequests::new(
                request,
                self.timeouts.current_timeout(),
                self.request.retries as u8,
            )),
        })
    }

    fn send_query(
        &self,
        range: &BlockRange,
        chunk_index: usize,
    ) -> Result<WorkerRequest, SendQueryError> {
        let Some(worker) = self
            .network
            .find_worker(&self.request.dataset_id, *range.start())
        else {
            return Err(SendQueryError::NoWorkers);
        };
        tracing::debug!(
            "Sending query for chunk {chunk_index} ({}-{}) to worker {}",
            range.start(),
            range.end(),
            worker,
        );
        let start_time = tokio::time::Instant::now();
        let receiver = self
            .network
            .query_worker(
                &worker,
                &self.request.dataset_id,
                self.request.query.with_range(&range),
            )
            .map_err(|_| SendQueryError::TransportQueueFull)?;
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
        self.stats.write_summary();
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

impl PendingRequests {
    fn new(request: WorkerRequest, timeout: Duration, retries: u8) -> Self {
        Self {
            requests: vec![request],
            tries_left: retries,
            timeout: Box::pin(tokio::time::sleep(timeout)),
            timeout_duration: timeout,
        }
    }

    fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = Box::pin(tokio::time::sleep(timeout));
        self.timeout_duration = timeout;
    }
}

fn parse_response(response: query_result::Result, range: &BlockRange) -> RequestState {
    let result = match response {
        query_result::Result::Ok(result) => result,
        e => return RequestState::Done(Err(RequestError::try_from(e).unwrap())),
    };
    let Some(last_block) = result.last_block else {
        return RequestState::Done(Err(RequestError::InternalError(
            "No last block in the response".to_string(),
        )));
    };
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

fn better_result(prev: Option<&query_result::Result>, new: &query_result::Result) -> bool {
    let Some(prev) = prev else {
        return true;
    };
    if retriable(prev) {
        return true;
    }
    matches!(new, query_result::Result::Ok(_))
}

fn retriable(result: &query_result::Result) -> bool {
    use query_result::Result;
    match result {
        Result::ServerError(_)
        | Result::TimeoutV1(_)
        | Result::Timeout(_)
        | Result::NoAllocation(_) => true,
        Result::Ok(_) | Result::BadRequest(_) => false,
    }
}

fn short_code(result: &RequestState) -> &'static str {
    match result {
        RequestState::Done(Ok(_)) => "ok",
        RequestState::Done(Err(e)) => e.short_code(),
        RequestState::Partial(_) => "partial",
        RequestState::Pending(_) => "-",
    }
}
