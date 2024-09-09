use std::{future::Future, sync::Arc};

use futures::FutureExt;
use subsquid_messages::{query_result, Range};
use tokio::task::JoinSet;
use tracing::{instrument, Instrument};

use crate::{
    controller::timeouts::TimeoutManager,
    network::NetworkClient,
    types::{ClientRequest, DataChunk, DatasetId, RequestError, ResponseChunk, SendQueryError},
    utils::{logging::StreamStats, SlidingArray},
};

pub struct StreamController {
    request: ClientRequest,
    network: Arc<NetworkClient>,
    next_index: usize,
    next_chunk: Option<DataChunk>,
    downloads: JoinSet<(Slot, usize)>,
    buffer: SlidingArray<Slot>,
    timeouts: Arc<TimeoutManager>,
    stats: Arc<StreamStats>,
    span: tracing::Span,
}

// The size of this structure never exceeds the maximum response size
enum Slot {
    Pending,
    Partial(PartialResult),
    Done(Result<ResponseChunk, RequestError>),
}

struct PartialResult {
    data: Vec<u8>,
    next_range: Range,
}

// TODO: handle "service overloaded" errors
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
            next_index: 0,
            next_chunk: Some(first_chunk),
            downloads: Default::default(),
            buffer,
            timeouts: Arc::new(TimeoutManager::new(timeout_quantile)),
            stats: Arc::new(StreamStats::new()),
            span: tracing::Span::current(),
        };
        if controller.schedule_next_chunk().is_err() {
            return Err(RequestError::Busy);
        } else {
            controller.buffer.push_back(Slot::Pending);
        }
        Ok(controller)
    }

    pub async fn poll_next_chunk(&mut self) -> Option<Result<ResponseChunk, RequestError>> {
        while self.buffer.len() < self.request.buffer_size && self.next_chunk.is_some() {
            if self.schedule_next_chunk().is_ok() {
                self.buffer.push_back(Slot::Pending);
            } else {
                break;
            }
        }

        while !self.buffer.front().is_some_and(Slot::ready) {
            let (result, index) = self
                .downloads
                .join_next()
                .await?
                .expect("Awaiting download panicked");
            self.buffer[index] = result;

            tracing::debug!(
                "Got result for chunk number {index} ({}), buffer:\n[{}]",
                short_code(&self.buffer[index]),
                self.buffer
                    .data()
                    .iter()
                    .map(Slot::debug_symbol)
                    .collect::<String>()
            );

            if let Slot::Done(Err(_)) = self.buffer[index] {
                // The stream will end at the first error. There is no need to start new downloads.
                self.next_chunk = None;
            }
        }

        let index = self.buffer.first_index();
        let result = match self.buffer.pop_front()? {
            Slot::Done(result) => Some(result),
            Slot::Partial(PartialResult { data, next_range }) => {
                match self.get_chunk_response(index, next_range) {
                    Ok(future) => {
                        self.downloads
                            .spawn(future.map(move |result| (result, index)));
                        self.buffer.push_front(Slot::Pending);
                    }
                    Err(_) => {
                        self.buffer.push_front(Slot::Done(Err(RequestError::Busy)));
                    }
                }
                Some(Ok(data))
            }
            Slot::Pending => unreachable!("First chunk is not ready"),
        };

        self.stats.maybe_write_log();

        if let Some(Ok(bytes)) = &result {
            self.stats.sent_response_chunk(bytes.len());
            tracing::debug!("Sending {} response bytes", bytes.len());
        }
        result
    }

    // Schedules downloading `self.next_chunk`
    fn schedule_next_chunk(&mut self) -> Result<(), SendQueryError> {
        let chunk = self.next_chunk.clone().expect("No next chunk to download");
        let range = self
            .request
            .query
            .intersect_with(&Range {
                begin: *chunk.start() as u32,
                end: *chunk.end() as u32,
            })
            .expect("Chunk doesn't contain requested data");
        let index: usize = self.next_index;
        let future = self
            .get_chunk_response(index, range)?
            .map(move |result| (result, index));

        self.downloads.spawn(future);

        self.next_chunk = self.network.next_chunk(&self.request.dataset_id, &chunk);
        if let Some(next_chunk) = &self.next_chunk {
            if self
                .request
                .query
                .last_block()
                .is_some_and(|last_block| last_block < *next_chunk.start())
            {
                tracing::debug!("The end of the requested range reached");
                self.next_chunk = None;
            }
        } else {
            tracing::debug!("No more chunks available");
        }
        self.next_index += 1;
        Ok(())
    }

    #[instrument(skip_all, fields(chunk_number = index))]
    fn get_chunk_response(
        &self,
        index: usize,
        range: Range,
    ) -> Result<impl Future<Output = Slot>, SendQueryError> {
        let fut = self.get_one_response(range)?.in_current_span();
        Ok(async move {
            let result = match fut.await {
                query_result::Result::Ok(result) => result,
                e => return Slot::Done(Err(RequestError::try_from(e).unwrap())),
            };
            let Some(last_block) = result.last_block else {
                return Slot::Done(Err(RequestError::InternalError(
                    "No last block in response".to_string(),
                )));
            };
            if last_block == range.end as u64 {
                Slot::Done(Ok(result.data))
            } else if last_block < range.begin as u64 {
                tracing::warn!("Got empty response for range {}-{}", range.begin, range.end);
                Slot::Done(Err(RequestError::InternalError(
                    "Got empty response".to_string(),
                )))
            } else {
                tracing::debug!(
                    "Got partial response with blocks {}-{}. {} blocks left in this chunk",
                    range.begin,
                    last_block,
                    range.end - last_block as u32,
                );
                Slot::Partial(PartialResult {
                    data: result.data,
                    next_range: Range {
                        begin: (last_block + 1) as u32,
                        end: range.end,
                    },
                })
            }
        }
        .in_current_span())
    }

    fn get_one_response(
        &self,
        range: Range,
    ) -> Result<impl Future<Output = query_result::Result>, SendQueryError> {
        let network = self.network.clone();
        let timeouts = self.timeouts.clone();
        let request = self.request.clone();
        let stats = self.stats.clone();
        // TODO: don't modify the query
        let query = request.query.with_range(&range);
        let send_query = move || {
            let start_time = tokio::time::Instant::now();
            Self::query_some_worker(
                network.clone(),
                query.clone(),
                request.dataset_id.clone(),
                range,
                stats.clone(),
            )
            .map(|future| {
                future
                    .in_current_span()
                    .map(move |result| (result, start_time))
            })
        };
        let mut requests = JoinSet::new();
        for _ in 0..request.request_multiplier {
            requests.spawn(send_query()?);
        }
        Ok(async move {
            let mut tries_left = request.retries;
            let timeouts = timeouts.clone();
            loop {
                let (result, start_time) = tokio::select! {
                    _ = timeouts.sleep(), if tries_left > 0 => {
                        tries_left -= 1;
                        tracing::debug!("Request is running for too long, sending one more query");
                        match send_query() {
                            Ok(fut) => _ = requests.spawn(fut),
                            Err(e) => tracing::warn!("Couldn't schedule query after timeout: {e:?}"),
                        };
                        continue;
                    }
                    result = requests.join_next() => {
                        result
                            .expect("No downloads left for chunk")
                            .expect("Awaiting download panicked")
                    }
                };
                if matches!(result, query_result::Result::Ok(_)) {
                    let elapsed = start_time.elapsed();
                    timeouts.observe(elapsed);
                    tracing::debug!("Query completed in {elapsed:?}");
                }
                if !retriable(&result) || tries_left == 0 {
                    return result;
                }
                tries_left -= 1;
                match send_query() {
                    Ok(fut) => {
                        tracing::debug!("Retrying query, {result:?}");
                        requests.spawn(fut);
                    },
                    Err(e) => {
                        tracing::warn!("Couldn't schedule retried query: {e:?}");
                        if requests.is_empty() {
                            return result;
                        }
                    }
                }
            }
        }
        .in_current_span())
    }

    fn query_some_worker(
        network: Arc<NetworkClient>,
        query: String,
        dataset_id: DatasetId,
        range: Range,
        stats: Arc<StreamStats>,
    ) -> Result<impl Future<Output = query_result::Result>, SendQueryError> {
        let worker = network.find_worker(&dataset_id, range.begin);
        let Some(worker) = worker else {
            return Err(SendQueryError::NoWorkers);
        };
        tracing::debug!(
            "Sending query for range ({}-{}) to worker {}",
            range.begin,
            range.end,
            worker,
        );
        let receiver = network
            .query_worker(&worker, &dataset_id, query)
            .map_err(|_| SendQueryError::TransportQueueFull)?;
        stats.query_sent();
        Ok(receiver.map(|recv_result| recv_result.expect("Sending query result failed")))
    }
}

impl Drop for StreamController {
    fn drop(&mut self) {
        let _enter = self.span.enter();
        self.stats.write_summary();
    }
}

impl Slot {
    fn ready(&self) -> bool {
        match &self {
            Slot::Pending => false,
            _ => true,
        }
    }

    fn debug_symbol(&self) -> char {
        match &self {
            Slot::Pending => '.',
            Slot::Partial(_) => '+',
            Slot::Done(Ok(_)) => '#',
            Slot::Done(Err(_)) => '!',
        }
    }
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

fn short_code(result: &Slot) -> &'static str {
    match result {
        Slot::Done(Ok(_)) => "ok",
        Slot::Done(Err(e)) => e.short_code(),
        Slot::Partial(_) => "partial",
        Slot::Pending => "-",
    }
}
