use std::{future::Future, sync::Arc};

use futures::FutureExt;
use subsquid_messages::{data_chunk::DataChunk, query_result, Range};
use tokio::task::JoinSet;
use tracing::instrument;

use crate::{
    controller::timeouts::TimeoutManager,
    network::NetworkClient,
    types::{ClientRequest, DatasetId, RequestError, ResponseChunk},
    utils::SlidingArray,
};

pub struct StreamController {
    request: ClientRequest,
    stream_id: usize,
    network: Arc<NetworkClient>,
    next_index: usize,
    next_chunk: Option<DataChunk>,
    downloads: JoinSet<(Result<ResponseChunk, RequestError>, usize)>,
    buffer: SlidingArray<Option<Result<ResponseChunk, RequestError>>>,
    timeouts: Arc<TimeoutManager>,
    // The first request is sent out to probe it's validity.
    // Only after that multiple requests are sent in parallel.
    trial: bool,
}

impl StreamController {
    pub fn new(
        request: ClientRequest,
        stream_id: usize,
        network: Arc<NetworkClient>,
    ) -> Result<Self, RequestError> {
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
        let mut streamer = Self {
            request,
            stream_id,
            network,
            next_index: 0,
            next_chunk: Some(first_chunk),
            downloads: Default::default(),
            buffer,
            timeouts: Arc::new(TimeoutManager::new(timeout_quantile)),
            trial: true,
        };

        // Intentionally schedule only one download not to waste resources in case of bad request
        streamer.schedule_next_chunk();

        Ok(streamer)
    }

    #[instrument(skip_all, fields(stream_id = self.stream_id))]
    pub async fn poll_next_chunk(&mut self) -> Option<Result<ResponseChunk, RequestError>> {
        while self.buffer.len() < self.request.buffer_size
            && self.next_chunk.is_some()
            && !self.trial
        {
            self.schedule_next_chunk();
        }

        while !self.buffer.front().is_some_and(Option::is_some) {
            let (result, index) = self
                .downloads
                .join_next()
                .await?
                .expect("Awaiting download panicked");
            if self.buffer.get(index).is_some_and(Option::is_none) {
                let code = short_code(&result);
                self.buffer[index] = Some(result);
                let mask = self
                    .buffer
                    .data()
                    .iter()
                    .map(|slot| if slot.is_some() { '#' } else { '.' })
                    .collect::<String>();
                tracing::debug!(
                    "Got response for chunk number {index} ({code}), buffer:\n[{mask}]",
                );
            } else {
                tracing::debug!("Dropping duplicate response for chunk number {index}");
            }
        }

        let result = self.buffer.pop_front()?.unwrap();
        self.trial = false;
        if let Ok(bytes) = &result {
            tracing::debug!("Sending {} response bytes", bytes.len());
        }
        Some(result)
    }

    // Schedules downloading `self.next_chunk`
    fn schedule_next_chunk(&mut self) {
        let chunk = self.next_chunk.clone().expect("No next chunk to download");
        let index: usize = self.next_index;
        let future = self
            .get_entire_chunk(index, chunk.clone())
            .map(move |result| (result, index));
        self.downloads.spawn(future);
        self.next_chunk = self.network.next_chunk(&self.request.dataset_id, &chunk);
        if let Some(next_chunk) = &self.next_chunk {
            if self
                .request
                .query
                .last_block()
                .is_some_and(|last_block| last_block < next_chunk.first_block() as u64)
            {
                tracing::debug!("The end of the requested range reached");
                self.next_chunk = None;
            }
        } else {
            tracing::debug!("No more chunks available");
        }
        self.next_index += 1;
        self.buffer.push_back(None);
    }

    fn get_entire_chunk(
        &self,
        index: usize,
        chunk: DataChunk,
    ) -> impl Future<Output = Result<ResponseChunk, RequestError>> {
        let network = self.network.clone();
        let timeouts = self.timeouts.clone();
        let full_range = Range {
            begin: std::cmp::max(chunk.first_block(), self.request.query.first_block() as u32),
            end: if let Some(last_block) = self.request.query.last_block() {
                std::cmp::min(chunk.last_block(), last_block as u32)
            } else {
                chunk.last_block()
            },
        };
        let mut current_range = full_range;
        let request = self.request.clone();
        async move {
            let mut accum_response = Vec::new();
            loop {
                let fut = Self::get_one_response(
                    request.clone(),
                    network.clone(),
                    timeouts.clone(),
                    index,
                    current_range,
                );
                let result = match fut.await {
                    Err(e) => return Err(e),
                    Ok(query_result::Result::Ok(result)) => result,
                    Ok(e) => return Err(RequestError::try_from(e).unwrap()),
                };
                let Some(last_block) = result.last_block else {
                    return Err(RequestError::InternalError(
                        "No last block in response".to_string(),
                    ));
                };
                accum_response.extend(result.data);
                if last_block == full_range.end as u64 {
                    return Ok(accum_response);
                } else if last_block < current_range.begin as u64 {
                    tracing::warn!(
                        "Got empty response for range {}-{}",
                        current_range.begin,
                        current_range.end
                    );
                    return Err(RequestError::InternalError(
                        "Got empty response".to_string(),
                    ));
                } else {
                    tracing::debug!(
                        "Got response for chunk {} with blocks {}-{}. {}/{} blocks fetched from this chunk. Current response length: {}",
                        index,
                        current_range.begin,
                        last_block,
                        last_block - full_range.begin as u64 + 1,
                        full_range.end - full_range.begin + 1,
                        accum_response.len()
                    );
                    current_range.begin = (last_block + 1) as u32;
                }
            }
        }
    }

    fn get_one_response(
        request: ClientRequest,
        network: Arc<NetworkClient>,
        timeouts: Arc<TimeoutManager>,
        index: usize,
        range: Range,
    ) -> impl Future<Output = Result<query_result::Result, RequestError>> {
        // TODO: don't modify the query
        let send_query = move || {
            let start_time = tokio::time::Instant::now();
            Self::query_some_worker(
                network.clone(),
                request.query.with_range(&range),
                request.dataset_id.clone(),
                index,
                range,
            )
            .map(move |result| (result, start_time))
        };
        async move {
            let timeouts = timeouts.clone();
            let mut requests = JoinSet::new();
            let mut tries_left = request.retries;
            for _ in 0..request.request_multiplier {
                requests.spawn(send_query());
                tries_left -= 1;
            }
            loop {
                let (send_result, start_time) = tokio::select! {
                    _ = timeouts.sleep(), if tries_left > 0 => {
                        tracing::debug!("Request for chunk {index} is running for too long, sending one more query");
                        requests.spawn(send_query());
                        tries_left -= 1;
                        continue;
                    }
                    result = requests.join_next() => {
                        result
                            .expect("No downloads left for chunk")
                            .expect("Awaiting download panicked")
                    }
                };
                let result = match send_result {
                    Ok(result) => result,
                    Err(e) => {
                        if tries_left == 0 {
                            return Err(e);
                        }
                        tries_left -= 1;
                        tracing::debug!(
                            "Couldn't schedule query ({e:?}), retrying in {:?}",
                            request.backoff
                        );
                        let backoff = request.backoff;
                        let send_query = send_query.clone();
                        requests.spawn(async move {
                            tokio::time::sleep(backoff).await;
                            send_query().await
                        });
                        continue;
                    }
                };
                match result {
                    query_result::Result::Ok(_) => {
                        let elapsed = start_time.elapsed();
                        timeouts.complete_ok(elapsed);
                        tracing::debug!("Query for chunk {index} completed in {elapsed:?}");
                    }
                    query_result::Result::Timeout(_) => {
                        timeouts.complete_timeout();
                    }
                    _ => {
                        timeouts.complete_err();
                    }
                };
                if retriable(&result) {
                    if tries_left == 0 {
                        tracing::info!("Giving up on query for chunk number {index}, {result:?}");
                        return Ok(result);
                    }
                    tries_left -= 1;
                    tracing::info!("Retrying query for chunk number {index}, {result:?}");
                    requests.spawn(send_query());
                    continue;
                } else {
                    return Ok(result);
                }
            }
        }
    }

    async fn query_some_worker(
        network: Arc<NetworkClient>,
        query: String,
        dataset_id: DatasetId,
        index: usize,
        range: Range,
    ) -> Result<query_result::Result, RequestError> {
        let worker = network.find_worker(&dataset_id, range.begin);
        let Some(worker) = worker else {
            let msg = format!(
                "No workers available for block {} in dataset {}",
                range.begin, dataset_id
            );
            tracing::warn!(msg);
            return Err(RequestError::NotFound(msg));
        };
        tracing::debug!(
            "Sending query for chunk {} ({}-{}) to worker {}",
            index,
            range.begin,
            range.end,
            worker,
        );
        let receiver = network
            .query_worker(&worker, &dataset_id, query)
            .map_err(|_| RequestError::Busy)?;
        Ok(receiver.await.expect("Sending query result failed"))
    }
}

impl Drop for StreamController {
    fn drop(&mut self) {
        tracing::info!("Finished stream {}", self.stream_id);
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

fn short_code<T>(result: &Result<T, RequestError>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(e) => e.short_code(),
    }
}
