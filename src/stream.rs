use std::{future::Future, sync::Arc};

use futures::FutureExt;
use subsquid_messages::{data_chunk::DataChunk, query_result};
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
        streamer.schedule_next_chunk()?;

        Ok(streamer)
    }

    #[instrument(skip_all, fields(stream_id = self.stream_id))]
    pub async fn poll_next_chunk(&mut self) -> Option<Result<ResponseChunk, RequestError>> {
        while self.buffer.len() < self.request.buffer_size
            && self.next_chunk.is_some()
            && !self.trial
        {
            let result = self.schedule_next_chunk();
            tokio::task::yield_now().await;
            if let Err(e) = result {
                tracing::debug!("Couldn't schedule next chunk: {e}");
                break;
            }
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
    fn schedule_next_chunk(&mut self) -> Result<(), RequestError> {
        let chunk = self.next_chunk.clone().expect("No next chunk to download");
        let index: usize = self.next_index;
        let future = self
            .query_chunk_with_retries(index, chunk.clone())
            .map(move |result| (result, index));
        self.downloads.spawn(future);
        self.next_chunk = self.network.next_chunk(&self.request.dataset_id, &chunk);
        if self.next_chunk.is_none() {
            tracing::debug!("No more chunks available");
        }
        self.next_index += 1;
        self.buffer.push_back(None);
        Ok(())
    }

    fn query_chunk_with_retries(
        &mut self,
        index: usize,
        chunk: DataChunk,
    ) -> impl Future<Output = Result<ResponseChunk, RequestError>> {
        let network = self.network.clone();
        let timeouts = self.timeouts.clone();
        let query = self.request.query.with_set_chunk(&chunk);
        let dataset_id = self.request.dataset_id.clone();
        let retries = self.request.retries;
        let backoff = self.request.backoff;
        let request_multiplier = self.request.request_multiplier;
        let send_query = move || {
            let start_time = tokio::time::Instant::now();
            Self::query_once(
                network.clone(),
                query.clone(),
                dataset_id.clone(),
                index,
                chunk.clone(),
            )
            .map(move |result| (result, start_time))
        };
        async move {
            let timeouts = timeouts.clone();
            let mut requests = JoinSet::new();
            let mut tries_left = retries;
            for _ in 0..request_multiplier {
                requests.spawn(send_query());
                tries_left -= 1;
            }
            loop {
                let (send_result, start_time) = tokio::select! {
                    _ = timeouts.sleep() => {
                        if tries_left > 0 {
                            tracing::debug!("Request for chunk {index} is running for too long, sending one more query");
                            requests.spawn(send_query());
                            tries_left -= 1;
                        } else {
                            return Err(RequestError::InternalError(format!("No response in {retries} tries")));
                        }
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
                        tracing::debug!("Couldn't schedule query ({e:?}), retrying in {backoff:?}");
                        let fut = send_query();
                        requests.spawn(async move {
                            tokio::time::sleep(backoff).await;
                            fut.await
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
                match result {
                    query_result::Result::ServerError(_)
                    | query_result::Result::Timeout(_)
                    | query_result::Result::NoAllocation(_) => {
                        if tries_left == 0 {
                            tracing::info!(
                                "Giving up on query for chunk number {index}, {result:?}"
                            );
                            return Err(RequestError::try_from(result).unwrap());
                        }
                        tries_left -= 1;
                        tracing::info!("Retrying query for chunk number {index}, {result:?}");
                        requests.spawn(send_query());
                        continue;
                    }
                    query_result::Result::Ok(result) => return Ok(result.data),
                    query_result::Result::BadRequest(_) => {
                        return Err(RequestError::try_from(result).unwrap())
                    }
                };
            }
        }
    }

    async fn query_once(
        network: Arc<NetworkClient>,
        query: String,
        dataset_id: DatasetId,
        index: usize,
        chunk: DataChunk,
    ) -> Result<query_result::Result, RequestError> {
        let worker = network.find_worker(&dataset_id, &chunk);
        let Some(worker) = worker else {
            let msg = format!("No workers available for chunk {}/{:?}", dataset_id, chunk);
            tracing::warn!(msg);
            return Err(RequestError::NotFound(msg));
        };
        tracing::debug!("Sending query for chunk {} to worker {}", index, worker);
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

fn _retriable(result: &query_result::Result) -> bool {
    use query_result::Result;
    match result {
        Result::ServerError(_) | Result::Timeout(_) | Result::NoAllocation(_) => true,
        Result::Ok(_) | Result::BadRequest(_) => false,
    }
}

fn short_code<T>(result: &Result<T, RequestError>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(e) => e.short_code(),
    }
}
