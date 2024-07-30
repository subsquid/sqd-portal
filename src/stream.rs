use std::{future::Future, sync::Arc};

use futures::FutureExt;
use subsquid_messages::{data_chunk::DataChunk, query_result};
use tokio::task::JoinSet;
use tracing::instrument;

use crate::{
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
        let mut streamer = Self {
            request,
            stream_id,
            network,
            next_index: 0,
            next_chunk: Some(first_chunk),
            downloads: Default::default(),
            buffer,
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
        // TODO: simplify this function
        let network = self.network.clone();
        let query = self.request.query.with_set_chunk(&chunk);
        let dataset_id = self.request.dataset_id.clone();
        let chunk_timeout = self.request.chunk_timeout;
        let retries = self.request.retries;
        let backoff = self.request.backoff;
        let request_multiplier = self.request.request_multiplier;
        async move {
            let mut requests = JoinSet::new();
            let mut tries_left = retries;
            for _ in 0..request_multiplier {
                requests.spawn(Self::query_once(
                    network.clone(),
                    query.clone(),
                    dataset_id.clone(),
                    index,
                    chunk.clone(),
                ));
                tries_left -= 1;
            }
            loop {
                let send_result = tokio::select! {
                    _ = tokio::time::sleep(chunk_timeout) => {
                        if tries_left > 0 {
                            tracing::debug!("Timeout for chunk {index}, sending one more query");
                            requests.spawn(Self::query_once(
                                network.clone(),
                                query.clone(),
                                dataset_id.clone(),
                                index,
                                chunk.clone(),
                            ));
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
                        let network = network.clone();
                        let query = query.clone();
                        let dataset_id = dataset_id.clone();
                        let chunk = chunk.clone();
                        requests.spawn(async move {
                            tokio::time::sleep(backoff).await;
                            Self::query_once(network, query, dataset_id, index, chunk).await
                        });
                        continue;
                    }
                };
                if retriable(&result) {
                    if tries_left == 0 {
                        return Err(RequestError::try_from(result).unwrap());
                    }
                    tries_left -= 1;
                    tracing::info!("Retrying query for chunk number {index}, {result:?}");
                    requests.spawn(Self::query_once(
                        network.clone(),
                        query.clone(),
                        dataset_id.clone(),
                        index,
                        chunk.clone(),
                    ));
                    continue;
                } else {
                    return match result {
                        query_result::Result::Ok(result) => Ok(result.data),
                        _ => Err(RequestError::try_from(result).unwrap()),
                    };
                }
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

fn retriable(result: &query_result::Result) -> bool {
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
