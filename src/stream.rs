use std::sync::Arc;

use subsquid_messages::{data_chunk::DataChunk, query_result};
use tokio::task::JoinSet;
use tracing::instrument;

use crate::{
    network::NetworkClient,
    types::{ClientRequest, RequestError, ResponseChunk},
    utils::SlidingArray,
};

const DUPLICATE_REQUESTS: usize = 1;

pub struct StreamController {
    request: ClientRequest,
    stream_id: usize,
    buffer_size: usize,
    network: Arc<NetworkClient>,
    next_index: usize,
    next_chunk: Option<DataChunk>,
    downloads: JoinSet<(query_result::Result, DataChunk, usize)>,
    buffer: SlidingArray<Option<Result<ResponseChunk, RequestError>>>,
    // The first request is sent out to probe it's validity.
    // Only after that multiple requests are sent in parallel.
    trial: bool,
}

impl StreamController {
    pub fn spawn(
        request: ClientRequest,
        stream_id: usize,
        buffer_size: usize,
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

        let mut streamer = Self {
            request,
            stream_id,
            buffer_size,
            network,
            next_index: 0,
            next_chunk: Some(first_chunk),
            downloads: Default::default(),
            buffer: SlidingArray::with_capacity(buffer_size),
            trial: true,
        };

        // Intentionally schedule only one download not to waste resources in case of bad request
        streamer.schedule_next_chunk()?;

        Ok(streamer)
    }

    #[instrument(skip_all, fields(stream_id = self.stream_id))]
    pub async fn poll_next_chunk(&mut self) -> Option<Result<ResponseChunk, RequestError>> {
        while self.buffer.len() < self.buffer_size && self.next_chunk.is_some() && !self.trial {
            let result = self.schedule_next_chunk();
            tokio::task::yield_now().await;
            if let Err(e) = result {
                tracing::debug!("Couldn't schedule next chunk: {e}");
                break;
            }
        }

        while !self.buffer.front().is_some_and(Option::is_some) {
            let (result, chunk, index) = self
                .downloads
                .join_next()
                .await?
                .expect("Awaiting download panicked");
            if retriable(&result) {
                if self.buffer.get(index).is_some_and(Option::is_none) {
                    tracing::info!("Retrying request for chunk number {index}, {result:?}");
                    // TODO: stop streaming if no download is scheduled for the chunk
                    self.schedule_download(index, chunk).ok();
                }
                continue;
            }
            let result = if let query_result::Result::Ok(result) = result {
                Ok(result.data)
            } else {
                Err(result.try_into().unwrap())
            };
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
                    "Got response for chunk number {} ({}), buffer:\n[{}]",
                    index,
                    code,
                    mask
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
        for _ in 0..DUPLICATE_REQUESTS {
            self.schedule_download(self.next_index, chunk.clone())?;
        }
        self.next_chunk = self.network.next_chunk(&self.request.dataset_id, &chunk);
        if self.next_chunk.is_none() {
            tracing::debug!("No more chunks available");
        }
        self.next_index += 1;
        self.buffer.push_back(None);
        Ok(())
    }

    fn schedule_download(&mut self, index: usize, chunk: DataChunk) -> Result<(), RequestError> {
        let worker = self.network.find_worker(&self.request.dataset_id, &chunk);
        let Some(worker) = worker else {
            let msg = format!(
                "No workers available for chunk {}/{:?}",
                self.request.dataset_id, chunk
            );
            tracing::warn!(msg);
            return Err(RequestError::NotFound(msg));
        };
        tracing::debug!("Sending query for chunk {} to worker {}", index, worker);
        let receiver = self
            .network
            .query_worker(
                &worker,
                &self.request.dataset_id,
                self.request.query.with_set_chunk(&chunk),
            )
            .map_err(|_| RequestError::Busy)?;
        self.downloads.spawn(async move {
            let result = receiver.await.expect("Sending query result failed");
            (result, chunk, index)
        });
        Ok(())
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
