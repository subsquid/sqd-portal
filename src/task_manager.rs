use std::sync::{
    atomic::{AtomicU8, AtomicUsize, Ordering},
    Arc,
};

use async_stream::stream;
use futures::Stream;

use crate::{
    network::NetworkClient,
    stream::StreamController,
    types::{ClientRequest, RequestError, ResponseChunk},
};

const MAX_PARALLEL_STREAMS: u8 = 5;

/// Tracks all existing streams
pub struct TaskManager {
    network_client: Arc<NetworkClient>,
    stream_buffer_size: usize,
    running_tasks: AtomicU8,
    next_stream_id: AtomicUsize,
}

impl TaskManager {
    pub fn new(network_client: Arc<NetworkClient>, stream_buffer_size: usize) -> TaskManager {
        TaskManager {
            network_client,
            stream_buffer_size,
            running_tasks: 0.into(),
            next_stream_id: 0.into(),
        }
    }

    pub async fn spawn_stream(
        self: Arc<Self>,
        request: ClientRequest,
    ) -> Result<impl Stream<Item = ResponseChunk>, RequestError> {
        if self.running_tasks.fetch_add(1, Ordering::Relaxed) >= MAX_PARALLEL_STREAMS {
            self.running_tasks.fetch_sub(1, Ordering::Relaxed);
            return Err(RequestError::Busy);
        }
        let self_clone = self.clone();
        let guard = scopeguard::guard((), move |_| {
            self_clone.running_tasks.fetch_sub(1, Ordering::Relaxed);
        });

        let streamer = StreamController::spawn(
            request,
            self.next_stream_id.fetch_add(1, Ordering::Relaxed),
            self.stream_buffer_size,
            self.network_client.clone(),
        );
        let mut streamer = streamer?;
        let first_chunk = streamer
            .poll_next_chunk()
            .await
            .expect("First chunk missing from the stream")?;
        Ok(stream! {
            let _ = guard;
            yield first_chunk;
            loop {
                match streamer.poll_next_chunk().await {
                    None => break,
                    Some(Ok(chunk)) => yield chunk,
                    Some(Err(e)) => {
                        tracing::warn!("Stream got interrupted: {:?}", e);
                        // There is no way to pass the error to the client
                        break;
                    }
                }
            }
        })
    }
}
