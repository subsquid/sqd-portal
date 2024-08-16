use std::sync::{
    atomic::{AtomicU8, AtomicUsize, Ordering},
    Arc,
};

use async_stream::stream;
use futures::Stream;

use crate::{
    metrics,
    network::NetworkClient,
    types::{ClientRequest, RequestError, ResponseChunk},
};

use super::stream::StreamController;

const MAX_PARALLEL_STREAMS: u8 = 5;

/// Tracks all existing streams
pub struct TaskManager {
    network_client: Arc<NetworkClient>,
    running_tasks: AtomicU8,
    next_stream_id: AtomicUsize,
}

impl TaskManager {
    pub fn new(network_client: Arc<NetworkClient>) -> TaskManager {
        TaskManager {
            network_client,
            running_tasks: 0.into(),
            next_stream_id: 0.into(),
        }
    }

    pub async fn spawn_stream(
        self: Arc<Self>,
        request: ClientRequest,
    ) -> Result<impl Stream<Item = ResponseChunk>, RequestError> {
        let running_tasks = self.running_tasks.fetch_add(1, Ordering::Relaxed);
        if running_tasks >= MAX_PARALLEL_STREAMS {
            self.running_tasks.fetch_sub(1, Ordering::Relaxed);
            return Err(RequestError::Busy);
        }
        metrics::ACTIVE_STREAMS.set(running_tasks as i64 + 1);

        let self_clone = self.clone();
        let guard = scopeguard::guard((), move |_| {
            let prev = self_clone.running_tasks.fetch_sub(1, Ordering::Relaxed);
            metrics::ACTIVE_STREAMS.set(prev as i64 - 1);
            metrics::COMPLETED_STREAMS.inc();
        });

        let streamer = StreamController::new(
            request,
            self.next_stream_id.fetch_add(1, Ordering::Relaxed),
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
