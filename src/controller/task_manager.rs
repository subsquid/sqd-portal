use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use async_stream::stream;
use futures::{Stream, StreamExt};
use tracing_futures::Instrument;

use crate::{
    metrics,
    network::NetworkClient,
    types::{ClientRequest, RequestError, ResponseChunk},
};

use super::stream::StreamController;

/// Tracks all existing streams
pub struct TaskManager {
    network_client: Arc<NetworkClient>,
    running_tasks: AtomicUsize,
    max_tasks: usize,
}

impl TaskManager {
    pub fn new(network_client: Arc<NetworkClient>, max_parallel_streams: usize) -> TaskManager {
        TaskManager {
            network_client,
            running_tasks: 0.into(),
            max_tasks: max_parallel_streams,
        }
    }

    pub async fn spawn_stream(
        self: Arc<Self>,
        request: ClientRequest,
    ) -> Result<impl Stream<Item = ResponseChunk>, RequestError> {
        let running_tasks = self.running_tasks.fetch_add(1, Ordering::Relaxed);
        if running_tasks >= self.max_tasks {
            self.running_tasks.fetch_sub(1, Ordering::Relaxed);
            return Err(RequestError::Unavailable);
        }
        metrics::ACTIVE_STREAMS.set(running_tasks as i64 + 1);

        let self_clone = self.clone();
        let guard = scopeguard::guard((), move |()| {
            let prev = self_clone.running_tasks.fetch_sub(1, Ordering::Relaxed);
            metrics::ACTIVE_STREAMS.set(prev as i64 - 1);
            metrics::COMPLETED_STREAMS.inc();
        });

        let mut streamer = StreamController::new(request, self.network_client.clone())?;
        let first_chunk = streamer
            .next()
            .in_current_span()
            .await
            .expect("First chunk missing from the stream")?;
        Ok(stream! {
            let _ = guard;
            yield first_chunk;
            loop {
                match streamer.next().await {
                    None => break,
                    Some(Ok(chunk)) => yield chunk,
                    Some(Err(e)) => {
                        tracing::warn!("Stream got interrupted: {:?}", e);
                        // There is no way to pass the error to the client
                        break;
                    }
                }
            }
        }
        .in_current_span())
    }
}
