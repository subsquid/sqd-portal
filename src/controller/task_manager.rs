use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use async_stream::stream;
use futures::{Stream, StreamExt};
use tracing_futures::Instrument;

use crate::{
    metrics,
    network::NetworkClient,
    types::{RequestError, ResponseChunk, StreamRequest},
};

use super::stream::StreamController;

/// Tracks all existing streams
pub struct TaskManager {
    network_client: Arc<NetworkClient>,
    running_tasks: AtomicUsize,
    max_tasks: usize,
    next_stream_index: AtomicU64,
    bandwidth_utilization_threshold: f64,
    priority_stride: u64,
}

impl TaskManager {
    pub fn new(
        network_client: Arc<NetworkClient>,
        max_parallel_streams: usize,
        bandwidth_utilization_threshold: f64,
        priority_stride: u64,
    ) -> TaskManager {
        TaskManager {
            network_client,
            running_tasks: 0.into(),
            max_tasks: max_parallel_streams,
            next_stream_index: AtomicU64::new(0),
            bandwidth_utilization_threshold,
            priority_stride,
        }
    }

    pub async fn spawn_stream(
        self: Arc<Self>,
        request: StreamRequest,
    ) -> Result<impl Stream<Item = ResponseChunk>, RequestError> {
        let running_tasks = self.running_tasks.fetch_add(1, Ordering::Relaxed);
        if running_tasks >= self.max_tasks {
            self.running_tasks.fetch_sub(1, Ordering::Relaxed);
            return Err(RequestError::Unavailable);
        }

        if let Some(util) = self.network_client.download_utilization() {
            if util > self.bandwidth_utilization_threshold {
                self.running_tasks.fetch_sub(1, Ordering::Relaxed);
                return Err(RequestError::BusyFor(Duration::from_secs(1)));
            }
        }

        metrics::ACTIVE_STREAMS.set(running_tasks as i64 + 1);

        let self_clone = self.clone();
        let guard = scopeguard::guard((), move |()| {
            let prev = self_clone.running_tasks.fetch_sub(1, Ordering::Relaxed);
            metrics::ACTIVE_STREAMS.set(prev as i64 - 1);
            metrics::COMPLETED_STREAMS.inc();
        });

        let stream_index = self.next_stream_index.fetch_add(1, Ordering::Relaxed);
        let mut streamer = StreamController::new(
            request,
            self.network_client.clone(),
            stream_index,
            self.priority_stride,
        )?;
        let first_chunk = streamer
            .next()
            .instrument(tracing::debug_span!("stream_next"))
            .await
            .expect("First chunk missing from the stream")?;
        Ok(stream! {
            let _ = guard;
            yield first_chunk;
            loop {
                match streamer.next().instrument(tracing::debug_span!("stream_next")).await {
                    None => break,
                    Some(Ok(chunk)) => yield chunk,
                    Some(Err(e)) => {
                        tracing::warn!("Stream got interrupted: {}", e);
                        // There is no way to pass the error to the client
                        break;
                    }
                }
            }
        }
        .in_current_span())
    }
}
