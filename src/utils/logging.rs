use std::sync::atomic::{AtomicU64, Ordering};

use axum::{extract::Request, response::IntoResponse};
use parking_lot::Mutex;
use tokio::time::{Duration, Instant};
use tracing::Instrument;

const LOG_INTERVAL: Duration = Duration::from_secs(5);

pub struct StreamStats {
    queries_sent: AtomicU64,
    chunks_downloaded: AtomicU64,
    response_bytes: AtomicU64,
    start_time: Instant,
    last_log: Mutex<Instant>,
}

impl StreamStats {
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            queries_sent: Default::default(),
            chunks_downloaded: Default::default(),
            response_bytes: Default::default(),
            start_time: now,
            last_log: Mutex::new(now),
        }
    }

    pub fn query_sent(&self) {
        self.queries_sent.fetch_add(1, Ordering::Relaxed);
    }

    pub fn sent_response_chunk(&self, size: usize) {
        self.chunks_downloaded.fetch_add(1, Ordering::Relaxed);
        self.response_bytes
            .fetch_add(size as u64, Ordering::Relaxed);
    }

    pub fn maybe_write_log(&self) {
        let mut last_log = self.last_log.lock();
        if last_log.elapsed() >= LOG_INTERVAL {
            tracing::info!(
                queries_sent = self.queries_sent.load(Ordering::Relaxed),
                chunks_downloaded = self.chunks_downloaded.load(Ordering::Relaxed),
                bytes_streamed = self.response_bytes.load(Ordering::Relaxed),
                "Streaming..."
            );
            *last_log = Instant::now();
        }
    }

    pub fn write_summary(&self) {
        tracing::info!(
            queries_sent = self.queries_sent.load(Ordering::Relaxed),
            chunks_downloaded = self.chunks_downloaded.load(Ordering::Relaxed),
            bytes_streamed = self.response_bytes.load(Ordering::Relaxed),
            total_time = ?self.start_time.elapsed(),
            "Stream finished"
        );
    }
}

pub async fn middleware(req: Request, next: axum::middleware::Next) -> impl IntoResponse {
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let version = req.version();
    let start = Instant::now();

    let span = tracing::span!(
        tracing::Level::INFO,
        "http_request",
        request_id = uuid::Uuid::new_v4().to_string()
    );

    let response = next.run(req).instrument(span.clone()).await;

    span.in_scope(|| {
        tracing::info!(
            target: "http_request",
            method,
            path,
            ?version,
            status = %response.status(),
            latency = ?start.elapsed(),
            "HTTP request processed"
        )
    });

    response
}
