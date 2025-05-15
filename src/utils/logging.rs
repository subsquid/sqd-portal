use axum::{extract::Request, response::IntoResponse};
use tokio::time::{Duration, Instant};
use tracing::Instrument;

use crate::{
    metrics,
    types::{ClientRequest, RequestId},
};

const LOG_INTERVAL: Duration = Duration::from_secs(5);

pub struct StreamStats {
    pub queries_sent: u64,
    pub chunks_downloaded: u64,
    pub response_blocks: u64,
    pub response_bytes: u64,
    pub start_time: Instant,
    pub last_log: Instant,
    pub throttled_for: Duration,
}

impl StreamStats {
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            queries_sent: 0,
            chunks_downloaded: 0,
            response_blocks: 0,
            response_bytes: 0,
            start_time: now,
            last_log: now,
            throttled_for: Duration::from_secs(0),
        }
    }

    pub fn query_sent(&mut self) {
        self.queries_sent += 1;
    }

    pub fn sent_response_chunk(&mut self, blocks: u64, bytes: usize) {
        self.chunks_downloaded += 1;
        self.response_blocks += blocks;
        self.response_bytes += bytes as u64;
    }

    pub fn throttled(&mut self, duration: Duration) {
        self.throttled_for += duration;
    }

    pub fn maybe_write_log(&mut self) {
        if self.last_log.elapsed() >= LOG_INTERVAL {
            tracing::info!(
                queries_sent = self.queries_sent,
                chunks_downloaded = self.chunks_downloaded,
                blocks_streamed = self.response_blocks,
                bytes_streamed = self.response_bytes,
                "Streaming..."
            );
            self.last_log = Instant::now();
        }
    }

    pub fn write_summary(&self, request: &ClientRequest, error: Option<String>) {
        // tracing::debug!(
        //     dataset = %request.dataset_id,
        //     query = request.query.to_string(),
        //     "Query processed"
        // );
        tracing::info!(
            dataset = %request.dataset_id,
            first_block = request.query.first_block(),
            last_block = request.query.last_block(),
            queries_sent = self.queries_sent,
            chunks_downloaded = self.chunks_downloaded,
            blocks_streamed = self.response_blocks,
            bytes_streamed = self.response_bytes,
            total_time = ?self.start_time.elapsed(),
            throttled_for = ?self.throttled_for,
            error = error.unwrap_or_else(|| "-".to_string()),
            "Stream finished"
        );
        metrics::report_stream_completed(self, &request.dataset_id, Some(&request.dataset_name));
    }
}

pub async fn middleware(mut req: Request, next: axum::middleware::Next) -> impl IntoResponse {
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let version = req.version();
    let start = Instant::now();
    let request_id = req
        .headers()
        .get("x-request-id")
        .map(|v| v.to_str().unwrap_or_default().to_owned())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    let span = tracing::span!(tracing::Level::INFO, "http_request", request_id);

    req.extensions_mut().insert(RequestId(request_id));
    let response = next.run(req).instrument(span.clone()).await;

    let latency = start.elapsed();
    span.in_scope(|| {
        tracing::info!(
            target: "http_request",
            method,
            path,
            ?version,
            status = %response.status(),
            ?latency,
            "HTTP request processed"
        );
    });

    let endpoint = {
        if path.contains("/query") {
            "/query".to_string()
        } else {
            let path = path.split('/').last().expect("HTTP Path can't be empty");
            format!("/{path}")
        }
    };
    metrics::report_http_response(endpoint, response.status(), latency.as_secs_f64());

    response
}
