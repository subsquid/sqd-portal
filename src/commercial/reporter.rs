use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

use reqwest::StatusCode;
use serde::Serialize;
use tokio::{sync::Notify, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use super::{config::CommercialConfig, types::StreamUsageEvent};
use crate::metrics;

const MAX_RETRY_ATTEMPTS: usize = 5;

pub trait UsageReporter: Send + Sync {
    fn report(&self, event: StreamUsageEvent);
}

#[derive(Debug, Default)]
pub struct NoopUsageReporter;

impl UsageReporter for NoopUsageReporter {
    fn report(&self, _event: StreamUsageEvent) {}
}

#[derive(Clone)]
struct ReporterOptions {
    control_plane_url: url::Url,
    service_token: String,
    flush_interval: Duration,
    flush_max_events: usize,
    capacity: usize,
}

pub struct BufferedUsageReporter {
    inner: Arc<BufferedUsageReporterInner>,
}

struct BufferedUsageReporterInner {
    options: ReporterOptions,
    queue: Mutex<VecDeque<StreamUsageEvent>>,
    notify: Notify,
    client: reqwest::Client,
}

#[derive(Serialize)]
struct UsageBatch<'a> {
    events: &'a [StreamUsageEvent],
}

impl BufferedUsageReporter {
    pub fn spawn(
        config: &CommercialConfig,
        cancel: CancellationToken,
    ) -> (Arc<Self>, JoinHandle<()>) {
        let service_token = std::env::var(&config.service_token_env).unwrap_or_default();
        let reporter = Self::new(ReporterOptions {
            control_plane_url: config.control_plane_url.clone(),
            service_token,
            flush_interval: Duration::from_secs(config.flush_interval_secs),
            flush_max_events: config.flush_max_events.max(1),
            capacity: config.usage_buffer_max_events.max(1),
        });
        let task_reporter = reporter.clone();
        let task = tokio::spawn(async move {
            task_reporter.run(cancel).await;
        });

        (reporter, task)
    }

    fn new(options: ReporterOptions) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(BufferedUsageReporterInner {
                options,
                queue: Mutex::new(VecDeque::new()),
                notify: Notify::new(),
                client: reqwest::Client::builder()
                    .timeout(Duration::from_secs(10))
                    .build()
                    .expect("reqwest client should build"),
            }),
        })
    }

    async fn run(self: Arc<Self>, cancel: CancellationToken) {
        let mut tick = Box::pin(tokio::time::sleep(self.inner.options.flush_interval));

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    let _ = tokio::time::timeout(Duration::from_secs(5), self.flush_remaining_once()).await;
                    return;
                }
                _ = self.inner.notify.notified() => {
                    if self.queue_len() >= self.inner.options.flush_max_events {
                        self.flush_one_batch_with_retry(&cancel).await;
                    }
                }
                _ = &mut tick => {
                    self.flush_one_batch_with_retry(&cancel).await;
                    tick.as_mut().reset(tokio::time::Instant::now() + self.inner.options.flush_interval);
                }
            }
        }
    }

    async fn flush_remaining_once(&self) {
        while let Some(batch) = self.pop_batch() {
            if let Err(err) = self.send_once(&batch).await {
                tracing::warn!(error = %err, count = batch.len(), "final commercial usage flush failed");
                return;
            }
        }
    }

    async fn flush_one_batch_with_retry(&self, cancel: &CancellationToken) {
        let Some(batch) = self.pop_batch() else {
            return;
        };

        if let Some(batch) = self.send_with_retry(batch, cancel).await {
            self.push_front_batch(batch);
        }
    }

    async fn send_with_retry(
        &self,
        batch: Vec<StreamUsageEvent>,
        cancel: &CancellationToken,
    ) -> Option<Vec<StreamUsageEvent>> {
        let mut delay = Duration::from_secs(1);
        let mut attempts = 0usize;
        loop {
            attempts += 1;
            let result = tokio::select! {
                _ = cancel.cancelled() => return Some(batch),
                result = self.send_once(&batch) => result,
            };
            match result {
                Ok(()) => return None,
                Err(SendBatchError::Drop(status)) => {
                    metrics::report_commercial_usage_dropped();
                    tracing::error!(
                        status = status.as_u16(),
                        count = batch.len(),
                        "dropping commercial usage batch after control-plane 4xx"
                    );
                    return None;
                }
                Err(SendBatchError::Retry(err)) => {
                    if attempts >= MAX_RETRY_ATTEMPTS {
                        metrics::report_commercial_usage_dropped();
                        tracing::error!(
                            error = %err,
                            attempts,
                            count = batch.len(),
                            "dropping commercial usage batch after retry cap"
                        );
                        return None;
                    }
                    tracing::warn!(
                        error = %err,
                        delay = ?delay,
                        count = batch.len(),
                        "commercial usage batch send failed; retrying"
                    );
                    tokio::select! {
                        _ = cancel.cancelled() => return Some(batch),
                        _ = tokio::time::sleep(delay) => {}
                    }
                    delay = (delay * 2).min(Duration::from_secs(30));
                }
            }
        }
    }

    async fn send_once(&self, batch: &[StreamUsageEvent]) -> Result<(), SendBatchError> {
        let url = self
            .inner
            .options
            .control_plane_url
            .join("/internal/portal/v1/usage")
            .map_err(|err| SendBatchError::Retry(err.to_string()))?;
        let response = self
            .inner
            .client
            .post(url)
            .bearer_auth(&self.inner.options.service_token)
            .json(&UsageBatch { events: batch })
            .send()
            .await
            .map_err(|err| SendBatchError::Retry(err.to_string()))?;
        let status = response.status();
        if status.is_success() {
            Ok(())
        } else if status.is_client_error() {
            Err(SendBatchError::Drop(status))
        } else {
            Err(SendBatchError::Retry(format!(
                "control plane status {status}"
            )))
        }
    }

    fn queue_len(&self) -> usize {
        self.inner.queue.lock().unwrap().len()
    }

    fn pop_batch(&self) -> Option<Vec<StreamUsageEvent>> {
        let mut queue = self.inner.queue.lock().unwrap();
        if queue.is_empty() {
            return None;
        }

        let count = queue.len().min(self.inner.options.flush_max_events);
        Some(queue.drain(..count).collect())
    }

    fn push_front_batch(&self, mut batch: Vec<StreamUsageEvent>) {
        let mut queue = self.inner.queue.lock().unwrap();
        while let Some(event) = batch.pop() {
            queue.push_front(event);
        }
        while queue.len() > self.inner.options.capacity {
            queue.pop_back();
            metrics::report_commercial_usage_dropped();
        }
    }
}

impl UsageReporter for BufferedUsageReporter {
    fn report(&self, event: StreamUsageEvent) {
        let mut queue = self.inner.queue.lock().unwrap();
        if queue.len() >= self.inner.options.capacity {
            queue.pop_front();
            metrics::report_commercial_usage_dropped();
        }
        queue.push_back(event);
        drop(queue);
        self.inner.notify.notify_one();
    }
}

#[derive(Debug)]
enum SendBatchError {
    Retry(String),
    Drop(StatusCode),
}

impl std::fmt::Display for SendBatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Retry(err) => f.write_str(err),
            Self::Drop(status) => write!(f, "control plane returned {status}"),
        }
    }
}

impl std::error::Error for SendBatchError {}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::{routing::post, Json, Router};
    use tokio::sync::mpsc;

    use super::*;
    use crate::commercial::types::{DataSource, Endpoint, UsageStatus};

    fn event(id: &str) -> StreamUsageEvent {
        StreamUsageEvent {
            event_id: id.to_string(),
            request_id: "request".to_string(),
            account_id: "account".to_string(),
            api_key_id: Some("key".to_string()),
            dataset: "ethereum-mainnet".to_string(),
            endpoint: Endpoint::Stream,
            data_source: DataSource::Network,
            logical_bytes: 1,
            wire_bytes: 1,
            blocks: 0,
            chunks: 1,
            started_at: 0.0,
            duration_ms: 0,
            status: UsageStatus::Completed,
            pod: "pod".to_string(),
            quota_version: 0,
        }
    }

    async fn mock_server(
        statuses: Vec<StatusCode>,
    ) -> (url::Url, mpsc::Receiver<Vec<StreamUsageEvent>>) {
        let (tx, rx) = mpsc::channel(8);
        let attempts = Arc::new(AtomicUsize::new(0));
        let statuses = Arc::new(statuses);
        let app = Router::new().route(
            "/internal/portal/v1/usage",
            post({
                let attempts = attempts.clone();
                let statuses = statuses.clone();
                move |Json(batch): Json<serde_json::Value>| {
                    let tx = tx.clone();
                    let statuses = statuses.clone();
                    let attempts = attempts.clone();
                    async move {
                        let events: Vec<StreamUsageEvent> =
                            serde_json::from_value(batch["events"].clone()).unwrap();
                        let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                        let status = statuses
                            .get(attempt)
                            .copied()
                            .unwrap_or(StatusCode::ACCEPTED);
                        if status.is_success() {
                            tx.send(events).await.unwrap();
                        }
                        status
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        (format!("http://{addr}").parse().unwrap(), rx)
    }

    fn reporter(
        url: url::Url,
        flush_interval: Duration,
        flush_max_events: usize,
        capacity: usize,
    ) -> Arc<BufferedUsageReporter> {
        BufferedUsageReporter::new(ReporterOptions {
            control_plane_url: url,
            service_token: "service-token".to_string(),
            flush_interval,
            flush_max_events,
            capacity,
        })
    }

    #[tokio::test]
    async fn flushes_by_size() {
        let (url, mut rx) = mock_server(vec![]).await;
        let reporter = reporter(url, Duration::from_secs(60), 2, 10);
        let cancel = CancellationToken::new();
        tokio::spawn(reporter.clone().run(cancel));

        reporter.report(event("one"));
        reporter.report(event("two"));

        let batch = rx.recv().await.unwrap();
        assert_eq!(
            batch
                .iter()
                .map(|event| event.event_id.as_str())
                .collect::<Vec<_>>(),
            ["one", "two"]
        );
    }

    #[tokio::test]
    async fn flushes_by_tick() {
        let (url, mut rx) = mock_server(vec![]).await;
        let reporter = reporter(url, Duration::from_millis(50), 10, 10);
        let cancel = CancellationToken::new();
        tokio::spawn(reporter.clone().run(cancel));

        reporter.report(event("one"));
        tokio::time::sleep(Duration::from_millis(100)).await;

        let batch = rx.recv().await.unwrap();
        assert_eq!(batch[0].event_id, "one");
    }

    #[test]
    fn drops_oldest_on_overflow() {
        let reporter = reporter(
            "http://127.0.0.1:1".parse().unwrap(),
            Duration::from_secs(60),
            10,
            2,
        );

        reporter.report(event("one"));
        reporter.report(event("two"));
        reporter.report(event("three"));

        let batch = reporter.pop_batch().unwrap();
        assert_eq!(
            batch
                .iter()
                .map(|event| event.event_id.as_str())
                .collect::<Vec<_>>(),
            ["two", "three"]
        );
    }

    #[tokio::test]
    async fn retries_then_delivers_batch_once() {
        let (url, mut rx) = mock_server(vec![
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::ACCEPTED,
        ])
        .await;
        let reporter = reporter(url, Duration::from_secs(60), 1, 10);
        let cancel = CancellationToken::new();
        tokio::spawn(reporter.clone().run(cancel));

        reporter.report(event("retry-me"));
        tokio::time::sleep(Duration::from_secs(1)).await;

        let batch = rx.recv().await.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].event_id, "retry-me");
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn cancel_during_retry_storm_exits_promptly() {
        let (url, _rx) = mock_server(vec![StatusCode::INTERNAL_SERVER_ERROR; 16]).await;
        let reporter = reporter(url, Duration::from_secs(60), 1, 10);
        let cancel = CancellationToken::new();
        let task = tokio::spawn(reporter.clone().run(cancel.clone()));

        reporter.report(event("retry-storm"));
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();

        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("reporter exits promptly")
            .expect("reporter task should not panic");
    }

    #[tokio::test]
    async fn final_flush_on_shutdown_attempts_buffered_events() {
        let (url, mut rx) = mock_server(vec![]).await;
        let reporter = reporter(url, Duration::from_secs(60), 10, 10);
        let cancel = CancellationToken::new();
        let task = tokio::spawn(reporter.clone().run(cancel.clone()));

        reporter.report(event("shutdown"));
        cancel.cancel();

        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("shutdown returns after final flush")
            .expect("reporter task should not panic");
        let batch = rx
            .try_recv()
            .expect("final flush delivered before shutdown");
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].event_id, "shutdown");
    }

    #[test]
    fn noop_reporter_drops_events() {
        let reporter = NoopUsageReporter;
        reporter.report(event("noop"));
    }
}
