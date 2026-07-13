use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use reqwest::StatusCode;
use serde::Serialize;
use tokio::{sync::Notify, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use super::{config::CommercialConfig, types::StreamUsageEvent};
use crate::metrics;

const MAX_RETRY_ATTEMPTS: usize = 5;
const USAGE_PATH_SEGMENTS: [&str; 4] = ["internal", "portal", "v1", "usage"];
type SendOnceFuture<'a> = Pin<Box<dyn Future<Output = Result<(), SendBatchError>> + Send + 'a>>;

pub trait UsageReporter: Send + Sync {
    fn report(&self, event: StreamUsageEvent);

    fn flush_shutdown(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }
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

#[derive(Clone)]
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
                    return;
                }
                _ = self.inner.notify.notified() => {
                    if self.queue_len() >= self.inner.options.flush_max_events {
                        self.flush_threshold_backlog(&cancel).await;
                    }
                }
                _ = &mut tick => {
                    self.flush_one_batch_with_retry(&cancel).await;
                    tick.as_mut().reset(tokio::time::Instant::now() + self.inner.options.flush_interval);
                }
            }
        }
    }

    async fn flush_threshold_backlog(&self, cancel: &CancellationToken) {
        while self.queue_len() >= self.inner.options.flush_max_events {
            if !self.flush_one_batch_with_retry(cancel).await || cancel.is_cancelled() {
                return;
            }
        }
    }

    async fn flush_remaining_once(&self) {
        let reporter = self.clone();
        self.flush_remaining_once_using(move |batch| {
            let reporter = reporter.clone();
            let batch = batch.to_vec();
            let future: SendOnceFuture<'_> =
                Box::pin(async move { reporter.send_once(&batch).await });
            future
        })
        .await
    }

    async fn flush_remaining_once_using<F>(&self, mut send_once: F)
    where
        F: for<'a> FnMut(&'a [StreamUsageEvent]) -> SendOnceFuture<'a>,
    {
        while let Some(batch) = self.pop_batch() {
            if let Err(err) = send_once(&batch).await {
                tracing::warn!(error = %err, count = batch.len(), "final commercial usage flush failed");
                return;
            }
        }
    }

    async fn flush_one_batch_with_retry(&self, cancel: &CancellationToken) -> bool {
        let Some(batch) = self.pop_batch() else {
            return true;
        };

        if let Some(batch) = self.send_with_retry(batch, cancel).await {
            self.push_front_batch(batch);
            return false;
        }
        true
    }

    #[cfg(test)]
    async fn flush_threshold_backlog_using<F>(&self, cancel: &CancellationToken, mut send_once: F)
    where
        F: for<'a> FnMut(&'a [StreamUsageEvent]) -> SendOnceFuture<'a>,
    {
        while self.queue_len() >= self.inner.options.flush_max_events {
            let Some(batch) = self.pop_batch() else {
                return;
            };

            if let Some(batch) = self
                .send_with_retry_using(batch, cancel, &mut send_once)
                .await
            {
                self.push_front_batch(batch);
                return;
            }

            if cancel.is_cancelled() {
                return;
            }
        }
    }

    async fn send_with_retry(
        &self,
        batch: Vec<StreamUsageEvent>,
        cancel: &CancellationToken,
    ) -> Option<Vec<StreamUsageEvent>> {
        let reporter = self.clone();
        self.send_with_retry_using(batch, cancel, move |batch| {
            let reporter = reporter.clone();
            let batch = batch.to_vec();
            let future: SendOnceFuture<'_> =
                Box::pin(async move { reporter.send_once(&batch).await });
            future
        })
        .await
    }

    async fn send_with_retry_using<F>(
        &self,
        batch: Vec<StreamUsageEvent>,
        cancel: &CancellationToken,
        mut send_once: F,
    ) -> Option<Vec<StreamUsageEvent>>
    where
        F: for<'a> FnMut(&'a [StreamUsageEvent]) -> SendOnceFuture<'a>,
    {
        let mut delay = Duration::from_secs(1);
        let mut attempts = 0usize;
        loop {
            attempts += 1;
            let result = tokio::select! {
                _ = cancel.cancelled() => return Some(batch),
                result = send_once(&batch) => result,
            };
            match result {
                Ok(()) => return None,
                Err(SendBatchError::Drop(status)) => {
                    metrics::report_commercial_usage_dropped();
                    tracing::error!(
                        status = status.as_u16(),
                        count = batch.len(),
                        "dropping commercial usage batch after control-plane payload validation error"
                    );
                    return None;
                }
                Err(SendBatchError::Retry(err)) => {
                    if attempts >= MAX_RETRY_ATTEMPTS {
                        tracing::warn!(
                            error = %err,
                            attempts,
                            delay = ?delay,
                            count = batch.len(),
                            "commercial usage batch exhausted retry budget; requeueing"
                        );
                        tokio::select! {
                            _ = cancel.cancelled() => return Some(batch),
                            _ = tokio::time::sleep(delay) => {}
                        }
                        return Some(batch);
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
        let url = usage_url(&self.inner.options.control_plane_url)?;
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
        } else {
            Err(error_for_status(status))
        }
    }

    fn queue_len(&self) -> usize {
        self.lock_queue().len()
    }

    fn pop_batch(&self) -> Option<Vec<StreamUsageEvent>> {
        let mut queue = self.lock_queue();
        if queue.is_empty() {
            metrics::set_commercial_usage_buffer_len(0);
            return None;
        }

        let count = queue.len().min(self.inner.options.flush_max_events);
        let batch = queue.drain(..count).collect();
        metrics::set_commercial_usage_buffer_len(queue.len() as i64);
        Some(batch)
    }

    fn push_front_batch(&self, mut batch: Vec<StreamUsageEvent>) {
        let mut queue = self.lock_queue();
        while let Some(event) = batch.pop() {
            queue.push_front(event);
        }
        while queue.len() > self.inner.options.capacity {
            queue.pop_back();
            metrics::report_commercial_usage_dropped();
        }
        metrics::set_commercial_usage_buffer_len(queue.len() as i64);
    }

    fn lock_queue(&self) -> MutexGuard<'_, VecDeque<StreamUsageEvent>> {
        match self.inner.queue.lock() {
            Ok(queue) => queue,
            Err(poisoned) => {
                tracing::warn!(
                    "commercial usage reporter queue lock poisoned; recovering buffered events"
                );
                poisoned.into_inner()
            }
        }
    }
}

impl UsageReporter for BufferedUsageReporter {
    fn report(&self, event: StreamUsageEvent) {
        let mut queue = self.lock_queue();
        if queue.len() >= self.inner.options.capacity {
            queue.pop_front();
            metrics::report_commercial_usage_dropped();
            tracing::warn!("commercial usage buffer full; dropping oldest event");
        }
        queue.push_back(event);
        metrics::set_commercial_usage_buffer_len(queue.len() as i64);
        drop(queue);
        self.inner.notify.notify_one();
    }

    fn flush_shutdown(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            match tokio::time::timeout(Duration::from_secs(5), self.flush_remaining_once()).await {
                Ok(()) => {}
                Err(_) => {
                    tracing::warn!("timed out flushing commercial usage reporter during shutdown");
                }
            }
        })
    }
}

fn error_for_status(status: StatusCode) -> SendBatchError {
    match status {
        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => SendBatchError::Drop(status),
        _ => SendBatchError::Retry(format!("control plane status {status}")),
    }
}

fn usage_url(base: &url::Url) -> Result<url::Url, SendBatchError> {
    let mut url = base.clone();
    url.set_query(None);
    url.set_fragment(None);
    let mut segments = url
        .path_segments_mut()
        .map_err(|_| SendBatchError::Retry("control plane URL cannot be a base".to_string()))?;
    segments.pop_if_empty();
    segments.extend(USAGE_PATH_SEGMENTS);
    drop(segments);
    Ok(url)
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
    use crate::{
        commercial::types::{DataSource, Endpoint, UsageStatus},
        metrics,
    };

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

    #[test]
    fn usage_url_appends_to_existing_base_path() {
        let url = usage_url(&"https://control.example.com/base/api/".parse().unwrap()).unwrap();

        assert_eq!(
            url.as_str(),
            "https://control.example.com/base/api/internal/portal/v1/usage"
        );
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

    #[tokio::test(start_paused = true)]
    async fn threshold_notify_drains_burst_without_interval_ticks() {
        let reporter = reporter(
            "http://127.0.0.1:1".parse().unwrap(),
            Duration::from_secs(60),
            2,
            10,
        );
        let cancel = CancellationToken::new();
        {
            let mut queue = reporter.lock_queue();
            for index in 0..6 {
                queue.push_back(event(&format!("burst-{index}")));
            }
        }
        metrics::set_commercial_usage_buffer_len(reporter.queue_len() as i64);

        reporter.inner.notify.notify_one();
        reporter.inner.notify.notified().await;

        let start = tokio::time::Instant::now();
        let delivered = Arc::new(Mutex::new(Vec::new()));
        reporter
            .flush_threshold_backlog_using(&cancel, {
                let delivered = delivered.clone();
                move |batch: &[StreamUsageEvent]| -> SendOnceFuture<'_> {
                    let delivered = delivered.clone();
                    let batch = batch
                        .iter()
                        .map(|event| event.event_id.clone())
                        .collect::<Vec<_>>();
                    Box::pin(async move {
                        delivered.lock().unwrap().extend(batch);
                        Ok(())
                    })
                }
            })
            .await;

        let delivered = delivered.lock().unwrap().clone();
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "drain waited for the flush interval"
        );
        assert_eq!(
            delivered,
            ["burst-0", "burst-1", "burst-2", "burst-3", "burst-4", "burst-5"]
                .map(str::to_string)
                .to_vec()
        );
        assert_eq!(reporter.queue_len(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn threshold_drain_stops_when_send_requeues_batch() {
        let reporter = reporter(
            "http://127.0.0.1:1".parse().unwrap(),
            Duration::from_secs(60),
            2,
            10,
        );
        let cancel = CancellationToken::new();
        {
            let mut queue = reporter.lock_queue();
            for index in 0..6 {
                queue.push_back(event(&format!("outage-{index}")));
            }
        }
        let attempts = Arc::new(AtomicUsize::new(0));

        reporter
            .flush_threshold_backlog_using(&cancel, {
                let attempts = attempts.clone();
                move |_: &[StreamUsageEvent]| -> SendOnceFuture<'_> {
                    let attempts = attempts.clone();
                    Box::pin(async move {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        Err(SendBatchError::Retry("outage".to_string()))
                    })
                }
            })
            .await;

        assert_eq!(attempts.load(Ordering::SeqCst), MAX_RETRY_ATTEMPTS);
        assert_eq!(reporter.queue_len(), 6);
        assert_eq!(
            reporter
                .pop_batch()
                .unwrap()
                .iter()
                .map(|event| event.event_id.as_str())
                .collect::<Vec<_>>(),
            ["outage-0", "outage-1"]
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
    async fn client_error_drops_batch_and_records_metric() {
        let (url, mut rx) = mock_server(vec![StatusCode::BAD_REQUEST]).await;
        let reporter = reporter(url, Duration::from_secs(60), 1, 10);
        let before = metrics::COMMERCIAL_USAGE_DROPPED.get();

        reporter.report(event("bad-request"));
        reporter
            .flush_one_batch_with_retry(&CancellationToken::new())
            .await;

        assert!(reporter.pop_batch().is_none());
        assert!(rx.try_recv().is_err());
        assert!(metrics::COMMERCIAL_USAGE_DROPPED.get() > before);
    }

    #[test]
    fn only_payload_validation_statuses_drop_usage_batches() {
        for status in [
            StatusCode::UNAUTHORIZED,
            StatusCode::FORBIDDEN,
            StatusCode::TOO_MANY_REQUESTS,
        ] {
            assert!(matches!(error_for_status(status), SendBatchError::Retry(_)));
        }

        for status in [StatusCode::BAD_REQUEST, StatusCode::UNPROCESSABLE_ENTITY] {
            assert!(matches!(error_for_status(status), SendBatchError::Drop(_)));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn unauthorized_response_requeues_and_delivers_after_recovery() {
        let reporter = reporter(
            "http://127.0.0.1:1".parse().unwrap(),
            Duration::from_secs(60),
            1,
            10,
        );
        let cancel = CancellationToken::new();
        let attempts = Arc::new(AtomicUsize::new(0));
        let batch = vec![event("auth-rotation")];

        let requeued = reporter
            .send_with_retry_using(batch, &cancel, {
                let attempts = attempts.clone();
                move |_| {
                    let attempts = attempts.clone();
                    Box::pin(async move {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        Err(error_for_status(StatusCode::UNAUTHORIZED))
                    })
                }
            })
            .await
            .expect("401 should exhaust retries and requeue the batch");

        assert_eq!(attempts.load(Ordering::SeqCst), MAX_RETRY_ATTEMPTS);
        reporter.push_front_batch(requeued);
        assert_eq!(reporter.queue_len(), 1);

        let delivered = Arc::new(Mutex::new(Vec::new()));
        let recovered = reporter.pop_batch().unwrap();
        assert!(reporter
            .send_with_retry_using(recovered, &cancel, {
                let delivered = delivered.clone();
                move |batch| {
                    let delivered = delivered.clone();
                    let batch = batch.to_vec();
                    Box::pin(async move {
                        delivered.lock().unwrap().extend(batch);
                        Ok(())
                    })
                }
            })
            .await
            .is_none());

        let delivered = delivered.lock().unwrap();
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].event_id, "auth-rotation");
        assert!(reporter.pop_batch().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn retry_exhaustion_requeues_batch_until_recovery() {
        let reporter = reporter(
            "http://127.0.0.1:1".parse().unwrap(),
            Duration::from_secs(60),
            1,
            10,
        );
        let cancel = CancellationToken::new();
        let attempts = Arc::new(AtomicUsize::new(0));
        let batch = vec![event("survives-outage")];

        let requeued = reporter
            .send_with_retry_using(batch, &cancel, {
                let attempts = attempts.clone();
                move |_| {
                    let attempts = attempts.clone();
                    Box::pin(async move {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        Err(SendBatchError::Retry("outage".to_string()))
                    })
                }
            })
            .await
            .expect("retry exhaustion should requeue the batch");

        assert_eq!(attempts.load(Ordering::SeqCst), MAX_RETRY_ATTEMPTS);
        reporter.push_front_batch(requeued);
        assert_eq!(reporter.queue_len(), 1);

        let delivered = Arc::new(Mutex::new(Vec::new()));
        let recovered = reporter.pop_batch().unwrap();
        assert!(reporter
            .send_with_retry_using(recovered, &cancel, {
                let delivered = delivered.clone();
                move |batch| {
                    let delivered = delivered.clone();
                    let batch = batch.to_vec();
                    Box::pin(async move {
                        delivered.lock().unwrap().extend(batch);
                        Ok(())
                    })
                }
            })
            .await
            .is_none());
        let delivered = delivered.lock().unwrap();
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].event_id, "survives-outage");
        assert!(reporter.pop_batch().is_none());
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

    #[tokio::test(start_paused = true)]
    async fn shutdown_flush_delivers_events_enqueued_after_reporter_cancel() {
        let reporter = reporter(
            "http://127.0.0.1:1".parse().unwrap(),
            Duration::from_secs(60),
            10,
            10,
        );
        let cancel = CancellationToken::new();
        let task = tokio::spawn(reporter.clone().run(cancel.clone()));

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("reporter task exits after cancel")
            .expect("reporter task should not panic");

        reporter.report(event("during-drain"));
        let delivered = Arc::new(Mutex::new(Vec::new()));
        reporter
            .flush_remaining_once_using({
                let delivered = delivered.clone();
                move |batch| {
                    let delivered = delivered.clone();
                    let batch = batch.to_vec();
                    Box::pin(async move {
                        delivered.lock().unwrap().extend(batch);
                        Ok(())
                    })
                }
            })
            .await;

        let delivered = delivered.lock().unwrap();
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].event_id, "during-drain");
    }

    #[test]
    fn noop_reporter_drops_events() {
        let reporter = NoopUsageReporter;
        reporter.report(event("noop"));
    }
}
