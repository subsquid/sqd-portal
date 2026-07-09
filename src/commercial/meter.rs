use std::{
    io::{self, Read, Write},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering},
        Arc, OnceLock,
    },
    task::{Context, Poll},
    time::{Duration, Instant as StdInstant, SystemTime, UNIX_EPOCH},
};

use async_compression::tokio::write::GzipDecoder;
use async_stream::stream;
use bytes::Bytes;
use flate2::bufread::MultiGzDecoder;
use futures::{Stream, StreamExt};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::Instant as TokioInstant;
use uuid::{NoContext, Timestamp, Uuid};

use crate::commercial::{concurrency::ConcurrencyPermit, tally::TallyHandle};
use crate::{
    commercial::{
        registry::StreamRegistration, ActiveStreamRegistry, DataSource, Endpoint, Granted,
        GrantedLimits, OnExceed, Principal, StreamUsageEvent, TallyStore, UsageReporter,
        UsageStatus,
    },
    metrics,
    types::Compression,
};

#[derive(Clone)]
pub struct MeterHandle {
    inner: Arc<MeterInner>,
}

struct MeterInner {
    principal: Principal,
    event_id: String,
    request_id: String,
    endpoint: Endpoint,
    dataset: String,
    data_source: AtomicU8,
    started_at: SystemTime,
    started_instant: StdInstant,
    logical: AtomicU64,
    unpaced_logical: AtomicU64,
    pending_tally_logical: AtomicU64,
    wire: AtomicU64,
    blocks: AtomicU64,
    chunks: AtomicU64,
    status: AtomicU8,
    flushed: AtomicBool,
    reporter: Arc<dyn UsageReporter>,
    pod: String,
    quota_version: u64,
    limits: GrantedLimits,
    on_exceed: OnExceed,
    quota_remaining_bytes: Option<i64>,
    tally: Option<TallyHandle>,
    kill: Arc<AtomicBool>,
    floor_bytes_per_sec: Arc<AtomicU64>,
    bucket: AsyncMutex<Option<TokenBucket>>,
    _registration: Option<StreamRegistration>,
    _concurrency_permit: Option<ConcurrencyPermit>,
}

struct TokenBucket {
    available: f64,
    burst: f64,
    last: TokioInstant,
}

impl MeterHandle {
    pub fn new(
        principal: Principal,
        request_id: String,
        endpoint: Endpoint,
        dataset: String,
        reporter: Arc<dyn UsageReporter>,
        quota_version: u64,
    ) -> Self {
        Self::from_parts(
            principal,
            None,
            GrantedLimits::default(),
            OnExceed::Reject,
            quota_version,
            None,
            request_id,
            endpoint,
            dataset,
            reporter,
            None,
            None,
            None,
        )
    }

    pub fn new_enforced(
        granted: Granted,
        request_id: String,
        endpoint: Endpoint,
        dataset: String,
        reporter: Arc<dyn UsageReporter>,
        tally: Arc<TallyStore>,
        registry: Arc<ActiveStreamRegistry>,
    ) -> Self {
        let concurrency_permit = granted.concurrency_permit;
        Self::from_parts(
            granted.principal,
            granted.tally_account_id,
            granted.limits,
            granted.on_exceed,
            granted.quota_version,
            granted.quota_remaining_bytes,
            request_id,
            endpoint,
            dataset,
            reporter,
            Some(tally),
            Some(registry),
            concurrency_permit,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn from_parts(
        principal: Principal,
        tally_account_id: Option<String>,
        limits: GrantedLimits,
        on_exceed: OnExceed,
        quota_version: u64,
        quota_remaining_bytes: Option<i64>,
        request_id: String,
        endpoint: Endpoint,
        dataset: String,
        reporter: Arc<dyn UsageReporter>,
        tally: Option<Arc<TallyStore>>,
        registry: Option<Arc<ActiveStreamRegistry>>,
        concurrency_permit: Option<ConcurrencyPermit>,
    ) -> Self {
        let event_id = uuid_v7();
        let tally_account_id = tally_account_id.unwrap_or_else(|| principal.account_id.clone());
        let kill = Arc::new(AtomicBool::new(false));
        let floor_bytes_per_sec = Arc::new(AtomicU64::new(0));
        let bucket = AsyncMutex::new(bucket_from_limits(&limits));
        let tally = tally.map(|tally| tally.handle(&tally_account_id, quota_version));
        let registration = registry.as_ref().map(|registry| {
            registry.register(
                event_id.clone(),
                principal.api_key_id.clone(),
                kill.clone(),
                floor_bytes_per_sec.clone(),
            )
        });
        Self {
            inner: Arc::new(MeterInner {
                principal,
                event_id,
                request_id,
                endpoint,
                dataset,
                data_source: AtomicU8::new(data_source_code(DataSource::Network)),
                started_at: SystemTime::now(),
                started_instant: StdInstant::now(),
                logical: AtomicU64::new(0),
                unpaced_logical: AtomicU64::new(0),
                pending_tally_logical: AtomicU64::new(0),
                wire: AtomicU64::new(0),
                blocks: AtomicU64::new(0),
                chunks: AtomicU64::new(0),
                status: AtomicU8::new(status_code(UsageStatus::ClientDisconnect)),
                flushed: AtomicBool::new(false),
                reporter,
                pod: pod_hostname().to_string(),
                quota_version,
                limits,
                on_exceed,
                quota_remaining_bytes,
                tally,
                kill,
                floor_bytes_per_sec,
                bucket,
                _registration: registration,
                _concurrency_permit: concurrency_permit,
            }),
        }
    }

    pub fn set_data_source(&self, source: DataSource) {
        self.inner
            .data_source
            .store(data_source_code(source), Ordering::Relaxed);
    }

    pub fn add_logical_bytes(&self, bytes: u64) {
        self.inner.add_logical_bytes(bytes);
    }

    pub fn add_wire_bytes(&self, bytes: u64) {
        self.inner.wire.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn add_blocks(&self, blocks: u64) {
        self.inner.blocks.fetch_add(blocks, Ordering::Relaxed);
    }

    pub fn add_chunk(&self) {
        self.inner.chunks.fetch_add(1, Ordering::Relaxed);
    }

    pub fn mark_error(&self) {
        self.inner.set_status(UsageStatus::Error);
    }

    pub fn complete(&self) {
        self.inner.flush_pending_tally();
        let _ = self.inner.status.compare_exchange(
            status_code(UsageStatus::ClientDisconnect),
            status_code(UsageStatus::Completed),
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        self.inner.flush_once();
    }

    pub fn discard(&self) {
        self.inner.flush_pending_tally();
        self.inner.flushed.store(true, Ordering::Release);
    }

    pub fn should_stop_after_chunk(&self) -> bool {
        self.inner.should_stop_after_chunk()
    }

    pub async fn pace_pending_logical(&self) {
        let bytes = self.inner.unpaced_logical.swap(0, Ordering::AcqRel);
        if bytes > 0 {
            self.inner.pace(bytes).await;
        }
    }

    pub fn record_plain_bytes_and_complete(&self, bytes: u64) {
        self.add_logical_bytes(bytes);
        self.add_wire_bytes(bytes);
        self.should_stop_after_chunk();
        self.complete();
    }

    pub fn record_gzip_body_and_complete(&self, bytes: &[u8]) -> anyhow::Result<()> {
        self.add_chunk();
        self.add_wire_bytes(bytes.len() as u64);
        self.add_logical_bytes(gzip_len(bytes)?);
        self.complete();
        Ok(())
    }
}

impl MeterInner {
    fn add_logical_bytes(&self, bytes: u64) {
        self.logical.fetch_add(bytes, Ordering::Relaxed);
        self.unpaced_logical.fetch_add(bytes, Ordering::Relaxed);
        if self.tally.is_some() {
            self.pending_tally_logical
                .fetch_add(bytes, Ordering::Relaxed);
        }
    }

    fn should_stop_after_chunk(&self) -> bool {
        let effective_remaining = self.flush_pending_tally();
        if self.kill.load(Ordering::Acquire) {
            self.set_status(UsageStatus::CutSuspended);
            return true;
        }

        let logical = self.logical.load(Ordering::Acquire);
        if self
            .limits
            .max_response_bytes
            .is_some_and(|max| logical > max)
        {
            self.set_status(UsageStatus::CutMaxBytes);
            return true;
        }

        let Some(effective_remaining) = effective_remaining else {
            return false;
        };
        if effective_remaining > 0 {
            return false;
        }

        match self.on_exceed {
            OnExceed::Reject => {
                self.set_status(UsageStatus::CutQuota);
                true
            }
            OnExceed::Throttle {
                floor_bytes_per_sec,
            } => {
                self.floor_bytes_per_sec
                    .store(floor_bytes_per_sec, Ordering::Release);
                false
            }
        }
    }

    fn flush_pending_tally(&self) -> Option<i64> {
        let Some(tally) = &self.tally else {
            return None;
        };
        let bytes = self.pending_tally_logical.swap(0, Ordering::AcqRel);
        match self.quota_remaining_bytes {
            Some(snapshot_remaining) => Some(tally.debit_and_effective_remaining(
                self.quota_version,
                bytes,
                snapshot_remaining,
            )),
            None => {
                if bytes > 0 {
                    tally.debit(self.quota_version, bytes);
                }
                None
            }
        }
    }

    fn set_status(&self, status: UsageStatus) {
        let code = status_code(status.clone());
        let old = self.status.swap(code, Ordering::Relaxed);
        if old != code
            && matches!(
                status,
                UsageStatus::CutQuota | UsageStatus::CutMaxBytes | UsageStatus::CutSuspended
            )
        {
            metrics::report_commercial_cutoff(&status);
        }
    }

    async fn pace(&self, bytes: u64) {
        let mut warned = false;
        loop {
            let Some((rate, burst)) = self.current_rate_and_burst() else {
                return;
            };
            let wait = {
                let mut bucket = self.bucket.lock().await;
                let bucket = bucket.get_or_insert_with(|| TokenBucket {
                    available: burst,
                    burst,
                    last: TokioInstant::now(),
                });
                bucket.refill(rate, burst);
                if bucket.available > 0.0 {
                    bucket.available -= bytes as f64;
                    return;
                }
                Duration::from_secs_f64(((-bucket.available) + 1.0) / rate)
            };
            metrics::observe_commercial_throttle_stall(wait);
            if wait > Duration::from_secs(30) && !warned {
                tracing::warn!(
                    stall_seconds = wait.as_secs_f64(),
                    "commercial throttle stalled a response chunk for more than 30 seconds"
                );
                warned = true;
            }
            tokio::time::sleep(wait).await;
        }
    }

    fn current_rate_and_burst(&self) -> Option<(f64, f64)> {
        let floor = self.floor_bytes_per_sec.load(Ordering::Acquire);
        if floor > 0 {
            let floor = floor as f64;
            return Some((floor, floor));
        }
        let rate = self.limits.throughput_bytes_per_sec? as f64;
        if rate <= 0.0 {
            return None;
        }
        let burst = self.limits.burst_bytes.unwrap_or(rate as u64) as f64;
        Some((rate, burst.max(1.0)))
    }

    fn flush_once(&self) {
        if self.flushed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.flush_pending_tally();

        let logical = self.logical.load(Ordering::Relaxed);
        let wire = self.wire.load(Ordering::Relaxed);
        let data_source = data_source_from_code(self.data_source.load(Ordering::Relaxed));
        metrics::report_commercial_stream_bytes(&self.endpoint, &data_source, logical, wire);
        self.reporter.report(StreamUsageEvent {
            event_id: self.event_id.clone(),
            request_id: self.request_id.clone(),
            account_id: self.principal.account_id.clone(),
            api_key_id: self.principal.api_key_id.clone(),
            dataset: self.dataset.clone(),
            endpoint: self.endpoint,
            data_source,
            logical_bytes: logical,
            wire_bytes: wire,
            blocks: self.blocks.load(Ordering::Relaxed),
            chunks: self.chunks.load(Ordering::Relaxed),
            started_at: seconds_since_epoch(self.started_at),
            duration_ms: duration_ms(self.started_instant.elapsed()),
            status: status_from_code(self.status.load(Ordering::Relaxed)),
            pod: self.pod.clone(),
            quota_version: self.quota_version,
        });
    }
}

impl Drop for MeterInner {
    fn drop(&mut self) {
        self.flush_once();
    }
}

pub fn tap_input_frames<S>(
    input: S,
    compression: Compression,
    meter: MeterHandle,
) -> impl Stream<Item = Vec<u8>> + Send
where
    S: Stream<Item = Vec<u8>> + Send + 'static,
{
    stream! {
        let mut zstd_decoder = match compression {
            Compression::Gzip => None,
            Compression::Zstd => match zstd::stream::write::Decoder::new(ZstdLogicalByteCounter {
                meter: meter.clone(),
            }) {
                Ok(decoder) => Some(decoder),
                Err(err) => {
                    tracing::warn!(error = %err, "failed to initialize zstd usage decoder");
                    meter.mark_error();
                    None
                }
            },
        };
        futures::pin_mut!(input);
        while let Some(frame) = input.next().await {
            meter.add_chunk();
            match compression {
                Compression::Gzip => match gzip_len(&frame) {
                    Ok(len) => meter.add_logical_bytes(len),
                    Err(err) => {
                        tracing::warn!(error = %err, "failed to decode portal usage gzip frame for logical byte count");
                        meter.mark_error();
                    }
                },
                Compression::Zstd => {
                    if let Some(decoder) = &mut zstd_decoder {
                        if let Err(err) = decoder.write_all(&frame) {
                            tracing::warn!(error = %err, "failed to decode portal usage zstd stream for logical byte count");
                            meter.mark_error();
                        }
                    }
                }
            }
            let stop = meter.should_stop_after_chunk();
            meter.pace_pending_logical().await;
            yield frame;
            if stop {
                break;
            }
        }
        if let Some(mut decoder) = zstd_decoder {
            if let Err(err) = decoder.flush() {
                tracing::warn!(error = %err, "failed to finish zstd stream usage decoder");
                meter.mark_error();
            }
        }
    }
}

pub fn tap_input_chunks<S>(input: S, meter: MeterHandle) -> impl Stream<Item = Vec<u8>> + Send
where
    S: Stream<Item = Vec<u8>> + Send + 'static,
{
    stream! {
        futures::pin_mut!(input);
        while let Some(frame) = input.next().await {
            meter.add_chunk();
            yield frame;
        }
    }
}

pub fn tap_plain_stream<S, E>(input: S, meter: MeterHandle) -> impl Stream<Item = Result<Bytes, E>>
where
    S: Stream<Item = Result<Bytes, E>> + Send + 'static,
{
    stream! {
        futures::pin_mut!(input);
        while let Some(item) = input.next().await {
            if let Ok(bytes) = &item {
                meter.add_logical_bytes(bytes.len() as u64);
                meter.add_wire_bytes(bytes.len() as u64);
            } else {
                meter.mark_error();
            }
            meter.pace_pending_logical().await;
            let stop = meter.should_stop_after_chunk();
            yield item;
            if stop {
                break;
            }
        }
        meter.complete();
    }
}

pub fn tap_wire_stream<S, E>(input: S, meter: MeterHandle) -> impl Stream<Item = Result<Bytes, E>>
where
    S: Stream<Item = Result<Bytes, E>> + Send + 'static,
{
    stream! {
        futures::pin_mut!(input);
        while let Some(item) = input.next().await {
            if let Ok(bytes) = &item {
                meter.add_wire_bytes(bytes.len() as u64);
            } else {
                meter.mark_error();
            }
            meter.pace_pending_logical().await;
            let stop = meter.should_stop_after_chunk();
            yield item;
            if stop {
                break;
            }
        }
        meter.complete();
    }
}

pub fn tap_wire_stream_no_complete<S, E>(
    input: S,
    meter: MeterHandle,
) -> impl Stream<Item = Result<Bytes, E>>
where
    S: Stream<Item = Result<Bytes, E>> + Send + 'static,
{
    stream! {
        futures::pin_mut!(input);
        while let Some(item) = input.next().await {
            if let Ok(bytes) = &item {
                meter.add_chunk();
                meter.add_wire_bytes(bytes.len() as u64);
            } else {
                meter.mark_error();
            }
            yield item;
        }
    }
}

pub fn tap_gzip_stream<S, E>(input: S, meter: MeterHandle) -> impl Stream<Item = Result<Bytes, E>>
where
    S: Stream<Item = Result<Bytes, E>> + Send + 'static,
{
    stream! {
        let mut decoder = GzipDecoder::new(LogicalByteCounter {
            meter: meter.clone(),
        });
        futures::pin_mut!(input);
        while let Some(item) = input.next().await {
            match &item {
                Ok(bytes) => {
                    meter.add_wire_bytes(bytes.len() as u64);
                    if let Err(err) = decoder.write_all(bytes).await {
                        tracing::warn!(error = %err, "failed to decode gzip stream for logical byte count");
                        meter.mark_error();
                    }
                }
                Err(_) => meter.mark_error(),
            }
            meter.pace_pending_logical().await;
            let stop = meter.should_stop_after_chunk();
            yield item;
            if stop {
                break;
            }
        }
        if let Err(err) = decoder.shutdown().await {
            tracing::warn!(error = %err, "failed to finish gzip stream usage decoder");
            meter.mark_error();
        }
        meter.complete();
    }
}

struct LogicalByteCounter {
    meter: MeterHandle,
}

impl AsyncWrite for LogicalByteCounter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.meter.add_logical_bytes(buf.len() as u64);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

struct ZstdLogicalByteCounter {
    meter: MeterHandle,
}

impl Write for ZstdLogicalByteCounter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.meter.add_logical_bytes(buf.len() as u64);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn gzip_len(bytes: &[u8]) -> anyhow::Result<u64> {
    let mut decoder = MultiGzDecoder::new(bytes);
    let mut decoded = Vec::new();
    decoder.read_to_end(&mut decoded)?;
    Ok(decoded.len() as u64)
}

fn bucket_from_limits(limits: &GrantedLimits) -> Option<TokenBucket> {
    let rate = limits.throughput_bytes_per_sec?;
    if rate == 0 {
        return None;
    }
    let burst = limits.burst_bytes.unwrap_or(rate).max(1);
    Some(TokenBucket {
        available: burst as f64,
        burst: burst as f64,
        last: TokioInstant::now(),
    })
}

impl TokenBucket {
    fn refill(&mut self, rate: f64, burst: f64) {
        let now = TokioInstant::now();
        let elapsed = now.duration_since(self.last).as_secs_f64();
        self.last = now;
        self.burst = burst.max(1.0);
        self.available = (self.available + elapsed * rate).min(self.burst);
    }
}

fn uuid_v7() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    Uuid::new_v7(Timestamp::from_unix(
        NoContext,
        now.as_secs(),
        now.subsec_nanos(),
    ))
    .to_string()
}

fn pod_hostname() -> &'static str {
    static HOSTNAME: OnceLock<String> = OnceLock::new();
    HOSTNAME
        .get_or_init(|| std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string()))
        .as_str()
}

fn seconds_since_epoch(time: SystemTime) -> f64 {
    let duration = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    duration.as_secs_f64()
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

fn data_source_code(source: DataSource) -> u8 {
    match source {
        DataSource::Network => 0,
        DataSource::RealTime => 1,
    }
}

fn data_source_from_code(code: u8) -> DataSource {
    match code {
        1 => DataSource::RealTime,
        _ => DataSource::Network,
    }
}

fn status_code(status: UsageStatus) -> u8 {
    match status {
        UsageStatus::Completed => 0,
        UsageStatus::ClientDisconnect => 1,
        UsageStatus::CutQuota => 2,
        UsageStatus::CutMaxBytes => 3,
        UsageStatus::CutSuspended => 4,
        UsageStatus::Error => 5,
    }
}

fn status_from_code(code: u8) -> UsageStatus {
    match code {
        0 => UsageStatus::Completed,
        2 => UsageStatus::CutQuota,
        3 => UsageStatus::CutMaxBytes,
        4 => UsageStatus::CutSuspended,
        5 => UsageStatus::Error,
        _ => UsageStatus::ClientDisconnect,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_compression::tokio::bufread::GzipEncoder;
    use futures::stream;
    use tokio::io::{AsyncBufReadExt, BufReader};

    use super::*;
    use crate::commercial::{ConcurrencyLimiter, UsageReporter};

    #[derive(Default)]
    struct RecordingReporter {
        events: Mutex<Vec<StreamUsageEvent>>,
    }

    impl UsageReporter for RecordingReporter {
        fn report(&self, event: StreamUsageEvent) {
            self.events.lock().unwrap().push(event);
        }
    }

    async fn gzip(bytes: &[u8]) -> Vec<u8> {
        let encoder = GzipEncoder::with_quality(bytes, async_compression::Level::Fastest);
        let mut reader = BufReader::new(encoder);
        reader.fill_buf().await.unwrap().to_vec()
    }

    fn meter(reporter: Arc<RecordingReporter>) -> MeterHandle {
        meter_for(reporter, Endpoint::Stream)
    }

    fn meter_for(reporter: Arc<RecordingReporter>, endpoint: Endpoint) -> MeterHandle {
        MeterHandle::new(
            Principal {
                account_id: "account".to_string(),
                api_key_id: Some("key".to_string()),
            },
            "request".to_string(),
            endpoint,
            "ethereum-mainnet".to_string(),
            reporter,
            7,
        )
    }

    fn enforced_meter(
        reporter: Arc<RecordingReporter>,
        limits: GrantedLimits,
        on_exceed: OnExceed,
        quota_remaining_bytes: Option<i64>,
    ) -> (MeterHandle, Arc<TallyStore>, Arc<ActiveStreamRegistry>) {
        let tally = Arc::new(TallyStore::default());
        let registry = Arc::new(ActiveStreamRegistry::default());
        let granted = Granted {
            principal: Principal {
                account_id: "account".to_string(),
                api_key_id: Some("key".to_string()),
            },
            tally_account_id: None,
            limits,
            on_exceed,
            quota_version: 7,
            quota_remaining_bytes,
            concurrency_permit: None,
        };
        (
            MeterHandle::new_enforced(
                granted,
                "request".to_string(),
                Endpoint::Stream,
                "ethereum-mainnet".to_string(),
                reporter,
                tally.clone(),
                registry.clone(),
            ),
            tally,
            registry,
        )
    }

    fn enforced_meter_with_grant(
        reporter: Arc<RecordingReporter>,
        granted: Granted,
    ) -> MeterHandle {
        MeterHandle::new_enforced(
            granted,
            "request".to_string(),
            Endpoint::Stream,
            "ethereum-mainnet".to_string(),
            reporter,
            Arc::new(TallyStore::default()),
            Arc::new(ActiveStreamRegistry::default()),
        )
    }

    fn assert_meter_holds_permit_until_last_handle_drops(account_key: &str, granted: Granted) {
        let reporter = Arc::new(RecordingReporter::default());
        let limiter = ConcurrencyLimiter::new(1);
        let mut granted = granted;
        granted.concurrency_permit = limiter.try_acquire(account_key, 1);

        let meter = enforced_meter_with_grant(reporter, granted);
        let other = limiter.try_acquire(account_key, 1).unwrap();
        assert!(limiter.try_acquire(account_key, 1).is_none());

        let cloned = meter.clone();
        drop(meter);
        assert!(limiter.try_acquire(account_key, 1).is_none());

        drop(cloned);
        assert!(limiter.try_acquire(account_key, 1).is_some());
        drop(other);
    }

    async fn drive_metered_frames(
        frames: Vec<Vec<u8>>,
        compression: Compression,
    ) -> StreamUsageEvent {
        let reporter = Arc::new(RecordingReporter::default());
        let meter = meter(reporter.clone());
        let input = tap_input_frames(stream::iter(frames), compression, meter.clone());
        let output = tap_wire_stream(
            input.map(|frame| Ok::<_, std::io::Error>(Bytes::from(frame))),
            meter,
        );
        let _: Vec<_> = output.collect().await;

        let event = reporter.events.lock().unwrap().pop().unwrap();
        event
    }

    #[tokio::test]
    async fn gzip_tap_counts_logical_and_wire_bytes() {
        let first = b"{\"a\":1}\n";
        let second = b"{\"b\":2}\n";
        let frames = vec![gzip(first).await, gzip(second).await];
        let wire = frames.iter().map(Vec::len).sum::<usize>() as u64;

        let event = drive_metered_frames(frames, Compression::Gzip).await;

        assert_eq!(event.logical_bytes, (first.len() + second.len()) as u64);
        assert_eq!(event.wire_bytes, wire);
        assert_eq!(event.chunks, 2);
        assert_eq!(event.status, UsageStatus::Completed);
    }

    #[tokio::test]
    async fn zstd_tap_counts_self_contained_frames() {
        let data = b"{\"zstd\":true}\n";
        let frame = zstd::stream::encode_all(&data[..], 0).unwrap();
        let wire = frame.len() as u64;

        let event = drive_metered_frames(vec![frame], Compression::Zstd).await;

        assert_eq!(event.logical_bytes, data.len() as u64);
        assert_eq!(event.wire_bytes, wire);
        assert_eq!(event.chunks, 1);
    }

    #[tokio::test]
    async fn zstd_tap_counts_split_frame_across_chunks() {
        let data = b"{\"split\":true,\"frame\":\"zstd\"}\n";
        let frame = zstd::stream::encode_all(&data[..], 0).unwrap();
        let split = frame.len() / 2;
        let wire = frame.len() as u64;

        let event = drive_metered_frames(
            vec![frame[..split].to_vec(), frame[split..].to_vec()],
            Compression::Zstd,
        )
        .await;

        assert_eq!(event.logical_bytes, data.len() as u64);
        assert_eq!(event.wire_bytes, wire);
        assert_eq!(event.chunks, 2);
        assert_eq!(event.status, UsageStatus::Completed);
    }

    #[tokio::test]
    async fn legacy_query_gzip_recording_counts_json_body() {
        let reporter = Arc::new(RecordingReporter::default());
        let meter = meter_for(reporter.clone(), Endpoint::LegacyQuery);
        let body = b"[{\"legacy\":true}]\n";
        let encoded = gzip(body).await;

        meter.record_gzip_body_and_complete(&encoded).unwrap();

        let event = reporter.events.lock().unwrap().pop().unwrap();
        assert_eq!(event.endpoint, Endpoint::LegacyQuery);
        assert_eq!(event.logical_bytes, body.len() as u64);
        assert_eq!(event.wire_bytes, encoded.len() as u64);
        assert_eq!(event.chunks, 1);
        assert_eq!(event.status, UsageStatus::Completed);
    }

    #[test]
    fn discard_suppresses_drop_flush() {
        let reporter = Arc::new(RecordingReporter::default());
        {
            let meter = meter(reporter.clone());
            meter.discard();
        }

        assert!(reporter.events.lock().unwrap().is_empty());
    }

    #[test]
    fn drop_flushes_pending_tally_debits() {
        let reporter = Arc::new(RecordingReporter::default());
        let (meter, tally, _) = enforced_meter(
            reporter,
            GrantedLimits::default(),
            OnExceed::Reject,
            Some(100),
        );

        meter.add_logical_bytes(4);
        drop(meter);

        assert_eq!(tally.bytes_for("account", 7), 4);
    }

    #[test]
    fn enforced_meter_holds_account_concurrency_permit_until_last_handle_drops() {
        assert_meter_holds_permit_until_last_handle_drops(
            "account",
            Granted {
                principal: Principal {
                    account_id: "account".to_string(),
                    api_key_id: Some("key".to_string()),
                },
                tally_account_id: None,
                limits: GrantedLimits::default(),
                on_exceed: OnExceed::Reject,
                quota_version: 7,
                quota_remaining_bytes: Some(100),
                concurrency_permit: None,
            },
        );
    }

    #[test]
    fn enforced_meter_holds_anonymous_concurrency_permit_until_last_handle_drops() {
        assert_meter_holds_permit_until_last_handle_drops(
            "anon:203.0.113.7/32",
            Granted {
                principal: Principal {
                    account_id: "anonymous".to_string(),
                    api_key_id: None,
                },
                tally_account_id: Some("anon:203.0.113.7/32".to_string()),
                limits: GrantedLimits::default(),
                on_exceed: OnExceed::Reject,
                quota_version: 120,
                quota_remaining_bytes: Some(100),
                concurrency_permit: None,
            },
        );
    }

    #[tokio::test]
    async fn plain_tap_counts_logical_and_wire_together() {
        let reporter = Arc::new(RecordingReporter::default());
        let meter = meter(reporter.clone());
        let chunks = vec![Ok::<_, std::io::Error>(Bytes::from_static(b"abc"))];
        let output = tap_plain_stream(stream::iter(chunks), meter);
        let _: Vec<_> = output.collect().await;
        let event = reporter.events.lock().unwrap().pop().unwrap();

        assert_eq!(event.logical_bytes, 3);
        assert_eq!(event.wire_bytes, 3);
        assert_eq!(event.status, UsageStatus::Completed);
    }

    #[tokio::test]
    async fn gzip_stream_tap_counts_split_encoded_body() {
        let reporter = Arc::new(RecordingReporter::default());
        let meter = meter(reporter.clone());
        let body = b"{\"header\":{\"number\":1}}\n";
        let gzip = gzip(body).await;
        let split = gzip.len() / 2;
        let chunks = vec![
            Ok::<_, std::io::Error>(Bytes::copy_from_slice(&gzip[..split])),
            Ok::<_, std::io::Error>(Bytes::copy_from_slice(&gzip[split..])),
        ];
        let output = tap_gzip_stream(stream::iter(chunks), meter);
        let _: Vec<_> = output.collect().await;
        let event = reporter.events.lock().unwrap().pop().unwrap();

        assert_eq!(event.logical_bytes, body.len() as u64);
        assert_eq!(event.wire_bytes, gzip.len() as u64);
        assert_eq!(event.status, UsageStatus::Completed);
    }

    #[tokio::test]
    async fn drop_flushes_client_disconnect_with_partial_counts() {
        let reporter = Arc::new(RecordingReporter::default());
        let meter = meter(reporter.clone());
        let chunks = vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"first")),
            Ok::<_, std::io::Error>(Bytes::from_static(b"second")),
        ];
        let mut output = Box::pin(tap_wire_stream(stream::iter(chunks), meter));
        assert!(output.next().await.is_some());
        drop(output);

        let event = reporter.events.lock().unwrap().pop().unwrap();
        assert_eq!(event.wire_bytes, 5);
        assert_eq!(event.status, UsageStatus::ClientDisconnect);
    }

    #[tokio::test]
    async fn enforced_plain_stream_cuts_after_max_response_chunk() {
        let reporter = Arc::new(RecordingReporter::default());
        let (meter, tally, _) = enforced_meter(
            reporter.clone(),
            GrantedLimits {
                max_response_bytes: Some(5),
                ..GrantedLimits::default()
            },
            OnExceed::Reject,
            Some(100),
        );
        let chunks = vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"12345")),
            Ok::<_, std::io::Error>(Bytes::from_static(b"6")),
            Ok::<_, std::io::Error>(Bytes::from_static(b"never")),
        ];

        let output = tap_plain_stream(stream::iter(chunks), meter);
        let emitted: Vec<_> = output.collect().await;

        assert_eq!(emitted.len(), 2);
        assert_eq!(tally.bytes_for("account", 7), 6);
        let event = reporter.events.lock().unwrap().pop().unwrap();
        assert_eq!(event.logical_bytes, 6);
        assert_eq!(event.status, UsageStatus::CutMaxBytes);
    }

    #[tokio::test]
    async fn enforced_plain_stream_cuts_when_quota_remaining_crosses_zero() {
        let reporter = Arc::new(RecordingReporter::default());
        let (meter, tally, _) = enforced_meter(
            reporter.clone(),
            GrantedLimits::default(),
            OnExceed::Reject,
            Some(5),
        );
        let chunks = vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"123")),
            Ok::<_, std::io::Error>(Bytes::from_static(b"456")),
            Ok::<_, std::io::Error>(Bytes::from_static(b"never")),
        ];

        let output = tap_plain_stream(stream::iter(chunks), meter);
        let emitted: Vec<_> = output.collect().await;

        assert_eq!(emitted.len(), 2);
        assert_eq!(tally.effective_remaining("account", 7, 5), -1);
        let event = reporter.events.lock().unwrap().pop().unwrap();
        assert_eq!(event.logical_bytes, 6);
        assert_eq!(event.status, UsageStatus::CutQuota);
    }

    #[tokio::test]
    async fn enforced_plain_stream_cuts_when_registry_kills_key() {
        let reporter = Arc::new(RecordingReporter::default());
        let (meter, _, registry) = enforced_meter(
            reporter.clone(),
            GrantedLimits::default(),
            OnExceed::Reject,
            Some(100),
        );
        assert_eq!(registry.kill_key("key"), 1);
        let chunks = vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"first")),
            Ok::<_, std::io::Error>(Bytes::from_static(b"never")),
        ];

        let output = tap_plain_stream(stream::iter(chunks), meter);
        let emitted: Vec<_> = output.collect().await;

        assert_eq!(emitted.len(), 1);
        let event = reporter.events.lock().unwrap().pop().unwrap();
        assert_eq!(event.status, UsageStatus::CutSuspended);
        assert_eq!(registry.len(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn pacing_waits_between_chunks_after_burst_is_spent() {
        let reporter = Arc::new(RecordingReporter::default());
        let (meter, _, _) = enforced_meter(
            reporter.clone(),
            GrantedLimits {
                throughput_bytes_per_sec: Some(1),
                burst_bytes: Some(1),
                ..GrantedLimits::default()
            },
            OnExceed::Reject,
            Some(100),
        );
        let chunks = vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"a")),
            Ok::<_, std::io::Error>(Bytes::from_static(b"b")),
            Ok::<_, std::io::Error>(Bytes::from_static(b"c")),
        ];
        let start = tokio::time::Instant::now();

        let output = tap_plain_stream(stream::iter(chunks), meter);
        let emitted: Vec<_> = output.collect().await;

        assert_eq!(emitted.len(), 3);
        assert!(start.elapsed() >= Duration::from_secs(2));
    }

    #[tokio::test(start_paused = true)]
    async fn burst_allows_initial_chunks_without_waiting() {
        let reporter = Arc::new(RecordingReporter::default());
        let (meter, _, _) = enforced_meter(
            reporter.clone(),
            GrantedLimits {
                throughput_bytes_per_sec: Some(1),
                burst_bytes: Some(2),
                ..GrantedLimits::default()
            },
            OnExceed::Reject,
            Some(100),
        );
        let chunks = vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"a")),
            Ok::<_, std::io::Error>(Bytes::from_static(b"b")),
        ];
        let start = tokio::time::Instant::now();

        let output = tap_plain_stream(stream::iter(chunks), meter);
        let emitted: Vec<_> = output.collect().await;

        assert_eq!(emitted.len(), 2);
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn oversized_chunk_creates_debt_without_deadlocking() {
        let reporter = Arc::new(RecordingReporter::default());
        let (meter, _, _) = enforced_meter(
            reporter.clone(),
            GrantedLimits {
                throughput_bytes_per_sec: Some(10),
                burst_bytes: Some(5),
                ..GrantedLimits::default()
            },
            OnExceed::Reject,
            Some(100),
        );
        let chunks = vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"abcdefghijklmnopqrst")),
            Ok::<_, std::io::Error>(Bytes::from_static(b"!")),
        ];
        let start = tokio::time::Instant::now();

        let output = tap_plain_stream(stream::iter(chunks), meter);
        let emitted: Vec<_> = output.collect().await;

        assert_eq!(emitted.len(), 2);
        assert!(start.elapsed() >= Duration::from_millis(1500));
    }

    #[tokio::test(start_paused = true)]
    async fn throttle_floor_switch_slows_later_chunks() {
        let reporter = Arc::new(RecordingReporter::default());
        let (meter, _, _) = enforced_meter(
            reporter.clone(),
            GrantedLimits {
                throughput_bytes_per_sec: Some(100),
                burst_bytes: Some(100),
                ..GrantedLimits::default()
            },
            OnExceed::Throttle {
                floor_bytes_per_sec: 1,
            },
            Some(1),
        );
        let chunks = vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"xx")),
            Ok::<_, std::io::Error>(Bytes::from_static(b"y")),
            Ok::<_, std::io::Error>(Bytes::from_static(b"z")),
        ];
        let start = tokio::time::Instant::now();

        let output = tap_plain_stream(stream::iter(chunks), meter);
        let emitted: Vec<_> = output.collect().await;

        assert_eq!(emitted.len(), 3);
        assert!(start.elapsed() >= Duration::from_secs(1));
        let event = reporter.events.lock().unwrap().pop().unwrap();
        assert_eq!(event.status, UsageStatus::Completed);
    }

    #[tokio::test(start_paused = true)]
    async fn throttle_floor_installs_bucket_for_unlimited_grant() {
        let reporter = Arc::new(RecordingReporter::default());
        let (meter, _, _) = enforced_meter(
            reporter.clone(),
            GrantedLimits::default(),
            OnExceed::Throttle {
                floor_bytes_per_sec: 1,
            },
            Some(1),
        );
        let chunks = vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"xx")),
            Ok::<_, std::io::Error>(Bytes::from_static(b"y")),
            Ok::<_, std::io::Error>(Bytes::from_static(b"z")),
        ];
        let start = tokio::time::Instant::now();

        let output = tap_plain_stream(stream::iter(chunks), meter);
        let emitted: Vec<_> = output.collect().await;

        assert_eq!(emitted.len(), 3);
        assert!(start.elapsed() >= Duration::from_secs(1));
        let event = reporter.events.lock().unwrap().pop().unwrap();
        assert_eq!(event.status, UsageStatus::Completed);
    }
}
