use std::{
    io::{self, Read, Write},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering},
        Arc, OnceLock,
    },
    task::{Context, Poll},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_compression::tokio::write::GzipDecoder;
use async_stream::stream;
use bytes::Bytes;
use flate2::bufread::MultiGzDecoder;
use futures::{Stream, StreamExt};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use uuid::{NoContext, Timestamp, Uuid};

use crate::{
    commercial::{DataSource, Endpoint, Principal, StreamUsageEvent, UsageReporter, UsageStatus},
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
    started_instant: Instant,
    logical: AtomicU64,
    wire: AtomicU64,
    blocks: AtomicU64,
    chunks: AtomicU64,
    status: AtomicU8,
    flushed: AtomicBool,
    reporter: Arc<dyn UsageReporter>,
    pod: String,
    quota_version: u64,
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
        Self {
            inner: Arc::new(MeterInner {
                principal,
                event_id: uuid_v7(),
                request_id,
                endpoint,
                dataset,
                data_source: AtomicU8::new(data_source_code(DataSource::Network)),
                started_at: SystemTime::now(),
                started_instant: Instant::now(),
                logical: AtomicU64::new(0),
                wire: AtomicU64::new(0),
                blocks: AtomicU64::new(0),
                chunks: AtomicU64::new(0),
                status: AtomicU8::new(status_code(UsageStatus::ClientDisconnect)),
                flushed: AtomicBool::new(false),
                reporter,
                pod: pod_hostname().to_string(),
                quota_version,
            }),
        }
    }

    pub fn set_data_source(&self, source: DataSource) {
        self.inner
            .data_source
            .store(data_source_code(source), Ordering::Relaxed);
    }

    pub fn add_logical_bytes(&self, bytes: u64) {
        self.inner.logical.fetch_add(bytes, Ordering::Relaxed);
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
        self.inner
            .status
            .store(status_code(UsageStatus::Error), Ordering::Relaxed);
    }

    pub fn complete(&self) {
        let _ = self.inner.status.compare_exchange(
            status_code(UsageStatus::ClientDisconnect),
            status_code(UsageStatus::Completed),
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        self.inner.flush_once();
    }

    pub fn discard(&self) {
        self.inner.flushed.store(true, Ordering::Release);
    }

    pub fn record_plain_bytes_and_complete(&self, bytes: u64) {
        self.add_logical_bytes(bytes);
        self.add_wire_bytes(bytes);
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
    fn flush_once(&self) {
        if self.flushed.swap(true, Ordering::AcqRel) {
            return;
        }

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
            yield frame;
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
            yield item;
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
            yield item;
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
            yield item;
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
    use crate::commercial::UsageReporter;

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
}
