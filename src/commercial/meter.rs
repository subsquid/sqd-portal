use std::{
    io::{self, Write as IoWrite},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering},
        Arc, OnceLock,
    },
    time::{Duration, Instant as StdInstant, SystemTime, UNIX_EPOCH},
};

use async_stream::stream;
use bytes::Bytes;
use flate2::{write::MultiGzDecoder as WriteMultiGzDecoder, Decompress, FlushDecompress, Status};
use futures::{Stream, StreamExt};
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::Instant as TokioInstant;
use uuid::{NoContext, Timestamp, Uuid};

use crate::commercial::{concurrency::ConcurrencyPermit, tally::TallyHandle};
use crate::{
    commercial::zero_limits,
    commercial::{
        registry::StreamRegistration, ActiveStreamRegistry, DataSource, Endpoint, Granted,
        GrantedLimits, KeyStatus, OnExceed, Principal, SnapshotStore, StreamUsageEvent, TallyStore,
        UsageReporter, UsageStatus,
    },
    metrics,
    types::Compression,
};

const PENDING_COMPRESSED_WARNING_BYTES: usize = 8 * 1024 * 1024;
// The 64 MiB pending compressed cap is a worker-misbehavior guard; normal
// gzip/zstd members should complete and drain well before reaching it.
const MAX_PENDING_COMPRESSED_BYTES: usize = 64 * 1024 * 1024;
const STREAMING_DECODE_BUFFER_BYTES: usize = 32 * 1024;
const DECODE_YIELD_ITERATIONS: usize = 64;
const OUTPUT_ONLY_DECODE_STEPS_PER_CALL: usize = 16;

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
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_enforced(
        granted: Granted,
        request_id: String,
        endpoint: Endpoint,
        dataset: String,
        reporter: Arc<dyn UsageReporter>,
        tally: Arc<TallyStore>,
        registry: Arc<ActiveStreamRegistry>,
        snapshot_store: Option<Arc<SnapshotStore>>,
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
            snapshot_store,
            concurrency_permit,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn from_parts(
        mut principal: Principal,
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
        snapshot_store: Option<Arc<SnapshotStore>>,
        concurrency_permit: Option<ConcurrencyPermit>,
    ) -> Self {
        let event_id = uuid_v7();
        let request_id = non_empty_or_uuid(request_id);
        principal.account_id = non_empty_or_unknown(principal.account_id, "account");
        principal.api_key_id = principal.api_key_id.and_then(non_empty_option);
        let tally_account_id = tally_account_id.unwrap_or_else(|| principal.account_id.clone());
        if limits.throughput_bytes_per_sec == Some(0) {
            zero_limits::report(
                principal
                    .api_key_id
                    .as_deref()
                    .unwrap_or(principal.account_id.as_str()),
                "throughput_bytes_per_sec",
            );
        }
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
        if let (Some(store), Some(key_id)) = (&snapshot_store, principal.api_key_id.as_deref()) {
            let snapshot = store.get(key_id);
            if store.is_ready()
                && snapshot
                    .as_ref()
                    .is_none_or(|snapshot| snapshot.status != KeyStatus::Active)
            {
                kill.store(true, Ordering::Release);
                tracing::info!(
                    key_id,
                    "commercial key became missing or non-active before meter registration completed"
                );
            }
        }
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

    /// Records an exact block count when an endpoint has one without parsing metered bytes.
    pub fn add_blocks(&self, blocks: u64) {
        self.inner.blocks.fetch_add(blocks, Ordering::Relaxed);
    }

    pub fn add_chunk(&self) {
        self.inner.chunks.fetch_add(1, Ordering::Relaxed);
    }

    pub fn mark_error(&self) {
        self.inner.mark_error();
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

    pub async fn complete_buffered_response(&self) {
        self.inner.observe_buffered_body_after_materialization();
        let bytes = self.inner.unpaced_logical.swap(0, Ordering::AcqRel);
        if bytes > 0 {
            self.inner.pace_buffered(bytes).await;
        }
        self.complete();
    }

    pub async fn record_plain_bytes_and_complete(&self, bytes: u64) {
        self.add_logical_bytes(bytes);
        self.add_wire_bytes(bytes);
        self.complete_buffered_response().await;
    }

    pub async fn record_gzip_body_and_complete(&self, bytes: &[u8]) -> anyhow::Result<()> {
        self.add_chunk();
        self.add_wire_bytes(bytes.len() as u64);
        self.add_logical_bytes(gzip_len(bytes)?);
        self.complete_buffered_response().await;
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
        // Admission folds effective remaining quota into max_response_bytes. With
        // multiple concurrent streams, quota exhaustion can therefore surface as
        // CutMaxBytes here instead of CutQuota; that attribution trade-off is
        // accepted because enforcement still stops on a complete chunk boundary.
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

    fn observe_buffered_body_after_materialization(&self) {
        let effective_remaining = self.flush_pending_tally();
        if self.kill.load(Ordering::Acquire) {
            self.set_status(UsageStatus::CutSuspended);
            return;
        }

        // Buffered endpoints already materialized the full response before
        // metering can inspect its size, so max_response_bytes remains an
        // admission-only guard here. Quota debiting and throttle pacing still
        // happen before returning the buffered response.
        let Some(effective_remaining) = effective_remaining else {
            return;
        };
        if effective_remaining > 0 {
            return;
        }

        match self.on_exceed {
            OnExceed::Reject => {
                self.set_status(UsageStatus::CutQuota);
            }
            OnExceed::Throttle {
                floor_bytes_per_sec,
            } => {
                self.floor_bytes_per_sec
                    .store(floor_bytes_per_sec, Ordering::Release);
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

    fn mark_error(&self) {
        let _ = self.status.compare_exchange(
            status_code(UsageStatus::ClientDisconnect),
            status_code(UsageStatus::Error),
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
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

    async fn pace_buffered(&self, bytes: u64) {
        let Some((rate, burst)) = self.current_rate_and_burst() else {
            return;
        };
        let wait = {
            let mut bucket = self.bucket.lock().await;
            let now = TokioInstant::now();
            let bucket = bucket.get_or_insert_with(|| TokenBucket {
                available: 0.0,
                burst,
                last: now,
            });
            if bucket.available > 0.0 {
                bucket.available = 0.0;
                bucket.last = now;
            }
            bucket.refill(rate, burst);
            bucket.available -= bytes as f64;
            if bucket.available >= 0.0 {
                return;
            }
            Duration::from_secs_f64(-bucket.available / rate)
        };
        metrics::observe_commercial_throttle_stall(wait);
        if wait > Duration::from_secs(30) {
            tracing::warn!(
                stall_seconds = wait.as_secs_f64(),
                "commercial throttle stalled a buffered response for more than 30 seconds"
            );
        }
        tokio::time::sleep(wait).await;
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
) -> Pin<Box<dyn Stream<Item = Vec<u8>> + Send>>
where
    S: Stream<Item = Vec<u8>> + Send + 'static,
{
    match compression {
        Compression::Gzip => Box::pin(tap_gzip_input_with_pending_limit(
            input,
            meter,
            MAX_PENDING_COMPRESSED_BYTES,
        )),
        Compression::Zstd => Box::pin(tap_zstd_input_with_pending_limit(
            input,
            meter,
            MAX_PENDING_COMPRESSED_BYTES,
        )),
    }
}

pub fn tap_input_chunks<S>(
    input: S,
    meter: MeterHandle,
) -> Pin<Box<dyn Stream<Item = Vec<u8>> + Send>>
where
    S: Stream<Item = Vec<u8>> + Send + 'static,
{
    Box::pin(tap_gzip_input_with_pending_limit(
        input,
        meter,
        MAX_PENDING_COMPRESSED_BYTES,
    ))
}

fn tap_gzip_input_with_pending_limit<S>(
    input: S,
    meter: MeterHandle,
    pending_limit: usize,
) -> impl Stream<Item = Vec<u8>> + Send
where
    S: Stream<Item = Vec<u8>> + Send + 'static,
{
    // DESIGN DEVIATION: this is not the "free tap" from the original design.
    // We accept a second gzip inflate so member-boundary cutoffs stay clean and
    // logical-byte counts do not trust workers; Phase-1 CPU review covers gzip.
    stream! {
        let mut decoder = GzipMemberCounter::new();
        let mut pending = PendingFrames::default();
        let mut passthrough = false;
        let mut decode_iterations = 0usize;
        futures::pin_mut!(input);
        'input: while let Some(frame) = input.next().await {
            meter.add_chunk();
            if passthrough {
                yield frame;
                continue;
            }

            let mut offset = 0;
            while offset < frame.len() {
                decode_iterations += 1;
                if decode_iterations >= DECODE_YIELD_ITERATIONS {
                    decode_iterations = 0;
                    tokio::task::yield_now().await;
                }
                match decoder.decode(&frame[offset..]) {
                    Ok(CompressedStep::Consumed { bytes }) => {
                        if bytes == 0 {
                            tracing::warn!("gzip usage decoder consumed no input");
                            meter.mark_error();
                            for frame in pending.drain() {
                                yield frame;
                            }
                            if offset < frame.len() {
                                yield frame[offset..].to_vec();
                            }
                            passthrough = true;
                            continue 'input;
                        }
                        if bytes > 0 {
                            let segment = frame[offset..offset + bytes].to_vec();
                            offset += bytes;
                            if !pending.try_push(segment, pending_limit, "gzip") {
                                meter.mark_error();
                                for frame in pending.drain() {
                                    yield frame;
                                }
                                break 'input;
                            }
                        }
                    }
                    Ok(CompressedStep::Completed {
                        consumed,
                        logical_bytes,
                    }) => {
                        if consumed > 0 {
                            let segment = frame[offset..offset + consumed].to_vec();
                            offset += consumed;
                            if !pending.try_push(segment, pending_limit, "gzip") {
                                meter.mark_error();
                                for frame in pending.drain() {
                                    yield frame;
                                }
                                break 'input;
                            }
                        }
                        meter.add_logical_bytes(logical_bytes);
                        let stop = meter.should_stop_after_chunk();
                        meter.pace_pending_logical().await;
                        for frame in pending.drain() {
                            yield frame;
                        }
                        if stop {
                            break 'input;
                        }
                    }
                    Ok(CompressedStep::NeedsMoreInput) => break,
                    Err(err) => {
                        tracing::warn!(error = %err, "failed to decode portal usage gzip input for logical byte count");
                        meter.mark_error();
                        for frame in pending.drain() {
                            yield frame;
                        }
                        if offset < frame.len() {
                            yield frame[offset..].to_vec();
                        }
                        passthrough = true;
                        continue 'input;
                    }
                }
            }
        }
        if !pending.is_empty() {
            meter.mark_error();
            for frame in pending.drain() {
                yield frame;
            }
        }
    }
}

fn tap_zstd_input_with_pending_limit<S>(
    input: S,
    meter: MeterHandle,
    pending_limit: usize,
) -> impl Stream<Item = Vec<u8>> + Send
where
    S: Stream<Item = Vec<u8>> + Send + 'static,
{
    stream! {
        let mut decoder = match ZstdFrameCounter::new() {
            Ok(decoder) => decoder,
            Err(err) => {
                tracing::warn!(error = %err, "failed to initialize portal usage zstd decoder");
                meter.mark_error();
                futures::pin_mut!(input);
                while let Some(frame) = input.next().await {
                    meter.add_chunk();
                    yield frame;
                }
                return;
            }
        };
        let mut pending = PendingFrames::default();
        let mut passthrough = false;
        let mut stopped_after_cut = false;
        let mut decode_iterations = 0usize;
        futures::pin_mut!(input);
        'input: while let Some(frame) = input.next().await {
            meter.add_chunk();
            if passthrough {
                yield frame;
                continue;
            }

            let mut offset = 0;
            while offset < frame.len() {
                let buffered = pending.bytes().saturating_add(decoder.held_len());
                pending.warn_if_above_threshold("zstd", decoder.held_len(), pending_limit);
                if buffered >= pending_limit {
                    meter.mark_error();
                    for frame in pending.drain() {
                        yield frame;
                    }
                    if let Some(frame) = decoder.drain_held() {
                        yield frame;
                    }
                    break 'input;
                }

                let capacity = pending_limit - buffered;
                let take = capacity.min(frame.len() - offset);
                decoder.push_input(&frame[offset..offset + take]);
                offset += take;

                loop {
                    decode_iterations += 1;
                    if decode_iterations >= DECODE_YIELD_ITERATIONS {
                        decode_iterations = 0;
                        tokio::task::yield_now().await;
                    }
                    match decoder.decode() {
                        Ok(ZstdStep::Consumed { bytes, completed }) => {
                            if !bytes.is_empty() && !pending.try_push(bytes, pending_limit, "zstd") {
                                meter.mark_error();
                                for frame in pending.drain() {
                                    yield frame;
                                }
                                if let Some(frame) = decoder.drain_held() {
                                    yield frame;
                                }
                                break 'input;
                            }

                            if completed {
                                meter.add_logical_bytes(decoder.take_completed_logical());
                                let stop = meter.should_stop_after_chunk();
                                meter.pace_pending_logical().await;
                                for frame in pending.drain() {
                                    yield frame;
                                }
                                if stop {
                                    decoder.discard_held();
                                    stopped_after_cut = true;
                                    break 'input;
                                }
                            }
                        }
                        Ok(ZstdStep::NeedsMoreInput) => break,
                        Err(err) => {
                            tracing::warn!(error = %err, "failed to decode portal usage zstd input for logical byte count");
                            meter.mark_error();
                            for frame in pending.drain() {
                                yield frame;
                            }
                            if let Some(frame) = decoder.drain_held() {
                                yield frame;
                            }
                            passthrough = true;
                            continue 'input;
                        }
                    }
                }
            }
        }
        if !stopped_after_cut && (!pending.is_empty() || decoder.held_len() > 0) {
            meter.mark_error();
            for frame in pending.drain() {
                yield frame;
            }
            if let Some(frame) = decoder.drain_held() {
                yield frame;
            }
        }
    }
}

#[derive(Default)]
struct PendingFrames {
    frames: Vec<Vec<u8>>,
    bytes: usize,
    warned_above_threshold: bool,
}

impl PendingFrames {
    fn try_push(&mut self, frame: Vec<u8>, limit: usize, compression: &'static str) -> bool {
        let Some(next_bytes) = self.bytes.checked_add(frame.len()) else {
            return false;
        };
        if next_bytes > limit {
            return false;
        }
        self.bytes = next_bytes;
        self.frames.push(frame);
        self.warn_if_above_threshold(compression, 0, limit);
        true
    }

    fn warn_if_above_threshold(
        &mut self,
        compression: &'static str,
        extra_bytes: usize,
        limit: usize,
    ) {
        let buffered = self.bytes.saturating_add(extra_bytes);
        if self.warned_above_threshold || buffered <= PENDING_COMPRESSED_WARNING_BYTES {
            return;
        }
        self.warned_above_threshold = true;
        tracing::warn!(
            compression,
            pending_compressed_bytes = buffered,
            warning_bytes = PENDING_COMPRESSED_WARNING_BYTES,
            cap_bytes = limit,
            "commercial pending compressed response buffer exceeded early warning threshold"
        );
        metrics::report_commercial_pending_compressed_buffer_warning(compression);
    }

    fn drain(&mut self) -> impl Iterator<Item = Vec<u8>> + '_ {
        self.bytes = 0;
        self.frames.drain(..)
    }

    fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    fn bytes(&self) -> usize {
        self.bytes
    }
}

#[derive(Default)]
struct CountingWriter {
    bytes: u64,
}

impl IoWrite for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes = self.bytes.saturating_add(buf.len() as u64);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

enum CompressedStep {
    Consumed { bytes: usize },
    Completed { consumed: usize, logical_bytes: u64 },
    NeedsMoreInput,
}

struct GzipMemberCounter {
    state: GzipDecodeState,
    decoder: Decompress,
    output: Vec<u8>,
    current_logical: u64,
}

impl GzipMemberCounter {
    fn new() -> Self {
        Self {
            state: GzipDecodeState::Header(GzipHeader::new()),
            decoder: Decompress::new(false),
            output: vec![0; STREAMING_DECODE_BUFFER_BYTES],
            current_logical: 0,
        }
    }

    fn decode(&mut self, input: &[u8]) -> io::Result<CompressedStep> {
        if input.is_empty() {
            return Ok(CompressedStep::NeedsMoreInput);
        }

        let mut offset = 0;
        loop {
            match &mut self.state {
                GzipDecodeState::Header(header) => {
                    while offset < input.len() {
                        let done = header.consume(input[offset])?;
                        offset += 1;
                        if done {
                            self.state = GzipDecodeState::Deflate;
                            break;
                        }
                    }
                    if offset == input.len() {
                        return Ok(CompressedStep::Consumed { bytes: offset });
                    }
                }
                GzipDecodeState::Deflate => {
                    let before_in = self.decoder.total_in();
                    let before_out = self.decoder.total_out();
                    let status = self
                        .decoder
                        .decompress(&input[offset..], &mut self.output, FlushDecompress::None)
                        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
                    let consumed = (self.decoder.total_in() - before_in) as usize;
                    let written = (self.decoder.total_out() - before_out) as usize;
                    offset += consumed;
                    self.current_logical = self.current_logical.saturating_add(written as u64);
                    if status == Status::StreamEnd {
                        self.state = GzipDecodeState::Trailer(8);
                        if offset == input.len() {
                            return Ok(CompressedStep::Consumed { bytes: offset });
                        }
                        continue;
                    }
                    return Ok(CompressedStep::Consumed { bytes: offset });
                }
                GzipDecodeState::Trailer(remaining) => {
                    let consumed = (*remaining).min(input.len() - offset);
                    *remaining -= consumed;
                    offset += consumed;
                    if *remaining == 0 {
                        let logical_bytes = std::mem::take(&mut self.current_logical);
                        self.state = GzipDecodeState::Header(GzipHeader::new());
                        self.decoder = Decompress::new(false);
                        return Ok(CompressedStep::Completed {
                            consumed: offset,
                            logical_bytes,
                        });
                    }
                    return Ok(CompressedStep::Consumed { bytes: offset });
                }
            }
        }
    }
}

enum GzipDecodeState {
    Header(GzipHeader),
    Deflate,
    Trailer(usize),
}

struct GzipHeader {
    phase: GzipHeaderPhase,
}

enum GzipHeaderPhase {
    Fixed {
        pos: usize,
        bytes: [u8; 10],
    },
    ExtraLen {
        flags: u8,
        pos: usize,
        bytes: [u8; 2],
    },
    Extra {
        flags: u8,
        remaining: usize,
    },
    FileName {
        flags: u8,
    },
    Comment {
        flags: u8,
    },
    HeaderCrc {
        remaining: usize,
    },
    Done,
}

impl GzipHeader {
    fn new() -> Self {
        Self {
            phase: GzipHeaderPhase::Fixed {
                pos: 0,
                bytes: [0; 10],
            },
        }
    }

    fn consume(&mut self, byte: u8) -> io::Result<bool> {
        match &mut self.phase {
            GzipHeaderPhase::Fixed { pos, bytes } => {
                bytes[*pos] = byte;
                *pos += 1;
                if *pos == bytes.len() {
                    if bytes[0] != 0x1f || bytes[1] != 0x8b || bytes[2] != 8 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid gzip header",
                        ));
                    }
                    let flags = bytes[3];
                    if flags & 0b1110_0000 != 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid gzip flags",
                        ));
                    }
                    self.phase = phase_after_fixed(flags);
                }
            }
            GzipHeaderPhase::ExtraLen { flags, pos, bytes } => {
                bytes[*pos] = byte;
                *pos += 1;
                if *pos == bytes.len() {
                    let remaining = u16::from_le_bytes(*bytes) as usize;
                    self.phase = if remaining == 0 {
                        phase_after_extra(*flags)
                    } else {
                        GzipHeaderPhase::Extra {
                            flags: *flags,
                            remaining,
                        }
                    };
                }
            }
            GzipHeaderPhase::Extra { flags, remaining } => {
                *remaining -= 1;
                if *remaining == 0 {
                    self.phase = phase_after_extra(*flags);
                }
            }
            GzipHeaderPhase::FileName { flags } => {
                if byte == 0 {
                    self.phase = phase_after_name(*flags);
                }
            }
            GzipHeaderPhase::Comment { flags } => {
                if byte == 0 {
                    self.phase = phase_after_comment(*flags);
                }
            }
            GzipHeaderPhase::HeaderCrc { remaining } => {
                *remaining -= 1;
                if *remaining == 0 {
                    self.phase = GzipHeaderPhase::Done;
                }
            }
            GzipHeaderPhase::Done => {}
        }

        Ok(matches!(self.phase, GzipHeaderPhase::Done))
    }
}

fn phase_after_fixed(flags: u8) -> GzipHeaderPhase {
    if flags & 0x04 != 0 {
        GzipHeaderPhase::ExtraLen {
            flags,
            pos: 0,
            bytes: [0; 2],
        }
    } else {
        phase_after_extra(flags)
    }
}

fn phase_after_extra(flags: u8) -> GzipHeaderPhase {
    if flags & 0x08 != 0 {
        GzipHeaderPhase::FileName { flags }
    } else {
        phase_after_name(flags)
    }
}

fn phase_after_name(flags: u8) -> GzipHeaderPhase {
    if flags & 0x10 != 0 {
        GzipHeaderPhase::Comment { flags }
    } else {
        phase_after_comment(flags)
    }
}

fn phase_after_comment(flags: u8) -> GzipHeaderPhase {
    if flags & 0x02 != 0 {
        GzipHeaderPhase::HeaderCrc { remaining: 2 }
    } else {
        GzipHeaderPhase::Done
    }
}

enum ZstdStep {
    Consumed { bytes: Vec<u8>, completed: bool },
    NeedsMoreInput,
}

struct ZstdFrameCounter {
    decoder: zstd::stream::raw::Decoder<'static>,
    output: Vec<u8>,
    current_logical: u64,
    held_input: Vec<u8>,
}

impl ZstdFrameCounter {
    fn new() -> io::Result<Self> {
        Ok(Self {
            decoder: zstd::stream::raw::Decoder::new()?,
            output: vec![0; STREAMING_DECODE_BUFFER_BYTES],
            current_logical: 0,
            held_input: Vec::new(),
        })
    }

    fn push_input(&mut self, input: &[u8]) {
        self.held_input.extend_from_slice(input);
    }

    fn held_len(&self) -> usize {
        self.held_input.len()
    }

    fn drain_held(&mut self) -> Option<Vec<u8>> {
        if self.held_input.is_empty() {
            None
        } else {
            Some(std::mem::take(&mut self.held_input))
        }
    }

    fn discard_held(&mut self) {
        self.held_input.clear();
    }

    fn decode(&mut self) -> io::Result<ZstdStep> {
        use zstd::stream::raw::Operation as _;

        if self.held_input.is_empty() {
            return Ok(ZstdStep::NeedsMoreInput);
        }

        let mut output_only_steps = 0;
        loop {
            let status = self
                .decoder
                .run_on_buffers(&self.held_input, &mut self.output)?;
            self.current_logical = self
                .current_logical
                .saturating_add(status.bytes_written as u64);
            let completed = status.remaining == 0;

            if status.bytes_read > 0 {
                let bytes = self.held_input.drain(..status.bytes_read).collect();
                if completed {
                    self.decoder.reinit()?;
                }
                return Ok(ZstdStep::Consumed { bytes, completed });
            }

            if completed {
                self.decoder.reinit()?;
                return Ok(ZstdStep::Consumed {
                    bytes: Vec::new(),
                    completed: true,
                });
            }

            if status.bytes_written > 0 {
                output_only_steps += 1;
                if output_only_steps >= OUTPUT_ONLY_DECODE_STEPS_PER_CALL {
                    return Ok(ZstdStep::Consumed {
                        bytes: Vec::new(),
                        completed: false,
                    });
                }
                continue;
            }

            if status.remaining > self.held_input.len() {
                return Ok(ZstdStep::NeedsMoreInput);
            }

            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "zstd decoder made no progress",
            ));
        }
    }

    fn take_completed_logical(&mut self) -> u64 {
        std::mem::take(&mut self.current_logical)
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

pub fn tap_wire_stream_without_cut<S, E>(
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
            meter.pace_pending_logical().await;
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
        // Hotblocks currently proxies opaque gzip bytes. Be tolerant of
        // concatenated members here, matching the network gzip path.
        let mut decoder = WriteMultiGzDecoder::new(CountingWriter::default());
        futures::pin_mut!(input);
        while let Some(item) = input.next().await {
            match &item {
                Ok(bytes) => {
                    meter.add_wire_bytes(bytes.len() as u64);
                    let before = decoder.get_ref().bytes;
                    if let Err(err) = decoder.write_all(bytes) {
                        tracing::warn!(error = %err, "failed to decode gzip stream for logical byte count");
                        meter.mark_error();
                    }
                    add_gzip_decoder_delta(&meter, &decoder, before);
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
        let before = decoder.get_ref().bytes;
        if let Err(err) = decoder.try_finish() {
            tracing::warn!(error = %err, "failed to finish gzip stream usage decoder");
            meter.mark_error();
        }
        add_gzip_decoder_delta(&meter, &decoder, before);
        meter.complete();
    }
}

fn add_gzip_decoder_delta(
    meter: &MeterHandle,
    decoder: &WriteMultiGzDecoder<CountingWriter>,
    before: u64,
) {
    let after = decoder.get_ref().bytes;
    if after > before {
        meter.add_logical_bytes(after - before);
    }
}

fn gzip_len(bytes: &[u8]) -> anyhow::Result<u64> {
    let mut decoder = WriteMultiGzDecoder::new(CountingWriter::default());
    decoder.write_all(bytes)?;
    decoder.try_finish()?;
    Ok(decoder.get_ref().bytes)
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

fn non_empty_or_uuid(value: String) -> String {
    if value.trim().is_empty() {
        Uuid::new_v4().to_string()
    } else {
        value
    }
}

fn non_empty_or_unknown(value: String, field: &str) -> String {
    if value.trim().is_empty() {
        format!("unknown-{field}-{}", Uuid::new_v4())
    } else {
        value
    }
}

fn non_empty_option(value: String) -> Option<String> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
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
    use flate2::{
        write::{DeflateEncoder, GzEncoder},
        Compression as FlateCompression, Crc,
    };
    use futures::stream;
    use std::io::Write;
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

    fn zero_limit_metric(limit: &str) -> u64 {
        let labels = vec![("limit".to_owned(), limit.to_owned())];
        crate::metrics::COMMERCIAL_ZERO_VALUED_LIMITS
            .get_or_create(&labels)
            .get()
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
            entitled_chains: None,
            entitled_traces: None,
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
                None,
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
            None,
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

    async fn drive_metered_gzip_chunks_with_limit(
        frames: Vec<Vec<u8>>,
        pending_limit: usize,
    ) -> (StreamUsageEvent, Vec<u8>) {
        let reporter = Arc::new(RecordingReporter::default());
        let meter = meter(reporter.clone());
        let input =
            tap_gzip_input_with_pending_limit(stream::iter(frames), meter.clone(), pending_limit);
        let output = tap_wire_stream(
            input.map(|frame| Ok::<_, std::io::Error>(Bytes::from(frame))),
            meter,
        );
        let emitted = output
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(Result::unwrap)
            .flat_map(|bytes| bytes.to_vec())
            .collect::<Vec<_>>();

        let event = reporter.events.lock().unwrap().pop().unwrap();
        (event, emitted)
    }

    async fn drive_metered_zstd_frames_with_limit(
        frames: Vec<Vec<u8>>,
        pending_limit: usize,
    ) -> (StreamUsageEvent, Vec<u8>) {
        let reporter = Arc::new(RecordingReporter::default());
        let meter = meter(reporter.clone());
        let input =
            tap_zstd_input_with_pending_limit(stream::iter(frames), meter.clone(), pending_limit);
        let output = tap_wire_stream(
            input.map(|frame| Ok::<_, std::io::Error>(Bytes::from(frame))),
            meter,
        );
        let emitted = output
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(Result::unwrap)
            .flat_map(|bytes| bytes.to_vec())
            .collect::<Vec<_>>();

        let event = reporter.events.lock().unwrap().pop().unwrap();
        (event, emitted)
    }

    fn split_every(bytes: &[u8], chunk: usize) -> Vec<Vec<u8>> {
        bytes.chunks(chunk).map(<[u8]>::to_vec).collect()
    }

    fn raw_deflate(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = DeflateEncoder::new(Vec::new(), FlateCompression::fast());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    fn crc32(bytes: &[u8]) -> u32 {
        let mut crc = Crc::new();
        crc.update(bytes);
        crc.sum()
    }

    #[derive(Default)]
    struct GzipHeaderFixture<'a> {
        extra: Option<&'a [u8]>,
        name: Option<&'a [u8]>,
        comment: Option<&'a [u8]>,
        header_crc: bool,
    }

    fn gzip_fixture_member(payload: &[u8], fixture: GzipHeaderFixture<'_>) -> Vec<u8> {
        let mut flags = 0;
        if fixture.extra.is_some() {
            flags |= 0x04;
        }
        if fixture.name.is_some() {
            flags |= 0x08;
        }
        if fixture.comment.is_some() {
            flags |= 0x10;
        }
        if fixture.header_crc {
            flags |= 0x02;
        }

        let mut header = vec![0x1f, 0x8b, 8, flags, 0, 0, 0, 0, 0, 255];
        if let Some(extra) = fixture.extra {
            header.extend_from_slice(&(extra.len() as u16).to_le_bytes());
            header.extend_from_slice(extra);
        }
        if let Some(name) = fixture.name {
            header.extend_from_slice(name);
            header.push(0);
        }
        if let Some(comment) = fixture.comment {
            header.extend_from_slice(comment);
            header.push(0);
        }
        if fixture.header_crc {
            let crc16 = crc32(&header) as u16;
            header.extend_from_slice(&crc16.to_le_bytes());
        }

        let mut member = header;
        member.extend_from_slice(&raw_deflate(payload));
        member.extend_from_slice(&crc32(payload).to_le_bytes());
        member.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        member
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
    async fn gzip_input_tap_counts_many_fragments_without_decoded_buffer() {
        let data = vec![b'x'; 128 * 1024];
        let encoded = gzip(&data).await;
        let fragments = split_every(&encoded, 3);
        let wire = encoded.len() as u64;
        let chunks = fragments.len() as u64;

        let (event, emitted) =
            drive_metered_gzip_chunks_with_limit(fragments, MAX_PENDING_COMPRESSED_BYTES).await;

        assert_eq!(emitted, encoded);
        assert_eq!(event.logical_bytes, data.len() as u64);
        assert_eq!(event.wire_bytes, wire);
        assert_eq!(event.chunks, chunks);
        assert_eq!(event.status, UsageStatus::Completed);
    }

    #[tokio::test]
    async fn zstd_input_tap_counts_many_fragments_without_decoded_buffer() {
        let data = vec![b'z'; 128 * 1024];
        let encoded = zstd::stream::encode_all(&data[..], 0).unwrap();
        let fragments = split_every(&encoded, 3);
        let wire = encoded.len() as u64;
        let chunks = fragments.len() as u64;

        let (event, emitted) =
            drive_metered_zstd_frames_with_limit(fragments, MAX_PENDING_COMPRESSED_BYTES).await;

        assert_eq!(emitted, encoded);
        assert_eq!(event.logical_bytes, data.len() as u64);
        assert_eq!(event.wire_bytes, wire);
        assert_eq!(event.chunks, chunks);
        assert_eq!(event.status, UsageStatus::Completed);
    }

    #[tokio::test]
    async fn gzip_input_tap_marks_error_when_pending_cap_is_hit() {
        let data = vec![b'g'; 1024];
        let encoded = gzip(&data).await;
        let fragments = split_every(&encoded, 5);

        let (event, emitted) = drive_metered_gzip_chunks_with_limit(fragments, 10).await;

        assert_eq!(emitted, encoded[..10]);
        assert_eq!(event.wire_bytes, emitted.len() as u64);
        assert_eq!(event.status, UsageStatus::Error);
    }

    #[tokio::test]
    async fn zstd_input_tap_marks_error_when_pending_cap_is_hit() {
        let data = vec![b'z'; 1024];
        let encoded = zstd::stream::encode_all(&data[..], 0).unwrap();
        let fragments = split_every(&encoded, 5);

        let (event, emitted) = drive_metered_zstd_frames_with_limit(fragments, 10).await;

        assert_eq!(emitted, encoded[..10]);
        assert_eq!(event.wire_bytes, emitted.len() as u64);
        assert_eq!(event.status, UsageStatus::Error);
    }

    #[test]
    fn pending_frames_warns_once_after_early_threshold() {
        let labels = vec![("compression".to_owned(), "gzip".to_owned())];
        let counter =
            crate::metrics::COMMERCIAL_PENDING_COMPRESSED_BUFFER_WARNINGS.get_or_create(&labels);
        let before = counter.get();
        let mut pending = PendingFrames::default();

        assert!(pending.try_push(
            vec![0; PENDING_COMPRESSED_WARNING_BYTES],
            MAX_PENDING_COMPRESSED_BYTES,
            "gzip",
        ));
        assert_eq!(counter.get(), before);

        assert!(pending.try_push(vec![0], MAX_PENDING_COMPRESSED_BYTES, "gzip"));
        assert_eq!(counter.get(), before + 1);

        assert!(pending.try_push(vec![0], MAX_PENDING_COMPRESSED_BYTES, "gzip"));
        assert_eq!(counter.get(), before + 1);
    }

    #[tokio::test]
    async fn gzip_input_tap_counts_multi_member_with_split_trailer_and_next_header() {
        let first_payload = b"first-member\n";
        let second_payload = b"second-member\n";
        let first = gzip_fixture_member(first_payload, GzipHeaderFixture::default());
        let second = gzip_fixture_member(second_payload, GzipHeaderFixture::default());
        let mut encoded = first.clone();
        encoded.extend_from_slice(&second);
        let split = first.len() - 4;
        let frames = vec![
            encoded[..split].to_vec(),
            encoded[split..split + 7].to_vec(),
            encoded[split + 7..].to_vec(),
        ];

        let (event, emitted) =
            drive_metered_gzip_chunks_with_limit(frames, MAX_PENDING_COMPRESSED_BYTES).await;

        assert_eq!(emitted, encoded);
        assert_eq!(
            event.logical_bytes,
            (first_payload.len() + second_payload.len()) as u64
        );
        assert_eq!(event.status, UsageStatus::Completed);
    }

    #[tokio::test]
    async fn gzip_input_tap_counts_fname_header_spanning_frames() {
        let payload = b"fname-payload";
        let encoded = gzip_fixture_member(
            payload,
            GzipHeaderFixture {
                name: Some(b"name-spans-frames.json"),
                ..GzipHeaderFixture::default()
            },
        );
        let frames = vec![
            encoded[..12].to_vec(),
            encoded[12..17].to_vec(),
            encoded[17..].to_vec(),
        ];

        let (event, emitted) =
            drive_metered_gzip_chunks_with_limit(frames, MAX_PENDING_COMPRESSED_BYTES).await;

        assert_eq!(emitted, encoded);
        assert_eq!(event.logical_bytes, payload.len() as u64);
        assert_eq!(event.status, UsageStatus::Completed);
    }

    #[tokio::test]
    async fn gzip_input_tap_counts_fextra_zero_and_spanning_payload_headers() {
        let first_payload = b"extra-empty";
        let second_payload = b"extra-payload";
        let first = gzip_fixture_member(
            first_payload,
            GzipHeaderFixture {
                extra: Some(b""),
                ..GzipHeaderFixture::default()
            },
        );
        let second = gzip_fixture_member(
            second_payload,
            GzipHeaderFixture {
                extra: Some(b"abcdef"),
                ..GzipHeaderFixture::default()
            },
        );
        let mut encoded = first.clone();
        encoded.extend_from_slice(&second);
        let split = first.len() + 13;
        let frames = vec![
            encoded[..split].to_vec(),
            encoded[split..split + 2].to_vec(),
            encoded[split + 2..].to_vec(),
        ];

        let (event, emitted) =
            drive_metered_gzip_chunks_with_limit(frames, MAX_PENDING_COMPRESSED_BYTES).await;

        assert_eq!(emitted, encoded);
        assert_eq!(
            event.logical_bytes,
            (first_payload.len() + second_payload.len()) as u64
        );
        assert_eq!(event.status, UsageStatus::Completed);
    }

    #[tokio::test]
    async fn gzip_input_tap_counts_fcomment_header() {
        let payload = b"commented";
        let encoded = gzip_fixture_member(
            payload,
            GzipHeaderFixture {
                comment: Some(b"comment-spans-frames"),
                ..GzipHeaderFixture::default()
            },
        );
        let frames = vec![
            encoded[..13].to_vec(),
            encoded[13..19].to_vec(),
            encoded[19..].to_vec(),
        ];

        let (event, emitted) =
            drive_metered_gzip_chunks_with_limit(frames, MAX_PENDING_COMPRESSED_BYTES).await;

        assert_eq!(emitted, encoded);
        assert_eq!(event.logical_bytes, payload.len() as u64);
        assert_eq!(event.status, UsageStatus::Completed);
    }

    #[tokio::test]
    async fn gzip_input_tap_counts_fhcrc_header() {
        let payload = b"fhcrc";
        let encoded = gzip_fixture_member(
            payload,
            GzipHeaderFixture {
                header_crc: true,
                ..GzipHeaderFixture::default()
            },
        );
        let frames = split_every(&encoded, 4);

        let (event, emitted) =
            drive_metered_gzip_chunks_with_limit(frames, MAX_PENDING_COMPRESSED_BYTES).await;

        assert_eq!(emitted, encoded);
        assert_eq!(event.logical_bytes, payload.len() as u64);
        assert_eq!(event.status, UsageStatus::Completed);
    }

    #[tokio::test]
    async fn gzip_input_tap_counts_combined_flags_header() {
        let payload = b"combined";
        let encoded = gzip_fixture_member(
            payload,
            GzipHeaderFixture {
                extra: Some(b"xy"),
                name: Some(b"combined-name"),
                comment: Some(b"combined-comment"),
                header_crc: true,
            },
        );
        let frames = split_every(&encoded, 3);

        let (event, emitted) =
            drive_metered_gzip_chunks_with_limit(frames, MAX_PENDING_COMPRESSED_BYTES).await;

        assert_eq!(emitted, encoded);
        assert_eq!(event.logical_bytes, payload.len() as u64);
        assert_eq!(event.status, UsageStatus::Completed);
    }

    #[tokio::test]
    async fn gzip_input_tap_reserved_flags_mark_error_and_passthrough_raw_bytes() {
        let raw = vec![
            0x1f, 0x8b, 8, 0xe0, 0, 0, 0, 0, 0, 255, b't', b'a', b'i', b'l',
        ];

        let (event, emitted) = drive_metered_gzip_chunks_with_limit(
            split_every(&raw, 3),
            MAX_PENDING_COMPRESSED_BYTES,
        )
        .await;

        assert_eq!(emitted, raw);
        assert_eq!(event.logical_bytes, 0);
        assert_eq!(event.wire_bytes, emitted.len() as u64);
        assert_eq!(event.status, UsageStatus::Error);
    }

    #[tokio::test]
    async fn gzip_input_tap_bad_magic_or_cm_mark_error_and_passthrough_raw_bytes() {
        for raw in [
            vec![0x00, 0x8b, 8, 0, 0, 0, 0, 0, 0, 255, b'x'],
            vec![0x1f, 0x8b, 9, 0, 0, 0, 0, 0, 0, 255, b'x'],
        ] {
            let (event, emitted) = drive_metered_gzip_chunks_with_limit(
                split_every(&raw, 3),
                MAX_PENDING_COMPRESSED_BYTES,
            )
            .await;

            assert_eq!(emitted, raw);
            assert_eq!(event.logical_bytes, 0);
            assert_eq!(event.wire_bytes, emitted.len() as u64);
            assert_eq!(event.status, UsageStatus::Error);
        }
    }

    #[tokio::test]
    async fn zstd_input_tap_counts_concatenated_frames_exact_total() {
        let first_payload = b"first-zstd-frame";
        let second_payload = b"second-zstd-frame";
        let first = zstd::stream::encode_all(&first_payload[..], 0).unwrap();
        let second = zstd::stream::encode_all(&second_payload[..], 0).unwrap();
        let mut encoded = first.clone();
        encoded.extend_from_slice(&second);
        let split = first.len() - 2;
        let frames = vec![encoded[..split].to_vec(), encoded[split..].to_vec()];

        let (event, emitted) =
            drive_metered_zstd_frames_with_limit(frames, MAX_PENDING_COMPRESSED_BYTES).await;

        assert_eq!(emitted, encoded);
        assert_eq!(
            event.logical_bytes,
            (first_payload.len() + second_payload.len()) as u64
        );
        assert_eq!(event.status, UsageStatus::Completed);
    }

    #[tokio::test]
    async fn compressed_input_taps_count_large_single_members() {
        let payload = vec![b'a'; 2 * 1024 * 1024];

        let mut gzip_encoder = GzEncoder::new(Vec::new(), FlateCompression::fast());
        gzip_encoder.write_all(&payload).unwrap();
        let gzip_encoded = gzip_encoder.finish().unwrap();
        let (gzip_event, gzip_emitted) = drive_metered_gzip_chunks_with_limit(
            vec![gzip_encoded.clone()],
            MAX_PENDING_COMPRESSED_BYTES,
        )
        .await;
        assert_eq!(gzip_emitted, gzip_encoded);
        assert_eq!(gzip_event.logical_bytes, payload.len() as u64);
        assert_eq!(gzip_event.status, UsageStatus::Completed);

        let zstd_encoded = zstd::stream::encode_all(&payload[..], 0).unwrap();
        let (zstd_event, zstd_emitted) = drive_metered_zstd_frames_with_limit(
            vec![zstd_encoded.clone()],
            MAX_PENDING_COMPRESSED_BYTES,
        )
        .await;
        assert_eq!(zstd_emitted, zstd_encoded);
        assert_eq!(zstd_event.logical_bytes, payload.len() as u64);
        assert_eq!(zstd_event.status, UsageStatus::Completed);
    }

    #[tokio::test]
    async fn zstd_input_tap_cut_discards_next_frame_prefix_and_keeps_cut_status() {
        let first_payload = b"first-frame";
        let second_payload = b"second-frame";
        let first = zstd::stream::encode_all(&first_payload[..], 0).unwrap();
        let second = zstd::stream::encode_all(&second_payload[..], 0).unwrap();
        let second_prefix_len = 4.min(second.len());
        let mut chunk = first.clone();
        chunk.extend_from_slice(&second[..second_prefix_len]);

        let reporter = Arc::new(RecordingReporter::default());
        let (meter, tally, _) = enforced_meter(
            reporter.clone(),
            GrantedLimits::default(),
            OnExceed::Reject,
            Some(first_payload.len() as i64),
        );
        let input = tap_zstd_input_with_pending_limit(
            stream::iter(vec![chunk]),
            meter.clone(),
            MAX_PENDING_COMPRESSED_BYTES,
        );
        let output = tap_wire_stream(
            input.map(|frame| Ok::<_, std::io::Error>(Bytes::from(frame))),
            meter,
        );
        let emitted = output
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(Result::unwrap)
            .flat_map(|bytes| bytes.to_vec())
            .collect::<Vec<_>>();

        assert_eq!(emitted, first);
        assert_eq!(
            zstd::stream::decode_all(&emitted[..]).unwrap(),
            first_payload
        );
        assert_eq!(tally.bytes_for("account", 7), first_payload.len() as u64);
        let event = reporter.events.lock().unwrap().pop().unwrap();
        assert_eq!(event.logical_bytes, first_payload.len() as u64);
        assert_eq!(event.wire_bytes, first.len() as u64);
        assert_eq!(event.status, UsageStatus::CutQuota);
    }

    #[tokio::test]
    async fn legacy_query_gzip_recording_counts_json_body() {
        let reporter = Arc::new(RecordingReporter::default());
        let meter = meter_for(reporter.clone(), Endpoint::LegacyQuery);
        let body = b"[{\"legacy\":true}]\n";
        let encoded = gzip(body).await;

        meter.record_gzip_body_and_complete(&encoded).await.unwrap();

        let event = reporter.events.lock().unwrap().pop().unwrap();
        assert_eq!(event.endpoint, Endpoint::LegacyQuery);
        assert_eq!(event.logical_bytes, body.len() as u64);
        assert_eq!(event.wire_bytes, encoded.len() as u64);
        assert_eq!(event.chunks, 1);
        assert_eq!(event.status, UsageStatus::Completed);
    }

    #[tokio::test(start_paused = true)]
    async fn buffered_plain_response_waits_for_token_bucket_before_completion() {
        let reporter = Arc::new(RecordingReporter::default());
        let (meter, _, _) = enforced_meter(
            reporter.clone(),
            GrantedLimits {
                throughput_bytes_per_sec: Some(10),
                burst_bytes: Some(10),
                ..GrantedLimits::default()
            },
            OnExceed::Reject,
            Some(1_000),
        );
        let start = tokio::time::Instant::now();

        meter.record_plain_bytes_and_complete(50).await;

        assert!(start.elapsed() >= Duration::from_secs(5));
        let event = reporter.events.lock().unwrap().pop().unwrap();
        assert_eq!(event.logical_bytes, 50);
        assert_eq!(event.wire_bytes, 50);
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
                entitled_chains: None,
                entitled_traces: None,
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
                entitled_chains: None,
                entitled_traces: None,
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
    async fn gzip_stream_tap_counts_concatenated_members() {
        let reporter = Arc::new(RecordingReporter::default());
        let meter = meter(reporter.clone());
        let first = b"{\"header\":{\"number\":1}}\n";
        let second = b"{\"header\":{\"number\":2}}\n";
        let mut encoded = gzip(first).await;
        encoded.extend_from_slice(&gzip(second).await);
        let split = encoded.len() / 2;
        let chunks = vec![
            Ok::<_, std::io::Error>(Bytes::copy_from_slice(&encoded[..split])),
            Ok::<_, std::io::Error>(Bytes::copy_from_slice(&encoded[split..])),
        ];

        let output = tap_gzip_stream(stream::iter(chunks), meter);
        let emitted = output
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(Result::unwrap)
            .flat_map(|bytes| bytes.to_vec())
            .collect::<Vec<_>>();
        let event = reporter.events.lock().unwrap().pop().unwrap();

        assert_eq!(emitted, encoded);
        assert_eq!(event.logical_bytes, (first.len() + second.len()) as u64);
        assert_eq!(event.wire_bytes, encoded.len() as u64);
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
    async fn null_or_zero_throughput_has_no_bucket_and_no_stall() {
        for limits in [
            GrantedLimits {
                throughput_bytes_per_sec: None,
                burst_bytes: Some(1),
                ..GrantedLimits::default()
            },
            GrantedLimits {
                throughput_bytes_per_sec: Some(0),
                burst_bytes: Some(0),
                ..GrantedLimits::default()
            },
        ] {
            let reporter = Arc::new(RecordingReporter::default());
            let (meter, _, _) = enforced_meter(reporter, limits, OnExceed::Reject, Some(100));
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
    }

    #[tokio::test(start_paused = true)]
    async fn zero_throughput_reports_metric_and_remains_unpaced() {
        let reporter = Arc::new(RecordingReporter::default());
        let before = zero_limit_metric("throughput_bytes_per_sec");
        let (meter, _, _) = enforced_meter(
            reporter,
            GrantedLimits {
                throughput_bytes_per_sec: Some(0),
                burst_bytes: Some(0),
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
        assert!(zero_limit_metric("throughput_bytes_per_sec") > before);
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
