use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use arc_swap::ArcSwap;
use dashmap::DashMap;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio::time::Instant as TokioInstant;
use tokio_util::sync::CancellationToken;

use super::{
    config::{CommercialConfig, PublicFallbackConfig},
    types::{
        Defaults, KeySnapshot, KeyStatus, OnExceed, PublicDefaults, PublicLimits, PublicQuota,
        SnapshotRecord,
    },
    ActiveStreamRegistry, TallyStore,
};
use crate::metrics;

const SNAPSHOT_PATH_SEGMENTS: [&str; 4] = ["internal", "portal", "v1", "snapshots"];
const RESOLVE_PATH_SEGMENTS: [&str; 4] = ["internal", "portal", "v1", "authorize"];
const DEFAULT_PAGE_LIMIT: u16 = 1000;

pub struct SnapshotStore {
    inner: Arc<SnapshotStoreInner>,
}

struct SnapshotStoreInner {
    records: ArcSwap<HashMap<String, Arc<KeySnapshot>>>,
    defaults: ArcSwap<Defaults>,
    cursor: AtomicU64,
    ready: AtomicBool,
    loaded_from_cache: AtomicBool,
    disk_cache_dirty: AtomicBool,
    last_sync_success: Mutex<Option<TokioInstant>>,
    cache_path: PathBuf,
    sync_interval: Duration,
    negative_cache_ttl: Duration,
    negative_cache: DashMap<String, Instant>,
    resolve_limiter: Mutex<ResolveLimiter>,
    client: reqwest::Client,
    control_plane_url: url::Url,
    service_token: String,
    hooks: SnapshotHooks,
}

#[derive(Clone, Default)]
pub struct SnapshotHooks {
    pub tally: Option<Arc<TallyStore>>,
    pub registry: Option<Arc<ActiveStreamRegistry>>,
}

#[derive(Debug, Clone)]
pub struct StoreView {
    pub records: Arc<HashMap<String, Arc<KeySnapshot>>>,
    pub defaults: Arc<Defaults>,
    pub cursor: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotPage {
    records: Vec<SnapshotRecord>,
    next_cursor: u64,
    #[serde(default)]
    reset: bool,
    #[serde(skip)]
    raw_record_count: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct RawSnapshotPage {
    records: Vec<serde_json::Value>,
    next_cursor: u64,
    #[serde(default)]
    reset: bool,
}

#[derive(Debug, Serialize)]
struct ResolveRequest<'a> {
    key_id: &'a str,
}

#[derive(Debug, Serialize, Deserialize)]
struct DiskCache {
    cursor: u64,
    records: Vec<serde_json::Value>,
    defaults: Defaults,
    saved_at: u64,
}

struct ResolveLimiter {
    rate_per_sec: f64,
    capacity: f64,
    tokens: f64,
    last: Instant,
}

impl SnapshotStore {
    pub fn spawn(
        config: &CommercialConfig,
        cancel: CancellationToken,
    ) -> anyhow::Result<(Arc<Self>, JoinHandle<()>)> {
        Self::spawn_with_hooks(config, cancel, SnapshotHooks::default())
    }

    pub fn spawn_with_hooks(
        config: &CommercialConfig,
        cancel: CancellationToken,
        hooks: SnapshotHooks,
    ) -> anyhow::Result<(Arc<Self>, JoinHandle<()>)> {
        let service_token = std::env::var(&config.service_token_env).unwrap_or_default();
        let sync_task_start = TokioInstant::now();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;
        let store = Self::new(config, hooks, service_token, client, sync_task_start);
        store.load_disk_cache();

        let task_store = store.clone();
        let task = tokio::spawn(async move {
            task_store.run_sync_loop(cancel).await;
        });
        Ok((store, task))
    }

    fn new(
        config: &CommercialConfig,
        hooks: SnapshotHooks,
        service_token: String,
        client: reqwest::Client,
        sync_task_start: TokioInstant,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(SnapshotStoreInner {
                records: ArcSwap::from_pointee(HashMap::new()),
                defaults: ArcSwap::from_pointee(defaults_from_fallback(&config.public_fallback)),
                cursor: AtomicU64::new(0),
                ready: AtomicBool::new(false),
                loaded_from_cache: AtomicBool::new(false),
                disk_cache_dirty: AtomicBool::new(false),
                last_sync_success: Mutex::new(Some(sync_task_start)),
                cache_path: config.snapshot_cache_path.clone(),
                sync_interval: Duration::from_secs(config.sync_interval_secs.max(1)),
                negative_cache_ttl: Duration::from_secs(config.negative_cache_secs.max(1)),
                negative_cache: DashMap::new(),
                resolve_limiter: Mutex::new(ResolveLimiter::new(config.resolve_rate_per_sec)),
                client,
                control_plane_url: config.control_plane_url.clone(),
                service_token,
                hooks,
            }),
        })
    }

    pub fn inactive(fallback: &PublicFallbackConfig) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(SnapshotStoreInner {
                records: ArcSwap::from_pointee(HashMap::new()),
                defaults: ArcSwap::from_pointee(defaults_from_fallback(fallback)),
                cursor: AtomicU64::new(0),
                ready: AtomicBool::new(true),
                loaded_from_cache: AtomicBool::new(false),
                disk_cache_dirty: AtomicBool::new(false),
                last_sync_success: Mutex::new(Some(TokioInstant::now())),
                cache_path: PathBuf::new(),
                sync_interval: Duration::from_secs(10),
                negative_cache_ttl: Duration::from_secs(15),
                negative_cache: DashMap::new(),
                resolve_limiter: Mutex::new(ResolveLimiter::new(0)),
                client: reqwest::Client::builder()
                    .no_proxy()
                    .build()
                    .expect("inactive snapshot store client should build"),
                control_plane_url: "http://127.0.0.1/"
                    .parse()
                    .expect("static URL should parse"),
                service_token: String::new(),
                hooks: SnapshotHooks::default(),
            }),
        })
    }

    pub fn is_ready(&self) -> bool {
        self.inner.ready.load(Ordering::Relaxed)
    }

    pub fn loaded_from_cache(&self) -> bool {
        self.inner.loaded_from_cache.load(Ordering::Relaxed)
    }

    pub fn view(&self) -> StoreView {
        StoreView {
            records: self.inner.records.load_full(),
            defaults: self.inner.defaults.load_full(),
            cursor: self.inner.cursor.load(Ordering::Relaxed),
        }
    }

    pub fn get(&self, key_id: &str) -> Option<Arc<KeySnapshot>> {
        self.inner.records.load().get(key_id).cloned()
    }

    pub async fn get_or_resolve(&self, key_id: &str) -> Option<Arc<KeySnapshot>> {
        if let Some(record) = self.get(key_id) {
            return Some(record);
        }
        self.resolve(key_id).await.ok().flatten()
    }

    pub(crate) fn sweep_negative_cache(&self) -> usize {
        let now = Instant::now();
        let mut removed = 0;
        self.inner.negative_cache.retain(|_, expires_at| {
            let expired = *expires_at <= now;
            if expired {
                removed += 1;
            }
            !expired
        });
        removed
    }

    #[cfg(test)]
    pub(crate) fn negative_cache_len(&self) -> usize {
        self.inner.negative_cache.len()
    }

    #[cfg(test)]
    pub(crate) fn insert_negative_cache_for_test(&self, key_id: &str, ttl: Duration) {
        self.inner
            .negative_cache
            .insert(key_id.to_string(), Instant::now() + ttl);
    }

    #[cfg(test)]
    pub(crate) fn set_ready_for_test(&self, ready: bool) {
        self.inner.ready.store(ready, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn set_loaded_from_cache_for_test(&self, loaded: bool) {
        self.inner
            .loaded_from_cache
            .store(loaded, Ordering::Release);
        if loaded {
            self.inner.ready.store(true, Ordering::Release);
        }
    }

    #[cfg(test)]
    pub(crate) fn install_records_for_test(&self, records: Vec<SnapshotRecord>, cursor: u64) {
        self.replace_records(records, cursor)
            .expect("test snapshot records should install");
        self.inner.ready.store(true, Ordering::Release);
    }

    async fn run_sync_loop(self: Arc<Self>, cancel: CancellationToken) {
        loop {
            let _ = self.run_sync_tick().await;

            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(self.inner.sync_interval) => {}
            }
        }
    }

    async fn run_sync_tick(&self) -> anyhow::Result<()> {
        let result = if !self.is_ready() {
            if let Err(err) = self.bootstrap().await {
                Err(err)
            } else {
                Ok(())
            }
        } else if let Err(err) = self.sync_once().await {
            Err(err)
        } else {
            Ok(())
        };

        match &result {
            Ok(()) => self.record_sync_success(),
            Err(err) if self.is_ready() => {
                tracing::warn!(error = %err, "commercial snapshot sync failed");
            }
            Err(err) => {
                tracing::warn!(error = %err, "commercial snapshot bootstrap failed");
            }
        }
        self.update_sync_age_metrics();
        self.persist_dirty_cache().await;
        result
    }

    async fn bootstrap(&self) -> anyhow::Result<()> {
        let mut cursor = 0;
        let mut all = Vec::new();
        loop {
            let page = self.fetch_page(cursor).await?;
            cursor = page.next_cursor;
            let len = page.raw_record_count;
            all.extend(page.records);
            if len < usize::from(DEFAULT_PAGE_LIMIT) {
                break;
            }
        }
        self.replace_records(all, cursor)?;
        self.inner.ready.store(true, Ordering::Release);
        tracing::info!(cursor, "commercial snapshot bootstrap applied");
        Ok(())
    }

    async fn sync_once(&self) -> anyhow::Result<()> {
        let cursor = self.inner.cursor.load(Ordering::Relaxed);
        let page = self.fetch_page(cursor).await?;
        if page.reset {
            tracing::info!(cursor, "commercial snapshot cursor reset requested");
            self.bootstrap().await?;
            return Ok(());
        }
        if page.raw_record_count == 0 {
            return Ok(());
        }
        let count = page.records.len();
        self.apply_records(page.records, page.next_cursor)?;
        tracing::info!(
            count,
            cursor = page.next_cursor,
            "commercial snapshot delta applied"
        );
        Ok(())
    }

    async fn fetch_page(&self, cursor: u64) -> anyhow::Result<SnapshotPage> {
        let mut url = commercial_url(&self.inner.control_plane_url, &SNAPSHOT_PATH_SEGMENTS)?;
        url.query_pairs_mut()
            .append_pair("cursor", &cursor.to_string())
            .append_pair("limit", &DEFAULT_PAGE_LIMIT.to_string());
        let response = self
            .inner
            .client
            .get(url)
            .bearer_auth(&self.inner.service_token)
            .send()
            .await?;
        let status = response.status();
        anyhow::ensure!(
            status.is_success(),
            "snapshot feed returned status {status}"
        );
        let page: RawSnapshotPage = response.json().await?;
        let raw_record_count = page.records.len();
        Ok(SnapshotPage {
            records: parse_snapshot_records(page.records, "snapshot_page"),
            next_cursor: page.next_cursor,
            reset: page.reset,
            raw_record_count,
        })
    }

    async fn resolve(&self, key_id: &str) -> anyhow::Result<Option<Arc<KeySnapshot>>> {
        if self.negative_cached(key_id) {
            metrics::report_commercial_resolve("negative_cache");
            return Ok(None);
        }
        if !self.take_resolve_token() {
            tracing::info!(
                key_id,
                "commercial snapshot resolve fail-closed: rate limited"
            );
            metrics::report_commercial_resolve("rate_limited");
            return Ok(None);
        }

        let url = commercial_url(&self.inner.control_plane_url, &RESOLVE_PATH_SEGMENTS)?;
        let response = self
            .inner
            .client
            .post(url)
            .bearer_auth(&self.inner.service_token)
            .json(&ResolveRequest { key_id })
            .send()
            .await?;
        match response.status() {
            StatusCode::OK => {
                let value: serde_json::Value = response.json().await?;
                let Some(record) = parse_key_snapshot(value, "resolve_response") else {
                    metrics::report_commercial_resolve("parse_error");
                    return Ok(None);
                };
                let record = self.upsert_key(record)?;
                metrics::report_commercial_resolve("hit");
                Ok(Some(record))
            }
            StatusCode::NOT_FOUND => {
                self.inner.negative_cache.insert(
                    key_id.to_string(),
                    Instant::now() + self.inner.negative_cache_ttl,
                );
                metrics::report_commercial_resolve("not_found");
                Ok(None)
            }
            status => {
                metrics::report_commercial_resolve("error");
                anyhow::bail!("resolve returned status {status}")
            }
        }
    }

    fn replace_records(&self, records: Vec<SnapshotRecord>, cursor: u64) -> anyhow::Result<()> {
        let mut map = HashMap::new();
        let mut defaults = None;
        let mut hook_records = Vec::new();
        for record in records {
            match record {
                SnapshotRecord::Defaults(record) => defaults = Some(Defaults::from(record)),
                SnapshotRecord::Key(record) => {
                    let record = Arc::new(record);
                    hook_records.push(record.clone());
                    map.insert(record.key_id.clone(), record);
                }
            }
        }
        if let Some(defaults) = defaults {
            self.inner.defaults.store(Arc::new(defaults));
        }
        self.inner.records.store(Arc::new(map));
        for record in hook_records {
            self.apply_snapshot_hooks(&record);
        }
        metrics::set_commercial_snapshot_records(self.inner.records.load().len() as i64);
        self.inner.cursor.store(cursor, Ordering::Release);
        self.mark_disk_cache_dirty();
        Ok(())
    }

    fn apply_records(&self, records: Vec<SnapshotRecord>, cursor: u64) -> anyhow::Result<()> {
        let mut map = (**self.inner.records.load()).clone();
        let mut hook_records = Vec::new();
        for record in records {
            match record {
                SnapshotRecord::Defaults(record) => {
                    self.inner.defaults.store(Arc::new(Defaults::from(record)));
                }
                SnapshotRecord::Key(record) => {
                    if record.status != KeyStatus::Active {
                        tracing::info!(key_id = record.key_id, "commercial key became non-active");
                    }
                    let record = Arc::new(record);
                    hook_records.push(record.clone());
                    map.insert(record.key_id.clone(), record);
                }
            }
        }
        self.inner.records.store(Arc::new(map));
        for record in hook_records {
            self.apply_snapshot_hooks(&record);
        }
        metrics::set_commercial_snapshot_records(self.inner.records.load().len() as i64);
        self.inner.cursor.store(cursor, Ordering::Release);
        self.mark_disk_cache_dirty();
        Ok(())
    }

    fn upsert_key(&self, record: KeySnapshot) -> anyhow::Result<Arc<KeySnapshot>> {
        let record = Arc::new(record);
        loop {
            let current = self.inner.records.load();
            if let Some(existing) = current.get(&record.key_id) {
                if existing.seq >= record.seq {
                    return Ok(existing.clone());
                }
            }

            let mut map = (**current).clone();
            map.insert(record.key_id.clone(), record.clone());
            let previous = self
                .inner
                .records
                .compare_and_swap(&*current, Arc::new(map));
            if Arc::ptr_eq(&*current, &*previous) {
                break;
            }
        }

        self.apply_snapshot_hooks(&record);
        metrics::set_commercial_snapshot_records(self.inner.records.load().len() as i64);
        self.mark_disk_cache_dirty();
        Ok(record)
    }

    fn load_disk_cache(&self) {
        let path = &self.inner.cache_path;
        if path.as_os_str().is_empty() {
            return;
        }
        let Ok(bytes) = std::fs::read(path) else {
            return;
        };
        let cache = match serde_json::from_slice::<DiskCache>(&bytes) {
            Ok(cache) => cache,
            Err(err) => {
                tracing::warn!(path = %path.display(), error = %err, "failed to load commercial snapshot cache");
                return;
            }
        };
        let mut map = HashMap::new();
        for record in parse_snapshot_records(cache.records, "disk_cache") {
            if let SnapshotRecord::Key(record) = record {
                map.insert(record.key_id.clone(), Arc::new(record));
            }
        }
        self.inner.records.store(Arc::new(map));
        self.inner.defaults.store(Arc::new(cache.defaults));
        self.inner.cursor.store(cache.cursor, Ordering::Release);
        self.inner.ready.store(true, Ordering::Release);
        self.inner.loaded_from_cache.store(true, Ordering::Release);
        let age = now_secs().saturating_sub(cache.saved_at);
        *self.inner.last_sync_success.lock().unwrap() =
            Some(TokioInstant::now() - Duration::from_secs(age));
        metrics::set_commercial_snapshot_age(age as i64);
        metrics::set_commercial_sync_staleness(age as i64);
        metrics::set_commercial_snapshot_records(self.inner.records.load().len() as i64);
        tracing::warn!(path = %path.display(), age_seconds = age, "loaded commercial snapshot disk cache");
    }

    async fn persist_disk_cache(&self) -> anyhow::Result<()> {
        let path = &self.inner.cache_path;
        if path.as_os_str().is_empty() {
            return Ok(());
        }
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            tokio::fs::create_dir_all(parent).await?;
        }
        let records = self
            .inner
            .records
            .load()
            .values()
            .map(|record| snapshot_record_to_value(SnapshotRecord::Key((**record).clone())))
            .collect();
        let cache = DiskCache {
            cursor: self.inner.cursor.load(Ordering::Relaxed),
            records,
            defaults: (**self.inner.defaults.load()).clone(),
            saved_at: now_secs(),
        };
        let bytes = serde_json::to_vec(&cache)?;
        let tmp = tmp_path(path);
        tokio::fs::write(&tmp, bytes).await?;
        tokio::fs::rename(&tmp, path).await?;
        Ok(())
    }

    fn mark_disk_cache_dirty(&self) {
        self.inner.disk_cache_dirty.store(true, Ordering::Release);
    }

    async fn persist_dirty_cache(&self) {
        if !self.inner.disk_cache_dirty.swap(false, Ordering::AcqRel) {
            return;
        }
        if let Err(err) = self.persist_disk_cache().await {
            self.inner.disk_cache_dirty.store(true, Ordering::Release);
            metrics::report_commercial_snapshot_persist_error();
            tracing::warn!(error = %err, "commercial snapshot disk cache persist failed");
        }
    }

    fn record_sync_success(&self) {
        *self.inner.last_sync_success.lock().unwrap() = Some(TokioInstant::now());
    }

    fn sync_age_seconds(&self) -> Option<i64> {
        self.inner
            .last_sync_success
            .lock()
            .unwrap()
            .map(|last_success| {
                TokioInstant::now()
                    .duration_since(last_success)
                    .as_secs()
                    .try_into()
                    .unwrap_or(i64::MAX)
            })
    }

    fn update_sync_age_metrics(&self) {
        let Some(seconds) = self.sync_age_seconds() else {
            return;
        };
        metrics::set_commercial_sync_staleness(seconds);
        metrics::set_commercial_snapshot_age(seconds);
    }

    fn negative_cached(&self, key_id: &str) -> bool {
        let Some(entry) = self.inner.negative_cache.get(key_id) else {
            return false;
        };
        if *entry.value() > Instant::now() {
            return true;
        }
        drop(entry);
        self.inner.negative_cache.remove(key_id);
        false
    }

    fn take_resolve_token(&self) -> bool {
        self.inner.resolve_limiter.lock().unwrap().take()
    }

    fn apply_snapshot_hooks(&self, record: &KeySnapshot) {
        if let (Some(tally), Some(account_id), Some(quota)) = (
            &self.inner.hooks.tally,
            record.account_id.as_deref(),
            record.quota.as_ref(),
        ) {
            tally.rebase_account(account_id, quota.version);
        }
        if record.status != KeyStatus::Active {
            if let Some(registry) = &self.inner.hooks.registry {
                let killed = registry.kill_key(&record.key_id);
                if killed > 0 {
                    tracing::info!(
                        key_id = record.key_id,
                        killed,
                        "commercial key kill flag applied to active streams"
                    );
                }
            }
        }
    }
}

impl ResolveLimiter {
    fn new(rate_per_sec: u64) -> Self {
        let capacity = rate_per_sec as f64;
        Self {
            rate_per_sec: capacity,
            capacity,
            tokens: capacity,
            last: Instant::now(),
        }
    }

    fn take(&mut self) -> bool {
        if self.rate_per_sec <= 0.0 {
            return false;
        }
        let now = Instant::now();
        let elapsed = now.duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + elapsed * self.rate_per_sec).min(self.capacity);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

fn defaults_from_fallback(config: &PublicFallbackConfig) -> Defaults {
    Defaults {
        public: PublicDefaults {
            limits: PublicLimits {
                throughput_bytes_per_sec: Some(config.throughput_bytes_per_sec),
                burst_bytes: Some(config.burst_bytes),
                max_response_bytes: Some(config.max_response_bytes),
                concurrency: Some(config.concurrency as u64),
            },
            quota: PublicQuota {
                volume_bytes: Some(config.volume_bytes),
                window_secs: config.window_secs,
                on_exceed: OnExceed::Reject,
            },
        },
        messages: HashMap::new(),
        seq: 0,
    }
}

fn commercial_url(base: &url::Url, segments: &[&str]) -> anyhow::Result<url::Url> {
    let mut url = base.clone();
    url.set_query(None);
    url.set_fragment(None);
    let mut path = url
        .path_segments_mut()
        .map_err(|_| anyhow::anyhow!("control plane URL cannot be a base"))?;
    path.pop_if_empty();
    path.extend(segments);
    drop(path);
    Ok(url)
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn parse_snapshot_records(
    records: Vec<serde_json::Value>,
    source: &'static str,
) -> Vec<SnapshotRecord> {
    records
        .into_iter()
        .enumerate()
        .filter_map(
            |(index, value)| match serde_json::from_value::<SnapshotRecord>(value) {
                Ok(record) => Some(record),
                Err(err) => {
                    metrics::report_commercial_snapshot_parse_error();
                    tracing::warn!(
                        source,
                        index,
                        error = %err,
                        "skipping unparseable commercial snapshot record"
                    );
                    None
                }
            },
        )
        .collect()
}

fn parse_key_snapshot(value: serde_json::Value, source: &'static str) -> Option<KeySnapshot> {
    match serde_json::from_value::<KeySnapshot>(value) {
        Ok(record) => Some(record),
        Err(err) => {
            metrics::report_commercial_snapshot_parse_error();
            tracing::warn!(
                source,
                error = %err,
                "skipping unparseable commercial snapshot resolve record"
            );
            None
        }
    }
}

fn snapshot_record_to_value(record: SnapshotRecord) -> serde_json::Value {
    serde_json::to_value(record).expect("commercial snapshot records should serialize")
}

#[cfg(test)]
pub mod test_support {
    use std::{
        collections::VecDeque,
        net::SocketAddr,
        sync::{Arc, Mutex},
    };

    use axum::{
        extract::{Query, State},
        http::{HeaderMap, StatusCode},
        routing::{get, post},
        Json, Router,
    };
    use serde::{Deserialize, Serialize};
    use tokio::task::JoinHandle;

    use super::*;
    use crate::commercial::{
        DefaultsRecord, Entitlements, Quota, SnapshotLimits, StreamUsageEvent,
    };

    pub const SERVICE_TOKEN: &str = "test-service-token";
    pub const KEY_ID: &str = "AbCdEfGhIjKlMnOp";
    pub const SECRET_SHA256: &str =
        "73337f479fe170d73e53e247f3052e4243cc9c2a0ffa621853d9385c619efb77";

    #[derive(Clone)]
    pub struct MockControlPlane {
        addr: SocketAddr,
        state: Arc<MockState>,
        _task: Arc<JoinHandle<()>>,
    }

    #[derive(Default)]
    struct MockState {
        pages: Mutex<VecDeque<MockPage>>,
        resolves: Mutex<HashMap<String, Option<KeySnapshot>>>,
        resolve_statuses: Mutex<HashMap<String, StatusCode>>,
        resolve_calls: Mutex<Vec<String>>,
        resolve_bodies: Mutex<Vec<serde_json::Value>>,
        usage_batches: Mutex<Vec<Vec<StreamUsageEvent>>>,
    }

    #[derive(Clone)]
    struct MockPage {
        min_cursor: u64,
        page: serde_json::Value,
    }

    #[derive(Deserialize)]
    struct CursorQuery {
        cursor: u64,
        #[allow(dead_code)]
        limit: Option<u16>,
    }

    impl MockControlPlane {
        pub async fn spawn() -> Self {
            let state = Arc::new(MockState::default());
            let app = Router::new()
                .route("/internal/portal/v1/snapshots", get(snapshots))
                .route("/internal/portal/v1/authorize", post(resolve))
                .route("/internal/portal/v1/usage", post(usage))
                .with_state(state.clone());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let task = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            Self {
                addr,
                state,
                _task: Arc::new(task),
            }
        }

        pub fn url(&self) -> url::Url {
            format!("http://{}", self.addr).parse().unwrap()
        }

        pub fn push_page(&self, min_cursor: u64, page: SnapshotPage) {
            self.push_raw_page(
                min_cursor,
                serde_json::to_value(page).expect("mock snapshot page should serialize"),
            );
        }

        pub fn push_raw_page(&self, min_cursor: u64, page: serde_json::Value) {
            self.state
                .pages
                .lock()
                .unwrap()
                .push_back(MockPage { min_cursor, page });
        }

        pub fn resolve_with(&self, key_id: &str, record: Option<KeySnapshot>) {
            self.state
                .resolves
                .lock()
                .unwrap()
                .insert(key_id.to_string(), record);
        }

        pub fn resolve_status(&self, key_id: &str, status: StatusCode) {
            self.state
                .resolve_statuses
                .lock()
                .unwrap()
                .insert(key_id.to_string(), status);
        }

        pub fn resolve_calls(&self) -> Vec<String> {
            self.state.resolve_calls.lock().unwrap().clone()
        }

        pub fn resolve_bodies(&self) -> Vec<serde_json::Value> {
            self.state.resolve_bodies.lock().unwrap().clone()
        }

        pub fn usage_batches(&self) -> Vec<Vec<StreamUsageEvent>> {
            self.state.usage_batches.lock().unwrap().clone()
        }
    }

    pub fn active_snapshot(seq: u64) -> KeySnapshot {
        KeySnapshot {
            key_id: KEY_ID.to_string(),
            secret_sha256: Some(SECRET_SHA256.to_string()),
            account_id: Some("account".to_string()),
            status: KeyStatus::Active,
            expires_at: None,
            limits: Some(SnapshotLimits {
                throughput_bytes_per_sec: Some(1_000_000),
                burst_bytes: Some(2_000_000),
                max_response_bytes: Some(3_000_000),
                concurrency: Some(4),
                max_chunks: None,
            }),
            entitlements: Some(Entitlements {
                chains: vec!["ethereum-mainnet".to_string()],
                traces: vec![],
            }),
            quota: Some(Quota {
                remaining_bytes: Some(10_000),
                period_end: Some(4_102_444_800),
                version: seq,
                on_exceed: OnExceed::Reject,
            }),
            seq,
        }
    }

    pub fn defaults_record(seq: u64) -> DefaultsRecord {
        DefaultsRecord {
            key_id: "__defaults__".to_string(),
            public: PublicDefaults {
                limits: PublicLimits {
                    throughput_bytes_per_sec: Some(100),
                    burst_bytes: Some(200),
                    max_response_bytes: Some(300),
                    concurrency: Some(2),
                },
                quota: PublicQuota {
                    volume_bytes: Some(400),
                    window_secs: 60,
                    on_exceed: OnExceed::Reject,
                },
            },
            messages: HashMap::from([("invalid_key".to_string(), "invalid".to_string())]),
            seq,
        }
    }

    pub fn page(records: Vec<SnapshotRecord>, next_cursor: u64, reset: bool) -> SnapshotPage {
        let raw_record_count = records.len();
        SnapshotPage {
            records,
            next_cursor,
            reset,
            raw_record_count,
        }
    }

    #[derive(Deserialize)]
    struct UsageBody {
        events: Vec<StreamUsageEvent>,
    }

    #[derive(Serialize)]
    struct UsageResponse {
        accepted: usize,
        duplicates: usize,
    }

    fn authorized(headers: &HeaderMap) -> bool {
        headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            == Some(&format!("Bearer {SERVICE_TOKEN}"))
    }

    async fn snapshots(
        State(state): State<Arc<MockState>>,
        headers: HeaderMap,
        Query(query): Query<CursorQuery>,
    ) -> Result<Json<serde_json::Value>, StatusCode> {
        if !authorized(&headers) {
            return Err(StatusCode::UNAUTHORIZED);
        }
        let mut pages = state.pages.lock().unwrap();
        let index = pages
            .iter()
            .position(|page| query.cursor >= page.min_cursor)
            .unwrap_or(0);
        let page = pages
            .remove(index)
            .map(|page| page.page)
            .unwrap_or_else(|| {
                serde_json::json!({
                    "records": [],
                    "next_cursor": query.cursor,
                    "reset": false,
                })
            });
        Ok(Json(page))
    }

    async fn resolve(
        State(state): State<Arc<MockState>>,
        headers: HeaderMap,
        Json(body): Json<serde_json::Value>,
    ) -> Result<Json<KeySnapshot>, StatusCode> {
        if !authorized(&headers) {
            return Err(StatusCode::UNAUTHORIZED);
        }
        state.resolve_bodies.lock().unwrap().push(body.clone());
        let key_id = body
            .get("key_id")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string();
        state.resolve_calls.lock().unwrap().push(key_id.clone());
        if let Some(status) = state.resolve_statuses.lock().unwrap().get(&key_id) {
            return Err(*status);
        }
        let record = state.resolves.lock().unwrap().get(&key_id).cloned();
        match record {
            Some(Some(record)) => Ok(Json(record)),
            _ => Err(StatusCode::NOT_FOUND),
        }
    }

    async fn usage(
        State(state): State<Arc<MockState>>,
        headers: HeaderMap,
        Json(body): Json<UsageBody>,
    ) -> Result<Json<UsageResponse>, StatusCode> {
        if !authorized(&headers) {
            return Err(StatusCode::UNAUTHORIZED);
        }
        let accepted = body.events.len();
        state.usage_batches.lock().unwrap().push(body.events);
        Ok(Json(UsageResponse {
            accepted,
            duplicates: 0,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{test_support::*, *};
    use bytes::Bytes;
    use futures::{stream, StreamExt};

    use crate::commercial::{
        meter::tap_plain_stream, Endpoint, Granted, GrantedLimits, MeterHandle, Principal,
        StreamUsageEvent, UsageReporter, UsageStatus,
    };

    #[derive(Default)]
    struct RecordingReporter {
        events: Mutex<Vec<StreamUsageEvent>>,
    }

    impl UsageReporter for RecordingReporter {
        fn report(&self, event: StreamUsageEvent) {
            self.events.lock().unwrap().push(event);
        }
    }

    static STALENESS_GAUGE_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn staleness_gauge_test_guard() -> std::sync::MutexGuard<'static, ()> {
        STALENESS_GAUGE_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn reset_staleness_gauges() {
        metrics::set_commercial_sync_staleness(0);
        metrics::set_commercial_snapshot_age(0);
    }

    fn config(
        mock: &MockControlPlane,
        cache_path: PathBuf,
        resolve_rate_per_sec: u64,
    ) -> CommercialConfig {
        CommercialConfig {
            control_plane_url: mock.url(),
            service_token_env: "PORTAL_STORE_TEST_TOKEN".to_string(),
            sync_interval_secs: 1,
            flush_interval_secs: 5,
            flush_max_events: 500,
            usage_buffer_max_events: 1000,
            snapshot_cache_path: cache_path,
            resolve_rate_per_sec,
            negative_cache_secs: 60,
            pod_count: 1,
            client_ip_header: "x-forwarded-for".to_string(),
            throttle_residual_secs: 60,
            sweep_horizon_window_multiplier: 2,
            public_fallback: PublicFallbackConfig {
                throughput_bytes_per_sec: 1,
                burst_bytes: 1,
                max_response_bytes: 1,
                volume_bytes: 1,
                window_secs: 1,
                concurrency: 1,
            },
        }
    }

    fn offline_config(cache_path: PathBuf) -> CommercialConfig {
        CommercialConfig {
            control_plane_url: "http://127.0.0.1/".parse().unwrap(),
            service_token_env: "PORTAL_STORE_TEST_TOKEN".to_string(),
            sync_interval_secs: 1,
            flush_interval_secs: 5,
            flush_max_events: 500,
            usage_buffer_max_events: 1000,
            snapshot_cache_path: cache_path,
            resolve_rate_per_sec: 0,
            negative_cache_secs: 60,
            pod_count: 1,
            client_ip_header: "x-forwarded-for".to_string(),
            throttle_residual_secs: 60,
            sweep_horizon_window_multiplier: 2,
            public_fallback: PublicFallbackConfig {
                throughput_bytes_per_sec: 1,
                burst_bytes: 1,
                max_response_bytes: 1,
                volume_bytes: 1,
                window_secs: 1,
                concurrency: 1,
            },
        }
    }

    fn cache_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("sqd-portal-{name}-{}.json", uuid::Uuid::new_v4()))
    }

    #[tokio::test]
    async fn bootstrap_pages_and_persists_cache() {
        std::env::set_var("PORTAL_STORE_TEST_TOKEN", SERVICE_TOKEN);
        let mock = MockControlPlane::spawn().await;
        let mut first_page = vec![SnapshotRecord::Defaults(defaults_record(1))];
        for i in 0..999 {
            let mut record = active_snapshot(1);
            record.key_id = format!("dummy-{i}");
            first_page.push(SnapshotRecord::Key(record));
        }
        mock.push_page(0, page(first_page, 1, false));
        mock.push_page(
            1,
            page(vec![SnapshotRecord::Key(active_snapshot(2))], 2, false),
        );
        let path = cache_path("bootstrap");
        let (store, task) =
            SnapshotStore::spawn(&config(&mock, path.clone(), 10), CancellationToken::new())
                .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            while !store.is_ready() || !path.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert!(store.get(KEY_ID).is_some());
        assert!(path.exists());
        task.abort();
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn bootstrap_skips_bad_snapshot_records_and_advances_cursor() {
        std::env::set_var("PORTAL_STORE_TEST_TOKEN", SERVICE_TOKEN);
        let mock = MockControlPlane::spawn().await;
        let mut next = active_snapshot(2);
        next.key_id = "next-key".to_string();
        let before = metrics::COMMERCIAL_SNAPSHOT_PARSE_ERRORS.get();
        mock.push_raw_page(
            0,
            serde_json::json!({
                "records": [
                    snapshot_record_to_value(SnapshotRecord::Key(active_snapshot(1))),
                    {"key_id": "poison", "seq": 2, "status": null},
                    snapshot_record_to_value(SnapshotRecord::Key(next.clone()))
                ],
                "next_cursor": 7,
                "reset": false
            }),
        );
        let path = cache_path("poison-page");
        let (store, task) =
            SnapshotStore::spawn(&config(&mock, path.clone(), 10), CancellationToken::new())
                .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            while !store.is_ready() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        let view = store.view();
        assert_eq!(view.cursor, 7);
        assert!(store.get(KEY_ID).is_some());
        assert!(store.get("next-key").is_some());
        assert!(metrics::COMMERCIAL_SNAPSHOT_PARSE_ERRORS.get() > before);
        task.abort();
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn corrupt_disk_cache_is_ignored_and_bootstrap_continues() {
        let path = cache_path("corrupt");
        tokio::fs::write(&path, b"{not-json").await.unwrap();

        std::env::set_var("PORTAL_STORE_TEST_TOKEN", SERVICE_TOKEN);
        let mock = MockControlPlane::spawn().await;
        mock.push_page(
            0,
            page(vec![SnapshotRecord::Key(active_snapshot(1))], 1, false),
        );
        let (store, task) =
            SnapshotStore::spawn(&config(&mock, path.clone(), 10), CancellationToken::new())
                .unwrap();

        assert!(!store.loaded_from_cache());
        tokio::time::timeout(Duration::from_secs(2), async {
            while !store.is_ready() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert!(store.get(KEY_ID).is_some());
        task.abort();
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn disk_cache_round_trip_is_ready_without_control_plane() {
        let path = cache_path("round-trip");
        let cache = DiskCache {
            cursor: 2,
            records: vec![snapshot_record_to_value(SnapshotRecord::Key(
                active_snapshot(2),
            ))],
            defaults: Defaults::from(defaults_record(1)),
            saved_at: now_secs(),
        };
        tokio::fs::write(&path, serde_json::to_vec(&cache).unwrap())
            .await
            .unwrap();

        std::env::set_var("PORTAL_STORE_TEST_TOKEN", SERVICE_TOKEN);
        let mock = MockControlPlane::spawn().await;
        let (store, task) =
            SnapshotStore::spawn(&config(&mock, path.clone(), 10), CancellationToken::new())
                .unwrap();

        assert!(store.is_ready());
        assert!(store.loaded_from_cache());
        assert!(store.get(KEY_ID).is_some());

        task.abort();
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn delta_upserts_and_tombstones() {
        std::env::set_var("PORTAL_STORE_TEST_TOKEN", SERVICE_TOKEN);
        let mock = MockControlPlane::spawn().await;
        mock.push_page(
            0,
            page(vec![SnapshotRecord::Key(active_snapshot(1))], 1, false),
        );
        let mut revoked = active_snapshot(2);
        revoked.status = KeyStatus::Revoked;
        mock.push_page(
            1,
            page(vec![SnapshotRecord::Key(revoked.clone())], 2, false),
        );
        let path = cache_path("delta");
        let (store, task) =
            SnapshotStore::spawn(&config(&mock, path.clone(), 10), CancellationToken::new())
                .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            while store.view().cursor < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(store.get(KEY_ID).unwrap().status, KeyStatus::Revoked);
        task.abort();
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn delta_skips_bad_snapshot_records_and_advances_cursor() {
        let mock = MockControlPlane::spawn().await;
        mock.push_page(
            0,
            page(vec![SnapshotRecord::Key(active_snapshot(1))], 10, false),
        );
        let path = cache_path("delta-poison-page");
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let store = SnapshotStore::new(
            &config(&mock, path.clone(), 10),
            SnapshotHooks::default(),
            SERVICE_TOKEN.to_string(),
            client,
            TokioInstant::now(),
        );

        store.run_sync_tick().await.unwrap();
        assert_eq!(store.view().cursor, 10);

        let before = metrics::COMMERCIAL_SNAPSHOT_PARSE_ERRORS.get();
        mock.push_raw_page(
            10,
            serde_json::json!({
                "records": [
                    {"key_id": "poison", "seq": 2, "status": null}
                ],
                "next_cursor": 11,
                "reset": false
            }),
        );

        store.run_sync_tick().await.unwrap();

        assert_eq!(store.view().cursor, 11);
        assert!(metrics::COMMERCIAL_SNAPSHOT_PARSE_ERRORS.get() > before);
        let _ = tokio::fs::remove_file(path).await;
    }

    #[test]
    fn resolve_upsert_does_not_advance_cursor_or_hide_intervening_delta() {
        let store = SnapshotStore::inactive(&PublicFallbackConfig {
            throughput_bytes_per_sec: 1,
            burst_bytes: 1,
            max_response_bytes: 1,
            volume_bytes: 1,
            window_secs: 1,
            concurrency: 1,
        });
        store
            .replace_records(vec![SnapshotRecord::Key(active_snapshot(10))], 10)
            .unwrap();

        let mut resolved = active_snapshot(12);
        resolved.key_id = "fresh-key".to_string();
        store.upsert_key(resolved).unwrap();

        assert_eq!(store.view().cursor, 10);
        assert!(store.get("fresh-key").is_some());

        let mut suspended = active_snapshot(11);
        suspended.status = KeyStatus::Suspended;
        store
            .apply_records(vec![SnapshotRecord::Key(suspended)], 11)
            .unwrap();

        assert_eq!(store.view().cursor, 11);
        assert_eq!(store.get(KEY_ID).unwrap().status, KeyStatus::Suspended);
    }

    #[test]
    fn resolve_upsert_keeps_newer_same_key_delta() {
        let store = SnapshotStore::inactive(&PublicFallbackConfig {
            throughput_bytes_per_sec: 1,
            burst_bytes: 1,
            max_response_bytes: 1,
            volume_bytes: 1,
            window_secs: 1,
            concurrency: 1,
        });
        let mut suspended = active_snapshot(12);
        suspended.status = KeyStatus::Suspended;
        store
            .apply_records(vec![SnapshotRecord::Key(suspended)], 12)
            .unwrap();

        let returned = store.upsert_key(active_snapshot(11)).unwrap();

        assert_eq!(returned.seq, 12);
        assert_eq!(returned.status, KeyStatus::Suspended);
        let stored = store.get(KEY_ID).unwrap();
        assert_eq!(stored.seq, 12);
        assert_eq!(stored.status, KeyStatus::Suspended);
    }

    #[tokio::test(start_paused = true)]
    async fn sync_staleness_gauge_grows_from_boot_before_first_success() {
        let _guard = staleness_gauge_test_guard();
        reset_staleness_gauges();
        let path = cache_path("from-boot-outage");
        let _ = tokio::fs::remove_file(&path).await;
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let store = SnapshotStore::new(
            &offline_config(path.clone()),
            SnapshotHooks::default(),
            String::new(),
            client,
            TokioInstant::now(),
        );
        store.load_disk_cache();

        assert!(!store.is_ready());
        assert!(!store.loaded_from_cache());

        store.update_sync_age_metrics();
        assert_eq!(metrics::COMMERCIAL_SYNC_STALENESS_SECONDS.get(), 0);
        assert_eq!(metrics::COMMERCIAL_SNAPSHOT_AGE_SECONDS.get(), 0);

        tokio::time::advance(Duration::from_secs(2)).await;
        store.update_sync_age_metrics();

        assert_eq!(metrics::COMMERCIAL_SYNC_STALENESS_SECONDS.get(), 2);
        assert_eq!(metrics::COMMERCIAL_SNAPSHOT_AGE_SECONDS.get(), 2);
        assert!(!store.is_ready());

        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test(start_paused = true)]
    async fn sync_staleness_gauges_grow_across_failed_ticks() {
        let _guard = staleness_gauge_test_guard();
        reset_staleness_gauges();
        let store = SnapshotStore::inactive(&PublicFallbackConfig {
            throughput_bytes_per_sec: 1,
            burst_bytes: 1,
            max_response_bytes: 1,
            volume_bytes: 1,
            window_secs: 1,
            concurrency: 1,
        });

        store.record_sync_success();
        store.update_sync_age_metrics();
        assert_eq!(metrics::COMMERCIAL_SYNC_STALENESS_SECONDS.get(), 0);
        assert_eq!(metrics::COMMERCIAL_SNAPSHOT_AGE_SECONDS.get(), 0);

        tokio::time::advance(Duration::from_secs(1)).await;
        store.update_sync_age_metrics();
        let first_staleness = metrics::COMMERCIAL_SYNC_STALENESS_SECONDS.get();
        let first_age = metrics::COMMERCIAL_SNAPSHOT_AGE_SECONDS.get();
        assert!(first_staleness >= 1);
        assert!(first_age >= 1);

        tokio::time::advance(Duration::from_secs(1)).await;
        store.update_sync_age_metrics();
        assert!(metrics::COMMERCIAL_SYNC_STALENESS_SECONDS.get() > first_staleness);
        assert!(metrics::COMMERCIAL_SNAPSHOT_AGE_SECONDS.get() > first_age);
    }

    #[tokio::test]
    async fn sync_hooks_rebase_tally_and_kill_non_active_streams() {
        std::env::set_var("PORTAL_STORE_TEST_TOKEN", SERVICE_TOKEN);
        let mock = MockControlPlane::spawn().await;
        mock.push_page(
            0,
            page(vec![SnapshotRecord::Key(active_snapshot(1))], 1, false),
        );
        let mut suspended = active_snapshot(2);
        suspended.status = KeyStatus::Suspended;
        mock.push_page(1, page(vec![SnapshotRecord::Key(suspended)], 2, false));
        let tally = Arc::new(TallyStore::default());
        tally.debit("account", 1, 100);
        let registry = Arc::new(ActiveStreamRegistry::default());
        let kill = Arc::new(AtomicBool::new(false));
        let _registration = registry.register(
            "event".to_string(),
            Some(KEY_ID.to_string()),
            kill.clone(),
            Arc::new(AtomicU64::new(0)),
        );
        let path = cache_path("hooks");
        let (store, task) = SnapshotStore::spawn_with_hooks(
            &config(&mock, path.clone(), 10),
            CancellationToken::new(),
            SnapshotHooks {
                tally: Some(tally.clone()),
                registry: Some(registry),
            },
        )
        .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            while store.view().cursor < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(tally.version_for("account"), Some(2));
        assert_eq!(tally.bytes_for("account", 2), 0);
        assert!(kill.load(Ordering::Acquire));

        task.abort();
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test(start_paused = true)]
    async fn suspension_kill_cuts_stream_within_one_interval_and_chunk() {
        let tally = Arc::new(TallyStore::default());
        let registry = Arc::new(ActiveStreamRegistry::default());
        let reporter = Arc::new(RecordingReporter::default());
        let meter = MeterHandle::new_enforced(
            Granted {
                principal: Principal {
                    account_id: "account".to_string(),
                    api_key_id: Some(KEY_ID.to_string()),
                },
                tally_account_id: None,
                entitled_chains: None,
                limits: GrantedLimits::default(),
                on_exceed: OnExceed::Reject,
                quota_version: 1,
                quota_remaining_bytes: Some(1_000),
                concurrency_permit: None,
            },
            "request".to_string(),
            Endpoint::Stream,
            "ethereum-mainnet".to_string(),
            reporter.clone(),
            tally,
            registry.clone(),
            None,
        );
        let start = tokio::time::Instant::now();

        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(registry.kill_key(KEY_ID), 1);

        let chunks = vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"first")),
            Ok::<_, std::io::Error>(Bytes::from_static(b"second")),
        ];
        let emitted: Vec<_> = tap_plain_stream(stream::iter(chunks), meter)
            .collect()
            .await;

        assert_eq!(emitted.len(), 1);
        assert!(start.elapsed() <= Duration::from_secs(1));
        let event = reporter.events.lock().unwrap().pop().unwrap();
        assert_eq!(event.status, UsageStatus::CutSuspended);
    }

    #[tokio::test]
    async fn meter_registration_rechecks_current_snapshot_status() {
        let store = SnapshotStore::inactive(&PublicFallbackConfig {
            throughput_bytes_per_sec: 1,
            burst_bytes: 1,
            max_response_bytes: 1,
            volume_bytes: 1,
            window_secs: 1,
            concurrency: 1,
        });
        store.install_records_for_test(vec![SnapshotRecord::Key(active_snapshot(1))], 1);

        let granted = Granted {
            principal: Principal {
                account_id: "account".to_string(),
                api_key_id: Some(KEY_ID.to_string()),
            },
            tally_account_id: None,
            entitled_chains: None,
            limits: GrantedLimits::default(),
            on_exceed: OnExceed::Reject,
            quota_version: 1,
            quota_remaining_bytes: Some(1_000),
            concurrency_permit: None,
        };
        let mut suspended = active_snapshot(2);
        suspended.status = KeyStatus::Suspended;
        store
            .apply_records(vec![SnapshotRecord::Key(suspended)], 2)
            .unwrap();

        let tally = Arc::new(TallyStore::default());
        let registry = Arc::new(ActiveStreamRegistry::default());
        let reporter = Arc::new(RecordingReporter::default());
        let meter = MeterHandle::new_enforced(
            granted,
            "request".to_string(),
            Endpoint::Stream,
            "ethereum-mainnet".to_string(),
            reporter.clone(),
            tally,
            registry,
            Some(store),
        );
        let chunks = vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"first")),
            Ok::<_, std::io::Error>(Bytes::from_static(b"never")),
        ];

        let emitted: Vec<_> = tap_plain_stream(stream::iter(chunks), meter)
            .collect()
            .await;

        assert_eq!(emitted.len(), 1);
        let event = reporter.events.lock().unwrap().pop().unwrap();
        assert_eq!(event.status, UsageStatus::CutSuspended);
    }

    #[tokio::test]
    async fn suspension_delta_cuts_meter_registered_after_target_kill_hook() {
        let registry = Arc::new(ActiveStreamRegistry::default());
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let store = SnapshotStore::new(
            &offline_config(PathBuf::new()),
            SnapshotHooks {
                tally: None,
                registry: Some(registry.clone()),
            },
            String::new(),
            client,
            TokioInstant::now(),
        );
        store.install_records_for_test(vec![SnapshotRecord::Key(active_snapshot(1))], 1);

        let marker_key = "marker-key";
        let marker_kill = Arc::new(AtomicBool::new(false));
        let _marker_registration = registry.register(
            "marker-event".to_string(),
            Some(marker_key.to_string()),
            marker_kill.clone(),
            Arc::new(AtomicU64::new(0)),
        );

        let mut records = Vec::new();
        let mut suspended = active_snapshot(2);
        suspended.status = KeyStatus::Suspended;
        records.push(SnapshotRecord::Key(suspended));

        let mut marker = active_snapshot(2);
        marker.key_id = marker_key.to_string();
        marker.status = KeyStatus::Suspended;
        records.push(SnapshotRecord::Key(marker));

        let mut dummy_registrations = Vec::new();
        for index in 0..2048 {
            let key_id = format!("dummy-key-{index}");
            dummy_registrations.push(registry.register(
                format!("dummy-event-{index}"),
                Some(key_id.clone()),
                Arc::new(AtomicBool::new(false)),
                Arc::new(AtomicU64::new(0)),
            ));

            let mut dummy = active_snapshot(2);
            dummy.key_id = key_id;
            dummy.status = KeyStatus::Suspended;
            records.push(SnapshotRecord::Key(dummy));
        }

        let applying_store = store.clone();
        let apply_thread = std::thread::spawn(move || {
            applying_store.apply_records(records, 2).unwrap();
        });

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !marker_kill.load(Ordering::Acquire) {
            assert!(
                std::time::Instant::now() < deadline,
                "marker hook should run before registering the late meter"
            );
            std::thread::yield_now();
        }

        let reporter = Arc::new(RecordingReporter::default());
        let meter = MeterHandle::new_enforced(
            Granted {
                principal: Principal {
                    account_id: "account".to_string(),
                    api_key_id: Some(KEY_ID.to_string()),
                },
                tally_account_id: None,
                entitled_chains: None,
                limits: GrantedLimits::default(),
                on_exceed: OnExceed::Reject,
                quota_version: 1,
                quota_remaining_bytes: Some(1_000),
                concurrency_permit: None,
            },
            "request".to_string(),
            Endpoint::Stream,
            "ethereum-mainnet".to_string(),
            reporter.clone(),
            Arc::new(TallyStore::default()),
            registry,
            Some(store),
        );

        apply_thread.join().unwrap();
        drop(dummy_registrations);

        let chunks = vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"first")),
            Ok::<_, std::io::Error>(Bytes::from_static(b"never")),
        ];
        let emitted: Vec<_> = tap_plain_stream(stream::iter(chunks), meter)
            .collect()
            .await;

        assert_eq!(emitted.len(), 1);
        let event = reporter.events.lock().unwrap().pop().unwrap();
        assert_eq!(event.status, UsageStatus::CutSuspended);
    }

    #[tokio::test]
    async fn reset_rebootstraps_without_clearing_serving_store_first() {
        std::env::set_var("PORTAL_STORE_TEST_TOKEN", SERVICE_TOKEN);
        let mock = MockControlPlane::spawn().await;
        mock.push_page(
            0,
            page(vec![SnapshotRecord::Key(active_snapshot(1))], 1, false),
        );
        mock.push_page(1, page(vec![], 1, true));
        let mut next = active_snapshot(3);
        next.account_id = Some("next".to_string());
        mock.push_page(0, page(vec![SnapshotRecord::Key(next.clone())], 3, false));
        let path = cache_path("reset");
        let (store, task) =
            SnapshotStore::spawn(&config(&mock, path.clone(), 10), CancellationToken::new())
                .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            while store.get(KEY_ID).is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            while store.view().cursor < 3 {
                assert!(store.get(KEY_ID).is_some());
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(
            store.get(KEY_ID).unwrap().account_id.as_deref(),
            Some("next")
        );
        task.abort();
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn resolve_budget_exhaustion_fails_closed_and_negative_cache_suppresses_repeats() {
        std::env::set_var("PORTAL_STORE_TEST_TOKEN", SERVICE_TOKEN);
        let mock = MockControlPlane::spawn().await;
        let path = cache_path("resolve");
        let config = config(&mock, path.clone(), 1);
        let (store, task) = SnapshotStore::spawn(&config, CancellationToken::new()).unwrap();
        store.inner.ready.store(true, Ordering::Release);

        mock.resolve_with("missing", None);
        assert!(store.get_or_resolve("missing").await.is_none());
        assert!(store.get_or_resolve("missing").await.is_none());
        assert_eq!(mock.resolve_calls(), vec!["missing".to_string()]);

        assert!(store.get_or_resolve("rate-limited").await.is_none());
        assert_eq!(mock.resolve_calls(), vec!["missing".to_string()]);

        task.abort();
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn resolve_server_error_and_network_error_fail_closed() {
        std::env::set_var("PORTAL_STORE_TEST_TOKEN", SERVICE_TOKEN);
        let mock = MockControlPlane::spawn().await;
        mock.resolve_status("server-error", StatusCode::INTERNAL_SERVER_ERROR);
        let path = cache_path("resolve-5xx");
        let (store, task) =
            SnapshotStore::spawn(&config(&mock, path.clone(), 10), CancellationToken::new())
                .unwrap();
        store.inner.ready.store(true, Ordering::Release);

        assert!(store.get_or_resolve("server-error").await.is_none());
        assert_eq!(mock.resolve_calls(), vec!["server-error".to_string()]);
        task.abort();
        let _ = tokio::fs::remove_file(path).await;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let network_path = cache_path("resolve-network-error");
        let mut config = config(&mock, network_path.clone(), 10);
        config.control_plane_url = format!("http://{addr}").parse().unwrap();
        let (store, task) = SnapshotStore::spawn(&config, CancellationToken::new()).unwrap();
        store.inner.ready.store(true, Ordering::Release);

        assert!(store.get_or_resolve("network-error").await.is_none());

        task.abort();
        let _ = tokio::fs::remove_file(network_path).await;
    }

    #[tokio::test]
    async fn resolve_hit_inserts_record() {
        std::env::set_var("PORTAL_STORE_TEST_TOKEN", SERVICE_TOKEN);
        let mock = MockControlPlane::spawn().await;
        let mut resolved = active_snapshot(42);
        resolved.key_id = "fresh-key".to_string();
        mock.resolve_with("fresh-key", Some(resolved.clone()));
        let path = cache_path("resolve-hit");
        let (store, task) =
            SnapshotStore::spawn(&config(&mock, path.clone(), 10), CancellationToken::new())
                .unwrap();
        store.inner.ready.store(true, Ordering::Release);

        let record = store.get_or_resolve("fresh-key").await.unwrap();

        assert_eq!(record.key_id, "fresh-key");
        assert_eq!(store.get("fresh-key").unwrap().seq, 42);
        assert_eq!(mock.resolve_calls(), vec!["fresh-key".to_string()]);
        assert_eq!(
            mock.resolve_bodies(),
            vec![serde_json::json!({ "key_id": "fresh-key" })]
        );
        let body = mock.resolve_bodies().pop().unwrap();
        assert!(body.get("secret").is_none());
        assert!(body.get("secret_sha256").is_none());
        assert!(body.get("hash").is_none());

        task.abort();
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn resolve_hit_grants_when_disk_cache_cannot_persist() {
        std::env::set_var("PORTAL_STORE_TEST_TOKEN", SERVICE_TOKEN);
        let mock = MockControlPlane::spawn().await;
        let mut resolved = active_snapshot(42);
        resolved.key_id = "fresh-key".to_string();
        mock.resolve_with("fresh-key", Some(resolved));

        let blocking_file = cache_path("cache-parent-file");
        tokio::fs::write(&blocking_file, b"not a directory")
            .await
            .unwrap();
        let path = blocking_file.join("snapshots.json");
        let (store, task) =
            SnapshotStore::spawn(&config(&mock, path, 10), CancellationToken::new()).unwrap();
        store.inner.ready.store(true, Ordering::Release);

        let record = store.get_or_resolve("fresh-key").await;

        assert_eq!(record.unwrap().key_id, "fresh-key");
        assert_eq!(store.get("fresh-key").unwrap().seq, 42);
        assert_eq!(mock.resolve_calls(), vec!["fresh-key".to_string()]);

        task.abort();
        let _ = tokio::fs::remove_file(blocking_file).await;
    }
}
