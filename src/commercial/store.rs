use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use arc_swap::ArcSwap;
use dashmap::{mapref::entry::Entry, DashMap};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::time::Instant as TokioInstant;
use tokio::{sync::OwnedSemaphorePermit, task::JoinHandle};
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
const MAX_BOOTSTRAP_PAGES: usize = 10_000;
const MAX_RESOLVE_FOLLOWERS: usize = 64;
const RESOLVE_FOLLOWER_TIMEOUT: Duration = Duration::from_secs(11);

#[derive(Clone)]
pub struct SnapshotStore {
    inner: Arc<SnapshotStoreInner>,
}

struct SnapshotStoreInner {
    // Lock order: authoritative_lock -> records ArcSwap CAS -> tally reset_lock,
    // with registry operations last. Code holding a tally or record-local lock
    // must never acquire authoritative_lock, so replacement and resolve cannot
    // deadlock while serializing their externally visible side effects.
    records: ArcSwap<HashMap<String, Arc<KeySnapshot>>>,
    defaults: ArcSwap<Defaults>,
    defaults_pending_authoritative_replace: AtomicBool,
    generation: AtomicU64,
    authoritative_lock: RwLock<()>,
    cursor: AtomicU64,
    epoch: Mutex<Option<String>>,
    ready: AtomicBool,
    loaded_from_cache: AtomicBool,
    disk_cache_dirty: AtomicBool,
    last_sync_success: Mutex<Option<TokioInstant>>,
    cache_path: PathBuf,
    sync_interval: Duration,
    negative_cache_ttl: Duration,
    negative_cache: DashMap<String, NegativeCacheEntry>,
    stale_quota_cooldowns: DashMap<String, Instant>,
    inflight_resolves: DashMap<String, Arc<ResolveFlight>>,
    inflight_resolve_permits: Arc<tokio::sync::Semaphore>,
    resolve_limiter: Mutex<ResolveLimiter>,
    client: reqwest::Client,
    control_plane_url: url::Url,
    service_token: String,
    hooks: SnapshotHooks,
    #[cfg(test)]
    resolve_application_gate: Mutex<ResolveApplicationGate>,
    #[cfg(test)]
    persist_steps: Mutex<Vec<&'static str>>,
}

#[cfg(test)]
#[derive(Default)]
struct ResolveApplicationGate {
    reached: Option<std::sync::mpsc::Sender<()>>,
    resume: Option<std::sync::mpsc::Receiver<()>>,
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
    pub(crate) generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotPage {
    records: Vec<SnapshotRecord>,
    next_cursor: u64,
    #[serde(default)]
    reset: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    epoch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    head_seq: Option<u64>,
    #[serde(skip)]
    raw_record_count: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct RawSnapshotPage {
    records: Vec<serde_json::Value>,
    next_cursor: u64,
    #[serde(default)]
    reset: bool,
    epoch: Option<String>,
    head_seq: Option<u64>,
}

struct ParsedKeySnapshot {
    record: KeySnapshot,
    fail_closed: bool,
}

#[derive(Debug, Serialize)]
struct ResolveRequest<'a> {
    key_id: &'a str,
}

#[derive(Debug, Serialize, Deserialize)]
struct DiskCache {
    cursor: u64,
    #[serde(default)]
    epoch: Option<String>,
    records: Vec<serde_json::Value>,
    defaults: Defaults,
    #[serde(default)]
    defaults_pending_authoritative_replace: bool,
    saved_at: u64,
}

#[derive(Debug, Clone, Copy)]
enum BootstrapMode {
    Normal,
    Authoritative,
    RecordsOnlyAuthoritative,
    DefaultsRecovery,
}

impl BootstrapMode {
    fn is_authoritative(self) -> bool {
        matches!(self, Self::Authoritative | Self::RecordsOnlyAuthoritative)
    }

    fn is_full_authoritative(self) -> bool {
        matches!(self, Self::Authoritative)
    }
}

#[derive(Debug)]
enum AuthoritativeResetReason {
    EpochChanged { stored: String, received: String },
    ResetWithUnknownEpoch { received: String },
    HeadRolledBack { cursor: u64, head_seq: u64 },
}

struct KeyRecordHooks {
    changed: Vec<Arc<KeySnapshot>>,
    removed_key_ids: Vec<String>,
}

struct NegativeCacheEntry {
    expires_at: Instant,
    generation: u64,
}

struct ResolveFlight {
    result: Mutex<Option<Option<Arc<KeySnapshot>>>>,
    notify: tokio::sync::Notify,
    follower_permits: Arc<tokio::sync::Semaphore>,
    _global_permit: OwnedSemaphorePermit,
}

struct ResolveFlightGuard<'a> {
    store: &'a SnapshotStore,
    key_id: String,
    flight: Arc<ResolveFlight>,
    completed: bool,
}

impl Drop for ResolveFlightGuard<'_> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let mut result = self.flight.result.lock().unwrap();
        if result.is_none() {
            *result = Some(None);
        }
        drop(result);
        self.store.remove_resolve_flight(&self.key_id, &self.flight);
        self.flight.notify.notify_waiters();
    }
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
                defaults_pending_authoritative_replace: AtomicBool::new(false),
                generation: AtomicU64::new(0),
                authoritative_lock: RwLock::new(()),
                cursor: AtomicU64::new(0),
                epoch: Mutex::new(None),
                ready: AtomicBool::new(false),
                loaded_from_cache: AtomicBool::new(false),
                disk_cache_dirty: AtomicBool::new(false),
                last_sync_success: Mutex::new(Some(sync_task_start)),
                cache_path: config.snapshot_cache_path.clone(),
                sync_interval: Duration::from_secs(config.sync_interval_secs.max(1)),
                negative_cache_ttl: Duration::from_secs(config.negative_cache_secs.max(1)),
                negative_cache: DashMap::new(),
                stale_quota_cooldowns: DashMap::new(),
                inflight_resolves: DashMap::new(),
                inflight_resolve_permits: Arc::new(tokio::sync::Semaphore::new(
                    config.max_inflight_resolves,
                )),
                resolve_limiter: Mutex::new(ResolveLimiter::new(config.resolve_rate_per_sec)),
                client,
                control_plane_url: config.control_plane_url.clone(),
                service_token,
                hooks,
                #[cfg(test)]
                resolve_application_gate: Mutex::new(ResolveApplicationGate::default()),
                #[cfg(test)]
                persist_steps: Mutex::new(Vec::new()),
            }),
        })
    }

    pub fn inactive(fallback: &PublicFallbackConfig) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(SnapshotStoreInner {
                records: ArcSwap::from_pointee(HashMap::new()),
                defaults: ArcSwap::from_pointee(defaults_from_fallback(fallback)),
                defaults_pending_authoritative_replace: AtomicBool::new(false),
                generation: AtomicU64::new(0),
                authoritative_lock: RwLock::new(()),
                cursor: AtomicU64::new(0),
                epoch: Mutex::new(None),
                ready: AtomicBool::new(true),
                loaded_from_cache: AtomicBool::new(false),
                disk_cache_dirty: AtomicBool::new(false),
                last_sync_success: Mutex::new(Some(TokioInstant::now())),
                cache_path: PathBuf::new(),
                sync_interval: Duration::from_secs(10),
                negative_cache_ttl: Duration::from_secs(15),
                negative_cache: DashMap::new(),
                stale_quota_cooldowns: DashMap::new(),
                inflight_resolves: DashMap::new(),
                inflight_resolve_permits: Arc::new(tokio::sync::Semaphore::new(
                    super::config::DEFAULT_MAX_INFLIGHT_RESOLVES,
                )),
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
                #[cfg(test)]
                resolve_application_gate: Mutex::new(ResolveApplicationGate::default()),
                #[cfg(test)]
                persist_steps: Mutex::new(Vec::new()),
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
        let _authoritative_guard = self.inner.authoritative_lock.read().unwrap();
        StoreView {
            records: self.inner.records.load_full(),
            defaults: self.inner.defaults.load_full(),
            cursor: self.inner.cursor.load(Ordering::Relaxed),
            generation: self.inner.generation.load(Ordering::Acquire),
        }
    }

    pub(crate) fn with_authoritative_generation<R>(&self, operation: impl FnOnce(u64) -> R) -> R {
        let _authoritative_guard = self.inner.authoritative_lock.read().unwrap();
        operation(self.inner.generation.load(Ordering::Acquire))
    }

    pub fn get(&self, key_id: &str) -> Option<Arc<KeySnapshot>> {
        self.inner.records.load().get(key_id).cloned()
    }

    pub async fn get_or_resolve(&self, key_id: &str) -> Option<Arc<KeySnapshot>> {
        self.get_or_resolve_inner(key_id, true).await
    }

    pub(crate) fn schedule_stale_quota_resolve(&self, key_id: &str) {
        let now = Instant::now();
        let next_attempt = now + self.inner.negative_cache_ttl;
        match self.inner.stale_quota_cooldowns.entry(key_id.to_string()) {
            Entry::Occupied(mut entry) => {
                if *entry.get() > now {
                    return;
                }
                entry.insert(next_attempt);
            }
            Entry::Vacant(entry) => {
                entry.insert(next_attempt);
            }
        }

        let store = self.clone();
        let key_id = key_id.to_string();
        tokio::spawn(async move {
            let _ = store.get_or_resolve_inner(&key_id, false).await;
        });
    }

    async fn get_or_resolve_inner(
        &self,
        key_id: &str,
        return_cached: bool,
    ) -> Option<Arc<KeySnapshot>> {
        loop {
            if return_cached {
                if let Some(record) = self.get(key_id) {
                    return Some(record);
                }
            }
            let (flight, leader, follower_permit) = match self
                .inner
                .inflight_resolves
                .entry(key_id.to_string())
            {
                Entry::Occupied(entry) => {
                    let flight = entry.get().clone();
                    let Ok(permit) = flight.follower_permits.clone().try_acquire_owned() else {
                        tracing::info!(
                            key_id,
                            "commercial snapshot resolve fail-closed: follower limit reached"
                        );
                        metrics::report_commercial_resolve("rate_limited");
                        return None;
                    };
                    (flight, false, Some(permit))
                }
                Entry::Vacant(entry) => {
                    let Ok(global_permit) = self
                        .inner
                        .inflight_resolve_permits
                        .clone()
                        .try_acquire_owned()
                    else {
                        tracing::info!(
                            key_id,
                            "commercial snapshot resolve fail-closed: global flight limit reached"
                        );
                        metrics::report_commercial_resolve("global_flight_limited");
                        return None;
                    };
                    let flight = Arc::new(ResolveFlight {
                        result: Mutex::new(None),
                        notify: tokio::sync::Notify::new(),
                        follower_permits: Arc::new(tokio::sync::Semaphore::new(
                            MAX_RESOLVE_FOLLOWERS,
                        )),
                        _global_permit: global_permit,
                    });
                    entry.insert(flight.clone());
                    (flight, true, None)
                }
            };
            if leader {
                let mut guard = ResolveFlightGuard {
                    store: self,
                    key_id: key_id.to_string(),
                    flight: flight.clone(),
                    completed: false,
                };
                let result = self.resolve(key_id).await.ok().flatten();
                *flight.result.lock().unwrap() = Some(result.clone());
                self.remove_resolve_flight(key_id, &flight);
                guard.completed = true;
                flight.notify.notify_waiters();
                return result;
            }
            let _follower_permit = follower_permit.expect("followers acquire a permit");
            let notified = flight.notify.notified();
            if let Some(result) = flight.result.lock().unwrap().clone() {
                return result;
            }
            if tokio::time::timeout(RESOLVE_FOLLOWER_TIMEOUT, notified)
                .await
                .is_err()
            {
                tracing::info!(
                    key_id,
                    "commercial snapshot resolve follower timed out; failing closed"
                );
                metrics::report_commercial_resolve("follower_timeout");
                return None;
            }
            if let Some(result) = flight.result.lock().unwrap().clone() {
                return result;
            };
        }
    }

    fn remove_resolve_flight(&self, key_id: &str, flight: &Arc<ResolveFlight>) {
        if let Entry::Occupied(entry) = self.inner.inflight_resolves.entry(key_id.to_string()) {
            if Arc::ptr_eq(entry.get(), flight) {
                entry.remove();
            }
        }
    }

    pub(crate) fn sweep_negative_cache(&self) -> usize {
        let now = Instant::now();
        let mut removed = 0;
        self.inner.negative_cache.retain(|_, entry| {
            let expired = entry.expires_at <= now;
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
        self.inner.negative_cache.insert(
            key_id.to_string(),
            NegativeCacheEntry {
                expires_at: Instant::now() + ttl,
                generation: self.inner.generation.load(Ordering::Acquire),
            },
        );
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
    fn pause_resolve_application_for_test(
        &self,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (reached_tx, reached_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        *self.inner.resolve_application_gate.lock().unwrap() = ResolveApplicationGate {
            reached: Some(reached_tx),
            resume: Some(resume_rx),
        };
        (reached_rx, resume_tx)
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
            if let Err(err) = self.bootstrap(BootstrapMode::Normal, None).await {
                Err(err)
            } else {
                Ok(())
            }
        } else {
            let defaults_recovery = if self
                .inner
                .defaults_pending_authoritative_replace
                .load(Ordering::Acquire)
            {
                metrics::report_commercial_defaults_recovery_pending_tick();
                tracing::warn!(
                    current_seq = self.inner.defaults.load().seq,
                    "authoritative commercial defaults recovery remains pending; retrying bootstrap"
                );
                let epoch = self.inner.epoch.lock().unwrap().clone();
                self.bootstrap(BootstrapMode::DefaultsRecovery, epoch).await
            } else {
                Ok(())
            };
            let sync = self.sync_once().await;
            defaults_recovery.and(sync)
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

    async fn bootstrap(
        &self,
        mode: BootstrapMode,
        fallback_epoch: Option<String>,
    ) -> anyhow::Result<()> {
        let mut cursor = 0;
        let mut all = Vec::new();
        let mut pages = 0usize;
        let mut attempt_pages = 0usize;
        let mut epoch = fallback_epoch;
        loop {
            if pages >= MAX_BOOTSTRAP_PAGES {
                tracing::warn!(
                    pages,
                    cursor,
                    "aborting commercial snapshot bootstrap after too many pages"
                );
                anyhow::bail!("snapshot bootstrap exceeded {MAX_BOOTSTRAP_PAGES} pages");
            }
            let page = self.fetch_page(cursor).await?;
            pages += 1;

            let epoch_changed = attempt_pages > 0
                && page
                    .epoch
                    .as_ref()
                    .zip(epoch.as_ref())
                    .is_some_and(|(received, expected)| received != expected);
            let reset_mid_bootstrap = attempt_pages > 0 && page.reset;
            let head_rolled_back =
                attempt_pages > 0 && page.head_seq.is_some_and(|head_seq| cursor > head_seq);
            if epoch_changed || reset_mid_bootstrap || head_rolled_back {
                tracing::warn!(
                    cursor,
                    received_epoch = ?page.epoch,
                    expected_epoch = ?epoch,
                    reset = page.reset,
                    head_seq = ?page.head_seq,
                    "commercial snapshot bootstrap metadata changed; restarting from cursor zero"
                );
                cursor = 0;
                all.clear();
                attempt_pages = 0;
                if page.epoch.is_some() {
                    epoch = page.epoch;
                }
                continue;
            }
            if let Some(page_epoch) = page.epoch.clone() {
                epoch = Some(page_epoch);
            }
            attempt_pages += 1;
            let next_cursor = page.next_cursor;
            let len = page.raw_record_count;
            all.extend(page.records);
            if len < usize::from(DEFAULT_PAGE_LIMIT) {
                cursor = next_cursor;
                break;
            }
            if next_cursor <= cursor {
                tracing::warn!(
                    cursor,
                    next_cursor,
                    len,
                    "aborting commercial snapshot bootstrap after stalled cursor"
                );
                anyhow::bail!("snapshot bootstrap cursor did not advance");
            }
            cursor = next_cursor;
        }
        self.replace_records_with_mode(all, cursor, mode)?;
        match mode {
            BootstrapMode::Normal => self.observe_epoch(epoch),
            BootstrapMode::Authoritative | BootstrapMode::RecordsOnlyAuthoritative => {
                self.set_epoch(epoch);
                if let Err(err) = self.persist_disk_cache().await {
                    metrics::report_commercial_snapshot_persist_error();
                    tracing::warn!(
                        error = %err,
                        "authoritative commercial snapshot cache persist failed"
                    );
                    self.remove_stale_cache_after_authoritative_persist_failure()
                        .await;
                }
            }
            BootstrapMode::DefaultsRecovery => {}
        }
        self.inner.ready.store(true, Ordering::Release);
        tracing::info!(
            cursor,
            authoritative = mode.is_authoritative(),
            "commercial snapshot bootstrap applied"
        );
        Ok(())
    }

    async fn sync_once(&self) -> anyhow::Result<()> {
        let cursor = self.inner.cursor.load(Ordering::Relaxed);
        let page = self.fetch_page(cursor).await?;
        if let Some(reason) = self.authoritative_reset_reason(cursor, &page) {
            match &reason {
                AuthoritativeResetReason::EpochChanged { stored, received } => {
                    tracing::warn!(
                        cursor,
                        stored_epoch = stored,
                        received_epoch = received,
                        "commercial snapshot feed epoch changed; authoritative re-bootstrap requested"
                    );
                }
                AuthoritativeResetReason::ResetWithUnknownEpoch { received } => {
                    tracing::warn!(
                        cursor,
                        received_epoch = received,
                        "commercial snapshot reset arrived without a stored epoch; authoritative re-bootstrap requested"
                    );
                }
                AuthoritativeResetReason::HeadRolledBack { cursor, head_seq } => {
                    tracing::warn!(
                        cursor,
                        head_seq,
                        "commercial snapshot feed head rolled back; authoritative re-bootstrap requested"
                    );
                }
            }
            // A reset with no stored epoch is normally a retention reset from a
            // legacy cache, so versions remain continuous and global tally/stream
            // hooks would be needless disruption. If a real restore coincides
            // with such a cache, the runbook epoch bump is what upgrades the next
            // comparison to a full authoritative reset.
            let mode = if matches!(
                reason,
                AuthoritativeResetReason::ResetWithUnknownEpoch { .. }
            ) {
                BootstrapMode::RecordsOnlyAuthoritative
            } else {
                BootstrapMode::Authoritative
            };
            self.bootstrap(mode, page.epoch).await?;
            return Ok(());
        }
        if page.reset {
            // Reset means the local cursor fell behind the feed's retained
            // range, not a once-in-a-lifetime control-plane event. Quota churn
            // can advance the minimum retained sequence quickly, so a lagging
            // pod may legitimately re-bootstrap more often than expected.
            tracing::info!(cursor, "commercial snapshot cursor reset requested");
            self.bootstrap(BootstrapMode::Normal, page.epoch).await?;
            return Ok(());
        }
        self.observe_epoch(page.epoch.clone());
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
            records: parse_snapshot_records(page.records, "snapshot_page")?,
            next_cursor: page.next_cursor,
            reset: page.reset,
            epoch: page.epoch,
            head_seq: page.head_seq,
            raw_record_count,
        })
    }

    fn authoritative_reset_reason(
        &self,
        cursor: u64,
        page: &SnapshotPage,
    ) -> Option<AuthoritativeResetReason> {
        if let Some(head_seq) = page.head_seq {
            if cursor > head_seq {
                return Some(AuthoritativeResetReason::HeadRolledBack { cursor, head_seq });
            }
        }

        let received = page.epoch.as_ref()?;
        match self.inner.epoch.lock().unwrap().clone() {
            Some(stored) if stored.as_str() != received.as_str() => {
                Some(AuthoritativeResetReason::EpochChanged {
                    stored,
                    received: received.clone(),
                })
            }
            None if page.reset => Some(AuthoritativeResetReason::ResetWithUnknownEpoch {
                received: received.clone(),
            }),
            _ => None,
        }
    }

    fn observe_epoch(&self, epoch: Option<String>) {
        let Some(epoch) = epoch else {
            return;
        };
        let mut stored = self.inner.epoch.lock().unwrap();
        if stored.as_deref() == Some(epoch.as_str()) {
            return;
        }
        *stored = Some(epoch);
        drop(stored);
        self.mark_disk_cache_dirty();
    }

    fn set_epoch(&self, epoch: Option<String>) {
        let mut stored = self.inner.epoch.lock().unwrap();
        if *stored == epoch {
            return;
        }
        *stored = epoch;
        drop(stored);
        self.mark_disk_cache_dirty();
    }

    async fn resolve(&self, key_id: &str) -> anyhow::Result<Option<Arc<KeySnapshot>>> {
        let generation = self.inner.generation.load(Ordering::Acquire);
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
                let parsed = match parse_key_snapshot(value, "resolve_response", key_id) {
                    Ok(parsed) => parsed,
                    Err(err) => {
                        metrics::report_commercial_resolve("parse_error");
                        return Err(err);
                    }
                };
                let fail_closed = parsed.fail_closed;
                let record = parsed.record;
                if fail_closed {
                    metrics::report_commercial_resolve("parse_error");
                }
                #[cfg(test)]
                self.wait_at_resolve_application_gate_for_test();
                let _authoritative_guard = self.inner.authoritative_lock.read().unwrap();
                let Some(record) = self.upsert_key_for_generation(record, generation)? else {
                    metrics::report_commercial_resolve("stale_generation");
                    return Ok(None);
                };
                if !fail_closed {
                    metrics::report_commercial_resolve("hit");
                }
                Ok(Some(record))
            }
            StatusCode::NOT_FOUND => {
                if self.inner.generation.load(Ordering::Acquire) != generation {
                    metrics::report_commercial_resolve("stale_generation");
                    return Ok(None);
                }
                let expires_at = Instant::now() + self.inner.negative_cache_ttl;
                if !self.insert_negative_cache_for_generation(key_id, generation, expires_at) {
                    metrics::report_commercial_resolve("stale_generation");
                    return Ok(None);
                }
                metrics::report_commercial_resolve("not_found");
                Ok(None)
            }
            status => {
                metrics::report_commercial_resolve("error");
                anyhow::bail!("resolve returned status {status}")
            }
        }
    }

    #[cfg(test)]
    fn replace_records(&self, records: Vec<SnapshotRecord>, cursor: u64) -> anyhow::Result<()> {
        self.replace_records_with_mode(records, cursor, BootstrapMode::Normal)
    }

    fn replace_records_with_mode(
        &self,
        records: Vec<SnapshotRecord>,
        cursor: u64,
        mode: BootstrapMode,
    ) -> anyhow::Result<()> {
        if matches!(mode, BootstrapMode::DefaultsRecovery) {
            if let Some(defaults) = records
                .into_iter()
                .filter_map(|record| match record {
                    SnapshotRecord::Defaults(record) => Some(Defaults::from(record)),
                    SnapshotRecord::Key(_) => None,
                })
                .next_back()
            {
                self.store_defaults(defaults);
                self.mark_disk_cache_dirty();
            }
            return Ok(());
        }
        let _authoritative_guard = mode
            .is_authoritative()
            .then(|| self.inner.authoritative_lock.write().unwrap());
        let mut defaults = None;
        let mut records_map = HashMap::new();
        for record in records {
            match record {
                SnapshotRecord::Defaults(record) => defaults = Some(Defaults::from(record)),
                SnapshotRecord::Key(record) => {
                    let record = Arc::new(record);
                    records_map.insert(record.key_id.clone(), record);
                }
            }
        }
        if let Some(defaults) = defaults {
            self.store_defaults_with_mode(defaults, mode);
        } else if mode.is_authoritative() {
            self.inner
                .defaults_pending_authoritative_replace
                .store(true, Ordering::Release);
            tracing::warn!(
                current_seq = self.inner.defaults.load().seq,
                "authoritative commercial snapshot contained no defaults; keeping current defaults"
            );
        }
        if mode.is_authoritative() {
            self.inner.generation.fetch_add(1, Ordering::AcqRel);
        }
        let hooks = self.replace_key_records(records_map, cursor, mode);
        if mode.is_full_authoritative() {
            self.apply_authoritative_hooks();
        }
        for record in hooks.changed {
            self.apply_snapshot_hooks(&record);
        }
        for key_id in hooks.removed_key_ids {
            self.apply_removed_key_hook(&key_id);
        }
        metrics::set_commercial_snapshot_records(self.inner.records.load().len() as i64);
        self.inner.cursor.store(cursor, Ordering::Release);
        self.mark_disk_cache_dirty();
        Ok(())
    }

    fn replace_key_records(
        &self,
        records: HashMap<String, Arc<KeySnapshot>>,
        cursor: u64,
        mode: BootstrapMode,
    ) -> KeyRecordHooks {
        let preserve_higher_seq = matches!(mode, BootstrapMode::Normal);
        loop {
            let current = self.inner.records.load();
            let mut map = HashMap::with_capacity(records.len().max(current.len()));
            let mut changed = Vec::new();
            let mut removed_key_ids = Vec::new();

            for (key_id, record) in &records {
                if let Some(existing) = current.get(key_id) {
                    if preserve_higher_seq && existing.seq > record.seq {
                        map.insert(key_id.clone(), existing.clone());
                        continue;
                    }
                }
                map.insert(key_id.clone(), record.clone());
                changed.push(record.clone());
            }

            for (key_id, existing) in current.iter() {
                if map.contains_key(key_id) {
                    continue;
                }
                if preserve_higher_seq && existing.seq > cursor {
                    map.insert(key_id.clone(), existing.clone());
                } else if !preserve_higher_seq {
                    removed_key_ids.push(key_id.clone());
                }
            }

            let previous = self
                .inner
                .records
                .compare_and_swap(&*current, Arc::new(map));
            if Arc::ptr_eq(&*current, &*previous) {
                return KeyRecordHooks {
                    changed,
                    removed_key_ids,
                };
            }
        }
    }

    fn store_defaults(&self, defaults: Defaults) {
        self.store_defaults_with_mode(defaults, BootstrapMode::Normal);
    }

    fn store_defaults_with_mode(&self, defaults: Defaults, mode: BootstrapMode) {
        let defaults = Arc::new(defaults);
        let force_replace = match mode {
            BootstrapMode::Normal => self
                .inner
                .defaults_pending_authoritative_replace
                .swap(false, Ordering::AcqRel),
            BootstrapMode::Authoritative => {
                self.inner
                    .defaults_pending_authoritative_replace
                    .store(false, Ordering::Release);
                true
            }
            BootstrapMode::RecordsOnlyAuthoritative => {
                self.inner
                    .defaults_pending_authoritative_replace
                    .store(false, Ordering::Release);
                true
            }
            BootstrapMode::DefaultsRecovery => unreachable!("handled before defaults storage"),
        };
        let preserve_higher_seq = matches!(mode, BootstrapMode::Normal) && !force_replace;
        loop {
            let current = self.inner.defaults.load();
            if preserve_higher_seq && current.seq > defaults.seq {
                return;
            }
            let previous = self
                .inner
                .defaults
                .compare_and_swap(&*current, defaults.clone());
            if Arc::ptr_eq(&*current, &*previous) {
                return;
            }
        }
    }

    fn apply_key_records(&self, records: &[Arc<KeySnapshot>]) -> Vec<Arc<KeySnapshot>> {
        loop {
            let current = self.inner.records.load();
            let mut map = (**current).clone();
            let mut hook_records = Vec::new();

            for record in records {
                if current
                    .get(&record.key_id)
                    .is_some_and(|existing| existing.seq >= record.seq)
                {
                    continue;
                }
                map.insert(record.key_id.clone(), record.clone());
                hook_records.push(record.clone());
            }

            if hook_records.is_empty() {
                return hook_records;
            }

            let previous = self
                .inner
                .records
                .compare_and_swap(&*current, Arc::new(map));
            if Arc::ptr_eq(&*current, &*previous) {
                return hook_records;
            }
        }
    }

    fn apply_records(&self, records: Vec<SnapshotRecord>, cursor: u64) -> anyhow::Result<()> {
        let mut key_records = Vec::new();
        for record in records {
            match record {
                SnapshotRecord::Defaults(record) => {
                    self.store_defaults(Defaults::from(record));
                }
                SnapshotRecord::Key(record) => {
                    if record.status != KeyStatus::Active {
                        tracing::info!(key_id = record.key_id, "commercial key became non-active");
                    }
                    key_records.push(Arc::new(record));
                }
            }
        }
        let hook_records = self.apply_key_records(&key_records);
        for record in hook_records {
            self.apply_snapshot_hooks(&record);
        }
        metrics::set_commercial_snapshot_records(self.inner.records.load().len() as i64);
        self.inner.cursor.store(cursor, Ordering::Release);
        self.mark_disk_cache_dirty();
        Ok(())
    }

    #[cfg(test)]
    fn upsert_key(&self, record: KeySnapshot) -> anyhow::Result<Arc<KeySnapshot>> {
        let _authoritative_guard = self.inner.authoritative_lock.read().unwrap();
        let generation = self.inner.generation.load(Ordering::Acquire);
        self.upsert_key_for_generation(record, generation)?
            .ok_or_else(|| anyhow::anyhow!("commercial snapshot generation changed during upsert"))
    }

    fn upsert_key_for_generation(
        &self,
        record: KeySnapshot,
        generation: u64,
    ) -> anyhow::Result<Option<Arc<KeySnapshot>>> {
        let record = Arc::new(record);
        loop {
            if self.inner.generation.load(Ordering::Acquire) != generation {
                return Ok(None);
            }
            let current = self.inner.records.load();
            if let Some(existing) = current.get(&record.key_id) {
                if existing.seq >= record.seq {
                    if self.inner.generation.load(Ordering::Acquire) != generation {
                        return Ok(None);
                    }
                    return Ok(Some(existing.clone()));
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

        if self.inner.generation.load(Ordering::Acquire) != generation {
            return Ok(None);
        }

        self.apply_snapshot_hooks(&record);
        metrics::set_commercial_snapshot_records(self.inner.records.load().len() as i64);
        self.mark_disk_cache_dirty();
        Ok(Some(record))
    }

    #[cfg(test)]
    fn wait_at_resolve_application_gate_for_test(&self) {
        let mut gate = self.inner.resolve_application_gate.lock().unwrap();
        if let (Some(reached), Some(resume)) = (gate.reached.take(), gate.resume.take()) {
            reached.send(()).unwrap();
            drop(gate);
            resume.recv().unwrap();
        }
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
                metrics::report_commercial_snapshot_parse_error();
                tracing::warn!(path = %path.display(), error = %err, "failed to load commercial snapshot cache");
                return;
            }
        };
        let mut map = HashMap::new();
        let records = match parse_snapshot_records(cache.records, "disk_cache") {
            Ok(records) => records,
            Err(err) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %err,
                    "refusing commercial snapshot cache with unidentified malformed record"
                );
                return;
            }
        };
        for record in records {
            if let SnapshotRecord::Key(record) = record {
                map.insert(record.key_id.clone(), Arc::new(record));
            }
        }
        self.inner.records.store(Arc::new(map));
        self.inner.defaults.store(Arc::new(cache.defaults));
        self.inner.defaults_pending_authoritative_replace.store(
            cache.defaults_pending_authoritative_replace,
            Ordering::Release,
        );
        self.inner.cursor.store(cache.cursor, Ordering::Release);
        *self.inner.epoch.lock().unwrap() = cache.epoch;
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
            epoch: self.inner.epoch.lock().unwrap().clone(),
            records,
            defaults: (**self.inner.defaults.load()).clone(),
            defaults_pending_authoritative_replace: self
                .inner
                .defaults_pending_authoritative_replace
                .load(Ordering::Acquire),
            saved_at: now_secs(),
        };
        let bytes = serde_json::to_vec(&cache)?;
        let tmp = tmp_path(path);
        #[cfg(test)]
        self.inner.persist_steps.lock().unwrap().push("write");
        let mut file = tokio::fs::File::create(&tmp).await?;
        file.write_all(&bytes).await?;
        file.sync_all().await?;
        #[cfg(test)]
        self.inner.persist_steps.lock().unwrap().push("sync_temp");
        drop(file);
        #[cfg(test)]
        self.inner.persist_steps.lock().unwrap().push("rename");
        tokio::fs::rename(&tmp, path).await?;
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        tokio::fs::File::open(parent).await?.sync_all().await?;
        #[cfg(test)]
        self.inner.persist_steps.lock().unwrap().push("sync_parent");
        Ok(())
    }

    async fn remove_stale_cache_after_authoritative_persist_failure(&self) {
        let path = &self.inner.cache_path;
        if path.as_os_str().is_empty() {
            return;
        }
        if let Err(err) = tokio::fs::remove_file(path).await {
            if err.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    path = %path.display(),
                    error = %err,
                    "failed to remove stale commercial snapshot cache after authoritative persist failure"
                );
            }
        }
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
        let entry_generation = entry.generation;
        if entry.generation == self.inner.generation.load(Ordering::Acquire)
            && entry.expires_at > Instant::now()
        {
            return true;
        }
        drop(entry);
        self.remove_negative_cache_for_generation(key_id, entry_generation);
        false
    }

    fn insert_negative_cache_for_generation(
        &self,
        key_id: &str,
        generation: u64,
        expires_at: Instant,
    ) -> bool {
        match self.inner.negative_cache.entry(key_id.to_string()) {
            Entry::Occupied(mut entry) => {
                if self.inner.generation.load(Ordering::Acquire) != generation
                    || entry.get().generation != generation
                {
                    return false;
                }
                entry.insert(NegativeCacheEntry {
                    expires_at,
                    generation,
                });
            }
            Entry::Vacant(entry) => {
                if self.inner.generation.load(Ordering::Acquire) != generation {
                    return false;
                }
                entry.insert(NegativeCacheEntry {
                    expires_at,
                    generation,
                });
            }
        }
        if self.inner.generation.load(Ordering::Acquire) != generation {
            self.remove_negative_cache_for_generation(key_id, generation);
            return false;
        }
        true
    }

    fn remove_negative_cache_for_generation(&self, key_id: &str, generation: u64) {
        if let Entry::Occupied(entry) = self.inner.negative_cache.entry(key_id.to_string()) {
            if entry.get().generation == generation {
                entry.remove();
            }
        }
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

    fn apply_authoritative_hooks(&self) {
        if let Some(tally) = &self.inner.hooks.tally {
            // Preserve each tally's Arc identity so meters held by active streams
            // keep debiting the same entry used by new admissions. A concurrent
            // debit between this reset and the restored snapshot rebase may be
            // discarded; that is bounded to post-restore over-serving.
            let rebased = tally.force_rebase_all();
            tracing::info!(
                rebased,
                "commercial quota tallies force-rebased for authoritative snapshot replacement"
            );
        }
        self.inner.negative_cache.clear();
        self.inner.stale_quota_cooldowns.clear();
        self.inner.resolve_limiter.lock().unwrap().reset();
        if let Some(registry) = &self.inner.hooks.registry {
            let killed = registry.kill_all();
            tracing::info!(
                killed,
                "commercial streams killed for authoritative snapshot replacement"
            );
        }
    }

    fn apply_removed_key_hook(&self, key_id: &str) {
        if let Some(registry) = &self.inner.hooks.registry {
            let killed = registry.kill_key(key_id);
            if killed > 0 {
                tracing::info!(
                    key_id,
                    killed,
                    "commercial key removed from authoritative snapshot; active streams killed"
                );
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

    fn reset(&mut self) {
        self.tokens = self.capacity;
        self.last = Instant::now();
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
            quota: Some(PublicQuota {
                volume_bytes: Some(config.volume_bytes),
                window_secs: config.window_secs,
                on_exceed: OnExceed::Reject,
            }),
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
) -> anyhow::Result<Vec<SnapshotRecord>> {
    let mut parsed = Vec::with_capacity(records.len());
    for (index, value) in records.into_iter().enumerate() {
        match serde_json::from_value::<SnapshotRecord>(value.clone()) {
            Ok(record) => parsed.push(record),
            Err(err) => {
                metrics::report_commercial_snapshot_parse_error();
                if let Some(tombstone) = fail_closed_tombstone(&value, None) {
                    tracing::warn!(
                        source,
                        index,
                        key_id = tombstone.key_id,
                        seq = tombstone.seq,
                        error = %err,
                        "installing fail-closed tombstone for unparseable commercial snapshot record"
                    );
                    parsed.push(SnapshotRecord::Key(tombstone));
                } else {
                    tracing::warn!(
                        source,
                        index,
                        error = %err,
                        "halting commercial snapshot parsing at unidentified record"
                    );
                    anyhow::bail!(
                        "unparseable commercial snapshot record at index {index} has no usable key_id and seq"
                    );
                }
            }
        }
    }
    Ok(parsed)
}

fn parse_key_snapshot(
    value: serde_json::Value,
    source: &'static str,
    expected_key_id: &str,
) -> anyhow::Result<ParsedKeySnapshot> {
    match serde_json::from_value::<KeySnapshot>(value.clone()) {
        Ok(record) => Ok(ParsedKeySnapshot {
            record,
            fail_closed: false,
        }),
        Err(err) => {
            metrics::report_commercial_snapshot_parse_error();
            if let Some(tombstone) = fail_closed_tombstone(&value, Some(expected_key_id)) {
                tracing::warn!(
                    source,
                    key_id = tombstone.key_id,
                    seq = tombstone.seq,
                    error = %err,
                    "installing fail-closed tombstone for unparseable commercial snapshot resolve record"
                );
                Ok(ParsedKeySnapshot {
                    record: tombstone,
                    fail_closed: true,
                })
            } else {
                tracing::warn!(
                    source,
                    key_id = expected_key_id,
                    error = %err,
                    "failing closed on unparseable commercial snapshot resolve record without seq"
                );
                anyhow::bail!("unparseable commercial snapshot resolve record has no usable seq")
            }
        }
    }
}

fn fail_closed_tombstone(
    value: &serde_json::Value,
    expected_key_id: Option<&str>,
) -> Option<KeySnapshot> {
    let key_id = expected_key_id
        .or_else(|| value.get("key_id")?.as_str())?
        .to_string();
    if key_id.trim().is_empty() || key_id == "__defaults__" {
        return None;
    }
    let seq = value.get("seq")?.as_u64()?;
    Some(KeySnapshot {
        key_id,
        secret_sha256: None,
        account_id: None,
        status: KeyStatus::Revoked,
        expires_at: None,
        limits: None,
        entitlements: None,
        quota: None,
        seq,
    })
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
        resolves: Mutex<HashMap<String, Option<serde_json::Value>>>,
        resolve_statuses: Mutex<HashMap<String, StatusCode>>,
        resolve_gates: Mutex<HashMap<String, Arc<tokio::sync::Notify>>>,
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
            self.state.resolves.lock().unwrap().insert(
                key_id.to_string(),
                record.map(|record| {
                    serde_json::to_value(record).expect("mock resolve record should serialize")
                }),
            );
        }

        pub fn resolve_raw(&self, key_id: &str, record: serde_json::Value) {
            self.state
                .resolves
                .lock()
                .unwrap()
                .insert(key_id.to_string(), Some(record));
        }

        pub fn resolve_status(&self, key_id: &str, status: StatusCode) {
            self.state
                .resolve_statuses
                .lock()
                .unwrap()
                .insert(key_id.to_string(), status);
        }

        pub fn pause_resolve(&self, key_id: &str) -> Arc<tokio::sync::Notify> {
            let gate = Arc::new(tokio::sync::Notify::new());
            self.state
                .resolve_gates
                .lock()
                .unwrap()
                .insert(key_id.to_string(), gate.clone());
            gate
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
                quota: Some(PublicQuota {
                    volume_bytes: Some(400),
                    window_secs: 60,
                    on_exceed: OnExceed::Reject,
                }),
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
            epoch: None,
            head_seq: None,
            raw_record_count,
        }
    }

    pub fn feed_page(
        records: Vec<SnapshotRecord>,
        next_cursor: u64,
        reset: bool,
        epoch: Option<&str>,
        head_seq: Option<u64>,
    ) -> SnapshotPage {
        let mut page = page(records, next_cursor, reset);
        page.epoch = epoch.map(str::to_string);
        page.head_seq = head_seq;
        page
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
    ) -> Result<Json<serde_json::Value>, StatusCode> {
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
        let gate = state.resolve_gates.lock().unwrap().get(&key_id).cloned();
        if let Some(gate) = gate {
            gate.notified().await;
        }
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
        config::{DEFAULT_MAX_INFLIGHT_RESOLVES, DEFAULT_USAGE_MAX_RETRY_AGE_SECS},
        meter::tap_plain_stream,
        Authorization, Credential, Endpoint, Granted, GrantedLimits, MeterHandle, Principal,
        QueryDescriptor, StreamUsageEvent, UsageReporter, UsageStatus,
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

    static STALENESS_GAUGE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    async fn staleness_gauge_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
        STALENESS_GAUGE_TEST_LOCK.lock().await
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
            usage_max_retry_age_secs: DEFAULT_USAGE_MAX_RETRY_AGE_SECS,
            snapshot_cache_path: cache_path,
            resolve_rate_per_sec,
            max_inflight_resolves: DEFAULT_MAX_INFLIGHT_RESOLVES,
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
            usage_max_retry_age_secs: DEFAULT_USAGE_MAX_RETRY_AGE_SECS,
            snapshot_cache_path: cache_path,
            resolve_rate_per_sec: 0,
            max_inflight_resolves: DEFAULT_MAX_INFLIGHT_RESOLVES,
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

    async fn registered_meter_emitted_chunks(
        store: Arc<SnapshotStore>,
        key_id: Option<&str>,
    ) -> usize {
        let reporter = Arc::new(RecordingReporter::default());
        let meter = MeterHandle::new_enforced(
            Granted {
                principal: Principal {
                    account_id: "account".to_string(),
                    api_key_id: key_id.map(str::to_string),
                },
                tally_account_id: None,
                entitled_chains: None,
                entitled_traces: None,
                limits: GrantedLimits::default(),
                on_exceed: OnExceed::Reject,
                quota_version: 1,
                quota_remaining_bytes: Some(1_000),
                snapshot_generation: None,
                concurrency_permit: None,
            },
            "request".to_string(),
            Endpoint::Stream,
            "ethereum-mainnet".to_string(),
            reporter,
            Arc::new(TallyStore::default()),
            Arc::new(ActiveStreamRegistry::default()),
            Some(store),
        );
        tap_plain_stream(
            stream::iter([
                Ok::<_, std::io::Error>(Bytes::from_static(b"first")),
                Ok::<_, std::io::Error>(Bytes::from_static(b"second")),
            ]),
            meter,
        )
        .collect::<Vec<_>>()
        .await
        .len()
    }

    fn authorization_request(credential: Credential) -> crate::commercial::AuthorizeRequest {
        crate::commercial::AuthorizeRequest {
            credential,
            dataset: "ethereum-mainnet".to_string(),
            endpoint: Endpoint::Stream,
            query: QueryDescriptor {
                requires_traces: false,
                requires_statediffs: false,
                first_block: Some(1),
                chain_kind: Some("evm".to_string()),
            },
        }
    }

    async fn grant_for(
        store: &SnapshotStore,
        tally: &TallyStore,
        credential: Credential,
    ) -> Granted {
        match crate::commercial::evaluate::evaluate_with_store(
            store,
            Some(tally),
            None,
            authorization_request(credential),
        )
        .await
        {
            Authorization::Granted(grant) => grant,
            Authorization::Rejected(rejected) => panic!("expected grant, got {rejected:?}"),
        }
    }

    fn meter_from_grant(
        store: Arc<SnapshotStore>,
        tally: Arc<TallyStore>,
        registry: Arc<ActiveStreamRegistry>,
        grant: Granted,
    ) -> MeterHandle {
        MeterHandle::new_enforced(
            grant,
            "request".to_string(),
            Endpoint::Stream,
            "ethereum-mainnet".to_string(),
            Arc::new(RecordingReporter::default()),
            tally,
            registry,
            Some(store),
        )
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
    async fn bootstrap_tombstones_identified_bad_snapshot_records_and_advances_cursor() {
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
        assert_eq!(store.get("poison").unwrap().status, KeyStatus::Revoked);
        assert_eq!(store.get("poison").unwrap().seq, 2);
        assert!(store.get("next-key").is_some());
        assert!(metrics::COMMERCIAL_SNAPSHOT_PARSE_ERRORS.get() > before);
        task.abort();
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn bootstrap_halts_on_bad_snapshot_record_without_identity() {
        let mock = MockControlPlane::spawn().await;
        mock.push_raw_page(
            0,
            serde_json::json!({
                "records": [{"seq": 1, "status": null}],
                "next_cursor": 1,
                "reset": false
            }),
        );
        let store = SnapshotStore::new(
            &config(&mock, PathBuf::new(), 10),
            SnapshotHooks::default(),
            SERVICE_TOKEN.to_string(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );

        assert!(store.run_sync_tick().await.is_err());
        assert!(!store.is_ready());
        assert_eq!(store.view().cursor, 0);
        assert!(store.view().records.is_empty());
    }

    #[tokio::test]
    async fn bootstrap_aborts_on_full_page_with_stalled_cursor() {
        std::env::set_var("PORTAL_STORE_TEST_TOKEN", SERVICE_TOKEN);
        let mock = MockControlPlane::spawn().await;
        let mut full_page = Vec::new();
        for i in 0..DEFAULT_PAGE_LIMIT {
            let mut record = active_snapshot(u64::from(i) + 1);
            record.key_id = format!("key-{i}");
            full_page.push(SnapshotRecord::Key(record));
        }
        mock.push_page(0, page(full_page, 0, false));
        let path = cache_path("stalled-bootstrap-cursor");
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let store = SnapshotStore::new(
            &config(&mock, path.clone(), 10),
            SnapshotHooks::default(),
            SERVICE_TOKEN.to_string(),
            client,
            TokioInstant::now(),
        );

        let err = store
            .bootstrap(BootstrapMode::Normal, None)
            .await
            .expect_err("stalled cursor aborts");

        assert!(err.to_string().contains("cursor did not advance"));
        assert!(!store.is_ready());
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn bootstrap_restarts_when_paginated_epoch_changes() {
        let mock = MockControlPlane::spawn().await;
        let mut epoch_a = active_snapshot(10);
        epoch_a.key_id = "epoch-a-key".to_string();
        epoch_a.account_id = Some("epoch-a".to_string());
        mock.push_page(
            0,
            feed_page(
                vec![SnapshotRecord::Key(epoch_a); usize::from(DEFAULT_PAGE_LIMIT)],
                10,
                false,
                Some("epoch-a"),
                Some(20),
            ),
        );
        let mut mixed_epoch_b = active_snapshot(20);
        mixed_epoch_b.key_id = "mixed-epoch-b-key".to_string();
        mock.push_page(
            10,
            feed_page(
                vec![SnapshotRecord::Key(mixed_epoch_b)],
                20,
                false,
                Some("epoch-b"),
                Some(20),
            ),
        );
        let mut restored = active_snapshot(5);
        restored.key_id = "epoch-b-key".to_string();
        restored.account_id = Some("epoch-b".to_string());
        mock.push_page(
            0,
            feed_page(
                vec![SnapshotRecord::Key(restored)],
                5,
                false,
                Some("epoch-b"),
                Some(5),
            ),
        );
        let store = SnapshotStore::new(
            &config(&mock, PathBuf::new(), 10),
            SnapshotHooks::default(),
            SERVICE_TOKEN.to_string(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );

        store.bootstrap(BootstrapMode::Normal, None).await.unwrap();

        assert_eq!(store.view().cursor, 5);
        assert_eq!(
            store.inner.epoch.lock().unwrap().as_deref(),
            Some("epoch-b")
        );
        assert!(store.get("epoch-a-key").is_none());
        assert!(store.get("mixed-epoch-b-key").is_none());
        assert_eq!(
            store.get("epoch-b-key").unwrap().account_id.as_deref(),
            Some("epoch-b")
        );
    }

    #[tokio::test]
    async fn corrupt_disk_cache_is_ignored_and_bootstrap_continues() {
        let path = cache_path("corrupt");
        tokio::fs::write(&path, b"{not-json").await.unwrap();
        let parse_errors_before = metrics::COMMERCIAL_SNAPSHOT_PARSE_ERRORS.get();

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
        assert!(metrics::COMMERCIAL_SNAPSHOT_PARSE_ERRORS.get() > parse_errors_before);
        task.abort();
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn disk_cache_round_trip_is_ready_without_control_plane() {
        let path = cache_path("round-trip");
        let cache = DiskCache {
            cursor: 2,
            epoch: None,
            records: vec![snapshot_record_to_value(SnapshotRecord::Key(
                active_snapshot(2),
            ))],
            defaults: Defaults::from(defaults_record(1)),
            defaults_pending_authoritative_replace: false,
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
    async fn delta_tombstones_identified_bad_record_and_persists_it() {
        let mock = MockControlPlane::spawn().await;
        mock.push_page(
            0,
            page(vec![SnapshotRecord::Key(active_snapshot(10))], 10, false),
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
                    {"key_id": KEY_ID, "seq": 11, "status": null}
                ],
                "next_cursor": 11,
                "reset": false
            }),
        );

        store.run_sync_tick().await.unwrap();

        assert_eq!(store.view().cursor, 11);
        assert_eq!(store.get(KEY_ID).unwrap().status, KeyStatus::Revoked);
        assert_eq!(store.get(KEY_ID).unwrap().seq, 11);
        assert!(metrics::COMMERCIAL_SNAPSHOT_PARSE_ERRORS.get() > before);

        let restarted = SnapshotStore::new(
            &config(&mock, path.clone(), 10),
            SnapshotHooks::default(),
            SERVICE_TOKEN.to_string(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );
        restarted.load_disk_cache();
        assert_eq!(restarted.view().cursor, 11);
        assert_eq!(restarted.get(KEY_ID).unwrap().status, KeyStatus::Revoked);
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn delta_halts_and_retries_bad_record_without_identity() {
        let mock = MockControlPlane::spawn().await;
        mock.push_page(
            0,
            page(vec![SnapshotRecord::Key(active_snapshot(10))], 10, false),
        );
        let poison_page = serde_json::json!({
            "records": [{"seq": 11, "status": null}],
            "next_cursor": 11,
            "reset": false
        });
        mock.push_raw_page(10, poison_page.clone());
        mock.push_raw_page(10, poison_page);
        let store = SnapshotStore::new(
            &config(&mock, PathBuf::new(), 10),
            SnapshotHooks::default(),
            SERVICE_TOKEN.to_string(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );

        store.run_sync_tick().await.unwrap();
        assert!(store.run_sync_tick().await.is_err());
        assert_eq!(store.view().cursor, 10);
        assert_eq!(store.get(KEY_ID).unwrap().status, KeyStatus::Active);
        assert!(store.run_sync_tick().await.is_err());
        assert_eq!(store.view().cursor, 10);
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

    #[test]
    fn delta_application_keeps_newer_resolved_record() {
        let store = SnapshotStore::inactive(&PublicFallbackConfig {
            throughput_bytes_per_sec: 1,
            burst_bytes: 1,
            max_response_bytes: 1,
            volume_bytes: 1,
            window_secs: 1,
            concurrency: 1,
        });
        let mut resolved = active_snapshot(12);
        resolved.account_id = Some("resolved".to_string());
        store.upsert_key(resolved).unwrap();

        let mut older_delta = active_snapshot(11);
        older_delta.status = KeyStatus::Suspended;
        store
            .apply_records(vec![SnapshotRecord::Key(older_delta)], 11)
            .unwrap();

        let stored = store.get(KEY_ID).unwrap();
        assert_eq!(stored.seq, 12);
        assert_eq!(stored.account_id.as_deref(), Some("resolved"));
        assert_eq!(stored.status, KeyStatus::Active);
    }

    #[test]
    fn replacement_preserves_resolved_records_newer_than_cursor() {
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

        store
            .replace_records(vec![SnapshotRecord::Key(active_snapshot(10))], 10)
            .unwrap();

        assert!(store.get("fresh-key").is_some());
        assert!(store.get(KEY_ID).is_some());
    }

    #[test]
    fn normal_replacement_refuses_lower_seq_defaults() {
        let store = SnapshotStore::inactive(&PublicFallbackConfig {
            throughput_bytes_per_sec: 1,
            burst_bytes: 1,
            max_response_bytes: 1,
            volume_bytes: 1,
            window_secs: 1,
            concurrency: 1,
        });
        store
            .replace_records_with_mode(
                vec![SnapshotRecord::Defaults(defaults_record(100))],
                100,
                BootstrapMode::Normal,
            )
            .unwrap();

        store
            .replace_records_with_mode(
                vec![SnapshotRecord::Defaults(defaults_record(5))],
                5,
                BootstrapMode::Normal,
            )
            .unwrap();

        assert_eq!(store.view().defaults.seq, 100);
    }

    #[test]
    fn authoritative_replacement_accepts_lower_seq_defaults() {
        let store = SnapshotStore::inactive(&PublicFallbackConfig {
            throughput_bytes_per_sec: 1,
            burst_bytes: 1,
            max_response_bytes: 1,
            volume_bytes: 1,
            window_secs: 1,
            concurrency: 1,
        });
        store
            .replace_records_with_mode(
                vec![SnapshotRecord::Defaults(defaults_record(100))],
                100,
                BootstrapMode::Normal,
            )
            .unwrap();

        store
            .replace_records_with_mode(
                vec![SnapshotRecord::Defaults(defaults_record(5))],
                5,
                BootstrapMode::Authoritative,
            )
            .unwrap();

        assert_eq!(store.view().defaults.seq, 5);
    }

    #[test]
    fn authoritative_replacement_without_defaults_keeps_current_defaults() {
        let store = SnapshotStore::inactive(&PublicFallbackConfig {
            throughput_bytes_per_sec: 1,
            burst_bytes: 1,
            max_response_bytes: 1,
            volume_bytes: 1,
            window_secs: 1,
            concurrency: 1,
        });
        store
            .replace_records_with_mode(
                vec![SnapshotRecord::Defaults(defaults_record(100))],
                100,
                BootstrapMode::Normal,
            )
            .unwrap();

        store
            .replace_records_with_mode(vec![], 5, BootstrapMode::Authoritative)
            .unwrap();

        assert_eq!(store.view().defaults.seq, 100);
    }

    #[tokio::test]
    async fn authoritative_replacement_resets_quota_tallies_to_restored_version() {
        let tally = Arc::new(TallyStore::default());
        let store = SnapshotStore::new(
            &offline_config(PathBuf::new()),
            SnapshotHooks {
                tally: Some(tally.clone()),
                registry: None,
            },
            String::new(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );
        store
            .replace_records_with_mode(
                vec![SnapshotRecord::Key(active_snapshot(100))],
                100,
                BootstrapMode::Normal,
            )
            .unwrap();
        tally.debit("account", 100, 9_000);
        tally.debit("anon:198.51.100.7/32", 100, 500);
        let active_stream_tally = tally.handle("account", 100);

        let mut restored = active_snapshot(5);
        restored.limits.as_mut().unwrap().concurrency = None;
        store
            .replace_records_with_mode(
                vec![SnapshotRecord::Key(restored)],
                5,
                BootstrapMode::Authoritative,
            )
            .unwrap();

        assert_eq!(tally.version_for("account"), Some(5));
        assert_eq!(tally.bytes_for("account", 5), 0);
        assert_eq!(tally.version_for("anon:198.51.100.7/32"), Some(0));

        let admission = crate::commercial::evaluate::evaluate_with_store(
            &store,
            Some(&tally),
            None,
            crate::commercial::AuthorizeRequest {
                credential: Credential::Key {
                    key_id: KEY_ID.to_string(),
                    secret_sha256: SECRET_SHA256.to_string(),
                },
                dataset: "ethereum-mainnet".to_string(),
                endpoint: Endpoint::Stream,
                query: QueryDescriptor {
                    requires_traces: false,
                    requires_statediffs: false,
                    first_block: Some(1),
                    chain_kind: Some("evm".to_string()),
                },
            },
        )
        .await;

        match admission {
            Authorization::Granted(grant) => {
                assert_eq!(grant.quota_version, 5);
                assert_eq!(grant.quota_remaining_bytes, Some(10_000));
            }
            Authorization::Rejected(rejected) => {
                panic!("restored quota should admit with its full budget: {rejected:?}")
            }
        }

        active_stream_tally.debit(100, 250);
        let fresh_tally = tally.handle("account", 5);
        assert_eq!(tally.version_for("account"), Some(5));
        assert_eq!(fresh_tally.bytes_for(5), 250);
        assert_eq!(
            fresh_tally.debit_and_effective_remaining(5, 0, 10_000),
            9_750
        );

        tally.rebase_account("account", 6);
        assert_eq!(tally.version_for("account"), Some(6));
        assert_eq!(tally.effective_remaining("account", 6, 9_000), 9_000);
    }

    #[tokio::test]
    async fn epoch_change_authoritatively_replaces_snapshot_and_kills_removed_key() {
        let mock = MockControlPlane::spawn().await;
        let removed_key = "revoked-after-restore";
        let mut removed = active_snapshot(90);
        removed.key_id = removed_key.to_string();
        mock.push_page(
            0,
            feed_page(
                vec![
                    SnapshotRecord::Key(active_snapshot(100)),
                    SnapshotRecord::Key(removed),
                ],
                100,
                false,
                Some("epoch-old"),
                Some(100),
            ),
        );
        mock.push_page(100, feed_page(vec![], 5, false, Some("epoch-new"), Some(5)));
        let mut restored = active_snapshot(5);
        restored.account_id = Some("restored".to_string());
        mock.push_page(
            0,
            feed_page(
                vec![SnapshotRecord::Key(restored.clone())],
                5,
                false,
                Some("epoch-new"),
                Some(5),
            ),
        );
        let registry = Arc::new(ActiveStreamRegistry::default());
        let tally = Arc::new(TallyStore::default());
        let path = cache_path("epoch-authoritative-reset");
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let store = SnapshotStore::new(
            &config(&mock, path.clone(), 10),
            SnapshotHooks {
                tally: Some(tally.clone()),
                registry: Some(registry.clone()),
            },
            SERVICE_TOKEN.to_string(),
            client,
            TokioInstant::now(),
        );

        store.run_sync_tick().await.unwrap();
        assert_eq!(store.view().cursor, 100);
        assert_eq!(
            store.inner.epoch.lock().unwrap().as_deref(),
            Some("epoch-old")
        );
        tally.debit("account", 100, 9_000);

        let kill = Arc::new(AtomicBool::new(false));
        let _registration = registry.register(
            "restore-victim".to_string(),
            Some(removed_key.to_string()),
            kill.clone(),
            Arc::new(AtomicU64::new(0)),
        );

        store.run_sync_tick().await.unwrap();

        assert_eq!(store.view().cursor, 5);
        assert_eq!(
            store.inner.epoch.lock().unwrap().as_deref(),
            Some("epoch-new")
        );
        assert_eq!(store.get(KEY_ID).unwrap().seq, 5);
        assert_eq!(
            store.get(KEY_ID).unwrap().account_id.as_deref(),
            Some("restored")
        );
        assert!(store.get(removed_key).is_none());
        assert!(kill.load(Ordering::Acquire));
        assert_eq!(tally.version_for("account"), Some(0));
        assert_eq!(tally.bytes_for("account", 0), 0);
        assert_eq!(tally.version_for("restored"), Some(5));

        let cache: DiskCache =
            serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
        assert_eq!(cache.cursor, 5);
        assert_eq!(cache.epoch.as_deref(), Some("epoch-new"));
        assert!(cache.records.iter().any(|record| {
            record.get("key_id").and_then(|value| value.as_str()) == Some(KEY_ID)
        }));
        assert!(!cache.records.iter().any(|record| {
            record.get("key_id").and_then(|value| value.as_str()) == Some(removed_key)
        }));
        let _ = tokio::fs::remove_file(path).await;
    }

    #[test]
    fn authoritative_replacement_kills_registered_active_key_stream() {
        let registry = Arc::new(ActiveStreamRegistry::default());
        let store = SnapshotStore::new(
            &offline_config(PathBuf::new()),
            SnapshotHooks {
                tally: None,
                registry: Some(registry.clone()),
            },
            String::new(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );
        store
            .replace_records_with_mode(
                vec![SnapshotRecord::Key(active_snapshot(100))],
                100,
                BootstrapMode::Normal,
            )
            .unwrap();
        let kill = Arc::new(AtomicBool::new(false));
        let _registration = registry.register(
            "active-before-restore".to_string(),
            Some(KEY_ID.to_string()),
            kill.clone(),
            Arc::new(AtomicU64::new(0)),
        );

        store
            .replace_records_with_mode(
                vec![SnapshotRecord::Key(active_snapshot(5))],
                5,
                BootstrapMode::Authoritative,
            )
            .unwrap();

        assert!(kill.load(Ordering::Acquire));
    }

    #[test]
    fn missing_authoritative_defaults_allows_next_lower_sequence_defaults() {
        let store = SnapshotStore::inactive(&PublicFallbackConfig {
            throughput_bytes_per_sec: 1,
            burst_bytes: 1,
            max_response_bytes: 1,
            volume_bytes: 1,
            window_secs: 1,
            concurrency: 1,
        });
        store.store_defaults(Defaults::from(defaults_record(100)));
        store
            .replace_records_with_mode(
                vec![SnapshotRecord::Key(active_snapshot(5))],
                5,
                BootstrapMode::Authoritative,
            )
            .unwrap();

        store
            .apply_records(vec![SnapshotRecord::Defaults(defaults_record(6))], 6)
            .unwrap();

        assert_eq!(store.view().defaults.seq, 6);
    }

    #[tokio::test]
    async fn pending_authoritative_defaults_survive_disk_cache_restart() {
        let path = cache_path("pending-defaults-restart");
        let config = offline_config(path.clone());
        let store = SnapshotStore::new(
            &config,
            SnapshotHooks::default(),
            String::new(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );
        store.store_defaults(Defaults::from(defaults_record(100)));
        store
            .replace_records_with_mode(vec![], 5, BootstrapMode::Authoritative)
            .unwrap();
        store.persist_disk_cache().await.unwrap();

        let restarted = SnapshotStore::new(
            &config,
            SnapshotHooks::default(),
            String::new(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );
        restarted.load_disk_cache();
        restarted
            .apply_records(vec![SnapshotRecord::Defaults(defaults_record(6))], 6)
            .unwrap();

        assert_eq!(restarted.view().defaults.seq, 6);
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn pending_authoritative_defaults_retry_bootstrap_and_report_each_tick() {
        let mock = MockControlPlane::spawn().await;
        mock.push_page(5, page(vec![], 5, false));
        mock.push_page(
            0,
            page(vec![SnapshotRecord::Defaults(defaults_record(5))], 5, false),
        );
        let store = SnapshotStore::new(
            &config(&mock, PathBuf::new(), 10),
            SnapshotHooks::default(),
            SERVICE_TOKEN.to_string(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );
        store.store_defaults(Defaults::from(defaults_record(100)));
        store
            .replace_records_with_mode(vec![], 5, BootstrapMode::Authoritative)
            .unwrap();
        store.set_ready_for_test(true);
        let pending_ticks_before = metrics::COMMERCIAL_DEFAULTS_RECOVERY_PENDING_TICKS.get();

        store.run_sync_tick().await.unwrap();

        assert_eq!(store.view().defaults.seq, 5);
        assert!(metrics::COMMERCIAL_DEFAULTS_RECOVERY_PENDING_TICKS.get() > pending_ticks_before);
    }

    #[tokio::test]
    async fn authoritative_bootstrap_persists_restored_cache_before_return() {
        let mock = MockControlPlane::spawn().await;
        let path = cache_path("authoritative-bootstrap-sync-persist");
        let store = SnapshotStore::new(
            &config(&mock, path.clone(), 10),
            SnapshotHooks::default(),
            SERVICE_TOKEN.to_string(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );
        store.install_records_for_test(vec![SnapshotRecord::Key(active_snapshot(100))], 100);
        store.set_epoch(Some("epoch-old".to_string()));
        store.persist_disk_cache().await.unwrap();

        let mut restored = active_snapshot(5);
        restored.account_id = Some("restored".to_string());
        mock.push_page(
            0,
            feed_page(
                vec![SnapshotRecord::Key(restored)],
                5,
                false,
                Some("epoch-new"),
                Some(5),
            ),
        );

        store
            .bootstrap(BootstrapMode::Authoritative, Some("epoch-new".to_string()))
            .await
            .unwrap();

        let restarted = SnapshotStore::new(
            &config(&mock, path.clone(), 10),
            SnapshotHooks::default(),
            SERVICE_TOKEN.to_string(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );
        restarted.load_disk_cache();

        assert_eq!(restarted.view().cursor, 5);
        assert_eq!(
            restarted.inner.epoch.lock().unwrap().as_deref(),
            Some("epoch-new")
        );
        assert_eq!(
            restarted.get(KEY_ID).unwrap().account_id.as_deref(),
            Some("restored")
        );
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn disk_cache_persist_syncs_temp_before_rename_and_parent_after() {
        let path = cache_path("persist-fsync-order");
        let store = SnapshotStore::new(
            &offline_config(path.clone()),
            SnapshotHooks::default(),
            String::new(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );

        store.persist_disk_cache().await.unwrap();

        assert_eq!(
            *store.inner.persist_steps.lock().unwrap(),
            ["write", "sync_temp", "rename", "sync_parent"]
        );
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn authoritative_persist_failure_removes_pre_restore_cache() {
        let mock = MockControlPlane::spawn().await;
        let path = cache_path("authoritative-persist-failure");
        tokio::fs::write(&path, b"pre-restore-cache").await.unwrap();
        let tmp = tmp_path(&path);
        tokio::fs::create_dir(&tmp).await.unwrap();
        mock.push_page(
            0,
            feed_page(
                vec![SnapshotRecord::Key(active_snapshot(5))],
                5,
                false,
                Some("restored"),
                Some(5),
            ),
        );
        let store = SnapshotStore::new(
            &config(&mock, path.clone(), 10),
            SnapshotHooks::default(),
            SERVICE_TOKEN.to_string(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );

        store
            .bootstrap(BootstrapMode::Authoritative, Some("restored".to_string()))
            .await
            .unwrap();

        assert!(!path.exists());
        assert!(store.is_ready());
        assert_eq!(store.get(KEY_ID).unwrap().seq, 5);
        let _ = tokio::fs::remove_dir(tmp).await;
    }

    #[tokio::test]
    async fn head_seq_rollback_authoritatively_rebootstraps() {
        let mock = MockControlPlane::spawn().await;
        let removed_key = "ahead-of-restored-head";
        let mut removed = active_snapshot(9);
        removed.key_id = removed_key.to_string();
        mock.push_page(
            0,
            page(
                vec![
                    SnapshotRecord::Key(active_snapshot(10)),
                    SnapshotRecord::Key(removed),
                ],
                10,
                false,
            ),
        );
        mock.push_page(10, feed_page(vec![], 3, false, None, Some(3)));
        mock.push_page(
            0,
            feed_page(
                vec![SnapshotRecord::Key(active_snapshot(3))],
                3,
                false,
                None,
                Some(3),
            ),
        );
        let path = cache_path("head-authoritative-reset");
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

        store.run_sync_tick().await.unwrap();

        assert_eq!(store.view().cursor, 3);
        assert_eq!(store.get(KEY_ID).unwrap().seq, 3);
        assert!(store.get(removed_key).is_none());
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn absent_epoch_and_head_seq_do_not_signal_authoritative_reset() {
        let mock = MockControlPlane::spawn().await;
        mock.push_raw_page(
            10,
            serde_json::json!({
                "records": [],
                "next_cursor": 3,
                "reset": false
            }),
        );
        let path = cache_path("absent-feed-metadata");
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let store = SnapshotStore::new(
            &config(&mock, path.clone(), 10),
            SnapshotHooks::default(),
            SERVICE_TOKEN.to_string(),
            client,
            TokioInstant::now(),
        );
        store.install_records_for_test(vec![SnapshotRecord::Key(active_snapshot(10))], 10);

        store.run_sync_tick().await.unwrap();

        assert_eq!(store.view().cursor, 10);
        assert_eq!(store.get(KEY_ID).unwrap().seq, 10);
        assert!(store.inner.epoch.lock().unwrap().is_none());
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn reset_with_received_epoch_and_no_stored_epoch_is_authoritative() {
        let mock = MockControlPlane::spawn().await;
        mock.push_page(
            100,
            feed_page(vec![], 5, true, Some("epoch-new"), Some(100)),
        );
        let mut restored = active_snapshot(5);
        restored.account_id = Some("restored".to_string());
        mock.push_page(
            0,
            feed_page(
                vec![SnapshotRecord::Key(restored)],
                5,
                false,
                Some("epoch-new"),
                Some(5),
            ),
        );
        let tally = Arc::new(TallyStore::default());
        let registry = Arc::new(ActiveStreamRegistry::default());
        let store = SnapshotStore::new(
            &config(&mock, PathBuf::new(), 10),
            SnapshotHooks {
                tally: Some(tally.clone()),
                registry: Some(registry.clone()),
            },
            SERVICE_TOKEN.to_string(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );
        let mut leftover = active_snapshot(90);
        leftover.key_id = "pre-restore-leftover".to_string();
        store.install_records_for_test(
            vec![
                SnapshotRecord::Key(active_snapshot(100)),
                SnapshotRecord::Key(leftover),
            ],
            100,
        );
        assert!(store.inner.epoch.lock().unwrap().is_none());
        tally.debit("account", 100, 9_000);
        let stream_kill = Arc::new(AtomicBool::new(false));
        let _registration = registry.register(
            "legacy-cache-stream".to_string(),
            Some(KEY_ID.to_string()),
            stream_kill.clone(),
            Arc::new(AtomicU64::new(0)),
        );

        store.sync_once().await.unwrap();

        assert_eq!(store.view().cursor, 5);
        assert_eq!(store.get(KEY_ID).unwrap().seq, 5);
        assert_eq!(
            store.get(KEY_ID).unwrap().account_id.as_deref(),
            Some("restored")
        );
        assert!(store.get("pre-restore-leftover").is_none());
        assert!(!stream_kill.load(Ordering::Acquire));
        assert_eq!(tally.version_for("account"), Some(100));
        assert_eq!(tally.bytes_for("account", 100), 9_000);
        assert_eq!(
            store.inner.epoch.lock().unwrap().as_deref(),
            Some("epoch-new")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn sync_staleness_gauge_grows_from_boot_before_first_success() {
        let _guard = staleness_gauge_test_guard().await;
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
        let _guard = staleness_gauge_test_guard().await;
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
                entitled_traces: None,
                limits: GrantedLimits::default(),
                on_exceed: OnExceed::Reject,
                quota_version: 1,
                quota_remaining_bytes: Some(1_000),
                snapshot_generation: None,
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

        assert!(emitted.is_empty());
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
            entitled_traces: None,
            limits: GrantedLimits::default(),
            on_exceed: OnExceed::Reject,
            quota_version: 1,
            quota_remaining_bytes: Some(1_000),
            snapshot_generation: None,
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

        assert!(emitted.is_empty());
        let event = reporter.events.lock().unwrap().pop().unwrap();
        assert_eq!(event.status, UsageStatus::CutSuspended);
    }

    #[tokio::test]
    async fn meter_registration_recheck_kills_key_missing_after_authoritative_replace() {
        let store = SnapshotStore::inactive(&PublicFallbackConfig {
            throughput_bytes_per_sec: 1,
            burst_bytes: 1,
            max_response_bytes: 1,
            volume_bytes: 1,
            window_secs: 1,
            concurrency: 1,
        });
        store.install_records_for_test(vec![SnapshotRecord::Key(active_snapshot(100))], 100);
        store
            .replace_records_with_mode(vec![], 5, BootstrapMode::Authoritative)
            .unwrap();

        assert_eq!(
            registered_meter_emitted_chunks(store, Some(KEY_ID)).await,
            0
        );
    }

    #[tokio::test]
    async fn grant_before_authoritative_reset_cannot_register_or_rebase_restored_tally() {
        let tally = Arc::new(TallyStore::default());
        let registry = Arc::new(ActiveStreamRegistry::default());
        let store = SnapshotStore::new(
            &offline_config(PathBuf::new()),
            SnapshotHooks {
                tally: Some(tally.clone()),
                registry: Some(registry.clone()),
            },
            String::new(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );
        let mut original = active_snapshot(100);
        original.limits.as_mut().unwrap().concurrency = None;
        store.install_records_for_test(vec![SnapshotRecord::Key(original)], 100);
        let grant = grant_for(
            &store,
            &tally,
            Credential::Key {
                key_id: KEY_ID.to_string(),
                secret_sha256: SECRET_SHA256.to_string(),
            },
        )
        .await;

        let mut restored = active_snapshot(5);
        restored.limits.as_mut().unwrap().concurrency = None;
        store
            .replace_records_with_mode(
                vec![SnapshotRecord::Key(restored)],
                5,
                BootstrapMode::Authoritative,
            )
            .unwrap();
        let meter = meter_from_grant(store, tally.clone(), registry, grant);

        assert!(meter.should_stop_after_chunk());
        meter.add_logical_bytes(1_000);
        assert!(meter.should_stop_after_chunk());
        assert_eq!(tally.version_for("account"), Some(5));
        assert_eq!(tally.effective_remaining("account", 5, 10_000), 10_000);
    }

    #[tokio::test]
    async fn anonymous_grant_before_authoritative_reset_cannot_register() {
        let tally = Arc::new(TallyStore::default());
        let registry = Arc::new(ActiveStreamRegistry::default());
        let store = SnapshotStore::new(
            &offline_config(PathBuf::new()),
            SnapshotHooks {
                tally: Some(tally.clone()),
                registry: Some(registry.clone()),
            },
            String::new(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );
        store.set_ready_for_test(true);
        let grant = grant_for(
            &store,
            &tally,
            Credential::None {
                ip_bucket: "198.51.100.7/32".to_string(),
            },
        )
        .await;

        store
            .replace_records_with_mode(vec![], 5, BootstrapMode::Authoritative)
            .unwrap();
        let meter = meter_from_grant(store, tally, registry, grant);

        assert!(meter.should_stop_after_chunk());
    }

    #[tokio::test]
    async fn grant_registered_without_reset_remains_live() {
        let tally = Arc::new(TallyStore::default());
        let registry = Arc::new(ActiveStreamRegistry::default());
        let store = SnapshotStore::new(
            &offline_config(PathBuf::new()),
            SnapshotHooks::default(),
            String::new(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );
        let mut snapshot = active_snapshot(5);
        snapshot.limits.as_mut().unwrap().concurrency = None;
        store.install_records_for_test(vec![SnapshotRecord::Key(snapshot)], 5);
        let grant = grant_for(
            &store,
            &tally,
            Credential::Key {
                key_id: KEY_ID.to_string(),
                secret_sha256: SECRET_SHA256.to_string(),
            },
        )
        .await;

        let meter = meter_from_grant(store, tally, registry, grant);

        assert!(!meter.should_stop_after_chunk());
    }

    #[tokio::test]
    async fn grant_between_successive_authoritative_resets_is_stale_for_second_reset() {
        let tally = Arc::new(TallyStore::default());
        let registry = Arc::new(ActiveStreamRegistry::default());
        let store = SnapshotStore::new(
            &offline_config(PathBuf::new()),
            SnapshotHooks {
                tally: Some(tally.clone()),
                registry: Some(registry.clone()),
            },
            String::new(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );
        let mut original = active_snapshot(100);
        original.limits.as_mut().unwrap().concurrency = None;
        store.install_records_for_test(vec![SnapshotRecord::Key(original)], 100);
        let mut first_restored = active_snapshot(20);
        first_restored.limits.as_mut().unwrap().concurrency = None;
        store
            .replace_records_with_mode(
                vec![SnapshotRecord::Key(first_restored)],
                20,
                BootstrapMode::Authoritative,
            )
            .unwrap();
        let grant = grant_for(
            &store,
            &tally,
            Credential::Key {
                key_id: KEY_ID.to_string(),
                secret_sha256: SECRET_SHA256.to_string(),
            },
        )
        .await;

        let mut second_restored = active_snapshot(5);
        second_restored.limits.as_mut().unwrap().concurrency = None;
        store
            .replace_records_with_mode(
                vec![SnapshotRecord::Key(second_restored)],
                5,
                BootstrapMode::Authoritative,
            )
            .unwrap();
        let meter = meter_from_grant(store, tally, registry, grant);

        assert!(meter.should_stop_after_chunk());
    }

    #[tokio::test]
    async fn meter_registration_recheck_leaves_anonymous_stream_alive() {
        let store = SnapshotStore::inactive(&PublicFallbackConfig {
            throughput_bytes_per_sec: 1,
            burst_bytes: 1,
            max_response_bytes: 1,
            volume_bytes: 1,
            window_secs: 1,
            concurrency: 1,
        });

        assert_eq!(registered_meter_emitted_chunks(store, None).await, 2);
    }

    #[tokio::test]
    async fn meter_registration_recheck_fails_open_while_store_not_ready() {
        let store = SnapshotStore::new(
            &offline_config(PathBuf::new()),
            SnapshotHooks::default(),
            String::new(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );
        assert!(!store.is_ready());

        assert_eq!(
            registered_meter_emitted_chunks(store, Some(KEY_ID)).await,
            2
        );
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
                entitled_traces: None,
                limits: GrantedLimits::default(),
                on_exceed: OnExceed::Reject,
                quota_version: 1,
                quota_remaining_bytes: Some(1_000),
                snapshot_generation: None,
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

        assert!(emitted.is_empty());
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
    async fn concurrent_resolves_for_one_missing_key_are_single_flight() {
        std::env::set_var("PORTAL_STORE_TEST_TOKEN", SERVICE_TOKEN);
        let mock = MockControlPlane::spawn().await;
        let key_id = "same-missing-key";
        let gate = mock.pause_resolve(key_id);
        let path = cache_path("resolve-single-flight");
        let config = config(&mock, path.clone(), 10);
        let (store, task) = SnapshotStore::spawn(&config, CancellationToken::new()).unwrap();
        store.inner.ready.store(true, Ordering::Release);

        let resolves = (0..10)
            .map(|_| {
                let store = store.clone();
                tokio::spawn(async move { store.get_or_resolve(key_id).await })
            })
            .collect::<Vec<_>>();
        tokio::time::timeout(Duration::from_secs(1), async {
            while mock.resolve_calls().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(mock.resolve_calls(), vec![key_id.to_string()]);

        gate.notify_waiters();
        for resolve in resolves {
            assert!(resolve.await.unwrap().is_none());
        }
        task.abort();
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn concurrent_resolve_followers_are_bounded_while_control_plane_is_blocked() {
        const REQUESTS: usize = 1_000;
        const EXPECTED_MAX_FOLLOWERS: usize = 64;

        std::env::set_var("PORTAL_STORE_TEST_TOKEN", SERVICE_TOKEN);
        let mock = MockControlPlane::spawn().await;
        let key_id = "blocked-missing-key";
        let gate = mock.pause_resolve(key_id);
        let path = cache_path("resolve-bounded-followers");
        let (store, task) = SnapshotStore::spawn(
            &config(&mock, path.clone(), REQUESTS as u64),
            CancellationToken::new(),
        )
        .unwrap();
        store.inner.ready.store(true, Ordering::Release);

        let resolves = (0..REQUESTS)
            .map(|_| {
                let store = store.clone();
                tokio::spawn(async move { store.get_or_resolve(key_id).await })
            })
            .collect::<Vec<_>>();
        tokio::time::timeout(Duration::from_secs(1), async {
            while mock.resolve_calls().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            while resolves
                .iter()
                .filter(|resolve| resolve.is_finished())
                .count()
                < REQUESTS - EXPECTED_MAX_FOLLOWERS - 1
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("followers beyond the per-flight bound should fail closed promptly");
        assert_eq!(mock.resolve_calls(), vec![key_id.to_string()]);
        assert!(
            resolves
                .iter()
                .filter(|resolve| !resolve.is_finished())
                .count()
                <= EXPECTED_MAX_FOLLOWERS + 1
        );

        gate.notify_one();
        for resolve in resolves {
            assert!(resolve.await.unwrap().is_none());
        }
        task.abort();
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn concurrent_distinct_resolve_flights_are_globally_bounded() {
        const MAX_INFLIGHT: usize = 2;
        const REQUESTS: usize = MAX_INFLIGHT + 2;

        std::env::set_var("PORTAL_STORE_TEST_TOKEN", SERVICE_TOKEN);
        let mock = MockControlPlane::spawn().await;
        let keys = (0..REQUESTS)
            .map(|index| format!("blocked-distinct-key-{index}"))
            .collect::<Vec<_>>();
        let gates = keys
            .iter()
            .map(|key_id| mock.pause_resolve(key_id))
            .collect::<Vec<_>>();
        let path = cache_path("resolve-bounded-distinct-flights");
        let mut config = config(&mock, path.clone(), REQUESTS as u64);
        config.max_inflight_resolves = MAX_INFLIGHT;
        let (store, task) = SnapshotStore::spawn(&config, CancellationToken::new()).unwrap();
        store.inner.ready.store(true, Ordering::Release);

        let resolves = keys
            .into_iter()
            .map(|key_id| {
                let store = store.clone();
                tokio::spawn(async move { store.get_or_resolve(&key_id).await })
            })
            .collect::<Vec<_>>();

        tokio::time::timeout(Duration::from_secs(1), async {
            while mock.resolve_calls().len() < MAX_INFLIGHT
                || resolves
                    .iter()
                    .filter(|resolve| resolve.is_finished())
                    .count()
                    < REQUESTS - MAX_INFLIGHT
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("distinct resolves beyond the global flight bound should fail closed promptly");
        assert_eq!(mock.resolve_calls().len(), MAX_INFLIGHT);
        assert!(store.inner.inflight_resolves.len() <= MAX_INFLIGHT);

        for gate in gates {
            gate.notify_waiters();
        }
        for resolve in resolves {
            assert!(resolve.await.unwrap().is_none());
        }
        task.abort();
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn small_concurrent_resolvable_burst_coalesces_and_all_succeed() {
        const KEYS: [&str; 3] = [
            "concurrent-resolvable-key-1",
            "concurrent-resolvable-key-2",
            "concurrent-resolvable-key-3",
        ];

        std::env::set_var("PORTAL_STORE_TEST_TOKEN", SERVICE_TOKEN);
        let mock = MockControlPlane::spawn().await;
        let gates = KEYS
            .iter()
            .enumerate()
            .map(|(index, key_id)| {
                let mut resolved = active_snapshot(42 + index as u64);
                resolved.key_id = (*key_id).to_string();
                mock.resolve_with(key_id, Some(resolved));
                mock.pause_resolve(key_id)
            })
            .collect::<Vec<_>>();
        let path = cache_path("resolve-small-burst");
        let mut config = config(&mock, path.clone(), 10);
        config.max_inflight_resolves = KEYS.len();
        let (store, task) = SnapshotStore::spawn(&config, CancellationToken::new()).unwrap();
        store.inner.ready.store(true, Ordering::Release);

        let mut resolves = Vec::new();
        for key_id in KEYS {
            for _ in 0..3 {
                let store = store.clone();
                resolves.push((
                    key_id,
                    tokio::spawn(async move { store.get_or_resolve(key_id).await }),
                ));
            }
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while mock.resolve_calls().len() < KEYS.len() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let calls = mock.resolve_calls();
        for key_id in KEYS {
            assert_eq!(calls.iter().filter(|called| *called == key_id).count(), 1);
        }

        for gate in gates {
            gate.notify_one();
        }
        for (key_id, resolve) in resolves {
            assert_eq!(resolve.await.unwrap().unwrap().key_id, key_id);
        }
        assert_eq!(mock.resolve_calls().len(), KEYS.len());
        task.abort();
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn cancelled_resolve_leader_cannot_strand_late_follower() {
        let store = SnapshotStore::inactive(&PublicFallbackConfig {
            throughput_bytes_per_sec: 1,
            burst_bytes: 1,
            max_response_bytes: 1,
            volume_bytes: 1,
            window_secs: 1,
            concurrency: 1,
        });
        let key_id = "cancelled-leader";
        let flight = Arc::new(ResolveFlight {
            result: Mutex::new(None),
            notify: tokio::sync::Notify::new(),
            follower_permits: Arc::new(tokio::sync::Semaphore::new(MAX_RESOLVE_FOLLOWERS)),
            _global_permit: store
                .inner
                .inflight_resolve_permits
                .clone()
                .try_acquire_owned()
                .unwrap(),
        });
        store
            .inner
            .inflight_resolves
            .insert(key_id.to_string(), flight.clone());
        let guard = ResolveFlightGuard {
            store: &store,
            key_id: key_id.to_string(),
            flight,
            completed: false,
        };

        let follower = store.inner.inflight_resolves.get(key_id).unwrap().clone();
        drop(guard);

        let notified = follower.notify.notified();
        let initial_result = { follower.result.lock().unwrap().clone() };
        let result = if let Some(result) = initial_result {
            result
        } else {
            tokio::time::timeout(Duration::from_millis(50), notified)
                .await
                .expect("leader cancellation must wake a late-registering follower");
            follower
                .result
                .lock()
                .unwrap()
                .clone()
                .expect("leader cancellation must publish a terminal result")
        };
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn negative_cache_preserves_exact_case_sensitive_key_identity() {
        std::env::set_var("PORTAL_STORE_TEST_TOKEN", SERVICE_TOKEN);
        let mock = MockControlPlane::spawn().await;
        let path = cache_path("resolve-exact-identity");
        let config = config(&mock, path.clone(), 3);
        let (store, task) = SnapshotStore::spawn(&config, CancellationToken::new()).unwrap();
        store.inner.ready.store(true, Ordering::Release);

        assert!(store.get_or_resolve("MissingKey").await.is_none());
        assert!(store.get_or_resolve("missingkey").await.is_none());
        assert!(store.get_or_resolve(" MissingKey ").await.is_none());
        assert_eq!(
            mock.resolve_calls(),
            vec![
                "MissingKey".to_string(),
                "missingkey".to_string(),
                " MissingKey ".to_string(),
            ]
        );
        assert_eq!(store.negative_cache_len(), 3);

        task.abort();
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn authoritative_reset_clears_negative_cache_before_next_resolve() {
        let mock = MockControlPlane::spawn().await;
        let key_id = "restored-resolve-key";
        let store = SnapshotStore::new(
            &config(&mock, PathBuf::new(), 10),
            SnapshotHooks::default(),
            SERVICE_TOKEN.to_string(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );
        store.set_ready_for_test(true);
        store.insert_negative_cache_for_test(key_id, Duration::from_secs(60));
        assert_eq!(store.negative_cache_len(), 1);

        store
            .replace_records_with_mode(vec![], 5, BootstrapMode::Authoritative)
            .unwrap();

        assert_eq!(store.negative_cache_len(), 0);
        let mut resolved = active_snapshot(6);
        resolved.key_id = key_id.to_string();
        mock.resolve_with(key_id, Some(resolved));
        assert_eq!(store.get_or_resolve(key_id).await.unwrap().seq, 6);
        assert_eq!(mock.resolve_calls(), vec![key_id.to_string()]);
    }

    #[test]
    fn stale_generation_negative_cache_mutations_preserve_fresh_entry_and_ttl() {
        let store = SnapshotStore::inactive(&PublicFallbackConfig {
            throughput_bytes_per_sec: 1,
            burst_bytes: 1,
            max_response_bytes: 1,
            volume_bytes: 1,
            window_secs: 1,
            concurrency: 1,
        });
        store.inner.generation.store(2, Ordering::Release);
        let fresh_expiry = Instant::now() + Duration::from_secs(60);
        assert!(store.insert_negative_cache_for_generation("missing", 2, fresh_expiry));

        assert!(!store.insert_negative_cache_for_generation(
            "missing",
            1,
            Instant::now() + Duration::from_secs(5),
        ));
        store.remove_negative_cache_for_generation("missing", 1);

        let fresh = store
            .inner
            .negative_cache
            .get("missing")
            .expect("fresh generation entry must survive stale cleanup");
        assert_eq!(fresh.generation, 2);
        assert_eq!(fresh.expires_at, fresh_expiry);
    }

    #[test]
    fn authoritative_reset_refills_exhausted_resolve_limiter() {
        let store = SnapshotStore::new(
            &offline_config(PathBuf::new()),
            SnapshotHooks::default(),
            String::new(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );
        *store.inner.resolve_limiter.lock().unwrap() = ResolveLimiter::new(1);
        assert!(store.take_resolve_token());
        assert!(!store.take_resolve_token());

        store
            .replace_records_with_mode(vec![], 5, BootstrapMode::Authoritative)
            .unwrap();

        assert!(store.take_resolve_token());
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
    async fn malformed_resolve_tombstones_requested_key_without_negative_cache() {
        let mock = MockControlPlane::spawn().await;
        mock.resolve_raw(
            KEY_ID,
            serde_json::json!({"key_id": KEY_ID, "seq": 11, "status": null}),
        );
        let store = SnapshotStore::new(
            &config(&mock, PathBuf::new(), 10),
            SnapshotHooks::default(),
            SERVICE_TOKEN.to_string(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );
        store.install_records_for_test(vec![SnapshotRecord::Key(active_snapshot(10))], 10);

        let resolved = store.get_or_resolve_inner(KEY_ID, false).await.unwrap();

        assert_eq!(resolved.status, KeyStatus::Revoked);
        assert_eq!(resolved.seq, 11);
        assert_eq!(store.get(KEY_ID).unwrap().status, KeyStatus::Revoked);
        assert!(!store.negative_cached(KEY_ID));
    }

    #[tokio::test]
    async fn malformed_resolve_without_sequence_is_not_negative_cached() {
        let mock = MockControlPlane::spawn().await;
        mock.resolve_raw(
            KEY_ID,
            serde_json::json!({"key_id": KEY_ID, "status": null}),
        );
        let store = SnapshotStore::new(
            &config(&mock, PathBuf::new(), 10),
            SnapshotHooks::default(),
            SERVICE_TOKEN.to_string(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );
        store.set_ready_for_test(true);

        assert!(store.get_or_resolve(KEY_ID).await.is_none());
        assert!(!store.negative_cached(KEY_ID));

        let mut healthy = active_snapshot(12);
        healthy.key_id = KEY_ID.to_string();
        mock.resolve_with(KEY_ID, Some(healthy));
        assert_eq!(store.get_or_resolve(KEY_ID).await.unwrap().seq, 12);
        assert_eq!(mock.resolve_calls().len(), 2);
    }

    #[tokio::test]
    async fn resolve_started_before_authoritative_replace_cannot_upsert_after_it() {
        let mock = MockControlPlane::spawn().await;
        let mut stale = active_snapshot(100);
        stale.account_id = Some("pre-restore".to_string());
        mock.resolve_with(KEY_ID, Some(stale));
        let gate = mock.pause_resolve(KEY_ID);
        let store = SnapshotStore::new(
            &config(&mock, PathBuf::new(), 10),
            SnapshotHooks::default(),
            SERVICE_TOKEN.to_string(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );
        store.set_ready_for_test(true);

        let resolving_store = store.clone();
        let resolve_task =
            tokio::spawn(async move { resolving_store.get_or_resolve(KEY_ID).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while mock.resolve_calls().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let mut restored = active_snapshot(5);
        restored.account_id = Some("restored".to_string());
        store
            .replace_records_with_mode(
                vec![SnapshotRecord::Key(restored)],
                5,
                BootstrapMode::Authoritative,
            )
            .unwrap();
        gate.notify_one();

        assert!(resolve_task.await.unwrap().is_none());
        assert_eq!(store.get(KEY_ID).unwrap().seq, 5);
        assert_eq!(
            store.get(KEY_ID).unwrap().account_id.as_deref(),
            Some("restored")
        );

        let fresh_key = "fresh-after-reset";
        let mut fresh = active_snapshot(6);
        fresh.key_id = fresh_key.to_string();
        mock.resolve_with(fresh_key, Some(fresh));

        assert_eq!(store.get_or_resolve(fresh_key).await.unwrap().seq, 6);
        assert_eq!(store.get(fresh_key).unwrap().seq, 6);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resolved_record_cannot_run_hooks_or_return_after_authoritative_replace() {
        let mock = MockControlPlane::spawn().await;
        let mut stale = active_snapshot(100);
        stale.status = KeyStatus::Suspended;
        mock.resolve_with(KEY_ID, Some(stale));
        let tally = Arc::new(TallyStore::default());
        let registry = Arc::new(ActiveStreamRegistry::default());
        let store = SnapshotStore::new(
            &config(&mock, PathBuf::new(), 10),
            SnapshotHooks {
                tally: Some(tally.clone()),
                registry: Some(registry.clone()),
            },
            SERVICE_TOKEN.to_string(),
            reqwest::Client::builder().no_proxy().build().unwrap(),
            TokioInstant::now(),
        );
        store.set_ready_for_test(true);
        let (application_reached, resume_application) = store.pause_resolve_application_for_test();

        let resolving_store = store.clone();
        let resolve_task =
            tokio::spawn(async move { resolving_store.get_or_resolve(KEY_ID).await });
        tokio::task::spawn_blocking(move || application_reached.recv().unwrap())
            .await
            .unwrap();

        store
            .replace_records_with_mode(
                vec![SnapshotRecord::Key(active_snapshot(5))],
                5,
                BootstrapMode::Authoritative,
            )
            .unwrap();
        let restored_stream_kill = Arc::new(AtomicBool::new(false));
        let _restored_registration = registry.register(
            "restored-stream".to_string(),
            Some(KEY_ID.to_string()),
            restored_stream_kill.clone(),
            Arc::new(AtomicU64::new(0)),
        );
        resume_application.send(()).unwrap();

        let returned = resolve_task.await.unwrap();
        assert!(
            returned.is_none(),
            "stale resolved record escaped: {returned:?}"
        );
        assert_eq!(tally.version_for("account"), Some(5));
        assert!(!restored_stream_kill.load(Ordering::Acquire));
        assert_eq!(store.get(KEY_ID).unwrap().seq, 5);
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
