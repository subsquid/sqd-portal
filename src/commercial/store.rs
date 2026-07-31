use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use tokio::{io::AsyncWriteExt, sync::Semaphore};
use tokio_util::sync::CancellationToken;

use super::{
    client::{Authorized, ControlPlaneClient, PAGE_LIMIT},
    config::CommercialConfig,
    now_secs,
    types::{KeyRecord, SnapshotPage},
};

/// Guards against a feed that never returns a short page.
const MAX_BOOTSTRAP_PAGES: usize = 10_000;

/// How long a request coalesced behind another request's authorize call waits
/// before failing closed on its own.
const RESOLVE_FOLLOWER_TIMEOUT: Duration = Duration::from_secs(6);

/// The portal's view of the control plane's key set: a full snapshot pulled at
/// startup, kept current by cursor-paged deltas, and backed by a disk cache so
/// a restart serves immediately. A sync failure never drops the served state —
/// the last good snapshot keeps answering until the control plane returns.
pub struct SnapshotStore {
    client: ControlPlaneClient,
    state: RwLock<State>,
    ready: AtomicBool,
    cache_dirty: AtomicBool,
    cache_path: Option<PathBuf>,
    sync_interval: Duration,
    negative_cache_ttl: Duration,
    negative_cache: Mutex<HashMap<String, Instant>>,
    inflight: Mutex<HashMap<String, Arc<ResolveFlight>>>,
    inflight_permits: Arc<Semaphore>,
    limiter: Mutex<RateLimiter>,
}

#[derive(Default)]
struct State {
    records: HashMap<String, Arc<KeyRecord>>,
    cursor: u64,
    epoch: Option<String>,
    /// Bumped by every full resync. A resolve that started before the resync
    /// must not insert its record into the new generation.
    generation: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct DiskCache {
    cursor: u64,
    #[serde(default)]
    epoch: Option<String>,
    records: Vec<KeyRecord>,
    saved_at: u64,
}

#[derive(Debug)]
enum ResyncReason {
    EpochChanged { stored: String, received: String },
    HeadRolledBack { cursor: u64, head_seq: u64 },
}

struct ResolveFlight {
    result: Mutex<Option<Option<Arc<KeyRecord>>>>,
    done: tokio::sync::Notify,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

/// Publishes a result even if the leading request is dropped mid-flight (a
/// disconnecting client cancels its handler), so followers are never stranded
/// behind a flight that will never complete.
struct FlightGuard<'a> {
    store: &'a SnapshotStore,
    key_id: &'a str,
    flight: Arc<ResolveFlight>,
}

impl Drop for FlightGuard<'_> {
    fn drop(&mut self) {
        let mut result = self.flight.result.lock().unwrap();
        if result.is_none() {
            *result = Some(None);
        }
        drop(result);
        self.store.end_flight(self.key_id, &self.flight);
        self.flight.done.notify_waiters();
    }
}

struct RateLimiter {
    rate_per_sec: f64,
    tokens: f64,
    last: Instant,
}

impl SnapshotStore {
    pub fn new(config: &CommercialConfig) -> anyhow::Result<Arc<Self>> {
        let store = Arc::new(Self {
            client: ControlPlaneClient::new(config)?,
            state: RwLock::new(State::default()),
            ready: AtomicBool::new(false),
            cache_dirty: AtomicBool::new(false),
            cache_path: config.snapshot_cache_path.clone(),
            sync_interval: config.sync_interval(),
            negative_cache_ttl: config.negative_cache_ttl(),
            negative_cache: Mutex::new(HashMap::new()),
            inflight: Mutex::new(HashMap::new()),
            inflight_permits: Arc::new(Semaphore::new(config.max_inflight_resolves)),
            limiter: Mutex::new(RateLimiter::new(config.resolve_rate_per_sec)),
        });
        store.load_disk_cache();
        Ok(store)
    }

    pub fn spawn_sync(self: &Arc<Self>, cancel: CancellationToken) {
        let store = self.clone();
        tokio::spawn(async move {
            loop {
                store.run_tick().await;
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(store.sync_interval) => {}
                }
            }
        });
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn install_for_test(&self, records: Vec<KeyRecord>) {
        self.install(records, 0, Some("test-epoch".to_string()));
    }

    pub fn get(&self, key_id: &str) -> Option<Arc<KeyRecord>> {
        self.state.read().unwrap().records.get(key_id).cloned()
    }

    /// Looks the key up in the snapshot and, on a miss, asks the control plane
    /// directly: a key minted seconds ago must work before the next sync tick.
    /// Concurrent misses for one key share a single call; unknown answers are
    /// cached briefly so a bad key cannot be used to hammer the control plane.
    pub async fn get_or_resolve(&self, key_id: &str) -> Option<Arc<KeyRecord>> {
        if let Some(record) = self.get(key_id) {
            return Some(record);
        }
        if self.negative_cached(key_id) {
            return None;
        }

        let (flight, leader) = self.begin_flight(key_id)?;
        if !leader {
            return self.follow_flight(key_id, &flight).await;
        }

        let guard = FlightGuard {
            store: self,
            key_id,
            flight: flight.clone(),
        };
        let result = self.resolve(key_id).await.unwrap_or_else(|err| {
            tracing::warn!(key_id, error = %err, "commercial authorize failed; rejecting unknown key");
            None
        });
        *flight.result.lock().unwrap() = Some(result.clone());
        drop(guard);
        result
    }

    fn begin_flight(&self, key_id: &str) -> Option<(Arc<ResolveFlight>, bool)> {
        let mut inflight = self.inflight.lock().unwrap();
        if let Some(flight) = inflight.get(key_id) {
            return Some((flight.clone(), false));
        }
        let Ok(permit) = self.inflight_permits.clone().try_acquire_owned() else {
            tracing::warn!(
                key_id,
                "commercial authorize skipped: too many lookups in flight"
            );
            return None;
        };
        let flight = Arc::new(ResolveFlight {
            result: Mutex::new(None),
            done: tokio::sync::Notify::new(),
            _permit: permit,
        });
        inflight.insert(key_id.to_owned(), flight.clone());
        Some((flight, true))
    }

    async fn follow_flight(
        &self,
        key_id: &str,
        flight: &Arc<ResolveFlight>,
    ) -> Option<Arc<KeyRecord>> {
        // Subscribing before the check closes the window where the leader
        // finishes between the two.
        let notified = flight.done.notified();
        if let Some(result) = flight.result.lock().unwrap().clone() {
            return result;
        }
        if tokio::time::timeout(RESOLVE_FOLLOWER_TIMEOUT, notified)
            .await
            .is_err()
        {
            tracing::warn!(
                key_id,
                "commercial authorize wait timed out; failing closed"
            );
            return None;
        }
        flight.result.lock().unwrap().clone().flatten()
    }

    fn end_flight(&self, key_id: &str, flight: &Arc<ResolveFlight>) {
        let mut inflight = self.inflight.lock().unwrap();
        if inflight
            .get(key_id)
            .is_some_and(|current| Arc::ptr_eq(current, flight))
        {
            inflight.remove(key_id);
        }
    }

    async fn resolve(&self, key_id: &str) -> anyhow::Result<Option<Arc<KeyRecord>>> {
        let generation = self.state.read().unwrap().generation;
        if !self.limiter.lock().unwrap().take() {
            tracing::warn!(key_id, "commercial authorize rate limited; failing closed");
            return Ok(None);
        }

        match self.client.authorize(key_id).await? {
            Authorized::Found(value) => {
                let record: KeyRecord = serde_json::from_value(value)?;
                anyhow::ensure!(
                    record.key_id == key_id,
                    "authorize returned key {} for {key_id}",
                    record.key_id
                );
                Ok(self.upsert_resolved(record, generation))
            }
            Authorized::Unknown => {
                let expires_at = Instant::now() + self.negative_cache_ttl;
                let state = self.state.read().unwrap();
                if state.generation == generation {
                    self.negative_cache
                        .lock()
                        .unwrap()
                        .insert(key_id.to_owned(), expires_at);
                }
                Ok(None)
            }
        }
    }

    fn upsert_resolved(&self, record: KeyRecord, generation: u64) -> Option<Arc<KeyRecord>> {
        let mut state = self.state.write().unwrap();
        if state.generation != generation {
            return None;
        }
        if let Some(existing) = state.records.get(&record.key_id) {
            if existing.seq >= record.seq {
                return Some(existing.clone());
            }
        }
        let record = Arc::new(record);
        state.records.insert(record.key_id.clone(), record.clone());
        drop(state);
        self.mark_cache_dirty();
        Some(record)
    }

    fn negative_cached(&self, key_id: &str) -> bool {
        let mut cache = self.negative_cache.lock().unwrap();
        match cache.get(key_id) {
            Some(expires_at) if *expires_at > Instant::now() => true,
            Some(_) => {
                cache.remove(key_id);
                false
            }
            None => false,
        }
    }

    async fn run_tick(&self) {
        let result = if self.is_ready() {
            self.sync_once().await
        } else {
            self.bootstrap(None).await
        };
        if let Err(err) = result {
            // Fail-static: the previously synced snapshot keeps serving.
            tracing::warn!(
                error = %err,
                ready = self.is_ready(),
                "commercial snapshot sync failed; serving last known keys"
            );
        }
        self.persist_if_dirty().await;
    }

    async fn sync_once(&self) -> anyhow::Result<()> {
        let cursor = self.state.read().unwrap().cursor;
        let page = self.client.fetch_page(cursor).await?;

        if let Some(reason) = self.resync_reason(cursor, &page) {
            match &reason {
                ResyncReason::EpochChanged { stored, received } => tracing::warn!(
                    cursor,
                    stored_epoch = stored,
                    received_epoch = received,
                    "commercial snapshot feed epoch changed; resyncing from scratch"
                ),
                ResyncReason::HeadRolledBack { cursor, head_seq } => tracing::warn!(
                    cursor,
                    head_seq,
                    "commercial snapshot feed head rolled back; resyncing from scratch"
                ),
            }
            return self.bootstrap(page.epoch).await;
        }

        self.observe_epoch(page.epoch);
        if page.records.is_empty() {
            return Ok(());
        }

        let next_cursor = page.next_cursor;
        let records = parse_records(page.records)?;
        let applied = self.apply_delta(records, next_cursor);
        tracing::info!(
            applied,
            cursor = next_cursor,
            "commercial snapshot delta applied"
        );
        Ok(())
    }

    /// Re-reads the whole feed from cursor zero and replaces the served state.
    async fn bootstrap(&self, fallback_epoch: Option<String>) -> anyhow::Result<()> {
        let mut cursor = 0;
        let mut records = Vec::new();
        let mut epoch = fallback_epoch;
        let mut pages = 0usize;
        let mut pages_this_attempt = 0usize;

        loop {
            anyhow::ensure!(
                pages < MAX_BOOTSTRAP_PAGES,
                "snapshot bootstrap exceeded {MAX_BOOTSTRAP_PAGES} pages"
            );
            let page = self.client.fetch_page(cursor).await?;
            pages += 1;

            let epoch_changed = pages_this_attempt > 0
                && page
                    .epoch
                    .as_ref()
                    .zip(epoch.as_ref())
                    .is_some_and(|(received, expected)| received != expected);
            if epoch_changed {
                tracing::warn!(
                    cursor,
                    received_epoch = ?page.epoch,
                    expected_epoch = ?epoch,
                    "commercial snapshot epoch changed mid-bootstrap; restarting"
                );
                cursor = 0;
                records.clear();
                pages_this_attempt = 0;
                epoch = page.epoch;
                continue;
            }
            if page.epoch.is_some() {
                epoch = page.epoch;
            }
            pages_this_attempt += 1;

            let page_len = page.records.len();
            records.extend(parse_records(page.records)?);
            if page_len < usize::from(PAGE_LIMIT) {
                cursor = page.next_cursor;
                break;
            }
            anyhow::ensure!(
                page.next_cursor > cursor,
                "snapshot bootstrap cursor did not advance past {cursor}"
            );
            cursor = page.next_cursor;
        }

        let count = records.len();
        self.install(records, cursor, epoch);
        tracing::info!(count, cursor, "commercial snapshot bootstrap applied");
        Ok(())
    }

    fn resync_reason(&self, cursor: u64, page: &SnapshotPage) -> Option<ResyncReason> {
        if let Some(head_seq) = page.head_seq {
            if cursor > head_seq {
                return Some(ResyncReason::HeadRolledBack { cursor, head_seq });
            }
        }
        let received = page.epoch.as_ref()?;
        match self.state.read().unwrap().epoch.clone() {
            Some(stored) if stored != *received => Some(ResyncReason::EpochChanged {
                stored,
                received: received.clone(),
            }),
            _ => None,
        }
    }

    fn observe_epoch(&self, epoch: Option<String>) {
        let Some(epoch) = epoch else {
            return;
        };
        let mut state = self.state.write().unwrap();
        if state.epoch.as_deref() == Some(epoch.as_str()) {
            return;
        }
        state.epoch = Some(epoch);
        drop(state);
        self.mark_cache_dirty();
    }

    fn install(&self, records: Vec<KeyRecord>, cursor: u64, epoch: Option<String>) {
        {
            let mut state = self.state.write().unwrap();
            state.records = records
                .into_iter()
                .map(|record| (record.key_id.clone(), Arc::new(record)))
                .collect();
            state.cursor = cursor;
            state.epoch = epoch;
            state.generation += 1;
            // Both caches describe the replaced generation.
            self.negative_cache.lock().unwrap().clear();
            self.limiter.lock().unwrap().reset();
        }
        self.ready.store(true, Ordering::Release);
        self.mark_cache_dirty();
    }

    fn apply_delta(&self, records: Vec<KeyRecord>, cursor: u64) -> usize {
        let mut applied = 0;
        {
            let mut state = self.state.write().unwrap();
            for record in records {
                if state
                    .records
                    .get(&record.key_id)
                    .is_some_and(|existing| existing.seq >= record.seq)
                {
                    continue;
                }
                state
                    .records
                    .insert(record.key_id.clone(), Arc::new(record));
                applied += 1;
            }
            state.cursor = cursor;
        }
        self.mark_cache_dirty();
        applied
    }

    fn load_disk_cache(&self) {
        let Some(path) = self.cache_path.as_deref() else {
            return;
        };
        let Ok(bytes) = std::fs::read(path) else {
            return;
        };
        let cache: DiskCache = match serde_json::from_slice(&bytes) {
            Ok(cache) => cache,
            Err(err) => {
                tracing::warn!(path = %path.display(), error = %err, "ignoring unreadable commercial snapshot cache");
                return;
            }
        };

        let age = now_secs().saturating_sub(cache.saved_at);
        let count = cache.records.len();
        self.install(cache.records, cache.cursor, cache.epoch);
        // Installing marks the cache dirty; nothing changed relative to disk.
        self.cache_dirty.store(false, Ordering::Release);
        tracing::info!(
            path = %path.display(),
            count,
            age_seconds = age,
            "loaded commercial snapshot disk cache"
        );
    }

    fn mark_cache_dirty(&self) {
        self.cache_dirty.store(true, Ordering::Release);
    }

    async fn persist_if_dirty(&self) {
        if self.cache_path.is_none() || !self.cache_dirty.swap(false, Ordering::AcqRel) {
            return;
        }
        if let Err(err) = self.persist_disk_cache().await {
            self.mark_cache_dirty();
            tracing::warn!(error = %err, "commercial snapshot cache persist failed");
        }
    }

    async fn persist_disk_cache(&self) -> anyhow::Result<()> {
        let Some(path) = self.cache_path.as_deref() else {
            return Ok(());
        };
        let cache = {
            let state = self.state.read().unwrap();
            DiskCache {
                cursor: state.cursor,
                epoch: state.epoch.clone(),
                records: state
                    .records
                    .values()
                    .map(|record| (**record).clone())
                    .collect(),
                saved_at: now_secs(),
            }
        };
        let bytes = serde_json::to_vec(&cache)?;

        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            tokio::fs::create_dir_all(parent).await?;
        }
        // Write-then-rename: a crash mid-write must not leave a half-written
        // cache that the next boot would refuse.
        let tmp = tmp_path(path);
        let mut file = tokio::fs::File::create(&tmp).await?;
        file.write_all(&bytes).await?;
        file.sync_all().await?;
        drop(file);
        tokio::fs::rename(&tmp, path).await?;
        Ok(())
    }
}

impl RateLimiter {
    fn new(rate_per_sec: u64) -> Self {
        Self {
            rate_per_sec: rate_per_sec as f64,
            tokens: rate_per_sec as f64,
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
        self.tokens = (self.tokens + elapsed * self.rate_per_sec).min(self.rate_per_sec);
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }

    fn reset(&mut self) {
        self.tokens = self.rate_per_sec;
        self.last = Instant::now();
    }
}

/// A record that fails to parse but still names a key is kept as a tombstone:
/// dropping it would leave the previous, possibly usable, version of that key
/// in place. One that cannot even be identified fails the whole page, which
/// keeps the current snapshot serving until the control plane is fixed.
fn parse_records(values: Vec<serde_json::Value>) -> anyhow::Result<Vec<KeyRecord>> {
    let mut records = Vec::with_capacity(values.len());
    for value in values {
        let err = match serde_json::from_value::<KeyRecord>(value.clone()) {
            Ok(record) => {
                records.push(record);
                continue;
            }
            Err(err) => err,
        };
        let identity = value
            .get("key_id")
            .and_then(serde_json::Value::as_str)
            .zip(value.get("seq").and_then(serde_json::Value::as_u64));
        let Some((key_id, seq)) = identity else {
            anyhow::bail!("unidentifiable malformed snapshot record: {err}");
        };
        tracing::warn!(key_id, seq, error = %err, "malformed commercial snapshot record; revoking key");
        records.push(KeyRecord::tombstone(key_id.to_owned(), seq));
    }
    Ok(records)
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commercial::{
        test_support::{key_record, page, MockControlPlane},
        types::KeyStatus,
    };

    fn cache_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "sqd-portal-commercial-{name}-{}.json",
            std::process::id()
        ))
    }

    async fn store_for(
        control_plane: &MockControlPlane,
        cache: Option<PathBuf>,
    ) -> Arc<SnapshotStore> {
        SnapshotStore::new(&control_plane.config(cache)).expect("store should build")
    }

    #[tokio::test]
    async fn bootstrap_pages_until_a_short_page_and_serves_records() {
        let cp = MockControlPlane::spawn().await;
        let first: Vec<_> = (0..PAGE_LIMIT)
            .map(|i| key_record(&format!("k{i}"), u64::from(i) + 1))
            .collect();
        cp.push_page(0, page(first, u64::from(PAGE_LIMIT), Some("e1"), None));
        cp.push_page(
            u64::from(PAGE_LIMIT),
            page(vec![key_record("last", 5000)], 5000, Some("e1"), Some(5000)),
        );

        let store = store_for(&cp, None).await;
        store.run_tick().await;

        assert!(store.is_ready());
        assert_eq!(
            store.state.read().unwrap().records.len(),
            usize::from(PAGE_LIMIT) + 1
        );
        assert_eq!(store.state.read().unwrap().cursor, 5000);
        assert_eq!(store.state.read().unwrap().epoch.as_deref(), Some("e1"));
        assert!(store.get("last").is_some());
        assert_eq!(cp.snapshot_cursors(), vec![0, u64::from(PAGE_LIMIT)]);
    }

    #[tokio::test]
    async fn delta_upserts_by_sequence_and_advances_the_cursor() {
        let cp = MockControlPlane::spawn().await;
        cp.push_page(0, page(vec![key_record("k1", 1)], 1, Some("e1"), Some(1)));
        let store = store_for(&cp, None).await;
        store.run_tick().await;

        let mut revoked = key_record("k1", 2);
        revoked.status = KeyStatus::Revoked;
        cp.push_page(1, page(vec![revoked], 2, Some("e1"), Some(2)));
        store.run_tick().await;

        assert_eq!(store.get("k1").unwrap().status, KeyStatus::Revoked);
        assert_eq!(store.state.read().unwrap().cursor, 2);

        // An out-of-order replay must not resurrect the older version.
        cp.push_page(2, page(vec![key_record("k1", 1)], 3, Some("e1"), Some(3)));
        store.run_tick().await;
        assert_eq!(store.get("k1").unwrap().status, KeyStatus::Revoked);
        assert_eq!(store.state.read().unwrap().cursor, 3);
    }

    #[tokio::test]
    async fn epoch_change_drops_state_and_resyncs_from_cursor_zero() {
        let cp = MockControlPlane::spawn().await;
        cp.push_page(
            0,
            page(
                vec![key_record("old", 1), key_record("k1", 2)],
                2,
                Some("e1"),
                Some(2),
            ),
        );
        let store = store_for(&cp, None).await;
        store.run_tick().await;
        assert!(store.get("old").is_some());

        // The next poll answers with a new epoch; the re-bootstrap that follows
        // starts from cursor zero and no longer publishes `old`.
        cp.push_page(2, page(vec![], 2, Some("e2"), Some(9)));
        cp.push_page(0, page(vec![key_record("k1", 9)], 9, Some("e2"), Some(9)));
        store.run_tick().await;

        assert!(
            store.get("old").is_none(),
            "state from the old epoch must be dropped"
        );
        assert_eq!(store.get("k1").unwrap().seq, 9);
        assert_eq!(store.state.read().unwrap().cursor, 9);
        assert_eq!(store.state.read().unwrap().epoch.as_deref(), Some("e2"));
        assert_eq!(cp.snapshot_cursors(), vec![0, 2, 0]);
    }

    #[tokio::test]
    async fn cursor_beyond_head_seq_resyncs_from_cursor_zero() {
        let cp = MockControlPlane::spawn().await;
        cp.push_page(
            0,
            page(vec![key_record("k1", 40)], 40, Some("e1"), Some(40)),
        );
        let store = store_for(&cp, None).await;
        store.run_tick().await;

        cp.push_page(40, page(vec![], 40, Some("e1"), Some(3)));
        cp.push_page(0, page(vec![key_record("k1", 3)], 3, Some("e1"), Some(3)));
        store.run_tick().await;

        assert_eq!(store.state.read().unwrap().cursor, 3);
        assert_eq!(cp.snapshot_cursors(), vec![0, 40, 0]);
    }

    #[tokio::test]
    async fn sync_failure_keeps_serving_the_last_snapshot() {
        let cp = MockControlPlane::spawn().await;
        cp.push_page(0, page(vec![key_record("k1", 1)], 1, Some("e1"), Some(1)));
        let store = store_for(&cp, None).await;
        store.run_tick().await;

        cp.fail_snapshots(true);
        store.run_tick().await;

        assert!(store.is_ready());
        assert!(store.get("k1").is_some());
    }

    #[tokio::test]
    async fn disk_cache_round_trips_across_a_restart_without_the_control_plane() {
        let path = cache_path("round-trip");
        let _ = std::fs::remove_file(&path);
        let cp = MockControlPlane::spawn().await;
        cp.push_page(0, page(vec![key_record("k1", 4)], 4, Some("e1"), Some(4)));

        let store = store_for(&cp, Some(path.clone())).await;
        store.run_tick().await;
        assert!(path.exists(), "a synced snapshot must be persisted");

        let restarted = store_for(&cp, Some(path.clone())).await;
        assert!(
            restarted.is_ready(),
            "the disk cache must serve before the first sync"
        );
        assert_eq!(restarted.get("k1").unwrap().seq, 4);
        assert_eq!(restarted.state.read().unwrap().cursor, 4);
        assert_eq!(restarted.state.read().unwrap().epoch.as_deref(), Some("e1"));

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn corrupt_disk_cache_is_ignored() {
        let path = cache_path("corrupt");
        std::fs::write(&path, b"{not json").unwrap();
        let cp = MockControlPlane::spawn().await;

        let store = store_for(&cp, Some(path.clone())).await;
        assert!(!store.is_ready());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn malformed_records_are_tombstoned_and_unidentifiable_ones_fail_the_page() {
        let cp = MockControlPlane::spawn().await;
        cp.push_page(0, page(vec![key_record("k1", 1)], 1, Some("e1"), Some(1)));
        let store = store_for(&cp, None).await;
        store.run_tick().await;
        assert_eq!(store.get("k1").unwrap().status, KeyStatus::Active);

        cp.push_page(
            1,
            serde_json::json!({
                "records": [{"key_id": "k1", "seq": 2, "status": 17}],
                "next_cursor": 2,
                "epoch": "e1",
            }),
        );
        store.run_tick().await;
        assert_eq!(
            store.get("k1").unwrap().status,
            KeyStatus::Revoked,
            "a malformed record must not leave the previous version usable"
        );

        cp.push_page(
            2,
            serde_json::json!({
                "records": [{"nothing": "identifiable"}],
                "next_cursor": 3,
                "epoch": "e1",
            }),
        );
        store.run_tick().await;
        assert_eq!(
            store.state.read().unwrap().cursor,
            2,
            "an unidentifiable record must not advance the cursor"
        );
    }

    #[tokio::test]
    async fn authorize_on_miss_inserts_the_record_and_dedups_concurrent_lookups() {
        let cp = MockControlPlane::spawn().await;
        cp.push_page(0, page(vec![], 0, Some("e1"), Some(0)));
        cp.authorize_with("k1", Some(key_record("k1", 3)));
        let store = store_for(&cp, None).await;
        store.run_tick().await;

        // The delay keeps the first lookup in flight long enough for the second
        // to find it and coalesce.
        cp.authorize_delay("k1", Duration::from_millis(100));
        let first = tokio::spawn({
            let store = store.clone();
            async move { store.get_or_resolve("k1").await }
        });
        let second = tokio::spawn({
            let store = store.clone();
            async move { store.get_or_resolve("k1").await }
        });

        assert_eq!(first.await.unwrap().unwrap().seq, 3);
        assert_eq!(second.await.unwrap().unwrap().seq, 3);
        assert_eq!(cp.authorize_calls(), vec!["k1".to_string()]);
        assert!(
            store.get("k1").is_some(),
            "a resolved key joins the snapshot"
        );
    }

    #[tokio::test]
    async fn unknown_keys_are_negative_cached() {
        let cp = MockControlPlane::spawn().await;
        cp.push_page(0, page(vec![], 0, Some("e1"), Some(0)));
        let store = store_for(&cp, None).await;
        store.run_tick().await;

        assert!(store.get_or_resolve("nope").await.is_none());
        assert!(store.get_or_resolve("nope").await.is_none());
        assert_eq!(
            cp.authorize_calls(),
            vec!["nope".to_string()],
            "a negative answer must suppress repeat lookups"
        );
    }

    #[tokio::test]
    async fn authorize_is_rate_limited_and_fails_closed() {
        let cp = MockControlPlane::spawn().await;
        cp.push_page(0, page(vec![], 0, Some("e1"), Some(0)));
        cp.authorize_with("k1", Some(key_record("k1", 1)));
        let mut config = cp.config(None);
        config.resolve_rate_per_sec = 0;
        let store = SnapshotStore::new(&config).unwrap();
        store.run_tick().await;

        assert!(store.get_or_resolve("k1").await.is_none());
        assert!(cp.authorize_calls().is_empty());
    }

    #[tokio::test]
    async fn control_plane_errors_during_authorize_fail_closed() {
        let cp = MockControlPlane::spawn().await;
        cp.push_page(0, page(vec![], 0, Some("e1"), Some(0)));
        cp.authorize_status("k1", 500);
        let store = store_for(&cp, None).await;
        store.run_tick().await;

        assert!(store.get_or_resolve("k1").await.is_none());
        // A server error is not a negative answer, so it must not be cached.
        assert!(store.get_or_resolve("k1").await.is_none());
        assert_eq!(cp.authorize_calls().len(), 2);
    }

    #[test]
    fn rate_limiter_refills_over_time() {
        let mut limiter = RateLimiter::new(2);
        assert!(limiter.take());
        assert!(limiter.take());
        assert!(!limiter.take());

        limiter.last = Instant::now() - Duration::from_secs(1);
        assert!(limiter.take());
    }
}
