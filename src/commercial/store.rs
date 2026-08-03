use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    time::{Duration, Instant},
};

use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::{
    client::{Authorized, ControlPlaneClient, PAGE_LIMIT},
    config::CommercialConfig,
    now_secs,
    types::{KeyRecord, SnapshotPage},
};

/// Guards against a feed that never returns a short page.
const MAX_BOOTSTRAP_PAGES: usize = 10_000;

/// Upper bound on remembered "the control plane has never heard of this key"
/// answers. The ids are attacker-chosen, so the map is capped: past the cap new
/// answers are dropped instead of cached, which costs a rate-limited lookup
/// rather than unbounded memory.
const NEGATIVE_CACHE_CAPACITY: usize = 4096;

/// How long one of those answers suppresses repeat lookups for the same id.
/// Short enough that a key minted moments after a miss still works quickly.
const NEGATIVE_CACHE_TTL: Duration = Duration::from_secs(15);

/// Token-bucket rate for authorize-on-miss lookups: what a portal may ask the
/// control plane about keys its snapshot has not caught up with yet.
const RESOLVE_RATE_PER_SEC: u64 = 20;

/// Upper bound on concurrent authorize-on-miss calls, so an unknown-key flood
/// cannot turn into an unbounded fan-out against the control plane, and a
/// control plane that stops answering cannot pile up handlers.
const MAX_INFLIGHT_RESOLVES: usize = 16;

/// The portal's view of the control plane's key set: a full snapshot pulled at
/// startup and kept current by cursor-paged deltas. A sync failure never drops
/// the served state — the last good snapshot keeps answering until the control
/// plane returns.
pub struct SnapshotStore {
    client: ControlPlaneClient,
    state: RwLock<State>,
    ready: AtomicBool,
    sync_interval: Duration,
    /// Wall-clock second of the last sync the control plane answered. Every
    /// failed tick reports the distance from it, so a feed that has been down
    /// for hours does not read like one that missed a single tick.
    last_success: AtomicU64,
    negative_cache: Mutex<HashMap<String, Instant>>,
    inflight_permits: Semaphore,
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

#[derive(Debug)]
enum ResyncReason {
    EpochChanged { stored: String, received: String },
    HeadRolledBack { cursor: u64, head_seq: u64 },
}

struct RateLimiter {
    rate_per_sec: f64,
    tokens: f64,
    last: Instant,
}

impl SnapshotStore {
    pub fn new(config: &CommercialConfig) -> anyhow::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            client: ControlPlaneClient::new(config)?,
            state: RwLock::new(State::default()),
            ready: AtomicBool::new(false),
            sync_interval: config.sync_interval(),
            last_success: AtomicU64::new(now_secs()),
            negative_cache: Mutex::new(HashMap::new()),
            inflight_permits: Semaphore::new(MAX_INFLIGHT_RESOLVES),
            limiter: Mutex::new(RateLimiter::new(RESOLVE_RATE_PER_SEC)),
        }))
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
    /// Unknown answers are cached briefly, and the lookups themselves are both
    /// rate limited and capped in flight, so a bad key cannot be used to hammer
    /// the control plane.
    pub async fn get_or_resolve(&self, key_id: &str) -> Option<Arc<KeyRecord>> {
        if let Some(record) = self.get(key_id) {
            return Some(record);
        }
        if self.negative_cached(key_id) {
            return None;
        }
        let Ok(_permit) = self.inflight_permits.try_acquire() else {
            tracing::warn!(
                key_id,
                "commercial authorize skipped: too many lookups in flight"
            );
            return None;
        };

        self.resolve(key_id).await.unwrap_or_else(|err| {
            tracing::warn!(key_id, error = %err, "commercial authorize failed; rejecting unknown key");
            None
        })
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
                self.cache_negative(key_id, generation);
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
        Some(record)
    }

    /// Remembers that the control plane knows nothing about `key_id`, unless a
    /// resync has replaced the generation the answer describes.
    fn cache_negative(&self, key_id: &str, generation: u64) {
        if self.state.read().unwrap().generation != generation {
            return;
        }
        let now = Instant::now();
        let mut cache = self.negative_cache.lock().unwrap();
        if cache.len() >= NEGATIVE_CACHE_CAPACITY && !cache.contains_key(key_id) {
            cache.retain(|_, expires_at| *expires_at > now);
            if cache.len() >= NEGATIVE_CACHE_CAPACITY {
                // Full of live entries: refuse rather than grow. The rate
                // limiter and the in-flight cap still bound what the lookups
                // this forgoes caching can cost.
                return;
            }
        }
        cache.insert(key_id.to_owned(), now + NEGATIVE_CACHE_TTL);
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
        match result {
            Ok(()) => self.last_success.store(now_secs(), Ordering::Release),
            Err(err) => self.report_sync_failure(&err),
        }
    }

    /// Seconds since the last sync the control plane answered.
    fn stale_for_seconds(&self) -> u64 {
        now_secs().saturating_sub(self.last_success.load(Ordering::Acquire))
    }

    /// A failed tick is routine — fail-static means the last good snapshot
    /// keeps serving — but how long that has been going on is not, so every
    /// failure carries it. An alert on the age is the operator's job.
    fn report_sync_failure(&self, err: &anyhow::Error) {
        tracing::warn!(
            error = %err,
            stale_for_seconds = self.stale_for_seconds(),
            ready = self.is_ready(),
            "commercial snapshot sync failed; serving last known keys"
        );
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
        // Records without forward movement cannot be trusted: `next_cursor`
        // defaults to zero, so applying such a page would also rewind the feed
        // to its first page and replay it forever. Fail the tick instead and
        // keep the last good state until the control plane makes sense again.
        anyhow::ensure!(
            next_cursor > cursor,
            "snapshot delta carried {} record(s) without advancing cursor {cursor} (next_cursor {next_cursor})",
            page.records.len()
        );
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
        let mut head = None;

        loop {
            anyhow::ensure!(
                pages < MAX_BOOTSTRAP_PAGES,
                "snapshot bootstrap exceeded {MAX_BOOTSTRAP_PAGES} pages"
            );
            let page = self.client.fetch_page(cursor).await?;
            pages += 1;

            // Pages from two epochs do not compose into a snapshot of either,
            // so a flip mid-bootstrap fails the tick. The next one starts from
            // cursor zero anyway, which is exactly the restart this used to do
            // by hand.
            let epoch_changed = pages > 1
                && page
                    .epoch
                    .as_ref()
                    .zip(epoch.as_ref())
                    .is_some_and(|(received, expected)| received != expected);
            anyhow::ensure!(
                !epoch_changed,
                "snapshot epoch changed mid-bootstrap: expected {epoch:?}, received {:?}",
                page.epoch
            );
            if page.epoch.is_some() {
                epoch = page.epoch;
            }
            if page.head_seq.is_some() {
                head = page.head_seq;
            }

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

        // A short page is the feed saying "that is all of it", and `head_seq`
        // is the feed saying how much there is. When they disagree the pages we
        // read are a subset of the key set — every key past `cursor` would 401
        // — so this is a failed tick, not a snapshot. The last good one keeps
        // serving; a portal that has none stays out of rotation.
        anyhow::ensure!(
            head.is_none_or(|head| cursor >= head),
            "snapshot bootstrap ended at cursor {cursor}, short of head {head:?}, after {pages} page(s)"
        );

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
        self.state.write().unwrap().epoch = Some(epoch);
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
    }

    fn apply_delta(&self, records: Vec<KeyRecord>, cursor: u64) -> usize {
        let mut applied = 0;
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
        applied
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commercial::{
        test_support::{key_record, page, MockControlPlane},
        types::KeyStatus,
    };

    async fn store_for(control_plane: &MockControlPlane) -> Arc<SnapshotStore> {
        SnapshotStore::new(&control_plane.config()).expect("store should build")
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

        let store = store_for(&cp).await;
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

    /// A feed that answers "no records, and I am 500 seqs ahead of you" is not
    /// describing an empty key set — it is not answering at all. Installing it
    /// would flip `ready` on a snapshot that knows nobody, so every valid key
    /// 401s fleet-wide. The tick fails and the store stays unready instead.
    #[tokio::test]
    async fn a_bootstrap_that_ends_short_of_the_head_installs_nothing() {
        let cp = MockControlPlane::spawn().await;
        cp.push_page(0, page(vec![], 0, Some("e1"), Some(500)));

        let store = store_for(&cp).await;
        store.run_tick().await;

        assert!(
            !store.is_ready(),
            "an empty snapshot 500 seqs behind the head must not install"
        );
        assert!(store.state.read().unwrap().records.is_empty());
        assert_eq!(store.state.read().unwrap().cursor, 0);
    }

    /// The same check catches the subtler shape: a page that is short — so the
    /// loop stops — while the head says most of the key set was never sent.
    /// Serving that subset 401s every customer it omits.
    #[tokio::test]
    async fn a_truncated_bootstrap_page_installs_nothing() {
        let cp = MockControlPlane::spawn().await;
        let records: Vec<_> = (0..3u64)
            .map(|i| key_record(&format!("k{i}"), i + 1))
            .collect();
        cp.push_page(0, page(records, 3, Some("e1"), Some(5000)));

        let store = store_for(&cp).await;
        store.run_tick().await;

        assert!(
            !store.is_ready(),
            "a 3-record subset of a 5000-seq feed must not install"
        );
        assert!(store.get("k0").is_none());
    }

    /// Pages read either side of a feed rebuild describe two different key
    /// sets, and a snapshot stitched from both is a snapshot of neither. The
    /// tick fails instead; the next one starts clean from cursor zero.
    #[tokio::test]
    async fn a_mid_bootstrap_epoch_change_installs_nothing() {
        let cp = MockControlPlane::spawn().await;
        let first: Vec<_> = (0..PAGE_LIMIT)
            .map(|i| key_record(&format!("k{i}"), u64::from(i) + 1))
            .collect();
        cp.push_page(0, page(first, u64::from(PAGE_LIMIT), Some("e1"), None));
        cp.push_page(
            u64::from(PAGE_LIMIT),
            page(vec![key_record("late", 5000)], 5000, Some("e2"), Some(5000)),
        );

        let store = store_for(&cp).await;
        store.run_tick().await;

        assert!(!store.is_ready(), "a mixed-epoch snapshot must not install");
        assert!(store.get("k0").is_none());
        assert!(store.get("late").is_none());
        assert_eq!(cp.snapshot_cursors(), vec![0, u64::from(PAGE_LIMIT)]);
    }

    #[tokio::test]
    async fn delta_upserts_by_sequence_and_advances_the_cursor() {
        let cp = MockControlPlane::spawn().await;
        cp.push_page(0, page(vec![key_record("k1", 1)], 1, Some("e1"), Some(1)));
        let store = store_for(&cp).await;
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
        let store = store_for(&cp).await;
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
        let store = store_for(&cp).await;
        store.run_tick().await;

        cp.push_page(40, page(vec![], 40, Some("e1"), Some(3)));
        cp.push_page(0, page(vec![key_record("k1", 3)], 3, Some("e1"), Some(3)));
        store.run_tick().await;

        assert_eq!(store.state.read().unwrap().cursor, 3);
        assert_eq!(cp.snapshot_cursors(), vec![0, 40, 0]);
    }

    /// `next_cursor` defaults to zero, so a page that omits it while carrying
    /// records would both apply them and rewind the cursor to the start of the
    /// feed — a first-page replay loop that never ends. Bootstrap already
    /// guards this; the delta path must too.
    #[tokio::test]
    async fn a_delta_that_does_not_advance_the_cursor_is_refused() {
        let cp = MockControlPlane::spawn().await;
        cp.push_page(0, page(vec![key_record("k1", 5)], 5, Some("e1"), Some(5)));
        let store = store_for(&cp).await;
        store.run_tick().await;
        assert_eq!(store.state.read().unwrap().cursor, 5);

        cp.push_page(
            5,
            serde_json::json!({
                "records": [key_record("k2", 6)],
                "epoch": "e1",
                "head_seq": 6,
            }),
        );
        store.run_tick().await;

        assert_eq!(
            store.state.read().unwrap().cursor,
            5,
            "an unusable page must leave the cursor where it was"
        );
        assert!(
            store.get("k2").is_none(),
            "and must not apply the records that came with it"
        );
        assert!(
            store.get("k1").is_some(),
            "the last good state keeps serving"
        );
    }

    /// A feed that has been down for days must not read like one that missed a
    /// single tick, so every failure says how old the served key set is.
    #[tokio::test]
    async fn a_failed_sync_reports_how_long_the_snapshot_has_been_stale() {
        let cp = MockControlPlane::spawn().await;
        cp.push_page(0, page(vec![key_record("k1", 1)], 1, Some("e1"), Some(1)));
        let store = store_for(&cp).await;
        store.run_tick().await;
        assert_eq!(store.stale_for_seconds(), 0);

        cp.fail_snapshots(true);
        store.run_tick().await;
        assert_eq!(store.stale_for_seconds(), 0, "one missed tick is routine");

        // As if the control plane had been unreachable for hours.
        store
            .last_success
            .store(now_secs() - 10_000, Ordering::Release);
        store.run_tick().await;
        assert!(
            store.stale_for_seconds() >= 10_000,
            "a failure does not refresh the age it reports"
        );

        cp.fail_snapshots(false);
        cp.push_page(1, page(vec![], 1, Some("e1"), Some(1)));
        store.run_tick().await;
        assert_eq!(store.stale_for_seconds(), 0, "a successful sync resets it");
    }

    #[tokio::test]
    async fn sync_failure_keeps_serving_the_last_snapshot() {
        let cp = MockControlPlane::spawn().await;
        cp.push_page(0, page(vec![key_record("k1", 1)], 1, Some("e1"), Some(1)));
        let store = store_for(&cp).await;
        store.run_tick().await;

        cp.fail_snapshots(true);
        store.run_tick().await;

        assert!(store.is_ready());
        assert!(store.get("k1").is_some());
    }

    #[tokio::test]
    async fn malformed_records_are_tombstoned_and_unidentifiable_ones_fail_the_page() {
        let cp = MockControlPlane::spawn().await;
        cp.push_page(0, page(vec![key_record("k1", 1)], 1, Some("e1"), Some(1)));
        let store = store_for(&cp).await;
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

        // A status this build predates is malformed like any other unreadable
        // field, and gets the same tombstone rather than a rule of its own —
        // digest and all, so the key cannot authenticate anyone.
        cp.push_page(
            2,
            serde_json::json!({
                "records": [{
                    "key_id": "k2",
                    "seq": 3,
                    "status": "suspended",
                    "secret_sha256": crate::commercial::test_support::SECRET_SHA256,
                }],
                "next_cursor": 3,
                "epoch": "e1",
            }),
        );
        store.run_tick().await;
        let k2 = store.get("k2").expect("an identifiable record is kept");
        assert_eq!(
            (k2.status, k2.secret_sha256.as_deref()),
            (KeyStatus::Revoked, None),
            "a status this build does not know must be tombstoned, not admitted"
        );

        cp.push_page(
            3,
            serde_json::json!({
                "records": [{"nothing": "identifiable"}],
                "next_cursor": 4,
                "epoch": "e1",
            }),
        );
        store.run_tick().await;
        assert_eq!(
            store.state.read().unwrap().cursor,
            3,
            "an unidentifiable record must not advance the cursor"
        );
    }

    /// The same status on the authorize path is a parse error rather than a
    /// record, so the key is rejected — and, unlike a "no such key" answer, not
    /// remembered: the control plane did have something to say.
    #[tokio::test]
    async fn an_authorize_answer_with_an_unknown_status_is_rejected_and_not_cached() {
        let cp = MockControlPlane::spawn().await;
        cp.push_page(0, page(vec![], 0, Some("e1"), Some(0)));
        cp.authorize_raw(
            "k1",
            serde_json::json!({"key_id": "k1", "seq": 1, "status": "suspended"}),
        );
        let store = store_for(&cp).await;
        store.run_tick().await;

        assert!(store.get_or_resolve("k1").await.is_none());
        assert!(store.get_or_resolve("k1").await.is_none());
        assert_eq!(cp.authorize_calls().len(), 2);
    }

    #[tokio::test]
    async fn authorize_on_miss_inserts_the_record() {
        let cp = MockControlPlane::spawn().await;
        cp.push_page(0, page(vec![], 0, Some("e1"), Some(0)));
        cp.authorize_with("k1", Some(key_record("k1", 3)));
        let store = store_for(&cp).await;
        store.run_tick().await;

        assert_eq!(store.get_or_resolve("k1").await.unwrap().seq, 3);
        assert_eq!(cp.authorize_calls(), vec!["k1".to_string()]);
        assert!(
            store.get("k1").is_some(),
            "a resolved key joins the snapshot"
        );
    }

    /// Every unknown key id a client presents lands here, so an anonymous
    /// flood of distinct tokens must not be able to grow the map without end.
    #[tokio::test]
    async fn the_negative_cache_is_capacity_capped() {
        let cp = MockControlPlane::spawn().await;
        let store = store_for(&cp).await;
        let generation = store.state.read().unwrap().generation;

        for i in 0..NEGATIVE_CACHE_CAPACITY + 500 {
            store.cache_negative(&format!("flood-{i}"), generation);
        }

        assert!(
            store.negative_cache.lock().unwrap().len() <= NEGATIVE_CACHE_CAPACITY,
            "the negative cache grew to {} entries",
            store.negative_cache.lock().unwrap().len()
        );
    }

    /// A resolve that started before an epoch resync answers for the OLD
    /// generation: its record must not join the new snapshot, and its
    /// negative answer must not be cached against it.
    #[tokio::test]
    async fn a_stale_generation_resolve_writes_nothing() {
        let cp = MockControlPlane::spawn().await;
        let store = store_for(&cp).await;
        let stale = store.state.read().unwrap().generation;

        // An epoch resync replaces the generation between the resolve's read
        // and its write-back.
        store.install(vec![], 0, Some("e2".to_owned()));

        assert!(store.upsert_resolved(key_record("k1", 5), stale).is_none());
        assert!(
            store.get("k1").is_none(),
            "a stale record must not join the new snapshot"
        );

        store.cache_negative("k2", stale);
        assert!(
            !store.negative_cached("k2"),
            "a stale negative answer must not be cached"
        );
    }

    #[tokio::test]
    async fn unknown_keys_are_negative_cached() {
        let cp = MockControlPlane::spawn().await;
        cp.push_page(0, page(vec![], 0, Some("e1"), Some(0)));
        let store = store_for(&cp).await;
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
        let store = store_for(&cp).await;
        store.run_tick().await;

        // What a burst of misses does to the bucket, without the burst.
        {
            let mut limiter = store.limiter.lock().unwrap();
            limiter.tokens = 0.0;
            limiter.last = Instant::now();
        }

        assert!(store.get_or_resolve("k1").await.is_none());
        assert!(cp.authorize_calls().is_empty());
    }

    #[tokio::test]
    async fn control_plane_errors_during_authorize_fail_closed() {
        let cp = MockControlPlane::spawn().await;
        cp.push_page(0, page(vec![], 0, Some("e1"), Some(0)));
        cp.authorize_status("k1", 500);
        let store = store_for(&cp).await;
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
