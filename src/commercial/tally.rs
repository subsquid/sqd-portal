use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use dashmap::DashMap;

const ANON_PREFIX: &str = "anon:";
const ANON_TALLY_ENTRY_CAP: usize = 100_000;

#[derive(Debug, Default)]
pub struct TallyStore {
    entries: DashMap<String, Arc<Tally>>,
}

#[derive(Debug, Clone)]
pub struct TallyHandle {
    tally: Arc<Tally>,
}

#[derive(Debug)]
struct Tally {
    version: AtomicU64,
    bytes: AtomicU64,
    last_seen: AtomicU64,
    lock: Mutex<()>,
}

impl TallyStore {
    pub(crate) fn force_rebase_all(&self) -> usize {
        let mut rebased = 0;
        for entry in &self.entries {
            let tally = entry.value();
            let _guard = tally.lock.lock().unwrap();
            tally.touch();
            tally.bytes.store(0, Ordering::Release);
            tally.version.store(0, Ordering::Release);
            rebased += 1;
        }
        rebased
    }

    pub fn rebase_account(&self, account_id: &str, version: u64) {
        self.handle(account_id, version).rebase_to(version);
    }

    pub fn debit(&self, account_id: &str, grant_version: u64, bytes: u64) -> u64 {
        self.handle(account_id, grant_version)
            .debit(grant_version, bytes)
    }

    pub fn bytes_for(&self, account_id: &str, snapshot_version: u64) -> u64 {
        self.handle(account_id, snapshot_version)
            .bytes_for(snapshot_version)
    }

    pub fn effective_remaining(
        &self,
        account_id: &str,
        grant_version: u64,
        snapshot_remaining: i64,
    ) -> i64 {
        let served = self.bytes_for(account_id, grant_version);
        effective_remaining_after_served(snapshot_remaining, served)
    }

    pub fn handle(&self, account_id: &str, version: u64) -> TallyHandle {
        TallyHandle {
            tally: self.entry(account_id, version),
        }
    }

    #[cfg(test)]
    pub fn version_for(&self, account_id: &str) -> Option<u64> {
        self.entries
            .get(account_id)
            .map(|entry| entry.version.load(Ordering::Acquire))
    }

    pub fn sweep_anonymous(&self, now_secs: u64, idle_secs: u64) -> usize {
        self.sweep_anonymous_with_cap(now_secs, idle_secs, ANON_TALLY_ENTRY_CAP)
    }

    fn sweep_anonymous_with_cap(&self, now_secs: u64, idle_secs: u64, cap: usize) -> usize {
        let cutoff = now_secs.saturating_sub(idle_secs);
        // A live meter caches its Tally handle, so a stream whose chunk gap
        // exceeds the sweep horizon can be orphaned from this map. Debits touch
        // last_seen, making that practically negligible for active streams.
        let mut keys: Vec<String> = self
            .entries
            .iter()
            .filter(|entry| {
                entry.key().starts_with(ANON_PREFIX)
                    && entry.value().last_seen.load(Ordering::Acquire) <= cutoff
            })
            .map(|entry| entry.key().clone())
            .collect();

        let mut selected: HashSet<String> = keys.iter().cloned().collect();
        let anon_count = self
            .entries
            .iter()
            .filter(|entry| entry.key().starts_with(ANON_PREFIX))
            .count();
        let remaining_after_idle = anon_count.saturating_sub(selected.len());
        if remaining_after_idle > cap {
            let mut candidates: Vec<(u64, String)> = self
                .entries
                .iter()
                .filter(|entry| {
                    entry.key().starts_with(ANON_PREFIX) && !selected.contains(entry.key())
                })
                .map(|entry| {
                    (
                        entry.value().last_seen.load(Ordering::Acquire),
                        entry.key().clone(),
                    )
                })
                .collect();
            candidates
                .sort_unstable_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
            for (_, key) in candidates.into_iter().take(remaining_after_idle - cap) {
                selected.insert(key.clone());
                keys.push(key);
            }
        }

        let count = keys.len();
        for key in keys {
            self.entries.remove(&key);
        }
        count
    }

    fn entry(&self, account_id: &str, version: u64) -> Arc<Tally> {
        self.entries
            .entry(account_id.to_string())
            .or_insert_with(|| Arc::new(Tally::new(version)))
            .clone()
    }
}

impl TallyHandle {
    pub fn debit(&self, grant_version: u64, bytes: u64) -> u64 {
        let _guard = self.tally.lock.lock().unwrap();
        self.tally.touch();
        self.tally.rebase_if_newer(grant_version);
        self.tally.bytes.fetch_add(bytes, Ordering::AcqRel) + bytes
    }

    pub fn bytes_for(&self, snapshot_version: u64) -> u64 {
        let _guard = self.tally.lock.lock().unwrap();
        self.tally.touch();
        self.tally.rebase_if_newer(snapshot_version);
        self.tally.bytes.load(Ordering::Acquire)
    }

    pub fn debit_and_effective_remaining(
        &self,
        grant_version: u64,
        bytes: u64,
        snapshot_remaining: i64,
    ) -> i64 {
        let _guard = self.tally.lock.lock().unwrap();
        self.tally.touch();
        self.tally.rebase_if_newer(grant_version);
        let served = if bytes == 0 {
            self.tally.bytes.load(Ordering::Acquire)
        } else {
            self.tally.bytes.fetch_add(bytes, Ordering::AcqRel) + bytes
        };
        effective_remaining_after_served(snapshot_remaining, served)
    }

    fn rebase_to(&self, version: u64) {
        let _guard = self.tally.lock.lock().unwrap();
        self.tally.touch();
        self.tally.rebase_if_newer(version);
    }
}

impl Tally {
    fn new(version: u64) -> Self {
        Self {
            version: AtomicU64::new(version),
            bytes: AtomicU64::new(0),
            last_seen: AtomicU64::new(now_secs()),
            lock: Mutex::new(()),
        }
    }

    fn touch(&self) {
        self.last_seen.store(now_secs(), Ordering::Release);
    }

    fn rebase_if_newer(&self, version: u64) {
        let local = self.version.load(Ordering::Acquire);
        if version > local {
            self.bytes.store(0, Ordering::Release);
            self.version.store(version, Ordering::Release);
        }
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(crate) fn effective_remaining_after_served(snapshot_remaining: i64, served: u64) -> i64 {
    snapshot_remaining
        .max(0)
        .saturating_sub(i64::try_from(served).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rebase_matrix_resets_only_for_newer_versions() {
        let tally = TallyStore::default();
        assert_eq!(tally.debit("account", 10, 100), 100);
        assert_eq!(tally.bytes_for("account", 10), 100);

        tally.rebase_account("account", 9);
        assert_eq!(tally.version_for("account"), Some(10));
        assert_eq!(tally.bytes_for("account", 9), 100);

        tally.rebase_account("account", 10);
        assert_eq!(tally.bytes_for("account", 10), 100);

        tally.rebase_account("account", 11);
        assert_eq!(tally.version_for("account"), Some(11));
        assert_eq!(tally.bytes_for("account", 11), 0);
    }

    #[test]
    fn stale_stream_debits_current_version_conservatively() {
        let tally = TallyStore::default();
        tally.rebase_account("account", 2);

        assert_eq!(tally.debit("account", 1, 50), 50);
        assert_eq!(tally.version_for("account"), Some(2));
        assert_eq!(tally.effective_remaining("account", 2, 100), 50);
    }

    #[test]
    fn handle_debits_and_checks_remaining_under_one_lock() {
        let tally = TallyStore::default();
        let handle = tally.handle("account", 7);

        assert_eq!(handle.debit_and_effective_remaining(7, 3, 5), 2);
        assert_eq!(handle.debit_and_effective_remaining(7, 4, 5), -2);
        assert_eq!(tally.bytes_for("account", 7), 7);
    }

    #[test]
    fn effective_remaining_clamps_negative_snapshots_and_saturates_served_bytes() {
        assert_eq!(effective_remaining_after_served(i64::MIN, 0), 0);
        assert_eq!(effective_remaining_after_served(i64::MIN, 1), -1);
        assert_eq!(
            effective_remaining_after_served(5, u64::MAX),
            5i64.saturating_sub(i64::MAX)
        );
    }

    #[test]
    fn anonymous_sweep_removes_only_idle_anon_entries() {
        let tally = TallyStore::default();
        tally.debit("anon:one", 10, 1);
        tally.debit("account", 10, 1);
        let now = now_secs();

        assert_eq!(tally.sweep_anonymous(now + 120, 60), 1);
        assert!(tally.version_for("anon:one").is_none());
        assert_eq!(tally.version_for("account"), Some(10));
    }

    #[test]
    fn anonymous_sweep_caps_oldest_anon_entries() {
        let tally = TallyStore::default();
        for i in 0..5 {
            let key = format!("anon:{i}");
            tally.debit(&key, 10, 1);
            tally
                .entries
                .get(&key)
                .unwrap()
                .last_seen
                .store(i + 1, Ordering::Release);
        }
        tally.debit("account", 10, 1);

        assert_eq!(tally.sweep_anonymous_with_cap(10, 20, 3), 2);
        assert!(tally.version_for("anon:0").is_none());
        assert!(tally.version_for("anon:1").is_none());
        assert_eq!(tally.version_for("anon:2"), Some(10));
        assert_eq!(tally.version_for("anon:3"), Some(10));
        assert_eq!(tally.version_for("anon:4"), Some(10));
        assert_eq!(tally.version_for("account"), Some(10));
    }
}
