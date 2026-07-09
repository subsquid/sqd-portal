use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use dashmap::DashMap;

#[derive(Debug, Default)]
pub struct TallyStore {
    entries: DashMap<String, Arc<Tally>>,
}

#[derive(Debug)]
struct Tally {
    version: AtomicU64,
    bytes: AtomicU64,
    last_seen: AtomicU64,
    lock: Mutex<()>,
}

impl TallyStore {
    pub fn rebase_account(&self, account_id: &str, version: u64) {
        let tally = self.entry(account_id, version);
        let _guard = tally.lock.lock().unwrap();
        tally.touch();
        let local = tally.version.load(Ordering::Acquire);
        if version > local {
            tally.bytes.store(0, Ordering::Release);
            tally.version.store(version, Ordering::Release);
        }
    }

    pub fn debit(&self, account_id: &str, grant_version: u64, bytes: u64) -> u64 {
        let tally = self.entry(account_id, grant_version);
        let _guard = tally.lock.lock().unwrap();
        tally.touch();
        let local = tally.version.load(Ordering::Acquire);
        if grant_version > local {
            tally.bytes.store(0, Ordering::Release);
            tally.version.store(grant_version, Ordering::Release);
        }
        tally.bytes.fetch_add(bytes, Ordering::AcqRel) + bytes
    }

    pub fn bytes_for(&self, account_id: &str, snapshot_version: u64) -> u64 {
        let tally = self.entry(account_id, snapshot_version);
        let _guard = tally.lock.lock().unwrap();
        tally.touch();
        let local = tally.version.load(Ordering::Acquire);
        if snapshot_version > local {
            tally.bytes.store(0, Ordering::Release);
            tally.version.store(snapshot_version, Ordering::Release);
            return 0;
        }
        tally.bytes.load(Ordering::Acquire)
    }

    pub fn effective_remaining(
        &self,
        account_id: &str,
        grant_version: u64,
        snapshot_remaining: i64,
    ) -> i64 {
        let served = self.bytes_for(account_id, grant_version);
        snapshot_remaining - served as i64
    }

    #[cfg(test)]
    pub fn version_for(&self, account_id: &str) -> Option<u64> {
        self.entries
            .get(account_id)
            .map(|entry| entry.version.load(Ordering::Acquire))
    }

    pub fn sweep_anonymous(&self, now_secs: u64, idle_secs: u64) -> usize {
        let cutoff = now_secs.saturating_sub(idle_secs);
        let keys: Vec<String> = self
            .entries
            .iter()
            .filter(|entry| {
                entry.key().starts_with("anon:")
                    && entry.value().last_seen.load(Ordering::Acquire) <= cutoff
            })
            .map(|entry| entry.key().clone())
            .collect();
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
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
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
    fn anonymous_sweep_removes_only_idle_anon_entries() {
        let tally = TallyStore::default();
        tally.debit("anon:one", 10, 1);
        tally.debit("account", 10, 1);
        let now = now_secs();

        assert_eq!(tally.sweep_anonymous(now + 120, 60), 1);
        assert!(tally.version_for("anon:one").is_none());
        assert_eq!(tally.version_for("account"), Some(10));
    }
}
