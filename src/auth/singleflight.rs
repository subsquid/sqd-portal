//! One call in flight per key, with the entry retired as soon as nothing holds
//! it — the keys are attacker-chosen fingerprints, so a map that only grows is
//! a memory bound an attacker picks. Cancellation has to release the entry too,
//! hence an RAII guard rather than a cleanup on the happy path.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

#[derive(Default)]
pub struct KeyedLocks {
    entries: Mutex<HashMap<String, Entry>>,
}

struct Entry {
    lock: Arc<AsyncMutex<()>>,
    /// Counted rather than read off the `Arc`: a guard is itself a clone, so a
    /// refcount would answer for the lock as well as for the entry.
    holders: usize,
}

/// Exclusive use of a key. Both the lock and the entry are released when it
/// drops.
pub struct Held<'a> {
    // Declared first, so it drops first: retiring under a live guard would let
    // the next caller install a fresh lock and run concurrently with this one.
    _guard: OwnedMutexGuard<()>,
    _slot: Slot<'a>,
}

/// A claim on the key's entry, held whether or not its owner ever got the lock.
struct Slot<'a> {
    locks: &'a KeyedLocks,
    key: &'a str,
    lock: Arc<AsyncMutex<()>>,
}

impl KeyedLocks {
    /// Queues behind whoever holds `key`.
    pub async fn acquire<'a>(&'a self, key: &'a str) -> Held<'a> {
        let slot = self.claim(key);
        let guard = slot.lock.clone().lock_owned().await;
        Held {
            _guard: guard,
            _slot: slot,
        }
    }

    /// Gives up rather than queueing when `key` is already held.
    pub fn try_acquire<'a>(&'a self, key: &'a str) -> Option<Held<'a>> {
        let slot = self.claim(key);
        // On `None` the slot drops here, which checks the entry back in.
        let guard = slot.lock.clone().try_lock_owned().ok()?;
        Some(Held {
            _guard: guard,
            _slot: slot,
        })
    }

    fn claim<'a>(&'a self, key: &'a str) -> Slot<'a> {
        let mut entries = self.entries.lock().unwrap();
        let entry = entries.entry(key.to_owned()).or_insert_with(|| Entry {
            lock: Arc::new(AsyncMutex::new(())),
            holders: 0,
        });
        entry.holders += 1;
        let lock = entry.lock.clone();
        drop(entries);
        Slot {
            locks: self,
            key,
            lock,
        }
    }

    fn release(&self, key: &str) {
        let mut entries = self.entries.lock().unwrap();
        let Some(entry) = entries.get_mut(key) else {
            return;
        };
        entry.holders -= 1;
        if entry.holders == 0 {
            entries.remove(key);
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        self.locks.release(self.key);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use super::*;

    #[tokio::test]
    async fn one_key_admits_one_holder_at_a_time() {
        let locks = Arc::new(KeyedLocks::default());
        let concurrent = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..16 {
            let (locks, concurrent, peak) = (locks.clone(), concurrent.clone(), peak.clone());
            tasks.spawn(async move {
                let _held = locks.acquire("k").await;
                let now = concurrent.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(1)).await;
                concurrent.fetch_sub(1, Ordering::SeqCst);
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }

        assert_eq!(peak.load(Ordering::SeqCst), 1);
        assert!(locks.is_empty(), "every entry should have been retired");
    }

    #[tokio::test]
    async fn distinct_keys_do_not_serialize() {
        let locks = KeyedLocks::default();
        let _first = locks.acquire("a").await;

        assert!(
            locks.try_acquire("b").is_some(),
            "another key must not be blocked"
        );
        assert_eq!(locks.len(), 1, "the second key's entry is already retired");
    }

    #[tokio::test]
    async fn a_held_key_refuses_a_try_and_strands_nothing() {
        let locks = KeyedLocks::default();
        let held = locks.acquire("k").await;

        assert!(locks.try_acquire("k").is_none());
        assert_eq!(locks.len(), 1, "the refused try must not retire the entry");

        drop(held);
        assert!(locks.is_empty());
    }

    /// The case the RAII guard exists for: a caller that goes away while queued
    /// still has an entry to give back.
    #[tokio::test]
    async fn a_cancelled_waiter_gives_its_entry_back() {
        let locks = KeyedLocks::default();

        for _ in 0..8 {
            let held = locks.acquire("k").await;
            let queued = tokio::time::timeout(Duration::from_millis(5), locks.acquire("k")).await;
            assert!(queued.is_err(), "the first holder still has it");
            drop(held);
        }

        assert!(locks.is_empty());
    }

    /// Whoever releases last retires the entry — not whoever releases first.
    #[tokio::test]
    async fn the_entry_outlives_every_holder_but_the_last() {
        let locks = Arc::new(KeyedLocks::default());
        let held = locks.acquire("k").await;

        let queued = tokio::spawn({
            let locks = locks.clone();
            async move {
                let _held = locks.acquire("k").await;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(locks.len(), 1);

        drop(held);
        queued.await.unwrap();
        assert!(locks.is_empty());
    }
}
