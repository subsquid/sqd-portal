use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

use dashmap::DashMap;

#[derive(Debug, Default)]
pub struct ActiveStreamRegistry {
    streams: DashMap<String, StreamCtl>,
}

#[derive(Debug, Clone)]
struct StreamCtl {
    key_id: Option<String>,
    kill: Arc<AtomicBool>,
    floor_bytes_per_sec: Arc<AtomicU64>,
}

pub struct StreamRegistration {
    registry: Arc<ActiveStreamRegistry>,
    event_id: String,
}

impl ActiveStreamRegistry {
    pub fn register(
        self: &Arc<Self>,
        event_id: String,
        key_id: Option<String>,
        kill: Arc<AtomicBool>,
        floor_bytes_per_sec: Arc<AtomicU64>,
    ) -> StreamRegistration {
        self.streams.insert(
            event_id.clone(),
            StreamCtl {
                key_id,
                kill,
                floor_bytes_per_sec,
            },
        );
        StreamRegistration {
            registry: self.clone(),
            event_id,
        }
    }

    pub fn kill_key(&self, key_id: &str) -> usize {
        let mut killed = 0;
        for entry in self.streams.iter() {
            if entry.value().key_id.as_deref() == Some(key_id) {
                entry.value().kill.store(true, Ordering::Release);
                killed += 1;
            }
        }
        killed
    }

    pub fn set_floor_for_key(&self, key_id: &str, floor_bytes_per_sec: u64) -> usize {
        let mut changed = 0;
        for entry in self.streams.iter() {
            if entry.value().key_id.as_deref() == Some(key_id) {
                entry
                    .value()
                    .floor_bytes_per_sec
                    .store(floor_bytes_per_sec, Ordering::Release);
                changed += 1;
            }
        }
        changed
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.streams.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.streams.is_empty()
    }
}

impl Drop for StreamRegistration {
    fn drop(&mut self) {
        self.registry.streams.remove(&self.event_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kill_key_flips_matching_streams_and_registration_drops() {
        let registry = Arc::new(ActiveStreamRegistry::default());
        let kill = Arc::new(AtomicBool::new(false));
        let floor = Arc::new(AtomicU64::new(0));
        let registration = registry.register(
            "event".to_string(),
            Some("key".to_string()),
            kill.clone(),
            floor.clone(),
        );

        assert_eq!(registry.kill_key("other"), 0);
        assert!(!kill.load(Ordering::Acquire));
        assert_eq!(registry.kill_key("key"), 1);
        assert!(kill.load(Ordering::Acquire));
        assert_eq!(registry.set_floor_for_key("key", 7), 1);
        assert_eq!(floor.load(Ordering::Acquire), 7);

        drop(registration);
        assert_eq!(registry.len(), 0);
    }
}
