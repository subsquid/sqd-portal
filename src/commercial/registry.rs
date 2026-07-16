use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::Notify;

#[derive(Debug, Default)]
pub struct ActiveStreamRegistry {
    state: Mutex<RegistryState>,
}

#[derive(Debug, Default)]
struct RegistryState {
    streams: HashMap<String, StreamCtl>,
    shutting_down: bool,
}

#[derive(Debug, Clone)]
struct StreamCtl {
    key_id: Option<String>,
    kill: Arc<AtomicBool>,
    kill_notify: Arc<Notify>,
    shutdown: StreamShutdown,
    floor_bytes_per_sec: Arc<AtomicU64>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct StreamShutdown {
    pub shutdown_cut: Arc<AtomicBool>,
    pub finalized: Arc<AtomicBool>,
    pub finalized_notify: Arc<Notify>,
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
        self.register_with_notify(
            event_id,
            key_id,
            kill,
            Arc::new(Notify::new()),
            StreamShutdown {
                finalized: Arc::new(AtomicBool::new(true)),
                ..Default::default()
            },
            floor_bytes_per_sec,
        )
        .0
    }

    pub(crate) fn register_with_notify(
        self: &Arc<Self>,
        event_id: String,
        key_id: Option<String>,
        kill: Arc<AtomicBool>,
        kill_notify: Arc<Notify>,
        shutdown: StreamShutdown,
        floor_bytes_per_sec: Arc<AtomicU64>,
    ) -> (StreamRegistration, bool) {
        let mut state = self.state.lock().unwrap();
        let shutting_down = state.shutting_down;
        if shutting_down {
            shutdown.shutdown_cut.store(true, Ordering::Release);
            kill.store(true, Ordering::Release);
            kill_notify.notify_one();
        }
        state.streams.insert(
            event_id.clone(),
            StreamCtl {
                key_id,
                kill,
                kill_notify,
                shutdown,
                floor_bytes_per_sec,
            },
        );
        (
            StreamRegistration {
                registry: self.clone(),
                event_id,
            },
            shutting_down,
        )
    }

    pub fn kill_key(&self, key_id: &str) -> usize {
        let state = self.state.lock().unwrap();
        let mut killed = 0;
        for stream in state.streams.values() {
            if stream.key_id.as_deref() == Some(key_id) {
                stream.kill.store(true, Ordering::Release);
                stream.kill_notify.notify_one();
                killed += 1;
            }
        }
        killed
    }

    pub fn kill_all(&self) -> usize {
        let state = self.state.lock().unwrap();
        let mut killed = 0;
        for stream in state.streams.values() {
            stream.kill.store(true, Ordering::Release);
            stream.kill_notify.notify_one();
            killed += 1;
        }
        killed
    }

    pub async fn cut_all_for_shutdown(&self) -> usize {
        let streams = {
            let mut state = self.state.lock().unwrap();
            state.shutting_down = true;
            let streams = state.streams.values().cloned().collect::<Vec<_>>();
            for stream in &streams {
                stream.shutdown.shutdown_cut.store(true, Ordering::Release);
                stream.kill.store(true, Ordering::Release);
                stream.kill_notify.notify_one();
            }
            streams
        };
        for stream in &streams {
            loop {
                let notified = stream.shutdown.finalized_notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if stream.shutdown.finalized.load(Ordering::Acquire) {
                    break;
                }
                notified.await;
            }
        }
        streams.len()
    }

    pub fn set_floor_for_key(&self, key_id: &str, floor_bytes_per_sec: u64) -> usize {
        let state = self.state.lock().unwrap();
        let mut changed = 0;
        for stream in state.streams.values() {
            if stream.key_id.as_deref() == Some(key_id) {
                stream
                    .floor_bytes_per_sec
                    .store(floor_bytes_per_sec, Ordering::Release);
                changed += 1;
            }
        }
        changed
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.state.lock().unwrap().streams.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.state.lock().unwrap().streams.is_empty()
    }
}

impl Drop for StreamRegistration {
    fn drop(&mut self) {
        self.registry
            .state
            .lock()
            .unwrap()
            .streams
            .remove(&self.event_id);
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
