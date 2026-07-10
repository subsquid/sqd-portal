use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub use client::{ControlPlaneClient, LocalControlPlane, NoopControlPlane};
pub use concurrency::ConcurrencyLimiter;
pub use config::{CommercialConfig, PublicFallbackConfig};
use evaluate::EvaluationPolicy;
pub use extractor::CommercialGrant;
pub use meter::MeterHandle;
pub use registry::ActiveStreamRegistry;
pub use reporter::{BufferedUsageReporter, NoopUsageReporter, UsageReporter};
pub use store::{SnapshotHooks, SnapshotStore};
pub use tally::TallyStore;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
pub use types::*;

mod client;
pub mod concurrency;
mod config;
pub mod evaluate;
pub mod extractor;
pub mod meter;
pub mod registry;
mod reporter;
pub mod store;
pub mod tally;
pub mod types;
pub(crate) mod zero_limits;

#[derive(Clone)]
pub struct CommercialRuntime {
    pub control_plane: Arc<dyn ControlPlaneClient>,
    pub usage_reporter: Arc<dyn UsageReporter>,
    pub snapshot_store: Option<Arc<SnapshotStore>>,
    pub tally: Option<Arc<TallyStore>>,
    pub registry: Option<Arc<ActiveStreamRegistry>>,
    reporter_task: Arc<std::sync::Mutex<Option<JoinHandle<()>>>>,
    snapshot_task: Arc<std::sync::Mutex<Option<JoinHandle<()>>>>,
    maintenance_task: Arc<std::sync::Mutex<Option<JoinHandle<()>>>>,
}

struct BuildParts {
    control_plane: Arc<dyn ControlPlaneClient>,
    usage_reporter: Arc<dyn UsageReporter>,
    snapshot_store: Option<Arc<SnapshotStore>>,
    tally: Option<Arc<TallyStore>>,
    registry: Option<Arc<ActiveStreamRegistry>>,
    reporter_task: Option<JoinHandle<()>>,
    snapshot_task: Option<JoinHandle<()>>,
    maintenance_task: Option<JoinHandle<()>>,
}

pub fn build(config: Option<&CommercialConfig>, cancel: CancellationToken) -> CommercialRuntime {
    let parts = match config {
        Some(config) => {
            let tally = Arc::new(TallyStore::default());
            let registry = Arc::new(ActiveStreamRegistry::default());
            let concurrency = Arc::new(ConcurrencyLimiter::new(config.pod_count));
            let (store, snapshot_task) = SnapshotStore::spawn_with_hooks(
                config,
                cancel.clone(),
                SnapshotHooks {
                    tally: Some(tally.clone()),
                    registry: Some(registry.clone()),
                },
            )
            .expect("snapshot store should build");
            let (reporter, reporter_task) = BufferedUsageReporter::spawn(config, cancel.clone());
            let maintenance_task = spawn_maintenance_task(
                tally.clone(),
                concurrency.clone(),
                store.clone(),
                Duration::from_secs(config.sync_interval_secs.max(1)),
                config.sweep_horizon(),
                cancel.clone(),
            );
            BuildParts {
                control_plane: Arc::new(LocalControlPlane::new(
                    store.clone(),
                    tally.clone(),
                    concurrency,
                    EvaluationPolicy {
                        throttle_residual_secs: config.throttle_residual_secs,
                    },
                )),
                usage_reporter: reporter,
                snapshot_store: Some(store),
                tally: Some(tally),
                registry: Some(registry),
                reporter_task: Some(reporter_task),
                snapshot_task: Some(snapshot_task),
                maintenance_task: Some(maintenance_task),
            }
        }
        None => BuildParts {
            control_plane: Arc::new(NoopControlPlane),
            usage_reporter: Arc::new(NoopUsageReporter),
            snapshot_store: None,
            tally: None,
            registry: None,
            reporter_task: None,
            snapshot_task: None,
            maintenance_task: None,
        },
    };

    CommercialRuntime {
        control_plane: parts.control_plane,
        usage_reporter: parts.usage_reporter,
        snapshot_store: parts.snapshot_store,
        tally: parts.tally,
        registry: parts.registry,
        reporter_task: Arc::new(std::sync::Mutex::new(parts.reporter_task)),
        snapshot_task: Arc::new(std::sync::Mutex::new(parts.snapshot_task)),
        maintenance_task: Arc::new(std::sync::Mutex::new(parts.maintenance_task)),
    }
}

fn spawn_maintenance_task(
    tally: Arc<TallyStore>,
    concurrency: Arc<ConcurrencyLimiter>,
    store: Arc<SnapshotStore>,
    interval: Duration,
    sweep_horizon: Duration,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let horizon_secs = sweep_horizon.as_secs();
            let tally_removed = tally.sweep_anonymous(now_secs(), horizon_secs);
            let concurrency_removed = concurrency.sweep_idle(sweep_horizon);
            let negative_cache_removed = store.sweep_negative_cache();
            if tally_removed > 0 || concurrency_removed > 0 || negative_cache_removed > 0 {
                tracing::debug!(
                    tally_removed,
                    concurrency_removed,
                    negative_cache_removed,
                    "commercial maintenance sweep removed idle entries"
                );
            }

            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(interval) => {}
            }
        }
    })
}

impl CommercialRuntime {
    pub async fn await_reporter_shutdown(&self) {
        self.await_maintenance_shutdown().await;
        self.await_snapshot_shutdown().await;
        let reporter_task = self.reporter_task.lock().unwrap().take();
        let Some(reporter_task) = reporter_task else {
            return;
        };

        match tokio::time::timeout(Duration::from_secs(5), reporter_task).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                tracing::warn!(error = %err, "commercial usage reporter task failed during shutdown");
            }
            Err(_) => {
                tracing::warn!("timed out waiting for commercial usage reporter shutdown");
            }
        }
    }

    async fn await_maintenance_shutdown(&self) {
        let maintenance_task = self.maintenance_task.lock().unwrap().take();
        let Some(maintenance_task) = maintenance_task else {
            return;
        };

        match tokio::time::timeout(Duration::from_secs(5), maintenance_task).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                tracing::warn!(error = %err, "commercial maintenance task failed during shutdown");
            }
            Err(_) => {
                tracing::warn!("timed out waiting for commercial maintenance shutdown");
            }
        }
    }

    async fn await_snapshot_shutdown(&self) {
        let snapshot_task = self.snapshot_task.lock().unwrap().take();
        let Some(snapshot_task) = snapshot_task else {
            return;
        };

        match tokio::time::timeout(Duration::from_secs(5), snapshot_task).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                tracing::warn!(error = %err, "commercial snapshot task failed during shutdown");
            }
            Err(_) => {
                tracing::warn!("timed out waiting for commercial snapshot shutdown");
            }
        }
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
    use crate::commercial::config::PublicFallbackConfig;

    #[tokio::test]
    async fn maintenance_task_sweeps_idle_commercial_maps() {
        let tally = Arc::new(TallyStore::default());
        tally.debit("anon:bucket", 1, 10);
        let concurrency = Arc::new(ConcurrencyLimiter::new(1));
        let permit = concurrency.try_acquire("anon:bucket", 1).unwrap();
        drop(permit);
        let store = SnapshotStore::inactive(&PublicFallbackConfig {
            throughput_bytes_per_sec: 1,
            burst_bytes: 1,
            max_response_bytes: 1,
            volume_bytes: 1,
            window_secs: 1,
            concurrency: 1,
        });
        store.insert_negative_cache_for_test("missing", Duration::ZERO);

        let cancel = CancellationToken::new();
        let task = spawn_maintenance_task(
            tally.clone(),
            concurrency.clone(),
            store.clone(),
            Duration::from_secs(60),
            Duration::ZERO,
            cancel.clone(),
        );
        tokio::task::yield_now().await;

        assert!(tally.version_for("anon:bucket").is_none());
        assert_eq!(concurrency.sweep_idle(Duration::ZERO), 0);
        assert_eq!(store.negative_cache_len(), 0);

        cancel.cancel();
        task.await.unwrap();
    }
}
