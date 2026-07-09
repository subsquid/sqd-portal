use std::sync::Arc;
use std::time::Duration;

pub use client::{ControlPlaneClient, LocalControlPlane, NoopControlPlane};
pub use config::CommercialConfig;
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
mod config;
pub mod evaluate;
pub mod extractor;
pub mod meter;
pub mod registry;
mod reporter;
pub mod store;
pub mod tally;
pub mod types;

#[derive(Clone)]
pub struct CommercialRuntime {
    pub control_plane: Arc<dyn ControlPlaneClient>,
    pub usage_reporter: Arc<dyn UsageReporter>,
    pub snapshot_store: Option<Arc<SnapshotStore>>,
    pub tally: Option<Arc<TallyStore>>,
    pub registry: Option<Arc<ActiveStreamRegistry>>,
    reporter_task: Arc<std::sync::Mutex<Option<JoinHandle<()>>>>,
    snapshot_task: Arc<std::sync::Mutex<Option<JoinHandle<()>>>>,
}

struct BuildParts {
    control_plane: Arc<dyn ControlPlaneClient>,
    usage_reporter: Arc<dyn UsageReporter>,
    snapshot_store: Option<Arc<SnapshotStore>>,
    tally: Option<Arc<TallyStore>>,
    registry: Option<Arc<ActiveStreamRegistry>>,
    reporter_task: Option<JoinHandle<()>>,
    snapshot_task: Option<JoinHandle<()>>,
}

pub fn build(config: Option<&CommercialConfig>, cancel: CancellationToken) -> CommercialRuntime {
    let parts = match config {
        Some(config) => {
            let tally = Arc::new(TallyStore::default());
            let registry = Arc::new(ActiveStreamRegistry::default());
            let (store, snapshot_task) = SnapshotStore::spawn_with_hooks(
                config,
                cancel.clone(),
                SnapshotHooks {
                    tally: Some(tally.clone()),
                    registry: Some(registry.clone()),
                },
            )
            .expect("snapshot store should build");
            let (reporter, reporter_task) = BufferedUsageReporter::spawn(config, cancel);
            BuildParts {
                control_plane: Arc::new(LocalControlPlane::new(store.clone(), tally.clone())),
                usage_reporter: reporter,
                snapshot_store: Some(store),
                tally: Some(tally),
                registry: Some(registry),
                reporter_task: Some(reporter_task),
                snapshot_task: Some(snapshot_task),
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
    }
}

impl CommercialRuntime {
    pub async fn await_reporter_shutdown(&self) {
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
