use std::sync::Arc;
use std::time::Duration;

pub use client::{ControlPlaneClient, NoopControlPlane};
pub use config::CommercialConfig;
pub use extractor::CommercialGrant;
pub use meter::MeterHandle;
pub use reporter::{BufferedUsageReporter, NoopUsageReporter, UsageReporter};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
pub use types::*;

mod client;
mod config;
pub mod extractor;
pub mod meter;
mod reporter;
pub mod types;

#[derive(Clone)]
pub struct CommercialRuntime {
    pub control_plane: Arc<dyn ControlPlaneClient>,
    pub usage_reporter: Arc<dyn UsageReporter>,
    reporter_task: Arc<std::sync::Mutex<Option<JoinHandle<()>>>>,
}

pub fn build(config: Option<&CommercialConfig>, cancel: CancellationToken) -> CommercialRuntime {
    let (usage_reporter, reporter_task): (Arc<dyn UsageReporter>, Option<JoinHandle<()>>) =
        match config {
            Some(config) => {
                let (reporter, task) = BufferedUsageReporter::spawn(config, cancel);
                (reporter, Some(task))
            }
            None => (Arc::new(NoopUsageReporter), None),
        };

    CommercialRuntime {
        control_plane: Arc::new(NoopControlPlane),
        usage_reporter,
        reporter_task: Arc::new(std::sync::Mutex::new(reporter_task)),
    }
}

impl CommercialRuntime {
    pub async fn await_reporter_shutdown(&self) {
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
}
