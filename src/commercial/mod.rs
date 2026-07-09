use std::sync::Arc;

pub use client::{ControlPlaneClient, NoopControlPlane};
pub use config::CommercialConfig;
pub use extractor::CommercialGrant;
pub use meter::MeterHandle;
pub use reporter::{BufferedUsageReporter, NoopUsageReporter, UsageReporter};
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
}

pub fn build(config: Option<&CommercialConfig>, cancel: CancellationToken) -> CommercialRuntime {
    let usage_reporter: Arc<dyn UsageReporter> = match config {
        Some(config) => BufferedUsageReporter::spawn(config, cancel),
        None => Arc::new(NoopUsageReporter),
    };

    CommercialRuntime {
        control_plane: Arc::new(NoopControlPlane),
        usage_reporter,
    }
}
