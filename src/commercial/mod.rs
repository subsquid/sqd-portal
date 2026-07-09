use std::sync::Arc;

pub use client::{ControlPlaneClient, NoopControlPlane};
pub use config::CommercialConfig;
pub use reporter::{NoopUsageReporter, UsageReporter};
pub use types::*;

mod client;
mod config;
pub mod extractor;
mod reporter;
pub mod types;

#[derive(Clone)]
pub struct CommercialRuntime {
    pub control_plane: Arc<dyn ControlPlaneClient>,
    pub usage_reporter: Arc<dyn UsageReporter>,
}

pub fn build(_config: Option<&CommercialConfig>) -> CommercialRuntime {
    CommercialRuntime {
        control_plane: Arc::new(NoopControlPlane),
        usage_reporter: Arc::new(NoopUsageReporter),
    }
}
