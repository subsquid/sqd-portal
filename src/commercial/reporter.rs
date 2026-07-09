use super::types::StreamUsageEvent;

pub trait UsageReporter: Send + Sync {
    fn report(&self, event: StreamUsageEvent);
}

#[derive(Debug, Default)]
pub struct NoopUsageReporter;

impl UsageReporter for NoopUsageReporter {
    fn report(&self, _event: StreamUsageEvent) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commercial::types::{DataSource, Endpoint, UsageStatus};

    #[test]
    fn noop_reporter_drops_events() {
        let reporter = NoopUsageReporter;
        reporter.report(StreamUsageEvent {
            event_id: "event".to_string(),
            request_id: "request".to_string(),
            account_id: "oss".to_string(),
            api_key_id: None,
            dataset: "ethereum-mainnet".to_string(),
            endpoint: Endpoint::Stream,
            data_source: DataSource::Network,
            logical_bytes: 1,
            wire_bytes: 1,
            blocks: 0,
            chunks: 0,
            started_at: 0.0,
            duration_ms: 0,
            status: UsageStatus::Completed,
            pod: "pod".to_string(),
            quota_version: 0,
        });
    }
}
