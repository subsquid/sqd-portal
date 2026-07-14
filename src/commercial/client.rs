use async_trait::async_trait;

use crate::metrics;

use super::{
    evaluate::{self, EvaluationPolicy},
    store::SnapshotStore,
    types::{Authorization, AuthorizeRequest, Granted, GrantedLimits, OnExceed, Principal},
    ConcurrencyLimiter, TallyStore,
};

#[async_trait]
pub trait ControlPlaneClient: Send + Sync {
    async fn authorize(&self, req: AuthorizeRequest) -> Authorization;
}

#[derive(Debug, Default)]
pub struct NoopControlPlane;

#[async_trait]
impl ControlPlaneClient for NoopControlPlane {
    async fn authorize(&self, _req: AuthorizeRequest) -> Authorization {
        Authorization::Granted(oss_grant())
    }
}

pub fn oss_grant() -> Granted {
    Granted {
        principal: Principal {
            account_id: "oss".to_string(),
            api_key_id: None,
        },
        tally_account_id: None,
        entitled_chains: None,
        entitled_traces: None,
        limits: GrantedLimits::default(),
        on_exceed: OnExceed::Reject,
        quota_version: 0,
        quota_remaining_bytes: None,
        snapshot_generation: None,
        concurrency_permit: None,
    }
}

pub struct LocalControlPlane {
    store: std::sync::Arc<SnapshotStore>,
    tally: std::sync::Arc<TallyStore>,
    concurrency: std::sync::Arc<ConcurrencyLimiter>,
    policy: EvaluationPolicy,
}

impl LocalControlPlane {
    pub fn new(
        store: std::sync::Arc<SnapshotStore>,
        tally: std::sync::Arc<TallyStore>,
        concurrency: std::sync::Arc<ConcurrencyLimiter>,
        policy: EvaluationPolicy,
    ) -> Self {
        Self {
            store,
            tally,
            concurrency,
            policy,
        }
    }
}

#[async_trait]
impl ControlPlaneClient for LocalControlPlane {
    async fn authorize(&self, req: AuthorizeRequest) -> Authorization {
        let result = evaluate::evaluate_with_store_and_policy(
            &self.store,
            Some(&self.tally),
            Some(&self.concurrency),
            req,
            self.policy,
        )
        .await;
        match &result {
            Authorization::Granted(_) => metrics::report_commercial_authorize("granted", "ok"),
            Authorization::Rejected(rejected) => {
                metrics::report_commercial_authorize("rejected", &rejected.reason)
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commercial::types::{Credential, Endpoint, QueryDescriptor};

    #[tokio::test]
    async fn noop_authorize_returns_unlimited_oss_grant() {
        let req = AuthorizeRequest {
            credential: Credential::None {
                ip_bucket: "unknown".to_string(),
            },
            dataset: "ethereum-mainnet".to_string(),
            endpoint: Endpoint::Stream,
            query: QueryDescriptor {
                requires_traces: false,
                requires_statediffs: false,
                first_block: None,
                chain_kind: None,
            },
        };

        let result = NoopControlPlane.authorize(req).await;

        assert_eq!(result, Authorization::Granted(oss_grant()));
    }
}
