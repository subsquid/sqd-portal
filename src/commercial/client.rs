use async_trait::async_trait;

use super::{
    evaluate,
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
        limits: GrantedLimits::default(),
        on_exceed: OnExceed::Reject,
        quota_version: 0,
        quota_remaining_bytes: None,
        concurrency_permit: None,
    }
}

pub struct LocalControlPlane {
    store: std::sync::Arc<SnapshotStore>,
    tally: std::sync::Arc<TallyStore>,
    concurrency: std::sync::Arc<ConcurrencyLimiter>,
}

impl LocalControlPlane {
    pub fn new(
        store: std::sync::Arc<SnapshotStore>,
        tally: std::sync::Arc<TallyStore>,
        concurrency: std::sync::Arc<ConcurrencyLimiter>,
    ) -> Self {
        Self {
            store,
            tally,
            concurrency,
        }
    }
}

#[async_trait]
impl ControlPlaneClient for LocalControlPlane {
    async fn authorize(&self, req: AuthorizeRequest) -> Authorization {
        evaluate::evaluate_with_store(&self.store, Some(&self.tally), Some(&self.concurrency), req)
            .await
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
