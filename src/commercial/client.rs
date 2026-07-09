use async_trait::async_trait;

use super::{
    store::SnapshotStore,
    types::{
        Authorization, AuthorizeRequest, Credential, Granted, GrantedLimits, OnExceed, Principal,
        Rejected,
    },
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
        limits: GrantedLimits::default(),
        on_exceed: OnExceed::Reject,
        quota_version: 0,
    }
}

pub struct LocalControlPlane {
    store: std::sync::Arc<SnapshotStore>,
}

impl LocalControlPlane {
    pub fn new(store: std::sync::Arc<SnapshotStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl ControlPlaneClient for LocalControlPlane {
    async fn authorize(&self, req: AuthorizeRequest) -> Authorization {
        if let Credential::Key { key_id, .. } = &req.credential {
            if self.store.get_or_resolve(key_id).await.is_none() {
                return Authorization::Rejected(Rejected {
                    reason: "invalid_key".to_string(),
                    http_status: 401,
                    message: "Invalid API key".to_string(),
                    retry_after_secs: None,
                });
            }
        }

        Authorization::Granted(oss_grant())
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
