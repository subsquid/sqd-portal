use async_trait::async_trait;

use super::types::{Authorization, AuthorizeRequest, Granted, GrantedLimits, OnExceed, Principal};

#[async_trait]
pub trait ControlPlaneClient: Send + Sync {
    async fn authorize(&self, req: AuthorizeRequest) -> Authorization;
}

#[derive(Debug, Default)]
pub struct NoopControlPlane;

#[async_trait]
impl ControlPlaneClient for NoopControlPlane {
    async fn authorize(&self, _req: AuthorizeRequest) -> Authorization {
        Authorization::Granted(Granted {
            principal: Principal {
                account_id: "oss".to_string(),
                api_key_id: None,
            },
            limits: GrantedLimits::default(),
            on_exceed: OnExceed::Reject,
            quota_version: 0,
        })
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

        assert_eq!(
            result,
            Authorization::Granted(Granted {
                principal: Principal {
                    account_id: "oss".to_string(),
                    api_key_id: None,
                },
                limits: GrantedLimits::default(),
                on_exceed: OnExceed::Reject,
                quota_version: 0,
            })
        );
    }
}
