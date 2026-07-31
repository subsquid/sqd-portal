use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use subtle::ConstantTimeEq;

use super::{
    extractor::Credential,
    store::SnapshotStore,
    types::{KeyRecord, KeyStatus},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Admit,
    Reject(Rejection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rejection {
    pub status: StatusCode,
    /// Stable label for logs. Never sent to the client.
    pub reason: &'static str,
    /// Sent to the client verbatim; a de-facto API, so keep it stable.
    pub message: &'static str,
}

/// Unknown key and wrong secret answer identically: telling a caller that a key
/// id exists turns the endpoint into an enumeration oracle. The remaining 401s
/// are only reachable by someone holding the right secret, so they can be
/// specific.
const MISSING_CREDENTIAL: Rejection = Rejection {
    status: StatusCode::UNAUTHORIZED,
    reason: "missing_credential",
    message: "API key required",
};
const UNKNOWN_KEY: Rejection = Rejection {
    status: StatusCode::UNAUTHORIZED,
    reason: "unknown_key",
    message: "Invalid API key",
};
const INVALID_SECRET: Rejection = Rejection {
    status: StatusCode::UNAUTHORIZED,
    reason: "invalid_secret",
    message: "Invalid API key",
};
const REVOKED: Rejection = Rejection {
    status: StatusCode::UNAUTHORIZED,
    reason: "revoked",
    message: "API key revoked",
};
const EXPIRED: Rejection = Rejection {
    status: StatusCode::UNAUTHORIZED,
    reason: "expired",
    message: "API key expired",
};
const PORTAL_NOT_ALLOWED: Rejection = Rejection {
    status: StatusCode::FORBIDDEN,
    reason: "portal_not_allowed",
    message: "API key is not valid for this portal",
};
const DATASET_NOT_ALLOWED: Rejection = Rejection {
    status: StatusCode::FORBIDDEN,
    reason: "dataset_not_allowed",
    message: "API key is not authorized for this dataset",
};

/// A credential that cannot even be parsed never reaches the ladder.
pub(super) const MALFORMED: Rejection = Rejection {
    status: StatusCode::UNAUTHORIZED,
    reason: "malformed_credential",
    message: "Invalid API key",
};

/// Phase-1 authorization: authentication plus coarse portal/dataset scoping.
/// A key that passes streams unrestricted — no limits, no quota, no metering.
pub async fn evaluate(
    store: &SnapshotStore,
    portal_id: &str,
    credential: Option<&Credential>,
    dataset: Option<&str>,
    now_secs: u64,
) -> Decision {
    let Some(credential) = credential else {
        return Decision::Reject(MISSING_CREDENTIAL);
    };
    let Some(record) = store.get_or_resolve(&credential.key_id).await else {
        return Decision::Reject(UNKNOWN_KEY);
    };

    evaluate_record(&record, portal_id, credential, dataset, now_secs)
}

fn evaluate_record(
    record: &KeyRecord,
    portal_id: &str,
    credential: &Credential,
    dataset: Option<&str>,
    now_secs: u64,
) -> Decision {
    let Some(expected) = record.secret_sha256.as_deref() else {
        return Decision::Reject(INVALID_SECRET);
    };
    if !constant_time_eq(expected, &credential.secret_sha256) {
        return Decision::Reject(INVALID_SECRET);
    }
    if record.status != KeyStatus::Active {
        return Decision::Reject(REVOKED);
    }
    if record.expires_at.is_some_and(|expiry| expiry <= now_secs) {
        return Decision::Reject(EXPIRED);
    }
    // A null list means "any"; an explicit list is matched exactly.
    if let Some(portal_ids) = record.portal_ids.as_deref() {
        if !portal_ids.iter().any(|allowed| allowed == portal_id) {
            return Decision::Reject(PORTAL_NOT_ALLOWED);
        }
    }
    if let Some(datasets) = record.datasets.as_deref() {
        let allowed =
            dataset.is_some_and(|dataset| datasets.iter().any(|allowed| allowed == dataset));
        if !allowed {
            return Decision::Reject(DATASET_NOT_ALLOWED);
        }
    }

    Decision::Admit
}

impl Rejection {
    pub fn into_response(self) -> Response {
        (
            self.status,
            axum::Json(serde_json::json!({ "message": self.message })),
        )
            .into_response()
    }
}

/// Secret digests are compared without an early exit so a wrong key cannot be
/// refined byte by byte from response timing.
fn constant_time_eq(left: &str, right: &str) -> bool {
    left.as_bytes().ct_eq(right.as_bytes()).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commercial::{
        test_support::{key_record, store_with, SECRET_SHA256},
        types::KeyStatus,
    };

    const NOW: u64 = 1_800_000_000;
    const PORTAL: &str = "portal-premium-eu";

    fn credential(key_id: &str, secret_sha256: &str) -> Credential {
        Credential {
            key_id: key_id.to_owned(),
            secret_sha256: secret_sha256.to_owned(),
        }
    }

    fn valid() -> Credential {
        credential("k1", SECRET_SHA256)
    }

    async fn decide(
        record: KeyRecord,
        credential: Option<&Credential>,
        dataset: Option<&str>,
    ) -> Decision {
        let store = store_with(vec![record]);
        evaluate(&store, PORTAL, credential, dataset, NOW).await
    }

    fn reason(decision: Decision) -> &'static str {
        match decision {
            Decision::Admit => "admit",
            Decision::Reject(rejection) => rejection.reason,
        }
    }

    #[tokio::test]
    async fn rule_1_no_credential_is_401() {
        let decision = decide(key_record("k1", 1), None, Some("ethereum-mainnet")).await;

        assert_eq!(reason(decision), "missing_credential");
        assert_eq!(
            decision,
            Decision::Reject(Rejection {
                status: StatusCode::UNAUTHORIZED,
                ..MISSING_CREDENTIAL
            })
        );
    }

    #[tokio::test]
    async fn rule_2_unknown_key_is_401() {
        let store = store_with(vec![key_record("k1", 1)]);
        let credential = credential("other", SECRET_SHA256);

        let decision = evaluate(&store, PORTAL, Some(&credential), Some("eth"), NOW).await;

        assert_eq!(reason(decision), "unknown_key");
    }

    #[tokio::test]
    async fn rule_3_secret_mismatch_is_401() {
        let wrong = credential("k1", &"0".repeat(64));

        let decision = decide(key_record("k1", 1), Some(&wrong), Some("ethereum-mainnet")).await;

        assert_eq!(reason(decision), "invalid_secret");
    }

    #[tokio::test]
    async fn a_record_without_a_secret_digest_cannot_authenticate() {
        let mut record = key_record("k1", 1);
        record.secret_sha256 = None;

        let decision = decide(record, Some(&valid()), Some("ethereum-mainnet")).await;

        assert_eq!(reason(decision), "invalid_secret");
    }

    #[tokio::test]
    async fn rule_4_revoked_is_401() {
        let mut record = key_record("k1", 1);
        record.status = KeyStatus::Revoked;

        let decision = decide(record, Some(&valid()), Some("ethereum-mainnet")).await;

        assert_eq!(reason(decision), "revoked");
    }

    #[tokio::test]
    async fn rule_5_expiry_is_401_only_once_past() {
        let mut record = key_record("k1", 1);
        record.expires_at = Some(NOW + 1);
        assert_eq!(
            reason(decide(record.clone(), Some(&valid()), Some("ethereum-mainnet")).await),
            "admit"
        );

        record.expires_at = Some(NOW);
        assert_eq!(
            reason(decide(record.clone(), Some(&valid()), Some("ethereum-mainnet")).await),
            "expired"
        );

        record.expires_at = None;
        assert_eq!(
            reason(decide(record, Some(&valid()), Some("ethereum-mainnet")).await),
            "admit"
        );
    }

    #[tokio::test]
    async fn rule_6_portal_membership() {
        let mut record = key_record("k1", 1);

        record.portal_ids = None;
        assert_eq!(
            reason(decide(record.clone(), Some(&valid()), Some("ethereum-mainnet")).await),
            "admit",
            "a null list means any portal"
        );

        record.portal_ids = Some(vec!["portal-other".to_string(), PORTAL.to_string()]);
        assert_eq!(
            reason(decide(record.clone(), Some(&valid()), Some("ethereum-mainnet")).await),
            "admit"
        );

        record.portal_ids = Some(vec!["portal-other".to_string()]);
        assert_eq!(
            reason(decide(record.clone(), Some(&valid()), Some("ethereum-mainnet")).await),
            "portal_not_allowed"
        );

        record.portal_ids = Some(Vec::new());
        assert_eq!(
            reason(decide(record, Some(&valid()), Some("ethereum-mainnet")).await),
            "portal_not_allowed",
            "an empty list means no portal"
        );
    }

    #[tokio::test]
    async fn rule_6_returns_403_not_401() {
        let mut record = key_record("k1", 1);
        record.portal_ids = Some(vec!["portal-other".to_string()]);

        let decision = decide(record, Some(&valid()), Some("ethereum-mainnet")).await;

        assert!(matches!(
            decision,
            Decision::Reject(Rejection {
                status: StatusCode::FORBIDDEN,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn rule_7_dataset_membership_is_exact() {
        let mut record = key_record("k1", 1);

        record.datasets = None;
        assert_eq!(
            reason(decide(record.clone(), Some(&valid()), Some("ethereum-mainnet")).await),
            "admit",
            "a null list means every dataset"
        );

        record.datasets = Some(vec!["ethereum-mainnet".to_string()]);
        assert_eq!(
            reason(decide(record.clone(), Some(&valid()), Some("ethereum-mainnet")).await),
            "admit"
        );
        assert_eq!(
            reason(decide(record.clone(), Some(&valid()), Some("base-mainnet")).await),
            "dataset_not_allowed"
        );
        assert_eq!(
            reason(decide(record.clone(), Some(&valid()), Some("ethereum")).await),
            "dataset_not_allowed",
            "matching is exact, not by prefix or alias"
        );

        // `*` is a dataset name like any other: null already means "all".
        record.datasets = Some(vec!["*".to_string()]);
        assert_eq!(
            reason(decide(record.clone(), Some(&valid()), Some("ethereum-mainnet")).await),
            "dataset_not_allowed"
        );

        record.datasets = Some(vec!["ethereum-mainnet".to_string()]);
        assert_eq!(
            reason(decide(record, Some(&valid()), None).await),
            "dataset_not_allowed",
            "a dataset-scoped key cannot use an endpoint with no dataset"
        );
    }

    #[tokio::test]
    async fn rule_7_returns_403_not_401() {
        let mut record = key_record("k1", 1);
        record.datasets = Some(vec!["base-mainnet".to_string()]);

        let decision = decide(record, Some(&valid()), Some("ethereum-mainnet")).await;

        assert!(matches!(
            decision,
            Decision::Reject(Rejection {
                status: StatusCode::FORBIDDEN,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn rule_8_an_unscoped_active_key_is_admitted() {
        let decision = decide(
            key_record("k1", 1),
            Some(&valid()),
            Some("ethereum-mainnet"),
        )
        .await;

        assert_eq!(decision, Decision::Admit);
    }

    #[tokio::test]
    async fn earlier_rules_win_over_later_ones() {
        let mut record = key_record("k1", 1);
        record.status = KeyStatus::Revoked;
        record.expires_at = Some(1);
        record.portal_ids = Some(Vec::new());
        record.datasets = Some(Vec::new());

        // Wrong secret outranks every scope check.
        let wrong = credential("k1", &"0".repeat(64));
        assert_eq!(
            reason(decide(record.clone(), Some(&wrong), Some("ethereum-mainnet")).await),
            "invalid_secret"
        );
        // With the right secret, revocation outranks expiry and scoping.
        assert_eq!(
            reason(decide(record, Some(&valid()), Some("ethereum-mainnet")).await),
            "revoked"
        );
    }

    #[test]
    fn rejections_render_as_json_with_their_status() {
        let response = UNKNOWN_KEY.into_response();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .unwrap(),
            "application/json"
        );
    }

    #[test]
    fn constant_time_eq_matches_string_equality() {
        assert!(constant_time_eq("abc", "abc"));
        assert!(!constant_time_eq("abc", "abd"));
        assert!(!constant_time_eq("abc", "ab"));
        assert!(!constant_time_eq("", "a"));
        assert!(constant_time_eq("", ""));
    }
}
