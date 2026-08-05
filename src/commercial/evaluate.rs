use std::sync::Mutex;

use axum::{
    http::{header, HeaderValue},
    response::Response,
};
use subtle::ConstantTimeEq;

use super::{
    extractor::Credential,
    store::{Lookup, SnapshotStore},
    types::{KeyRecord, KeyStatus},
};
use crate::types::{coded_response, ErrorCode, RETRY_AFTER_FLOOR};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Admit,
    Reject(Rejection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rejection {
    /// The wire code, and through it the status (IB-5) and the type. Several
    /// rejections share one on purpose.
    pub code: ErrorCode,
    /// Stable label for protected logs. Never sent to the client and never a
    /// metric label — four of these collapse onto `invalid_credential`, and
    /// splitting them anywhere a client can read would undo that (INV-39).
    pub reason: &'static str,
}

impl Rejection {
    const fn new(code: ErrorCode, reason: &'static str) -> Self {
        Self { code, reason }
    }
}

/// Unknown key and wrong secret answer identically: telling a caller that a key
/// id exists turns the endpoint into an enumeration oracle. The remaining 401s
/// are only reachable by someone holding the right secret, so they can be
/// specific.
const MISSING_CREDENTIAL: Rejection =
    Rejection::new(ErrorCode::MissingCredential, "missing_credential");
const UNKNOWN_KEY: Rejection = Rejection::new(ErrorCode::InvalidCredential, "unknown_key");
const INVALID_SECRET: Rejection = Rejection::new(ErrorCode::InvalidCredential, "invalid_secret");
/// A record with no digest cannot establish that the caller holds the secret, so
/// it fails the secret rung rather than disclosing the later revoked one
/// (REQ-53).
const NO_DIGEST: Rejection = Rejection::new(ErrorCode::InvalidCredential, "no_digest");
const REVOKED: Rejection = Rejection::new(ErrorCode::RevokedCredential, "revoked");
/// A tombstone is digestless, so it cannot earn the specific `revoked` answer —
/// that would confirm a guessed key id exists. The operator's need for the
/// distinction is real and is met here, on `reason`, which only protected logs
/// see (ADR-017).
const REVOKED_TOMBSTONE: Rejection =
    Rejection::new(ErrorCode::InvalidCredential, "revoked_tombstone");
const EXPIRED: Rejection = Rejection::new(ErrorCode::ExpiredCredential, "expired");
const PORTAL_NOT_ALLOWED: Rejection =
    Rejection::new(ErrorCode::PortalNotAllowed, "portal_not_allowed");
const DATASET_NOT_ALLOWED: Rejection =
    Rejection::new(ErrorCode::DatasetNotAllowed, "dataset_not_allowed");

/// A credential that cannot even be parsed never reaches the ladder.
pub(super) const MALFORMED: Rejection =
    Rejection::new(ErrorCode::InvalidCredential, "malformed_credential");

/// Neither of these is an auth verdict: the portal did not decide the credential
/// is bad, it failed to find out. Answering `invalid_credential` would tell a
/// customer whose key was minted seconds ago to stop retrying (ADR-016 §3,
/// REQ-54).
const LOOKUP_SATURATED: Rejection = Rejection::new(ErrorCode::Overloaded, "lookup_saturated");
const LOOKUP_FAILED: Rejection = Rejection::new(ErrorCode::UpstreamUnavailable, "lookup_failed");

/// The dataset a request targets, named on demand. Naming it is not free: it
/// canonicalizes through the network client's catalog and interns the result in
/// a process-wide pool. Only one rung of the ladder needs it, and only for
/// dataset-scoped keys, so the ladder asks for it there rather than being
/// handed it up front — an unauthenticated request must not be able to buy that
/// work. The laziness is the whole point of this type.
pub struct LazyDataset<F: Fn() -> Option<String>> {
    name: F,
    resolved: Mutex<Option<Option<String>>>,
}

impl<F: Fn() -> Option<String>> LazyDataset<F> {
    pub fn new(name: F) -> Self {
        Self {
            name,
            resolved: Mutex::new(None),
        }
    }

    /// Resolves on first call and remembers the answer.
    pub fn resolve(&self) -> Option<String> {
        self.resolved
            .lock()
            .unwrap()
            .get_or_insert_with(&self.name)
            .clone()
    }

    /// What an earlier rung already resolved, if anything. Never resolves.
    pub fn resolved(&self) -> Option<String> {
        self.resolved.lock().unwrap().clone().flatten()
    }
}

/// Phase-1 authorization: authentication plus coarse portal/dataset scoping.
/// A key that passes streams unrestricted — no limits, no quota, no metering.
pub async fn evaluate<F: Fn() -> Option<String>>(
    store: &SnapshotStore,
    portal_id: &str,
    credential: Option<&Credential>,
    dataset: &LazyDataset<F>,
    now_secs: u64,
) -> Decision {
    let Some(credential) = credential else {
        return Decision::Reject(MISSING_CREDENTIAL);
    };
    let record = match store.get_or_resolve(&credential.key_id).await {
        Lookup::Found(record) => record,
        Lookup::Unknown => return Decision::Reject(UNKNOWN_KEY),
        Lookup::Saturated => return Decision::Reject(LOOKUP_SATURATED),
        Lookup::Unavailable => return Decision::Reject(LOOKUP_FAILED),
    };

    evaluate_record(&record, portal_id, credential, dataset, now_secs)
}

fn evaluate_record<F: Fn() -> Option<String>>(
    record: &KeyRecord,
    portal_id: &str,
    credential: &Credential,
    dataset: &LazyDataset<F>,
    now_secs: u64,
) -> Decision {
    let Some(expected) = record.secret_sha256.as_deref() else {
        // No digest is the control plane's tombstone shape, and a record that
        // cannot prove the caller holds the secret cannot disclose anything a
        // caller holding it would learn. Both answers are `invalid_credential`
        // on the wire; only `reason` tells them apart, in the protected log.
        return Decision::Reject(if record.status == KeyStatus::Active {
            NO_DIGEST
        } else {
            REVOKED_TOMBSTONE
        });
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
        // The first and only rung that needs the request's dataset named.
        let requested = dataset.resolve();
        let allowed = requested
            .as_deref()
            .is_some_and(|dataset| datasets.iter().any(|allowed| allowed == dataset));
        if !allowed {
            return Decision::Reject(DATASET_NOT_ALLOWED);
        }
    }

    Decision::Admit
}

impl Rejection {
    /// Answers in the ADR-011 envelope, so the routed middleware leaves the
    /// status alone and the response counts on the error-code axis as what it
    /// is. Built outside it, the same refusal is rewritten to 400
    /// `malformed_request` — the whole of GAP-29.
    pub fn into_response(self) -> Response {
        let mut response = coded_response(self.code, self.code.default_message());
        let headers = response.headers_mut();
        if self.code.challenges() {
            headers.insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        // Only the two lookup outcomes reach this; no auth verdict is retryable.
        if self.code.requires_hint() {
            headers.insert(header::RETRY_AFTER, RETRY_AFTER_FLOOR.into());
        }
        response
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
        test_support::{key_record, offline_store, store_with, SECRET_SHA256},
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
        let store = store_with(vec![record]).await;
        evaluate(&store, PORTAL, credential, &named(dataset), NOW).await
    }

    /// Tests hand the ladder a dataset name directly; only the request path has
    /// a resolution to defer.
    fn named(dataset: Option<&str>) -> LazyDataset<impl Fn() -> Option<String> + '_> {
        LazyDataset::new(move || dataset.map(str::to_owned))
    }

    fn reason(decision: Decision) -> &'static str {
        match decision {
            Decision::Admit => "admit",
            Decision::Reject(rejection) => rejection.reason,
        }
    }

    /// What the client is told, as opposed to what the log records.
    fn code(decision: Decision) -> &'static str {
        match decision {
            Decision::Admit => "admit",
            Decision::Reject(rejection) => rejection.code.as_str(),
        }
    }

    #[tokio::test]
    async fn rule_1_no_credential_is_401() {
        let decision = decide(key_record("k1", 1), None, Some("ethereum-mainnet")).await;

        assert_eq!(reason(decision), "missing_credential");
    }

    #[tokio::test]
    async fn rule_2_unknown_key_is_401() {
        let store = store_with(vec![key_record("k1", 1)]).await;
        let credential = credential("other", SECRET_SHA256);

        let decision = evaluate(&store, PORTAL, Some(&credential), &named(Some("eth")), NOW).await;

        assert_eq!(reason(decision), "unknown_key");
        assert_eq!(code(decision), "invalid_credential");
    }

    /// REQ-54: a snapshot miss the portal could not resolve is not a verdict on
    /// the credential. Answering `invalid_credential` — non-retryable — would
    /// tell the holder of a key minted seconds ago to give up, on the strength
    /// of the portal's own dependency being down.
    #[tokio::test]
    async fn an_unresolvable_miss_is_retryable_rather_than_a_bad_credential() {
        let store = offline_store(vec![key_record("k1", 1)]);
        let credential = credential("minted-just-now", SECRET_SHA256);

        let decision = evaluate(&store, PORTAL, Some(&credential), &named(None), NOW).await;

        assert_eq!(reason(decision), "lookup_failed");
        assert_eq!(code(decision), "upstream_unavailable");
        let Decision::Reject(rejection) = decision else {
            panic!("an unresolvable miss is refused");
        };
        assert!(
            rejection.code.error_type().retryable(),
            "the client must be told this one is worth retrying"
        );

        // A snapshot *hit* is unaffected: fail-static means the last good
        // snapshot keeps answering through the same outage.
        let decision = evaluate(&store, PORTAL, Some(&valid()), &named(None), NOW).await;
        assert_eq!(reason(decision), "admit");
    }

    /// The other half of REQ-54: the budget running out is congestion, and owes
    /// the client a back-off interval rather than a verdict.
    #[tokio::test]
    async fn a_spent_lookup_budget_is_overload() {
        let store = offline_store(Vec::new());
        store.exhaust_lookup_budget_for_test();

        let decision = evaluate(
            &store,
            PORTAL,
            Some(&credential("minted-just-now", SECRET_SHA256)),
            &named(None),
            NOW,
        )
        .await;

        assert_eq!(reason(decision), "lookup_saturated");
        assert_eq!(code(decision), "overloaded");

        let response = match decision {
            Decision::Reject(rejection) => rejection.into_response(),
            Decision::Admit => panic!("a spent budget refuses"),
        };
        assert_eq!(response.status(), crate::types::server_overloaded());
        assert!(
            response.headers().contains_key(header::RETRY_AFTER),
            "INV-26: an overload owes a back-off interval"
        );
    }

    #[tokio::test]
    async fn rule_3_secret_mismatch_is_401() {
        let wrong = credential("k1", &"0".repeat(64));

        let decision = decide(key_record("k1", 1), Some(&wrong), Some("ethereum-mainnet")).await;

        assert_eq!(reason(decision), "invalid_secret");
    }

    /// An *active* record with no digest is malformed rather than tombstoned,
    /// and there is nothing to authenticate against: it stays indistinguishable
    /// from a wrong secret.
    #[tokio::test]
    async fn a_record_without_a_secret_digest_cannot_authenticate() {
        let mut record = key_record("k1", 1);
        record.secret_sha256 = None;

        let decision = decide(record, Some(&valid()), Some("ethereum-mainnet")).await;

        assert_eq!(reason(decision), "no_digest");
        assert_eq!(code(decision), "invalid_credential");
    }

    /// The control plane publishes a revoked key as a tombstone: identity,
    /// status and sequence, and no digest at all. That is the only shape a
    /// revoked key ever arrives in, so it is the shape this rung is tested on.
    fn cp_tombstone() -> KeyRecord {
        serde_json::from_value(serde_json::json!({
            "key_id": "k1",
            "organization_id": "11111111-1111-1111-1111-111111111111",
            "status": "revoked",
            "seq": 2,
        }))
        .expect("the control plane's tombstone shape must parse")
    }

    /// A tombstone cannot check the presented secret, so it cannot tell the
    /// caller anything only a secret-holder should learn — including that the
    /// id it guessed exists. `revoked_credential` is reserved for a record that
    /// proved the caller holds the secret; the tombstone answers
    /// `invalid_credential` and keeps its own reason for the log (ADR-017).
    #[tokio::test]
    async fn rule_4_a_tombstone_is_revoked_in_the_log_and_invalid_on_the_wire() {
        let record = cp_tombstone();
        assert_eq!(record.secret_sha256, None);

        let decision = decide(record, Some(&valid()), Some("ethereum-mainnet")).await;

        assert_eq!(
            reason(decision),
            "revoked_tombstone",
            "the operator has to be able to tell a revoked key from a wrong secret"
        );
        assert_eq!(
            code(decision),
            "invalid_credential",
            "the client must not learn that the guessed id exists"
        );
    }

    /// A key that still carries a digest is a different matter: the caller has
    /// proved it holds the secret, so `revoked` discloses nothing it did not
    /// already know — but only after the secret is checked.
    #[tokio::test]
    async fn a_revoked_key_that_still_carries_a_digest_checks_the_secret_first() {
        let mut record = key_record("k1", 1);
        record.status = KeyStatus::Revoked;

        let wrong = credential("k1", &"0".repeat(64));
        assert_eq!(
            reason(decide(record.clone(), Some(&wrong), Some("ethereum-mainnet")).await),
            "invalid_secret"
        );
        let decision = decide(record, Some(&valid()), Some("ethereum-mainnet")).await;
        assert_eq!(reason(decision), "revoked");
        assert_eq!(code(decision), "revoked_credential");
    }

    /// The wire coarsening INV-39 rests on: four internal rungs, one code.
    #[tokio::test]
    async fn every_unauthenticated_rung_answers_the_same_code() {
        let mut active_without_digest = key_record("k1", 1);
        active_without_digest.secret_sha256 = None;

        let store = store_with(vec![key_record("k1", 1)]).await;
        let unknown_id = credential("other", SECRET_SHA256);

        let decisions = [
            // Unknown key id.
            evaluate(&store, PORTAL, Some(&unknown_id), &named(None), NOW).await,
            // Known id, wrong secret.
            decide(
                key_record("k1", 1),
                Some(&credential("k1", &"0".repeat(64))),
                None,
            )
            .await,
            // Active record carrying no digest.
            decide(active_without_digest, Some(&valid()), None).await,
            // Tombstone.
            decide(cp_tombstone(), Some(&valid()), None).await,
        ];

        let reasons: Vec<_> = decisions.iter().map(|d| reason(*d)).collect();
        assert_eq!(
            reasons,
            [
                "unknown_key",
                "invalid_secret",
                "no_digest",
                "revoked_tombstone"
            ],
            "the operator keeps the distinction on the protected axis"
        );
        for decision in decisions {
            assert_eq!(
                code(decision),
                "invalid_credential",
                "{}: the wire must not distinguish these",
                reason(decision)
            );
        }
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
    fn constant_time_eq_matches_string_equality() {
        assert!(constant_time_eq("abc", "abc"));
        assert!(!constant_time_eq("abc", "abd"));
        assert!(!constant_time_eq("abc", "ab"));
        assert!(!constant_time_eq("", "a"));
        assert!(constant_time_eq("", ""));
    }
}
