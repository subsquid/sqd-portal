use std::sync::{Arc, Mutex};

use axum::{http::header, response::Response};

use super::{
    cache::{CachedGrant, GrantCache, Resolved},
    extractor::Credential,
    types::denial,
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
    /// metric label: several of these collapse onto `invalid_credential`
    /// (INV-39).
    pub reason: &'static str,
}

impl Rejection {
    const fn new(code: ErrorCode, reason: &'static str) -> Self {
        Self { code, reason }
    }
}

/// Unknown key and wrong secret answer identically, or the endpoint becomes an
/// enumeration oracle. The specific codes are reachable only by someone holding
/// the right secret, and share a status with the ones that are not.
const MISSING_CREDENTIAL: Rejection =
    Rejection::new(ErrorCode::MissingCredential, "missing_credential");
const UNKNOWN_KEY: Rejection = Rejection::new(ErrorCode::InvalidCredential, "unknown_key");
const INVALID_SECRET: Rejection = Rejection::new(ErrorCode::InvalidCredential, "invalid_secret");
/// A denial this build has no code for. Still a denial — reported as the
/// coarsest one, which is the fail-closed direction — and its raw reason
/// reaches the protected log.
const UNRECOGNIZED_DENIAL: Rejection =
    Rejection::new(ErrorCode::InvalidCredential, "denied_unrecognized");
const REVOKED: Rejection = Rejection::new(ErrorCode::RevokedCredential, "revoked");
const EXPIRED: Rejection = Rejection::new(ErrorCode::ExpiredCredential, "expired");
const PORTAL_NOT_ALLOWED: Rejection =
    Rejection::new(ErrorCode::PortalNotAllowed, "portal_not_allowed");
const DATASET_NOT_ALLOWED: Rejection =
    Rejection::new(ErrorCode::DatasetNotAllowed, "dataset_not_allowed");

/// A credential that cannot even be parsed never reaches the ladder, and never
/// costs an exchange.
pub(super) const MALFORMED: Rejection =
    Rejection::new(ErrorCode::InvalidCredential, "malformed_credential");

/// Neither of these is an auth verdict: the portal did not decide the credential
/// is bad, it failed to find out. Answering `invalid_credential` — non-retryable
/// — would tell a customer whose key is perfectly good to stop retrying, on the
/// strength of the portal's own dependency being down (REQ-54).
const EXCHANGE_SATURATED: Rejection = Rejection::new(ErrorCode::Overloaded, "exchange_saturated");
const EXCHANGE_FAILED: Rejection =
    Rejection::new(ErrorCode::UpstreamUnavailable, "exchange_failed");

/// The dataset a request targets, named on demand. Naming it canonicalizes
/// through the catalog and interns into a process-wide pool, and only one rung
/// needs it — an unauthenticated request must not be able to buy that work.
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

/// The verdict, and the raw denial reason behind it where there was one. The
/// reason exists only for the protected log — the wire and the scrape both see
/// `Decision` and nothing finer.
pub struct Verdict {
    pub decision: Decision,
    pub denial_reason: Option<String>,
}

impl Verdict {
    fn of(decision: Decision) -> Self {
        Self {
            decision,
            denial_reason: None,
        }
    }
}

/// Authentication plus coarse dataset scoping; a key that passes streams
/// unrestricted. Revocation, expiry and portal scope arrive already decided —
/// they are the control plane's answer to the exchange (REQ-53).
pub async fn evaluate<F: Fn() -> Option<String>>(
    cache: &Arc<GrantCache>,
    credential: Option<&Credential>,
    dataset: &LazyDataset<F>,
    now_secs: u64,
) -> Verdict {
    let Some(credential) = credential else {
        return Verdict::of(Decision::Reject(MISSING_CREDENTIAL));
    };

    let grant = match cache.resolve(credential, now_secs).await {
        Resolved::Grant(grant) => grant,
        Resolved::Denied(reason) => {
            return Verdict {
                decision: Decision::Reject(rejection_for(&reason)),
                denial_reason: Some(reason),
            }
        }
        Resolved::Saturated => return Verdict::of(Decision::Reject(EXCHANGE_SATURATED)),
        Resolved::Unavailable => return Verdict::of(Decision::Reject(EXCHANGE_FAILED)),
    };

    Verdict::of(evaluate_scope(&grant, dataset))
}

/// The one rung the grant does not settle: a grant is per credential and outlives
/// the request, so which dataset this particular request asked for has to be
/// matched here.
fn evaluate_scope<F: Fn() -> Option<String>>(
    grant: &CachedGrant,
    dataset: &LazyDataset<F>,
) -> Decision {
    // A null list means "any"; an explicit list is matched exactly.
    let Some(datasets) = grant.datasets.as_deref() else {
        return Decision::Admit;
    };
    // The first and only rung that needs the request's dataset named.
    let requested = dataset.resolve();
    let allowed = requested
        .as_deref()
        .is_some_and(|dataset| datasets.iter().any(|allowed| allowed == dataset));
    if allowed {
        Decision::Admit
    } else {
        Decision::Reject(DATASET_NOT_ALLOWED)
    }
}

/// A reason this build does not know is still a refusal. Mapping it to the
/// coarsest code rather than failing the exchange keeps the fail-closed
/// direction: a newer control plane that denies for a new reason must not have
/// its denial read as a dependency failure and retried.
fn rejection_for(reason: &str) -> Rejection {
    match reason {
        denial::UNKNOWN_KEY => UNKNOWN_KEY,
        denial::INVALID_SECRET => INVALID_SECRET,
        denial::REVOKED => REVOKED,
        denial::EXPIRED => EXPIRED,
        denial::PORTAL_NOT_ALLOWED => PORTAL_NOT_ALLOWED,
        _ => UNRECOGNIZED_DENIAL,
    }
}

impl Rejection {
    /// The envelope is what keeps the routed middleware from rewriting this to
    /// 400 `malformed_request` (GAP-29).
    pub fn into_response(self) -> Response {
        let mut response = coded_response(self.code, self.code.default_message());
        // Only the two exchange outcomes reach this; no auth verdict is retryable.
        if self.code.requires_hint() {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, RETRY_AFTER_FLOOR.into());
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commercial::test_support::{cache_for, credential, MockControlPlane, KEY_ID};

    const NOW: u64 = 1_800_000_000;

    /// Tests hand the ladder a dataset name directly; only the request path has
    /// a resolution to defer.
    fn named(dataset: Option<&str>) -> LazyDataset<impl Fn() -> Option<String> + '_> {
        LazyDataset::new(move || dataset.map(str::to_owned))
    }

    fn reason(verdict: &Verdict) -> &'static str {
        match verdict.decision {
            Decision::Admit => "admit",
            Decision::Reject(rejection) => rejection.reason,
        }
    }

    /// What the client is told, as opposed to what the log records.
    fn code(verdict: &Verdict) -> &'static str {
        match verdict.decision {
            Decision::Admit => "admit",
            Decision::Reject(rejection) => rejection.code.as_str(),
        }
    }

    async fn decide(cp: &MockControlPlane, dataset: Option<&str>) -> Verdict {
        let cache = cache_for(cp).await;
        evaluate(&cache, Some(&credential()), &named(dataset), NOW).await
    }

    #[tokio::test]
    async fn rung_1_no_credential_is_refused_without_an_exchange() {
        let cp = MockControlPlane::spawn().await;
        let cache = cache_for(&cp).await;

        let verdict = evaluate(&cache, None, &named(Some("ethereum-mainnet")), NOW).await;

        assert_eq!(reason(&verdict), "missing_credential");
        assert_eq!(cp.exchanges(), 0);
    }

    #[tokio::test]
    async fn the_control_planes_denials_map_onto_their_wire_codes() {
        for (denied, expected_reason, expected_code) in [
            ("unknown_key", "unknown_key", "invalid_credential"),
            ("invalid_secret", "invalid_secret", "invalid_credential"),
            ("revoked", "revoked", "revoked_credential"),
            ("expired", "expired", "expired_credential"),
            (
                "portal_not_allowed",
                "portal_not_allowed",
                "portal_not_allowed",
            ),
        ] {
            let cp = MockControlPlane::spawn().await;
            cp.deny(KEY_ID, denied);

            let verdict = decide(&cp, Some("ethereum-mainnet")).await;

            assert_eq!(reason(&verdict), expected_reason);
            assert_eq!(code(&verdict), expected_code);
            assert_eq!(verdict.denial_reason.as_deref(), Some(denied));
        }
    }

    /// A newer control plane denying for a reason this build predates must not
    /// have its refusal read as a dependency failure and retried.
    #[tokio::test]
    async fn an_unrecognized_denial_is_still_a_refusal() {
        let cp = MockControlPlane::spawn().await;
        cp.deny(KEY_ID, "quota_exhausted_for_the_billing_period");

        let verdict = decide(&cp, Some("ethereum-mainnet")).await;

        assert_eq!(reason(&verdict), "denied_unrecognized");
        assert_eq!(code(&verdict), "invalid_credential");
        assert_eq!(
            verdict.denial_reason.as_deref(),
            Some("quota_exhausted_for_the_billing_period"),
            "the operator needs the reason the client must not get"
        );
    }

    /// The wire coarsening INV-39 rests on: distinct internal rungs, one code,
    /// and no way to tell an unknown id from a known one with a wrong secret.
    #[tokio::test]
    async fn every_unauthenticated_rung_answers_the_same_code() {
        let mut verdicts = Vec::new();
        for denied in ["unknown_key", "invalid_secret", "something_new"] {
            let cp = MockControlPlane::spawn().await;
            cp.deny(KEY_ID, denied);
            verdicts.push(decide(&cp, None).await);
        }

        assert_eq!(
            verdicts.iter().map(reason).collect::<Vec<_>>(),
            ["unknown_key", "invalid_secret", "denied_unrecognized"],
            "the operator keeps the distinction on the protected axis"
        );
        for verdict in &verdicts {
            assert_eq!(
                code(verdict),
                "invalid_credential",
                "{}: the wire must not distinguish these",
                reason(verdict)
            );
        }
    }

    /// REQ-54: an exchange the portal could not make is not a verdict on the
    /// credential, and the client is told it is worth retrying.
    #[tokio::test]
    async fn an_unresolvable_credential_is_retryable_rather_than_bad() {
        let cp = MockControlPlane::spawn().await;
        cp.stop();

        let verdict = decide(&cp, None).await;

        assert_eq!(reason(&verdict), "exchange_failed");
        assert_eq!(code(&verdict), "upstream_unavailable");
        let Decision::Reject(rejection) = verdict.decision else {
            panic!("an unreachable control plane refuses");
        };
        assert!(
            rejection.code.error_type().retryable(),
            "the client must be told this one is worth retrying"
        );
    }

    /// The other half of REQ-54: the budget running out is congestion, and owes
    /// the client a back-off interval rather than a verdict.
    #[tokio::test]
    async fn a_spent_exchange_budget_is_overload() {
        let cp = MockControlPlane::spawn().await;
        let cache = cache_for(&cp).await;
        cache.exhaust_budget_for_test();

        let verdict = evaluate(&cache, Some(&credential()), &named(None), NOW).await;

        assert_eq!(reason(&verdict), "exchange_saturated");
        assert_eq!(code(&verdict), "overloaded");

        let Decision::Reject(rejection) = verdict.decision else {
            panic!("a spent budget refuses");
        };
        let response = rejection.into_response();
        assert_eq!(response.status(), crate::types::server_overloaded());
        assert!(
            response.headers().contains_key(header::RETRY_AFTER),
            "INV-26: an overload owes a back-off interval"
        );
    }

    #[tokio::test]
    async fn dataset_membership_is_exact() {
        async fn scope(datasets: Option<Vec<&str>>, requested: Option<&str>) -> &'static str {
            let cp = MockControlPlane::spawn().await;
            cp.grant(
                KEY_ID,
                datasets.map(|list| list.into_iter().map(str::to_owned).collect()),
                NOW + 300,
                NOW + 900,
            );
            reason(&decide(&cp, requested).await)
        }

        assert_eq!(
            scope(None, Some("ethereum-mainnet")).await,
            "admit",
            "a null list means every dataset"
        );
        assert_eq!(
            scope(Some(vec!["ethereum-mainnet"]), Some("ethereum-mainnet")).await,
            "admit"
        );
        assert_eq!(
            scope(Some(vec!["ethereum-mainnet"]), Some("base-mainnet")).await,
            "dataset_not_allowed"
        );
        assert_eq!(
            scope(Some(vec!["ethereum-mainnet"]), Some("ethereum")).await,
            "dataset_not_allowed",
            "matching is exact, not by prefix or alias"
        );
        assert_eq!(
            scope(Some(vec!["*"]), Some("ethereum-mainnet")).await,
            "dataset_not_allowed",
            "`*` is a dataset name like any other: null already means all"
        );
        assert_eq!(
            scope(Some(Vec::new()), Some("ethereum-mainnet")).await,
            "dataset_not_allowed",
            "an empty list means no dataset"
        );
        assert_eq!(
            scope(Some(vec!["ethereum-mainnet"]), None).await,
            "dataset_not_allowed",
            "a dataset-scoped key cannot use an endpoint with no dataset"
        );
    }

    #[tokio::test]
    async fn an_unscoped_grant_is_admitted() {
        let cp = MockControlPlane::spawn().await;
        cp.grant(KEY_ID, None, NOW + 300, NOW + 900);

        let verdict = decide(&cp, Some("ethereum-mainnet")).await;

        assert_eq!(verdict.decision, Decision::Admit);
    }

    /// A grant past its hard expiry admits nothing, and says so as a dependency
    /// failure rather than as a claim about the key.
    #[tokio::test]
    async fn an_expired_grant_admits_nothing_even_with_the_control_plane_down() {
        let cp = MockControlPlane::spawn().await;
        cp.grant(KEY_ID, None, NOW + 300, NOW + 900);
        let cache = cache_for(&cp).await;
        evaluate(&cache, Some(&credential()), &named(None), NOW).await;

        cp.stop();
        let verdict = evaluate(&cache, Some(&credential()), &named(None), NOW + 901).await;

        assert_eq!(reason(&verdict), "exchange_failed");
    }
}
