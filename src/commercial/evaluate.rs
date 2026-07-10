use subtle::ConstantTimeEq;

use super::{
    config::default_throttle_residual_secs,
    types::{
        Authorization, AuthorizeRequest, Credential, Defaults, Granted, GrantedLimits, KeySnapshot,
        KeyStatus, OnExceed, Principal, Rejected, SnapshotLimits,
    },
    ConcurrencyLimiter, SnapshotStore, TallyStore,
};

const INVALID_KEY: &str = "invalid_key";
const EXPIRED: &str = "expired";
const SUSPENDED: &str = "suspended";
const QUOTA_EXHAUSTED: &str = "quota_exhausted";
const DATASET_NOT_ENTITLED: &str = "dataset_not_entitled";
const TRACES_NOT_ENTITLED: &str = "traces_not_entitled";
const CONCURRENCY_LIMIT: &str = "concurrency_limit";
const ANON_LIMITED: &str = "anon_limited";

#[derive(Debug, Clone, Copy)]
pub struct EvaluationState {
    pub now_secs: u64,
    pub tally_bytes: u64,
    pub concurrency_available: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct EvaluationPolicy {
    pub throttle_residual_secs: u64,
}

impl Default for EvaluationState {
    fn default() -> Self {
        Self {
            now_secs: now_secs(),
            tally_bytes: 0,
            concurrency_available: true,
        }
    }
}

impl Default for EvaluationPolicy {
    fn default() -> Self {
        Self {
            throttle_residual_secs: default_throttle_residual_secs(),
        }
    }
}

pub async fn evaluate_with_store(
    store: &SnapshotStore,
    tally: Option<&TallyStore>,
    concurrency: Option<&ConcurrencyLimiter>,
    req: AuthorizeRequest,
) -> Authorization {
    evaluate_with_store_and_policy(store, tally, concurrency, req, EvaluationPolicy::default())
        .await
}

pub async fn evaluate_with_store_and_policy(
    store: &SnapshotStore,
    tally: Option<&TallyStore>,
    concurrency: Option<&ConcurrencyLimiter>,
    req: AuthorizeRequest,
    policy: EvaluationPolicy,
) -> Authorization {
    let view = store.view();
    match &req.credential {
        Credential::Key { key_id, .. } => {
            let snapshot = if let Some(snapshot) = view.records.get(key_id).cloned() {
                Some(snapshot)
            } else {
                store.get_or_resolve(key_id).await
            };
            let mut state = EvaluationState::default();
            if let (Some(tally), Some(snapshot)) = (tally, snapshot.as_deref()) {
                if let (Some(account_id), Some(quota)) =
                    (snapshot.account_id.as_deref(), snapshot.quota.as_ref())
                {
                    state.tally_bytes = tally.bytes_for(account_id, quota.version);
                }
            }
            if let Credential::Key {
                key_id: _,
                secret_sha256,
            } = &req.credential
            {
                if let Err(rejected) = evaluate_key_prechecks(
                    &req,
                    snapshot.as_deref(),
                    &view.defaults,
                    state,
                    secret_sha256,
                ) {
                    return Authorization::Rejected(rejected);
                }
            }
            let (permit, concurrency_available) =
                acquire_key_permit(snapshot.as_deref(), concurrency);
            state.concurrency_available = concurrency_available;
            match evaluate_with_policy(&req, snapshot.as_deref(), &view.defaults, state, policy) {
                Authorization::Granted(mut granted) => {
                    granted.concurrency_permit = permit;
                    Authorization::Granted(granted)
                }
                rejected => rejected,
            }
        }
        Credential::None { ip_bucket } => evaluate_anonymous_with_state(
            &view.defaults,
            policy,
            tally,
            concurrency,
            ip_bucket,
            EvaluationState::default().now_secs,
        ),
        Credential::Internal { .. } => Authorization::Granted(super::client::oss_grant()),
    }
}

pub fn evaluate(
    req: &AuthorizeRequest,
    snapshot: Option<&KeySnapshot>,
    defaults: &Defaults,
    state: EvaluationState,
) -> Authorization {
    evaluate_with_policy(req, snapshot, defaults, state, EvaluationPolicy::default())
}

pub fn evaluate_with_policy(
    req: &AuthorizeRequest,
    snapshot: Option<&KeySnapshot>,
    defaults: &Defaults,
    state: EvaluationState,
    policy: EvaluationPolicy,
) -> Authorization {
    match &req.credential {
        Credential::Key {
            key_id,
            secret_sha256,
        } => evaluate_key(
            req,
            snapshot,
            defaults,
            state,
            policy,
            key_id,
            secret_sha256,
        ),
        Credential::None { .. } => evaluate_anonymous(defaults),
        Credential::Internal { .. } => Authorization::Granted(super::client::oss_grant()),
    }
}

fn evaluate_key(
    req: &AuthorizeRequest,
    snapshot: Option<&KeySnapshot>,
    defaults: &Defaults,
    state: EvaluationState,
    policy: EvaluationPolicy,
    key_id: &str,
    secret_sha256: &str,
) -> Authorization {
    let snapshot = match evaluate_key_prechecks(req, snapshot, defaults, state, secret_sha256) {
        Ok(snapshot) => snapshot,
        Err(rejected) => return Authorization::Rejected(rejected),
    };
    if !state.concurrency_available {
        return reject(defaults, CONCURRENCY_LIMIT, 429, Some(1));
    }

    let quota = snapshot.quota.as_ref();
    let on_exceed = quota
        .map(|quota| quota.on_exceed.clone())
        .unwrap_or(OnExceed::Reject);
    let effective = quota
        .and_then(|quota| quota.remaining_bytes)
        .map(|remaining_bytes| remaining_bytes - state.tally_bytes as i64);
    if effective.is_some_and(|effective| effective <= 0) {
        return match on_exceed {
            OnExceed::Reject => reject(
                defaults,
                QUOTA_EXHAUSTED,
                402,
                quota
                    .and_then(|quota| quota.period_end)
                    .map(|period_end| period_end.saturating_sub(state.now_secs)),
            ),
            OnExceed::Throttle {
                floor_bytes_per_sec,
            } => Authorization::Granted(grant(
                snapshot,
                key_id,
                GrantedLimits {
                    throughput_bytes_per_sec: Some(floor_bytes_per_sec),
                    burst_bytes: Some(floor_bytes_per_sec),
                    max_response_bytes: Some(
                        floor_bytes_per_sec.saturating_mul(policy.throttle_residual_secs),
                    ),
                    max_chunks: snapshot
                        .limits
                        .as_ref()
                        .and_then(|limits| limits.max_chunks),
                },
                on_exceed,
            )),
        };
    }

    let mut limits = granted_limits(snapshot.limits.as_ref());
    if let Some(effective) = effective.and_then(|effective| u64::try_from(effective).ok()) {
        limits.max_response_bytes = min_optional(limits.max_response_bytes, Some(effective));
    }
    Authorization::Granted(grant(snapshot, key_id, limits, on_exceed))
}

fn evaluate_key_prechecks<'a>(
    req: &AuthorizeRequest,
    snapshot: Option<&'a KeySnapshot>,
    defaults: &Defaults,
    state: EvaluationState,
    secret_sha256: &str,
) -> Result<&'a KeySnapshot, Rejected> {
    let Some(snapshot) = snapshot else {
        return Err(rejected(defaults, INVALID_KEY, 401, None));
    };
    if !hash_matches(
        secret_sha256,
        snapshot.secret_sha256.as_deref().unwrap_or_default(),
    ) {
        return Err(rejected(defaults, INVALID_KEY, 401, None));
    }
    match snapshot.status {
        KeyStatus::Revoked => return Err(rejected(defaults, INVALID_KEY, 401, None)),
        KeyStatus::Active | KeyStatus::Suspended => {}
    }
    if snapshot
        .expires_at
        .is_some_and(|expires_at| expires_at < state.now_secs)
    {
        return Err(rejected(defaults, EXPIRED, 401, None));
    }
    if snapshot.status == KeyStatus::Suspended {
        return Err(rejected(defaults, SUSPENDED, 402, None));
    }

    let entitlements = snapshot.entitlements.as_ref();
    if !entitlements.is_some_and(|entitlements| {
        entitlements
            .chains
            .iter()
            .any(|chain| chain == &req.dataset)
    }) {
        return Err(rejected(defaults, DATASET_NOT_ENTITLED, 403, None));
    }
    if req.query.chain_kind.as_deref() == Some("evm")
        && (req.query.requires_traces || req.query.requires_statediffs)
        && !entitlements.is_some_and(|entitlements| {
            entitlements
                .traces
                .iter()
                .any(|chain| chain == &req.dataset)
        })
    {
        return Err(rejected(defaults, TRACES_NOT_ENTITLED, 403, None));
    }

    Ok(snapshot)
}

fn acquire_key_permit(
    snapshot: Option<&KeySnapshot>,
    concurrency: Option<&ConcurrencyLimiter>,
) -> (Option<super::concurrency::ConcurrencyPermit>, bool) {
    let Some((account_id, limit)) = snapshot.and_then(|snapshot| {
        Some((
            snapshot.account_id.as_deref()?,
            snapshot.limits.as_ref()?.concurrency?,
        ))
    }) else {
        return (None, true);
    };
    let permit = concurrency.and_then(|limiter| limiter.try_acquire(account_id, limit));
    let available = permit.is_some();
    (permit, available)
}

fn evaluate_anonymous(defaults: &Defaults) -> Authorization {
    Authorization::Granted(Granted {
        principal: Principal {
            account_id: "anonymous".to_string(),
            api_key_id: None,
        },
        tally_account_id: None,
        entitled_chains: None,
        limits: GrantedLimits {
            max_response_bytes: defaults.public.limits.max_response_bytes,
            throughput_bytes_per_sec: defaults.public.limits.throughput_bytes_per_sec,
            burst_bytes: defaults.public.limits.burst_bytes,
            max_chunks: None,
        },
        on_exceed: defaults.public.quota.on_exceed.clone(),
        quota_version: defaults.seq,
        quota_remaining_bytes: None,
        concurrency_permit: None,
    })
}

fn evaluate_anonymous_with_state(
    defaults: &Defaults,
    policy: EvaluationPolicy,
    tally: Option<&TallyStore>,
    concurrency: Option<&ConcurrencyLimiter>,
    ip_bucket: &str,
    now_secs: u64,
) -> Authorization {
    let account_key = format!("anon:{ip_bucket}");
    let window_secs = defaults.public.quota.window_secs.max(1);
    let window_start = now_secs - (now_secs % window_secs);

    let permit = match (defaults.public.limits.concurrency, concurrency) {
        (Some(limit), Some(limiter)) => limiter.try_acquire(&account_key, limit),
        _ => None,
    };
    if concurrency.is_some() && defaults.public.limits.concurrency.is_some() && permit.is_none() {
        return reject(defaults, CONCURRENCY_LIMIT, 429, Some(1));
    }

    let served = tally
        .map(|tally| tally.bytes_for(&account_key, window_start))
        .unwrap_or(0);
    let volume_bytes = defaults.public.quota.volume_bytes;
    let effective = volume_bytes.map(|volume_bytes| volume_bytes as i64 - served as i64);
    if effective.is_some_and(|effective| effective <= 0) {
        return match defaults.public.quota.on_exceed.clone() {
            OnExceed::Reject => reject(
                defaults,
                ANON_LIMITED,
                429,
                Some(window_start + window_secs - now_secs),
            ),
            OnExceed::Throttle {
                floor_bytes_per_sec,
            } => Authorization::Granted(anonymous_grant(
                defaults,
                account_key,
                window_start,
                GrantedLimits {
                    throughput_bytes_per_sec: Some(floor_bytes_per_sec),
                    burst_bytes: Some(floor_bytes_per_sec),
                    max_response_bytes: Some(
                        floor_bytes_per_sec.saturating_mul(policy.throttle_residual_secs),
                    ),
                    max_chunks: None,
                },
                volume_bytes.map(|volume_bytes| volume_bytes as i64),
                permit,
            )),
        };
    }

    let mut limits = GrantedLimits {
        max_response_bytes: defaults.public.limits.max_response_bytes,
        throughput_bytes_per_sec: defaults.public.limits.throughput_bytes_per_sec,
        burst_bytes: defaults.public.limits.burst_bytes,
        max_chunks: None,
    };
    if let Some(effective) = effective.and_then(|effective| u64::try_from(effective).ok()) {
        limits.max_response_bytes = min_optional(limits.max_response_bytes, Some(effective));
    }

    Authorization::Granted(anonymous_grant(
        defaults,
        account_key,
        window_start,
        limits,
        volume_bytes.map(|volume_bytes| volume_bytes as i64),
        permit,
    ))
}

fn anonymous_grant(
    defaults: &Defaults,
    account_key: String,
    quota_version: u64,
    limits: GrantedLimits,
    quota_remaining_bytes: Option<i64>,
    concurrency_permit: Option<super::concurrency::ConcurrencyPermit>,
) -> Granted {
    Granted {
        principal: Principal {
            account_id: "anonymous".to_string(),
            api_key_id: None,
        },
        tally_account_id: Some(account_key),
        entitled_chains: None,
        limits,
        on_exceed: defaults.public.quota.on_exceed.clone(),
        quota_version,
        quota_remaining_bytes,
        concurrency_permit,
    }
}

fn granted_limits(limits: Option<&SnapshotLimits>) -> GrantedLimits {
    let Some(limits) = limits else {
        return GrantedLimits::default();
    };
    GrantedLimits {
        max_response_bytes: limits.max_response_bytes,
        throughput_bytes_per_sec: limits.throughput_bytes_per_sec,
        burst_bytes: limits.burst_bytes,
        max_chunks: limits.max_chunks,
    }
}

fn grant(
    snapshot: &KeySnapshot,
    key_id: &str,
    limits: GrantedLimits,
    on_exceed: OnExceed,
) -> Granted {
    Granted {
        principal: Principal {
            account_id: snapshot.account_id.clone().unwrap_or_default(),
            api_key_id: Some(key_id.to_string()),
        },
        tally_account_id: None,
        entitled_chains: snapshot.entitlements.as_ref().map(|entitlements| {
            entitlements
                .chains
                .iter()
                .cloned()
                .collect::<std::collections::HashSet<_>>()
        }),
        limits,
        on_exceed,
        quota_version: snapshot
            .quota
            .as_ref()
            .map(|quota| quota.version)
            .unwrap_or(0),
        quota_remaining_bytes: snapshot
            .quota
            .as_ref()
            .and_then(|quota| quota.remaining_bytes),
        concurrency_permit: None,
    }
}

fn reject(
    defaults: &Defaults,
    reason: &str,
    http_status: u16,
    retry_after_secs: Option<u64>,
) -> Authorization {
    Authorization::Rejected(rejected(defaults, reason, http_status, retry_after_secs))
}

fn rejected(
    defaults: &Defaults,
    reason: &str,
    http_status: u16,
    retry_after_secs: Option<u64>,
) -> Rejected {
    Rejected {
        reason: reason.to_string(),
        http_status,
        message: defaults
            .messages
            .get(reason)
            .cloned()
            .unwrap_or_else(|| fallback_message(reason)),
        retry_after_secs,
    }
}

fn hash_matches(presented: &str, expected: &str) -> bool {
    let Ok(presented) = hex::decode(presented) else {
        return false;
    };
    let Ok(expected) = hex::decode(expected) else {
        return false;
    };
    if presented.len() != expected.len() {
        return false;
    }
    presented.ct_eq(&expected).into()
}

fn min_optional(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn fallback_message(reason: &str) -> String {
    match reason {
        INVALID_KEY => "Invalid API key".to_string(),
        EXPIRED => "API key expired".to_string(),
        SUSPENDED => "Portal access is suspended".to_string(),
        QUOTA_EXHAUSTED => "Portal volume cap reached".to_string(),
        DATASET_NOT_ENTITLED => "Dataset is not enabled for this key".to_string(),
        TRACES_NOT_ENTITLED => "Traces are not enabled for this key".to_string(),
        CONCURRENCY_LIMIT => "Too many concurrent Portal streams".to_string(),
        ANON_LIMITED => "Anonymous Portal limit reached".to_string(),
        _ => "Portal request rejected".to_string(),
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, time::Duration};

    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::commercial::{
        config::{CommercialConfig, PublicFallbackConfig},
        store::test_support::{
            active_snapshot, defaults_record, page, MockControlPlane, KEY_ID, SECRET_SHA256,
            SERVICE_TOKEN,
        },
        ConcurrencyLimiter, Credential, Endpoint, QueryDescriptor, SnapshotRecord, SnapshotStore,
        TallyStore,
    };

    const WRONG_SHA256: &str = "aac742262c66b43621baf5b5439b30aa0de646e131c665f1d2118372cdb9b0ed";

    fn defaults() -> Defaults {
        Defaults::from(defaults_record(1))
    }

    fn request(secret_sha256: &str) -> AuthorizeRequest {
        AuthorizeRequest {
            credential: Credential::Key {
                key_id: KEY_ID.to_string(),
                secret_sha256: secret_sha256.to_string(),
            },
            dataset: "ethereum-mainnet".to_string(),
            endpoint: Endpoint::Stream,
            query: QueryDescriptor {
                requires_traces: false,
                requires_statediffs: false,
                first_block: Some(1),
                chain_kind: Some("evm".to_string()),
            },
        }
    }

    fn state() -> EvaluationState {
        EvaluationState {
            now_secs: 1_700_000_000,
            tally_bytes: 0,
            concurrency_available: true,
        }
    }

    fn store_config(mock: &MockControlPlane, cache_path: PathBuf) -> CommercialConfig {
        CommercialConfig {
            control_plane_url: mock.url(),
            service_token_env: "PORTAL_EVALUATE_TEST_TOKEN".to_string(),
            sync_interval_secs: 1,
            flush_interval_secs: 5,
            flush_max_events: 500,
            usage_buffer_max_events: 1000,
            snapshot_cache_path: cache_path,
            resolve_rate_per_sec: 10,
            negative_cache_secs: 60,
            pod_count: 1,
            client_ip_header: "x-forwarded-for".to_string(),
            throttle_residual_secs: 60,
            sweep_horizon_window_multiplier: 2,
            public_fallback: PublicFallbackConfig {
                throughput_bytes_per_sec: 1,
                burst_bytes: 1,
                max_response_bytes: 1,
                volume_bytes: 1,
                window_secs: 1,
                concurrency: 1,
            },
        }
    }

    fn cache_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "sqd-portal-evaluate-{name}-{}.json",
            uuid::Uuid::new_v4()
        ))
    }

    fn rejected_reason(result: Authorization) -> String {
        match result {
            Authorization::Rejected(rejected) => rejected.reason,
            Authorization::Granted(_) => panic!("expected rejection"),
        }
    }

    #[test]
    fn rejects_missing_and_mismatched_key() {
        assert_eq!(
            rejected_reason(evaluate(
                &request(SECRET_SHA256),
                None,
                &defaults(),
                state()
            )),
            "invalid_key"
        );
        assert_eq!(
            rejected_reason(evaluate(
                &request(WRONG_SHA256),
                Some(&active_snapshot(1)),
                &defaults(),
                state()
            )),
            "invalid_key"
        );
    }

    #[test]
    fn suspended_beats_entitlement() {
        let mut snapshot = active_snapshot(1);
        snapshot.status = KeyStatus::Suspended;
        snapshot.entitlements.as_mut().unwrap().chains.clear();

        assert_eq!(
            rejected_reason(evaluate(
                &request(SECRET_SHA256),
                Some(&snapshot),
                &defaults(),
                state()
            )),
            "suspended"
        );
    }

    #[test]
    fn expired_beats_suspended() {
        let mut snapshot = active_snapshot(1);
        snapshot.status = KeyStatus::Suspended;
        snapshot.expires_at = Some(1);

        assert_eq!(
            rejected_reason(evaluate(
                &request(SECRET_SHA256),
                Some(&snapshot),
                &defaults(),
                state()
            )),
            "expired"
        );
    }

    #[test]
    fn rejects_revoked_expired_dataset_and_traces() {
        let mut revoked = active_snapshot(1);
        revoked.status = KeyStatus::Revoked;
        assert_eq!(
            rejected_reason(evaluate(
                &request(SECRET_SHA256),
                Some(&revoked),
                &defaults(),
                state()
            )),
            "invalid_key"
        );

        let mut expired = active_snapshot(1);
        expired.expires_at = Some(1);
        assert_eq!(
            rejected_reason(evaluate(
                &request(SECRET_SHA256),
                Some(&expired),
                &defaults(),
                state()
            )),
            "expired"
        );

        let mut no_dataset = active_snapshot(1);
        no_dataset.entitlements.as_mut().unwrap().chains.clear();
        assert_eq!(
            rejected_reason(evaluate(
                &request(SECRET_SHA256),
                Some(&no_dataset),
                &defaults(),
                state()
            )),
            "dataset_not_entitled"
        );

        let mut trace_request = request(SECRET_SHA256);
        trace_request.query.requires_traces = true;
        assert_eq!(
            rejected_reason(evaluate(
                &trace_request,
                Some(&active_snapshot(1)),
                &defaults(),
                state()
            )),
            "traces_not_entitled"
        );
        trace_request.query.chain_kind = Some("solana".to_string());
        assert!(matches!(
            evaluate(
                &trace_request,
                Some(&active_snapshot(1)),
                &defaults(),
                state()
            ),
            Authorization::Granted(_)
        ));
    }

    #[test]
    fn rejects_concurrency_and_quota_or_throttles() {
        let mut unavailable = state();
        unavailable.concurrency_available = false;
        assert_eq!(
            rejected_reason(evaluate(
                &request(SECRET_SHA256),
                Some(&active_snapshot(1)),
                &defaults(),
                unavailable
            )),
            "concurrency_limit"
        );

        let mut quota_state = state();
        quota_state.tally_bytes = 10_000;
        let rejected = evaluate(
            &request(SECRET_SHA256),
            Some(&active_snapshot(1)),
            &defaults(),
            quota_state,
        );
        match rejected {
            Authorization::Rejected(rejected) => {
                assert_eq!(rejected.reason, "quota_exhausted");
                assert_eq!(rejected.http_status, 402);
                assert!(rejected.retry_after_secs.is_some());
            }
            Authorization::Granted(_) => panic!("expected rejection"),
        }

        let mut throttled = active_snapshot(1);
        throttled.quota.as_mut().unwrap().on_exceed = OnExceed::Throttle {
            floor_bytes_per_sec: 7,
        };
        match evaluate(
            &request(SECRET_SHA256),
            Some(&throttled),
            &defaults(),
            quota_state,
        ) {
            Authorization::Granted(grant) => {
                assert_eq!(grant.limits.throughput_bytes_per_sec, Some(7));
                assert_eq!(grant.limits.max_response_bytes, Some(420));
            }
            Authorization::Rejected(_) => panic!("expected throttle grant"),
        }
    }

    #[test]
    fn throttle_residual_uses_policy_seconds() {
        let mut quota_state = state();
        quota_state.tally_bytes = 10_000;
        let mut throttled = active_snapshot(1);
        throttled.quota.as_mut().unwrap().on_exceed = OnExceed::Throttle {
            floor_bytes_per_sec: 7,
        };

        match evaluate_with_policy(
            &request(SECRET_SHA256),
            Some(&throttled),
            &defaults(),
            quota_state,
            EvaluationPolicy {
                throttle_residual_secs: 10,
            },
        ) {
            Authorization::Granted(grant) => {
                assert_eq!(grant.limits.max_response_bytes, Some(70));
            }
            Authorization::Rejected(_) => panic!("expected throttle grant"),
        }
    }

    #[test]
    fn grant_caps_max_response_by_effective_remaining_and_anonymous_uses_defaults() {
        let mut quota_state = state();
        quota_state.tally_bytes = 9_500;
        match evaluate(
            &request(SECRET_SHA256),
            Some(&active_snapshot(1)),
            &defaults(),
            quota_state,
        ) {
            Authorization::Granted(grant) => {
                assert_eq!(grant.principal.api_key_id.as_deref(), Some(KEY_ID));
                assert_eq!(grant.limits.max_response_bytes, Some(500));
                assert_eq!(grant.quota_version, 1);
                let entitled = grant
                    .entitled_chains
                    .as_ref()
                    .expect("keyed grant should carry chain entitlements");
                assert_eq!(entitled.len(), 1);
                assert!(entitled.contains("ethereum-mainnet"));
            }
            Authorization::Rejected(_) => panic!("expected grant"),
        }

        let anon = AuthorizeRequest {
            credential: Credential::None {
                ip_bucket: "local".to_string(),
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
        match evaluate(&anon, None, &defaults(), state()) {
            Authorization::Granted(grant) => {
                assert_eq!(grant.principal.account_id, "anonymous");
                assert_eq!(grant.limits.max_response_bytes, Some(300));
                assert_eq!(grant.entitled_chains, None);
            }
            Authorization::Rejected(_) => panic!("expected anonymous grant"),
        }
    }

    #[test]
    fn null_quota_remaining_and_public_volume_are_unlimited() {
        let mut snapshot = active_snapshot(1);
        snapshot.quota.as_mut().unwrap().remaining_bytes = None;
        let mut quota_state = state();
        quota_state.tally_bytes = u64::MAX;

        match evaluate(
            &request(SECRET_SHA256),
            Some(&snapshot),
            &defaults(),
            quota_state,
        ) {
            Authorization::Granted(grant) => {
                assert_eq!(grant.quota_remaining_bytes, None);
                assert_eq!(grant.limits.max_response_bytes, Some(3_000_000));
            }
            Authorization::Rejected(rejected) => {
                panic!("expected unlimited quota grant, got {rejected:?}")
            }
        }

        let mut defaults = defaults();
        defaults.public.quota.volume_bytes = None;
        let tally = TallyStore::default();
        tally.debit("anon:local", 120, u64::MAX);
        match evaluate_anonymous_with_state(
            &defaults,
            EvaluationPolicy::default(),
            Some(&tally),
            None,
            "local",
            120,
        ) {
            Authorization::Granted(grant) => {
                assert_eq!(grant.quota_remaining_bytes, None);
                assert_eq!(grant.limits.max_response_bytes, Some(300));
            }
            Authorization::Rejected(rejected) => {
                panic!("expected unlimited public quota grant, got {rejected:?}")
            }
        }
    }

    #[test]
    fn keyed_sql_grant_carries_snapshot_entitled_chains() {
        let mut snapshot = active_snapshot(1);
        snapshot.entitlements.as_mut().unwrap().chains = vec![
            "sql".to_string(),
            "solana-mainnet".to_string(),
            "ethereum-mainnet".to_string(),
        ];
        let mut req = request(SECRET_SHA256);
        req.dataset = "sql".to_string();
        req.endpoint = Endpoint::SqlQuery;

        match evaluate(&req, Some(&snapshot), &defaults(), state()) {
            Authorization::Granted(grant) => {
                let entitled = grant
                    .entitled_chains
                    .expect("keyed grants should carry the snapshot chain set");
                assert_eq!(grant.principal.api_key_id.as_deref(), Some(KEY_ID));
                assert!(entitled.contains("sql"));
                assert!(entitled.contains("solana-mainnet"));
                assert!(entitled.contains("ethereum-mainnet"));
            }
            Authorization::Rejected(rejected) => panic!("expected grant, got {rejected:?}"),
        }
    }

    #[test]
    fn anonymous_quota_is_per_bucket_and_keeps_public_principal() {
        let defaults = defaults();
        let tally = TallyStore::default();
        tally.debit(
            "anon:local",
            120,
            defaults.public.quota.volume_bytes.unwrap(),
        );

        match evaluate_anonymous_with_state(
            &defaults,
            EvaluationPolicy::default(),
            Some(&tally),
            None,
            "local",
            120,
        ) {
            Authorization::Rejected(rejected) => {
                assert_eq!(rejected.reason, "anon_limited");
                assert_eq!(rejected.http_status, 429);
            }
            Authorization::Granted(_) => panic!("expected anon limit rejection"),
        }

        match evaluate_anonymous_with_state(
            &defaults,
            EvaluationPolicy::default(),
            Some(&tally),
            None,
            "other",
            120,
        ) {
            Authorization::Granted(grant) => {
                assert_eq!(grant.principal.account_id, "anonymous");
                assert_eq!(grant.tally_account_id.as_deref(), Some("anon:other"));
                assert_eq!(grant.quota_version, 120);
                assert_eq!(grant.limits.max_response_bytes, Some(300));
            }
            Authorization::Rejected(_) => panic!("expected distinct IP bucket to pass"),
        }
    }

    #[test]
    fn anonymous_quota_rolls_over_on_fixed_window_boundary() {
        let defaults = defaults();
        let tally = TallyStore::default();
        tally.debit(
            "anon:local",
            120,
            defaults.public.quota.volume_bytes.unwrap(),
        );

        match evaluate_anonymous_with_state(
            &defaults,
            EvaluationPolicy::default(),
            Some(&tally),
            None,
            "local",
            179,
        ) {
            Authorization::Rejected(rejected) => {
                assert_eq!(rejected.reason, "anon_limited");
                assert_eq!(rejected.retry_after_secs, Some(1));
            }
            Authorization::Granted(_) => panic!("expected current window to reject"),
        }

        match evaluate_anonymous_with_state(
            &defaults,
            EvaluationPolicy::default(),
            Some(&tally),
            None,
            "local",
            180,
        ) {
            Authorization::Granted(grant) => {
                assert_eq!(grant.tally_account_id.as_deref(), Some("anon:local"));
                assert_eq!(grant.quota_version, 180);
            }
            Authorization::Rejected(_) => panic!("expected next window to pass"),
        }
    }

    #[test]
    fn anonymous_throttle_grants_floor_when_window_exhausted() {
        let mut defaults = defaults();
        defaults.public.quota.on_exceed = OnExceed::Throttle {
            floor_bytes_per_sec: 7,
        };
        let tally = TallyStore::default();
        tally.debit(
            "anon:local",
            120,
            defaults.public.quota.volume_bytes.unwrap(),
        );

        match evaluate_anonymous_with_state(
            &defaults,
            EvaluationPolicy::default(),
            Some(&tally),
            None,
            "local",
            120,
        ) {
            Authorization::Granted(grant) => {
                assert_eq!(grant.principal.account_id, "anonymous");
                assert_eq!(grant.limits.throughput_bytes_per_sec, Some(7));
                assert_eq!(grant.limits.max_response_bytes, Some(420));
            }
            Authorization::Rejected(_) => panic!("expected throttled anon grant"),
        }
    }

    #[test]
    fn anonymous_concurrency_exhaustion_rejects() {
        let defaults = defaults();
        let limiter = ConcurrencyLimiter::new(1);
        let mut held = Vec::new();
        for _ in 0..3 {
            match evaluate_anonymous_with_state(
                &defaults,
                EvaluationPolicy::default(),
                None,
                Some(&limiter),
                "local",
                120,
            ) {
                Authorization::Granted(grant) => held.push(grant),
                Authorization::Rejected(_) => panic!("expected permit"),
            }
        }

        match evaluate_anonymous_with_state(
            &defaults,
            EvaluationPolicy::default(),
            None,
            Some(&limiter),
            "local",
            120,
        ) {
            Authorization::Rejected(rejected) => {
                assert_eq!(rejected.reason, "concurrency_limit");
                assert_eq!(rejected.http_status, 429);
            }
            Authorization::Granted(_) => panic!("expected concurrency rejection"),
        }
    }

    #[tokio::test]
    async fn bad_secret_does_not_touch_concurrency_limiter() {
        std::env::set_var("PORTAL_EVALUATE_TEST_TOKEN", SERVICE_TOKEN);
        let mock = MockControlPlane::spawn().await;
        mock.push_page(
            0,
            page(vec![SnapshotRecord::Key(active_snapshot(1))], 1, false),
        );
        let path = cache_path("bad-secret");
        let (store, task) =
            SnapshotStore::spawn(&store_config(&mock, path.clone()), CancellationToken::new())
                .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !store.is_ready() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        let limiter = ConcurrencyLimiter::new(1);
        let result = evaluate_with_store(&store, None, Some(&limiter), request(WRONG_SHA256)).await;

        assert_eq!(rejected_reason(result), "invalid_key");
        assert_eq!(limiter.sweep_idle(Duration::ZERO), 0);

        task.abort();
        let _ = tokio::fs::remove_file(path).await;
    }
}
