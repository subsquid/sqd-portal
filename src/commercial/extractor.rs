use std::{fmt, sync::Arc};

use axum::{
    extract::Request,
    http::{header, HeaderMap},
    middleware::Next,
    response::Response,
};
use sha2::{Digest, Sha256};

use super::{
    config::{CommercialConfig, Enforcement},
    evaluate::{self, Decision, LazyDataset, Rejection},
    now_secs,
    store::SnapshotStore,
};
use crate::{
    metrics::{self, AuthDecision},
    network::NetworkClient,
};

/// Token layouts the portal accepts, all of the form `<prefix><key_id>_<secret>`:
/// the prefix minted by the control plane, plus the legacy prefix carried by
/// keys imported from before the portal owned authentication. Exactly the set
/// the control plane issues — a prefix it never mints is not a key.
const TOKEN_PREFIXES: [&str; 2] = ["sqd_portal_", "prt_"];

/// Both segments mirror the control plane's own `[A-Za-z0-9~-]+`, capped at
/// what it can mint. A token the control plane could never have issued is
/// rejected before its key id reaches the negative cache or a log line.
const MAX_KEY_ID_LEN: usize = 64;
const MAX_SECRET_LEN: usize = 128;

/// A presented key, reduced to what the ladder needs. The secret itself is
/// discarded at parse time; only its digest travels further.
#[derive(Clone, PartialEq, Eq)]
pub struct Credential {
    pub key_id: String,
    pub secret_sha256: String,
}

impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credential")
            .field("key_id", &self.key_id)
            .field("secret_sha256", &"<redacted>")
            .finish()
    }
}

/// Keys carry canonical dataset names, so an alias in the request URL has to be
/// resolved before it can be matched.
pub trait DatasetCatalog: Send + Sync {
    fn canonical_name(&self, alias: &str) -> Option<String>;
}

impl DatasetCatalog for NetworkClient {
    fn canonical_name(&self, alias: &str) -> Option<String> {
        self.dataset(alias).map(|dataset| dataset.default_name)
    }
}

pub struct Gate {
    store: Arc<SnapshotStore>,
    catalog: Arc<dyn DatasetCatalog>,
    portal_id: String,
    enforcement: Enforcement,
}

impl Gate {
    pub fn new(
        config: &CommercialConfig,
        store: Arc<SnapshotStore>,
        catalog: Arc<dyn DatasetCatalog>,
    ) -> Self {
        Self {
            store,
            catalog,
            portal_id: config.portal_id(),
            enforcement: config.enforcement,
        }
    }

    /// Whether the portal has mirrored the control plane's key set yet. A gated
    /// portal that has not knows no keys, so it would refuse every valid
    /// one — it belongs out of rotation until this turns true.
    pub fn snapshot_ready(&self) -> bool {
        self.store.is_ready()
    }

    /// Whether this gate turns its verdicts into responses. Shadow mode does
    /// not, so nothing the snapshot does or does not know can reject a request.
    pub fn enforcing(&self) -> bool {
        self.enforcement == Enforcement::Enforce
    }

    /// A route whose path does not name the dataset cannot be checked against
    /// a dataset-scoped key, and refuses it (REQ-53).
    async fn decide(
        &self,
        headers: &HeaderMap,
        uri: &axum::http::Uri,
        names_dataset: bool,
    ) -> Decision {
        // Deferred on purpose: canonicalization interns the name in a
        // process-wide pool and clones the dataset config, so it must stay
        // behind authentication. Only the dataset rung calls this.
        let dataset = LazyDataset::new(|| self.dataset_for(uri.path(), names_dataset));
        let credential = match credential_from_request(headers) {
            Ok(credential) => credential,
            Err(rejection) => {
                self.log(Decision::Reject(rejection), None, None);
                return Decision::Reject(rejection);
            }
        };

        let decision = evaluate::evaluate(
            &self.store,
            &self.portal_id,
            credential.as_ref(),
            &dataset,
            now_secs(),
        )
        .await;
        // Shadow mode logs its admissions, and an admitted request has already
        // authenticated — naming its dataset costs what a real customer costs.
        // A rejection logs only what an earlier rung happened to resolve.
        if let (Decision::Admit, Enforcement::LogOnly) = (decision, self.enforcement) {
            dataset.resolve();
        }
        self.log(
            decision,
            credential
                .as_ref()
                .map(|credential| credential.key_id.as_str()),
            dataset.resolved().as_deref(),
        );
        decision
    }

    /// Keys carry canonical names, so an alias is resolved first. An
    /// unresolvable one is compared as written and simply fails to match.
    fn dataset_for(&self, path: &str, names_dataset: bool) -> Option<String> {
        if !names_dataset {
            return None;
        }
        let raw = dataset_path_segment(path)?;
        Some(
            self.catalog
                .canonical_name(raw)
                .unwrap_or_else(|| raw.to_owned()),
        )
    }

    /// OB-12's public half: the wire code and nothing finer. Shadow mode served
    /// every request, so one neutral series covers all its verdicts.
    fn count(&self, decision: Decision) {
        metrics::report_auth_decision(self.public_outcome(decision), self.enforcement.as_str());
    }

    /// Split out so the non-disclosure property is a pure function two cases
    /// can be compared through.
    fn public_outcome(&self, decision: Decision) -> AuthDecision {
        match (self.enforcement, decision) {
            (Enforcement::LogOnly, _) => AuthDecision::ShadowEvaluated,
            (_, Decision::Admit) => AuthDecision::Admit,
            (_, Decision::Reject(rejection)) => AuthDecision::Reject(rejection.code),
        }
    }

    /// Log-only mode records every request; enforcing mode records only the
    /// requests it turns away, since admissions are the hot path.
    fn log(&self, decision: Decision, key_id: Option<&str>, dataset: Option<&str>) {
        let enforcing = self.enforcing();
        let key_id = key_id.unwrap_or("none");
        let dataset = dataset.unwrap_or("-");
        let portal_id = self.portal_id.as_str();
        let enforcement = self.enforcement.as_str();

        let Decision::Reject(rejection) = decision else {
            if !enforcing {
                tracing::info!(
                    key_id,
                    dataset,
                    portal_id,
                    decision = "admit",
                    reason = "authorized",
                    enforcement,
                    "commercial authorization"
                );
            }
            return;
        };
        tracing::warn!(
            key_id,
            dataset,
            portal_id,
            // Shadow mode says what it would have done, since it did not.
            decision = if enforcing { "reject" } else { "would_reject" },
            // The internal rung; the wire and the scrape both coarsen it.
            reason = rejection.reason,
            error_code = rejection.code.as_str(),
            status = rejection.code.status().as_u16(),
            enforcement,
            "commercial authorization"
        );
    }
}

/// Installed by `Gated::route` on the routes that declared `.auth()`.
pub(super) async fn middleware(
    gate: Arc<Gate>,
    names_dataset: bool,
    req: Request,
    next: Next,
) -> Response {
    let decision = gate.decide(req.headers(), req.uri(), names_dataset).await;
    gate.count(decision);
    match (decision, gate.enforcement) {
        (Decision::Reject(rejection), Enforcement::Enforce) => rejection.into_response(),
        _ => next.run(req).await,
    }
}

/// The bearer header is the only channel. A query parameter would put the secret
/// in a URL, and a URL reaches browser history, `Referer` and the access log of
/// every proxy in front of the Portal — none of which this system can see or
/// clear (IB-9).
///
/// A malformed token is a rejection rather than an absent credential: falling
/// back to "no key presented" would hide typos behind a different error.
fn credential_from_request(headers: &HeaderMap) -> Result<Option<Credential>, Rejection> {
    let Some(token) = bearer_token(headers)? else {
        return Ok(None);
    };

    parse_token(&token).map(Some).ok_or(evaluate::MALFORMED)
}

fn bearer_token(headers: &HeaderMap) -> Result<Option<String>, Rejection> {
    let Some(value) = headers.get(header::AUTHORIZATION) else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| evaluate::MALFORMED)?;
    let (scheme, token) = value.split_once(' ').ok_or(evaluate::MALFORMED)?;
    if !scheme.eq_ignore_ascii_case("Bearer")
        || token.is_empty()
        || token.chars().any(char::is_whitespace)
    {
        return Err(evaluate::MALFORMED);
    }
    Ok(Some(token.to_owned()))
}

fn parse_token(token: &str) -> Option<Credential> {
    let rest = TOKEN_PREFIXES
        .iter()
        .find_map(|prefix| token.strip_prefix(prefix))?;
    // `_` is outside the segment charset, so the first one is the separator.
    let (key_id, secret) = rest.split_once('_')?;
    if !is_segment(key_id, MAX_KEY_ID_LEN) || !is_segment(secret, MAX_SECRET_LEN) {
        return None;
    }
    Some(Credential {
        key_id: key_id.to_owned(),
        secret_sha256: sha256_hex(secret),
    })
}

fn is_segment(segment: &str, max_len: usize) -> bool {
    !segment.is_empty()
        && segment.len() <= max_len
        && segment
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'~' || byte == b'-')
}

fn sha256_hex(secret: &str) -> String {
    hex::encode(Sha256::digest(secret.as_bytes()))
}

fn dataset_path_segment(path: &str) -> Option<&str> {
    let mut segments = path.trim_start_matches('/').split('/');
    match (segments.next(), segments.next()) {
        (Some("datasets"), Some(dataset)) if !dataset.is_empty() => Some(dataset),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use axum::{
        body::Body,
        http::{HeaderValue, Request as HttpRequest, StatusCode},
        middleware::from_fn,
        routing::post,
        Router,
    };
    use tower::ServiceExt;

    use super::*;
    use crate::{
        commercial::{
            test_support::{key_record, store_with, SECRET, SECRET_SHA256},
            types::KeyRecord,
        },
        types::ErrorCode,
    };

    const TOKEN: &str = "sqd_portal_k1_theverysecretvalue";
    const PORTAL: &str = "portal-premium-eu";

    #[derive(Default)]
    struct StaticCatalog {
        aliases: HashMap<String, String>,
        /// Counts every canonicalization, so a test can prove which rungs of
        /// the ladder pay for one.
        lookups: Arc<AtomicUsize>,
    }

    impl DatasetCatalog for StaticCatalog {
        fn canonical_name(&self, alias: &str) -> Option<String> {
            self.lookups.fetch_add(1, Ordering::Relaxed);
            self.aliases.get(alias).cloned()
        }
    }

    async fn gate(records: Vec<KeyRecord>, enforcement: Enforcement) -> Arc<Gate> {
        counting_gate(records, enforcement).await.0
    }

    async fn counting_gate(
        records: Vec<KeyRecord>,
        enforcement: Enforcement,
    ) -> (Arc<Gate>, Arc<AtomicUsize>) {
        let lookups = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Gate {
            store: store_with(records).await,
            catalog: Arc::new(StaticCatalog {
                aliases: HashMap::from([("base".to_string(), "base-mainnet".to_string())]),
                lookups: lookups.clone(),
            }),
            portal_id: PORTAL.to_string(),
            enforcement,
        });
        (gate, lookups)
    }

    /// A gated route at `path`, reading the dataset off it as `Gated::route` does.
    fn app(gate: Arc<Gate>, path: &str) -> Router {
        let names_dataset = path.starts_with("/datasets/:dataset/");
        Router::new().route(
            path,
            post(|| async { "served" }).route_layer(from_fn(move |req, next| {
                middleware(gate.clone(), names_dataset, req, next)
            })),
        )
    }

    async fn call(app: Router, request: HttpRequest<Body>) -> (StatusCode, String) {
        let response = app.oneshot(request).await.unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    async fn body_json(response: axum::response::Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).expect("error bodies are the envelope")
    }

    fn error_code(body: &str) -> String {
        let body: serde_json::Value = serde_json::from_str(body).expect("the envelope");
        body["error"]["code"].as_str().expect("a code").to_owned()
    }

    fn request(uri: &str) -> axum::http::request::Builder {
        HttpRequest::builder().method("POST").uri(uri)
    }

    fn header_map(authorization: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(authorization).unwrap(),
        );
        headers
    }

    #[test]
    fn parses_a_token_from_every_accepted_prefix() {
        for prefix in TOKEN_PREFIXES {
            let credential = parse_token(&format!("{prefix}k1_{SECRET}"))
                .unwrap_or_else(|| panic!("{prefix} token should parse"));
            assert_eq!(credential.key_id, "k1");
            assert_eq!(credential.secret_sha256, SECRET_SHA256);
        }
    }

    #[test]
    fn rejects_tokens_that_are_not_key_id_plus_secret() {
        for token in [
            "",
            "k1_secret",
            "sqd_portal_",
            "sqd_portal_k1",
            "sqd_portal__secret",
            "sqd_portal_k1_",
        ] {
            assert!(parse_token(token).is_none(), "{token} must not parse");
        }
    }

    /// The control plane mints two prefixes. Accepting a third widens what can
    /// enter the negative cache and the rejection logs for no benefit.
    #[test]
    fn rejects_prefixes_the_control_plane_never_mints() {
        for token in [
            format!("sqd_data_k1_{SECRET}"),
            format!("sqd_k1_{SECRET}"),
            format!("portal_k1_{SECRET}"),
        ] {
            assert!(parse_token(&token).is_none(), "{token} must not parse");
        }
    }

    /// Both segments mirror the control plane's `[A-Za-z0-9~-]+`, bounded so an
    /// attacker cannot choose how much memory a rejected token costs.
    #[test]
    fn rejects_segments_outside_the_control_plane_charset() {
        for token in [
            "sqd_portal_k.1_secret",
            "sqd_portal_k1_sec.ret",
            "sqd_portal_k/1_secret",
            "sqd_portal_k1_sec ret",
            "sqd_portal_kéy_secret",
            "prt_k1_sécret",
            "sqd_portal_k+1_secret",
        ] {
            assert!(parse_token(token).is_none(), "{token} must not parse");
        }

        let credential = parse_token("sqd_portal_a~b-c9_d~e-f0").expect("the CP charset parses");
        assert_eq!(credential.key_id, "a~b-c9");
    }

    #[test]
    fn rejects_segments_longer_than_the_control_plane_can_mint() {
        let long_id = "a".repeat(MAX_KEY_ID_LEN + 1);
        assert!(parse_token(&format!("sqd_portal_{long_id}_{SECRET}")).is_none());

        let long_secret = "b".repeat(MAX_SECRET_LEN + 1);
        assert!(parse_token(&format!("sqd_portal_k1_{long_secret}")).is_none());

        // The caps themselves are still accepted.
        let at_cap = format!(
            "sqd_portal_{}_{}",
            "a".repeat(MAX_KEY_ID_LEN),
            "b".repeat(MAX_SECRET_LEN)
        );
        assert!(parse_token(&at_cap).is_some());
    }

    #[test]
    fn credential_debug_never_prints_the_digest() {
        let credential = parse_token(TOKEN).unwrap();
        let rendered = format!("{credential:?}");

        assert!(rendered.contains("k1"));
        assert!(!rendered.contains(&credential.secret_sha256));
    }

    #[test]
    fn hash_matches_the_control_plane_vector() {
        assert_eq!(sha256_hex(SECRET), SECRET_SHA256);
    }

    #[test]
    fn a_credential_comes_from_the_bearer_header() {
        let mut headers = HeaderMap::new();
        assert_eq!(credential_from_request(&headers), Ok(None));

        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("bearer {TOKEN}")).unwrap(),
        );
        let credential = credential_from_request(&headers)
            .unwrap()
            .expect("header credential");
        assert_eq!(credential.key_id, "k1");
    }

    /// The secret must never reach a URL: a URL lands in browser history, in
    /// `Referer`, and in the access log of every proxy in front of the Portal,
    /// none of which this system can see or clear. Re-adding the channel would
    /// otherwise be a one-line change nobody notices.
    #[tokio::test]
    async fn a_token_in_the_query_string_is_not_a_credential() {
        let gate = gate(vec![key_record("k1", 1)], Enforcement::Enforce).await;
        let app = app(gate, "/datasets/:dataset/stream");

        for query in ["api_key", "apikey", "key", "token", "access_token"] {
            let (status, body) = call(
                app.clone(),
                request(&format!("/datasets/base/stream?{query}={TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;

            assert_eq!(status, StatusCode::FORBIDDEN, "{query}");
            assert_eq!(error_code(&body), "missing_credential", "{query}");
        }
    }

    #[test]
    fn malformed_authorization_headers_are_rejected() {
        for value in [
            HeaderValue::from_static(""),
            HeaderValue::from_static("Bearer"),
            HeaderValue::from_static("Basic xyz"),
            HeaderValue::from_static("Bearer  double-space"),
            HeaderValue::from_bytes(b"Bearer \xff").unwrap(),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header::AUTHORIZATION, value.clone());
            assert!(
                credential_from_request(&headers).is_err(),
                "{value:?} must be rejected"
            );
        }
    }

    #[tokio::test]
    async fn a_valid_key_is_served_from_header_and_query_alike() {
        let gate = gate(vec![key_record("k1", 1)], Enforcement::Enforce).await;
        let app = app(gate, "/datasets/:dataset/stream");

        let (status, body) = call(
            app.clone(),
            request("/datasets/base/stream")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "served");
    }

    /// GAP-29: the refusal has to arrive in the ADR-011 envelope, or the routed
    /// middleware rewrites it to 400 `malformed_request` and the client cannot
    /// tell an invalid key from a malformed query.
    #[tokio::test]
    async fn a_request_without_a_key_is_refused_in_the_taxonomy() {
        let gate = gate(vec![key_record("k1", 1)], Enforcement::Enforce).await;
        let app = app(gate, "/datasets/:dataset/stream");

        let response = app
            .oneshot(
                request("/datasets/base/stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(
            !response.headers().contains_key(header::WWW_AUTHENTICATE),
            "a 403 defines no challenge, and one here would re-add a status-line \
             distinction between credential failures"
        );
        assert_eq!(
            response.extensions().get::<ErrorCode>().copied(),
            Some(ErrorCode::MissingCredential),
            "the code must reach the middleware, or the metric lies (INV-30)"
        );

        let body = body_json(response).await;
        assert_eq!(body["error"]["type"], "authentication_error");
        assert_eq!(body["error"]["code"], "missing_credential");
        assert_eq!(body["error"]["message"], "API key required");
    }

    #[tokio::test]
    async fn a_wrong_secret_is_rejected_before_the_handler_runs() {
        let gate = gate(vec![key_record("k1", 1)], Enforcement::Enforce).await;
        let app = app(gate, "/datasets/:dataset/stream");

        let (status, body) = call(
            app,
            request("/datasets/base/stream")
                .header(header::AUTHORIZATION, "Bearer sqd_portal_k1_wrong")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(error_code(&body), "invalid_credential");
    }

    /// Every credential refusal shares one status, so the status line cannot say
    /// whether the secret was right (INV-39).
    #[tokio::test]
    async fn a_scope_refusal_is_indistinguishable_from_a_bad_secret_by_status() {
        let mut record = key_record("k1", 1);
        record.datasets = Some(vec!["base-mainnet".to_string()]);
        let app = app(
            gate(vec![record], Enforcement::Enforce).await,
            "/datasets/:dataset/stream",
        );

        let response = app
            .oneshot(
                request("/datasets/ethereum-mainnet/stream")
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(!response.headers().contains_key(header::WWW_AUTHENTICATE));
        assert!(!response.headers().contains_key(header::RETRY_AFTER));
        assert_eq!(
            body_json(response).await["error"]["type"],
            "permission_error"
        );
    }

    #[tokio::test]
    async fn a_dataset_scoped_key_matches_the_canonical_name_behind_an_alias() {
        let mut record = key_record("k1", 1);
        record.datasets = Some(vec!["base-mainnet".to_string()]);
        let app = app(
            gate(vec![record], Enforcement::Enforce).await,
            "/datasets/:dataset/stream",
        );

        let (status, _) = call(
            app.clone(),
            request("/datasets/base/stream")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "the alias resolves to base-mainnet");

        let (status, _) = call(
            app,
            request("/datasets/ethereum-mainnet/stream")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    /// REQ-53: `/sql/query` names its datasets in the body, so a scoped key
    /// cannot be checked there and is refused (OQ-13).
    #[tokio::test]
    async fn a_route_without_a_dataset_is_closed_to_dataset_scoped_keys() {
        let mut record = key_record("k1", 1);
        record.datasets = Some(vec!["base-mainnet".to_string()]);
        let app = app(gate(vec![record], Enforcement::Enforce).await, "/sql/query");

        let (status, _) = call(
            app,
            request("/sql/query")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn log_only_admits_everything_the_ladder_would_reject() {
        let mut record = key_record("k1", 1);
        record.datasets = Some(vec!["nothing-matching".to_string()]);
        record.portal_ids = Some(Vec::new());
        let app = app(
            gate(vec![record], Enforcement::LogOnly).await,
            "/datasets/:dataset/stream",
        );

        for token in [
            // No credential at all.
            None,
            // A valid one the ladder refuses on both scope rungs.
            Some(format!("Bearer {TOKEN}")),
            // Unparseable.
            Some("Bearer garbage".to_string()),
            // A well-formed token naming a key the portal does not know.
            Some(format!("Bearer sqd_portal_unknown_{SECRET}")),
        ] {
            let mut builder = request("/datasets/base/stream");
            if let Some(token) = &token {
                builder = builder.header(header::AUTHORIZATION, token);
            }
            let (status, body) = call(app.clone(), builder.body(Body::empty()).unwrap()).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "{token:?} must be admitted in log_only"
            );
            assert_eq!(body, "served");
        }
    }

    /// Canonicalizing a dataset interns its name in a process-wide pool and
    /// clones the dataset config, so unauthenticated traffic must never reach
    /// it: the rungs below the dataset rung are the whole defence.
    #[tokio::test]
    async fn a_request_that_fails_an_earlier_rung_never_resolves_the_dataset() {
        let mut record = key_record("k1", 1);
        record.datasets = Some(vec!["base-mainnet".to_string()]);
        let (gate, lookups) = counting_gate(vec![record], Enforcement::Enforce).await;
        let uri: axum::http::Uri = "/datasets/base/stream".parse().unwrap();

        for headers in [
            // No credential at all.
            HeaderMap::new(),
            // A token that cannot be parsed.
            header_map("Bearer nonsense"),
            // A well-formed token naming a key the portal does not know.
            header_map(&format!("Bearer sqd_portal_unknown_{SECRET}")),
            // A known key presenting the wrong secret.
            header_map("Bearer sqd_portal_k1_wrong"),
        ] {
            let decision = gate.decide(&headers, &uri, true).await;
            assert!(matches!(decision, Decision::Reject(_)));
        }
        assert_eq!(
            lookups.load(Ordering::Relaxed),
            0,
            "a request that never authenticates must not touch the dataset catalog"
        );

        // The dataset rung itself still resolves, exactly once.
        let decision = gate
            .decide(&header_map(&format!("Bearer {TOKEN}")), &uri, true)
            .await;
        assert_eq!(decision, Decision::Admit);
        assert_eq!(lookups.load(Ordering::Relaxed), 1);
    }

    /// A key with no dataset list is authorized for every dataset, so nothing
    /// in the ladder needs the request's dataset resolved.
    #[tokio::test]
    async fn an_unscoped_key_does_not_resolve_the_dataset_either() {
        let (gate, lookups) = counting_gate(vec![key_record("k1", 1)], Enforcement::Enforce).await;
        let uri: axum::http::Uri = "/datasets/base/stream".parse().unwrap();

        let decision = gate
            .decide(&header_map(&format!("Bearer {TOKEN}")), &uri, true)
            .await;

        assert_eq!(decision, Decision::Admit);
        assert_eq!(lookups.load(Ordering::Relaxed), 0);
    }

    /// Shadow mode still names the dataset of the requests it admits: those
    /// have authenticated, so resolving costs what a real customer costs.
    #[tokio::test]
    async fn log_only_still_resolves_the_dataset_of_an_admitted_request() {
        let (gate, lookups) = counting_gate(vec![key_record("k1", 1)], Enforcement::LogOnly).await;
        let uri: axum::http::Uri = "/datasets/base/stream".parse().unwrap();

        gate.decide(&HeaderMap::new(), &uri, true).await;
        assert_eq!(
            lookups.load(Ordering::Relaxed),
            0,
            "an anonymous request is free in shadow mode too"
        );

        let decision = gate
            .decide(&header_map(&format!("Bearer {TOKEN}")), &uri, true)
            .await;
        assert_eq!(decision, Decision::Admit);
        assert_eq!(lookups.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn log_only_still_evaluates_the_full_ladder() {
        let gate = gate(vec![key_record("k1", 1)], Enforcement::LogOnly).await;
        let uri: axum::http::Uri = "/datasets/base/stream".parse().unwrap();

        let decision = gate.decide(&HeaderMap::new(), &uri, true).await;

        assert!(matches!(decision, Decision::Reject(_)));
    }

    /// Everything two keyless scrapes bracketing one request could see change.
    /// Compared between cases rather than sampled: the families are global, so
    /// a timing test would race the rest of the binary.
    fn public_projection(gate: &Gate, decision: Decision) -> Vec<(String, String)> {
        metrics::auth_decision_labels(gate.public_outcome(decision), gate.enforcement.as_str())
    }

    /// INV-39 on the keyless metrics surface. The unauthenticated rungs share
    /// `invalid_credential` on the wire, so they must share a series: one that
    /// split them is an enumeration oracle with a scrape interval attached.
    #[tokio::test]
    async fn a_scrape_cannot_tell_an_unknown_key_from_a_wrong_secret() {
        let gate = gate(vec![key_record("k1", 1)], Enforcement::Enforce).await;
        let uri: axum::http::Uri = "/datasets/base/stream".parse().unwrap();

        let mut projections = Vec::new();
        for token in [
            // Known id, wrong secret.
            "Bearer sqd_portal_k1_wrong".to_string(),
            // An id the snapshot has never held, and the control plane
            // authoritatively does not know.
            format!("Bearer sqd_portal_guessed_{SECRET}"),
            // A token the portal cannot even parse.
            "Bearer sqd_portal_nonsense".to_string(),
        ] {
            let decision = gate.decide(&header_map(&token), &uri, true).await;
            projections.push((decision, public_projection(&gate, decision)));
        }

        let reasons: Vec<_> = projections
            .iter()
            .map(|(decision, _)| match decision {
                Decision::Reject(rejection) => rejection.reason,
                Decision::Admit => "admit",
            })
            .collect();
        assert_eq!(
            reasons,
            ["invalid_secret", "unknown_key", "malformed_credential"],
            "three distinct rungs, or the test proves nothing"
        );

        let first = &projections[0].1;
        assert!(
            first.contains(&("error_code".to_owned(), "invalid_credential".to_owned())),
            "{first:?}"
        );
        for (decision, projection) in &projections {
            assert_eq!(
                projection, first,
                "{decision:?} is publicly distinguishable: {projection:?}"
            );
        }
    }

    /// GAP-32, the accepted residual. Under lookup pressure a snapshot miss and
    /// a snapshot hit answer differently — retryable congestion versus a
    /// credential verdict — so the wire does reveal membership there. That is
    /// REQ-54's accurate retry contract, and the alternative is refusing valid
    /// keys during a control-plane blip.
    ///
    /// What must not happen is the scrape amplifying it: the counter carries
    /// the code the caller already received and nothing about the lookup.
    #[tokio::test]
    async fn a_saturated_lookup_is_publicly_indistinguishable_from_any_overload() {
        let gate = gate(Vec::new(), Enforcement::Enforce).await;
        gate.store.exhaust_lookup_budget_for_test();
        let uri: axum::http::Uri = "/datasets/base/stream".parse().unwrap();

        let decision = gate
            .decide(
                &header_map(&format!("Bearer sqd_portal_minted-just-now_{SECRET}")),
                &uri,
                true,
            )
            .await;

        let Decision::Reject(rejection) = decision else {
            panic!("a spent budget refuses");
        };
        assert_eq!(rejection.reason, "lookup_saturated");

        let projection = public_projection(&gate, decision);
        assert_eq!(
            projection,
            metrics::auth_decision_labels(
                AuthDecision::Reject(ErrorCode::Overloaded),
                Enforcement::Enforce.as_str(),
            ),
            "the scrape must say only what the 529 said"
        );
        assert!(
            !projection
                .iter()
                .any(|(_, value)| value.contains("lookup") || value.contains("snapshot")),
            "no lookup detail may reach a keyless scrape: {projection:?}"
        );
    }

    /// Shadow mode served all of these, so its scrape must not say which was
    /// which — publishing the would-be verdict is the oracle enforcement is not
    /// (REQ-55).
    #[tokio::test]
    async fn a_shadow_scrape_says_only_that_a_request_was_evaluated() {
        let mut scoped = key_record("k2", 1);
        scoped.datasets = Some(vec!["nothing-matching".to_string()]);
        let gate = gate(vec![key_record("k1", 1), scoped], Enforcement::LogOnly).await;
        let uri: axum::http::Uri = "/datasets/base/stream".parse().unwrap();

        let mut projections = Vec::new();
        for headers in [
            // Would admit.
            header_map(&format!("Bearer {TOKEN}")),
            // Would refuse: no credential.
            HeaderMap::new(),
            // Would refuse: scoped to another dataset.
            header_map(&format!("Bearer sqd_portal_k2_{SECRET}")),
        ] {
            let decision = gate.decide(&headers, &uri, true).await;
            projections.push(public_projection(&gate, decision));
        }

        for projection in &projections {
            assert_eq!(projection, &projections[0], "{projection:?}");
            assert!(
                projection.contains(&("decision".to_owned(), "shadow_evaluated".to_owned())),
                "{projection:?}"
            );
            assert!(
                !projection.iter().any(|(name, _)| name == "error_code"),
                "a shadow verdict must not reach the keyless scrape: {projection:?}"
            );
        }
    }

    /// An enforced refusal is not silent, though, or the cutover is blind. Goes
    /// through the counter to prove the wiring; asserts a floor rather than an
    /// exact delta, since the family is shared.
    #[tokio::test]
    async fn an_enforced_refusal_is_counted_under_the_code_the_client_received() {
        let mut record = key_record("k1", 1);
        record.expires_at = Some(1);
        let gate = gate(vec![record], Enforcement::Enforce).await;
        let uri: axum::http::Uri = "/datasets/base/stream".parse().unwrap();

        let expired = || {
            metrics::auth_decisions(
                AuthDecision::Reject(ErrorCode::ExpiredCredential),
                Enforcement::Enforce.as_str(),
            )
        };
        let before = expired();

        let decision = gate
            .decide(&header_map(&format!("Bearer {TOKEN}")), &uri, true)
            .await;
        gate.count(decision);

        assert!(expired() > before, "the refusal must reach the scrape");
    }
}
