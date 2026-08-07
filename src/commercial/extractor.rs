use std::{fmt, sync::Arc};

use axum::{
    extract::Request,
    http::{header, HeaderMap},
    middleware::Next,
    response::Response,
};
use sha2::{Digest, Sha256};

use super::{
    cache::GrantCache,
    config::{CommercialConfig, Enforcement},
    evaluate::{self, Decision, LazyDataset, Rejection, Verdict},
    now_secs,
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
/// rejected before it reaches the denial cache, a log line, or an exchange.
const MAX_KEY_ID_LEN: usize = 64;
const MAX_SECRET_LEN: usize = 128;

/// The presented token, held only as far as the exchange that carries it. Its
/// renderings are redacted so the one way it can be disclosed is by asking for
/// it (INV-38).
#[derive(Clone, PartialEq, Eq)]
pub struct SecretToken(String);

impl SecretToken {
    /// Named to be greppable: every call site should be one an audit expects.
    /// Today there is exactly one, in the exchange request body.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// A presented key, reduced to what the gate needs. The fingerprint covers the
/// *whole* token: keyed on the id alone the cache would admit the next caller to
/// name it, and would answer differently for a wrong secret (INV-39).
#[derive(Clone, PartialEq, Eq)]
pub struct Credential {
    pub key_id: String,
    pub fingerprint: String,
    pub token: SecretToken,
}

impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credential")
            .field("key_id", &self.key_id)
            .field("fingerprint", &self.fingerprint)
            .field("token", &self.token)
            .finish()
    }
}

/// Grants carry canonical dataset names, so an alias in the request URL has to
/// be resolved before it can be matched.
pub trait DatasetCatalog: Send + Sync {
    fn canonical_name(&self, alias: &str) -> Option<String>;
}

impl DatasetCatalog for NetworkClient {
    fn canonical_name(&self, alias: &str) -> Option<String> {
        self.dataset(alias).map(|dataset| dataset.default_name)
    }
}

pub struct Gate {
    cache: Arc<GrantCache>,
    catalog: Arc<dyn DatasetCatalog>,
    portal_id: String,
    enforcement: Enforcement,
}

impl Gate {
    pub fn new(
        config: &CommercialConfig,
        cache: Arc<GrantCache>,
        catalog: Arc<dyn DatasetCatalog>,
    ) -> Self {
        Self {
            cache,
            catalog,
            portal_id: config.portal_id(),
            enforcement: config.enforcement,
        }
    }

    /// Whether this gate turns its verdicts into responses. Shadow mode does
    /// not, so nothing the control plane says can reject a request.
    pub fn enforcing(&self) -> bool {
        self.enforcement == Enforcement::Enforce
    }

    /// Republished on scrape so it climbs through an outage rather than
    /// freezing at the last value (OB-13).
    pub fn publish_freshness(&self) {
        if !self.enforcing() {
            return;
        }
        metrics::report_exchange_success_age(self.cache.last_exchange_success_age());
        metrics::report_grant_cache_capacity(self.cache.capacity());
        let (in_grace, min_remaining) = self.cache.grace_census(now_secs());
        metrics::report_grace_census(in_grace, min_remaining);
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
                let verdict = Verdict {
                    decision: Decision::Reject(rejection),
                    denial_reason: None,
                };
                self.log(&verdict, None, None);
                return verdict.decision;
            }
        };

        let verdict =
            evaluate::evaluate(&self.cache, credential.as_ref(), &dataset, now_secs()).await;
        // Shadow mode logs its admissions, and an admitted request has already
        // authenticated — naming its dataset costs what a real customer costs.
        // A rejection logs only what an earlier rung happened to resolve.
        if let (Decision::Admit, Enforcement::LogOnly) = (verdict.decision, self.enforcement) {
            dataset.resolve();
        }
        self.log(
            &verdict,
            credential
                .as_ref()
                .map(|credential| credential.key_id.as_str()),
            dataset.resolved().as_deref(),
        );
        verdict.decision
    }

    /// Grants carry canonical names, so an alias is resolved first. An
    /// unresolvable one is compared as written and simply fails to match.
    fn dataset_for(&self, path: &str, names_dataset: bool) -> Option<String> {
        if !names_dataset {
            return None;
        }
        let raw = dataset_path_segment(path)?;
        // Decoded the way the handler's `Path` extractor decodes, or the gate
        // and the handler disagree about which dataset a request names and a
        // scoped key is refused on any encoded spelling of an allowed URL. A
        // segment that does not decode stays unresolved, which refuses a
        // scoped key — the handler answers 400 to such a path anyway.
        let decoded = percent_encoding::percent_decode_str(raw)
            .decode_utf8()
            .ok()?;
        Some(
            self.catalog
                .canonical_name(&decoded)
                .unwrap_or_else(|| decoded.into_owned()),
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
    fn log(&self, verdict: &Verdict, key_id: Option<&str>, dataset: Option<&str>) {
        let enforcing = self.enforcing();
        let key_id = key_id.unwrap_or("none");
        let dataset = dataset.unwrap_or("-");
        let portal_id = self.portal_id.as_str();
        let enforcement = self.enforcement.as_str();

        let Decision::Reject(rejection) = verdict.decision else {
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
            // What the control plane called it, where it was the one refusing.
            // The only place a reason this build has no code for survives.
            denial_reason = verdict.denial_reason.as_deref().unwrap_or("-"),
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

/// Bearer header only: a query parameter puts the secret in browser history,
/// `Referer` and every proxy's access log (IB-9). A malformed token rejects
/// rather than reading as absent, which would hide typos behind another error.
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

#[cfg(test)]
pub(super) fn parse_token_for_test(token: &str) -> Option<Credential> {
    parse_token(token)
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
        fingerprint: sha256_hex(token),
        token: SecretToken(token.to_owned()),
    })
}

fn is_segment(segment: &str, max_len: usize) -> bool {
    !segment.is_empty()
        && segment.len() <= max_len
        && segment
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'~' || byte == b'-')
}

fn sha256_hex(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
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
        commercial::test_support::{cache_with, MockControlPlane, KEY_ID, SECRET, TOKEN},
        types::ErrorCode,
    };

    const PORTAL: &str = "portal-premium-eu";
    const NOW: u64 = 1_800_000_000;

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

    /// A gate whose control plane grants `KEY_ID` the given dataset scope. The
    /// grant is minted far enough ahead that no test races its renewal.
    async fn gate_granting(datasets: Option<Vec<&str>>, enforcement: Enforcement) -> Arc<Gate> {
        let cp = MockControlPlane::spawn().await;
        cp.grant(
            KEY_ID,
            datasets.map(|list| list.into_iter().map(str::to_owned).collect()),
            NOW + 86_400,
            NOW + 86_400,
        );
        gate_for(&cp, enforcement).await.0
    }

    /// The cache shares the gate's enforcement, exactly as `commercial::build`
    /// wires it: a shadow gate over an enforcing cache is a combination
    /// production cannot produce, and it would run these tests with the
    /// cache-side OB-13 metrics suppression disabled (INV-39).
    async fn gate_for(
        cp: &MockControlPlane,
        enforcement: Enforcement,
    ) -> (Arc<Gate>, Arc<AtomicUsize>) {
        let lookups = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Gate {
            cache: cache_with(cp, enforcement).await,
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
            let token = format!("{prefix}k1_{SECRET}");
            let credential =
                parse_token(&token).unwrap_or_else(|| panic!("{prefix} token should parse"));
            assert_eq!(credential.key_id, "k1");
            assert_eq!(credential.token.expose(), token);
        }
    }

    /// The property the cache key rests on: the same id with a different secret
    /// is a different credential, so it can never reach the other's grant.
    #[test]
    fn the_fingerprint_covers_the_secret_and_not_just_the_id() {
        let mine = parse_token(&format!("sqd_portal_k1_{SECRET}")).unwrap();
        let guessed = parse_token("sqd_portal_k1_wrong").unwrap();

        assert_eq!(mine.key_id, guessed.key_id);
        assert_ne!(mine.fingerprint, guessed.fingerprint);
        assert_eq!(
            mine.fingerprint,
            sha256_hex(&format!("sqd_portal_k1_{SECRET}"))
        );
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
    /// buy an exchange and enter the rejection logs, for no benefit.
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
    fn no_rendering_of_a_credential_prints_the_token() {
        let credential = parse_token(TOKEN).unwrap();

        for rendered in [
            format!("{credential:?}"),
            format!("{:?}", credential.token),
            format!("{:#?}", credential),
        ] {
            assert!(
                !rendered.contains(SECRET),
                "the secret reached a rendering: {rendered}"
            );
        }
        assert!(format!("{credential:?}").contains("k1"));
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
        let app = app(
            gate_granting(None, Enforcement::Enforce).await,
            "/datasets/:dataset/stream",
        );

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
    async fn a_valid_key_is_served() {
        let app = app(
            gate_granting(None, Enforcement::Enforce).await,
            "/datasets/:dataset/stream",
        );

        let (status, body) = call(
            app,
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
        let app = app(
            gate_granting(None, Enforcement::Enforce).await,
            "/datasets/:dataset/stream",
        );

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
    async fn a_denied_credential_is_rejected_before_the_handler_runs() {
        let cp = MockControlPlane::spawn().await;
        cp.deny(KEY_ID, "invalid_secret");
        let app = app(
            gate_for(&cp, Enforcement::Enforce).await.0,
            "/datasets/:dataset/stream",
        );

        let (status, body) = call(
            app,
            request("/datasets/base/stream")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
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
        let app = app(
            gate_granting(Some(vec!["base-mainnet"]), Enforcement::Enforce).await,
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

    /// The handler reads the dataset through axum's `Path`, which
    /// percent-decodes; the gate must decode the same way, or a scoped key is
    /// refused on an encoded spelling of a URL the grant covers — and which
    /// spelling arrives is the client library's choice, not the customer's.
    /// The handler here extracts `Path` for real, so the test pins the
    /// gate-vs-handler agreement itself, not just the gate's half of it.
    #[tokio::test]
    async fn a_scoped_key_matches_a_percent_encoded_spelling_of_an_allowed_dataset() {
        let gate = gate_granting(Some(vec!["base-mainnet"]), Enforcement::Enforce).await;
        let app = Router::new().route(
            "/datasets/:dataset/stream",
            post(
                |axum::extract::Path(dataset): axum::extract::Path<String>| async move { dataset },
            )
            .route_layer(from_fn(move |req, next| {
                middleware(gate.clone(), true, req, next)
            })),
        );

        // The canonical name and the alias, each spelled with an encoded byte;
        // the body is what the handler's `Path` decoded.
        for (uri, served) in [
            ("/datasets/base%2Dmainnet/stream", "base-mainnet"),
            ("/datasets/bas%65/stream", "base"),
        ] {
            let (status, body) = call(
                app.clone(),
                request(uri)
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{uri}");
            assert_eq!(body, served, "{uri}");
        }

        // An undecodable segment resolves nothing and refuses a scoped key.
        let (status, _) = call(
            app,
            request("/datasets/base%FFmainnet/stream")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn a_dataset_scoped_key_matches_the_canonical_name_behind_an_alias() {
        let app = app(
            gate_granting(Some(vec!["base-mainnet"]), Enforcement::Enforce).await,
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
        let app = app(
            gate_granting(Some(vec!["base-mainnet"]), Enforcement::Enforce).await,
            "/sql/query",
        );

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
        let cp = MockControlPlane::spawn().await;
        cp.deny(KEY_ID, "revoked");
        let app = app(
            gate_for(&cp, Enforcement::LogOnly).await.0,
            "/datasets/:dataset/stream",
        );

        for token in [
            // No credential at all.
            None,
            // A well-formed one the control plane refuses.
            Some(format!("Bearer {TOKEN}")),
            // Unparseable.
            Some("Bearer garbage".to_string()),
            // A well-formed token naming a key the control plane never answers
            // about, so the exchange fails.
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
        let cp = MockControlPlane::spawn().await;
        cp.deny(KEY_ID, "invalid_secret");
        cp.deny("unknown", "unknown_key");
        let (gate, lookups) = gate_for(&cp, Enforcement::Enforce).await;
        let uri: axum::http::Uri = "/datasets/base/stream".parse().unwrap();

        for headers in [
            // No credential at all.
            HeaderMap::new(),
            // A token that cannot be parsed.
            header_map("Bearer nonsense"),
            // A well-formed token the control plane says nothing good about.
            header_map(&format!("Bearer sqd_portal_unknown_{SECRET}")),
            // The right id with the wrong secret.
            header_map(&format!("Bearer {TOKEN}")),
        ] {
            let decision = gate.decide(&headers, &uri, true).await;
            assert!(matches!(decision, Decision::Reject(_)), "{headers:?}");
        }
        assert_eq!(
            lookups.load(Ordering::Relaxed),
            0,
            "a request that never authenticates must not touch the dataset catalog"
        );

        // The dataset rung itself still resolves, exactly once.
        let (gate, lookups) = gate_for(&cp, Enforcement::Enforce).await;
        cp.grant(
            KEY_ID,
            Some(vec!["base-mainnet".to_string()]),
            NOW + 86_400,
            NOW + 86_400,
        );
        let decision = gate
            .decide(&header_map(&format!("Bearer {TOKEN}")), &uri, true)
            .await;
        assert_eq!(decision, Decision::Admit);
        assert_eq!(lookups.load(Ordering::Relaxed), 1);
    }

    /// A grant with no dataset list authorizes every dataset, so nothing in the
    /// ladder needs the request's dataset resolved.
    #[tokio::test]
    async fn an_unscoped_key_does_not_resolve_the_dataset_either() {
        let cp = MockControlPlane::spawn().await;
        cp.grant(KEY_ID, None, NOW + 86_400, NOW + 86_400);
        let (gate, lookups) = gate_for(&cp, Enforcement::Enforce).await;
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
        let cp = MockControlPlane::spawn().await;
        cp.grant(KEY_ID, None, NOW + 86_400, NOW + 86_400);
        let (gate, lookups) = gate_for(&cp, Enforcement::LogOnly).await;
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
        let gate = gate_granting(None, Enforcement::LogOnly).await;
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
        let cp = MockControlPlane::spawn().await;
        cp.deny(KEY_ID, "invalid_secret");
        cp.deny("guessed", "unknown_key");
        let (gate, _) = gate_for(&cp, Enforcement::Enforce).await;
        let uri: axum::http::Uri = "/datasets/base/stream".parse().unwrap();

        let mut projections = Vec::new();
        for token in [
            // Known id, wrong secret.
            format!("Bearer {TOKEN}"),
            // An id the control plane authoritatively does not know.
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

    /// GAP-32, the accepted residual: under exchange pressure the wire reveals
    /// cache membership, because the alternative is answering a dependency
    /// failure with a claim about someone's key. What must not happen is the
    /// scrape amplifying it.
    #[tokio::test]
    async fn a_saturated_exchange_is_publicly_indistinguishable_from_any_overload() {
        let cp = MockControlPlane::spawn().await;
        let (gate, _) = gate_for(&cp, Enforcement::Enforce).await;
        gate.cache.exhaust_budget_for_test();
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
        assert_eq!(rejection.reason, "exchange_saturated");

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
                .any(|(_, value)| value.contains("exchange") || value.contains("grant")),
            "no exchange detail may reach a keyless scrape: {projection:?}"
        );
    }

    /// Shadow mode served all of these, so its scrape must not say which was
    /// which — publishing the would-be verdict is the oracle enforcement is not
    /// (REQ-55).
    #[tokio::test]
    async fn a_shadow_scrape_says_only_that_a_request_was_evaluated() {
        let cp = MockControlPlane::spawn().await;
        cp.grant(KEY_ID, None, NOW + 86_400, NOW + 86_400);
        cp.deny("k2", "revoked");
        let (gate, _) = gate_for(&cp, Enforcement::LogOnly).await;
        let uri: axum::http::Uri = "/datasets/base/stream".parse().unwrap();

        let mut projections = Vec::new();
        for headers in [
            // Would admit.
            header_map(&format!("Bearer {TOKEN}")),
            // Would refuse: no credential.
            HeaderMap::new(),
            // Would refuse: the control plane revoked it.
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
        let cp = MockControlPlane::spawn().await;
        cp.deny(KEY_ID, "expired");
        let (gate, _) = gate_for(&cp, Enforcement::Enforce).await;
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
