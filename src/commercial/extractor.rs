use std::{fmt, sync::Arc};

use axum::{
    extract::{MatchedPath, Request},
    http::{header, HeaderMap},
    middleware::Next,
    response::Response,
};
use sha2::{Digest, Sha256};
use url::form_urlencoded;

use super::{
    config::{CommercialConfig, Enforcement, GatedRoutes},
    evaluate::{self, Decision, LazyDataset, Rejection},
    now_secs,
    routes::{classify, Gating},
    store::SnapshotStore,
};
use crate::{
    metrics::{self, AuthDecision},
    network::NetworkClient,
    types::DatasetId,
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

const QUERY_PARAM: &str = "api_key";

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
    fn canonical_name_for_id(&self, id: &DatasetId) -> Option<String>;
}

impl DatasetCatalog for NetworkClient {
    fn canonical_name(&self, alias: &str) -> Option<String> {
        self.dataset(alias).map(|dataset| dataset.default_name)
    }

    fn canonical_name_for_id(&self, id: &DatasetId) -> Option<String> {
        self.datasets().read().default_name(id).map(str::to_owned)
    }
}

/// How the dataset under request is named in the route's path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DatasetSource {
    /// `/datasets/:dataset/…` — a name or an alias.
    Alias,
    /// `/datasets/:dataset_id/query/:worker_id` — a base64 dataset id.
    EncodedId,
    /// The route names no dataset, so a dataset-scoped key cannot use it.
    Absent,
}

/// What kind of answer a route gives, which decides whether the `data`-only
/// mode gates it. Data routes are always gated; metadata routes only under
/// `gated_routes: all`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteClass {
    /// Serves blocks, query results or block lookups.
    Data,
    /// Describes the portal or its datasets: lists, heads, heights, state,
    /// worker inventory, docs.
    Metadata,
}

impl RouteClass {
    /// The class, not the route: a series per path would name the datasets
    /// `all` exists to hide (REQ-51).
    const fn as_str(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Metadata => "metadata",
        }
    }
}

pub struct Gate {
    store: Arc<SnapshotStore>,
    catalog: Arc<dyn DatasetCatalog>,
    portal_id: String,
    enforcement: Enforcement,
    gated_routes: GatedRoutes,
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
            gated_routes: config.gated_routes,
        }
    }

    /// Whether this route needs a key at all. A metadata route on a shared
    /// portal does not, and must then cost exactly what it costs today: the
    /// middleware returns before reading the credential or the dataset.
    pub fn gates(&self, class: RouteClass) -> bool {
        match class {
            RouteClass::Data => true,
            RouteClass::Metadata => self.gated_routes == GatedRoutes::All,
        }
    }

    /// Whether the portal has mirrored the control plane's key set yet. A gated
    /// portal that has not knows no keys, so it would answer 401 to every valid
    /// one — it belongs out of rotation until this turns true.
    pub fn snapshot_ready(&self) -> bool {
        self.store.is_ready()
    }

    /// Whether this gate turns its verdicts into responses. Shadow mode does
    /// not, so nothing the snapshot does or does not know can reject a request.
    pub fn enforcing(&self) -> bool {
        self.enforcement == Enforcement::Enforce
    }

    async fn decide(
        &self,
        headers: &HeaderMap,
        uri: &axum::http::Uri,
        source: DatasetSource,
    ) -> Decision {
        // Deferred on purpose: canonicalization interns the name in a
        // process-wide pool and clones the dataset config, so it must stay
        // behind authentication. Only the dataset rung calls this.
        let dataset = LazyDataset::new(|| self.dataset_for(uri.path(), source));
        let credential = match credential_from_request(headers, uri.query()) {
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

    fn dataset_for(&self, path: &str, source: DatasetSource) -> Option<String> {
        let raw = dataset_path_segment(path)?;
        match source {
            DatasetSource::Absent => None,
            DatasetSource::Alias => Some(
                self.catalog
                    .canonical_name(raw)
                    .unwrap_or_else(|| raw.to_owned()),
            ),
            DatasetSource::EncodedId => Some(
                DatasetId::from_base64(raw)
                    .ok()
                    .and_then(|id| self.catalog.canonical_name_for_id(&id))
                    .unwrap_or_else(|| raw.to_owned()),
            ),
        }
    }

    /// OB-12's public half: the wire code and nothing finer. Shadow mode served
    /// every request, so one neutral series covers all its verdicts.
    fn count(&self, decision: Decision, class: RouteClass) {
        metrics::report_auth_decision(
            self.public_outcome(decision),
            class.as_str(),
            self.enforcement.as_str(),
        );
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

/// One layer over the whole router. What each route needs is looked up from its
/// *matched* path — the template, never the client-supplied one.
pub async fn middleware(gate: Arc<Gate>, req: Request, next: Next) -> Response {
    let Gating::Gated(source, class) = classify(matched_path(&req)) else {
        return next.run(req).await;
    };
    if !gate.gates(class) {
        return next.run(req).await;
    }
    let decision = gate.decide(req.headers(), req.uri(), source).await;
    gate.count(decision, class);
    match (decision, gate.enforcement) {
        (Decision::Reject(rejection), Enforcement::Enforce) => rejection.into_response(),
        _ => next.run(req).await,
    }
}

/// Falls back to the raw path only if axum did not record a match, which a
/// `route_layer` should make impossible. Classification is fail-closed either
/// way, so the fallback cannot open anything.
fn matched_path(req: &Request) -> &str {
    req.extensions()
        .get::<MatchedPath>()
        .map_or_else(|| req.uri().path(), MatchedPath::as_str)
}

/// A malformed token is a rejection rather than an absent credential: falling
/// back to "no key presented" would hide typos behind a different error.
fn credential_from_request(
    headers: &HeaderMap,
    query: Option<&str>,
) -> Result<Option<Credential>, Rejection> {
    let token = match bearer_token(headers)? {
        Some(token) => Some(token),
        None => query_token(query),
    };
    let Some(token) = token else {
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

/// Browser SDKs cannot set headers on every transport, so the key may also
/// arrive as a query parameter.
fn query_token(query: Option<&str>) -> Option<String> {
    form_urlencoded::parse(query?.as_bytes())
        .find(|(key, _)| key == QUERY_PARAM)
        .map(|(_, value)| value.into_owned())
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
        ids: HashMap<String, String>,
        /// Counts every canonicalization, so a test can prove which rungs of
        /// the ladder pay for one.
        lookups: Arc<AtomicUsize>,
    }

    impl DatasetCatalog for StaticCatalog {
        fn canonical_name(&self, alias: &str) -> Option<String> {
            self.lookups.fetch_add(1, Ordering::Relaxed);
            self.aliases.get(alias).cloned()
        }

        fn canonical_name_for_id(&self, id: &DatasetId) -> Option<String> {
            self.lookups.fetch_add(1, Ordering::Relaxed);
            self.ids.get(&id.to_base64()).cloned()
        }
    }

    async fn gate(records: Vec<KeyRecord>, enforcement: Enforcement) -> Arc<Gate> {
        counting_gate(records, enforcement).await.0
    }

    async fn counting_gate(
        records: Vec<KeyRecord>,
        enforcement: Enforcement,
    ) -> (Arc<Gate>, Arc<AtomicUsize>) {
        let dataset_id = DatasetId::from_url("s3://base-mainnet");
        let lookups = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Gate {
            store: store_with(records).await,
            catalog: Arc::new(StaticCatalog {
                aliases: HashMap::from([("base".to_string(), "base-mainnet".to_string())]),
                ids: HashMap::from([(dataset_id.to_base64(), "base-mainnet".to_string())]),
                lookups: lookups.clone(),
            }),
            portal_id: PORTAL.to_string(),
            enforcement,
            gated_routes: GatedRoutes::Data,
        });
        (gate, lookups)
    }

    /// The middleware reads what a route needs from its matched path, so a test
    /// router mounts the real path and the table decides the rest.
    fn app(gate: Arc<Gate>, path: &str) -> Router {
        Router::new().route(
            path,
            post(|| async { "served" }).route_layer(from_fn(move |req, next| {
                middleware(gate.clone(), req, next)
            })),
        )
    }

    /// A gate whose metadata routes are closed too, as on a single-tenant
    /// portal.
    async fn metadata_gate(records: Vec<KeyRecord>) -> (Arc<Gate>, Arc<AtomicUsize>) {
        let (gate, lookups) = counting_gate(records, Enforcement::Enforce).await;
        let gate = Arc::new(Gate {
            store: gate.store.clone(),
            catalog: gate.catalog.clone(),
            portal_id: gate.portal_id.clone(),
            enforcement: gate.enforcement,
            gated_routes: GatedRoutes::All,
        });
        (gate, lookups)
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
    fn credentials_come_from_the_header_or_the_query_parameter() {
        let mut headers = HeaderMap::new();
        assert_eq!(credential_from_request(&headers, None), Ok(None));

        let from_query = credential_from_request(&headers, Some(&format!("{QUERY_PARAM}={TOKEN}")))
            .unwrap()
            .expect("query credential");
        assert_eq!(from_query.key_id, "k1");

        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("bearer {TOKEN}")).unwrap(),
        );
        let from_header = credential_from_request(&headers, None)
            .unwrap()
            .expect("header credential");
        assert_eq!(from_header, from_query);

        // The header wins, and its own malformation is not papered over by a
        // valid query parameter.
        headers.insert(header::AUTHORIZATION, HeaderValue::from_static("Bearer"));
        assert!(
            credential_from_request(&headers, Some(&format!("{QUERY_PARAM}={TOKEN}"))).is_err()
        );
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
                credential_from_request(&headers, None).is_err(),
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

        let (status, _) = call(
            app,
            request(&format!("/datasets/base/stream?{QUERY_PARAM}={TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    /// The default: a shared portal's dataset list is public, and asking for it
    /// must cost nothing — no credential parsed, no dataset resolved.
    #[tokio::test]
    async fn a_metadata_route_is_open_and_free_under_the_data_only_default() {
        let (gate, lookups) = counting_gate(vec![key_record("k1", 1)], Enforcement::Enforce).await;
        let app = app(gate, "/datasets/:dataset/metadata");

        let (status, body) = call(
            app,
            request("/datasets/base/metadata")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "served");
        assert_eq!(
            lookups.load(Ordering::Relaxed),
            0,
            "an ungated route must not do gate work"
        );
    }

    /// A single-tenant portal: the metadata surface says which datasets that
    /// customer bought, so it needs the same key the data routes do.
    #[tokio::test]
    async fn a_metadata_route_requires_a_key_when_every_route_is_gated() {
        let (gate, _) = metadata_gate(vec![key_record("k1", 1)]).await;
        let anonymous = app(gate.clone(), "/datasets/:dataset/metadata");

        let (status, body) = call(
            anonymous,
            request("/datasets/base/metadata")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(error_code(&body), "missing_credential");

        let authorized = app(gate, "/datasets/:dataset/metadata");
        let (status, body) = call(
            authorized,
            request("/datasets/base/metadata")
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer sqd_portal_k1_{SECRET}"),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert_eq!(status, StatusCode::OK, "a valid key opens it: {body}");
    }

    /// The point of one middleware over a wrapper per route: a route nobody
    /// classified refuses rather than serves. Forgetting the old wrapper opened
    /// a route silently; forgetting the table entry closes one loudly.
    #[tokio::test]
    async fn a_route_nobody_classified_is_gated() {
        let (gate, _) = counting_gate(vec![key_record("k1", 1)], Enforcement::Enforce).await;
        let app = app(gate, "/datasets/:dataset/brand-new");

        let (status, body) = call(
            app,
            request("/datasets/base/brand-new")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(error_code(&body), "missing_credential");
    }

    /// And the one thing the default must not close: a pod that cannot answer
    /// its own probe leaves rotation, whatever the gate scope (REQ-51).
    #[tokio::test]
    async fn the_ops_surface_answers_without_a_key_under_every_scope() {
        for (gate, _) in [
            counting_gate(Vec::new(), Enforcement::Enforce).await,
            metadata_gate(Vec::new()).await,
        ] {
            for path in ["/ready", "/metrics", "/api-docs/openapi.json"] {
                let (status, _) = call(
                    app(gate.clone(), path),
                    request(path).body(Body::empty()).unwrap(),
                )
                .await;
                assert_eq!(status, StatusCode::OK, "{path}");
            }
        }
    }

    /// Closing the metadata surface must not close the data routes' own
    /// behaviour, and must never gate more than the two classes.
    #[tokio::test]
    async fn gating_classes_are_decided_by_the_mode() {
        let (data_only, _) = counting_gate(Vec::new(), Enforcement::Enforce).await;
        assert!(data_only.gates(RouteClass::Data));
        assert!(!data_only.gates(RouteClass::Metadata));

        let (everything, _) = metadata_gate(Vec::new()).await;
        assert!(everything.gates(RouteClass::Data));
        assert!(everything.gates(RouteClass::Metadata));
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

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers()[header::WWW_AUTHENTICATE],
            "Bearer",
            "a 401 must name the scheme to retry with"
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

        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(error_code(&body), "invalid_credential");
    }

    /// A refusal for want of permission is not a challenge: the credential
    /// authenticated, so re-presenting it changes nothing.
    #[tokio::test]
    async fn a_permission_refusal_carries_no_bearer_challenge() {
        let mut record = key_record("k1", 1);
        record.datasets = Some(vec!["base-mainnet".to_string()]);
        let app = app(
            gate(vec![record], Enforcement::Enforce).await,
            "/datasets/:dataset/stream",
        );

        let response = app
            .oneshot(
                request(&format!(
                    "/datasets/ethereum-mainnet/stream?{QUERY_PARAM}={TOKEN}"
                ))
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
            request(&format!("/datasets/base/stream?{QUERY_PARAM}={TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "the alias resolves to base-mainnet");

        let (status, _) = call(
            app,
            request(&format!(
                "/datasets/ethereum-mainnet/stream?{QUERY_PARAM}={TOKEN}"
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn a_dataset_scoped_key_matches_a_base64_dataset_id() {
        let mut record = key_record("k1", 1);
        record.datasets = Some(vec!["base-mainnet".to_string()]);
        let encoded = DatasetId::from_url("s3://base-mainnet").to_base64();
        let app = app(
            gate(vec![record], Enforcement::Enforce).await,
            "/datasets/:dataset_id/query/:worker_id",
        );

        let (status, _) = call(
            app,
            request(&format!(
                "/datasets/{encoded}/query/worker?{QUERY_PARAM}={TOKEN}"
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn a_route_without_a_dataset_is_closed_to_dataset_scoped_keys() {
        let mut record = key_record("k1", 1);
        record.datasets = Some(vec!["base-mainnet".to_string()]);
        let app = app(gate(vec![record], Enforcement::Enforce).await, "/sql/query");

        let (status, _) = call(
            app,
            request(&format!("/sql/query?{QUERY_PARAM}={TOKEN}"))
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

        for uri in [
            "/datasets/base/stream".to_string(),
            format!("/datasets/base/stream?{QUERY_PARAM}={TOKEN}"),
            format!("/datasets/base/stream?{QUERY_PARAM}=garbage"),
            "/datasets/base/stream?api_key=sqd_portal_unknown_secret".to_string(),
        ] {
            let (status, body) =
                call(app.clone(), request(&uri).body(Body::empty()).unwrap()).await;
            assert_eq!(status, StatusCode::OK, "{uri} must be admitted in log_only");
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
            let decision = gate.decide(&headers, &uri, DatasetSource::Alias).await;
            assert!(matches!(decision, Decision::Reject(_)));
        }
        assert_eq!(
            lookups.load(Ordering::Relaxed),
            0,
            "a request that never authenticates must not touch the dataset catalog"
        );

        // The dataset rung itself still resolves, exactly once.
        let decision = gate
            .decide(
                &header_map(&format!("Bearer {TOKEN}")),
                &uri,
                DatasetSource::Alias,
            )
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
            .decide(
                &header_map(&format!("Bearer {TOKEN}")),
                &uri,
                DatasetSource::Alias,
            )
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

        gate.decide(&HeaderMap::new(), &uri, DatasetSource::Alias)
            .await;
        assert_eq!(
            lookups.load(Ordering::Relaxed),
            0,
            "an anonymous request is free in shadow mode too"
        );

        let decision = gate
            .decide(
                &header_map(&format!("Bearer {TOKEN}")),
                &uri,
                DatasetSource::Alias,
            )
            .await;
        assert_eq!(decision, Decision::Admit);
        assert_eq!(lookups.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn log_only_still_evaluates_the_full_ladder() {
        let gate = gate(vec![key_record("k1", 1)], Enforcement::LogOnly).await;
        let uri: axum::http::Uri = "/datasets/base/stream".parse().unwrap();

        let decision = gate
            .decide(&HeaderMap::new(), &uri, DatasetSource::Alias)
            .await;

        assert!(matches!(decision, Decision::Reject(_)));
    }

    /// Everything two keyless scrapes bracketing one request could see change.
    /// Compared between cases rather than sampled: the families are global, so
    /// a timing test would race the rest of the binary.
    fn public_projection(
        gate: &Gate,
        decision: Decision,
        class: RouteClass,
    ) -> Vec<(String, String)> {
        metrics::auth_decision_labels(
            gate.public_outcome(decision),
            class.as_str(),
            gate.enforcement.as_str(),
        )
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
            let decision = gate
                .decide(&header_map(&token), &uri, DatasetSource::Alias)
                .await;
            projections.push((
                decision,
                public_projection(&gate, decision, RouteClass::Data),
            ));
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
                DatasetSource::Alias,
            )
            .await;

        let Decision::Reject(rejection) = decision else {
            panic!("a spent budget refuses");
        };
        assert_eq!(rejection.reason, "lookup_saturated");

        let projection = public_projection(&gate, decision, RouteClass::Data);
        assert_eq!(
            projection,
            metrics::auth_decision_labels(
                AuthDecision::Reject(ErrorCode::Overloaded),
                RouteClass::Data.as_str(),
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
            let decision = gate.decide(&headers, &uri, DatasetSource::Alias).await;
            projections.push(public_projection(&gate, decision, RouteClass::Data));
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
                RouteClass::Data.as_str(),
                Enforcement::Enforce.as_str(),
            )
        };
        let before = expired();

        let decision = gate
            .decide(
                &header_map(&format!("Bearer {TOKEN}")),
                &uri,
                DatasetSource::Alias,
            )
            .await;
        gate.count(decision, RouteClass::Data);

        assert!(expired() > before, "the refusal must reach the scrape");
    }
}
