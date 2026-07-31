use std::{
    fmt,
    sync::{Arc, Mutex},
};

use axum::{
    extract::Request,
    http::{header, HeaderMap},
    middleware::Next,
    response::Response,
};
use sha2::{Digest, Sha256};
use url::form_urlencoded;

use super::{
    config::{CommercialConfig, Enforcement},
    evaluate::{self, DatasetResolver, Decision, Rejection},
    now_secs,
    store::SnapshotStore,
};
use crate::{network::NetworkClient, types::DatasetId};

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

    pub fn enforcement(&self) -> Enforcement {
        self.enforcement
    }

    /// Whether the portal has mirrored the control plane's key set yet. A gated
    /// portal that has not knows no keys, so it would answer 401 to every valid
    /// one — it belongs out of rotation until this turns true.
    pub fn snapshot_ready(&self) -> bool {
        self.store.is_ready()
    }

    async fn decide(
        &self,
        headers: &HeaderMap,
        uri: &axum::http::Uri,
        source: DatasetSource,
    ) -> Decision {
        let dataset = RequestDataset {
            gate: self,
            path: uri.path(),
            source,
            resolved: Mutex::new(None),
        };
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

    /// Log-only mode records every request; enforcing mode records only the
    /// requests it turns away, since admissions are the hot path.
    fn log(&self, decision: Decision, key_id: Option<&str>, dataset: Option<&str>) {
        let key_id = key_id.unwrap_or("none");
        let dataset = dataset.unwrap_or("-");
        let portal_id = self.portal_id.as_str();
        match (decision, self.enforcement) {
            (Decision::Admit, Enforcement::Enforce) => {}
            (Decision::Admit, Enforcement::LogOnly) => tracing::info!(
                key_id,
                dataset,
                portal_id,
                decision = "admit",
                reason = "authorized",
                enforcement = "log_only",
                "commercial authorization"
            ),
            (Decision::Reject(rejection), Enforcement::LogOnly) => tracing::warn!(
                key_id,
                dataset,
                portal_id,
                decision = "would_reject",
                reason = rejection.reason,
                status = rejection.status.as_u16(),
                enforcement = "log_only",
                "commercial authorization"
            ),
            (Decision::Reject(rejection), Enforcement::Enforce) => tracing::warn!(
                key_id,
                dataset,
                portal_id,
                decision = "reject",
                reason = rejection.reason,
                status = rejection.status.as_u16(),
                enforcement = "enforce",
                "commercial authorization"
            ),
        }
    }
}

/// The dataset one request names, resolved at most once and only if a rung of
/// the ladder asks for it. Canonicalization interns the name in a process-wide
/// pool and clones the dataset config, so it must stay behind authentication.
struct RequestDataset<'a> {
    gate: &'a Gate,
    path: &'a str,
    source: DatasetSource,
    resolved: Mutex<Option<Option<String>>>,
}

impl DatasetResolver for RequestDataset<'_> {
    fn resolve(&self) -> Option<String> {
        let mut resolved = self.resolved.lock().unwrap();
        resolved
            .get_or_insert_with(|| self.gate.dataset_for(self.path, self.source))
            .clone()
    }

    fn resolved(&self) -> Option<String> {
        self.resolved.lock().unwrap().clone().flatten()
    }
}

pub async fn middleware(
    gate: Arc<Gate>,
    source: DatasetSource,
    req: Request,
    next: Next,
) -> Response {
    let decision = gate.decide(req.headers(), req.uri(), source).await;
    match (decision, gate.enforcement()) {
        (Decision::Reject(rejection), Enforcement::Enforce) => rejection.into_response(),
        _ => next.run(req).await,
    }
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
    use crate::commercial::{
        test_support::{key_record, store_with, SECRET, SECRET_SHA256},
        types::KeyRecord,
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

    fn gate(records: Vec<KeyRecord>, enforcement: Enforcement) -> Arc<Gate> {
        counting_gate(records, enforcement).0
    }

    fn counting_gate(
        records: Vec<KeyRecord>,
        enforcement: Enforcement,
    ) -> (Arc<Gate>, Arc<AtomicUsize>) {
        let dataset_id = DatasetId::from_url("s3://base-mainnet");
        let lookups = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Gate {
            store: store_with(records),
            catalog: Arc::new(StaticCatalog {
                aliases: HashMap::from([("base".to_string(), "base-mainnet".to_string())]),
                ids: HashMap::from([(dataset_id.to_base64(), "base-mainnet".to_string())]),
                lookups: lookups.clone(),
            }),
            portal_id: PORTAL.to_string(),
            enforcement,
        });
        (gate, lookups)
    }

    fn app(gate: Arc<Gate>, path: &str, source: DatasetSource) -> Router {
        Router::new().route(
            path,
            post(|| async { "served" }).route_layer(from_fn(move |req, next| {
                middleware(gate.clone(), source, req, next)
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
        let gate = gate(vec![key_record("k1", 1)], Enforcement::Enforce);
        let app = app(gate, "/datasets/:dataset/stream", DatasetSource::Alias);

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

    #[tokio::test]
    async fn a_request_without_a_key_is_rejected_as_json() {
        let gate = gate(vec![key_record("k1", 1)], Enforcement::Enforce);
        let app = app(gate, "/datasets/:dataset/stream", DatasetSource::Alias);

        let (status, body) = call(
            app,
            request("/datasets/base/stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body, r#"{"message":"API key required"}"#);
    }

    #[tokio::test]
    async fn a_wrong_secret_is_rejected_before_the_handler_runs() {
        let gate = gate(vec![key_record("k1", 1)], Enforcement::Enforce);
        let app = app(gate, "/datasets/:dataset/stream", DatasetSource::Alias);

        let (status, body) = call(
            app,
            request("/datasets/base/stream")
                .header(header::AUTHORIZATION, "Bearer sqd_portal_k1_wrong")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body, r#"{"message":"Invalid API key"}"#);
    }

    #[tokio::test]
    async fn a_dataset_scoped_key_matches_the_canonical_name_behind_an_alias() {
        let mut record = key_record("k1", 1);
        record.datasets = Some(vec!["base-mainnet".to_string()]);
        let app = app(
            gate(vec![record], Enforcement::Enforce),
            "/datasets/:dataset/stream",
            DatasetSource::Alias,
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
            gate(vec![record], Enforcement::Enforce),
            "/datasets/:dataset_id/query/:worker_id",
            DatasetSource::EncodedId,
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
        let app = app(
            gate(vec![record], Enforcement::Enforce),
            "/sql/query",
            DatasetSource::Absent,
        );

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
            gate(vec![record], Enforcement::LogOnly),
            "/datasets/:dataset/stream",
            DatasetSource::Alias,
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
        let (gate, lookups) = counting_gate(vec![record], Enforcement::Enforce);
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
        let (gate, lookups) = counting_gate(vec![key_record("k1", 1)], Enforcement::Enforce);
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
        let (gate, lookups) = counting_gate(vec![key_record("k1", 1)], Enforcement::LogOnly);
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
        let gate = gate(vec![key_record("k1", 1)], Enforcement::LogOnly);
        let uri: axum::http::Uri = "/datasets/base/stream".parse().unwrap();

        let decision = gate
            .decide(&HeaderMap::new(), &uri, DatasetSource::Alias)
            .await;

        assert!(matches!(decision, Decision::Reject(_)));
    }
}
