use std::{fmt, sync::Arc};

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
    evaluate::{self, Decision, Rejection},
    now_secs,
    store::SnapshotStore,
};
use crate::{network::NetworkClient, types::DatasetId};

/// Token layouts the portal accepts, all of the form `<prefix><key_id>_<secret>`:
/// the prefix minted by the control plane, plus the legacy prefixes carried by
/// keys imported from before the portal owned authentication.
const TOKEN_PREFIXES: [&str; 3] = ["sqd_portal_", "sqd_data_", "prt_"];

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

    async fn decide(
        &self,
        headers: &HeaderMap,
        uri: &axum::http::Uri,
        source: DatasetSource,
    ) -> Decision {
        let dataset = self.dataset_for(uri.path(), source);
        let credential = match credential_from_request(headers, uri.query()) {
            Ok(credential) => credential,
            Err(rejection) => {
                self.log(Decision::Reject(rejection), None, dataset.as_deref());
                return Decision::Reject(rejection);
            }
        };

        let decision = evaluate::evaluate(
            &self.store,
            &self.portal_id,
            credential.as_ref(),
            dataset.as_deref(),
            now_secs(),
        )
        .await;
        self.log(
            decision,
            credential
                .as_ref()
                .map(|credential| credential.key_id.as_str()),
            dataset.as_deref(),
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
    let (key_id, secret) = rest.split_once('_')?;
    if key_id.is_empty() || secret.is_empty() {
        return None;
    }
    Some(Credential {
        key_id: key_id.to_owned(),
        secret_sha256: sha256_hex(secret),
    })
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
    use std::collections::HashMap;

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
    }

    impl DatasetCatalog for StaticCatalog {
        fn canonical_name(&self, alias: &str) -> Option<String> {
            self.aliases.get(alias).cloned()
        }

        fn canonical_name_for_id(&self, id: &DatasetId) -> Option<String> {
            self.ids.get(&id.to_base64()).cloned()
        }
    }

    fn gate(records: Vec<KeyRecord>, enforcement: Enforcement) -> Arc<Gate> {
        let dataset_id = DatasetId::from_url("s3://base-mainnet");
        Arc::new(Gate {
            store: store_with(records),
            catalog: Arc::new(StaticCatalog {
                aliases: HashMap::from([("base".to_string(), "base-mainnet".to_string())]),
                ids: HashMap::from([(dataset_id.to_base64(), "base-mainnet".to_string())]),
            }),
            portal_id: PORTAL.to_string(),
            enforcement,
        })
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
