use std::{net::IpAddr, sync::Arc};

use axum::{
    body::{to_bytes, Body},
    extract::Request,
    http::{header, HeaderMap, Method},
    middleware::Next,
    response::{IntoResponse, Response},
};
use sha2::{Digest, Sha256};
use tracing::field;
use url::form_urlencoded;

use super::{
    client::oss_grant, ActiveStreamRegistry, Authorization, AuthorizeRequest, ControlPlaneClient,
    Credential, Endpoint, Granted, QueryDescriptor, Rejected, TallyStore,
};
use crate::{
    config::Config,
    metrics,
    types::{ParsedQuery, RequestError},
};

const API_KEY_PREFIX: &str = "sqd_data_";

#[derive(Clone, Debug)]
pub struct CommercialGrant {
    pub granted: Granted,
    pub tally: Option<Arc<TallyStore>>,
    pub registry: Option<Arc<ActiveStreamRegistry>>,
}

pub async fn middleware(mut req: Request, next: Next, endpoint: Endpoint) -> Response {
    let client_ip_header = req
        .extensions()
        .get::<Arc<Config>>()
        .and_then(|config| {
            config
                .commercial
                .as_ref()
                .map(|commercial| commercial.client_ip_header.as_str())
        })
        .unwrap_or("x-forwarded-for");
    let credential =
        match credential_from_request(req.headers(), req.uri().query(), client_ip_header) {
            Ok(credential) => credential,
            Err(rejected) => {
                metrics::report_commercial_authorize("rejected", &rejected.reason);
                return RequestError::from(rejected).into_response();
            }
        };
    let dataset = dataset_from_path(req.uri().path(), &endpoint);
    let query = match query_descriptor_from_request(req, endpoint).await {
        Ok((next_req, query)) => {
            req = next_req;
            query
        }
        Err(response) => return response,
    };
    let key_id = match &credential {
        Credential::Key { key_id, .. } => key_id.clone(),
        _ => "-".to_string(),
    };
    let granted = match req
        .extensions()
        .get::<Arc<dyn ControlPlaneClient>>()
        .cloned()
    {
        Some(client) => {
            let authorization = client
                .authorize(AuthorizeRequest {
                    credential,
                    dataset,
                    endpoint,
                    query,
                })
                .await;
            match authorization {
                Authorization::Granted(granted) => granted,
                Authorization::Rejected(rejected) => {
                    tracing::warn!(
                        reason = rejected.reason,
                        status = rejected.http_status,
                        "commercial authorization rejected"
                    );
                    return RequestError::from(rejected).into_response();
                }
            }
        }
        None => {
            tracing::warn!("commercial control-plane client extension missing; falling back to OSS attribution");
            oss_grant()
        }
    };

    tracing::Span::current().record("key_id", field::display(&key_id));
    tracing::Span::current().record("account_id", field::display(&granted.principal.account_id));
    req.extensions_mut().insert(granted.principal.clone());
    let tally = req.extensions().get::<Arc<TallyStore>>().cloned();
    let registry = req.extensions().get::<Arc<ActiveStreamRegistry>>().cloned();
    req.extensions_mut().insert(CommercialGrant {
        granted,
        tally,
        registry,
    });

    next.run(req).await
}

pub fn parse_api_key_token(token: &str) -> Option<(String, String)> {
    let rest = token.strip_prefix(API_KEY_PREFIX)?;
    let (key_id, secret) = rest.split_once('_')?;
    if key_id.is_empty() || secret.is_empty() {
        return None;
    }

    Some((key_id.to_string(), secret_sha256(secret)))
}

fn credential_from_request(
    headers: &HeaderMap,
    query: Option<&str>,
    client_ip_header: &str,
) -> Result<Credential, Rejected> {
    if let Some(value) = bearer_token(headers).or_else(|| query_api_key(query)) {
        if let Some((key_id, secret_sha256)) = parse_api_key_token(&value) {
            return Ok(Credential::Key {
                key_id,
                secret_sha256,
            });
        }

        if value.starts_with(API_KEY_PREFIX) {
            tracing::warn!("malformed portal API key rejected");
            return Err(invalid_key());
        }
    }

    Ok(Credential::None {
        ip_bucket: ip_bucket_from_headers(headers, client_ip_header),
    })
}

async fn query_descriptor_from_request(
    req: Request,
    endpoint: Endpoint,
) -> Result<(Request, QueryDescriptor), Response> {
    if req.method() != Method::POST || !endpoint_uses_stream_query(endpoint) {
        return Ok((req, empty_query_descriptor()));
    }

    let limit = req
        .extensions()
        .get::<Arc<Config>>()
        .map(|config| usize::try_from(config.query_size_limit).unwrap_or(usize::MAX))
        .unwrap_or(usize::MAX);
    let (parts, body) = req.into_parts();
    let bytes = match to_bytes(body, limit).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return Err(RequestError::BadRequest("Query is too large".to_string()).into_response());
        }
    };
    let raw = match std::str::from_utf8(&bytes) {
        Ok(raw) => raw.to_string(),
        Err(err) => {
            return Err(
                RequestError::BadRequest(format!("Couldn't parse query: {err}")).into_response(),
            );
        }
    };
    let query = match ParsedQuery::try_from(raw) {
        Ok(query) => query,
        Err(err) => return Err(RequestError::BadRequest(format!("{err:#}")).into_response()),
    };
    let descriptor = QueryDescriptor {
        requires_traces: query.requires_traces(),
        requires_statediffs: query.requires_statediffs(),
        first_block: Some(query.first_block()),
        chain_kind: Some(query.chain_kind().to_string()),
    };

    Ok((Request::from_parts(parts, Body::from(bytes)), descriptor))
}

fn endpoint_uses_stream_query(endpoint: Endpoint) -> bool {
    matches!(
        endpoint,
        Endpoint::Stream
            | Endpoint::FinalizedStream
            | Endpoint::ArchivalStream
            | Endpoint::LegacyQuery
    )
}

fn empty_query_descriptor() -> QueryDescriptor {
    QueryDescriptor {
        requires_traces: false,
        requires_statediffs: false,
        first_block: None,
        chain_kind: None,
    }
}

fn invalid_key() -> Rejected {
    Rejected {
        reason: "invalid_key".to_string(),
        http_status: 401,
        message: "Invalid API key".to_string(),
        retry_after_secs: None,
    }
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    if scheme != "Bearer" || token.contains(' ') || token.is_empty() {
        return None;
    }

    Some(token.to_string())
}

fn query_api_key(query: Option<&str>) -> Option<String> {
    form_urlencoded::parse(query?.as_bytes())
        .find(|(key, _)| key == "api_key")
        .map(|(_, value)| value.into_owned())
}

fn ip_bucket_from_headers(headers: &HeaderMap, client_ip_header: &str) -> String {
    let Some(ip) = headers
        .get(client_ip_header)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.rsplit(',').find_map(|part| parse_ip(part.trim())))
    else {
        return "local".to_string();
    };

    match ip {
        IpAddr::V4(ip) => format!("{ip}/32"),
        IpAddr::V6(ip) => {
            let segments = ip.segments();
            format!(
                "{:x}:{:x}:{:x}:{:x}::/64",
                segments[0], segments[1], segments[2], segments[3]
            )
        }
    }
}

fn parse_ip(value: &str) -> Option<IpAddr> {
    value.parse().ok()
}

fn secret_sha256(secret: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    hex::encode(hasher.finalize())
}

fn dataset_from_path(path: &str, endpoint: &Endpoint) -> String {
    if matches!(endpoint, Endpoint::SqlQuery) {
        return "sql".to_string();
    }

    let mut segments = path.trim_start_matches('/').split('/');
    match (segments.next(), segments.next()) {
        (Some("datasets"), Some(dataset)) if !dataset.is_empty() => dataset.to_string(),
        _ => "unknown".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use axum::{
        body::{to_bytes, Body},
        http::Request as HttpRequest,
        middleware::from_fn,
        routing::{get, post},
    };
    use tower::ServiceExt;

    use super::*;
    use crate::commercial::{build, Credential, Principal};

    const TOKEN: &str = "sqd_data_AbCdEfGhIjKlMnOp_0123456789abcdefghijklmnopqrstuv";

    struct RejectingControlPlane;

    #[derive(Default)]
    struct RecordingControlPlane {
        requests: Mutex<Vec<AuthorizeRequest>>,
    }

    #[async_trait]
    impl ControlPlaneClient for RejectingControlPlane {
        async fn authorize(&self, _req: AuthorizeRequest) -> Authorization {
            Authorization::Rejected(Rejected {
                reason: "quota_exhausted".to_string(),
                http_status: 402,
                message: "quota exhausted".to_string(),
                retry_after_secs: Some(7),
            })
        }
    }

    #[async_trait]
    impl ControlPlaneClient for RecordingControlPlane {
        async fn authorize(&self, req: AuthorizeRequest) -> Authorization {
            self.requests.lock().unwrap().push(req);
            Authorization::Granted(oss_grant())
        }
    }

    #[test]
    fn parses_header_token_with_contract_hash_vector() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {TOKEN}").parse().unwrap(),
        );

        assert_eq!(
            credential_from_request(&headers, None, "x-forwarded-for").unwrap(),
            Credential::Key {
                key_id: "AbCdEfGhIjKlMnOp".to_string(),
                secret_sha256: "73337f479fe170d73e53e247f3052e4243cc9c2a0ffa621853d9385c619efb77"
                    .to_string(),
            }
        );
    }

    #[test]
    fn parses_query_token() {
        let headers = HeaderMap::new();

        assert!(matches!(
            credential_from_request(
                &headers,
                Some(&format!("api_key={TOKEN}")),
                "x-forwarded-for"
            )
            .unwrap(),
            Credential::Key { .. }
        ));
    }

    #[test]
    fn malformed_tokens_reject_as_invalid_key() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            "Bearer sqd_data_key_".parse().unwrap(),
        );

        let rejected = credential_from_request(&headers, None, "x-forwarded-for").unwrap_err();
        assert_eq!(rejected.reason, "invalid_key");
        assert_eq!(rejected.http_status, 401);
    }

    #[test]
    fn credential_debug_redacts_secret_hash() {
        let credential = credential_from_request(
            &HeaderMap::new(),
            Some(&format!("api_key={TOKEN}")),
            "x-forwarded-for",
        )
        .unwrap();
        let rendered = format!("{credential:?}");

        assert!(!rendered.contains("0123456789abcdefghijklmnopqrstuv"));
        assert!(
            !rendered.contains("73337f479fe170d73e53e247f3052e4243cc9c2a0ffa621853d9385c619efb77")
        );
    }

    #[test]
    fn keyless_requests_bucket_by_rightmost_forwarded_ip() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            "203.0.113.9, 2001:db8:abcd:12:3456::1".parse().unwrap(),
        );

        assert_eq!(
            credential_from_request(&headers, None, "x-forwarded-for").unwrap(),
            Credential::None {
                ip_bucket: "2001:db8:abcd:12::/64".to_string(),
            }
        );

        headers.insert("x-client-ip", "198.51.100.7".parse().unwrap());
        assert_eq!(
            credential_from_request(&headers, None, "x-client-ip").unwrap(),
            Credential::None {
                ip_bucket: "198.51.100.7/32".to_string(),
            }
        );
    }

    #[test]
    fn keyless_requests_without_forwarded_header_use_local_bucket() {
        assert_eq!(
            credential_from_request(&HeaderMap::new(), None, "x-forwarded-for").unwrap(),
            Credential::None {
                ip_bucket: "local".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn middleware_attaches_oss_principal_without_changing_response() {
        let runtime = build(None, tokio_util::sync::CancellationToken::new());
        let app = axum::Router::new()
            .route(
                "/datasets/ethereum-mainnet/stream",
                get(
                    |axum::Extension(principal): axum::Extension<Principal>| async move {
                        principal.account_id
                    },
                ),
            )
            .route_layer(from_fn(|req, next| middleware(req, next, Endpoint::Stream)))
            .layer(axum::Extension(runtime.control_plane));

        let response = app
            .oneshot(
                HttpRequest::builder()
                    .uri(format!("/datasets/ethereum-mainnet/stream?api_key={TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::OK);
    }

    #[tokio::test]
    async fn middleware_falls_back_to_oss_when_client_extension_is_missing() {
        let app = axum::Router::new()
            .route(
                "/datasets/ethereum-mainnet/stream",
                get(
                    |axum::Extension(principal): axum::Extension<Principal>| async move {
                        principal.account_id
                    },
                ),
            )
            .route_layer(from_fn(|req, next| middleware(req, next, Endpoint::Stream)));

        let response = app
            .oneshot(
                HttpRequest::builder()
                    .uri(format!("/datasets/ethereum-mainnet/stream?api_key={TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"oss");
    }

    #[tokio::test]
    async fn middleware_maps_rejections_to_http_response() {
        let app = axum::Router::new()
            .route(
                "/datasets/ethereum-mainnet/stream",
                get(|| async move { "should not run" }),
            )
            .route_layer(from_fn(|req, next| middleware(req, next, Endpoint::Stream)))
            .layer(axum::Extension(
                Arc::new(RejectingControlPlane) as Arc<dyn ControlPlaneClient>
            ));

        let response = app
            .oneshot(
                HttpRequest::builder()
                    .uri(format!("/datasets/ethereum-mainnet/stream?api_key={TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::PAYMENT_REQUIRED);
        assert_eq!(response.headers()["retry-after"], "7");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"quota exhausted");
    }

    #[tokio::test]
    async fn middleware_authorizes_with_stream_query_descriptor_and_reinserts_body() {
        let control_plane = Arc::new(RecordingControlPlane::default());
        let app = axum::Router::new()
            .route(
                "/datasets/ethereum-mainnet/stream",
                post(|body: String| async move { body }),
            )
            .route_layer(from_fn(|req, next| middleware(req, next, Endpoint::Stream)))
            .layer(axum::Extension(
                control_plane.clone() as Arc<dyn ControlPlaneClient>
            ));
        let body = r#"{
            "type": "evm",
            "fromBlock": 100,
            "toBlock": 101,
            "fields": {"block": {"number": true}},
            "traces": [{}]
        }"#;

        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri(format!("/datasets/ethereum-mainnet/stream?api_key={TOKEN}"))
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let echoed = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&echoed[..], body.as_bytes());

        let requests = control_plane.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].dataset, "ethereum-mainnet");
        assert_eq!(requests[0].query.first_block, Some(100));
        assert_eq!(requests[0].query.chain_kind.as_deref(), Some("evm"));
        assert!(requests[0].query.requires_traces);
        assert!(!requests[0].query.requires_statediffs);
    }
}
