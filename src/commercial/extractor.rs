use std::sync::Arc;

use axum::{
    extract::Request,
    http::{header, HeaderMap},
    middleware::Next,
    response::Response,
};
use sha2::{Digest, Sha256};
use tracing::field;
use url::form_urlencoded;

use super::{
    client::oss_grant, Authorization, AuthorizeRequest, ControlPlaneClient, Credential, Endpoint,
    Granted, QueryDescriptor,
};

const API_KEY_PREFIX: &str = "sqd_data_";

#[derive(Clone, Debug)]
pub struct CommercialGrant(pub Granted);

pub async fn middleware(mut req: Request, next: Next, endpoint: Endpoint) -> Response {
    let credential = credential_from_request(req.headers(), req.uri().query());
    let dataset = dataset_from_path(req.uri().path(), &endpoint);
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
                    query: QueryDescriptor {
                        requires_traces: false,
                        requires_statediffs: false,
                        first_block: None,
                        chain_kind: None,
                    },
                })
                .await;
            match authorization {
                Authorization::Granted(granted) => granted,
                Authorization::Rejected(rejected) => {
                    tracing::warn!(
                        reason = rejected.reason,
                        status = rejected.http_status,
                        "commercial authorization rejected during attribution-only phase"
                    );
                    oss_grant()
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
    req.extensions_mut().insert(CommercialGrant(granted));

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

fn credential_from_request(headers: &HeaderMap, query: Option<&str>) -> Credential {
    if let Some(value) = bearer_token(headers).or_else(|| query_api_key(query)) {
        if let Some((key_id, secret_sha256)) = parse_api_key_token(&value) {
            return Credential::Key {
                key_id,
                secret_sha256,
            };
        }

        if value.starts_with(API_KEY_PREFIX) {
            tracing::warn!("malformed portal API key ignored during attribution-only phase");
        }
    }

    Credential::None {
        ip_bucket: "unknown".to_string(),
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
    use axum::{
        body::{to_bytes, Body},
        http::Request as HttpRequest,
        middleware::from_fn,
        routing::get,
    };
    use tower::ServiceExt;

    use super::*;
    use crate::commercial::{build, Credential, Principal};

    const TOKEN: &str = "sqd_data_AbCdEfGhIjKlMnOp_0123456789abcdefghijklmnopqrstuv";

    #[test]
    fn parses_header_token_with_contract_hash_vector() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {TOKEN}").parse().unwrap(),
        );

        assert_eq!(
            credential_from_request(&headers, None),
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
            credential_from_request(&headers, Some(&format!("api_key={TOKEN}"))),
            Credential::Key { .. }
        ));
    }

    #[test]
    fn malformed_tokens_fall_through_to_anonymous_in_attribution_phase() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            "Bearer sqd_data_key_".parse().unwrap(),
        );

        assert_eq!(
            credential_from_request(&headers, None),
            Credential::None {
                ip_bucket: "unknown".to_string(),
            }
        );
    }

    #[test]
    fn credential_debug_redacts_secret_hash() {
        let credential =
            credential_from_request(&HeaderMap::new(), Some(&format!("api_key={TOKEN}")));
        let rendered = format!("{credential:?}");

        assert!(!rendered.contains("0123456789abcdefghijklmnopqrstuv"));
        assert!(
            !rendered.contains("73337f479fe170d73e53e247f3052e4243cc9c2a0ffa621853d9385c619efb77")
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
}
