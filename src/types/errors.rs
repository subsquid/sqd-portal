use std::time::Duration;

use axum::http::StatusCode;
use axum::response::Response;
use serde::Serialize;
use sqd_contract_client::PeerId;
use tokio::time::Instant;

/// Coarse category, after Stripe's `type`. `Api` is the only one that should page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorType {
    InvalidRequest,
    RateLimit,
    Availability,
    /// An invariant we own was violated.
    Api,
}

impl ErrorType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request_error",
            Self::RateLimit => "rate_limit_error",
            Self::Availability => "availability_error",
            Self::Api => "api_error",
        }
    }

    pub const fn retryable(self) -> bool {
        match self {
            Self::RateLimit | Self::Availability => true,
            Self::InvalidRequest | Self::Api => false,
        }
    }
}

/// Specific cause, after Stripe's `code`. [`ErrorCode::as_str`] is public API twice over:
/// the `code` error field and the `error_code` metric label. Adding a variant is safe;
/// renaming one breaks client matches and dashboards silently.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorCode {
    MalformedRequest,
    UnknownDataset,
    NotFound,
    BaseBlockMismatch,
    /// Range not served yet — the client should keep polling.
    NoData,
    Overloaded,
    NoWorkers,
    /// Workers were reachable; every attempt failed transiently until retries ran out.
    RetriesExhausted,
    UpstreamUnavailable,
    /// `/ready` declining traffic, so a routine drain doesn't read as an error.
    NotReady,
    /// A worker returned something that cannot be right.
    WorkerFailure,
    Internal,
    /// Unset by any handler. `api_error` so it pages instead of hiding; `http_labels`
    /// re-types an unclassified 4xx.
    Unclassified,
}

impl ErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MalformedRequest => "malformed_request",
            Self::UnknownDataset => "unknown_dataset",
            Self::NotFound => "not_found",
            Self::BaseBlockMismatch => "base_block_mismatch",
            Self::NoData => "no_data",
            Self::Overloaded => "overloaded",
            Self::NoWorkers => "no_workers",
            Self::RetriesExhausted => "retries_exhausted",
            Self::UpstreamUnavailable => "upstream_unavailable",
            Self::NotReady => "not_ready",
            Self::WorkerFailure => "worker_failure",
            Self::Internal => "internal_error",
            Self::Unclassified => "unclassified",
        }
    }

    pub const fn error_type(self) -> ErrorType {
        match self {
            Self::MalformedRequest
            | Self::UnknownDataset
            | Self::NotFound
            | Self::BaseBlockMismatch => ErrorType::InvalidRequest,

            Self::Overloaded => ErrorType::RateLimit,

            Self::NoData
            | Self::NoWorkers
            | Self::RetriesExhausted
            | Self::UpstreamUnavailable
            | Self::NotReady => ErrorType::Availability,

            Self::WorkerFailure | Self::Internal | Self::Unclassified => ErrorType::Api,
        }
    }

    /// For proxied responses, where the status is all we see. Lossy: hotblocks splits 400
    /// five ways but keeps the discriminant in extensions, which never reach the wire.
    pub fn from_upstream_status(status: StatusCode) -> Self {
        match status.as_u16() {
            204 => Self::NoData,
            409 => Self::BaseBlockMismatch,
            404 => Self::UnknownDataset,
            429 | 503 | 529 => Self::Overloaded,
            500..=599 => Self::UpstreamUnavailable,
            400..=499 => Self::MalformedRequest,
            _ => Self::Unclassified,
        }
    }
}

/// Error body shared by every endpoint, shaped after Stripe's.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct ErrorResponse {
    pub error: ErrorDetail,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct ErrorDetail {
    /// Coarse category to branch on.
    #[serde(rename = "type")]
    #[schema(rename = "type", example = "rate_limit_error")]
    pub error_type: &'static str,
    /// Specific cause. Stable — match on this.
    #[schema(example = "overloaded")]
    pub code: &'static str,
    /// Human-readable detail. Prose, not stable; do not parse.
    #[schema(example = "Service is overloaded, please try again later")]
    pub message: String,
    /// The request parameter at fault, when the error is about one.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "buffer_size")]
    pub param: Option<&'static str>,
    /// Echo of `x-request-id`, for support. Filled in by the logging middleware.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

impl ErrorResponse {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            error: ErrorDetail {
                error_type: code.error_type().as_str(),
                code: code.as_str(),
                message: message.into(),
                param: None,
                request_id: None,
            },
        }
    }

    pub fn with_param(mut self, param: &'static str) -> Self {
        self.error.param = Some(param);
        self
    }
}

/// The code rides in extensions so the middleware can label the metric without parsing
/// the body.
pub fn error_response(status: StatusCode, code: ErrorCode, message: impl Into<String>) -> Response {
    body_response(status, code, ErrorResponse::new(code, message))
}

/// As [`error_response`], naming the request parameter at fault.
pub fn param_error_response(
    status: StatusCode,
    code: ErrorCode,
    param: &'static str,
    message: impl Into<String>,
) -> Response {
    body_response(
        status,
        code,
        ErrorResponse::new(code, message).with_param(param),
    )
}

fn body_response(status: StatusCode, code: ErrorCode, body: ErrorResponse) -> Response {
    use axum::response::IntoResponse;
    with_error_code((status, axum::Json(body)).into_response(), code)
}

/// Tag a response whose body isn't the standard envelope: 204, and proxied upstreams.
pub fn with_error_code(mut response: Response, code: ErrorCode) -> Response {
    response.extensions_mut().insert(code);
    response
}

#[derive(thiserror::Error, Debug)]
pub enum RequestError {
    #[error("{0}")]
    BadRequest(String),
    /// Names the offending parameter, as Stripe's `param` does.
    #[error("{message}")]
    InvalidParam {
        param: &'static str,
        message: String,
    },
    #[error("No data")]
    NoData,
    #[error("{0}")]
    RetriesExhausted(String),
    /// Split from [`Self::RetriesExhausted`]: telling a client to retry our own bug
    /// wastes its time and hides ours.
    #[error("{0}")]
    Internal(String),
    #[error("{0}")]
    Failure(String),
    #[error("No available workers to serve the request")]
    Unavailable,
    #[error("Rate limit exceeded")]
    RateLimitExceeded,
    #[error("Service is overloaded, please try again later")]
    BusyFor(Duration),
    #[error("Base block mismatch")]
    BaseBlockMismatch(sqd_primitives::BlockRef),
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum QueryError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    Retriable(String),
    #[error("{0}")]
    Failure(String),
    #[error("rate limit exceeded")]
    RateLimitExceeded,
    #[error("base block mismatch")]
    BaseBlockMismatch(sqd_primitives::BlockRef),
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum SendQueryError {
    #[error("no workers for the given block are available")]
    NoWorkers,
    #[error("the rate limit has been exceeded for all workers")]
    Backoff(Instant),
}

impl RequestError {
    pub fn from_query_error(value: QueryError, worker: PeerId) -> Self {
        match value {
            QueryError::BadRequest(s) => RequestError::BadRequest(s),
            QueryError::Retriable(s) => RequestError::RetriesExhausted(format!(
                "received an error from worker {worker}: {s}"
            )),
            QueryError::Failure(s) => RequestError::Failure(format!("worker {worker} failed: {s}")),
            QueryError::RateLimitExceeded => RequestError::RateLimitExceeded,
            QueryError::BaseBlockMismatch(block_ref) => RequestError::BaseBlockMismatch(block_ref),
        }
    }

    pub fn code(&self) -> ErrorCode {
        match self {
            Self::BadRequest(_) | Self::InvalidParam { .. } => ErrorCode::MalformedRequest,
            Self::NoData => ErrorCode::NoData,
            Self::RetriesExhausted(_) => ErrorCode::RetriesExhausted,
            Self::Internal(_) => ErrorCode::Internal,
            Self::Failure(_) => ErrorCode::WorkerFailure,
            Self::Unavailable => ErrorCode::NoWorkers,
            Self::RateLimitExceeded | Self::BusyFor(_) => ErrorCode::Overloaded,
            Self::BaseBlockMismatch(_) => ErrorCode::BaseBlockMismatch,
        }
    }
}

/// Non-standard "Site is overloaded" status used to signal clients to back off.
fn server_overloaded() -> StatusCode {
    StatusCode::from_u16(529).expect("529 should be a valid status code")
}

impl axum::response::IntoResponse for RequestError {
    fn into_response(self) -> axum::response::Response {
        use axum::http::header;
        let code = self.code();

        let retry_after = match &self {
            Self::BusyFor(duration) => Some(duration.as_secs() + 1),
            Self::RateLimitExceeded => Some(1),
            _ => None,
        };

        let mut response = match self {
            // 204 must not carry a body, so the code rides only in extensions.
            Self::NoData => {
                return with_error_code((StatusCode::NO_CONTENT, ()).into_response(), code)
            }

            // `previousBlocks` stays top-level: documented reorg-recovery contract.
            Self::BaseBlockMismatch(ref block_ref) => {
                let body = serde_json::json!({
                    "error": ErrorResponse::new(code, self.to_string()).error,
                    "previousBlocks": [block_ref],
                });
                return with_error_code(
                    (StatusCode::CONFLICT, axum::Json(body)).into_response(),
                    code,
                );
            }

            Self::InvalidParam { param, ref message } => param_error_response(
                StatusCode::BAD_REQUEST,
                code,
                param,
                format!("Bad request: {message}"),
            ),
            Self::BadRequest(ref e) => {
                let message = format!("Bad request: {e}");
                error_response(StatusCode::BAD_REQUEST, code, message)
            }
            Self::Unavailable => {
                error_response(StatusCode::SERVICE_UNAVAILABLE, code, self.to_string())
            }
            Self::Failure(_) => {
                error_response(StatusCode::INTERNAL_SERVER_ERROR, code, self.to_string())
            }
            Self::Internal(ref e) => {
                let message = format!("Internal error: {e}");
                error_response(StatusCode::INTERNAL_SERVER_ERROR, code, message)
            }
            Self::RetriesExhausted(ref e) => {
                let message = format!("All query attempts failed: {e}");
                error_response(StatusCode::SERVICE_UNAVAILABLE, code, message)
            }
            Self::BusyFor(_) | Self::RateLimitExceeded => {
                error_response(server_overloaded(), code, self.to_string())
            }
        };

        if let Some(seconds) = retry_after {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, seconds.into());
        }
        response
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct GenericError {
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;

    async fn parts(response: Response) -> (StatusCode, Option<ErrorCode>, serde_json::Value) {
        let status = response.status();
        let code = response.extensions().get::<ErrorCode>().copied();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).expect("error bodies are JSON")
        };
        (status, code, body)
    }

    #[tokio::test]
    async fn every_variant_is_classified_and_carries_type_and_code() {
        let cases = [
            (
                RequestError::BadRequest("nope".into()),
                StatusCode::BAD_REQUEST,
                ErrorCode::MalformedRequest,
            ),
            (
                RequestError::RetriesExhausted("boom".into()),
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorCode::RetriesExhausted,
            ),
            (
                RequestError::Internal("bug".into()),
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::Internal,
            ),
            (
                RequestError::Failure("worker".into()),
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::WorkerFailure,
            ),
            (
                RequestError::Unavailable,
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorCode::NoWorkers,
            ),
            (
                RequestError::RateLimitExceeded,
                server_overloaded(),
                ErrorCode::Overloaded,
            ),
            (
                RequestError::BusyFor(Duration::from_secs(3)),
                server_overloaded(),
                ErrorCode::Overloaded,
            ),
        ];

        for (error, want_status, want_code) in cases {
            let (status, code, body) = parts(error.into_response()).await;
            assert_eq!(status, want_status, "{want_code:?}");
            assert_eq!(code, Some(want_code), "code must reach the middleware");
            assert_eq!(body["error"]["code"], want_code.as_str());
            assert_eq!(body["error"]["type"], want_code.error_type().as_str());
            assert!(
                body["error"]["message"]
                    .as_str()
                    .is_some_and(|m| !m.is_empty()),
                "{want_code:?} must explain itself"
            );
        }
    }

    #[tokio::test]
    async fn a_bad_parameter_names_itself() {
        let error = RequestError::InvalidParam {
            param: "buffer_size",
            message: "buffer_size must be greater than 0".into(),
        };
        let (status, code, body) = parts(error.into_response()).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(code, Some(ErrorCode::MalformedRequest));
        assert_eq!(body["error"]["param"], "buffer_size");
        assert_eq!(body["error"]["type"], "invalid_request_error");
    }

    /// `param` is absent, not null, when the error isn't about a parameter.
    #[tokio::test]
    async fn optional_fields_are_omitted_when_unset() {
        let (_, _, body) = parts(RequestError::Unavailable.into_response()).await;
        let error = body["error"].as_object().unwrap();
        assert!(
            !error.contains_key("param"),
            "param must be omitted: {error:?}"
        );
        assert!(!error.contains_key("request_id"));
    }

    #[tokio::test]
    async fn no_data_is_classified_but_bodiless() {
        let (status, code, body) = parts(RequestError::NoData.into_response()).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert_eq!(code, Some(ErrorCode::NoData));
        assert_eq!(body, serde_json::Value::Null);
    }

    /// `previousBlocks` is what clients walk back to find a shared ancestor after a
    /// reorg. It stays top-level; the error envelope is added beside it.
    #[tokio::test]
    async fn conflict_keeps_previous_blocks_top_level() {
        let block = sqd_primitives::BlockRef {
            number: 42,
            hash: "0xdead".to_owned(),
        };
        let (status, code, body) =
            parts(RequestError::BaseBlockMismatch(block).into_response()).await;

        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(code, Some(ErrorCode::BaseBlockMismatch));
        assert_eq!(body["previousBlocks"][0]["number"], 42);
        assert_eq!(body["previousBlocks"][0]["hash"], "0xdead");
        assert_eq!(body["error"]["code"], "base_block_mismatch");
        assert_eq!(body["error"]["type"], "invalid_request_error");
    }

    #[tokio::test]
    async fn overload_tells_the_client_how_long_to_wait() {
        let response = RequestError::BusyFor(Duration::from_secs(3)).into_response();
        assert_eq!(response.headers()[axum::http::header::RETRY_AFTER], "4");

        let response = RequestError::RateLimitExceeded.into_response();
        assert_eq!(response.headers()[axum::http::header::RETRY_AFTER], "1");
    }

    /// The point of the split: a client that hits our bug must not be told to retry.
    #[test]
    fn retryability_follows_the_type() {
        assert!(ErrorCode::RetriesExhausted.error_type().retryable());
        assert!(ErrorCode::Overloaded.error_type().retryable());
        assert!(!ErrorCode::Internal.error_type().retryable());
        assert!(!ErrorCode::WorkerFailure.error_type().retryable());
        assert!(!ErrorCode::MalformedRequest.error_type().retryable());
    }

    /// Only api_error should page, so an unclassified response must land there.
    #[test]
    fn unclassified_is_an_api_error() {
        assert_eq!(ErrorCode::Unclassified.error_type(), ErrorType::Api);
    }

    #[test]
    fn upstream_statuses_map_onto_the_taxonomy() {
        use ErrorCode::*;
        let cases = [
            (204, NoData),
            (400, MalformedRequest),
            (404, UnknownDataset),
            (409, BaseBlockMismatch),
            (503, Overloaded),
            (529, Overloaded),
            (500, UpstreamUnavailable),
        ];
        for (status, want) in cases {
            let got = ErrorCode::from_upstream_status(StatusCode::from_u16(status).unwrap());
            assert_eq!(got, want, "upstream {status}");
        }
    }

    /// Clients match on these and dashboards group by them; a rename is a silent
    /// breaking change. Update only alongside that migration.
    #[test]
    fn the_wire_vocabulary_is_frozen() {
        use ErrorCode::*;
        let expected = [
            (
                MalformedRequest,
                "malformed_request",
                "invalid_request_error",
            ),
            (UnknownDataset, "unknown_dataset", "invalid_request_error"),
            (NotFound, "not_found", "invalid_request_error"),
            (
                BaseBlockMismatch,
                "base_block_mismatch",
                "invalid_request_error",
            ),
            (NoData, "no_data", "availability_error"),
            (Overloaded, "overloaded", "rate_limit_error"),
            (NoWorkers, "no_workers", "availability_error"),
            (RetriesExhausted, "retries_exhausted", "availability_error"),
            (
                UpstreamUnavailable,
                "upstream_unavailable",
                "availability_error",
            ),
            (NotReady, "not_ready", "availability_error"),
            (WorkerFailure, "worker_failure", "api_error"),
            (Internal, "internal_error", "api_error"),
            (Unclassified, "unclassified", "api_error"),
        ];
        for (code, want_code, want_type) in expected {
            assert_eq!(code.as_str(), want_code);
            assert_eq!(code.error_type().as_str(), want_type);
        }
    }
}
