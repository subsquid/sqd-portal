use std::time::Duration;

use axum::http::{HeaderValue, StatusCode};
use serde::Serialize;
use sqd_contract_client::PeerId;
use tokio::time::Instant;

use crate::commercial::Rejected;

#[derive(thiserror::Error, Debug)]
pub enum RequestError {
    #[error("{0}")]
    BadRequest(String),
    #[error("No data")]
    NoData,
    #[error("{0}")]
    InternalError(String),
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
    #[error("{0}")]
    Unauthorized(String),
    #[error("{message}")]
    PaymentRequired {
        message: String,
        retry_after: Option<u64>,
    },
    #[error("{0}")]
    Forbidden(String),
    #[error("{message}")]
    TooManyRequests {
        message: String,
        retry_after: Option<u64>,
    },
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
            QueryError::Retriable(s) => {
                RequestError::InternalError(format!("received an error from worker {worker}: {s}"))
            }
            QueryError::Failure(s) => RequestError::Failure(format!("worker {worker} failed: {s}")),
            QueryError::RateLimitExceeded => RequestError::RateLimitExceeded,
            QueryError::BaseBlockMismatch(block_ref) => RequestError::BaseBlockMismatch(block_ref),
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
        let mut response = match self {
            Self::BadRequest(e) => {
                (StatusCode::BAD_REQUEST, format!("Bad request: {e}")).into_response()
            }

            Self::NoData => (StatusCode::NO_CONTENT, ()).into_response(),

            s @ Self::Unavailable => {
                (StatusCode::SERVICE_UNAVAILABLE, s.to_string()).into_response()
            }

            s @ Self::Failure(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, s.to_string()).into_response()
            }

            s @ Self::BusyFor(duration) => axum::http::Response::builder()
                .status(server_overloaded())
                .header(header::RETRY_AFTER, duration.as_secs() + 1)
                .body(axum::body::Body::from(s.to_string()))
                .unwrap(),

            s @ Self::RateLimitExceeded => axum::http::Response::builder()
                .status(server_overloaded())
                .header(header::RETRY_AFTER, 1)
                .body(axum::body::Body::from(s.to_string()))
                .unwrap(),

            Self::InternalError(e) => (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("All query attempts failed, last error: {e}"),
            )
                .into_response(),

            Self::BaseBlockMismatch(block_ref) => {
                let body = serde_json::json!({
                    "previousBlocks": [block_ref],
                });
                return (StatusCode::CONFLICT, axum::Json(body)).into_response();
            }

            Self::Unauthorized(message) => (StatusCode::UNAUTHORIZED, message).into_response(),

            Self::PaymentRequired {
                message,
                retry_after,
            } => {
                let mut response = (StatusCode::PAYMENT_REQUIRED, message).into_response();
                if let Some(retry_after) = retry_after {
                    response.headers_mut().insert(
                        header::RETRY_AFTER,
                        HeaderValue::from_str(&retry_after.to_string()).unwrap(),
                    );
                }
                response
            }

            Self::Forbidden(message) => (StatusCode::FORBIDDEN, message).into_response(),

            Self::TooManyRequests {
                message,
                retry_after,
            } => {
                let mut response = (StatusCode::TOO_MANY_REQUESTS, message).into_response();
                if let Some(retry_after) = retry_after {
                    response.headers_mut().insert(
                        header::RETRY_AFTER,
                        HeaderValue::from_str(&retry_after.to_string()).unwrap(),
                    );
                }
                response
            }
        };
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        response
    }
}

impl RequestError {
    pub fn short_code(&self) -> &'static str {
        match self {
            Self::BadRequest(_) => "bad_request",
            Self::NoData => "no_data",
            Self::InternalError(_) => "internal_error",
            Self::Failure(_) => "failure",
            Self::Unavailable | Self::BusyFor(_) | Self::RateLimitExceeded => "overloaded",
            Self::BaseBlockMismatch(_) => "base_block_mismatch",
            Self::Unauthorized(_) => "unauthorized",
            Self::PaymentRequired { .. } => "payment_required",
            Self::Forbidden(_) => "forbidden",
            Self::TooManyRequests { .. } => "too_many_requests",
        }
    }
}

impl From<Rejected> for RequestError {
    fn from(rejected: Rejected) -> Self {
        match rejected.http_status {
            401 => Self::Unauthorized(rejected.message),
            402 => Self::PaymentRequired {
                message: rejected.message,
                retry_after: rejected.retry_after_secs,
            },
            403 => Self::Forbidden(rejected.message),
            429 => Self::TooManyRequests {
                message: rejected.message,
                retry_after: rejected.retry_after_secs,
            },
            _ => Self::Forbidden(rejected.message),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct GenericError {
    pub message: String,
}
