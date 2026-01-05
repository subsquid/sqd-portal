use std::time::Duration;

use axum::http::StatusCode;
use serde::Serialize;
use sqd_contract_client::PeerId;
use tokio::time::Instant;

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
        }
    }
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
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .header(header::RETRY_AFTER, duration.as_secs() + 1)
                .body(axum::body::Body::from(s.to_string()))
                .unwrap(),

            s @ Self::RateLimitExceeded => axum::http::Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .header(header::RETRY_AFTER, 1)
                .body(axum::body::Body::from(s.to_string()))
                .unwrap(),

            Self::InternalError(e) => (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("All query attempts failed, last error: {e}"),
            )
                .into_response(),
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
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct GenericError {
    pub message: String,
}
