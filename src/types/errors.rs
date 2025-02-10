use std::time::Duration;

use axum::http::StatusCode;
use tokio::time::Instant;

#[derive(thiserror::Error, Debug)]
pub enum RequestError {
    #[error("{0}")]
    BadRequest(String),
    #[error("No data")]
    NoData,
    #[error("{0}")]
    InternalError(String),
    #[error("Service is overloaded")]
    Busy,
    #[error("Service is overloaded")]
    BusyFor(Duration),
}

#[derive(thiserror::Error, Debug)]
pub enum QueryError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    Retriable(String),
}

#[derive(thiserror::Error, Debug)]
pub enum SendQueryError {
    #[error("No workers available")]
    NoWorkers,
    #[error("Rate limited")]
    Backoff(Instant),
}

impl From<QueryError> for RequestError {
    fn from(value: QueryError) -> Self {
        match value {
            QueryError::BadRequest(s) => RequestError::BadRequest(s),
            QueryError::Retriable(s) => RequestError::InternalError(s),
        }
    }
}

impl axum::response::IntoResponse for RequestError {
    fn into_response(self) -> axum::response::Response {
        use axum::http::header;
        let mut response = match self {
            s @ Self::BadRequest(_) => (StatusCode::BAD_REQUEST, s.to_string()).into_response(),
            Self::NoData => (StatusCode::NO_CONTENT, ()).into_response(),
            s @ Self::Busy => (StatusCode::SERVICE_UNAVAILABLE, s.to_string()).into_response(),
            s @ Self::BusyFor(duration) => axum::http::Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .header(header::RETRY_AFTER, duration.as_secs() + 1)
                .body(axum::body::Body::from(s.to_string()))
                .unwrap(),
            s @ Self::InternalError(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, s.to_string()).into_response()
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
            Self::Busy | Self::BusyFor(_) => "overloaded",
        }
    }
}
