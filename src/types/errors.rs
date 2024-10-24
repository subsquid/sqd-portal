use axum::http::StatusCode;

#[derive(thiserror::Error, Debug)]
pub enum RequestError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    InternalError(String),
    #[error("Service is overloaded")]
    Busy,
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
    #[error("Transport queue full")]
    TransportQueueFull,
    #[error("No workers available")]
    NoWorkers,
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
        match self {
            s @ Self::BadRequest(_) => (StatusCode::BAD_REQUEST, s.to_string()).into_response(),
            s @ Self::NotFound(_) => (StatusCode::NOT_FOUND, s.to_string()).into_response(),
            s @ Self::Busy => (StatusCode::SERVICE_UNAVAILABLE, s.to_string()).into_response(),
            s @ Self::InternalError(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, s.to_string()).into_response()
            }
        }
    }
}

impl RequestError {
    pub fn short_code(&self) -> &'static str {
        match self {
            Self::BadRequest(_) => "bad_request",
            Self::NotFound(_) => "not_found",
            Self::InternalError(_) => "internal_error",
            Self::Busy => "overloaded",
        }
    }
}
