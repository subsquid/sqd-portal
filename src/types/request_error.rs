use axum::http::StatusCode;
use sqd_messages::query_result;

#[derive(thiserror::Error, Debug)]
pub enum RequestError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NoData(String),
    #[error("{0}")]
    InternalError(String),
    #[error("Service is overloaded")]
    Busy,
}

#[derive(thiserror::Error, Debug)]
pub enum SendQueryError {
    #[error("Transport queue full")]
    TransportQueueFull,
    #[error("No workers available")]
    NoWorkers,
}

impl TryFrom<query_result::Result> for RequestError {
    type Error = ();

    fn try_from(value: query_result::Result) -> Result<Self, Self::Error> {
        match value {
            query_result::Result::Ok(_) => Err(()),
            query_result::Result::BadRequest(e) => Ok(Self::BadRequest(e)),
            query_result::Result::ServerError(e) => Ok(Self::InternalError(e)),
            query_result::Result::NoAllocation(()) => {
                Ok(Self::InternalError("Not enough CU allocated".to_string()))
            }
            query_result::Result::TimeoutV1(()) => {
                Ok(Self::InternalError("Query timed out".to_string()))
            }
            query_result::Result::Timeout(e) => {
                Ok(Self::InternalError(format!("Query timed out: {}", e)))
            }
        }
    }
}

impl axum::response::IntoResponse for RequestError {
    fn into_response(self) -> axum::response::Response {
        match self {
            s @ Self::BadRequest(_) => (StatusCode::BAD_REQUEST, s.to_string()).into_response(),
            s @ Self::NoData(_) => (StatusCode::NO_CONTENT, s.to_string()).into_response(),
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
            Self::NoData(_) => "no_data",
            Self::InternalError(_) => "internal_error",
            Self::Busy => "overloaded",
        }
    }
}
