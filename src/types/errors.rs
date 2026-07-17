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
    /// Not `NotReady::NoWorkers`: workers are known, none could be leased for this chunk.
    #[error("No available workers to serve the request")]
    NoAvailableWorkers,
    #[error("Too many concurrent streams, please try again later")]
    TooManyStreams,
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
pub(crate) fn server_overloaded() -> StatusCode {
    StatusCode::from_u16(529).expect("529 should be a valid status code")
}

impl RequestError {
    /// `Some` for the overload variants, which are the ones a client should retry
    /// on a timer. Clients that get no hint fall back to their own schedule.
    ///
    /// Matched exhaustively on purpose: a new variant that means "overloaded" must
    /// not inherit `None` by default — that omission is what made the cap's 503s
    /// retry every 10ms.
    pub fn retry_after_secs(&self) -> Option<u64> {
        match self {
            Self::TooManyStreams | Self::RateLimitExceeded => Some(1),
            Self::BusyFor(duration) => Some(duration.as_secs().saturating_add(1)),
            Self::BadRequest(_)
            | Self::NoData
            | Self::InternalError(_)
            | Self::Failure(_)
            | Self::NoAvailableWorkers
            | Self::BaseBlockMismatch(_) => None,
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

            s @ Self::NoAvailableWorkers => {
                (StatusCode::SERVICE_UNAVAILABLE, s.to_string()).into_response()
            }

            s @ Self::Failure(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, s.to_string()).into_response()
            }

            s @ (Self::BusyFor(_) | Self::TooManyStreams | Self::RateLimitExceeded) => {
                let retry_after = s
                    .retry_after_secs()
                    .expect("overload variants must define a retry hint");
                axum::http::Response::builder()
                    .status(server_overloaded())
                    .header(header::RETRY_AFTER, retry_after)
                    .body(axum::body::Body::from(s.to_string()))
                    .unwrap()
            }

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
            Self::NoAvailableWorkers => "no_available_workers",
            Self::BusyFor(_) => "overloaded",
            Self::RateLimitExceeded => "rate_limit_exceeded",
            Self::TooManyStreams => "too_many_streams",
            Self::BaseBlockMismatch(_) => "base_block_mismatch",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct GenericError {
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{http::header, response::IntoResponse};

    /// One of each. The match is the only thing that can force a new variant to be
    /// added to the list, since the list itself is unchecked.
    fn every_variant() -> Vec<RequestError> {
        let all = vec![
            RequestError::BadRequest("bad".to_owned()),
            RequestError::NoData,
            RequestError::InternalError("internal".to_owned()),
            RequestError::Failure("failure".to_owned()),
            RequestError::NoAvailableWorkers,
            RequestError::TooManyStreams,
            RequestError::RateLimitExceeded,
            RequestError::BusyFor(Duration::from_secs(3)),
            RequestError::BaseBlockMismatch(sqd_primitives::BlockRef {
                number: 1,
                hash: "0x00".to_owned(),
            }),
        ];

        for error in &all {
            match error {
                RequestError::BadRequest(_)
                | RequestError::NoData
                | RequestError::InternalError(_)
                | RequestError::Failure(_)
                | RequestError::NoAvailableWorkers
                | RequestError::TooManyStreams
                | RequestError::RateLimitExceeded
                | RequestError::BusyFor(_)
                | RequestError::BaseBlockMismatch(_) => {}
            }
        }

        all
    }

    /// `retry_after_secs` and `into_response` decide the hint separately. This is
    /// what stops them drifting: a hint implies 529 + that exact header, and no
    /// hint implies no header.
    #[test]
    fn retry_hint_matches_the_response_for_every_variant() {
        for error in every_variant() {
            let hint = error.retry_after_secs();
            let short_code = error.short_code();
            let response = error.into_response();

            match hint {
                Some(secs) => {
                    assert_eq!(response.status().as_u16(), 529, "{short_code}");
                    assert_eq!(
                        response.headers().get(header::RETRY_AFTER),
                        Some(&header::HeaderValue::from(secs)),
                        "{short_code} answered with a hint other than its own",
                    );
                }
                None => assert!(
                    !response.headers().contains_key(header::RETRY_AFTER),
                    "{short_code} has no hint but answered with Retry-After",
                ),
            }
        }
    }

    /// Each cause needs a different remedy, so none of them may share a code.
    #[test]
    fn short_codes_are_distinct() {
        let mut codes: Vec<_> = every_variant().iter().map(|e| e.short_code()).collect();
        codes.sort_unstable();
        let total = codes.len();
        codes.dedup();

        assert_eq!(codes.len(), total, "two variants share a short_code");
    }

    /// Without Retry-After the squid-sdk falls back to its own schedule, first step 10ms.
    #[test]
    fn too_many_streams_is_529_with_retry_after() {
        let response = RequestError::TooManyStreams.into_response();

        assert_eq!(response.status().as_u16(), 529);
        assert_eq!(
            response.headers().get(header::RETRY_AFTER),
            Some(&header::HeaderValue::from_static("1")),
        );
    }

    /// No retry hint is possible here, so it must not drift into the overload path.
    #[test]
    fn no_available_workers_stays_503_without_retry_after() {
        let response = RequestError::NoAvailableWorkers.into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers().get(header::RETRY_AFTER), None);
    }

    #[test]
    fn overload_variants_all_signal_backoff() {
        for error in [
            RequestError::TooManyStreams,
            RequestError::RateLimitExceeded,
            RequestError::BusyFor(Duration::from_secs(3)),
        ] {
            let short_code = error.short_code();
            let response = error.into_response();

            assert_eq!(response.status().as_u16(), 529, "{short_code}");
            assert!(
                response.headers().contains_key(header::RETRY_AFTER),
                "{short_code} must carry Retry-After, or the sdk retries on its own schedule",
            );
        }
    }
}
