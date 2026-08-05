use std::time::Duration;

use axum::http::StatusCode;
use axum::response::Response;
use serde::Serialize;
use sqd_contract_client::PeerId;
use tokio::time::Instant;

/// Coarse category, the wire's `type`. `Api` is the only one that should page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorType {
    InvalidRequest,
    RateLimit,
    Availability,
    /// An invariant we own was violated.
    Api,
    /// The credential is absent, unreadable, or does not authenticate (ADR-017).
    Authentication,
    /// The credential authenticated but does not cover this request (ADR-017).
    Permission,
}

impl ErrorType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request_error",
            Self::RateLimit => "rate_limit_error",
            Self::Availability => "availability_error",
            Self::Api => "api_error",
            Self::Authentication => "authentication_error",
            Self::Permission => "permission_error",
        }
    }

    /// Both auth types answer no: retrying with the same credential cannot succeed, and
    /// a client that treats a refusal as transient produces the storm ADR-012 prevents.
    pub const fn retryable(self) -> bool {
        match self {
            Self::RateLimit | Self::Availability => true,
            Self::InvalidRequest | Self::Api | Self::Authentication | Self::Permission => false,
        }
    }
}

/// Declare the code vocabulary once, and derive the enum, [`ErrorCode::ALL`] and
/// [`ErrorCode::as_str`] from that single list.
///
/// A hand-written `ALL` beside a hand-written enum can fall behind it, and a compile-time
/// tripwire cannot stop that: an exhaustive `match` forces an arm *in the match*, never an
/// entry in a `&[Self]` literal. Since every test that pins DEF-10 and IB-5 iterates `ALL`,
/// a code missing from it ships with no status row and no frozen wire string, silently.
/// Generating both makes that divergence unrepresentable rather than merely asserted.
macro_rules! error_codes {
    (
        $(#[$enum_meta:meta])*
        pub enum $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident => $wire:literal ),+ $(,)?
        }
    ) => {
        $(#[$enum_meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum $name {
            $( $(#[$variant_meta])* $variant, )+
        }

        impl $name {
            /// Every code. Derived from the same list as the enum, so a variant that is
            /// missing here does not exist.
            pub const ALL: &'static [Self] = &[ $(Self::$variant),+ ];

            pub const fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $wire,)+ }
            }
        }
    };
}

error_codes! {
    /// Specific cause, the wire's `code`. [`ErrorCode::as_str`] is public API twice over:
    /// the `code` error field and the `error_code` metric label. Adding a variant is safe;
    /// renaming one breaks client matches and dashboards silently.
    ///
    /// A 204 has no code: it is the correct answer to a query whose range isn't produced
    /// yet, not a failure. It carries no body, so a code could never reach a client anyway
    /// (IB-4, ADR-014) — and on the metric `no_data` only ever restated `status="204"`.
    pub enum ErrorCode {
        MalformedRequest => "malformed_request",
        /// Right path, wrong verb. Keeps 405 rather than folding into
        /// [`Self::MalformedRequest`]: the request is well-formed, and `Allow` tells the
        /// client what to do — a detail a 400 has nowhere to put.
        MethodNotAllowed => "method_not_allowed",
        UnknownDataset => "unknown_dataset",
        NotFound => "not_found",
        BaseBlockMismatch => "base_block_mismatch",
        Overloaded => "overloaded",
        NoWorkers => "no_workers",
        /// Workers were reachable; every attempt failed transiently until retries ran out.
        RetriesExhausted => "retries_exhausted",
        UpstreamUnavailable => "upstream_unavailable",
        /// `/ready` declining traffic, so a routine drain doesn't read as an error.
        NotReady => "not_ready",
        /// A worker returned something that cannot be right.
        WorkerFailure => "worker_failure",
        Internal => "internal_error",
        /// Unset by any handler. `api_error` so it pages instead of hiding; `http_labels`
        /// re-types an unclassified 4xx.
        Unclassified => "unclassified",

        // ADR-017. Commercial deployments only; vacuous without a `commercial:` block.
        MissingCredential => "missing_credential",
        /// One wire code for four internal reasons — unparseable token, unknown key id,
        /// wrong secret, digestless record. Distinguishing them would tell a caller which
        /// of its guesses to keep (INV-39); the operator gets the distinction on the
        /// protected log axis instead.
        InvalidCredential => "invalid_credential",
        /// Only reachable by someone already holding the right secret, so it can be
        /// specific without leaking anything.
        RevokedCredential => "revoked_credential",
        ExpiredCredential => "expired_credential",
        PortalNotAllowed => "portal_not_allowed",
        DatasetNotAllowed => "dataset_not_allowed",
    }
}

impl ErrorCode {
    pub const fn error_type(self) -> ErrorType {
        match self {
            Self::MalformedRequest
            | Self::MethodNotAllowed
            | Self::UnknownDataset
            | Self::NotFound
            | Self::BaseBlockMismatch => ErrorType::InvalidRequest,

            Self::Overloaded => ErrorType::RateLimit,

            Self::NoWorkers
            | Self::RetriesExhausted
            | Self::UpstreamUnavailable
            | Self::NotReady => ErrorType::Availability,

            Self::WorkerFailure | Self::Internal | Self::Unclassified => ErrorType::Api,

            // Never `api_error`: turning away an unauthenticated request is the system
            // working, and must not page.
            Self::MissingCredential
            | Self::InvalidCredential
            | Self::RevokedCredential
            | Self::ExpiredCredential => ErrorType::Authentication,

            Self::PortalNotAllowed | Self::DatasetNotAllowed => ErrorType::Permission,
        }
    }

    /// IB-5's status for this code. The binding is closed, so it lives here rather than
    /// being restated at each site that builds a response — those restatements were free
    /// to disagree, and only a hand-written test said they didn't.
    ///
    /// Two documented exceptions keep a status of their own: a *proxied* refusal retains
    /// the upstream's (a 429 rather than our 529, its own 5xx rather than our 502), and
    /// `Unclassified` reports whatever escaped classification — 500 is only its floor.
    pub fn status(self) -> StatusCode {
        match self {
            Self::MalformedRequest => StatusCode::BAD_REQUEST,
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            Self::UnknownDataset | Self::NotFound => StatusCode::NOT_FOUND,
            Self::BaseBlockMismatch => StatusCode::CONFLICT,
            Self::Overloaded => server_overloaded(),
            Self::NoWorkers | Self::RetriesExhausted | Self::NotReady => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            Self::UpstreamUnavailable => StatusCode::BAD_GATEWAY,
            Self::WorkerFailure | Self::Internal | Self::Unclassified => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            Self::MissingCredential
            | Self::InvalidCredential
            | Self::RevokedCredential
            | Self::ExpiredCredential => StatusCode::UNAUTHORIZED,
            Self::PortalNotAllowed | Self::DatasetNotAllowed => StatusCode::FORBIDDEN,
        }
    }

    /// ADR-017: a 401 must name the scheme the client should retry with. Not folded into
    /// [`Self::status`] because the header is the type's obligation, not the status's —
    /// a 403 is also a refusal and owes no challenge.
    pub const fn challenges(self) -> bool {
        matches!(self.error_type(), ErrorType::Authentication)
    }

    /// INV-26: the one class that always owes the client a back-off interval. The value
    /// is context's (a worker's backoff, an upstream's header); only the obligation is
    /// the code's.
    pub const fn requires_hint(self) -> bool {
        matches!(self, Self::Overloaded)
    }

    /// Stands in when an upstream error body carries no usable prose of its own.
    pub const fn default_message(self) -> &'static str {
        match self {
            Self::MalformedRequest => "Bad request",
            Self::MethodNotAllowed => "Method not allowed for this endpoint",
            Self::UnknownDataset => "Unknown dataset",
            Self::NotFound => "Not found",
            Self::BaseBlockMismatch => "Base block mismatch",
            Self::Overloaded => "Service is overloaded, please try again later",
            Self::NoWorkers => "No available workers to serve the request",
            Self::RetriesExhausted => "All query attempts failed",
            Self::UpstreamUnavailable => "Upstream data source unavailable",
            Self::NotReady => "Portal is not ready",
            Self::WorkerFailure => "Worker returned invalid data",
            Self::Internal => "Internal error",
            Self::Unclassified => "Unclassified error",
            Self::MissingCredential => "API key required",
            Self::InvalidCredential => "Invalid API key",
            Self::RevokedCredential => "API key revoked",
            Self::ExpiredCredential => "API key expired",
            Self::PortalNotAllowed => "API key is not valid for this portal",
            Self::DatasetNotAllowed => "API key is not authorized for this dataset",
        }
    }

    /// Classify a proxied response, where the status is all we see, into the public
    /// status and code. Lossy: hotblocks splits 400 five ways but keeps the discriminant
    /// in extensions, which never reach the wire.
    ///
    /// A matched status is preserved; an *unmatched* 4xx is normalized to 400, because
    /// the upstream's choice among them describes a contract the portal generated and the
    /// client cannot act on (DC-4, ADR-014).
    ///
    /// Only 429 and 529 read as OVERLOADED. A 503 is unavailability, not congestion —
    /// that is the whole of ADR-007, applied to the upstream instead of to ourselves.
    /// Lumping it in said "capacity is exhausted" about a source that has no capacity at
    /// all, made a dead dependency indistinguishable from a healthy one shedding load,
    /// and had us hand the client a retry hint pointing back into it.
    pub fn classify_upstream(status: StatusCode) -> (StatusCode, Self) {
        match status.as_u16() {
            409 => (status, Self::BaseBlockMismatch),
            404 => (status, Self::UnknownDataset),
            // The two that keep the upstream's status rather than the code's: its 429 is
            // more specific than our 529, and its 5xx than our 502.
            429 | 529 => (status, Self::Overloaded),
            500..=599 => (status, Self::UpstreamUnavailable),
            400..=499 => (Self::MalformedRequest.status(), Self::MalformedRequest),
            _ => (status, Self::Unclassified),
        }
    }
}

/// Non-standard "Site is overloaded" status used to signal clients to back off (ADR-007).
pub fn server_overloaded() -> StatusCode {
    StatusCode::from_u16(529).expect("529 is a valid status code")
}

/// Error body shared by every endpoint.
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
    /// Echo of `x-request-id`, for support. Filled in by the logging middleware on 5xx
    /// only; every response carries the header regardless.
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
}

/// The envelope plus any top-level siblings the binding requires, kept as a struct so
/// exactly one place decides the shape. Every error body in the portal — locally
/// produced or rewritten from an upstream — is built here and rendered by
/// [`error_body_response`], which is what keeps the two `/stream` data sources from
/// drifting into two shapes (IB-5).
#[derive(Debug, Clone)]
pub struct ErrorBody {
    code: ErrorCode,
    response: ErrorResponse,
    siblings: serde_json::Map<String, serde_json::Value>,
}

impl ErrorBody {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            response: ErrorResponse::new(code, message),
            siblings: serde_json::Map::new(),
        }
    }

    pub fn with_param(mut self, param: &'static str) -> Self {
        self.response.error.param = Some(param);
        self
    }

    /// A top-level key beside `error`. Only for keys the binding pins there — today
    /// just 409's `previousBlocks`, which clients walk to find a shared ancestor and
    /// which therefore cannot move under `error`.
    pub fn with_sibling(mut self, key: &str, value: serde_json::Value) -> Self {
        self.siblings.insert(key.to_owned(), value);
        self
    }

    pub fn code(&self) -> ErrorCode {
        self.code
    }

    pub fn set_request_id(&mut self, request_id: &str) {
        self.response.error.request_id = Some(request_id.to_owned());
    }

    pub fn to_json(&self) -> serde_json::Value {
        let mut object = serde_json::Map::with_capacity(1 + self.siblings.len());
        object.insert(
            "error".to_owned(),
            serde_json::to_value(&self.response.error).expect("ErrorDetail is serializable"),
        );
        object.extend(self.siblings.clone());
        serde_json::Value::Object(object)
    }
}

/// Render an error body, and keep the struct in extensions so the logging middleware can
/// stamp in `request_id` — the one field only it knows — by re-rendering rather than
/// parsing the bytes back out.
pub fn error_body_response(status: StatusCode, body: ErrorBody) -> Response {
    use axum::response::IntoResponse;
    let mut response = (status, axum::Json(body.to_json())).into_response();
    response.extensions_mut().insert(body.code);
    response.extensions_mut().insert(body);
    response
}

pub fn error_response(status: StatusCode, code: ErrorCode, message: impl Into<String>) -> Response {
    error_body_response(status, ErrorBody::new(code, message))
}

/// The usual case: the status is the one IB-5 binds the code to. Prefer this to
/// [`error_response`], which exists for the two places that carry a status of their own —
/// a proxied refusal, and a deprecated route with its own history.
pub fn coded_response(code: ErrorCode, message: impl Into<String>) -> Response {
    error_response(code.status(), code, message)
}

#[derive(thiserror::Error, Debug)]
pub enum RequestError {
    #[error("{0}")]
    BadRequest(String),
    /// Names the offending parameter, so a client need not parse the message for it.
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
    /// The response contradicts the query contract — bad signature, undecodable
    /// body, or data outside the queried range. Rerouted like a transient
    /// failure, but exhaustion pages instead of reporting a transient outage:
    /// the network is serving bad data or verification is broken (DC-1, FM-2).
    #[error("{0}")]
    Integrity(String),
    #[error("{0}")]
    Failure(String),
    #[error("rate limit exceeded")]
    RateLimitExceeded,
    #[error("base block mismatch")]
    BaseBlockMismatch(sqd_primitives::BlockRef),
}

/// What an exhausted run of one failure means for the client, once every attempt agrees
/// on it. A total function rather than a list of names at the call site: a new
/// [`QueryError`] then has to say which bucket it falls in, instead of joining the
/// transient one by omission — which is how a run of capacity refusals came to answer a
/// bare 503 (DC-1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExhaustionClass {
    /// The network serves bad data or verification is broken. Pages.
    Integrity,
    /// Congestion: the client is owed a status to back off from and a hint.
    Capacity,
    /// A later retry can still succeed.
    Transient,
}

impl QueryError {
    pub const fn exhaustion_class(&self) -> ExhaustionClass {
        match self {
            Self::Integrity(_) => ExhaustionClass::Integrity,
            Self::RateLimitExceeded => ExhaustionClass::Capacity,
            Self::BadRequest(_)
            | Self::Retriable(_)
            | Self::Failure(_)
            | Self::BaseBlockMismatch(_) => ExhaustionClass::Transient,
        }
    }
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
            QueryError::Integrity(s) => {
                RequestError::Failure(format!("worker {worker} returned invalid data: {s}"))
            }
            QueryError::Failure(s) => RequestError::Failure(format!("worker {worker} failed: {s}")),
            QueryError::RateLimitExceeded => RequestError::RateLimitExceeded,
            QueryError::BaseBlockMismatch(block_ref) => RequestError::BaseBlockMismatch(block_ref),
        }
    }

    /// `None` for [`Self::NoData`] alone: a 204 is the correct answer, not a failure,
    /// and the taxonomy covers failures.
    pub fn code(&self) -> Option<ErrorCode> {
        Some(match self {
            Self::NoData => return None,
            Self::BadRequest(_) | Self::InvalidParam { .. } => ErrorCode::MalformedRequest,
            Self::RetriesExhausted(_) => ErrorCode::RetriesExhausted,
            Self::Internal(_) => ErrorCode::Internal,
            Self::Failure(_) => ErrorCode::WorkerFailure,
            Self::Unavailable => ErrorCode::NoWorkers,
            Self::RateLimitExceeded | Self::BusyFor(_) => ErrorCode::Overloaded,
            Self::BaseBlockMismatch(_) => ErrorCode::BaseBlockMismatch,
        })
    }
}

impl axum::response::IntoResponse for RequestError {
    fn into_response(self) -> axum::response::Response {
        use axum::http::header;

        // Not an error: no body, no envelope, no code (IB-4).
        let Some(code) = self.code() else {
            return Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(axum::body::Body::empty())
                .expect("204 with an empty body is valid");
        };

        // What the client is told, beyond the code: prose, and the two structured fields
        // a variant can carry. The status is the code's (IB-5) and is not restated.
        let body = match self {
            Self::NoData => unreachable!("NoData has no code"),
            Self::BaseBlockMismatch(ref block_ref) => ErrorBody::new(code, self.to_string())
                .with_sibling("previousBlocks", serde_json::json!([block_ref])),
            Self::InvalidParam { param, ref message } => {
                ErrorBody::new(code, format!("Bad request: {message}")).with_param(param)
            }
            Self::BadRequest(ref e) => ErrorBody::new(code, format!("Bad request: {e}")),
            Self::Internal(ref e) => ErrorBody::new(code, format!("Internal error: {e}")),
            Self::RetriesExhausted(ref e) => {
                ErrorBody::new(code, format!("All query attempts failed: {e}"))
            }
            Self::Unavailable | Self::Failure(_) | Self::BusyFor(_) | Self::RateLimitExceeded => {
                ErrorBody::new(code, self.to_string())
            }
        };

        let mut response = error_body_response(code.status(), body);

        if code.requires_hint() {
            // The obligation is the code's, the interval the variant's.
            let seconds = match &self {
                Self::BusyFor(duration) => duration.as_secs() + 1,
                _ => RETRY_AFTER_FLOOR,
            };
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, seconds.into());
        }
        response
    }
}

/// P-RETRY-AFTER-MIN: the smallest back-off a refusal may ask for.
pub const RETRY_AFTER_FLOOR: u64 = 1;

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

    /// A 204 is the right answer to a range that isn't produced yet, not a failure. It
    /// carries no body and no taxonomy code, so nothing downstream can report it as one.
    #[tokio::test]
    async fn no_data_is_not_an_error() {
        let (status, code, body) = parts(RequestError::NoData.into_response()).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert_eq!(code, None, "204 must carry no error code");
        assert_eq!(body, serde_json::Value::Null);
        assert_eq!(RequestError::NoData.code(), None);
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
        // Retrying the same key cannot change the answer (ADR-017).
        assert!(!ErrorCode::InvalidCredential.error_type().retryable());
        assert!(!ErrorCode::DatasetNotAllowed.error_type().retryable());
    }

    /// An auth refusal is the system working. Typing one `api_error` would page the team
    /// every time a client mistypes its key.
    #[test]
    fn no_auth_refusal_pages() {
        for code in ErrorCode::ALL {
            let is_auth = matches!(
                code.error_type(),
                ErrorType::Authentication | ErrorType::Permission
            );
            assert_eq!(
                is_auth,
                matches!(code.status().as_u16(), 401 | 403),
                "{} must be an auth type iff it answers 401/403",
                code.as_str()
            );
        }
    }

    /// Only api_error should page, so an unclassified response must land there.
    #[test]
    fn unclassified_is_an_api_error() {
        assert_eq!(ErrorCode::Unclassified.error_type(), ErrorType::Api);
    }

    /// A matched upstream status is preserved; an unmatched 4xx normalizes to 400,
    /// because the upstream's choice among them describes a contract the portal
    /// generated and the client cannot act on (DC-4).
    ///
    /// 503 is the line ADR-007 draws: unavailability, not congestion. Reading it as
    /// OVERLOADED claimed exhausted capacity of a source that may have none running.
    #[test]
    fn upstream_statuses_map_onto_the_taxonomy() {
        use ErrorCode::*;
        let cases = [
            (400, 400, MalformedRequest),
            (403, 400, MalformedRequest),
            (418, 400, MalformedRequest),
            (404, 404, UnknownDataset),
            (409, 409, BaseBlockMismatch),
            (429, 429, Overloaded),
            (529, 529, Overloaded),
            (500, 500, UpstreamUnavailable),
            (502, 502, UpstreamUnavailable),
            (503, 503, UpstreamUnavailable),
        ];
        for (upstream, want_status, want_code) in cases {
            let (status, code) =
                ErrorCode::classify_upstream(StatusCode::from_u16(upstream).unwrap());
            assert_eq!(status.as_u16(), want_status, "upstream {upstream}");
            assert_eq!(code, want_code, "upstream {upstream}");
        }
    }

    /// IB-5 calls the code→status mapping closed, which was previously only assertable
    /// code by code because each response site named its own status. Now it is one
    /// object, so the whole binding is checked against the spec in one place — and a
    /// code added without a status cannot compile.
    #[test]
    fn every_code_answers_the_status_ib5_binds_it_to() {
        use ErrorCode::*;
        let binding = [
            (MalformedRequest, 400),
            (MethodNotAllowed, 405),
            (UnknownDataset, 404),
            (NotFound, 404),
            (BaseBlockMismatch, 409),
            (Overloaded, 529),
            (NoWorkers, 503),
            (RetriesExhausted, 503),
            (UpstreamUnavailable, 502),
            (NotReady, 503),
            (WorkerFailure, 500),
            (Internal, 500),
            (Unclassified, 500),
            (MissingCredential, 401),
            (InvalidCredential, 401),
            (RevokedCredential, 401),
            (ExpiredCredential, 401),
            (PortalNotAllowed, 403),
            (DatasetNotAllowed, 403),
        ];
        for (code, want) in binding {
            assert_eq!(code.status().as_u16(), want, "{}", code.as_str());
        }

        // The tripwire, and it holds only because `ALL` is generated from the enum: a new
        // code is in `ALL` by construction, so it must appear here and in DEF-10.
        for code in ErrorCode::ALL {
            assert!(
                binding.iter().any(|(listed, _)| listed == code),
                "{} needs a row here and in DEF-10",
                code.as_str()
            );
        }

        // INV-26 is an iff over the same object.
        for (code, _) in binding {
            assert_eq!(
                code.requires_hint(),
                code == Overloaded,
                "{}",
                code.as_str()
            );
        }

        // ADR-017's challenge obligation is likewise an iff, over the 401 rows.
        for (code, status) in binding {
            assert_eq!(code.challenges(), status == 401, "{}", code.as_str());
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
            (
                MethodNotAllowed,
                "method_not_allowed",
                "invalid_request_error",
            ),
            (UnknownDataset, "unknown_dataset", "invalid_request_error"),
            (NotFound, "not_found", "invalid_request_error"),
            (
                BaseBlockMismatch,
                "base_block_mismatch",
                "invalid_request_error",
            ),
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
            (
                MissingCredential,
                "missing_credential",
                "authentication_error",
            ),
            (
                InvalidCredential,
                "invalid_credential",
                "authentication_error",
            ),
            (
                RevokedCredential,
                "revoked_credential",
                "authentication_error",
            ),
            (
                ExpiredCredential,
                "expired_credential",
                "authentication_error",
            ),
            (PortalNotAllowed, "portal_not_allowed", "permission_error"),
            (DatasetNotAllowed, "dataset_not_allowed", "permission_error"),
        ];
        for (code, want_code, want_type) in expected {
            assert_eq!(code.as_str(), want_code);
            assert_eq!(code.error_type().as_str(), want_type);
        }
        for code in ErrorCode::ALL {
            assert!(
                expected.iter().any(|(listed, ..)| listed == code),
                "{} is not pinned here, so renaming it would break clients silently",
                code.as_str()
            );
        }
    }
}
