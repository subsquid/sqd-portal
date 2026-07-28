use std::{future::Future, sync::Arc};

use axum::{
    extract::Path,
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Extension,
};
use futures::{pin_mut, StreamExt};
use tower_http::request_id::RequestId;

use crate::{
    config::Config,
    controller::task_manager::TaskManager,
    datasets::DatasetConfig,
    hotblocks::{HeadMode, HotblocksErr, HotblocksHandle, Status},
    network::NetworkClient,
    openapi::BlockNumberResponse,
    types::{
        coded_response, error_response, Compression, DatasetId, ErrorCode, ErrorResponse,
        ParsedQuery, RequestError, StreamRequest,
    },
    utils::{
        conversion::collect_to_string,
        internal_query::{build_blocknumber_query, find_block_in_chunk},
    },
};

use super::stream::{DATA_SOURCE_HEADER, DATA_SOURCE_NETWORK_METRIC, DATA_SOURCE_REALTIME_METRIC};

/// Block at Timestamp
///
/// Returns the first block whose timestamp is greater than or equal to the given value.
#[utoipa::path(
    get,
    path = "/datasets/{dataset}/timestamps/{timestamp}/block",
    params(
        ("dataset" = String, Path, description = "Dataset name"),
        ("timestamp" = u64, Path, description = "Timestamp in seconds"),
    ),
    responses(
        (status = 200, description = "Block number resolved", body = BlockNumberResponse),
        (status = 400, description = "\
Unparseable `timestamp` path segment — or the real-time source refused the query the Portal \
generated on the client's behalf, which is classified by upstream status like any other \
(ADR-011). The request itself carries only a dataset name and an integer, so a 400 here is \
not necessarily the caller's to fix; read `error.message`.", body = ErrorResponse),
        (status = 404, description = "No block found for timestamp", body = ErrorResponse),
        (status = 429, description = "Too many requests - retry after the interval in `Retry-After`", body = ErrorResponse,
            headers(
                ("Retry-After" = String, description = "Delay in seconds before retrying (at least 1)"),
            )),
        (status = 500, description = "Internal server error", body = ErrorResponse),
        (status = 502, description = "The requested data could not be retrieved right now - retry later", body = ErrorResponse),
        (status = 503, description = "Service temporarily unavailable - retry later. May carry `Retry-After`; honour it when present", body = ErrorResponse),
        (status = 529, description = "Overloaded - retry after the interval in `Retry-After`", body = ErrorResponse,
            headers(
                ("Retry-After" = String, description = "Delay in seconds before retrying (at least 1)"),
            )),
    ),
    tag = "Datasets"
)]
pub(crate) async fn get_blocknumber_by_timestamp(
    Path((_, timestamp)): Path<(DatasetId, u64)>,
    Extension(req): Extension<RequestId>,
    Extension(network): Extension<Arc<NetworkClient>>,
    Extension(task_manager): Extension<Arc<TaskManager>>,
    Extension(config): Extension<Arc<Config>>,
    Extension(hotblocks): Extension<Arc<HotblocksHandle>>,
    dataset: DatasetConfig,
) -> Response {
    resolve(
        timestamp,
        &req,
        &network,
        &task_manager,
        &config,
        &hotblocks,
        &dataset,
    )
    .await
    .map(|resolved| {
        (
            [(DATA_SOURCE_HEADER, resolved.data_source.as_str())],
            axum::Json(BlockNumberResponse {
                block_number: resolved.block_number,
            }),
        )
            .into_response()
    })
    .unwrap_or_else(BlockNumberLookupError::into_response)
}

pub struct ResolvedBlockNumber {
    block_number: u64,
    data_source: BlockNumberDataSource,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BlockNumberDataSource {
    Network,
    Hotblocks,
}

impl BlockNumberDataSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Network => DATA_SOURCE_NETWORK_METRIC,
            Self::Hotblocks => DATA_SOURCE_REALTIME_METRIC,
        }
    }
}

/// Failure modes for resolving a block number by timestamp.
///
/// These map directly to the public HTTP responses returned by the timestamp
/// route: missing data, internal processing failures, and temporarily
/// unavailable upstream data sources.
#[derive(Debug)]
pub enum BlockNumberLookupError {
    NotFound(String),
    Internal(String),
    /// The real-time source failed to answer. 502 per IB-5: the Portal's own call
    /// upstream failed, which is not the same as the Portal having no capacity.
    Unavailable(String),
    /// The head arrived, the body did not finish — a mid-body reset, or a stall past the
    /// read deadline. UPSTREAM-FAILURE like [`Self::Unavailable`], but never replayed: the
    /// read budget is already spent, and DC-4 bars a second attempt precisely so a stalled
    /// upstream cannot push past the client's deadline (REQ-22).
    UpstreamIncomplete(String),
    /// A refusal the network layer already classified, carried whole. Flattening it into
    /// [`Self::Unavailable`] lost the 529 and the `Retry-After` that an overload refusal
    /// owes the client (INV-26), and reported congestion as an upstream fault.
    Refused(RequestError),
    /// A real-time source refusal, classified by the same rules the stream proxy uses.
    /// One upstream status cannot mean two things depending on which endpoint asked.
    Upstream {
        status: StatusCode,
        code: ErrorCode,
        retry_after: Option<String>,
    },
}

/// Classify a failed hotblocks response the way [`forward_response`] does, so a 429 stays
/// an overload here too instead of becoming a Portal bug, and keeps the hint INV-26 owes
/// the client.
///
/// [`forward_response`]: crate::http_server::forward_response
fn upstream_failure(response: &reqwest::Response) -> BlockNumberLookupError {
    let (status, code) = ErrorCode::classify_upstream(response.status());
    BlockNumberLookupError::Upstream {
        status,
        code,
        retry_after: response
            .headers()
            .get(header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
    }
}

impl BlockNumberLookupError {
    pub fn class(&self) -> ErrorCode {
        match self {
            Self::NotFound(_) => ErrorCode::NotFound,
            Self::Internal(_) => ErrorCode::Internal,
            Self::Unavailable(_) | Self::UpstreamIncomplete(_) => ErrorCode::UpstreamUnavailable,
            // NoData never reaches here: it is a stream outcome, not a lookup one.
            Self::Refused(e) => e.code().unwrap_or(ErrorCode::Unclassified),
            Self::Upstream { code, .. } => *code,
        }
    }

    /// Convert a resolver error into the timestamp endpoint's HTTP response.
    pub fn into_response(self) -> Response {
        let class = self.class();
        let message = match self {
            // Already carries its own status, headers and envelope.
            Self::Refused(e) => return e.into_response(),
            Self::Upstream {
                status,
                code,
                retry_after,
            } => {
                let mut response = error_response(status, code, code.default_message());
                match code {
                    // INV-26: an overload always carries a usable hint, upstream or not.
                    ErrorCode::Overloaded => {
                        let seconds = retry_after
                            .and_then(|value| value.trim().parse::<u64>().ok())
                            .filter(|seconds| *seconds >= 1)
                            .unwrap_or(1);
                        response
                            .headers_mut()
                            .insert(header::RETRY_AFTER, seconds.into());
                    }
                    // Any other class forwards the header verbatim, as the proxy does.
                    // Parsing it first swallowed the RFC's date form on a 503, so one
                    // upstream response carried a hint on /stream and none here.
                    _ => {
                        if let Some(value) = retry_after.and_then(|v| HeaderValue::try_from(v).ok())
                        {
                            response.headers_mut().insert(header::RETRY_AFTER, value);
                        }
                    }
                }
                return response;
            }
            Self::NotFound(message)
            | Self::Internal(message)
            | Self::Unavailable(message)
            | Self::UpstreamIncomplete(message) => message,
        };

        coded_response(class, message)
    }
}

/// Resolve the first block whose timestamp is greater than or equal to `timestamp`.
///
/// Archive data is preferred when the dataset has an SQD Network mapping. If
/// the archive lookup cannot find a matching chunk, the resolver falls back to
/// HotblocksDB when the dataset has a real-time data source configured.
pub async fn resolve(
    timestamp: u64,
    req: &RequestId,
    network: &NetworkClient,
    task_manager: &Arc<TaskManager>,
    config: &Config,
    hotblocks: &HotblocksHandle,
    dataset: &DatasetConfig,
) -> Result<ResolvedBlockNumber, BlockNumberLookupError> {
    get_blocknumber_by_timestamp_inner(
        dataset.network_id.is_some(),
        dataset.hotblocks.is_some(),
        || async {
            let Some(dataset_id) = dataset.network_id.as_ref() else {
                return Err(BlockNumberLookupError::NotFound(
                    "No archive configured for dataset".to_string(),
                ));
            };

            get_archival_blocknumber_by_timestamp(
                timestamp,
                req,
                network,
                task_manager,
                config,
                dataset,
                dataset_id,
            )
            .await
        },
        || async { get_hotblocks_blocknumber_by_timestamp(timestamp, hotblocks, dataset).await },
    )
    .await
}

async fn get_blocknumber_by_timestamp_inner<
    ArchiveLookup,
    ArchiveFuture,
    HotblocksLookup,
    HotblocksFuture,
>(
    has_archive: bool,
    has_hotblocks: bool,
    archive_lookup: ArchiveLookup,
    hotblocks_lookup: HotblocksLookup,
) -> Result<ResolvedBlockNumber, BlockNumberLookupError>
where
    ArchiveLookup: FnOnce() -> ArchiveFuture,
    ArchiveFuture: Future<Output = Result<u64, BlockNumberLookupError>>,
    HotblocksLookup: FnOnce() -> HotblocksFuture,
    HotblocksFuture: Future<Output = Result<u64, BlockNumberLookupError>>,
{
    tracing::debug!(
        has_archive,
        has_hotblocks,
        "resolving block number by timestamp"
    );

    if has_archive {
        match archive_lookup().await {
            Ok(block_number) => {
                return Ok(ResolvedBlockNumber {
                    block_number,
                    data_source: BlockNumberDataSource::Network,
                })
            }
            Err(BlockNumberLookupError::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
    }

    if has_hotblocks {
        return hotblocks_lookup()
            .await
            .map(|block_number| ResolvedBlockNumber {
                block_number,
                data_source: BlockNumberDataSource::Hotblocks,
            });
    }

    Err(BlockNumberLookupError::NotFound(
        "No block found for timestamp".to_string(),
    ))
}

async fn get_archival_blocknumber_by_timestamp(
    timestamp: u64,
    req: &RequestId,
    network: &NetworkClient,
    task_manager: &Arc<TaskManager>,
    config: &Config,
    dataset: &DatasetConfig,
    dataset_id: &DatasetId,
) -> Result<u64, BlockNumberLookupError> {
    let ts = timestamp
        .checked_mul(1000) // milliseconds
        .ok_or_else(|| BlockNumberLookupError::Internal("timestamp overflow".to_string()))?;
    let chunk = network
        .find_chunk_by_timestamp(dataset_id, ts)
        .map_err(|_| {
            BlockNumberLookupError::NotFound("No chunk found for timestamp".to_string())
        })?;

    let Ok(pquery) = build_blocknumber_query(&dataset.kind, chunk.first_block, chunk.last_block)
    else {
        tracing::warn!("cannot build blocknumber query for {}", dataset_id);
        return Err(BlockNumberLookupError::Internal(format!(
            "Cannot build timestamp query for {dataset_id}"
        )));
    };

    let request = build_request(
        config,
        req.header_value().to_str().unwrap_or(""),
        pquery,
        dataset_id.to_owned(),
        dataset.default_name.clone(),
        Some(1),
    );

    let stream = match task_manager.clone().spawn_stream(request).await {
        Ok(stream) => stream,
        Err(e) => {
            tracing::warn!("spawn stream error: {:?}", e);
            return Err(BlockNumberLookupError::Refused(e));
        }
    };

    pin_mut!(stream);

    let js = collect_to_string(
        stream.map(|result| std::io::Result::Ok(tokio_util::bytes::Bytes::from_owner(result))),
    )
    .await
    .map_err(|e| {
        tracing::warn!("stream processing error: {:?}", e);
        BlockNumberLookupError::Internal("stream processing error".to_string())
    })?;

    find_block_in_chunk(timestamp, &js).map_err(|e| {
        tracing::warn!("cannot find blocknumber in chunk: {:?}", e);
        BlockNumberLookupError::NotFound("block not in chunk".to_string())
    })
}

/// Resolve a block number from HotblocksDB, retrying once on a transient failure.
///
/// It sends two requests non-atomically. A change of state between them can lead
/// to, e.g., "range unavailable" responses. Retry to reduce the number of 503s.
async fn get_hotblocks_blocknumber_by_timestamp(
    timestamp: u64,
    hotblocks: &HotblocksHandle,
    dataset: &DatasetConfig,
) -> Result<u64, BlockNumberLookupError> {
    retry_once_on_unavailable(|| {
        get_hotblocks_blocknumber_by_timestamp_once(timestamp, hotblocks, dataset)
    })
    .await
}

async fn retry_once_on_unavailable<Lookup, LookupFuture>(
    mut lookup: Lookup,
) -> Result<u64, BlockNumberLookupError>
where
    Lookup: FnMut() -> LookupFuture,
    LookupFuture: Future<Output = Result<u64, BlockNumberLookupError>>,
{
    let first = lookup().await;
    let reason = match &first {
        // The call itself failed transiently.
        Err(BlockNumberLookupError::Unavailable(reason)) => reason.clone(),
        // A refusal that arrived as a response. The state change this retry exists for
        // surfaces here — as the 400 a pruned range answers — so skipping the whole
        // variant would leave it covering only the rarer half.
        Err(BlockNumberLookupError::Upstream { status, code, .. }) if retryable(*code) => {
            status.to_string()
        }
        _ => return first,
    };
    tracing::debug!(
        reason,
        "hotblocks timestamp lookup unavailable, retrying once"
    );
    lookup().await
}

/// Whether a second attempt can answer differently. Excluded: an overload, because the
/// source just shed load and a retry adds to it; and refusals that are properties of the
/// request rather than of the moment, which the same request reproduces exactly.
fn retryable(code: ErrorCode) -> bool {
    !matches!(
        code,
        ErrorCode::Overloaded | ErrorCode::UnknownDataset | ErrorCode::BaseBlockMismatch
    )
}

async fn get_hotblocks_blocknumber_by_timestamp_once(
    timestamp: u64,
    hotblocks: &HotblocksHandle,
    dataset: &DatasetConfig,
) -> Result<u64, BlockNumberLookupError> {
    let response = hotblocks
        .request_status(&dataset.default_name)
        .await
        .map_err(|e| {
            tracing::warn!("hotblocks status error: {:?}", e);
            classify_hotblocks_error(e, "Hotblocks status error")
        })?;
    let status = decode_status(response).await?;

    get_hotblocks_blocknumber_by_timestamp_inner(
        timestamp,
        &dataset.kind,
        &dataset.default_name,
        status,
        |query| async move {
            let response = hotblocks
                .stream(&dataset.default_name, &query, HeadMode::RealTime)
                .await
                .map_err(|e| {
                    tracing::warn!("hotblocks stream error: {:?}", e);
                    classify_hotblocks_error(e, "Hotblocks stream error")
                })?;

            let status = response.status();
            if !status.is_success() {
                tracing::warn!("hotblocks stream failed with status {}", status);
                return Err(upstream_failure(&response));
            }

            collect_hotblocks_stream(response).await
        },
    )
    .await
}

/// Classify the status response before decoding it, the way every other call to the
/// real-time source is classified. `get_status` resolves the status internally, so a
/// refusal reached this endpoint as a transport error and answered 502 whatever it
/// actually was — the one hotblocks call that never saw the shared classifier (DC-4).
async fn decode_status(response: reqwest::Response) -> Result<Status, BlockNumberLookupError> {
    if !response.status().is_success() {
        tracing::warn!("hotblocks status failed with status {}", response.status());
        return Err(upstream_failure(&response));
    }
    // Read and parse separately. reqwest wraps a body that never finished — a mid-body reset,
    // a stall past the read timeout — in the same `Kind::Decode` as a body that arrived and
    // wasn't JSON, so `is_decode()` cannot tell a DC-4 transport fault from our own bug. Only
    // the second is ours: reporting the first as `Internal` pages on a hotblocks roll and
    // tells the client not to retry something a retry fixes.
    let bytes = response.bytes().await.map_err(|e| {
        tracing::warn!("hotblocks status body error: {:?}", e);
        BlockNumberLookupError::UpstreamIncomplete("Hotblocks status error".to_owned())
    })?;
    serde_json::from_slice(&bytes).map_err(|e| {
        tracing::warn!("hotblocks status decode error: {:?}", e);
        // Ours to fix, and identical on a second attempt.
        BlockNumberLookupError::Internal("Hotblocks status error".to_owned())
    })
}

/// Classify a hotblocks client error for the timestamp endpoint.
///
/// Only transient failures become `Unavailable` — the variant retried by
/// `retry_once_on_unavailable`. A missing dataset URL or an unparseable
/// response body fails identically on a second attempt.
fn classify_hotblocks_error(error: HotblocksErr, message: &str) -> BlockNumberLookupError {
    match &error {
        HotblocksErr::UnknownDataset => BlockNumberLookupError::Internal(message.to_string()),
        HotblocksErr::Request(e) if e.is_decode() => {
            BlockNumberLookupError::Internal(message.to_string())
        }
        HotblocksErr::Request(_) => BlockNumberLookupError::Unavailable(message.to_string()),
    }
}

async fn get_hotblocks_blocknumber_by_timestamp_inner<StreamLookup, StreamFuture>(
    timestamp: u64,
    kind: &str,
    dataset_name: &str,
    status: Status,
    stream_lookup: StreamLookup,
) -> Result<u64, BlockNumberLookupError>
where
    StreamLookup: FnOnce(String) -> StreamFuture,
    StreamFuture: Future<Output = Result<String, BlockNumberLookupError>>,
{
    let Some(data) = status.data else {
        return Err(BlockNumberLookupError::NotFound(
            "No hotblocks found for timestamp".to_string(),
        ));
    };

    if data
        .last_block_timestamp
        .is_some_and(|last_timestamp| timestamp > last_timestamp)
    {
        return Err(BlockNumberLookupError::NotFound(
            "block not in hotblocks".to_string(),
        ));
    }

    let pquery = build_blocknumber_query(kind, data.first_block, data.last_block).map_err(|e| {
        tracing::warn!("cannot build hotblocks blocknumber query: {:?}", e);
        BlockNumberLookupError::Internal(format!(
            "Cannot build timestamp query for {}",
            dataset_name
        ))
    })?;

    let js = stream_lookup(pquery.into_string()).await?;

    find_block_in_chunk(timestamp, &js).map_err(|e| {
        tracing::warn!("cannot find blocknumber in hotblocks: {:?}", e);
        BlockNumberLookupError::NotFound("block not in hotblocks".to_string())
    })
}

async fn collect_hotblocks_stream(
    response: reqwest::Response,
) -> Result<String, BlockNumberLookupError> {
    let is_gzip = response
        .headers()
        .get(header::CONTENT_ENCODING)
        .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"gzip"));
    let bytes = response.bytes().await.map_err(|e| {
        tracing::warn!("hotblocks stream body error: {:?}", e);
        BlockNumberLookupError::UpstreamIncomplete("Hotblocks stream error".to_owned())
    })?;

    decode_hotblocks_stream_body(is_gzip, bytes.as_ref()).map_err(|e| {
        tracing::warn!("hotblocks stream processing error: {:?}", e);
        BlockNumberLookupError::Internal("hotblocks stream processing error".to_owned())
    })
}

fn decode_hotblocks_stream_body(is_gzip: bool, bytes: &[u8]) -> anyhow::Result<String> {
    if is_gzip || bytes.starts_with(&[0x1f, 0x8b]) {
        let mut decoder = flate2::read::GzDecoder::new(bytes);
        let mut decoded = String::new();
        std::io::Read::read_to_string(&mut decoder, &mut decoded)?;
        return Ok(decoded);
    }

    Ok(String::from_utf8(bytes.to_vec())?)
}

fn build_request(
    config: &Config,
    req_id: &str,
    pq: ParsedQuery,
    did: DatasetId,
    dname: String,
    max_chunks: Option<usize>,
) -> StreamRequest {
    StreamRequest {
        query: pq,
        dataset_id: did,
        dataset_name: dname,
        request_id: req_id.to_string(),
        buffer_size: config.max_buffer_size,
        max_stored_results_per_chunk: config.max_stored_results_per_chunk.max(1),
        max_chunks,
        timeout_quantile: config.default_timeout_quantile,
        retries: config.default_retries,
        compression: Compression::Gzip,
        skip_parent_hash_validation: false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use flate2::{write::GzEncoder, Compression};
    use sqd_primitives::BlockRef;
    use std::io::Write;

    use crate::hotblocks::StatusData;

    use super::*;

    /// A refusal the network layer already classified must reach the client intact.
    /// Flattening it into `Unavailable` dropped the 529 and the `Retry-After` that INV-26
    /// requires, and reported the Portal's own congestion as an upstream fault.
    #[tokio::test]
    async fn a_classified_refusal_keeps_its_status_and_hint() {
        let response = BlockNumberLookupError::Refused(RequestError::BusyFor(
            std::time::Duration::from_secs(3),
        ))
        .into_response();

        assert_eq!(response.status().as_u16(), 529);
        assert_eq!(response.headers()[axum::http::header::RETRY_AFTER], "4");
        assert_eq!(
            response.extensions().get::<ErrorCode>().copied(),
            Some(ErrorCode::Overloaded)
        );

        let response = BlockNumberLookupError::Refused(RequestError::Unavailable).into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.extensions().get::<ErrorCode>().copied(),
            Some(ErrorCode::NoWorkers),
            "no capacity is not an upstream failure"
        );
    }

    /// One upstream status cannot mean two things depending on which endpoint asked. This
    /// path used to call every 4xx a Portal bug — so a hotblocks 429 surfaced as a 500 —
    /// and every 5xx an upstream outage, so a 503 or 529 lost both its status and the hint
    /// INV-26 owes the client.
    #[tokio::test]
    async fn upstream_refusals_are_classified_like_the_stream_proxy() {
        async fn refusal(status: u16, retry_after: Option<&str>) -> Response {
            let mut upstream = axum::http::Response::builder().status(status);
            if let Some(hint) = retry_after {
                upstream = upstream.header(header::RETRY_AFTER, hint);
            }
            let response = reqwest::Response::from(upstream.body(Vec::new()).unwrap());
            upstream_failure(&response).into_response()
        }

        for (upstream, want_status, want_code) in [
            (429u16, 429u16, ErrorCode::Overloaded),
            (529, 529, ErrorCode::Overloaded),
            (404, 404, ErrorCode::UnknownDataset),
            (400, 400, ErrorCode::MalformedRequest),
            (500, 500, ErrorCode::UpstreamUnavailable),
            // ADR-007: 503 is unavailability, not congestion.
            (503, 503, ErrorCode::UpstreamUnavailable),
        ] {
            let response = refusal(upstream, None).await;
            assert_eq!(
                response.status().as_u16(),
                want_status,
                "upstream {upstream}"
            );
            assert_eq!(
                response.extensions().get::<ErrorCode>().copied(),
                Some(want_code),
                "upstream {upstream}"
            );
            assert_eq!(
                response.headers().contains_key(header::RETRY_AFTER),
                want_code == ErrorCode::Overloaded,
                "upstream {upstream}: a hint is owed by OVERLOADED and invented by nothing else"
            );
        }

        let response = refusal(429, Some("30")).await;
        assert_eq!(response.headers()[header::RETRY_AFTER], "30");
        let response = refusal(429, Some("0")).await;
        assert_eq!(response.headers()[header::RETRY_AFTER], "1");

        // The proxy forwards an upstream hint whatever its class or form, so this
        // surface must too: one response cannot carry it on /stream and not here.
        for hint in ["30", "Wed, 21 Oct 2015 07:28:00 GMT"] {
            let response = refusal(503, Some(hint)).await;
            assert_eq!(response.headers()[header::RETRY_AFTER], hint);
        }
    }

    /// IB-5 fixes a local upstream failure at 502. 503 read as "the Portal has no
    /// capacity", which is a different fault with a different owner.
    #[tokio::test]
    async fn an_upstream_failure_answers_502() {
        let response = BlockNumberLookupError::Unavailable("Hotblocks stream error".to_owned())
            .into_response();

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(
            response.extensions().get::<ErrorCode>().copied(),
            Some(ErrorCode::UpstreamUnavailable)
        );
    }

    #[tokio::test]
    async fn get_blocknumber_by_timestamp_uses_archive_path_first() {
        let archive_calls = Arc::new(AtomicUsize::new(0));
        let hotblocks_calls = Arc::new(AtomicUsize::new(0));

        let result = get_blocknumber_by_timestamp_inner(
            true,
            true,
            {
                let archive_calls = archive_calls.clone();
                || async move {
                    archive_calls.fetch_add(1, Ordering::Relaxed);
                    Ok(42)
                }
            },
            {
                let hotblocks_calls = hotblocks_calls.clone();
                || async move {
                    hotblocks_calls.fetch_add(1, Ordering::Relaxed);
                    Ok(43)
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(result.block_number, 42);
        assert_eq!(result.data_source, BlockNumberDataSource::Network);
        assert_eq!(archive_calls.load(Ordering::Relaxed), 1);
        assert_eq!(hotblocks_calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn get_blocknumber_by_timestamp_falls_back_to_hotblocks_after_archive_miss() {
        let archive_calls = Arc::new(AtomicUsize::new(0));
        let hotblocks_calls = Arc::new(AtomicUsize::new(0));

        let result = get_blocknumber_by_timestamp_inner(
            true,
            true,
            {
                let archive_calls = archive_calls.clone();
                || async move {
                    archive_calls.fetch_add(1, Ordering::Relaxed);
                    Err(BlockNumberLookupError::NotFound(
                        "No chunk found for timestamp".to_string(),
                    ))
                }
            },
            {
                let hotblocks_calls = hotblocks_calls.clone();
                || async move {
                    hotblocks_calls.fetch_add(1, Ordering::Relaxed);
                    Ok(84)
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(result.block_number, 84);
        assert_eq!(result.data_source, BlockNumberDataSource::Hotblocks);
        assert_eq!(archive_calls.load(Ordering::Relaxed), 1);
        assert_eq!(hotblocks_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn get_blocknumber_by_timestamp_uses_hotblocks_when_archive_is_absent() {
        let archive_calls = Arc::new(AtomicUsize::new(0));
        let hotblocks_calls = Arc::new(AtomicUsize::new(0));

        let result = get_blocknumber_by_timestamp_inner(
            false,
            true,
            {
                let archive_calls = archive_calls.clone();
                || async move {
                    archive_calls.fetch_add(1, Ordering::Relaxed);
                    Ok(42)
                }
            },
            {
                let hotblocks_calls = hotblocks_calls.clone();
                || async move {
                    hotblocks_calls.fetch_add(1, Ordering::Relaxed);
                    Ok(168)
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(result.block_number, 168);
        assert_eq!(result.data_source, BlockNumberDataSource::Hotblocks);
        assert_eq!(archive_calls.load(Ordering::Relaxed), 0);
        assert_eq!(hotblocks_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn hotblocks_lookup_uses_status_and_stream_query() {
        assert_eq!(
            get_hotblocks_blocknumber_by_timestamp_inner(
                1_700_000_000,
                "evm",
                "base-mainnet",
                fake_status(),
                fake_stream,
            )
            .await
            .unwrap(),
            100
        );
        assert_eq!(
            get_hotblocks_blocknumber_by_timestamp_inner(
                1_700_000_005,
                "evm",
                "base-mainnet",
                fake_status(),
                fake_stream,
            )
            .await
            .unwrap(),
            101
        );
        assert!(matches!(
            get_hotblocks_blocknumber_by_timestamp_inner(
                1_700_009_999,
                "evm",
                "base-mainnet",
                fake_status(),
                fake_stream,
            )
            .await,
            Err(BlockNumberLookupError::NotFound(_))
        ));
    }

    #[test]
    fn decode_hotblocks_stream_body_accepts_plain_utf8() {
        let body = b"{\"header\":{\"number\":100}}\n";

        assert_eq!(
            decode_hotblocks_stream_body(false, body).unwrap(),
            "{\"header\":{\"number\":100}}\n"
        );
    }

    #[test]
    fn decode_hotblocks_stream_body_accepts_gzip_by_header() {
        let body = gzip(b"{\"header\":{\"number\":100}}\n");

        assert_eq!(
            decode_hotblocks_stream_body(true, &body).unwrap(),
            "{\"header\":{\"number\":100}}\n"
        );
    }

    #[test]
    fn decode_hotblocks_stream_body_accepts_gzip_by_magic_bytes() {
        let body = gzip(b"{\"header\":{\"number\":101}}\n");

        assert_eq!(
            decode_hotblocks_stream_body(false, &body).unwrap(),
            "{\"header\":{\"number\":101}}\n"
        );
    }

    #[test]
    fn decode_hotblocks_stream_body_rejects_invalid_utf8() {
        let err = decode_hotblocks_stream_body(false, &[0xff]).unwrap_err();

        assert!(err.to_string().contains("invalid utf-8"));
    }

    #[test]
    fn decode_hotblocks_stream_body_rejects_invalid_gzip() {
        assert!(decode_hotblocks_stream_body(true, b"not gzip").is_err());
    }

    #[tokio::test]
    async fn an_unfinished_stream_body_is_an_upstream_failure_not_our_bug() {
        let truncated = futures::stream::iter([
            Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"partial")),
            Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection reset mid-body",
            )),
        ]);
        let upstream = axum::http::Response::builder()
            .status(200)
            .body(reqwest::Body::wrap_stream(truncated))
            .unwrap();

        let error = collect_hotblocks_stream(reqwest::Response::from(upstream))
            .await
            .unwrap_err();

        assert!(
            matches!(error, BlockNumberLookupError::UpstreamIncomplete(_)),
            "a truncated body is the upstream's fault: {error:?}"
        );
        assert_eq!(error.class(), ErrorCode::UpstreamUnavailable);
        assert_eq!(error.into_response().status(), StatusCode::BAD_GATEWAY);

        // Once the body arrived, decoding it is local processing and remains a 500.
        let malformed = axum::http::Response::builder()
            .status(200)
            .header(header::CONTENT_ENCODING, "gzip")
            .body(Vec::from("not gzip"))
            .unwrap();
        let error = collect_hotblocks_stream(reqwest::Response::from(malformed))
            .await
            .unwrap_err();
        assert!(matches!(error, BlockNumberLookupError::Internal(_)));
        assert_eq!(
            error.into_response().status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    fn fake_status() -> Status {
        Status {
            kind: "evm".to_string(),
            retention_strategy: serde_json::json!({"Head": 20}),
            data: Some(StatusData {
                first_block: 100,
                last_block: 101,
                last_block_hash:
                    "0x0202020202020202020202020202020202020202020202020202020202020202".to_string(),
                last_block_timestamp: Some(1_700_000_012),
                finalized_head: Some(BlockRef {
                    number: 101,
                    hash: "0x0202020202020202020202020202020202020202020202020202020202020202"
                        .to_string(),
                }),
            }),
        }
    }

    async fn fake_stream(body: String) -> Result<String, BlockNumberLookupError> {
        let query: serde_json::Value = serde_json::from_str(&body).unwrap();
        if query["fromBlock"] != 100 || query["toBlock"] != 101 {
            return Err(BlockNumberLookupError::Internal(format!(
                "unexpected query range: {query}"
            )));
        }

        Ok(concat!(
            "{\"header\":{\"number\":100,\"timestamp\":1700000000}}\n",
            "{\"header\":{\"number\":101,\"timestamp\":1700000012}}\n"
        )
        .to_string())
    }

    #[tokio::test]
    async fn retry_once_on_unavailable_retries_after_stale_retention_window() {
        let calls = Arc::new(AtomicUsize::new(0));

        let result = retry_once_on_unavailable(|| {
            let calls = calls.clone();
            async move {
                if calls.fetch_add(1, Ordering::Relaxed) == 0 {
                    // What a pruned range looks like coming back from HotblocksDB.
                    Err(BlockNumberLookupError::Unavailable(
                        "Hotblocks stream failed with status 400 Bad Request".to_string(),
                    ))
                } else {
                    Ok(42)
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(result, 42);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn retry_once_on_unavailable_gives_up_after_a_single_retry() {
        let calls = Arc::new(AtomicUsize::new(0));

        let result = retry_once_on_unavailable(|| {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::Relaxed);
                Err(BlockNumberLookupError::Unavailable(
                    "still gone".to_string(),
                ))
            }
        })
        .await;

        assert!(matches!(
            result,
            Err(BlockNumberLookupError::Unavailable(ref m)) if m == "still gone"
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn retry_once_on_unavailable_does_not_retry_other_errors() {
        for error in [
            BlockNumberLookupError::NotFound("block not in hotblocks".to_string()),
            BlockNumberLookupError::Internal("stream processing error".to_string()),
            // DC-4: a read stall is UPSTREAM-FAILURE but never replayed — the read budget
            // is spent, so a second attempt would push past the client's deadline (REQ-22).
            BlockNumberLookupError::UpstreamIncomplete("Hotblocks status error".to_string()),
        ] {
            let calls = Arc::new(AtomicUsize::new(0));
            let error = Arc::new(std::sync::Mutex::new(Some(error)));

            let result = retry_once_on_unavailable(|| {
                let calls = calls.clone();
                let error = error.clone();
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    Err(error.lock().unwrap().take().expect("called twice"))
                }
            })
            .await;

            assert!(result.is_err());
            assert_eq!(calls.load(Ordering::Relaxed), 1);
        }
    }

    /// The state change the retry exists for surfaces as a refusal *response* — a
    /// pruned range answers 400 — which the taxonomy turned from `Unavailable` into
    /// `Upstream`. Every test above builds its error by hand, so the retry could stop
    /// covering the case it was added for without one of them noticing. An overload is
    /// excluded: the source just shed load.
    /// reqwest reports a body that never finished and a body that wasn't JSON with the same
    /// `Kind::Decode`, so the split has to be structural. Only the second is our bug: the
    /// first is DC-4's, and answering `internal_error` paged on a hotblocks roll while
    /// telling the client not to retry.
    #[tokio::test]
    async fn an_unfinished_status_body_is_an_upstream_failure_not_our_bug() {
        let incomplete =
            BlockNumberLookupError::UpstreamIncomplete("Hotblocks status error".to_owned());
        assert_eq!(incomplete.class(), ErrorCode::UpstreamUnavailable);
        assert_eq!(
            incomplete.class().error_type().as_str(),
            "availability_error"
        );
        assert_eq!(
            incomplete.into_response().status(),
            StatusCode::BAD_GATEWAY,
            "IB-5 binds upstream_unavailable to 502"
        );

        // A body that arrived and wasn't JSON stays ours.
        let ours = BlockNumberLookupError::Internal("Hotblocks status error".to_owned());
        assert_eq!(ours.class(), ErrorCode::Internal);
        assert_eq!(ours.class().error_type().as_str(), "api_error");
    }

    #[tokio::test]
    async fn retry_once_on_unavailable_retries_an_upstream_refusal_but_not_an_overload() {
        let attempts = |code, status: u16| async move {
            let calls = Arc::new(AtomicUsize::new(0));
            let _ = retry_once_on_unavailable(|| {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    Err(BlockNumberLookupError::Upstream {
                        status: StatusCode::from_u16(status).unwrap(),
                        code,
                        retry_after: None,
                    })
                }
            })
            .await;
            calls.load(Ordering::Relaxed)
        };

        assert_eq!(attempts(ErrorCode::MalformedRequest, 400).await, 2);
        assert_eq!(attempts(ErrorCode::UpstreamUnavailable, 500).await, 2);
        assert_eq!(attempts(ErrorCode::Overloaded, 429).await, 1);
        // The same request reproduces these exactly; a retry only costs a round trip.
        assert_eq!(attempts(ErrorCode::UnknownDataset, 404).await, 1);
        assert_eq!(attempts(ErrorCode::BaseBlockMismatch, 409).await, 1);
    }

    /// The status call resolved its own HTTP status inside the client, so a refusal
    /// reached the endpoint as a transport error: every one of them answered 502
    /// `upstream_unavailable`, and a 404 was even retried. It is the one call to the
    /// real-time source that never saw the shared classifier (DC-4).
    #[tokio::test]
    async fn a_status_refusal_is_classified_like_every_other_upstream_response() {
        let respond = |status: u16, body: &'static str| async move {
            let upstream = axum::http::Response::builder()
                .status(status)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Vec::from(body))
                .unwrap();
            decode_status(reqwest::Response::from(upstream)).await
        };

        for (upstream, want_status, want_code) in [
            (404u16, 404u16, ErrorCode::UnknownDataset),
            (500, 500, ErrorCode::UpstreamUnavailable),
            (503, 503, ErrorCode::UpstreamUnavailable),
        ] {
            let error = respond(upstream, "refused").await.unwrap_err();
            assert_eq!(error.class(), want_code, "upstream {upstream}");
            assert_eq!(
                error.into_response().status().as_u16(),
                want_status,
                "upstream {upstream}"
            );
        }

        // A body that does not decode is ours, not the source's, and never retried.
        let error = respond(200, "not json").await.unwrap_err();
        assert_eq!(error.class(), ErrorCode::Internal);

        // A body that never finished is the source's. reqwest reports both as
        // `Kind::Decode`, so only reading before parsing separates them.
        let truncated = futures::stream::iter([
            Ok::<_, std::io::Error>(bytes::Bytes::from_static(br#"{"kind":"ev"#)),
            Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection reset mid-body",
            )),
        ]);
        let upstream = axum::http::Response::builder()
            .status(200)
            .header(header::CONTENT_TYPE, "application/json")
            .body(reqwest::Body::wrap_stream(truncated))
            .unwrap();
        let error = decode_status(reqwest::Response::from(upstream))
            .await
            .unwrap_err();
        assert!(
            matches!(error, BlockNumberLookupError::UpstreamIncomplete(_)),
            "a truncated body is DC-4's fault, not ours: {error:?}"
        );
        assert_eq!(error.class(), ErrorCode::UpstreamUnavailable);
        assert_eq!(error.into_response().status(), StatusCode::BAD_GATEWAY);

        // The shape the real-time source serves on /status.
        let status = respond(200, r#"{"kind":"evm","retentionStrategy":{},"data":null}"#).await;
        assert!(
            status.is_ok(),
            "a healthy status must still decode: {status:?}"
        );
    }
}
