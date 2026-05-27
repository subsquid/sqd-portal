use std::{future::Future, sync::Arc};

use axum::{
    extract::Path,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Extension,
};
use futures::{pin_mut, StreamExt};
use tower_http::request_id::RequestId;

use crate::{
    config::Config,
    controller::task_manager::TaskManager,
    datasets::DatasetConfig,
    hotblocks::{HotblocksHandle, Status, StreamMode},
    network::NetworkClient,
    openapi::BlockNumberResponse,
    types::{Compression, DatasetId, GenericError, ParsedQuery, StreamRequest},
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
        (status = 404, description = "No block found for timestamp"),
        (status = 500, description = "Internal server error"),
        (status = 503, description = "Upstream data source unavailable"),
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
    Unavailable(String),
}

impl BlockNumberLookupError {
    /// Convert a resolver error into the timestamp endpoint's HTTP response.
    pub fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::NotFound(message) => (StatusCode::NOT_FOUND, message),
            Self::Internal(message) => (StatusCode::INTERNAL_SERVER_ERROR, message),
            Self::Unavailable(message) => (StatusCode::SERVICE_UNAVAILABLE, message),
        };

        (status, axum::Json(GenericError { message })).into_response()
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
            return Err(BlockNumberLookupError::Unavailable(
                "SQD Network error".to_string(),
            ));
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

async fn get_hotblocks_blocknumber_by_timestamp(
    timestamp: u64,
    hotblocks: &HotblocksHandle,
    dataset: &DatasetConfig,
) -> Result<u64, BlockNumberLookupError> {
    let status = hotblocks
        .get_status(&dataset.default_name)
        .await
        .map_err(|e| {
            tracing::warn!("hotblocks status error: {:?}", e);
            BlockNumberLookupError::Unavailable("Hotblocks status error".to_string())
        })?;

    get_hotblocks_blocknumber_by_timestamp_inner(
        timestamp,
        &dataset.kind,
        &dataset.default_name,
        status,
        |query| async move {
            let response = hotblocks
                .stream(&dataset.default_name, &query, StreamMode::RealTime)
                .await
                .map_err(|e| {
                    tracing::warn!("hotblocks stream error: {:?}", e);
                    BlockNumberLookupError::Unavailable("Hotblocks stream error".to_string())
                })?;

            let status = response.status();
            if !status.is_success() {
                tracing::warn!("hotblocks stream failed with status {}", status);
                return Err(BlockNumberLookupError::Unavailable(format!(
                    "Hotblocks stream failed with status {status}"
                )));
            }

            collect_hotblocks_stream(response).await.map_err(|e| {
                tracing::warn!("hotblocks stream processing error: {:?}", e);
                BlockNumberLookupError::Internal("hotblocks stream processing error".to_string())
            })
        },
    )
    .await
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

async fn collect_hotblocks_stream(response: reqwest::Response) -> anyhow::Result<String> {
    let is_gzip = response
        .headers()
        .get(header::CONTENT_ENCODING)
        .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"gzip"));
    let bytes = response.bytes().await?;

    decode_hotblocks_stream_body(is_gzip, bytes.as_ref())
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
}
