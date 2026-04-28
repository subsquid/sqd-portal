use std::{future::Future, sync::Arc};

use axum::http::{header, StatusCode};
use futures::{pin_mut, StreamExt};
use tower_http::request_id::RequestId;

use crate::{
    config::Config,
    controller::task_manager::TaskManager,
    datasets::DatasetConfig,
    hotblocks::{HotblocksHandle, StreamMode},
    network::NetworkClient,
    types::{Compression, DatasetId, GenericError, ParsedQuery, StreamRequest},
    utils::{
        conversion::collect_to_string,
        internal_query::{build_blocknumber_query, find_block_in_chunk},
    },
};

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
    pub fn into_response(self) -> (StatusCode, axum::Json<GenericError>) {
        let (status, message) = match self {
            Self::NotFound(message) => (StatusCode::NOT_FOUND, message),
            Self::Internal(message) => (StatusCode::INTERNAL_SERVER_ERROR, message),
            Self::Unavailable(message) => (StatusCode::SERVICE_UNAVAILABLE, message),
        };

        (status, GenericError { message }.into())
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
) -> Result<u64, BlockNumberLookupError> {
    get_blocknumber_by_timestamp_inner(
        timestamp,
        dataset.network_id.is_some(),
        dataset.hotblocks.is_some(),
        |_| async {
            let dataset_id = dataset
                .network_id
                .as_ref()
                .expect("archive lookup should be called only for archival datasets");
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
        |_| async { get_hotblocks_blocknumber_by_timestamp(timestamp, hotblocks, dataset).await },
    )
    .await
}

async fn get_blocknumber_by_timestamp_inner<
    ArchiveLookup,
    ArchiveFuture,
    HotblocksLookup,
    HotblocksFuture,
>(
    timestamp: u64,
    has_archive: bool,
    has_hotblocks: bool,
    archive_lookup: ArchiveLookup,
    hotblocks_lookup: HotblocksLookup,
) -> Result<u64, BlockNumberLookupError>
where
    ArchiveLookup: FnOnce(u64) -> ArchiveFuture,
    ArchiveFuture: Future<Output = Result<u64, BlockNumberLookupError>>,
    HotblocksLookup: FnOnce(u64) -> HotblocksFuture,
    HotblocksFuture: Future<Output = Result<u64, BlockNumberLookupError>>,
{
    tracing::info!("has archive {has_archive} and hotblocks {has_hotblocks}");
    if has_archive {
        match archive_lookup(timestamp).await {
            Ok(n) => return Ok(n),
            Err(BlockNumberLookupError::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
    }

    if has_hotblocks {
        return hotblocks_lookup(timestamp).await;
    }

    Err(BlockNumberLookupError::NotFound(
        "No chunk found for timestamp".to_string(),
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
    let ts = timestamp * 1000; // milliseconds
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
    .await;

    if let Err(e) = js {
        tracing::warn!("stream processing error: {:?}", e);
        return Err(BlockNumberLookupError::Internal(
            "stream processing error".to_string(),
        ));
    };

    let js = js.unwrap();

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

    let pquery = build_blocknumber_query(&dataset.kind, data.first_block, data.last_block)
        .map_err(|e| {
            tracing::warn!("cannot build hotblocks blocknumber query: {:?}", e);
            BlockNumberLookupError::Internal(format!(
                "Cannot build timestamp query for {}",
                dataset.default_name
            ))
        })?;

    let response = hotblocks
        .stream(
            &dataset.default_name,
            &pquery.into_string(),
            StreamMode::RealTime,
        )
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

    let js = collect_hotblocks_stream(response).await.map_err(|e| {
        tracing::warn!("hotblocks stream processing error: {:?}", e);
        BlockNumberLookupError::Internal("hotblocks stream processing error".to_string())
    })?;

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

    if is_gzip || bytes.starts_with(&[0x1f, 0x8b]) {
        let mut decoder = flate2::read::GzDecoder::new(&bytes[..]);
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

    use super::*;

    #[tokio::test]
    async fn get_blocknumber_by_timestamp_uses_archive_path_first() {
        let archive_calls = Arc::new(AtomicUsize::new(0));
        let hotblocks_calls = Arc::new(AtomicUsize::new(0));

        let result = get_blocknumber_by_timestamp_inner(
            1_700_000_000,
            true,
            true,
            {
                let archive_calls = archive_calls.clone();
                |timestamp| async move {
                    archive_calls.fetch_add(1, Ordering::Relaxed);
                    assert_eq!(timestamp, 1_700_000_000);
                    Ok(42)
                }
            },
            {
                let hotblocks_calls = hotblocks_calls.clone();
                |_| async move {
                    hotblocks_calls.fetch_add(1, Ordering::Relaxed);
                    Ok(43)
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(result, 42);
        assert_eq!(archive_calls.load(Ordering::Relaxed), 1);
        assert_eq!(hotblocks_calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn get_blocknumber_by_timestamp_falls_back_to_hotblocks_after_archive_miss() {
        let archive_calls = Arc::new(AtomicUsize::new(0));
        let hotblocks_calls = Arc::new(AtomicUsize::new(0));

        let result = get_blocknumber_by_timestamp_inner(
            1_700_000_001,
            true,
            true,
            {
                let archive_calls = archive_calls.clone();
                |timestamp| async move {
                    archive_calls.fetch_add(1, Ordering::Relaxed);
                    assert_eq!(timestamp, 1_700_000_001);
                    Err(BlockNumberLookupError::NotFound(
                        "No chunk found for timestamp".to_string(),
                    ))
                }
            },
            {
                let hotblocks_calls = hotblocks_calls.clone();
                |timestamp| async move {
                    hotblocks_calls.fetch_add(1, Ordering::Relaxed);
                    assert_eq!(timestamp, 1_700_000_001);
                    Ok(84)
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(result, 84);
        assert_eq!(archive_calls.load(Ordering::Relaxed), 1);
        assert_eq!(hotblocks_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn get_blocknumber_by_timestamp_uses_hotblocks_when_archive_is_absent() {
        let archive_calls = Arc::new(AtomicUsize::new(0));
        let hotblocks_calls = Arc::new(AtomicUsize::new(0));

        let result = get_blocknumber_by_timestamp_inner(
            1_700_000_002,
            false,
            true,
            {
                let archive_calls = archive_calls.clone();
                |_| async move {
                    archive_calls.fetch_add(1, Ordering::Relaxed);
                    Ok(42)
                }
            },
            {
                let hotblocks_calls = hotblocks_calls.clone();
                |timestamp| async move {
                    hotblocks_calls.fetch_add(1, Ordering::Relaxed);
                    assert_eq!(timestamp, 1_700_000_002);
                    Ok(168)
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(result, 168);
        assert_eq!(archive_calls.load(Ordering::Relaxed), 0);
        assert_eq!(hotblocks_calls.load(Ordering::Relaxed), 1);
    }
}
