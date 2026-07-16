use std::{sync::Arc, time::Duration};

use axum::{
    body::Body,
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Extension,
};
use bytes::Bytes;
use futures::{Stream, StreamExt};

use crate::{
    config::Config,
    controller::task_manager::TaskManager,
    datasets::DatasetConfig,
    hotblocks::{traceless_key, HotblocksHandle, StreamMode},
    http_server::{forward_hotblocks_response, forward_response},
    network::NetworkClient,
    types::{Compression, DatasetId, RequestError, StreamRequest},
    utils::conversion::{join_gzip_default, recompress_gzip},
};

/// [INTERNAL] Archival stream
///
/// Returns only archived blocks (no hotblocks). Server-side limits apply to the query.
#[utoipa::path(
    post,
    path = "/datasets/{dataset}/archival-stream",
    params(
        ("dataset" = String, Path, description = "Dataset name"),
    ),
    request_body = StreamRequestBody,
    responses(
        (status = 200, description = "Archival data stream", content_type = "application/jsonl",
            headers(
                ("X-Sqd-Finalized-Head-Number" = Option<u64>, description = "Finalized head block number"),
                ("X-Sqd-Finalized-Head-Hash" = Option<String>, description = "Finalized head block hash"),
                ("X-Sqd-Head-Number" = Option<u64>, description = "Last available block number"),
            )),
        (status = 204, description = "No new blocks available in the requested range"),
        (status = 400, description = "Invalid request parameters or query"),
        (status = 404, description = "Dataset not found"),
        (status = 529, description = "Overloaded - not enough compute units or all workers busy, retry later"),
        (status = 500, description = "Internal server error"),
        (status = 503, description = "Service temporarily unavailable"),
    ),
    tag = "Archival stream"
)]
pub(crate) async fn run_archival_stream_restricted(
    task_manager: Extension<Arc<TaskManager>>,
    network: Extension<Arc<NetworkClient>>,
    config: Extension<Arc<Config>>,
    dataset_id: DatasetId,
    raw_request: StreamRequest,
) -> Response {
    let request = restrict_request(&config, raw_request);
    run_archival_stream(task_manager, network, config, dataset_id, request).await
}

/// [INTERNAL] Archival stream (debug)
///
/// Debug variant of /archival-stream without server-side query restrictions.
#[utoipa::path(
    post,
    path = "/datasets/{dataset}/archival-stream/debug",
    params(
        ("dataset" = String, Path, description = "Dataset name"),
    ),
    request_body = StreamRequestBody,
    responses(
        (status = 200, description = "Archival data stream", content_type = "application/jsonl",
            headers(
                ("X-Sqd-Finalized-Head-Number" = Option<u64>, description = "Finalized head block number"),
                ("X-Sqd-Finalized-Head-Hash" = Option<String>, description = "Finalized head block hash"),
                ("X-Sqd-Head-Number" = Option<u64>, description = "Last available block number"),
            )),
        (status = 204, description = "No new blocks available in the requested range"),
        (status = 400, description = "Invalid request parameters or query"),
        (status = 404, description = "Dataset not found"),
        (status = 529, description = "Overloaded - not enough compute units or all workers busy, retry later"),
        (status = 500, description = "Internal server error"),
        (status = 503, description = "Service temporarily unavailable"),
    ),
    tag = "Archival stream"
)]
pub(crate) async fn run_archival_stream(
    Extension(task_manager): Extension<Arc<TaskManager>>,
    Extension(network): Extension<Arc<NetworkClient>>,
    Extension(config): Extension<Arc<Config>>,
    dataset_id: DatasetId,
    mut request: StreamRequest,
) -> Response {
    let mut res = Response::builder();
    res = res.header(DATA_SOURCE_HEADER, DATA_SOURCE_NETWORK);
    if let Some(head) = network.head(&dataset_id) {
        // Don't use hotblocks data source at all for this endpoint.
        res = res
            .header(FINALIZED_NUMBER_HEADER, head.number)
            .header(FINALIZED_HASH_HEADER, head.hash)
            .header(HEAD_NUMBER_HEADER, head.number);
    }

    request.dataset_id = dataset_id;
    let compression = request.compression;

    match task_manager.spawn_stream(request).await {
        Ok(stream) => {
            let body = response_body(stream, compression, config.use_gzjoin);
            res.header(header::CONTENT_TYPE, "application/jsonl")
                .header(header::CONTENT_ENCODING, compression.content_encoding())
                .body(body)
                .unwrap()
        }
        Err(RequestError::NoData) => {
            // Delay request from this client for 5 seconds to avoid unnecessary retries.
            tokio::time::sleep(Duration::from_secs(5)).await;
            res.status(StatusCode::NO_CONTENT).body(().into()).unwrap()
        }
        Err(e) => e.into_response(),
    }
}

/// Stream
///
/// Returns blocks matching the query. May include unfinalized blocks from the chain tip.
#[utoipa::path(
    post,
    path = "/datasets/{dataset}/stream",
    params(
        ("dataset" = String, Path, description = "Dataset name"),
    ),
    request_body = StreamRequestBody,
    responses(
        (status = 200, description = "Real-time data stream", content_type = "application/jsonl",
            headers(
                ("X-Sqd-Finalized-Head-Number" = Option<u64>, description = "Finalized head block number"),
                ("X-Sqd-Finalized-Head-Hash" = Option<String>, description = "Finalized head block hash"),
                ("X-Sqd-Head-Number" = Option<u64>, description = "Last available block number"),
            )),
        (status = 204, description = "No new blocks available in the requested range"),
        (status = 400, description = "Invalid request parameters or query"),
        (status = 404, description = "Dataset not found"),
        (status = 409, description = "\
Parent block hash mismatch — the `parentHash` of the first requested block does not match \
`query.parentBlockHash`. This typically indicates a chain reorganization relative to the \
client's state.\n\n\
The response body is a JSON object with a single `previousBlocks` array of `{number, hash}` \
pairs from the current canonical chain. The array may have arbitrary length but is \
guaranteed to contain at least the parent of the first requested block. Clients should \
find the last known shared ancestor and re-request from there; if no shared block is \
found, request earlier blocks until one is."),
        (status = 529, description = "Overloaded - not enough compute units or all workers busy, retry later"),
        (status = 500, description = "Internal server error"),
        (status = 503, description = "Service temporarily unavailable"),
    ),
    tag = "Stream"
)]
pub(crate) async fn run_stream(
    Extension(task_manager): Extension<Arc<TaskManager>>,
    Extension(config): Extension<Arc<Config>>,
    Extension(network): Extension<Arc<NetworkClient>>,
    Extension(hotblocks): Extension<Arc<HotblocksHandle>>,
    dataset: DatasetConfig,
    request: StreamRequest,
) -> Response {
    run_stream_internal(
        task_manager,
        config,
        network,
        hotblocks,
        dataset,
        request,
        StreamMode::RealTime,
    )
    .await
}

/// Finalized stream
///
/// Returns only finalized blocks matching the query. Same request format as /stream;
/// no chain reorganizations (no 409 responses).
#[utoipa::path(
    post,
    path = "/datasets/{dataset}/finalized-stream",
    params(
        ("dataset" = String, Path, description = "Dataset name"),
    ),
    request_body = StreamRequestBody,
    responses(
        (status = 200, description = "Finalized data stream", content_type = "application/jsonl",
            headers(
                ("X-Sqd-Finalized-Head-Number" = Option<u64>, description = "Finalized head block number"),
                ("X-Sqd-Finalized-Head-Hash" = Option<String>, description = "Finalized head block hash"),
                ("X-Sqd-Head-Number" = Option<u64>, description = "Last available block number"),
            )),
        (status = 204, description = "No new blocks available in the requested range"),
        (status = 400, description = "Invalid request parameters or query"),
        (status = 404, description = "Dataset not found"),
        (status = 529, description = "Overloaded - not enough compute units or all workers busy, retry later"),
        (status = 500, description = "Internal server error"),
        (status = 503, description = "Service temporarily unavailable"),
    ),
    tag = "Stream"
)]
pub(crate) async fn run_finalized_stream(
    Extension(task_manager): Extension<Arc<TaskManager>>,
    Extension(config): Extension<Arc<Config>>,
    Extension(network): Extension<Arc<NetworkClient>>,
    Extension(hotblocks): Extension<Arc<HotblocksHandle>>,
    dataset: DatasetConfig,
    request: StreamRequest,
) -> Response {
    run_stream_internal(
        task_manager,
        config,
        network,
        hotblocks,
        dataset,
        request,
        StreamMode::Finalized,
    )
    .await
}

async fn run_stream_internal(
    task_manager: Arc<TaskManager>,
    config: Arc<Config>,
    network: Arc<NetworkClient>,
    hotblocks: Arc<HotblocksHandle>,
    dataset: DatasetConfig,
    request: StreamRequest,
    mode: StreamMode,
) -> Response {
    let request = restrict_request(&config, request);

    // Use the traceless hotblocks variant when the query doesn't need traces/statediffs
    let hotblocks_name = if dataset
        .hotblocks
        .as_ref()
        .and_then(|r| r.dataset_traceless.as_ref())
        .is_some()
        && request.query.is_traceless()
    {
        traceless_key(&dataset.default_name)
    } else {
        dataset.default_name.clone()
    };

    match dataset.network_id.clone() {
        Some(dataset_id)
            if network
                .get_height(&dataset_id)
                .is_some_and(|height| request.query.first_block() <= height) =>
        {
            stream_from_network(
                task_manager,
                config,
                network,
                hotblocks,
                request,
                mode,
                dataset_id,
                hotblocks_name,
            )
            .await
        }
        _ if dataset.hotblocks.is_some() => {
            stream_from_hotblocks(network, hotblocks, dataset, request, mode, hotblocks_name).await
        }
        Some(dataset_id) => stream_after_network_head(&network, dataset_id).await,
        None => {
            unreachable!(
                "invalid dataset name should have been handled in the ClientRequest parser"
            )
        }
    }
}

async fn stream_from_network(
    task_manager: Arc<TaskManager>,
    config: Arc<Config>,
    network: Arc<NetworkClient>,
    hotblocks: Arc<HotblocksHandle>,
    mut request: StreamRequest,
    mode: StreamMode,
    dataset_id: DatasetId,
    hotblocks_name: String,
) -> Response {
    let archival_head = network.head(&dataset_id);
    let head_task = tokio::spawn({
        let archival_head = archival_head.clone();
        async move {
            let head = match mode {
                StreamMode::RealTime => hotblocks.get_head(&hotblocks_name).await,
                StreamMode::Finalized => hotblocks.get_finalized_head(&hotblocks_name).await,
            };
            head.ok().or(archival_head.clone()).map(|b| b.number)
        }
    });

    request.dataset_id = dataset_id;
    let compression = request.compression;

    let stream = match task_manager.spawn_stream(request).await {
        Ok(stream) => stream,
        Err(e) => {
            let mut response = e.into_response();
            response
                .headers_mut()
                .insert(DATA_SOURCE_HEADER, DATA_SOURCE_NETWORK);
            return response;
        }
    };

    let mut res = Response::builder().header(DATA_SOURCE_HEADER, DATA_SOURCE_NETWORK);
    if let Some(head) = archival_head {
        res = res.header(FINALIZED_NUMBER_HEADER, head.number);
        res = res.header(FINALIZED_HASH_HEADER, head.hash);
    }
    if let Some(head) = head_task.await.unwrap() {
        res = res.header(HEAD_NUMBER_HEADER, head);
    }
    let body = response_body(stream, compression, config.use_gzjoin);
    res.header(header::CONTENT_TYPE, "application/jsonl")
        .header(header::CONTENT_ENCODING, compression.content_encoding())
        .body(body)
        .unwrap()
}

async fn stream_from_hotblocks(
    network: Arc<NetworkClient>,
    hotblocks: Arc<HotblocksHandle>,
    dataset: DatasetConfig,
    request: StreamRequest,
    mode: StreamMode,
    hotblocks_name: String,
) -> Response {
    let first_block = request.query.first_block();
    let hotblocks_response = hotblocks
        .stream(&hotblocks_name, &request.query.into_string(), mode)
        .await;

    let mut res = match hotblocks_response {
        Ok(response) => {
            if !response.status().is_success()
                && should_treat_as_hotblocks_gap(
                    &network,
                    &hotblocks,
                    dataset.network_id.as_ref(),
                    &hotblocks_name,
                    first_block,
                )
                .await
            {
                return delayed_no_content_response(DATA_SOURCE_REALTIME).await;
            }

            forward_response(&dataset.default_name, response)
        }
        Err(e) => forward_hotblocks_response(&dataset.default_name, Err(e)),
    };

    res.headers_mut()
        .insert(DATA_SOURCE_HEADER, DATA_SOURCE_REALTIME);
    res
}

async fn stream_after_network_head(network: &NetworkClient, dataset_id: DatasetId) -> Response {
    let mut res = Response::builder();
    res = res.header(DATA_SOURCE_HEADER, DATA_SOURCE_NETWORK);
    if let Some(head) = network.head(&dataset_id) {
        res = res.header(FINALIZED_NUMBER_HEADER, head.number);
        res = res.header(FINALIZED_HASH_HEADER, head.hash);
        res = res.header(HEAD_NUMBER_HEADER, head.number);
    }
    delayed_no_content_response_with_builder(res).await
}

fn response_body(
    stream: impl Stream<Item = Vec<u8>> + Send + 'static,
    compression: Compression,
    use_gzjoin: bool,
) -> Body {
    match compression {
        Compression::Gzip if use_gzjoin => Body::from_stream(join_gzip_default(stream)),
        Compression::Gzip => Body::from_stream(recompress_gzip(stream)),
        Compression::Zstd => {
            Body::from_stream(stream.map(|result| std::io::Result::Ok(Bytes::from_owner(result))))
        }
    }
}

pub(crate) fn restrict_request(config: &Config, request: StreamRequest) -> StreamRequest {
    let max_chunks = match (request.max_chunks, config.max_chunks_per_stream) {
        (Some(requested), Some(limit)) => Some(requested.min(limit)),
        (Some(requested), None) => Some(requested),
        (None, Some(limit)) => Some(limit),
        (None, None) => None,
    };
    StreamRequest {
        buffer_size: request.buffer_size.min(config.max_buffer_size),
        // This is an operator-controlled memory cap, not a client-tunable request option.
        max_stored_results_per_chunk: config.max_stored_results_per_chunk.max(1),
        max_chunks,
        timeout_quantile: config.default_timeout_quantile,
        retries: config.default_retries,
        ..request
    }
}

async fn should_treat_as_hotblocks_gap(
    network: &NetworkClient,
    hotblocks: &HotblocksHandle,
    dataset_id: Option<&DatasetId>,
    dataset_name: &str,
    first_block: u64,
) -> bool {
    let Some(dataset_id) = dataset_id else {
        return false;
    };
    let Some(height) = network.get_height(dataset_id) else {
        return false;
    };
    if first_block <= height {
        return false;
    }

    // Only convert a hotblocks gap to delayed 204 when status exposes the
    // retained range. If status has no data or cannot be fetched, preserve
    // the old behavior and let hotblocks answer the stream request.
    let Ok(status) = hotblocks.get_status(dataset_name).await else {
        return false;
    };
    let Some(data) = status.data else {
        return false;
    };

    is_hotblocks_gap(first_block, height, data.first_block)
}

fn is_hotblocks_gap(first_block: u64, archival_height: u64, hotblocks_first_block: u64) -> bool {
    first_block > archival_height && first_block < hotblocks_first_block
}

async fn delayed_no_content_response(data_source: HeaderValue) -> Response {
    delayed_no_content_response_with_builder(
        Response::builder().header(DATA_SOURCE_HEADER, data_source),
    )
    .await
}

async fn delayed_no_content_response_with_builder(
    builder: axum::http::response::Builder,
) -> Response {
    // Delay request from this client for 5 seconds to avoid unnecessary retries.
    tokio::time::sleep(Duration::from_secs(5)).await;
    builder
        .status(StatusCode::NO_CONTENT)
        .body(().into())
        .unwrap()
}

const FINALIZED_NUMBER_HEADER: &str = "x-sqd-finalized-head-number";
const FINALIZED_HASH_HEADER: &str = "x-sqd-finalized-head-hash";
const HEAD_NUMBER_HEADER: &str = "x-sqd-head-number";
pub(crate) const DATA_SOURCE_HEADER: &str = "x-sqd-data-source";
pub(crate) const DATA_SOURCE_NETWORK_METRIC: &str = "network";
pub(crate) const DATA_SOURCE_REALTIME_METRIC: &str = "real_time";
const DATA_SOURCE_NETWORK: HeaderValue = HeaderValue::from_static(DATA_SOURCE_NETWORK_METRIC);
const DATA_SOURCE_REALTIME: HeaderValue = HeaderValue::from_static(DATA_SOURCE_REALTIME_METRIC);

#[cfg(test)]
mod tests {
    use super::is_hotblocks_gap;

    #[test]
    fn hotblocks_gap_detects_missing_blocks_after_archival_height() {
        assert!(is_hotblocks_gap(101, 100, 105));
    }

    #[test]
    fn hotblocks_gap_does_not_apply_at_archival_height() {
        assert!(!is_hotblocks_gap(100, 100, 105));
    }

    #[test]
    fn hotblocks_gap_does_not_apply_before_archival_height() {
        assert!(!is_hotblocks_gap(99, 100, 105));
    }

    #[test]
    fn hotblocks_gap_does_not_apply_at_first_hotblock() {
        assert!(!is_hotblocks_gap(105, 100, 105));
    }

    #[test]
    fn hotblocks_gap_does_not_apply_after_first_hotblock() {
        assert!(!is_hotblocks_gap(200, 100, 105));
    }

    #[test]
    fn hotblocks_gap_requires_actual_gap_between_sources() {
        assert!(!is_hotblocks_gap(101, 100, 101));
    }
}
