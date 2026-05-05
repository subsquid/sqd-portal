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
    hotblocks::{HotblocksHandle, StreamMode},
    http_server::{forward_hotblocks_response, forward_response},
    network::NetworkClient,
    types::{Compression, DatasetId, RequestError, StreamRequest},
    utils::conversion::{join_gzip_default, recompress_gzip},
};

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
                dataset,
                request,
                mode,
                dataset_id,
            )
            .await
        }
        _ if dataset.hotblocks.is_some() => {
            stream_from_hotblocks(network, hotblocks, dataset, request, mode).await
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
    dataset: DatasetConfig,
    mut request: StreamRequest,
    mode: StreamMode,
    dataset_id: DatasetId,
) -> Response {
    let archival_head = network.head(&dataset_id);
    let head_task = tokio::spawn({
        let archival_head = archival_head.clone();
        async move {
            let ds = &dataset.default_name;
            let head = match mode {
                StreamMode::RealTime => hotblocks.get_head(ds).await,
                StreamMode::Finalized => hotblocks.get_finalized_head(ds).await,
            };
            head.ok().or(archival_head.clone()).map(|b| b.number)
        }
    });

    request.dataset_id = dataset_id;
    let compression = request.compression;

    let stream = match task_manager.spawn_stream(request).await {
        Ok(stream) => stream,
        Err(e) => return e.into_response(),
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
) -> Response {
    let first_block = request.query.first_block();
    let hotblocks_response = hotblocks
        .stream(&dataset.default_name, &request.query.into_string(), mode)
        .await;

    let mut res = match hotblocks_response {
        Ok(response) => {
            if !response.status().is_success()
                && should_treat_as_hotblocks_gap(
                    &network,
                    &hotblocks,
                    dataset.network_id.as_ref(),
                    &dataset.default_name,
                    first_block,
                )
                .await
            {
                return delayed_no_content_response(DATA_SOURCE_REALTIME).await;
            }

            forward_response(response)
        }
        Err(e) => forward_hotblocks_response(Err(e)),
    };

    if res.status().is_success() {
        res.headers_mut()
            .append(DATA_SOURCE_HEADER, DATA_SOURCE_REALTIME);
    }
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

fn restrict_request(config: &Config, request: StreamRequest) -> StreamRequest {
    let max_chunks = match (request.max_chunks, config.max_chunks_per_stream) {
        (Some(requested), Some(limit)) => Some(requested.min(limit)),
        (Some(requested), None) => Some(requested),
        (None, Some(limit)) => Some(limit),
        (None, None) => None,
    };
    StreamRequest {
        buffer_size: request.buffer_size.min(config.max_buffer_size),
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
const DATA_SOURCE_HEADER: &str = "x-sqd-data-source";
const DATA_SOURCE_NETWORK: HeaderValue = HeaderValue::from_static("network");
const DATA_SOURCE_REALTIME: HeaderValue = HeaderValue::from_static("real_time");

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
