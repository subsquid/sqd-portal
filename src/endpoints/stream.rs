use std::{pin::Pin, sync::Arc, time::Duration};

use axum::{
    body::Body,
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Extension,
};
use bytes::Bytes;
use futures::{Stream, StreamExt};

use crate::{
    commercial::{
        meter::{
            tap_gzip_stream, tap_input_chunks, tap_input_frames, tap_plain_stream,
            tap_wire_stream_without_cut,
        },
        CommercialGrant, DataSource, Endpoint, GrantedLimits, MeterHandle, UsageReporter,
    },
    config::Config,
    controller::task_manager::TaskManager,
    datasets::DatasetConfig,
    hotblocks::{traceless_key, HotblocksHandle, StreamMode},
    http_server::forward_hotblocks_response,
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
    grant: Option<Extension<CommercialGrant>>,
    reporter: Option<Extension<Arc<dyn UsageReporter>>>,
    dataset: DatasetConfig,
    raw_request: StreamRequest,
) -> Response {
    let Some(dataset_id) = dataset.network_id.clone() else {
        return (
            StatusCode::NOT_FOUND,
            format!(
                "Dataset {} doesn't have archival data",
                dataset.default_name
            ),
        )
            .into_response();
    };
    let request = restrict_request(&config, raw_request);
    let request =
        restrict_request_to_grant(request, grant.as_ref().map(|grant| &grant.0.granted.limits));
    let meter = meter_from_extensions(
        grant,
        reporter,
        Endpoint::ArchivalStream,
        dataset.default_name,
        request.request_id.clone(),
    );
    run_archival_stream_inner(task_manager, network, config, dataset_id, request, meter).await
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
    grant: Option<Extension<CommercialGrant>>,
    reporter: Option<Extension<Arc<dyn UsageReporter>>>,
    dataset: DatasetConfig,
    request: StreamRequest,
) -> Response {
    let Some(dataset_id) = dataset.network_id.clone() else {
        return (
            StatusCode::NOT_FOUND,
            format!(
                "Dataset {} doesn't have archival data",
                dataset.default_name
            ),
        )
            .into_response();
    };
    let request = restrict_archival_debug_request(&config, request, grant.as_ref().map(|g| &g.0));
    let meter = meter_from_extensions(
        grant,
        reporter,
        Endpoint::ArchivalStream,
        dataset.default_name,
        request.request_id.clone(),
    );
    run_archival_stream_inner(
        Extension(task_manager),
        Extension(network),
        Extension(config),
        dataset_id,
        request,
        meter,
    )
    .await
}

async fn run_archival_stream_inner(
    Extension(task_manager): Extension<Arc<TaskManager>>,
    Extension(network): Extension<Arc<NetworkClient>>,
    Extension(config): Extension<Arc<Config>>,
    dataset_id: DatasetId,
    mut request: StreamRequest,
    meter: Option<MeterHandle>,
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
            if let Some(meter) = &meter {
                meter.set_data_source(DataSource::Network);
            }
            let body = response_body(stream, compression, config.use_gzjoin, meter);
            res.header(header::CONTENT_TYPE, "application/jsonl")
                .header(header::CONTENT_ENCODING, compression.content_encoding())
                .body(body)
                .unwrap()
        }
        Err(RequestError::NoData) => {
            discard_meter(&meter);
            // Delay request from this client for 5 seconds to avoid unnecessary retries.
            tokio::time::sleep(Duration::from_secs(5)).await;
            res.status(StatusCode::NO_CONTENT).body(().into()).unwrap()
        }
        Err(e) => {
            discard_meter(&meter);
            e.into_response()
        }
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
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_stream(
    Extension(task_manager): Extension<Arc<TaskManager>>,
    Extension(config): Extension<Arc<Config>>,
    Extension(network): Extension<Arc<NetworkClient>>,
    Extension(hotblocks): Extension<Arc<HotblocksHandle>>,
    grant: Option<Extension<CommercialGrant>>,
    reporter: Option<Extension<Arc<dyn UsageReporter>>>,
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
        grant,
        reporter,
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
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_finalized_stream(
    Extension(task_manager): Extension<Arc<TaskManager>>,
    Extension(config): Extension<Arc<Config>>,
    Extension(network): Extension<Arc<NetworkClient>>,
    Extension(hotblocks): Extension<Arc<HotblocksHandle>>,
    grant: Option<Extension<CommercialGrant>>,
    reporter: Option<Extension<Arc<dyn UsageReporter>>>,
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
        grant,
        reporter,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_stream_internal(
    task_manager: Arc<TaskManager>,
    config: Arc<Config>,
    network: Arc<NetworkClient>,
    hotblocks: Arc<HotblocksHandle>,
    dataset: DatasetConfig,
    request: StreamRequest,
    mode: StreamMode,
    grant: Option<Extension<CommercialGrant>>,
    reporter: Option<Extension<Arc<dyn UsageReporter>>>,
) -> Response {
    let request = restrict_request(&config, request);
    let request =
        restrict_request_to_grant(request, grant.as_ref().map(|grant| &grant.0.granted.limits));
    let endpoint = match mode {
        StreamMode::RealTime => Endpoint::Stream,
        StreamMode::Finalized => Endpoint::FinalizedStream,
    };
    let meter = meter_from_extensions(
        grant,
        reporter,
        endpoint,
        dataset.default_name.clone(),
        request.request_id.clone(),
    );

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
                meter,
            )
            .await
        }
        _ if dataset.hotblocks.is_some() => {
            stream_from_hotblocks(
                network,
                hotblocks,
                dataset,
                request,
                mode,
                hotblocks_name,
                meter,
            )
            .await
        }
        Some(dataset_id) => {
            discard_meter(&meter);
            stream_after_network_head(&network, dataset_id).await
        }
        None => {
            unreachable!(
                "invalid dataset name should have been handled in the ClientRequest parser"
            )
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn stream_from_network(
    task_manager: Arc<TaskManager>,
    config: Arc<Config>,
    network: Arc<NetworkClient>,
    hotblocks: Arc<HotblocksHandle>,
    mut request: StreamRequest,
    mode: StreamMode,
    dataset_id: DatasetId,
    hotblocks_name: String,
    meter: Option<MeterHandle>,
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
            discard_meter(&meter);
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
    if let Some(meter) = &meter {
        meter.set_data_source(DataSource::Network);
    }
    let body = response_body(stream, compression, config.use_gzjoin, meter);
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
    meter: Option<MeterHandle>,
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
                discard_meter(&meter);
                return delayed_no_content_response(DATA_SOURCE_REALTIME).await;
            }

            if response.status().is_success() {
                if let Some(meter) = &meter {
                    meter.set_data_source(DataSource::RealTime);
                }
                forward_response_metered(response, meter)
            } else {
                forward_hotblocks_error_response(response, meter)
            }
        }
        Err(e) => {
            discard_meter(&meter);
            forward_hotblocks_response(Err(e))
        }
    };

    res.headers_mut()
        .insert(DATA_SOURCE_HEADER, DATA_SOURCE_REALTIME);
    res
}

fn forward_response_metered(
    response: reqwest::Response,
    meter: Option<MeterHandle>,
) -> axum::response::Response {
    let is_gzip = response
        .headers()
        .get(header::CONTENT_ENCODING)
        .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"gzip"));
    let mut builder = Response::builder().status(response.status());
    for (key, value) in response.headers() {
        builder = builder.header(key, value);
    }
    let body = match meter {
        Some(meter) if is_gzip => {
            // Hotblocks arrive as an opaque proxied gzip byte stream, so there is
            // no safe input-frame boundary where we can stop and flush a trailer.
            // A cutoff here is an abrupt compressed-body termination; clients see
            // it as a disconnect and receive the reject on their next poll.
            Body::from_stream(tap_gzip_stream(response.bytes_stream(), meter))
        }
        Some(meter) => Body::from_stream(tap_plain_stream(response.bytes_stream(), meter)),
        None => Body::from_stream(response.bytes_stream()),
    };
    builder.body(body).unwrap()
}

fn forward_hotblocks_error_response(
    response: reqwest::Response,
    meter: Option<MeterHandle>,
) -> axum::response::Response {
    if let Some(meter) = &meter {
        meter.set_data_source(DataSource::RealTime);
        meter.mark_error();
    }

    let mut builder = Response::builder().status(response.status());
    for (key, value) in response.headers() {
        if meter.is_some() && key == header::CONTENT_LENGTH {
            continue;
        }
        builder = builder.header(key, value);
    }
    let body = match meter {
        Some(meter) => {
            Body::from_stream(tap_wire_stream_without_cut(response.bytes_stream(), meter))
        }
        None => Body::from_stream(response.bytes_stream()),
    };
    builder.body(body).unwrap()
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
    meter: Option<MeterHandle>,
) -> Body {
    let stream: Pin<Box<dyn Stream<Item = Vec<u8>> + Send>> = match (meter.clone(), compression) {
        (Some(meter), Compression::Gzip) => tap_input_chunks(stream, meter),
        (Some(meter), Compression::Zstd) => tap_input_frames(stream, compression, meter),
        (None, _) => Box::pin(stream),
    };

    match compression {
        Compression::Gzip if use_gzjoin => match meter {
            Some(meter) => Body::from_stream(tap_wire_stream_without_cut(
                join_gzip_default(stream),
                meter,
            )),
            None => Body::from_stream(join_gzip_default(stream)),
        },
        Compression::Gzip => match meter {
            Some(meter) => {
                Body::from_stream(tap_wire_stream_without_cut(recompress_gzip(stream), meter))
            }
            None => Body::from_stream(recompress_gzip(stream)),
        },
        Compression::Zstd => {
            let output = stream.map(|result| std::io::Result::Ok(Bytes::from_owner(result)));
            match meter {
                Some(meter) => Body::from_stream(tap_wire_stream_without_cut(output, meter)),
                None => Body::from_stream(output),
            }
        }
    }
}

fn meter_from_extensions(
    grant: Option<Extension<CommercialGrant>>,
    reporter: Option<Extension<Arc<dyn UsageReporter>>>,
    endpoint: Endpoint,
    dataset: String,
    request_id: String,
) -> Option<MeterHandle> {
    let grant = grant?;
    let reporter = reporter?;
    let snapshot_store = grant.0.snapshot_store.clone();
    match (grant.0.tally.clone(), grant.0.registry.clone()) {
        (Some(tally), Some(registry)) => Some(MeterHandle::new_enforced(
            grant.0.granted.clone(),
            request_id,
            endpoint,
            dataset,
            reporter.0,
            tally,
            registry,
            snapshot_store,
        )),
        _ => Some(MeterHandle::new(
            grant.0.granted.principal.clone(),
            request_id,
            endpoint,
            dataset,
            reporter.0,
            grant.0.granted.quota_version,
        )),
    }
}

fn discard_meter(meter: &Option<MeterHandle>) {
    if let Some(meter) = meter {
        meter.discard();
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

fn restrict_request_to_grant(
    request: StreamRequest,
    limits: Option<&GrantedLimits>,
) -> StreamRequest {
    let grant_max_chunks = limits
        .and_then(|limits| limits.max_chunks)
        .and_then(|max_chunks| usize::try_from(max_chunks).ok());
    let Some(grant_max_chunks) = grant_max_chunks else {
        return request;
    };
    let max_chunks = match request.max_chunks {
        Some(requested) => Some(requested.min(grant_max_chunks)),
        None => Some(grant_max_chunks),
    };
    StreamRequest {
        max_chunks,
        ..request
    }
}

fn restrict_archival_debug_request(
    config: &Config,
    request: StreamRequest,
    grant: Option<&CommercialGrant>,
) -> StreamRequest {
    let request = if grant.is_some_and(|grant| grant.granted.principal.api_key_id.is_some()) {
        restrict_request(config, request)
    } else {
        request
    };
    restrict_request_to_grant(request, grant.map(|grant| &grant.granted.limits))
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
    use std::{
        io::{Read, Write},
        sync::{Arc, Mutex},
    };

    use axum::body::to_bytes;
    use flate2::{read::MultiGzDecoder, write::GzEncoder, Compression as GzipCompression};
    use futures::stream;

    use super::*;
    use crate::{
        commercial::{
            ActiveStreamRegistry, Granted, OnExceed, Principal, StreamUsageEvent, TallyStore,
            UsageReporter, UsageStatus,
        },
        types::ParsedQuery,
    };

    #[derive(Default)]
    struct RecordingReporter {
        events: Mutex<Vec<StreamUsageEvent>>,
    }

    impl UsageReporter for RecordingReporter {
        fn report(&self, event: StreamUsageEvent) {
            self.events.lock().unwrap().push(event);
        }
    }

    fn meter(reporter: Arc<RecordingReporter>) -> MeterHandle {
        MeterHandle::new(
            Principal {
                account_id: "account".to_string(),
                api_key_id: Some("key".to_string()),
            },
            "request".to_string(),
            Endpoint::Stream,
            "ethereum-mainnet".to_string(),
            reporter,
            0,
        )
    }

    fn enforced_meter(reporter: Arc<RecordingReporter>, quota_remaining: i64) -> MeterHandle {
        MeterHandle::new_enforced(
            Granted {
                principal: Principal {
                    account_id: "account".to_string(),
                    api_key_id: Some("key".to_string()),
                },
                tally_account_id: None,
                entitled_chains: None,
                entitled_traces: None,
                limits: GrantedLimits::default(),
                on_exceed: OnExceed::Reject,
                quota_version: 7,
                quota_remaining_bytes: Some(quota_remaining),
                concurrency_permit: None,
            },
            "request".to_string(),
            Endpoint::Stream,
            "ethereum-mainnet".to_string(),
            reporter,
            Arc::new(TallyStore::default()),
            Arc::new(ActiveStreamRegistry::default()),
            None,
        )
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), GzipCompression::default());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    fn gunzip_all(bytes: &[u8]) -> Vec<u8> {
        let mut decoded = Vec::new();
        MultiGzDecoder::new(bytes)
            .read_to_end(&mut decoded)
            .unwrap();
        decoded
    }

    async fn collect_response_body(
        frames: Vec<Vec<u8>>,
        compression: Compression,
        use_gzjoin: bool,
        meter: Option<MeterHandle>,
    ) -> Vec<u8> {
        to_bytes(
            response_body(stream::iter(frames), compression, use_gzjoin, meter),
            usize::MAX,
        )
        .await
        .unwrap()
        .to_vec()
    }

    fn split_frame(frame: Vec<u8>) -> Vec<Vec<u8>> {
        let split = frame.len() / 2;
        vec![frame[..split].to_vec(), frame[split..].to_vec()]
    }

    async fn reqwest_response(status: StatusCode, body: Vec<u8>) -> reqwest::Response {
        let app = axum::Router::new().route(
            "/hotblocks",
            axum::routing::get(move || {
                let body = body.clone();
                async move {
                    (
                        status,
                        [(header::CONTENT_LENGTH, body.len().to_string())],
                        Body::from(body),
                    )
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        reqwest::get(format!("http://{addr}/hotblocks"))
            .await
            .unwrap()
    }

    fn stream_request(max_chunks: Option<usize>) -> StreamRequest {
        StreamRequest {
            dataset_id: DatasetId::from_url("test-dataset"),
            dataset_name: "test-dataset".to_string(),
            query: ParsedQuery::try_from(
                r#"{"type":"evm","fromBlock":1,"fields":{"block":{"number":true}}}"#.to_string(),
            )
            .unwrap(),
            request_id: "request".to_string(),
            buffer_size: 10,
            max_stored_results_per_chunk: 2,
            max_chunks,
            timeout_quantile: 0.5,
            retries: 1,
            compression: Compression::Gzip,
            skip_parent_hash_validation: false,
        }
    }

    fn config_for_restriction_test() -> Config {
        serde_yaml::from_str(
            r#"
hostname: portal.example
sqd_network:
  datasets: https://example.invalid/datasets.yaml
max_buffer_size: 4
max_stored_results_per_chunk: 1
max_chunks_per_stream: 2
default_timeout_quantile: 0.25
default_retries: 0
"#,
        )
        .expect("test config should parse")
    }

    fn commercial_grant(
        api_key_id: Option<&str>,
        grant_max_chunks: Option<u64>,
    ) -> CommercialGrant {
        CommercialGrant {
            granted: Granted {
                principal: Principal {
                    account_id: "account".to_string(),
                    api_key_id: api_key_id.map(str::to_string),
                },
                tally_account_id: None,
                entitled_chains: None,
                entitled_traces: None,
                limits: GrantedLimits {
                    max_chunks: grant_max_chunks,
                    ..GrantedLimits::default()
                },
                on_exceed: OnExceed::Reject,
                quota_version: 7,
                quota_remaining_bytes: Some(100),
                concurrency_permit: None,
            },
            tally: None,
            registry: None,
            snapshot_store: None,
        }
    }

    #[test]
    fn grant_caps_max_chunks_after_operator_restriction() {
        let capped = restrict_request_to_grant(
            stream_request(Some(10)),
            Some(&GrantedLimits {
                max_chunks: Some(3),
                ..GrantedLimits::default()
            }),
        );
        assert_eq!(capped.max_chunks, Some(3));

        let capped = restrict_request_to_grant(
            stream_request(None),
            Some(&GrantedLimits {
                max_chunks: Some(4),
                ..GrantedLimits::default()
            }),
        );
        assert_eq!(capped.max_chunks, Some(4));
    }

    #[test]
    fn archival_debug_applies_operator_restrictions_for_keyed_callers_only() {
        let config = config_for_restriction_test();
        let keyed = commercial_grant(Some("key"), Some(3));
        let anonymous = commercial_grant(None, Some(3));
        let request = stream_request(Some(10));

        let keyed_request = restrict_archival_debug_request(&config, request.clone(), Some(&keyed));
        assert_eq!(keyed_request.buffer_size, 4);
        assert_eq!(keyed_request.max_stored_results_per_chunk, 1);
        assert_eq!(keyed_request.max_chunks, Some(2));
        assert_eq!(keyed_request.timeout_quantile, 0.25);
        assert_eq!(keyed_request.retries, 0);

        let anonymous_request =
            restrict_archival_debug_request(&config, request.clone(), Some(&anonymous));
        assert_eq!(anonymous_request.buffer_size, request.buffer_size);
        assert_eq!(
            anonymous_request.max_stored_results_per_chunk,
            request.max_stored_results_per_chunk
        );
        assert_eq!(anonymous_request.max_chunks, Some(3));
        assert_eq!(anonymous_request.timeout_quantile, request.timeout_quantile);
        assert_eq!(anonymous_request.retries, request.retries);

        let oss_request = restrict_archival_debug_request(&config, request.clone(), None);
        assert_eq!(oss_request.buffer_size, request.buffer_size);
        assert_eq!(oss_request.max_chunks, request.max_chunks);
    }

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

    #[tokio::test(start_paused = true)]
    async fn delayed_no_content_discards_meter_without_usage_event() {
        let reporter = Arc::new(RecordingReporter::default());
        let meter = Some(MeterHandle::new(
            Principal {
                account_id: "account".to_string(),
                api_key_id: Some("key".to_string()),
            },
            "request".to_string(),
            Endpoint::Stream,
            "ethereum-mainnet".to_string(),
            reporter.clone(),
            0,
        ));
        discard_meter(&meter);

        let task = tokio::spawn(delayed_no_content_response(DATA_SOURCE_REALTIME));
        tokio::time::advance(Duration::from_secs(5)).await;
        let response = task.await.unwrap();
        drop(meter);

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(reporter.events.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn hotblocks_error_response_marks_meter_error_without_cutting_body() {
        let reporter = Arc::new(RecordingReporter::default());
        let body = b"hotblocks failed with detail".to_vec();
        let response = reqwest_response(StatusCode::BAD_GATEWAY, body.clone()).await;
        let forwarded =
            forward_hotblocks_error_response(response, Some(enforced_meter(reporter.clone(), 1)));

        assert_eq!(forwarded.status(), StatusCode::BAD_GATEWAY);
        assert!(!forwarded.headers().contains_key(header::CONTENT_LENGTH));
        let forwarded_body = to_bytes(forwarded.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&forwarded_body[..], &body);

        let event = reporter.events.lock().unwrap().pop().unwrap();
        assert_eq!(event.status, UsageStatus::Error);
        assert_eq!(event.data_source, DataSource::RealTime);
        assert_eq!(event.logical_bytes, 0);
        assert_eq!(event.wire_bytes, body.len() as u64);
    }

    #[tokio::test]
    async fn response_body_gzjoin_taps_chunks_without_changing_wire_output() {
        let plain = b"{\"gzjoin\":true}\n";
        let frames = split_frame(gzip(plain));
        let expected = collect_response_body(frames.clone(), Compression::Gzip, true, None).await;
        let reporter = Arc::new(RecordingReporter::default());
        let metered = collect_response_body(
            frames,
            Compression::Gzip,
            true,
            Some(meter(reporter.clone())),
        )
        .await;

        assert_eq!(metered, expected);
        let event = reporter.events.lock().unwrap().pop().unwrap();
        assert_eq!(event.logical_bytes, plain.len() as u64);
        assert_eq!(event.wire_bytes, expected.len() as u64);
        assert_eq!(event.chunks, 2);
        assert_eq!(event.status, UsageStatus::Completed);
    }

    #[tokio::test]
    async fn response_body_recompress_taps_chunks_without_changing_wire_output() {
        let plain = b"{\"recompress\":true}\n";
        let frames = split_frame(gzip(plain));
        let expected = collect_response_body(frames.clone(), Compression::Gzip, false, None).await;
        let reporter = Arc::new(RecordingReporter::default());
        let metered = collect_response_body(
            frames,
            Compression::Gzip,
            false,
            Some(meter(reporter.clone())),
        )
        .await;

        assert_eq!(metered, expected);
        let event = reporter.events.lock().unwrap().pop().unwrap();
        assert_eq!(event.logical_bytes, plain.len() as u64);
        assert_eq!(event.wire_bytes, expected.len() as u64);
        assert_eq!(event.chunks, 2);
        assert_eq!(event.status, UsageStatus::Completed);
    }

    #[tokio::test]
    async fn response_body_zstd_taps_frames_without_changing_wire_output() {
        let plain = b"{\"zstd\":true}\n";
        let frames = split_frame(zstd::stream::encode_all(&plain[..], 0).unwrap());
        let expected = collect_response_body(frames.clone(), Compression::Zstd, false, None).await;
        let reporter = Arc::new(RecordingReporter::default());
        let metered = collect_response_body(
            frames,
            Compression::Zstd,
            false,
            Some(meter(reporter.clone())),
        )
        .await;

        assert_eq!(metered, expected);
        let event = reporter.events.lock().unwrap().pop().unwrap();
        assert_eq!(event.logical_bytes, plain.len() as u64);
        assert_eq!(event.wire_bytes, expected.len() as u64);
        assert_eq!(event.chunks, 2);
        assert_eq!(event.status, UsageStatus::Completed);
    }

    async fn assert_gzip_cut_by_quota_decodes(use_gzjoin: bool) {
        let first = b"{\"block\":1}\n";
        let second = b"{\"block\":2}\n";
        let third = b"{\"block\":3}\n";
        let frames = vec![gzip(first), gzip(second), gzip(third)];
        let reporter = Arc::new(RecordingReporter::default());
        let metered = collect_response_body(
            frames,
            Compression::Gzip,
            use_gzjoin,
            Some(enforced_meter(
                reporter.clone(),
                first.len().saturating_add(1) as i64,
            )),
        )
        .await;

        assert_eq!(
            gunzip_all(&metered),
            [first.as_slice(), second.as_slice()].concat()
        );
        let event = reporter.events.lock().unwrap().pop().unwrap();
        assert_eq!(event.logical_bytes, (first.len() + second.len()) as u64);
        assert_eq!(event.chunks, 2);
        assert_eq!(event.status, UsageStatus::CutQuota);
    }

    #[tokio::test]
    async fn response_body_gzjoin_cut_by_quota_finishes_decodable_gzip() {
        assert_gzip_cut_by_quota_decodes(true).await;
    }

    #[tokio::test]
    async fn response_body_recompress_cut_by_quota_finishes_decodable_gzip() {
        assert_gzip_cut_by_quota_decodes(false).await;
    }

    #[tokio::test]
    async fn response_body_zstd_cut_by_quota_keeps_decodable_frames() {
        let first = b"{\"zstd\":1}\n";
        let second = b"{\"zstd\":2}\n";
        let third = b"{\"zstd\":3}\n";
        let frames = vec![
            zstd::stream::encode_all(&first[..], 0).unwrap(),
            zstd::stream::encode_all(&second[..], 0).unwrap(),
            zstd::stream::encode_all(&third[..], 0).unwrap(),
        ];
        let reporter = Arc::new(RecordingReporter::default());
        let metered = collect_response_body(
            frames,
            Compression::Zstd,
            false,
            Some(enforced_meter(
                reporter.clone(),
                first.len().saturating_add(1) as i64,
            )),
        )
        .await;

        let decoded = zstd::stream::decode_all(&metered[..]).unwrap();
        assert_eq!(decoded, [first.as_slice(), second.as_slice()].concat());
        let event = reporter.events.lock().unwrap().pop().unwrap();
        assert_eq!(event.logical_bytes, (first.len() + second.len()) as u64);
        assert_eq!(event.chunks, 2);
        assert_eq!(event.status, UsageStatus::CutQuota);
    }
}
