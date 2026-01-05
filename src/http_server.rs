use std::time::Duration;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use axum::http::{HeaderValue, Method};
use axum::{
    async_trait,
    body::Body,
    extract::{FromRequest, FromRequestParts, Path, Query, Request},
    http::{header, request::Parts, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension, RequestExt, Router,
};
use futures::{pin_mut, StreamExt};
use prometheus_client::registry::Registry;
use serde::Serialize;
use serde_json::{json, Value};
use sqd_contract_client::PeerId;
use tokio::time::Instant;
use tower_http::cors::{Any, CorsLayer};
use tower_http::decompression::RequestDecompressionLayer;
use tower_http::request_id::{
    MakeRequestUuid, PropagateRequestIdLayer, RequestId, SetRequestIdLayer,
};

use crate::datasets::DatasetConfig;
use crate::hotblocks::{HotblocksErr, StreamMode};
use crate::types::api_types::AvailableDatasetApiResponse;
use crate::utils::conversion::{collect_to_string, json_lines_to_json, recompress_gzip};
use crate::utils::internal_query::{build_blocknumber_query, find_block_in_chunk};
use crate::utils::logging::MethodRouterExt;
use crate::{
    config::Config,
    controller::task_manager::TaskManager,
    hotblocks::HotblocksHandle,
    network::{NetworkClient, NoWorker},
    types::{ChunkId, ClientRequest, DatasetId, GenericError, ParsedQuery, RequestError},
    utils::logging,
};

pub async fn run_server(
    task_manager: Arc<TaskManager>,
    network_client: Arc<NetworkClient>,
    metrics_registry: Registry,
    addr: SocketAddr,
    config: Arc<Config>,
    hotblocks: Arc<HotblocksHandle>,
) -> anyhow::Result<()> {
    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS, Method::PUT])
        .allow_headers(Any)
        .allow_origin(Any);

    tracing::info!("Starting HTTP server listening on {addr}");
    let app = Router::new()
        .layer(
            // This layer should be called before the response reaches trace layers
            PropagateRequestIdLayer::x_request_id(),
        )
        // Portal status
        .route("/status", get(get_status).endpoint("/status"))
        .route("/datasets", get(get_datasets).endpoint("/datasets"))
        // Streaming data
        .route(
            "/datasets/:dataset/archival-stream",
            post(run_archival_stream_restricted).endpoint("/archival-stream"),
        )
        .route(
            "/datasets/:dataset/archival-stream/debug",
            post(run_archival_stream).endpoint("/archival-stream/debug"),
        )
        .route(
            "/datasets/:dataset/finalized-stream",
            post(run_finalized_stream).endpoint("/finalized-stream"),
        )
        .route(
            "/datasets/:dataset/stream",
            post(run_stream).endpoint("/stream"),
        )
        // Getting head
        .route(
            "/datasets/:dataset/archival-head",
            get(get_archival_head).endpoint("/archival-head"),
        )
        .route(
            "/datasets/:dataset/finalized-head",
            get(get_finalized_head).endpoint("/finalized-head"),
        )
        .route("/datasets/:dataset/head", get(get_head).endpoint("/head"))
        // Dataset info
        .route(
            "/datasets/:dataset/state",
            get(get_dataset_state).endpoint("/state"),
        )
        .route(
            "/datasets/:dataset/metadata",
            get(get_dataset_metadata).endpoint("/metadata"),
        )
        .route(
            "/datasets/:dataset/timestamps/:timestamp/block",
            get(get_blocknumber_by_timestamp).endpoint("/timestamps/block"),
        )
        // Backward compatibility routes
        .route(
            "/datasets/:dataset/archival-stream/height",
            get(get_height).endpoint("/height"),
        )
        .route(
            "/datasets/:dataset_id/query/:worker_id",
            post(execute_query).endpoint("/query"),
        )
        .route(
            "/datasets/:dataset/height",
            get(get_height).endpoint("/height"),
        )
        .route(
            "/datasets/:dataset/:start_block/worker",
            get(get_worker).endpoint("/worker"),
        )
        // Internal routes
        .route(
            "/debug/workers",
            get(get_all_workers).endpoint("/debug/workers"),
        )
        .route(
            "/datasets/:dataset/:block/debug",
            get(get_debug_block).endpoint("/block/debug"),
        )
        .route("/metrics", get(get_metrics))
        .route("/ready", get(get_readiness))
        .route_layer(axum::middleware::from_fn(logging::middleware))
        .layer(RequestDecompressionLayer::new())
        .layer(cors)
        .layer(
            // This layer is added here to be applied before the request reaches trace layers
            SetRequestIdLayer::x_request_id(MakeRequestUuid::default()),
        )
        .layer(Extension(task_manager))
        .layer(Extension(network_client))
        .layer(Extension(config))
        .layer(Extension(Arc::new(metrics_registry)))
        .layer(Extension(hotblocks));

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    tracing::info!("HTTP server stopped");
    Ok(())
}

/// Gets the archival head corresponding to the /archival-stream behaviour.
///
/// The successful response is either "null" or a JSON object with "number" and "hash" fields.
async fn get_archival_head(
    Extension(network): Extension<Arc<NetworkClient>>,
    dataset: DatasetConfig,
) -> Response {
    // Prefer network data source to correspond to the /archival-stream behaviour
    if let Some(dataset_id) = dataset.network_id {
        let head = network.head(&dataset_id).map(|head| {
            json!({
                "number": head.number,
                "hash": head.hash,
            })
        });
        return axum::Json(head).into_response();
    }

    (
        StatusCode::NOT_FOUND,
        format!(
            "Dataset {} has no archival data source",
            dataset.default_name
        ),
    )
        .into_response()
}

/// Gets the finalized head corresponding to the /finalized-stream behaviour.
///
/// The successful response is either "null" or a JSON object with "number" and "hash" fields.
async fn get_finalized_head(
    Extension(hotblocks): Extension<Arc<HotblocksHandle>>,
    Extension(network): Extension<Arc<NetworkClient>>,
    dataset: DatasetConfig,
) -> Response {
    if dataset.hotblocks.is_some() {
        return forward_hotblocks_response(
            hotblocks
                .request_finalized_head(&dataset.default_name)
                .await,
        );
    }

    // Fall back to network data source
    if let Some(dataset_id) = dataset.network_id {
        let head = network.head(&dataset_id).map(|head| {
            json!({
                "number": head.number,
                "hash": head.hash,
            })
        });
        return axum::Json(head).into_response();
    }

    (
        StatusCode::NOT_FOUND,
        format!("Dataset {} has no data sources", dataset.default_name),
    )
        .into_response()
}

/// Gets the head corresponding to the /stream behaviour.
/// If the dataset has a real-time data source, its head is returned.
/// Otherwise, archival head is returned.
///
/// The successful response is either "null" or a JSON object with "number" and "hash" fields.
async fn get_head(
    Extension(hotblocks): Extension<Arc<HotblocksHandle>>,
    Extension(network): Extension<Arc<NetworkClient>>,
    dataset: DatasetConfig,
) -> Response {
    if dataset.hotblocks.is_some() {
        return forward_hotblocks_response(hotblocks.request_head(&dataset.default_name).await);
    }

    // Fall back to network data source
    if let Some(dataset_id) = dataset.network_id {
        let head = network.head(&dataset_id).map(|head| {
            json!({
                "number": head.number,
                "hash": head.hash,
            })
        });
        return axum::Json(head).into_response();
    }

    (
        StatusCode::NOT_FOUND,
        format!("Dataset {} has no data sources", dataset.default_name),
    )
        .into_response()
}

async fn run_archival_stream_restricted(
    task_manager: Extension<Arc<TaskManager>>,
    network: Extension<Arc<NetworkClient>>,
    Extension(config): Extension<Arc<Config>>,
    dataset_id: DatasetId,
    raw_request: ClientRequest,
) -> Response {
    let request = restrict_request(&config, raw_request);
    run_archival_stream(task_manager, network, dataset_id, request).await
}

async fn run_archival_stream(
    Extension(task_manager): Extension<Arc<TaskManager>>,
    Extension(network): Extension<Arc<NetworkClient>>,
    dataset_id: DatasetId,
    mut request: ClientRequest,
) -> Response {
    let mut res = Response::builder();
    res = res.header(DATA_SOURCE_HEADER, DATA_SOURCE_NETWORK);
    if let Some(head) = network.head(&dataset_id) {
        // Don't use hotblocks data source at all for this endpoint
        res = res
            .header(FINALIZED_NUMBER_HEADER, head.number)
            .header(FINALIZED_HASH_HEADER, head.hash)
            .header(HEAD_NUMBER_HEADER, head.number);
    }

    request.dataset_id = dataset_id;

    match task_manager.spawn_stream(request).await {
        Ok(stream) => res
            .header(header::CONTENT_TYPE, "application/jsonl")
            .header(header::CONTENT_ENCODING, "gzip")
            .body(Body::from_stream(recompress_gzip(stream)))
            .unwrap(),
        Err(RequestError::NoData) => {
            // Delay request from this client for 5 seconds to avoid unnecessary retries
            tokio::time::sleep(Duration::from_secs(5)).await;
            res.status(StatusCode::NO_CONTENT).body(().into()).unwrap()
        }
        Err(e) => e.into_response(),
    }
}

async fn run_stream(
    Extension(task_manager): Extension<Arc<TaskManager>>,
    Extension(config): Extension<Arc<Config>>,
    Extension(network): Extension<Arc<NetworkClient>>,
    Extension(hotblocks): Extension<Arc<HotblocksHandle>>,
    dataset: DatasetConfig,
    request: ClientRequest,
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

async fn run_finalized_stream(
    Extension(task_manager): Extension<Arc<TaskManager>>,
    Extension(config): Extension<Arc<Config>>,
    Extension(network): Extension<Arc<NetworkClient>>,
    Extension(hotblocks): Extension<Arc<HotblocksHandle>>,
    dataset: DatasetConfig,
    request: ClientRequest,
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
    request: ClientRequest,
    mode: StreamMode,
) -> Response {
    let mut request = restrict_request(&config, request);

    match dataset.network_id {
        // Prefer data from the network
        Some(dataset_id)
            if network
                .get_height(&dataset_id)
                .is_some_and(|height| request.query.first_block() <= height) =>
        {
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
            res.header(header::CONTENT_TYPE, "application/jsonl")
                .header(header::CONTENT_ENCODING, "gzip")
                .body(Body::from_stream(recompress_gzip(stream)))
                .unwrap()
        }

        // Then try hotblocks storage
        // TODO: if the query is below the first hotblock, return 500 instead of 400
        _ if dataset.hotblocks.is_some() => {
            let mut res = forward_hotblocks_response(
                hotblocks
                    .stream(&dataset.default_name, &request.query.into_string(), mode)
                    .await,
            );

            if res.status().is_success() {
                res.headers_mut()
                    .append(DATA_SOURCE_HEADER, DATA_SOURCE_REALTIME);
            }
            res
        }
        // If requested block is above the network height and there is no hotblocks storage, return 204
        Some(dataset_id) => {
            // Delay request from this client for 5 seconds to avoid unnecessary retries
            tokio::time::sleep(Duration::from_secs(5)).await;

            let mut res = Response::builder();
            res = res.header(DATA_SOURCE_HEADER, DATA_SOURCE_NETWORK);
            if let Some(head) = network.head(&dataset_id) {
                res = res.header(FINALIZED_NUMBER_HEADER, head.number);
                res = res.header(FINALIZED_HASH_HEADER, head.hash);
                res = res.header(HEAD_NUMBER_HEADER, head.number);
            }
            res.status(StatusCode::NO_CONTENT).body(().into()).unwrap()
        }
        None => {
            unreachable!(
                "invalid dataset name should have been handled in the ClientRequest parser"
            )
        }
    }
}

async fn get_status(Extension(client): Extension<Arc<NetworkClient>>) -> impl IntoResponse {
    let status = client.get_status();

    let Ok(mut res) = serde_json::to_value(status) else {
        return axum::Json(json!({"error": "failed to serialize status"})).into_response();
    };

    let Value::Object(ref mut map) = res else {
        return axum::Json(json!({"error": "failed to serialize status"})).into_response();
    };

    map.insert(
        "portal_version".into(),
        Value::String(env!("CARGO_PKG_VERSION").into()),
    );

    axum::Json(res).into_response()
}

async fn get_datasets(Extension(network): Extension<Arc<NetworkClient>>) -> impl IntoResponse {
    let datasets = network.datasets().read();
    let res: Vec<AvailableDatasetApiResponse> = datasets
        .iter()
        .map(|metadata| metadata.clone().into())
        .collect();

    axum::Json(res)
}

async fn get_dataset_state(
    dataset_id: DatasetId,
    Extension(network): Extension<Arc<NetworkClient>>,
) -> impl IntoResponse {
    axum::Json(network.dataset_state(&dataset_id))
}

async fn get_dataset_metadata(
    Extension(network): Extension<Arc<NetworkClient>>,
    Extension(hotblocks): Extension<Arc<HotblocksHandle>>,
    metadata: DatasetConfig,
) -> impl IntoResponse {
    let first_block = if let Some(first_block) = metadata
        .network_id
        .as_ref()
        .and_then(|dataset| network.first_existing_block(dataset))
    {
        Some(first_block)
    } else if let Ok(status) = hotblocks.get_status(&metadata.default_name).await {
        status.data.map(|d| d.first_block)
    } else {
        None
    };
    axum::Json(AvailableDatasetApiResponse::new(metadata, first_block))
}

#[derive(Debug, Clone, Serialize)]
struct BlockNumberResponse {
    block_number: u64,
}

async fn get_blocknumber_by_timestamp(
    Path((_, timestamp)): Path<(DatasetId, u64)>,
    Extension(req): Extension<RequestId>,
    Extension(network): Extension<Arc<NetworkClient>>,
    Extension(task_manager): Extension<Arc<TaskManager>>,
    Extension(config): Extension<Arc<Config>>,
    dataset: DatasetConfig,
) -> Result<axum::Json<BlockNumberResponse>, (StatusCode, axum::Json<GenericError>)> {
    let ts = timestamp * 1000; // milliseconds
    let dataset_id = dataset
        .network_id
        .expect("invalid dataset name should have been handled in request parser");

    let Ok(chunk) = network.find_chunk_by_timestamp(&dataset_id, ts) else {
        return Err((
            StatusCode::NOT_FOUND,
            GenericError {
                message: "No chunk found for timestamp".to_string(),
            }
            .into(),
        ));
    };

    let Ok(pquery) = build_blocknumber_query(&dataset.kind, chunk.first_block, chunk.last_block)
    else {
        tracing::warn!("cannot build blocknumber query for {}", dataset_id);
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            GenericError {
                message: format!("Cannot build timestamp query for {dataset_id}"),
            }
            .into(),
        ));
    };

    let request = build_request(
        &config,
        req.header_value().to_str().unwrap_or(""),
        pquery,
        dataset_id.clone(),
        dataset.default_name.clone(),
        Some(1),
    );

    let stream = match task_manager.spawn_stream(request).await {
        Ok(stream) => stream,
        Err(e) => {
            tracing::warn!("spawn stream error: {:?}", e);
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                GenericError {
                    message: "SQD Network error".to_string(),
                }
                .into(),
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
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            GenericError {
                message: "stream processing error".to_string(),
            }
            .into(),
        ));
    };

    let js = js.unwrap();

    let n = match find_block_in_chunk(timestamp, &js) {
        Ok(n) => n,
        Err(e) => {
            tracing::warn!("cannot find blocknumber in chunk: {:?}", e);
            return Err((
                StatusCode::NOT_FOUND,
                GenericError {
                    message: "block not in chunk".to_string(),
                }
                .into(),
            ));
        }
    };

    Ok(BlockNumberResponse { block_number: n }.into())
}

async fn get_debug_block(
    Path((_dataset, block)): Path<(String, u64)>,
    dataset_id: DatasetId,
    Extension(client): Extension<Arc<NetworkClient>>,
) -> axum::Json<serde_json::Value> {
    axum::Json(json!({
        "workers": client.get_workers(&dataset_id, block),
    }))
}

async fn get_all_workers(
    Extension(client): Extension<Arc<NetworkClient>>,
) -> axum::Json<serde_json::Value> {
    axum::Json(json!({
        "workers": client.get_all_workers(),
    }))
}

async fn get_metrics(Extension(registry): Extension<Arc<Registry>>) -> impl IntoResponse {
    lazy_static::lazy_static! {
        static ref HEADERS: HeaderMap = {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                "application/openmetrics-text; version=1.0.0; charset=utf-8"
                    .parse()
                    .unwrap(),
            );
            headers
        };
    }

    let mut buffer = String::new();
    prometheus_client::encoding::text::encode(&mut buffer, &registry).unwrap();

    (HEADERS.clone(), buffer)
}

async fn get_readiness(Extension(client): Extension<Arc<NetworkClient>>) -> impl IntoResponse {
    if client.is_ready() {
        return (StatusCode::OK, "Ready").into_response();
    }

    (StatusCode::SERVICE_UNAVAILABLE, "Not ready").into_response()
}

// Deprecated
async fn get_height(
    Extension(network): Extension<Arc<NetworkClient>>,
    Path(dataset): Path<String>,
    dataset_id: DatasetId,
) -> impl IntoResponse {
    match network.get_height(&dataset_id) {
        Some(height) => (StatusCode::OK, height.to_string()),
        None => (
            StatusCode::NOT_FOUND,
            format!("No data for dataset {dataset}"),
        ),
    }
}

// Deprecated
async fn get_worker(
    Path((dataset, start_block)): Path<(String, u64)>,
    dataset_id: DatasetId,
    Extension(client): Extension<Arc<NetworkClient>>,
    Extension(config): Extension<Arc<Config>>,
) -> Response {
    let worker_id = match client.find_worker(&dataset_id, start_block, false) {
        Ok(worker_id) => worker_id,
        Err(NoWorker::AllUnavailable) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("No available worker for dataset {dataset} block {start_block}"),
            )
                .into_response();
        }
        Err(NoWorker::Backoff(retry_at)) => {
            let seconds = retry_at.duration_since(Instant::now()).as_secs() + 1; // +1 for rounding up
            return Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .header(header::RETRY_AFTER, seconds)
                .body(Body::from("Too many requests"))
                .unwrap();
        }
    };

    (
        StatusCode::OK,
        format!(
            "{}/datasets/{}/query/{worker_id}",
            config.hostname,
            dataset_id.to_base64(),
        ),
    )
        .into_response()
}

// Deprecated
async fn execute_query(
    Path((dataset_id_encoded, worker_id)): Path<(String, PeerId)>,
    Extension(client): Extension<Arc<NetworkClient>>,
    Extension(req): Extension<RequestId>,
    query: ParsedQuery, // request body
) -> Response {
    let dataset_id = match DatasetId::from_base64(&dataset_id_encoded) {
        Ok(dataset_id) => dataset_id,
        Err(e) => {
            return (
                StatusCode::NOT_FOUND,
                format!("Couldn't parse dataset id: {e}"),
            )
                .into_response()
        }
    };

    let request_id = req.header_value().to_str().unwrap_or("").to_string();

    let Ok(chunk) = client.find_chunk(&dataset_id, query.first_block()) else {
        return RequestError::NoData.into_response();
    };
    let range = query
        .intersect_with(&chunk.block_range())
        .expect("Found chunk should intersect with query");
    let fut = client.query_worker(
        worker_id,
        request_id,
        ChunkId::new(dataset_id, chunk),
        range,
        query.into_string(),
        true,
    );
    let result = match fut.await {
        Ok(result) => result,
        Err(err) => return RequestError::from_query_error(err, worker_id).into_response(),
    };
    match json_lines_to_json(&result.data) {
        Ok(data) => Response::builder()
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_ENCODING, "gzip")
            .body(Body::from(data))
            .unwrap(),
        Err(e) => {
            RequestError::InternalError(format!("Couldn't convert response: {e}")).into_response()
        }
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for DatasetConfig
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        use axum::RequestPartsExt;

        let Path(args) = parts
            .extract::<Path<Vec<(String, String)>>>()
            .await
            .map_err(IntoResponse::into_response)?;
        let (_, alias) = args
            .first()
            .ok_or((StatusCode::NOT_FOUND, format!("not enough arguments")).into_response())?;
        let Extension(network) = parts
            .extract::<Extension<Arc<NetworkClient>>>()
            .await
            .map_err(IntoResponse::into_response)?;

        match network.dataset(&alias) {
            Some(config) => Ok(config.clone()),
            None => {
                Err((StatusCode::NOT_FOUND, format!("Unknown dataset: {alias}")).into_response())
            }
        }
    }
}

#[async_trait]
impl<S> FromRequest<S> for ClientRequest
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(mut req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        let dataset = req
            .extract_parts::<DatasetConfig>()
            .await
            .map_err(IntoResponse::into_response)?;

        let req_id = req
            .extract_parts::<Extension<RequestId>>()
            .await
            .expect("RequestId should be set by the SetRequestIdLayer")
            .header_value()
            .to_str()
            .expect("RequestId should be a valid string")
            .to_owned();

        let Query(params) = req
            .extract_parts::<Query<HashMap<String, String>>>()
            .await
            .map_err(IntoResponse::into_response)?;
        let Extension(config) = req
            .extract_parts::<Extension<Arc<Config>>>()
            .await
            .map_err(IntoResponse::into_response)?;
        let mut query: ParsedQuery = req.extract().await.map_err(IntoResponse::into_response)?;

        if config.skip_parent_hash_validation {
            query.remove_parent_hash();
        }

        let buffer_size = match params.get("buffer_size").map(|v| v.parse()) {
            Some(Ok(value)) => value,
            Some(Err(e)) => {
                return Err(
                    RequestError::BadRequest(format!("Couldn't parse buffer_size: {e}"))
                        .into_response(),
                )
            }
            None => config.default_buffer_size,
        };
        let timeout_quantile = match params.get("timeout_quantile") {
            Some(value) => match value.parse() {
                Ok(quantile) => quantile,
                Err(e) => {
                    return Err(RequestError::BadRequest(format!(
                        "Couldn't parse timeout_quantile: {e}"
                    ))
                    .into_response())
                }
            },
            None => config.default_timeout_quantile,
        };
        let retries = match params.get("retries") {
            Some(value) => match value.parse() {
                Ok(value) => value,
                Err(e) => {
                    return Err(
                        RequestError::BadRequest(format!("Couldn't parse retries: {e}"))
                            .into_response(),
                    )
                }
            },
            None => config.default_retries,
        };
        let max_chunks = match params.get("max_chunks") {
            Some(value) => match value.parse() {
                Ok(value) => Some(value),
                Err(e) => {
                    return Err(
                        RequestError::BadRequest(format!("Couldn't parse max_chunks: {e}"))
                            .into_response(),
                    )
                }
            },
            None => None,
        };

        Ok(ClientRequest {
            dataset_id: DatasetId::from_url("-"), // will be filled later, if the request goes to the network
            dataset_name: dataset.default_name,
            query,
            request_id: req_id,
            buffer_size,
            max_chunks,
            timeout_quantile,
            retries,
        })
    }
}

#[async_trait]
impl<S> FromRequest<S> for ParsedQuery
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        let body: String = req
            .with_limited_body()
            .extract()
            .await
            .map_err(IntoResponse::into_response)?;

        if body.len() as u64 > sqd_network_transport::protocol::MAX_RAW_QUERY_SIZE {
            return Err(RequestError::BadRequest("Query is too large".to_string()).into_response());
        }

        ParsedQuery::try_from(body)
            .map_err(|e| RequestError::BadRequest(format!("{:#}", e)).into_response())
    }
}

// Used with network-only endpoints
#[async_trait]
impl<S> FromRequestParts<S> for DatasetId
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        use axum::RequestPartsExt;

        let dataset = parts
            .extract::<DatasetConfig>()
            .await
            .map_err(IntoResponse::into_response)?;

        match dataset.network_id {
            Some(dataset_id) => Ok(dataset_id),
            None => Err((
                StatusCode::NOT_FOUND,
                format!(
                    "Dataset {} doesn't have archival data",
                    dataset.default_name
                ),
            )
                .into_response()),
        }
    }
}

fn restrict_request(config: &Config, request: ClientRequest) -> ClientRequest {
    let max_chunks = match (request.max_chunks, config.max_chunks_per_stream) {
        (Some(requested), Some(limit)) => Some(requested.min(limit)),
        (Some(requested), None) => Some(requested),
        (None, Some(limit)) => Some(limit),
        (None, None) => None,
    };
    build_request(
        &config,
        &request.request_id,
        request.query,
        request.dataset_id,
        request.dataset_name,
        max_chunks,
    )
}

fn build_request(
    config: &Config,
    req_id: &str,
    pq: ParsedQuery,
    did: DatasetId,
    dname: String,
    max_chunks: Option<usize>,
) -> ClientRequest {
    ClientRequest {
        query: pq,
        dataset_id: did,
        dataset_name: dname,
        request_id: req_id.to_string(),
        buffer_size: config.max_buffer_size,
        max_chunks: max_chunks,
        timeout_quantile: config.default_timeout_quantile,
        retries: config.default_retries,
    }
}

fn forward_hotblocks_response(response: Result<reqwest::Response, HotblocksErr>) -> Response {
    match response {
        Ok(response) => forward_response(response),
        Err(HotblocksErr::UnknownDataset) => {
            unreachable!("dataset should be known by the hotblocks service")
        }
        Err(HotblocksErr::Request(e)) => (
            StatusCode::BAD_GATEWAY,
            format!("Hotblocks request error: {e}"),
        )
            .into_response(),
    }
}

fn forward_response(response: reqwest::Response) -> axum::response::Response {
    let mut builder = Response::builder().status(response.status());
    for (key, value) in response.headers() {
        builder = builder.header(key, value);
    }
    let body = Body::from_stream(response.bytes_stream());
    builder.body(body).unwrap()
}

const FINALIZED_NUMBER_HEADER: &str = "x-sqd-finalized-head-number";
const FINALIZED_HASH_HEADER: &str = "x-sqd-finalized-head-hash";
const HEAD_NUMBER_HEADER: &str = "x-sqd-head-number";
const DATA_SOURCE_HEADER: &str = "x-sqd-data-source";
const DATA_SOURCE_NETWORK: HeaderValue = HeaderValue::from_static("network");
const DATA_SOURCE_REALTIME: HeaderValue = HeaderValue::from_static("real_time");
