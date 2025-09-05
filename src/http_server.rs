use std::time::Duration;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use axum::body::Bytes;
use axum::http::Method;
use axum::BoxError;
use axum::{
    async_trait,
    body::Body,
    extract::{FromRequest, FromRequestParts, Path, Query, Request},
    http::{header, request::Parts, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension, RequestExt, Router,
};
use futures::TryStream;
use prometheus_client::registry::Registry;
use serde_json::{json, Value};
use sqd_contract_client::PeerId;
use sqd_hotblocks::error::UnknownDataset;
use tokio::time::Instant;
use tower_http::cors::{Any, CorsLayer};
use tower_http::decompression::RequestDecompressionLayer;
use tower_http::request_id::{
    MakeRequestUuid, PropagateRequestIdLayer, RequestId, SetRequestIdLayer,
};

use crate::datasets::DatasetConfig;
use crate::types::api_types::AvailableDatasetApiResponse;
use crate::utils::conversion::{json_lines_to_json, recompress_gzip};
use crate::{
    config::Config,
    controller::task_manager::TaskManager,
    hotblocks::HotblocksHandle,
    network::{NetworkClient, NoWorker},
    types::{ChunkId, ClientRequest, DatasetId, ParsedQuery, RequestError},
    utils::logging,
};

pub async fn run_server(
    task_manager: Arc<TaskManager>,
    network_client: Arc<NetworkClient>,
    metrics_registry: Registry,
    addr: SocketAddr,
    config: Arc<Config>,
    hotblocks: Option<Arc<HotblocksHandle>>,
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
        .route("/datasets", get(get_datasets))
        .route(
            "/datasets/:dataset/finalized-stream",
            post(run_finalized_stream_restricted),
        )
        .route(
            "/datasets/:dataset/finalized-stream/debug",
            post(run_finalized_stream),
        )
        .route("/datasets/:dataset/stream", post(run_stream))
        .route("/datasets/:dataset/finalized-head", get(get_finalized_head))
        .route("/datasets/:dataset/head", get(get_head))
        .route("/datasets/:dataset/state", get(get_dataset_state))
        .route("/datasets/:dataset/metadata", get(get_dataset_metadata))
        // backward compatibility routes
        .route(
            "/datasets/:dataset/finalized-stream/height",
            get(get_height),
        )
        .route(
            "/datasets/:dataset_id/query/:worker_id",
            post(execute_query),
        )
        .route("/datasets/:dataset/height", get(get_height))
        .route("/datasets/:dataset/:start_block/worker", get(get_worker))
        // end backward compatibility routes
        .route("/status", get(get_status))
        .route("/debug/db_stats", get(get_db_stats))
        .route("/debug/db_property/:cf/:name", get(get_db_property))
        .route("/debug/workers", get(get_all_workers))
        .route("/datasets/:dataset/:block/debug", get(get_debug_block))
        .layer(axum::middleware::from_fn(logging::middleware))
        .route("/metrics", get(get_metrics))
        .route("/ready", get(get_readiness))
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

async fn get_finalized_head(
    Extension(hotblocks): Extension<Option<Arc<HotblocksHandle>>>,
    Extension(network): Extension<Arc<NetworkClient>>,
    dataset: DatasetConfig,
) -> Response {
    match (hotblocks, dataset) {
        (
            _,
            DatasetConfig {
                network_id: Some(dataset_id),
                ..
            },
        ) => {
            // Prefer network data source to correspond to the /finalized-stream behaviour
            let head = network.head(&dataset_id).map(|head| {
                json!({
                    "number": head.number,
                    "hash": head.hash,
                })
            });
            axum::Json(head).into_response()
        }

        (Some(hotblocks), dataset) if dataset.hotblocks.is_some() => {
            match hotblocks
                .server
                .get_finalized_head(dataset.default_name.parse().unwrap())
            {
                Ok(head) => axum::Json(head).into_response(),
                Err(UnknownDataset { .. }) => {
                    unreachable!("dataset should be known by the hotblocks service")
                }
            }
        }

        (_, dataset) => {
            return (
                StatusCode::NOT_FOUND,
                format!("Dataset {} has no data sources", dataset.default_name),
            )
                .into_response()
        }
    }
}

async fn get_head(
    Extension(hotblocks): Extension<Option<Arc<HotblocksHandle>>>,
    Extension(network): Extension<Arc<NetworkClient>>,
    dataset: DatasetConfig,
) -> Response {
    match (hotblocks, dataset) {
        (Some(hotblocks), dataset) if dataset.hotblocks.is_some() => {
            match hotblocks
                .server
                .get_head(dataset.default_name.parse().unwrap())
            {
                Ok(head) => axum::Json(head).into_response(),
                Err(UnknownDataset { .. }) => {
                    unreachable!("dataset should be known by the hotblocks service")
                }
            }
        }

        (
            _,
            DatasetConfig {
                network_id: Some(dataset_id),
                ..
            },
        ) => {
            // Fall back to network data source
            let head = network.head(&dataset_id).map(|head| {
                json!({
                    "number": head.number,
                    "hash": head.hash,
                })
            });
            axum::Json(head).into_response()
        }

        (_, dataset) => {
            return (
                StatusCode::NOT_FOUND,
                format!("Dataset {} has no data sources", dataset.default_name),
            )
                .into_response()
        }
    }
}

async fn run_finalized_stream_restricted(
    task_manager: Extension<Arc<TaskManager>>,
    network: Extension<Arc<NetworkClient>>,
    Extension(config): Extension<Arc<Config>>,
    dataset_id: DatasetId,
    raw_request: ClientRequest,
) -> Response {
    let request = restrict_request(&config, raw_request);
    run_finalized_stream(task_manager, network, dataset_id, request).await
}

async fn run_finalized_stream(
    Extension(task_manager): Extension<Arc<TaskManager>>,
    Extension(network): Extension<Arc<NetworkClient>>,
    dataset_id: DatasetId,
    mut request: ClientRequest,
) -> Response {
    let head = network.head(&dataset_id);
    request.dataset_id = dataset_id;
    request.query.prepare_for_network();
    let stream = match task_manager.spawn_stream(request).await {
        Ok(stream) => stream,
        Err(e @ RequestError::NoData) => {
            // Delay request from this client for 5 seconds to avoid unnecessary retries
            tokio::time::sleep(Duration::from_secs(5)).await;
            return e.into_response();
        }
        Err(e) => return e.into_response(),
    };

    let mut res = Response::builder()
        .header(header::CONTENT_TYPE, "application/jsonl")
        .header(header::CONTENT_ENCODING, "gzip");
    if let Some(head) = head {
        res = res.header(FINALIZED_NUMBER_HEADER, head.number);
        res = res.header(FINALIZED_HASH_HEADER, head.hash);
        res = res.header(DATA_SOURCE_HEADER, DATA_SOURCE_NETWORK);
    }
    res.body(Body::from_stream(recompress_gzip(stream)))
        .unwrap()
}

async fn run_stream(
    Extension(task_manager): Extension<Arc<TaskManager>>,
    Extension(config): Extension<Arc<Config>>,
    Extension(network): Extension<Arc<NetworkClient>>,
    Extension(hotblocks): Extension<Option<Arc<HotblocksHandle>>>,
    dataset: DatasetConfig,
    request: ClientRequest,
) -> Response {
    let mut request = restrict_request(&config, request);

    let (head, body, source) = match (dataset.network_id, hotblocks) {
        // TODO: prefer hotblocks storage
        // Prefer data from the network
        (Some(dataset_id), _)
            if network
                .get_height(&dataset_id)
                .is_some_and(|height| request.query.first_block() <= height) =>
        {
            let head = network.head(&dataset_id);
            request.dataset_id = dataset_id;
            request.query.prepare_for_network();

            let stream = match task_manager.spawn_stream(request).await {
                Ok(stream) => stream,
                Err(e) => return e.into_response(),
            };

            (
                head,
                Body::from_stream(recompress_gzip(stream)),
                DATA_SOURCE_NETWORK,
            )
        }
        // Then try hotblocks storage
        (_, Some(hotblocks)) if dataset.hotblocks.is_some() => {
            let result = hotblocks
                .server
                .query(
                    (*dataset.default_name).try_into().unwrap(),
                    request.query.into_parsed(),
                )
                .await;
            match result {
                Ok(resp) => (
                    resp.finalized_head().cloned(),
                    Body::from_stream(into_stream(resp)),
                    DATA_SOURCE_REALTIME,
                ),
                Err(err) => return hotblocks_error_to_response(err),
            }
        }
        // If requested block is above the network height and there is no hotblocks storage, return 204
        (Some(_dataset_id), _) => {
            // Delay request from this client for 5 seconds to avoid unnecessary retries
            // TODO: actually wait for data arrival
            tokio::time::sleep(Duration::from_secs(5)).await;
            return RequestError::NoData.into_response();
        }
        (None, _) => {
            unreachable!(
                "invalid dataset name should have been handled in the ClientRequest parser"
            )
        }
    };

    let mut res = Response::builder()
        .header(header::CONTENT_TYPE, "application/jsonl")
        .header(header::CONTENT_ENCODING, "gzip")
        .header(DATA_SOURCE_HEADER, source);

    if let Some(head) = head {
        res = res.header(FINALIZED_NUMBER_HEADER, head.number);
        res = res.header(FINALIZED_HASH_HEADER, head.hash);
    }
    res.body(body).unwrap()
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
    metadata: DatasetConfig,
) -> impl IntoResponse {
    let first_block = metadata
        .network_id
        .as_ref()
        .and_then(|dataset| network.first_existing_block(dataset));
    axum::Json(AvailableDatasetApiResponse::new(metadata, first_block))
}

async fn get_db_stats(
    Extension(hotblocks): Extension<Option<Arc<HotblocksHandle>>>,
) -> impl IntoResponse {
    let Some(hotblocks) = hotblocks else {
        return (
            StatusCode::NOT_FOUND,
            "Hotblocks storage is not enabled".to_string(),
        )
            .into_response();
    };

    match hotblocks.db.get_statistics() {
        Some(stats) => stats.into_response(),
        None => (
            StatusCode::NOT_FOUND,
            "Failed to get hotblocks database statistics".to_string(),
        )
            .into_response(),
    }
}

async fn get_db_property(
    Path((cf, property)): Path<(String, String)>,
    Extension(hotblocks): Extension<Option<Arc<HotblocksHandle>>>,
) -> Response {
    let Some(hotblocks) = hotblocks else {
        return (
            StatusCode::NOT_FOUND,
            "Hotblocks storage is not enabled".to_string(),
        )
            .into_response();
    };

    match hotblocks.db.get_property(&cf, &property) {
        Ok(Some(stats)) => stats.into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "Not found".to_string()).into_response(),
        Err(e) => {
            tracing::error!("Failed to get hotblocks database property: {e:?}");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
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

    let Ok(chunk) = client.find_chunk(&dataset_id, query.first_block()) else {
        return RequestError::NoData.into_response();
    };
    let range = query
        .intersect_with(&chunk.block_range())
        .expect("Found chunk should intersect with query");
    let fut = client.query_worker(
        worker_id,
        "".to_string(), // assuming this function is deprecated
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
        let query: ParsedQuery = req.extract().await.map_err(IntoResponse::into_response)?;

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

        Ok(ClientRequest {
            dataset_id: DatasetId::from_url("-"), // will be filled later, if the request goes to the network
            dataset_name: dataset.default_name,
            query,
            request_id: req_id,
            buffer_size,
            max_chunks: config.max_chunks_per_stream,
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
    ClientRequest {
        query: request.query,
        dataset_id: request.dataset_id,
        dataset_name: request.dataset_name,
        request_id: request.request_id,
        buffer_size: request.buffer_size.min(config.max_buffer_size),
        max_chunks: request.max_chunks.min(config.max_chunks_per_stream),
        timeout_quantile: config.default_timeout_quantile,
        retries: config.default_retries,
    }
}

fn into_stream(resp: sqd_hotblocks::QueryResponse) -> impl TryStream<Ok = Bytes, Error = BoxError> {
    futures::stream::try_unfold(resp, |mut resp| async {
        if let Some(bytes) = resp.next_bytes().await? {
            Ok(Some((bytes, resp)))
        } else {
            Ok(None)
        }
    })
}

#[allow(clippy::if_same_then_else)]
fn hotblocks_error_to_response(err: anyhow::Error) -> Response {
    if let Some(above_the_head) = err.downcast_ref::<sqd_hotblocks::error::QueryIsAboveTheHead>() {
        let mut res = Response::builder().status(204);
        if let Some(head) = above_the_head.finalized_head.as_ref() {
            res = res.header(FINALIZED_NUMBER_HEADER, head.number);
            res = res.header(FINALIZED_HASH_HEADER, head.hash.as_str());
        }
        return res.body(Body::empty()).unwrap();
    }

    if let Some(fork) = err.downcast_ref::<sqd_hotblocks::error::UnexpectedBaseBlock>() {
        return (
            StatusCode::CONFLICT,
            axum::Json(serde_json::json!({
                "previousBlocks": &fork.prev_blocks
            })),
        )
            .into_response();
    }

    let status_code = if err.is::<sqd_hotblocks::error::UnknownDataset>() {
        StatusCode::NOT_FOUND
    } else if err.is::<sqd_hotblocks::error::QueryKindMismatch>() {
        StatusCode::BAD_REQUEST
    } else if err.is::<sqd_hotblocks::error::BlockRangeMissing>() {
        StatusCode::BAD_REQUEST
    } else if err.is::<sqd_hotblocks::error::Busy>() {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };

    (status_code, format!("{:#}", err)).into_response()
}

const FINALIZED_NUMBER_HEADER: &str = "X-Sqd-Finalized-Head-Number";
const FINALIZED_HASH_HEADER: &str = "X-Sqd-Finalized-Head-Hash";
const DATA_SOURCE_HEADER: &str = "X-Sqd-Data-Source";
const DATA_SOURCE_NETWORK: &str = "network";
const DATA_SOURCE_REALTIME: &str = "real_time";
