use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use axum::http::Method;
use axum::{
    async_trait,
    body::Body,
    extract::{FromRequest, FromRequestParts, Path, Query, Request},
    http::{header, request::Parts, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension, RequestExt, Router,
};
use prometheus_client::registry::Registry;
use sentry::integrations::tower as sentry_tower;
use serde_json::{json, Value};
use serde::Serialize;
use sqd_contract_client::PeerId;

use crate::network::NoWorker;
use tokio::time::Instant;
use tower_http::cors::{Any, CorsLayer};
use tower_http::decompression::RequestDecompressionLayer;
use tower_http::request_id::{
    MakeRequestUuid, PropagateRequestIdLayer, RequestId, SetRequestIdLayer,
};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::datasets::DatasetConfig;
use crate::endpoints::{
    block_number_by_timestamp::get_blocknumber_by_timestamp,
    stream::{
        run_archival_stream, run_archival_stream_restricted, run_finalized_stream, run_stream,
    },
};
use crate::hotblocks::{HotblocksErr, StreamMode};
use crate::openapi::ApiDoc;
use crate::types::api_types::AvailableDatasetApiResponse;
use crate::types::Compression;
use crate::utils::conversion::json_lines_to_json;
use crate::utils::logging::MethodRouterExt;
use crate::{
    config::Config,
    controller::task_manager::TaskManager,
    hotblocks::HotblocksHandle,
    network::{NetworkClient, NoWorker},
    types::{ChunkId, DatasetId, GenericError, ParsedQuery, RequestError, StreamRequest},
    utils::logging,
};

#[cfg(feature = "sql")]
use crate::sql;
#[cfg(feature = "sql")]
use axum::body;

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
        .layer(sentry_tower::NewSentryLayer::new_from_top())
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
            "/datasets/:dataset",
            get(get_dataset_metadata).endpoint("/dataset"),
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
            "/datasets/:dataset/finalized-stream/height",
            get(get_height).endpoint("/height"),
        )
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
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()));

    // SQL Query Engine
    #[cfg(feature = "sql")]
    let app = app
        .route("/sql/query", post(sql_query).endpoint("/sql/query"))
        .route("/sql/metadata", get(sql_metadata).endpoint("/sql/metadata"));

    let app = app
        .route_layer(axum::middleware::from_fn(logging::middleware))
        .layer(RequestDecompressionLayer::new())
        .layer(cors)
        .layer(
            // This layer is added here to be applied before the request reaches trace layers
            SetRequestIdLayer::x_request_id(MakeRequestUuid),
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
/// Get the archival head block
///
/// Returns the block number and hash of the current archival data head
#[utoipa::path(
    get,
    path = "/datasets/{dataset}/archival-head",
    params(
        ("dataset" = String, Path, description = "Dataset name"),
    ),
    responses(
        (status = 200, description = "Archival head block retrieved", body = Option<serde_json::Value>),
        (status = 404, description = "Dataset has no archival data source"),
    ),
    tag = "head"
)]
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
#[utoipa::path(
    get,
    path = "/datasets/{dataset}/finalized-head",
    params(
        ("dataset" = String, Path, description = "Dataset name"),
    ),
    responses(
        (status = 200, description = "Finalized head block retrieved", body = Option<serde_json::Value>),
        (status = 404, description = "Dataset has no data sources"),
    ),
    tag = "head"
)]
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
#[utoipa::path(
    get,
    path = "/datasets/{dataset}/head",
    params(
        ("dataset" = String, Path, description = "Dataset name"),
    ),
    responses(
        (status = 200, description = "Head block retrieved", body = Option<serde_json::Value>),
        (status = 404, description = "Dataset has no data sources"),
    ),
    tag = "head"
)]
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

/// Stream archival data with request restrictions applied
///
/// Streams blockchain data from the archival layer with rate limits and restrictions
#[utoipa::path(
    post,
    path = "/datasets/{dataset}/archival-stream",
    params(
        ("dataset" = String, Path, description = "Dataset name"),
    ),
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Archival data stream", content_type = "application/jsonl"),
        (status = 400, description = "Invalid request parameters"),
        (status = 404, description = "Dataset not found"),
    ),
    tag = "stream"
)]
async fn run_archival_stream_restricted(
    task_manager: Extension<Arc<TaskManager>>,
    network: Extension<Arc<NetworkClient>>,
    config: Extension<Arc<Config>>,
    dataset_id: DatasetId,
    raw_request: StreamRequest,
) -> Response {
    let request = restrict_request(&config, raw_request);
    run_archival_stream(task_manager, network, config, dataset_id, request).await
}

/// Get the current status of the portal
///
/// Responds with a JSON with human-readable information about the portal's state. The exact format may change.
#[utoipa::path(
    get,
    path = "/status",
    responses(
        (status = 200, description = "Portal status retrieved successfully", body = StatusResponse,
         example = json!({
             "peer_id": "12D3KooWDJwsMFBEUxSUMxTKaBXLMvnn7pr9zsP73PMQrb9kPrtL",
             "status": "registered",
             "operator": "0xd1c2…11fa",
             "current_epoch": {
                 "number": 19619,
                 "started_at": "2025-02-04T10:40:35+00:00",
                 "ended_at": "2025-02-04T11:00:47+00:00",
                 "duration_seconds": 1212
             },
             "sqd_locked": "100000",
             "cu_per_epoch": "100000",
             "workers": {
                 "active_count": 1645,
                 "rate_limit_per_worker": "0.04950495049504951"
             },
             "portal_version": "0.5.5"
         })),
    ),
    tag = "status"
)]
async fn get_status(Extension(client): Extension<Arc<NetworkClient>>) -> impl IntoResponse {
    let response = crate::openapi::StatusResponse {
        portal_version: env!("CARGO_PKG_VERSION").to_string(),
        status: client.get_status(),
    };
    axum::Json(response).into_response()
}

#[derive(serde::Deserialize)]
struct MetadataQuery {
    #[serde(default, rename = "expand[]")]
    expand: Vec<String>,
}

/// List available datasets
///
/// Lists the existing datasets as a JSON array. See /metadata endpoint for the field description.
#[utoipa::path(
    get,
    path = "/datasets",
    params(
        ("expand[]" = Option<Vec<String>>, Query, description = "Fields to expand in response"),
    ),
    responses(
        (status = 200, description = "List of available datasets", body = Vec<AvailableDatasetApiResponse>,
         example = json!([
             {
                 "dataset": "ethereum-mainnet",
                 "aliases": ["eth-mainnet"],
                 "real_time": false
             },
             {
                 "dataset": "solana-mainnet",
                 "aliases": [],
                 "start_block": 250000000,
                 "real_time": true
             }
         ])),
    ),
    tag = "datasets"
)]
async fn get_datasets(
    axum_extra::extract::Query(query): axum_extra::extract::Query<MetadataQuery>,
    Extension(network): Extension<Arc<NetworkClient>>,
) -> impl IntoResponse {
    let datasets = network.datasets().read();
    let res: Vec<AvailableDatasetApiResponse> = datasets
        .iter()
        .map(|d| {
            let resp: AvailableDatasetApiResponse = d.clone().into();
            resp.with_fields(&query.expand)
        })
        .collect();

    axum::Json(res)
}

/// Get the state of a specific dataset
///
/// Returns state information for the requested dataset
#[utoipa::path(
    get,
    path = "/datasets/{dataset}/state",
    params(
        ("dataset" = String, Path, description = "Dataset name"),
    ),
    responses(
        (status = 200, description = "Dataset state retrieved successfully", body = serde_json::Value),
        (status = 404, description = "Dataset not found"),
    ),
    tag = "datasets"
)]
async fn get_dataset_state(
    dataset_id: DatasetId,
    Extension(network): Extension<Arc<NetworkClient>>,
) -> impl IntoResponse {
    axum::Json(network.dataset_state(&dataset_id))
}

/// Get metadata for a specific dataset
///
/// Returns metadata including chain info, first block, and optional expanded fields
#[utoipa::path(
    get,
    path = "/datasets/{dataset}/metadata",
    params(
        ("dataset" = String, Path, description = "Dataset name"),
        ("expand[]" = Option<Vec<String>>, Query, description = "Fields to expand in response"),
    ),
    responses(
        (status = 200, description = "Dataset metadata retrieved successfully", body = AvailableDatasetApiResponse),
        (status = 404, description = "Dataset not found"),
    ),
    tag = "datasets"
)]
async fn get_dataset_metadata(
    axum_extra::extract::Query(query): axum_extra::extract::Query<MetadataQuery>,
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
    let resp = AvailableDatasetApiResponse::new(metadata, first_block);
    axum::Json(resp.with_fields(&query.expand))
}

/// Get debug information for a specific block
///
/// Returns worker information for the given block
#[utoipa::path(
    get,
    path = "/datasets/{dataset}/{block}/debug",
    params(
        ("dataset" = String, Path, description = "Dataset name"),
        ("block" = u64, Path, description = "Block number"),
    ),
    responses(
        (status = 200, description = "Debug information retrieved", body = serde_json::Value),
        (status = 404, description = "Dataset or block not found"),
    ),
    tag = "debug"
)]
>>>>>>> efef0a6 (work in progress)
async fn get_debug_block(
    Path((_dataset, block)): Path<(String, u64)>,
    dataset_id: DatasetId,
    Extension(client): Extension<Arc<NetworkClient>>,
) -> axum::Json<serde_json::Value> {
    axum::Json(json!({
        "workers": client.get_workers(&dataset_id, block),
    }))
}

/// Get information about all workers
///
/// Returns information about all available workers in the network
#[utoipa::path(
    get,
    path = "/debug/workers",
    responses(
        (status = 200, description = "All worker information retrieved", body = serde_json::Value),
    ),
    tag = "debug"
)]
async fn get_all_workers(
    Extension(client): Extension<Arc<NetworkClient>>,
) -> axum::Json<serde_json::Value> {
    axum::Json(json!({
        "workers": client.get_all_workers(),
    }))
}

/// Get Prometheus metrics
///
/// Returns portal metrics in OpenMetrics format
#[utoipa::path(
    get,
    path = "/metrics",
    responses(
        (status = 200, description = "Metrics in OpenMetrics format", content_type = "application/openmetrics-text"),
    ),
    tag = "status"
)]
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

/// Check if the portal is ready to serve requests
///
/// Returns 200 if the portal is ready, 503 if not
#[utoipa::path(
    get,
    path = "/ready",
    responses(
        (status = 200, description = "Portal is ready"),
        (status = 503, description = "Portal is not ready"),
    ),
    tag = "status"
)]
async fn get_readiness(Extension(client): Extension<Arc<NetworkClient>>) -> impl IntoResponse {
    if client.is_ready() {
        return (StatusCode::OK, "Ready").into_response();
    }

    (StatusCode::SERVICE_UNAVAILABLE, "Not ready").into_response()
}

/// Get the height of a dataset (deprecated)
///
/// Returns the current height of the dataset. This endpoint is deprecated and kept for backward compatibility.
#[utoipa::path(
    get,
    path = "/datasets/{dataset}/height",
    params(
        ("dataset" = String, Path, description = "Dataset name"),
    ),
    responses(
        (status = 200, description = "Height retrieved successfully", body = String),
        (status = 404, description = "Dataset not found"),
    ),
    tag = "query"
)]
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

/// Get worker information for a block range (deprecated)
///
/// Returns worker information for the given dataset and block range. This endpoint is deprecated.
#[utoipa::path(
    get,
    path = "/datasets/{dataset}/{start_block}/worker",
    params(
        ("dataset" = String, Path, description = "Dataset name"),
        ("start_block" = u64, Path, description = "Starting block number"),
    ),
    responses(
        (status = 200, description = "Worker information retrieved", body = serde_json::Value),
        (status = 404, description = "Dataset not found"),
    ),
    tag = "debug"
)]
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

/// Execute a SQL query on the network
///
/// Executes a SQL query against a specific worker in the network
#[utoipa::path(
    post,
    path = "/datasets/{dataset_id}/query/{worker_id}",
    params(
        ("dataset_id" = String, Path, description = "Dataset ID"),
        ("worker_id" = String, Path, description = "Worker ID"),
    ),
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Query executed successfully", body = String),
        (status = 400, description = "Invalid query"),
        (status = 404, description = "Dataset or worker not found"),
        (status = 503, description = "Service unavailable"),
    ),
    tag = "query"
)]
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
        Compression::Gzip,
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
            .ok_or((StatusCode::NOT_FOUND, "not enough arguments".to_string()).into_response())?;
        let Extension(network) = parts
            .extract::<Extension<Arc<NetworkClient>>>()
            .await
            .map_err(IntoResponse::into_response)?;

        match network.dataset(alias) {
            Some(config) => Ok(config.clone()),
            None => {
                Err((StatusCode::NOT_FOUND, format!("Unknown dataset: {alias}")).into_response())
            }
        }
    }
}

#[async_trait]
impl<S> FromRequest<S> for StreamRequest
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

        let buffer_size = match params.get("buffer_size").map(|v| v.parse()) {
            Some(Ok(0)) => {
                return Err(RequestError::BadRequest(
                    "buffer_size must be greater than 0".to_string(),
                )
                .into_response())
            }
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
                Ok(0) => {
                    return Err(RequestError::BadRequest(
                        "max_chunks must be greater than 0".to_string(),
                    )
                    .into_response())
                }
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

        let compression = determine_compression_format(req.headers());

        let query: ParsedQuery = req.extract().await.map_err(IntoResponse::into_response)?;

        Ok(StreamRequest {
            dataset_id: DatasetId::from_url("-"), // will be filled later, if the request goes to the network
            dataset_name: dataset.default_name,
            query,
            request_id: req_id,
            buffer_size,
            max_chunks,
            timeout_quantile,
            retries,
            compression,
            skip_parent_hash_validation: config.skip_parent_hash_validation,
        })
    }
}

fn determine_compression_format(headers: &HeaderMap) -> Compression {
    // Prefer zstd if the client supports it
    if let Some(encodings) = headers
        // Prefer X-Forwarded-Accept-Encoding for compatibility with Cloudflare
        .get("X-Forwarded-Accept-Encoding")
        .or_else(|| headers.get(header::ACCEPT_ENCODING))
    {
        if let Ok(encodings_str) = encodings.to_str() {
            if encodings_str.contains("zstd") {
                return Compression::Zstd;
            }
        }
    }
    // Default to gzip
    Compression::Gzip
}

#[async_trait]
impl<S> FromRequest<S> for ParsedQuery
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(mut req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        let Extension(config) = req
            .extract_parts::<Extension<Arc<Config>>>()
            .await
            .map_err(IntoResponse::into_response)?;

        let body: String = req
            .with_limited_body()
            .extract()
            .await
            .map_err(IntoResponse::into_response)?;

        if body.len() as u64 > config.query_size_limit {
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

pub(crate) fn forward_hotblocks_response(
    response: Result<reqwest::Response, HotblocksErr>,
) -> Response {
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

pub(crate) fn forward_response(response: reqwest::Response) -> axum::response::Response {
    let mut builder = Response::builder().status(response.status());
    for (key, value) in response.headers() {
        builder = builder.header(key, value);
    }
    let body = Body::from_stream(response.bytes_stream());
    builder.body(body).unwrap()
}

#[cfg(feature = "sql")]
async fn sql_query(
    Extension(network): Extension<Arc<NetworkClient>>,
    query: body::Bytes,
) -> impl axum::response::IntoResponse {
    match sql::query(query, &network).await {
        Ok(res) => axum::Json(res).into_response(),
        Err(e) => {
            tracing::warn!("cannot query data: {:?}", e);
            e.into_response()
        }
    }
}

#[cfg(feature = "sql")]
async fn sql_metadata(
    Extension(network): Extension<Arc<NetworkClient>>,
) -> impl axum::response::IntoResponse {
    match sql::get_all_metadata(network).await {
        Ok(md) => axum::Json(md.datasets).into_response(),
        Err(e) => {
            tracing::warn!("cannot fetch metadata: {:?}", e);
            e.into_response()
        }
    }
}
