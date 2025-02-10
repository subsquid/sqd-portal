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
use futures::{StreamExt, TryStream};
use itertools::Itertools;
use parking_lot::RwLock;
use prometheus_client::registry::Registry;
use serde_json::{json, Value};
use sqd_contract_client::PeerId;
use sqd_node::Node as HotblocksServer;
use tokio::time::Instant;
use tower_http::cors::{Any, CorsLayer};
use tower_http::decompression::RequestDecompressionLayer;

use crate::types::api_types::AvailableDatasetApiResponse;
use crate::{
    cli::Config,
    controller::task_manager::TaskManager,
    datasets::DatasetsMapping,
    network::{NetworkClient, NoWorker},
    types::{ChunkId, ClientRequest, DatasetId, ParsedQuery, RequestError},
    utils::logging,
};

async fn get_height(
    Extension(network_state): Extension<Arc<NetworkClient>>,
    Path(slug): Path<String>,
    dataset_id: DatasetId,
) -> impl IntoResponse {
    match network_state.get_height(&dataset_id) {
        Some(height) => (StatusCode::OK, height.to_string()),
        None => (StatusCode::NOT_FOUND, format!("No data for dataset {slug}")),
    }
}

async fn get_worker(
    Path((slug, start_block)): Path<(String, u64)>,
    Extension(client): Extension<Arc<NetworkClient>>,
    Extension(datasets): Extension<Arc<RwLock<DatasetsMapping>>>,
    Extension(config): Extension<Arc<Config>>,
) -> Response {
    let Some(dataset_id) = datasets.read().dataset_id(&slug) else {
        return (StatusCode::NOT_FOUND, format!("Unknown dataset: {slug}")).into_response();
    };

    let worker_id = match client.find_worker(&dataset_id, start_block, false) {
        Ok(worker_id) => worker_id,
        Err(NoWorker::AllUnavailable) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("No available worker for dataset {slug} block {start_block}"),
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

    let Some(chunk) = client.find_chunk(&dataset_id, query.first_block()) else {
        return RequestError::NoData.into_response();
    };
    let range = query
        .intersect_with(&chunk.block_range())
        .expect("Found chunk should intersect with query");
    let fut = client.query_worker(
        worker_id,
        ChunkId::new(dataset_id, chunk),
        range,
        query.into_string(),
        true,
    );
    let result = match fut.await {
        Ok(result) => result,
        Err(err) => return RequestError::from(err).into_response(),
    };
    match convert_response(&result.data) {
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

async fn run_finalized_stream_restricted(
    Extension(task_manager): Extension<Arc<TaskManager>>,
    Extension(config): Extension<Arc<Config>>,
    raw_request: ClientRequest,
) -> Response {
    let request = restrict_request(&config, raw_request);
    run_finalized_stream(Extension(task_manager), request).await
}

async fn run_finalized_stream(
    Extension(task_manager): Extension<Arc<TaskManager>>,
    request: ClientRequest,
) -> Response {
    let stream = match task_manager.spawn_stream(request).await {
        Ok(stream) => stream,
        Err(e) => return e.into_response(),
    };
    let stream = stream.map(anyhow::Ok);
    Response::builder()
        .header(header::CONTENT_TYPE, "application/jsonl")
        .header(header::CONTENT_ENCODING, "gzip")
        .body(Body::from_stream(stream))
        .unwrap()
}

async fn run_stream(
    Extension(task_manager): Extension<Arc<TaskManager>>,
    Extension(config): Extension<Arc<Config>>,
    Extension(client): Extension<Arc<NetworkClient>>,
    Extension(hotblocks): Extension<Option<Arc<HotblocksServer>>>,
    request: ClientRequest,
) -> Response {
    let request = restrict_request(&config, request);

    let block = request.query.first_block();
    if client
        .get_height(&request.dataset_id)
        .is_some_and(|height| height > 0 && block <= height)
    {
        let stream = match task_manager.spawn_stream(request).await {
            Ok(stream) => stream,
            Err(e) => return e.into_response(),
        };
        let stream = stream.map(anyhow::Ok);
        Response::builder()
            .header(header::CONTENT_TYPE, "application/jsonl")
            .header(header::CONTENT_ENCODING, "gzip")
            .body(Body::from_stream(stream))
            .unwrap()
        // TODO: set finalized head
    } else if request.dataset_name == "solana-mainnet" && hotblocks.is_some() {  // FIXME
        let result = hotblocks
            .unwrap()
            .query("solana".try_into().unwrap(), request.query.into_parsed())
            .await;
        match result {
            Ok(resp) => {
                let mut res = Response::builder()
                    .header(header::CONTENT_TYPE, "application/jsonl")
                    .header(header::CONTENT_ENCODING, "gzip");

                if let Some(head) = resp.finalized_head() {
                    res = res.header("x-sqd-finalized-head-number", head.number);
                    res = res.header("x-sqd-finalized-head-hash", head.hash.as_str());
                }

                res.body(Body::from_stream(into_stream(resp))).unwrap()
            }
            Err(err) => hotblocks_error_to_response(err),
        }
    } else {
        RequestError::NoData.into_response()
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

async fn get_datasets(
    Extension(datasets): Extension<Arc<RwLock<DatasetsMapping>>>,
) -> impl IntoResponse {
    let res: Vec<AvailableDatasetApiResponse> = datasets
        .read()
        .iter()
        .map(|d| AvailableDatasetApiResponse {
            slug: d.slug.clone(),
            aliases: d.aliases.clone(),
            real_time: false,
        })
        .collect();

    axum::Json(res)
}

async fn get_dataset_state(
    Path(slug): Path<String>,
    Extension(client): Extension<Arc<NetworkClient>>,
    Extension(datasets): Extension<Arc<RwLock<DatasetsMapping>>>,
) -> impl IntoResponse {
    let Some(dataset_id) = datasets.read().dataset_id(&slug) else {
        return (StatusCode::NOT_FOUND, format!("Unknown dataset: {slug}")).into_response();
    };

    axum::Json(client.dataset_state(&dataset_id)).into_response()
}

async fn get_dataset_metadata(
    Path(slug): Path<String>,
    Extension(datasets): Extension<Arc<RwLock<DatasetsMapping>>>,
) -> impl IntoResponse {
    let datasets = datasets.read();
    let Some(dataset) = datasets.find_dataset(&slug) else {
        return (StatusCode::NOT_FOUND, format!("Unknown dataset: {slug}")).into_response();
    };

    axum::Json(AvailableDatasetApiResponse {
        slug: dataset.slug.clone(),
        aliases: dataset.aliases.clone(),
        real_time: false,
    })
    .into_response()
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

pub async fn run_server(
    task_manager: Arc<TaskManager>,
    network_state: Arc<NetworkClient>,
    metrics_registry: Registry,
    addr: SocketAddr,
    config: Arc<Config>,
    datasets: Arc<RwLock<DatasetsMapping>>,
    hotblocks: Option<Arc<HotblocksServer>>,
) -> anyhow::Result<()> {
    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS, Method::PUT])
        .allow_headers(Any)
        .allow_origin(Any);

    tracing::info!("Starting HTTP server listening on {addr}");
    let app = Router::new()
        .route("/datasets", get(get_datasets))
        .route(
            "/datasets/:dataset/finalized-stream/height",
            get(get_height),
        )
        .route(
            "/datasets/:dataset/finalized-stream",
            post(run_finalized_stream_restricted),
        )
        .route(
            "/datasets/:dataset/finalized-stream/debug",
            post(run_finalized_stream),
        )
        .route("/datasets/:dataset/stream", post(run_stream))
        .route("/datasets/:dataset/state", get(get_dataset_state))
        .route("/datasets/:dataset/metadata", get(get_dataset_metadata))
        // backward compatibility routes
        .route(
            "/datasets/:dataset_id/query/:worker_id",
            post(execute_query),
        )
        .route("/datasets/:dataset/height", get(get_height))
        .route("/datasets/:dataset/:start_block/worker", get(get_worker))
        // end backward compatibility routes
        .route("/status", get(get_status))
        .layer(axum::middleware::from_fn(logging::middleware))
        .route("/metrics", get(get_metrics))
        .layer(RequestDecompressionLayer::new())
        .layer(cors)
        .layer(Extension(task_manager))
        .layer(Extension(network_state))
        .layer(Extension(config))
        .layer(Extension(Arc::new(metrics_registry)))
        .layer(Extension(datasets))
        .layer(Extension(hotblocks));

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    tracing::info!("HTTP server stopped");
    Ok(())
}

#[async_trait]
impl<S> FromRequest<S> for ClientRequest
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(mut req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        let Path(dataset_name) = req
            .extract_parts::<Path<String>>()
            .await
            .map_err(IntoResponse::into_response)?;
        let dataset_id = req.extract_parts::<DatasetId>().await?;
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
            dataset_id,
            dataset_name,
            query,
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

#[async_trait]
impl<S> FromRequestParts<S> for DatasetId
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        use axum::RequestPartsExt;

        let Path(dataset) = parts
            .extract::<Path<String>>()
            .await
            .map_err(IntoResponse::into_response)?;
        let Extension(datasets) = parts
            .extract::<Extension<Arc<RwLock<DatasetsMapping>>>>()
            .await
            .map_err(IntoResponse::into_response)?;

        let Some(dataset_id) = datasets.read().dataset_id(&dataset) else {
            return Err(
                (StatusCode::NOT_FOUND, format!("Unknown dataset: {dataset}")).into_response(),
            );
        };

        Ok(dataset_id)
    }
}

fn restrict_request(config: &Config, request: ClientRequest) -> ClientRequest {
    ClientRequest {
        query: request.query,
        dataset_id: request.dataset_id,
        dataset_name: request.dataset_name,
        buffer_size: request.buffer_size.min(config.max_buffer_size),
        max_chunks: config.max_chunks_per_stream,
        timeout_quantile: config.default_timeout_quantile,
        retries: config.default_retries,
    }
}

fn into_stream(resp: sqd_node::QueryResponse) -> impl TryStream<Ok = Bytes, Error = BoxError> {
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
    if let Some(above_the_head) = err.downcast_ref::<sqd_node::error::QueryIsAboveTheHead>() {
        let mut res = Response::builder().status(204);
        if let Some(head) = above_the_head.finalized_head.as_ref() {
            res = res.header("x-sqd-finalized-head-number", head.number);
            res = res.header("x-sqd-finalized-head-hash", head.hash.as_str());
        }
        return res.body(Body::empty()).unwrap();
    }

    if let Some(fork) = err.downcast_ref::<sqd_node::error::UnexpectedBaseBlock>() {
        return (StatusCode::CONFLICT, axum::Json(&fork.prev_blocks)).into_response();
    }

    let status_code = if err.is::<sqd_node::error::UnknownDataset>() {
        StatusCode::NOT_FOUND
    } else if err.is::<sqd_node::error::QueryKindMismatch>() {
        StatusCode::BAD_REQUEST
    } else if err.is::<sqd_node::error::BlockRangeMissing>() {
        StatusCode::BAD_REQUEST
    } else if err.is::<sqd_node::error::Busy>() {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };

    (status_code, format!("{:#}", err)).into_response()
}

#[allow(unstable_name_collisions)]
fn convert_response(data: &[u8]) -> anyhow::Result<Vec<u8>> {
    use std::io::{Read, Write};
    let mut reader = flate2::bufread::GzDecoder::new(data);
    let mut json_lines = String::new();
    reader.read_to_string(&mut json_lines)?;
    let mut writer = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    writer.write_all("[".as_bytes())?;
    for chunk in json_lines.trim_end().lines().intersperse(",") {
        writer.write_all(chunk.as_bytes())?;
    }
    writer.write_all("]".as_bytes())?;
    Ok(writer.finish()?)
}
