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
use futures::StreamExt;
use itertools::Itertools;
use prometheus_client::registry::Registry;
use sqd_contract_client::PeerId;
use sqd_messages::query_result;
use tower_http::cors::{Any, CorsLayer};

use crate::api_types::PortalConfigApiResponse;
use crate::{
    cli::Config,
    controller::task_manager::TaskManager,
    network::NetworkClient,
    types::{ClientRequest, DatasetId, RequestError},
    utils::logging,
};

async fn get_height(
    Extension(network_state): Extension<Arc<NetworkClient>>,
    Path(dataset): Path<String>,
    dataset_id: DatasetId,
) -> impl IntoResponse {
    match network_state.get_height(&dataset_id) {
        Some(height) => (StatusCode::OK, height.to_string()),
        None => (
            StatusCode::NOT_FOUND,
            format!("No data for dataset {dataset}"),
        ),
    }
}

async fn get_worker(
    Path((dataset, start_block)): Path<(String, u64)>,
    Extension(client): Extension<Arc<NetworkClient>>,
    Extension(config): Extension<Arc<Config>>,
) -> Response {
    let Some(dataset_id) = config.dataset_id(&dataset) else {
        return (StatusCode::NOT_FOUND, format!("Unknown dataset: {dataset}")).into_response();
    };

    let worker_id = match client.find_worker(&dataset_id, start_block) {
        Some(worker_id) => worker_id,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("No available worker for dataset {dataset} block {start_block}"),
            )
                .into_response()
        }
    };

    (
        StatusCode::OK,
        format!("{}/query/{dataset_id}/{worker_id}", config.hostname),
    )
        .into_response()
}

async fn execute_query(
    Path((dataset_id, worker_id)): Path<(DatasetId, PeerId)>,
    Extension(client): Extension<Arc<NetworkClient>>,
    query: String, // request body
) -> Response {
    let Ok(fut) = client.query_worker(&worker_id, &dataset_id, query) else {
        return RequestError::Busy.into_response();
    };
    let result = match fut.await {
        Err(_) => {
            return RequestError::InternalError("Receiving result failed".to_string())
                .into_response()
        }
        Ok(query_result::Result::Ok(result)) => result,
        Ok(res) => return RequestError::try_from(res).into_response(),
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

async fn execute_stream_restricted(
    Extension(task_manager): Extension<Arc<TaskManager>>,
    Extension(config): Extension<Arc<Config>>,
    raw_request: ClientRequest,
) -> Response {
    let request = ClientRequest {
        query: raw_request.query,
        dataset_id: raw_request.dataset_id,
        buffer_size: raw_request.buffer_size.min(config.max_buffer_size),
        max_chunks: config.max_chunks_per_stream,
        timeout_quantile: config.default_timeout_quantile,
        retries: config.default_retries,
    };
    execute_stream(Extension(task_manager), request).await
}

async fn execute_stream(
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

async fn get_status(Extension(client): Extension<Arc<NetworkClient>>) -> impl IntoResponse {
    axum::Json(PortalConfigApiResponse {
        peer_id: client.peer_id(),
    })
}

async fn get_dataset_state(
    Path(dataset): Path<String>,
    Extension(client): Extension<Arc<NetworkClient>>,
    Extension(config): Extension<Arc<Config>>,
) -> impl IntoResponse {
    let Some(dataset_id) = config.dataset_id(&dataset) else {
        return (StatusCode::NOT_FOUND, format!("Unknown dataset: {dataset}")).into_response();
    };

    axum::Json(client.dataset_state(dataset_id)).into_response()
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
    addr: &SocketAddr,
    config: Arc<Config>,
) -> anyhow::Result<()> {
    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS, Method::PUT])
        .allow_headers(Any)
        .allow_origin(Any);

    tracing::info!("Starting HTTP server listening on {addr}");
    let app = Router::new()
        .route("/datasets/:dataset/height", get(get_height))
        .route("/datasets/:dataset/stream", post(execute_stream_restricted))
        .route("/datasets/:dataset/stream/debug", post(execute_stream))
        .route("/datasets/:dataset/state", get(get_dataset_state))
        // backward compatibility routes
        .route("/datasets/:dataset/:start_block/worker", get(get_worker))
        .route("/network/:dataset/height", get(get_height))
        .route("/network/:dataset/:start_block/worker", get(get_worker))
        .route("/query/:dataset_id/:worker_id", post(execute_query))
        // end backward compatibility routes
        .layer(axum::middleware::from_fn(logging::middleware))
        .route("/metrics", get(get_metrics))
        .route("/status", get(get_status))
        .layer(cors)
        .layer(Extension(task_manager))
        .layer(Extension(network_state))
        .layer(Extension(config))
        .layer(Extension(Arc::new(metrics_registry)));

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
        let dataset_id = req.extract_parts::<DatasetId>().await?;
        let Query(params) = req
            .extract_parts::<Query<HashMap<String, String>>>()
            .await
            .map_err(IntoResponse::into_response)?;
        let Extension(config) = req
            .extract_parts::<Extension<Arc<Config>>>()
            .await
            .map_err(IntoResponse::into_response)?;
        let query: String = req.extract().await.map_err(IntoResponse::into_response)?;

        let query = query.parse().map_err(|e| {
            RequestError::BadRequest(format!("Couldn't parse query: {e}")).into_response()
        })?;
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
            query,
            buffer_size,
            max_chunks: config.max_chunks_per_stream,
            timeout_quantile,
            retries,
        })
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
        let Extension(config) = parts
            .extract::<Extension<Arc<Config>>>()
            .await
            .map_err(IntoResponse::into_response)?;

        let Some(dataset_id) = config.dataset_id(&dataset) else {
            return Err(
                (StatusCode::NOT_FOUND, format!("Unknown dataset: {dataset}")).into_response(),
            );
        };

        Ok(dataset_id)
    }
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
