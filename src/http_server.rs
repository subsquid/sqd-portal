use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    async_trait,
    body::Body,
    extract::{FromRequest, FromRequestParts, Path, Query, Request},
    http::{header, request::Parts, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension, RequestExt, Router,
};
use contract_client::PeerId;
use futures::StreamExt;
use prometheus_client::registry::Registry;
use subsquid_messages::query_result;

use crate::{
    cli::Config,
    controller::task_manager::TaskManager,
    network::NetworkClient,
    types::{ClientRequest, DatasetId, RequestError}, utils::logging,
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
    Path((dataset, start_block)): Path<(String, u32)>,
    Extension(client): Extension<Arc<NetworkClient>>,
    Extension(config): Extension<Arc<Config>>,
) -> Response {
    let Some(dataset_id) = config.dataset_id(&dataset) else {
        return RequestError::NotFound(format!("Unknown dataset: {dataset}")).into_response();
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
    match fut.await {
        Err(_) => {
            RequestError::InternalError("Receiving result failed".to_string()).into_response()
        }
        Ok(query_result::Result::Ok(result)) => Response::builder()
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_ENCODING, "gzip")
            .body(Body::from(result.data))
            .unwrap(),
        Ok(res) => RequestError::try_from(res).into_response(),
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
        chunk_timeout: config.default_chunk_timeout,
        timeout_quantile: config.default_timeout_quantile,
        request_multiplier: config.default_request_multiplier,
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
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CONTENT_ENCODING, "gzip")
        .body(Body::from_stream(stream))
        .unwrap()
}

async fn get_network_state(Extension(client): Extension<Arc<NetworkClient>>) -> impl IntoResponse {
    axum::Json(client.network_state())
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
    tracing::info!("Starting HTTP server listening on {addr}");
    let app = Router::new()
        .route("/network/:dataset/height", get(get_height))
        .route("/network/:dataset/stream", post(execute_stream_restricted))
        .route("/network/:dataset/debug", post(execute_stream))
        // for backward compatibility
        .route("/network/:dataset/:start_block/worker", get(get_worker))
        .route("/query/:dataset_id/:worker_id", post(execute_query))
        .route("/network/state", get(get_network_state))
        .layer(axum::middleware::from_fn(logging::middleware))
        .route("/metrics", get(get_metrics))
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
        let chunk_timeout = match params.get("chunk_timeout") {
            Some(value) => match value.parse() {
                Ok(seconds) => Duration::from_secs(seconds),
                Err(e) => {
                    return Err(RequestError::BadRequest(format!(
                        "Couldn't parse chunk_timeout: {e}"
                    ))
                    .into_response())
                }
            },
            None => config.default_chunk_timeout,
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
        let request_multiplier = match params.get("request_multiplier") {
            Some(value) => match value.parse() {
                Ok(value) => value,
                Err(e) => {
                    return Err(RequestError::BadRequest(format!(
                        "Couldn't parse request_multiplier: {e}"
                    ))
                    .into_response())
                }
            },
            None => config.default_request_multiplier,
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
            chunk_timeout,
            timeout_quantile,
            request_multiplier,
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
                RequestError::NotFound(format!("Unknown dataset: {dataset}")).into_response(),
            );
        };

        Ok(dataset_id)
    }
}
