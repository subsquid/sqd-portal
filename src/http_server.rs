use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    async_trait,
    body::Body,
    extract::{FromRequest, Path, Query, Request},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension, RequestExt, Router,
};
use futures::StreamExt;
use prometheus_client::registry::Registry;

use crate::{
    cli::Config,
    controller::task_manager::TaskManager,
    network::NetworkClient,
    types::{ClientRequest, RequestError},
};

async fn get_height(
    Extension(network_state): Extension<Arc<NetworkClient>>,
    Extension(config): Extension<Arc<Config>>,
    Path(dataset): Path<String>,
) -> impl IntoResponse {
    tracing::debug!("Get height dataset={dataset}");
    let dataset_id = match config.dataset_id(&dataset) {
        Some(dataset_id) => dataset_id,
        None => return (StatusCode::NOT_FOUND, format!("Unknown dataset: {dataset}")),
    };

    match network_state.get_height(&dataset_id) {
        Some(height) => (StatusCode::OK, height.to_string()),
        None => (
            StatusCode::NOT_FOUND,
            format!("No data for dataset {dataset}"),
        ),
    }
}

async fn execute_query(
    Path(dataset): Path<String>,
    Extension(task_manager): Extension<Arc<TaskManager>>,
    request: ClientRequest,
) -> Response {
    tracing::info!("Processing query for dataset {dataset}");

    let stream = match task_manager.spawn_stream(request).await {
        Ok(stream) => stream,
        Err(e) => return e.into_response(),
    };
    let stream = stream.map(anyhow::Ok);
    Response::builder().body(Body::from_stream(stream)).unwrap()
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
        .route("/stream/:dataset", post(execute_query))
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
        let Path(dataset) = req
            .extract_parts::<Path<String>>()
            .await
            .map_err(IntoResponse::into_response)?;
        let Query(params) = req
            .extract_parts::<Query<HashMap<String, String>>>()
            .await
            .map_err(IntoResponse::into_response)?;
        let Extension(config) = req
            .extract_parts::<Extension<Arc<Config>>>()
            .await
            .map_err(IntoResponse::into_response)?;
        let query: String = req.extract().await.map_err(IntoResponse::into_response)?;

        let Some(dataset_id) = config.dataset_id(&dataset) else {
            return Err(
                RequestError::NotFound(format!("Unknown dataset: {dataset}")).into_response(),
            );
        };
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
        let backoff = match params.get("backoff") {
            Some(value) => match value.parse() {
                Ok(seconds) => Duration::from_secs(seconds),
                Err(e) => {
                    return Err(
                        RequestError::BadRequest(format!("Couldn't parse backoff: {e}"))
                            .into_response(),
                    )
                }
            },
            None => config.default_backoff,
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
            backoff,
            request_multiplier,
            retries,
        })
    }
}
