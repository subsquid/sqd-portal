use std::{net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    body::Body,
    extract::Path,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension, Router,
};
use futures::StreamExt;
use prometheus_client::registry::Registry;

use crate::{
    cli::Config,
    network::NetworkClient,
    task_manager::TaskManager,
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
    Extension(config): Extension<Arc<Config>>,
    query: String, // request body
) -> Response {
    tracing::info!("Processing query for dataset {dataset}");
    let Some(dataset_id) = config.dataset_id(&dataset) else {
        return RequestError::NotFound(format!("Unknown dataset: {dataset}")).into_response();
    };
    let query = match query.parse() {
        Ok(query) => query,
        Err(e) => {
            return RequestError::BadRequest(format!("Couldn't parse query: {e}")).into_response()
        }
    };

    let stream = match task_manager
        .spawn_stream(ClientRequest {
            chunk_timeout: Duration::from_secs(60),
            dataset_id,
            query,
        })
        .await
    {
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
