use std::future::IntoFuture;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use axum::http::Method;
use axum::{
    async_trait,
    body::Body,
    extract::{FromRequest, FromRequestParts, Path, Query, Request},
    http::{header, request::Parts, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post, MethodRouter},
    Extension, RequestExt, Router,
};
use prometheus_client::registry::Registry;
use sentry::integrations::tower as sentry_tower;
use serde_json::json;
use sqd_contract_client::PeerId;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tower_http::cors::{Any, CorsLayer};
use tower_http::decompression::RequestDecompressionLayer;
use tower_http::request_id::{
    MakeRequestUuid, PropagateRequestIdLayer, RequestId, SetRequestIdLayer,
};
use utoipa::OpenApi;
use utoipa_scalar::{Scalar, Servable as _};
use utoipa_swagger_ui::SwaggerUi;

use crate::datasets::DatasetConfig;
use crate::endpoints::{
    block_number_by_timestamp::get_blocknumber_by_timestamp,
    stream::{
        run_archival_stream, run_archival_stream_restricted, run_finalized_stream, run_stream,
    },
};
use crate::hotblocks::HotblocksErr;
use crate::openapi::ApiDoc;
use crate::types::api_types::AvailableDatasetApiResponse;
use crate::types::Compression;
use crate::utils::conversion::json_lines_to_json;
use crate::utils::logging::MethodRouterExt;
use crate::{
    commercial::{
        extractor as commercial_extractor, CommercialGrant, CommercialRuntime, Endpoint,
        MeterHandle, SnapshotStore, UsageReporter,
    },
    config::Config,
    controller::task_manager::TaskManager,
    hotblocks::HotblocksHandle,
    network::{NetworkClient, NoWorker, NotReady},
    types::{ChunkId, DatasetId, ParsedQuery, RequestError, StreamRequest},
    utils::logging,
};

#[cfg(feature = "sql")]
use crate::sql;
#[cfg(feature = "sql")]
use axum::body;

/// Endpoints whose doc-comment summary starts with this marker are considered internal.
/// Mark a handler like:
///
/// ```ignore
/// /// [INTERNAL] Get debug information for a specific block
/// ```
///
/// They are stripped from the served OpenAPI spec unless `show_internal` is true.
/// When shown, the marker is stripped from the summary for a clean UI.
const INTERNAL_MARKER: &str = "[INTERNAL]";

fn commercial_route_layer(
    route: MethodRouter,
    commercial_enabled: bool,
    endpoint: Endpoint,
) -> MethodRouter {
    if commercial_enabled {
        route.route_layer(axum::middleware::from_fn(move |req, next| {
            commercial_extractor::middleware(req, next, endpoint)
        }))
    } else {
        route
    }
}

#[derive(Clone, Copy, Debug)]
struct CommercialRouteDef {
    method: CommercialRouteMethod,
    path: &'static str,
    endpoint_path: &'static str,
    endpoint: Endpoint,
    handler: CommercialRouteHandler,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CommercialRouteMethod {
    Get,
    Post,
}

#[derive(Clone, Copy, Debug)]
enum CommercialRouteHandler {
    ArchivalStreamRestricted,
    ArchivalStreamDebug,
    FinalizedStream,
    Stream,
    TimestampBlock,
    LegacyQuery,
    #[cfg(feature = "sql")]
    SqlQuery,
}

const COMMERCIAL_ROUTES: &[CommercialRouteDef] = &[
    CommercialRouteDef {
        method: CommercialRouteMethod::Post,
        path: "/datasets/:dataset/archival-stream",
        endpoint_path: "/archival-stream",
        endpoint: Endpoint::ArchivalStream,
        handler: CommercialRouteHandler::ArchivalStreamRestricted,
    },
    CommercialRouteDef {
        method: CommercialRouteMethod::Post,
        path: "/datasets/:dataset/archival-stream/debug",
        endpoint_path: "/archival-stream/debug",
        endpoint: Endpoint::ArchivalStream,
        handler: CommercialRouteHandler::ArchivalStreamDebug,
    },
    CommercialRouteDef {
        method: CommercialRouteMethod::Post,
        path: "/datasets/:dataset/finalized-stream",
        endpoint_path: "/finalized-stream",
        endpoint: Endpoint::FinalizedStream,
        handler: CommercialRouteHandler::FinalizedStream,
    },
    CommercialRouteDef {
        method: CommercialRouteMethod::Post,
        path: "/datasets/:dataset/stream",
        endpoint_path: "/stream",
        endpoint: Endpoint::Stream,
        handler: CommercialRouteHandler::Stream,
    },
    CommercialRouteDef {
        method: CommercialRouteMethod::Get,
        path: "/datasets/:dataset/timestamps/:timestamp/block",
        endpoint_path: "/timestamps/block",
        endpoint: Endpoint::TsLookup,
        handler: CommercialRouteHandler::TimestampBlock,
    },
    CommercialRouteDef {
        method: CommercialRouteMethod::Post,
        path: "/datasets/:dataset_id/query/:worker_id",
        endpoint_path: "/query",
        endpoint: Endpoint::LegacyQuery,
        handler: CommercialRouteHandler::LegacyQuery,
    },
    #[cfg(feature = "sql")]
    CommercialRouteDef {
        method: CommercialRouteMethod::Post,
        path: "/sql/query",
        endpoint_path: "/sql/query",
        endpoint: Endpoint::SqlQuery,
        handler: CommercialRouteHandler::SqlQuery,
    },
];

fn register_commercial_routes<F>(
    mut router: Router,
    commercial_enabled: bool,
    mut route_for: F,
) -> Router
where
    F: FnMut(CommercialRouteDef) -> MethodRouter,
{
    for def in COMMERCIAL_ROUTES {
        router = router.route(
            def.path,
            commercial_route_layer(route_for(*def), commercial_enabled, def.endpoint)
                .endpoint(def.endpoint_path),
        );
    }
    router
}

#[allow(deprecated)]
fn production_commercial_route(def: CommercialRouteDef) -> MethodRouter {
    match (def.method, def.handler) {
        (CommercialRouteMethod::Post, CommercialRouteHandler::ArchivalStreamRestricted) => {
            post(run_archival_stream_restricted)
        }
        (CommercialRouteMethod::Post, CommercialRouteHandler::ArchivalStreamDebug) => {
            post(run_archival_stream)
        }
        (CommercialRouteMethod::Post, CommercialRouteHandler::FinalizedStream) => {
            post(run_finalized_stream)
        }
        (CommercialRouteMethod::Post, CommercialRouteHandler::Stream) => post(run_stream),
        (CommercialRouteMethod::Get, CommercialRouteHandler::TimestampBlock) => {
            get(get_blocknumber_by_timestamp)
        }
        (CommercialRouteMethod::Post, CommercialRouteHandler::LegacyQuery) => post(execute_query),
        #[cfg(feature = "sql")]
        (CommercialRouteMethod::Post, CommercialRouteHandler::SqlQuery) => post(sql_query),
        _ => unreachable!("commercial route method and handler are inconsistent"),
    }
}

fn build_openapi_spec(show_internal: bool) -> utoipa::openapi::OpenApi {
    let mut spec = ApiDoc::openapi();
    spec.paths.paths.retain(|_, item| {
        item.operations.retain(|_, op| {
            let internal = op
                .summary
                .as_deref()
                .is_some_and(|s| s.trim_start().starts_with(INTERNAL_MARKER));
            if internal && !show_internal {
                return false;
            }
            if internal {
                if let Some(summary) = op.summary.as_mut() {
                    *summary = summary
                        .trim_start()
                        .strip_prefix(INTERNAL_MARKER)
                        .unwrap_or(summary)
                        .trim_start()
                        .to_string();
                }
            }
            true
        });
        !item.operations.is_empty()
    });

    // Drop tag declarations that no surviving operation references — otherwise
    // Scalar/Swagger render empty groups.
    let used_tags: std::collections::HashSet<String> = spec
        .paths
        .paths
        .values()
        .flat_map(|item| item.operations.values())
        .flat_map(|op| op.tags.iter().flatten().cloned())
        .collect();
    if let Some(tags) = spec.tags.as_mut() {
        tags.retain(|t| used_tags.contains(&t.name));
    }
    spec
}

#[allow(deprecated)]
#[allow(clippy::too_many_arguments)]
pub async fn run_server(
    task_manager: Arc<TaskManager>,
    network_client: Arc<NetworkClient>,
    metrics_registry: Registry,
    addr: SocketAddr,
    config: Arc<Config>,
    hotblocks: Arc<HotblocksHandle>,
    shutting_down: Arc<AtomicBool>,
    shutdown_signal: CancellationToken,
    commercial: CommercialRuntime,
    show_internal_docs: bool,
) -> anyhow::Result<()> {
    let openapi_spec = build_openapi_spec(show_internal_docs);
    let commercial_enabled = config.commercial.is_some();
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
        .route("/datasets", get(get_datasets).endpoint("/datasets"));
    let app = register_commercial_routes(app, commercial_enabled, production_commercial_route)
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
        // Backward compatibility routes
        .route(
            "/datasets/:dataset/finalized-stream/height",
            get(get_finalized_stream_height).endpoint("/height"),
        )
        .route(
            "/datasets/:dataset/archival-stream/height",
            get(get_archival_stream_height).endpoint("/height"),
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
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", openapi_spec.clone()))
        .merge(Scalar::with_url("/docs", openapi_spec));

    // SQL Query Engine
    #[cfg(feature = "sql")]
    let app = app.route("/sql/metadata", get(sql_metadata).endpoint("/sql/metadata"));

    let drain_timeout = config.drain_timeout;

    let mut app = app
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
        .layer(Extension(hotblocks))
        .layer(Extension(commercial.control_plane))
        .layer(Extension(commercial.usage_reporter))
        .layer(Extension(shutting_down));
    if let Some(snapshot_store) = commercial.snapshot_store {
        app = app.layer(Extension(snapshot_store));
    }
    if let Some(tally) = commercial.tally {
        app = app.layer(Extension(tally));
    }
    if let Some(registry) = commercial.registry {
        app = app.layer(Extension(registry));
    }

    let listener = tokio::net::TcpListener::bind(addr).await?;

    let cancel_for_serve = shutdown_signal.clone();
    let serve = axum::serve(listener, app)
        .with_graceful_shutdown(async move { cancel_for_serve.cancelled().await });

    drive_serve_with_drain(serve.into_future(), shutdown_signal, drain_timeout).await?;

    tracing::info!("HTTP server stopped");
    Ok(())
}

/// Races axum's graceful drain against a hard `drain_timeout` deadline.
///
/// See [`docs/GRACEFUL_SHUTDOWN.md`](../docs/GRACEFUL_SHUTDOWN.md) for the
/// drain semantics — including what "force-close" actually does (and does
/// not) to in-flight per-connection tasks.
async fn drive_serve_with_drain<F>(
    serve: F,
    shutdown_signal: CancellationToken,
    drain_timeout: std::time::Duration,
) -> std::io::Result<()>
where
    F: std::future::Future<Output = std::io::Result<()>>,
{
    let force_close = async {
        shutdown_signal.cancelled().await;
        tokio::time::sleep(drain_timeout).await;
    };

    tokio::select! {
        res = serve => {
            res?;
            tracing::info!("HTTP server drained cleanly");
        }
        _ = force_close => {
            tracing::warn!(
                "Drain timeout {:?} exceeded; listener will close on serve drop. \
                 In-flight connections are detached and will be aborted only on \
                 process exit (runtime drop after main() returns).",
                drain_timeout
            );
        }
    }
    Ok(())
}

/// [INTERNAL] Latest archival head
///
/// Returns the block number and hash of the highest archived block (no hotblocks).
#[utoipa::path(
    get,
    path = "/datasets/{dataset}/archival-head",
    params(
        ("dataset" = String, Path, description = "Dataset name"),
    ),
    responses(
        (status = 200, description = "Archival head block retrieved", body = Option<BlockHead>),
        (status = 404, description = "Dataset has no archival data source"),
    ),
    tag = "Archival stream"
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

/// Latest finalized head
///
/// Returns the block number and hash of the highest finalized block.
#[utoipa::path(
    get,
    path = "/datasets/{dataset}/finalized-head",
    params(
        ("dataset" = String, Path, description = "Dataset name"),
    ),
    responses(
        (status = 200, description = "Finalized head block retrieved", body = Option<BlockHead>),
        (status = 404, description = "Dataset has no data sources"),
    ),
    tag = "Stream"
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

/// Latest head
///
/// Returns the block number and hash of the highest block, including real-time data.
#[utoipa::path(
    get,
    path = "/datasets/{dataset}/head",
    params(
        ("dataset" = String, Path, description = "Dataset name"),
    ),
    responses(
        (status = 200, description = "Head block retrieved", body = Option<BlockHead>),
        (status = 404, description = "Dataset has no data sources"),
    ),
    tag = "Stream"
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

/// [INTERNAL] Portal Status
///
/// Returns a JSON document with human-readable information about the portal's state.
/// The exact format may change without notice.
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
    tag = "Status"
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

/// Available Datasets
///
/// Returns the list of datasets served by this portal.
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
    tag = "Datasets"
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

/// [INTERNAL] Dataset State
///
/// Returns the current state of the dataset.
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
    tag = "Datasets"
)]
async fn get_dataset_state(
    dataset_id: DatasetId,
    Extension(network): Extension<Arc<NetworkClient>>,
) -> impl IntoResponse {
    axum::Json(network.dataset_state(&dataset_id))
}

/// Dataset Metadata
///
/// Returns dataset metadata: chain info, first block, and optional expanded fields.
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
    tag = "Datasets"
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

/// [INTERNAL] Block Debug Info
///
/// Returns worker information for the given dataset and block.
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
    tag = "Debug"
)]
async fn get_debug_block(
    Path((_dataset, block)): Path<(String, u64)>,
    dataset_id: DatasetId,
    Extension(client): Extension<Arc<NetworkClient>>,
) -> axum::Json<serde_json::Value> {
    axum::Json(json!({
        "workers": client.get_workers(&dataset_id, block),
    }))
}

/// [INTERNAL] Worker Inventory
///
/// Returns information about all workers currently visible to the portal.
#[utoipa::path(
    get,
    path = "/debug/workers",
    responses(
        (status = 200, description = "All worker information retrieved", body = serde_json::Value),
    ),
    tag = "Debug"
)]
async fn get_all_workers(
    Extension(client): Extension<Arc<NetworkClient>>,
) -> axum::Json<serde_json::Value> {
    axum::Json(json!({
        "workers": client.get_all_workers(),
    }))
}

/// [INTERNAL] Prometheus Metrics
///
/// Returns portal metrics in OpenMetrics text format.
#[utoipa::path(
    get,
    path = "/metrics",
    responses(
        (status = 200, description = "Metrics in OpenMetrics format", content_type = "application/openmetrics-text"),
    ),
    tag = "Status"
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

/// [INTERNAL] Readiness Probe
///
/// Returns 200 once the portal is ready to serve requests; 503 otherwise (e.g. during shutdown).
#[utoipa::path(
    get,
    path = "/ready",
    responses(
        (status = 200, description = "Portal is ready"),
        (status = 503, description = "Portal is not ready"),
    ),
    tag = "Status"
)]
async fn get_readiness(
    Extension(client): Extension<Arc<NetworkClient>>,
    Extension(shutting_down): Extension<Arc<AtomicBool>>,
    snapshot_store: Option<Extension<Arc<SnapshotStore>>>,
) -> impl IntoResponse {
    let decision = readiness_decision(
        shutting_down.load(Ordering::Relaxed),
        snapshot_store.as_ref().map(|store| store.is_ready()),
        || client.readiness(),
    );

    readiness_response(decision)
}

// Stable discriminant per readiness *category*. `/ready` is polled
// continuously, so we log only when the category changes — entering a new
// state logs once (with live detail), while fluctuating connection counts
// within `InsufficientConnections` do not. Starts `READY` so a portal that
// never becomes ready still logs the reason on its first probe.
const READINESS_READY: u8 = 0;
const READINESS_SHUTTING_DOWN: u8 = 1;
const READINESS_NO_WORKERS: u8 = 2;
const READINESS_INSUFFICIENT_CONNECTIONS: u8 = 3;
const READINESS_SNAPSHOTS_NOT_READY: u8 = 4;

#[derive(Clone, Copy)]
struct ReadinessDecision {
    state: u8,
    code: StatusCode,
    body: &'static str,
    reason: Option<NotReady>,
}

fn readiness_decision(
    shutting_down: bool,
    snapshots_ready: Option<bool>,
    client_readiness: impl FnOnce() -> Result<(), NotReady>,
) -> ReadinessDecision {
    if shutting_down {
        return ReadinessDecision {
            state: READINESS_SHUTTING_DOWN,
            code: StatusCode::SERVICE_UNAVAILABLE,
            body: "Shutting down",
            reason: None,
        };
    }

    if snapshots_ready.is_some_and(|ready| !ready) {
        return ReadinessDecision {
            state: READINESS_SNAPSHOTS_NOT_READY,
            code: StatusCode::SERVICE_UNAVAILABLE,
            body: "Commercial snapshots not ready",
            reason: None,
        };
    }

    match client_readiness() {
        Ok(()) => ReadinessDecision {
            state: READINESS_READY,
            code: StatusCode::OK,
            body: "Ready",
            reason: None,
        },
        Err(reason) => {
            let state = match reason {
                NotReady::NoWorkers => READINESS_NO_WORKERS,
                NotReady::InsufficientConnections { .. } => READINESS_INSUFFICIENT_CONNECTIONS,
            };
            ReadinessDecision {
                state,
                code: StatusCode::SERVICE_UNAVAILABLE,
                body: "Not ready",
                reason: Some(reason),
            }
        }
    }
}

fn readiness_response(decision: ReadinessDecision) -> Response {
    static LAST_STATE: AtomicU8 = AtomicU8::new(READINESS_READY);
    if LAST_STATE.swap(decision.state, Ordering::Relaxed) != decision.state {
        match (decision.state, decision.reason) {
            (READINESS_READY, _) => tracing::info!("readiness check now passing: portal is ready"),
            (READINESS_SHUTTING_DOWN, _) => {
                tracing::info!("readiness check now failing: shutting down")
            }
            (READINESS_SNAPSHOTS_NOT_READY, _) => {
                tracing::warn!("readiness check now failing: commercial snapshots not ready");
            }
            (_, Some(reason)) => tracing::warn!("readiness check now failing: {reason}"),
            (_, None) => {}
        }
    }

    (decision.code, decision.body).into_response()
}

/// [INTERNAL] Dataset Height (deprecated)
///
/// Returns the current height of the dataset. Kept for backward compatibility.
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
    tag = "Stream",
)]
#[deprecated]
async fn get_height(
    Extension(network): Extension<Arc<NetworkClient>>,
    Path(dataset): Path<String>,
    dataset_id: DatasetId,
) -> impl IntoResponse {
    height_response(&network, &dataset, &dataset_id)
}

/// [INTERNAL] Finalized Stream Height (deprecated)
///
/// Same as /datasets/{dataset}/height. Kept for backward compatibility.
#[utoipa::path(
    get,
    path = "/datasets/{dataset}/finalized-stream/height",
    params(
        ("dataset" = String, Path, description = "Dataset name"),
    ),
    responses(
        (status = 200, description = "Height retrieved successfully", body = String),
        (status = 404, description = "Dataset not found"),
    ),
    tag = "Stream",
)]
#[deprecated]
async fn get_finalized_stream_height(
    Extension(network): Extension<Arc<NetworkClient>>,
    Path(dataset): Path<String>,
    dataset_id: DatasetId,
) -> impl IntoResponse {
    height_response(&network, &dataset, &dataset_id)
}

/// [INTERNAL] Archival Stream Height (deprecated)
///
/// Same as /datasets/{dataset}/height. Kept for backward compatibility.
#[utoipa::path(
    get,
    path = "/datasets/{dataset}/archival-stream/height",
    params(
        ("dataset" = String, Path, description = "Dataset name"),
    ),
    responses(
        (status = 200, description = "Height retrieved successfully", body = String),
        (status = 404, description = "Dataset not found"),
    ),
    tag = "Archival stream",
)]
#[deprecated]
async fn get_archival_stream_height(
    Extension(network): Extension<Arc<NetworkClient>>,
    Path(dataset): Path<String>,
    dataset_id: DatasetId,
) -> impl IntoResponse {
    height_response(&network, &dataset, &dataset_id)
}

fn height_response(
    network: &NetworkClient,
    dataset: &str,
    dataset_id: &DatasetId,
) -> (StatusCode, String) {
    match network.get_height(dataset_id) {
        Some(height) => (StatusCode::OK, height.to_string()),
        None => (
            StatusCode::NOT_FOUND,
            format!("No data for dataset {dataset}"),
        ),
    }
}

/// [INTERNAL] Get worker information for a block range (deprecated)
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
        (status = 200, description = "Worker URL retrieved", body = String),
        (status = 404, description = "Dataset not found"),
        (status = 429, description = "Rate limit exceeded"),
        (status = 503, description = "No available workers"),
    ),
    tag = "Debug",
)]
#[deprecated]
async fn get_worker(
    Path((dataset, start_block)): Path<(String, u64)>,
    dataset_id: DatasetId,
    Extension(client): Extension<Arc<NetworkClient>>,
    Extension(config): Extension<Arc<Config>>,
) -> Response {
    let worker_id = match client.find_worker(&dataset_id, start_block) {
        Ok(worker_id) => worker_id.worker(),
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

/// [INTERNAL] Worker Query (deprecated)
///
/// Sends a data query to a specific worker in the network. Deprecated in favor of /stream.
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
    tag = "Stream",
)]
#[deprecated]
async fn execute_query(
    Path((dataset_id_encoded, worker_id)): Path<(String, PeerId)>,
    Extension(client): Extension<Arc<NetworkClient>>,
    Extension(req): Extension<RequestId>,
    grant: Option<Extension<CommercialGrant>>,
    reporter: Option<Extension<Arc<dyn UsageReporter>>>,
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
    let meter = meter_from_extensions(
        grant,
        reporter,
        Endpoint::LegacyQuery,
        dataset_id.to_url().to_string(),
        request_id.clone(),
    );

    let Ok(chunk) = client.find_chunk(&dataset_id, query.first_block()) else {
        finish_meter_error(&meter);
        return RequestError::NoData.into_response();
    };
    let range = query
        .intersect_with(&chunk.block_range())
        .expect("Found chunk should intersect with query");

    let lease = match client.reserve_worker(worker_id) {
        Some(lease) => lease,
        None => {
            finish_meter_error(&meter);
            return RequestError::BadRequest(format!("Worker {} does not exist", worker_id))
                .into_response();
        }
    };
    let fut = client.query_worker(
        lease,
        request_id,
        ChunkId::new(dataset_id, chunk),
        range,
        query.into_string(),
        Compression::Gzip,
        None,
    );
    let result = match fut.await {
        Ok(success) => success.ok,
        Err(err) => {
            finish_meter_error(&meter);
            return RequestError::from_query_error(err, worker_id).into_response();
        }
    };
    match json_lines_to_json(&result.data) {
        Ok(data) => {
            if let Some(meter) = &meter {
                if let Err(err) = meter.record_gzip_body_and_complete(&data) {
                    tracing::warn!(error = %err, "cannot decode legacy query response for usage meter");
                    meter.mark_error();
                    meter.complete();
                }
            }
            Response::builder()
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::CONTENT_ENCODING, "gzip")
                .body(Body::from(data))
                .unwrap()
        }
        Err(e) => {
            finish_meter_error(&meter);
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
            .ok_or((StatusCode::NOT_FOUND, "not enough arguments").into_response())?;
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
            max_stored_results_per_chunk: config.max_stored_results_per_chunk.max(1),
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
    grant: Option<Extension<CommercialGrant>>,
    reporter: Option<Extension<Arc<dyn UsageReporter>>>,
    request_id: Option<Extension<RequestId>>,
    query: body::Bytes,
) -> impl axum::response::IntoResponse {
    match sql::query(query, &network).await {
        Ok(res) => {
            if let Some(meter) = meter_from_extensions(
                grant,
                reporter,
                Endpoint::SqlQuery,
                "sql".to_string(),
                request_id
                    .and_then(|id| id.header_value().to_str().ok().map(str::to_owned))
                    .unwrap_or_default(),
            ) {
                record_json_response_usage(meter, &res, "sql");
            }
            axum::Json(res).into_response()
        }
        Err(e) => {
            tracing::warn!("cannot query data: {:?}", e);
            e.into_response()
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
    match (grant.0.tally.clone(), grant.0.registry.clone()) {
        (Some(tally), Some(registry)) => Some(MeterHandle::new_enforced(
            grant.0.granted.clone(),
            request_id,
            endpoint,
            dataset,
            reporter.0,
            tally,
            registry,
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

fn finish_meter_error(meter: &Option<MeterHandle>) {
    if let Some(meter) = meter {
        meter.mark_error();
        meter.complete();
    }
}

#[cfg(feature = "sql")]
fn record_json_response_usage<T: serde::Serialize>(meter: MeterHandle, res: &T, endpoint: &str) {
    match serde_json::to_vec(res) {
        Ok(bytes) => meter.record_plain_bytes_and_complete(bytes.len() as u64),
        Err(err) => {
            tracing::warn!(error = %err, endpoint, "cannot serialize response for usage meter");
            meter.mark_error();
            meter.complete();
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::async_trait;
    use std::future;
    use std::io;
    use std::sync::Mutex;
    use std::time::Duration;

    use tower::ServiceExt;

    use crate::commercial::{
        evaluate::EvaluationPolicy,
        store::test_support::{active_snapshot, defaults_record, KEY_ID},
        ActiveStreamRegistry, Authorization, ConcurrencyLimiter, ControlPlaneClient, Credential,
        Granted, GrantedLimits, LocalControlPlane, OnExceed, Principal, PublicFallbackConfig,
        SnapshotRecord, TallyStore,
    };

    #[test]
    fn hide_internal_drops_marked_ops_and_empty_tags() {
        let hidden = build_openapi_spec(false);
        let summaries: Vec<&str> = hidden
            .paths
            .paths
            .values()
            .flat_map(|i| i.operations.values())
            .filter_map(|o| o.summary.as_deref())
            .collect();
        assert!(
            summaries.iter().all(|s| !s.contains(INTERNAL_MARKER)),
            "no surviving op should carry the marker: {summaries:?}"
        );

        let tag_names: Vec<&str> = hidden
            .tags
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|t| t.name.as_str())
            .collect();
        for empty_tag in ["Status", "Debug", "Archival stream"] {
            assert!(
                !tag_names.contains(&empty_tag),
                "tag {empty_tag} should be pruned (all its operations are internal); got {tag_names:?}"
            );
        }
    }

    #[test]
    fn show_internal_keeps_all_and_strips_marker() {
        let shown = build_openapi_spec(true);
        let full = ApiDoc::openapi();
        assert_eq!(shown.paths.paths.len(), full.paths.paths.len());
        let summaries: Vec<&str> = shown
            .paths
            .paths
            .values()
            .flat_map(|i| i.operations.values())
            .filter_map(|o| o.summary.as_deref())
            .collect();
        assert!(
            summaries.iter().all(|s| !s.contains(INTERNAL_MARKER)),
            "marker should be stripped from summaries when shown: {summaries:?}"
        );
    }

    fn assert_readiness_decision(
        decision: ReadinessDecision,
        state: u8,
        code: StatusCode,
        body: &'static str,
        reason: Option<NotReady>,
    ) {
        assert_eq!(decision.state, state);
        assert_eq!(decision.code, code);
        assert_eq!(decision.body, body);
        assert_eq!(decision.reason, reason);
    }

    #[test]
    fn ready_endpoint_flips_to_503_when_shutdown_flag_set() {
        let mut client_readiness_called = false;

        let decision = readiness_decision(true, None, || {
            client_readiness_called = true;
            Ok(())
        });

        assert_readiness_decision(
            decision,
            READINESS_SHUTTING_DOWN,
            StatusCode::SERVICE_UNAVAILABLE,
            "Shutting down",
            None,
        );
        assert!(!client_readiness_called);
    }

    #[derive(Default)]
    struct RecordingReporter {
        events: Mutex<Vec<crate::commercial::StreamUsageEvent>>,
    }

    impl UsageReporter for RecordingReporter {
        fn report(&self, event: crate::commercial::StreamUsageEvent) {
            self.events.lock().unwrap().push(event);
        }
    }

    async fn metered_test_handler(
        endpoint: Option<Extension<Endpoint>>,
        grant: Option<Extension<CommercialGrant>>,
        reporter: Option<Extension<Arc<dyn UsageReporter>>>,
    ) -> Response {
        const BODY: &[u8] = b"{\"ok\":true}\n";
        let endpoint = endpoint
            .map(|endpoint| endpoint.0)
            .unwrap_or(Endpoint::Stream);
        let meter = meter_from_extensions(
            grant,
            reporter,
            endpoint,
            "ethereum-mainnet".to_string(),
            "request".to_string(),
        );
        let meter_created = meter.is_some();
        if let Some(meter) = meter {
            meter.record_plain_bytes_and_complete(BODY.len() as u64);
        }

        Response::builder()
            .header("x-meter-created", meter_created.to_string())
            .body(Body::from(BODY))
            .unwrap()
    }

    fn test_commercial_route(def: CommercialRouteDef) -> MethodRouter {
        let route = match def.method {
            CommercialRouteMethod::Get => get(metered_test_handler),
            CommercialRouteMethod::Post => post(metered_test_handler),
        };
        route.layer(Extension(def.endpoint))
    }

    fn http_method(method: CommercialRouteMethod) -> Method {
        match method {
            CommercialRouteMethod::Get => Method::GET,
            CommercialRouteMethod::Post => Method::POST,
        }
    }

    fn sample_path(def: CommercialRouteDef) -> &'static str {
        match def.path {
            "/datasets/:dataset/archival-stream" => "/datasets/ethereum-mainnet/archival-stream",
            "/datasets/:dataset/archival-stream/debug" => {
                "/datasets/ethereum-mainnet/archival-stream/debug"
            }
            "/datasets/:dataset/finalized-stream" => "/datasets/ethereum-mainnet/finalized-stream",
            "/datasets/:dataset/stream" => "/datasets/ethereum-mainnet/stream",
            "/datasets/:dataset/timestamps/:timestamp/block" => {
                "/datasets/ethereum-mainnet/timestamps/1/block"
            }
            "/datasets/:dataset_id/query/:worker_id" => "/datasets/ethereum-mainnet/query/worker",
            "/sql/query" => "/sql/query",
            path => unreachable!("missing commercial route test sample for {path}"),
        }
    }

    async fn held_meter_stream_handler(
        endpoint: Option<Extension<Endpoint>>,
        grant: Option<Extension<CommercialGrant>>,
        reporter: Option<Extension<Arc<dyn UsageReporter>>>,
    ) -> Response {
        let endpoint = endpoint
            .map(|endpoint| endpoint.0)
            .unwrap_or(Endpoint::Stream);
        let meter = meter_from_extensions(
            grant,
            reporter,
            endpoint,
            "ethereum-mainnet".to_string(),
            "request".to_string(),
        )
        .expect("commercial middleware should attach a grant");
        let stream = futures::stream::once(async move {
            let _meter = meter;
            future::pending::<Result<bytes::Bytes, io::Error>>().await
        });

        Response::builder()
            .header("x-meter-created", "true")
            .body(Body::from_stream(stream))
            .unwrap()
    }

    fn held_meter_test_route(def: CommercialRouteDef) -> MethodRouter {
        let route = match def.method {
            CommercialRouteMethod::Get => get(held_meter_stream_handler),
            CommercialRouteMethod::Post => post(held_meter_stream_handler),
        };
        route.layer(Extension(def.endpoint))
    }

    fn commercial_concurrency_test_app(store: Arc<SnapshotStore>) -> axum::Router {
        let tally = Arc::new(TallyStore::default());
        let registry = Arc::new(ActiveStreamRegistry::default());
        let concurrency = Arc::new(ConcurrencyLimiter::new(1));
        let control_plane = Arc::new(LocalControlPlane::new(
            store,
            tally.clone(),
            concurrency,
            EvaluationPolicy {
                throttle_residual_secs: 60,
            },
        )) as Arc<dyn ControlPlaneClient>;

        register_commercial_routes(axum::Router::new(), true, held_meter_test_route)
            .layer(Extension(control_plane))
            .layer(Extension(
                Arc::new(RecordingReporter::default()) as Arc<dyn UsageReporter>
            ))
            .layer(Extension(tally))
            .layer(Extension(registry))
    }

    fn public_fallback_for_concurrency_test() -> PublicFallbackConfig {
        PublicFallbackConfig {
            throughput_bytes_per_sec: 1_000_000,
            burst_bytes: 1_000_000,
            max_response_bytes: 1_000_000,
            volume_bytes: 1_000_000,
            window_secs: 60,
            // The runtime limiter adds one guardrail permit to the configured
            // value, so 0 is the non-invasive way to test one effective permit.
            concurrency: 0,
        }
    }

    fn store_for_keyed_concurrency_test() -> Arc<SnapshotStore> {
        let store = SnapshotStore::inactive(&public_fallback_for_concurrency_test());
        let mut defaults = defaults_record(10);
        defaults.messages.insert(
            "concurrency_limit".to_string(),
            "concurrency_limit".to_string(),
        );
        let mut snapshot = active_snapshot(11);
        snapshot.limits.as_mut().unwrap().concurrency = Some(0);
        store.install_records_for_test(
            vec![
                SnapshotRecord::Defaults(defaults),
                SnapshotRecord::Key(snapshot),
            ],
            11,
        );
        store
    }

    fn store_for_anonymous_concurrency_test() -> Arc<SnapshotStore> {
        let store = SnapshotStore::inactive(&public_fallback_for_concurrency_test());
        let mut defaults = defaults_record(10);
        defaults.public.limits.concurrency = Some(0);
        defaults.public.quota.volume_bytes = 1_000_000;
        defaults.messages.insert(
            "concurrency_limit".to_string(),
            "concurrency_limit".to_string(),
        );
        store.install_records_for_test(vec![SnapshotRecord::Defaults(defaults)], 10);
        store
    }

    fn stream_request(uri: &str, forwarded_for: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder().method(Method::POST).uri(uri);
        if let Some(forwarded_for) = forwarded_for {
            builder = builder.header("x-forwarded-for", forwarded_for);
        }
        builder
            .body(Body::from(
                r#"{"type":"evm","fromBlock":1,"fields":{"block":{"number":true}}}"#,
            ))
            .unwrap()
    }

    async fn assert_concurrency_rejection(response: Response) {
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()["retry-after"], "1");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], b"concurrency_limit");
    }

    #[tokio::test]
    async fn commercial_disabled_skips_extractor_and_meter_on_metered_route() {
        let reporter = Arc::new(RecordingReporter::default());
        let app = axum::Router::new()
            .route(
                "/datasets/ethereum-mainnet/stream",
                commercial_route_layer(get(metered_test_handler), false, Endpoint::Stream),
            )
            .layer(Extension(reporter.clone() as Arc<dyn UsageReporter>));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/datasets/ethereum-mainnet/stream?api_key=sqd_data_key_secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-meter-created"], "false");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], b"{\"ok\":true}\n");
        assert!(
            reporter.events.lock().unwrap().is_empty(),
            "no meter means no usage event or commercial metric side effect"
        );
    }

    fn public_fallback_for_ready_test() -> PublicFallbackConfig {
        PublicFallbackConfig {
            throughput_bytes_per_sec: 1,
            burst_bytes: 1,
            max_response_bytes: 1,
            volume_bytes: 1,
            window_secs: 1,
            concurrency: 1,
        }
    }

    #[test]
    fn ready_endpoint_waits_for_commercial_snapshot_store_before_network_readiness() {
        let store = SnapshotStore::inactive(&public_fallback_for_ready_test());
        store.set_ready_for_test(false);
        let mut client_readiness_called = false;

        let decision = readiness_decision(false, Some(store.is_ready()), || {
            client_readiness_called = true;
            Ok(())
        });

        assert_readiness_decision(
            decision,
            READINESS_SNAPSHOTS_NOT_READY,
            StatusCode::SERVICE_UNAVAILABLE,
            "Commercial snapshots not ready",
            None,
        );
        assert!(!client_readiness_called);

        store.set_ready_for_test(true);
        let decision = readiness_decision(false, Some(store.is_ready()), || Ok(()));

        assert_readiness_decision(decision, READINESS_READY, StatusCode::OK, "Ready", None);
    }

    #[test]
    fn ready_endpoint_is_ready_immediately_when_snapshot_store_loaded_from_disk_cache() {
        let store = SnapshotStore::inactive(&public_fallback_for_ready_test());
        store.set_loaded_from_cache_for_test(true);
        assert!(store.loaded_from_cache());

        let decision = readiness_decision(false, Some(store.is_ready()), || Ok(()));

        assert_readiness_decision(decision, READINESS_READY, StatusCode::OK, "Ready", None);
    }

    #[test]
    fn ready_endpoint_oss_mode_passthrough_matches_network_readiness() {
        let ready = readiness_decision(false, None, || Ok(()));

        assert_readiness_decision(ready, READINESS_READY, StatusCode::OK, "Ready", None);

        let not_ready = readiness_decision(false, None, || Err(NotReady::NoWorkers));

        assert_readiness_decision(
            not_ready,
            READINESS_NO_WORKERS,
            StatusCode::SERVICE_UNAVAILABLE,
            "Not ready",
            Some(NotReady::NoWorkers),
        );
    }

    struct GrantingControlPlane;

    #[async_trait]
    impl ControlPlaneClient for GrantingControlPlane {
        async fn authorize(&self, req: crate::commercial::AuthorizeRequest) -> Authorization {
            let api_key_id = match req.credential {
                Credential::Key { key_id, .. } => Some(key_id),
                Credential::None { .. } | Credential::Internal { .. } => None,
            };
            Authorization::Granted(Granted {
                principal: Principal {
                    account_id: "account".to_string(),
                    api_key_id,
                },
                tally_account_id: None,
                limits: GrantedLimits::default(),
                on_exceed: OnExceed::Reject,
                quota_version: 1,
                quota_remaining_bytes: Some(1_000_000),
                concurrency_permit: None,
            })
        }
    }

    #[tokio::test]
    async fn commercial_enabled_meters_all_metered_route_identities() {
        let query = r#"{"type":"evm","fromBlock":1,"fields":{"block":{"number":true}}}"#;
        let expected = vec![
            (
                CommercialRouteMethod::Post,
                "/datasets/:dataset/archival-stream",
                Endpoint::ArchivalStream,
            ),
            (
                CommercialRouteMethod::Post,
                "/datasets/:dataset/archival-stream/debug",
                Endpoint::ArchivalStream,
            ),
            (
                CommercialRouteMethod::Post,
                "/datasets/:dataset/finalized-stream",
                Endpoint::FinalizedStream,
            ),
            (
                CommercialRouteMethod::Post,
                "/datasets/:dataset/stream",
                Endpoint::Stream,
            ),
            (
                CommercialRouteMethod::Get,
                "/datasets/:dataset/timestamps/:timestamp/block",
                Endpoint::TsLookup,
            ),
            (
                CommercialRouteMethod::Post,
                "/datasets/:dataset_id/query/:worker_id",
                Endpoint::LegacyQuery,
            ),
            #[cfg(feature = "sql")]
            (
                CommercialRouteMethod::Post,
                "/sql/query",
                Endpoint::SqlQuery,
            ),
        ];
        let actual = COMMERCIAL_ROUTES
            .iter()
            .map(|def| (def.method, def.path, def.endpoint))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);

        let reporter = Arc::new(RecordingReporter::default());
        let app = register_commercial_routes(axum::Router::new(), true, test_commercial_route)
            .layer(Extension(reporter.clone() as Arc<dyn UsageReporter>))
            .layer(Extension(
                Arc::new(GrantingControlPlane) as Arc<dyn ControlPlaneClient>
            ));

        for def in COMMERCIAL_ROUTES {
            let body = if matches!(
                def.endpoint,
                Endpoint::Stream
                    | Endpoint::FinalizedStream
                    | Endpoint::ArchivalStream
                    | Endpoint::LegacyQuery
            ) {
                Some(query)
            } else {
                None
            };
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(http_method(def.method))
                        .uri(format!("{}?api_key=sqd_data_key_secret", sample_path(*def)))
                        .body(body.map_or_else(Body::empty, Body::from))
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::OK, "{}", def.path);
            assert_eq!(
                response.headers()["x-meter-created"],
                "true",
                "{}",
                def.path
            );
            let event = reporter.events.lock().unwrap().pop().expect("usage event");
            assert_eq!(event.endpoint, def.endpoint, "{}", def.path);
            assert_eq!(event.logical_bytes, 12, "{}", def.path);
        }
    }

    #[tokio::test]
    async fn keyed_live_stream_concurrency_rejects_until_meter_drops() {
        let app = commercial_concurrency_test_app(store_for_keyed_concurrency_test());
        let keyed_uri = format!(
            "/datasets/ethereum-mainnet/stream?api_key=sqd_data_{KEY_ID}_0123456789abcdefghijklmnopqrstuv"
        );

        let response_a = app
            .clone()
            .oneshot(stream_request(&keyed_uri, None))
            .await
            .unwrap();
        assert_eq!(response_a.status(), StatusCode::OK);
        assert_eq!(response_a.headers()["x-meter-created"], "true");

        let response_b = app
            .clone()
            .oneshot(stream_request(&keyed_uri, None))
            .await
            .unwrap();
        assert_concurrency_rejection(response_b).await;

        drop(response_a);

        let response_b_retry = app.oneshot(stream_request(&keyed_uri, None)).await.unwrap();
        assert_eq!(response_b_retry.status(), StatusCode::OK);
        assert_eq!(response_b_retry.headers()["x-meter-created"], "true");
    }

    #[tokio::test]
    async fn anonymous_live_stream_concurrency_rejects_until_meter_drops() {
        let app = commercial_concurrency_test_app(store_for_anonymous_concurrency_test());
        let anon_uri = "/datasets/ethereum-mainnet/stream";

        let response_a = app
            .clone()
            .oneshot(stream_request(anon_uri, Some("203.0.113.7")))
            .await
            .unwrap();
        assert_eq!(response_a.status(), StatusCode::OK);
        assert_eq!(response_a.headers()["x-meter-created"], "true");

        let response_b = app
            .clone()
            .oneshot(stream_request(anon_uri, Some("203.0.113.7")))
            .await
            .unwrap();
        assert_concurrency_rejection(response_b).await;

        drop(response_a);

        let response_b_retry = app
            .oneshot(stream_request(anon_uri, Some("203.0.113.7")))
            .await
            .unwrap();
        assert_eq!(response_b_retry.status(), StatusCode::OK);
        assert_eq!(response_b_retry.headers()["x-meter-created"], "true");
    }

    #[tokio::test]
    async fn hotblocks_head_status_forwards_stay_unmetered() {
        for body in [
            b"{\"number\":42}".as_slice(),
            b"{\"status\":\"ok\"}".as_slice(),
        ] {
            let response = reqwest_response(body.to_vec()).await;
            let forwarded = forward_hotblocks_response(Ok(response));
            let forwarded_body = axum::body::to_bytes(forwarded.into_body(), usize::MAX)
                .await
                .unwrap();

            assert_eq!(&forwarded_body[..], body);
        }
    }

    async fn reqwest_response(body: Vec<u8>) -> reqwest::Response {
        let app = axum::Router::new().route(
            "/hotblocks",
            axum::routing::get(move || {
                let body = body.clone();
                async move { Body::from(body) }
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

    #[tokio::test(start_paused = true)]
    async fn drive_serve_returns_ok_when_serve_finishes_before_drain_timeout() {
        let cancel = CancellationToken::new();
        let drain_timeout = Duration::from_secs(10);

        let serve = async {
            // Simulates axum::serve resolving promptly after cancellation
            // (the with_graceful_shutdown future fires, hyper drains and returns Ok).
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok::<(), std::io::Error>(())
        };

        let driver = tokio::spawn(drive_serve_with_drain(serve, cancel.clone(), drain_timeout));

        tokio::time::advance(Duration::from_millis(10)).await;
        cancel.cancel();

        // Serve resolves before drain_timeout, so we get back quickly.
        tokio::time::advance(Duration::from_millis(100)).await;
        let res = driver.await.expect("join");
        res.expect("clean drain returns Ok");
    }

    #[tokio::test(start_paused = true)]
    async fn drive_serve_force_closes_when_serve_outlasts_drain_timeout() {
        let cancel = CancellationToken::new();
        let drain_timeout = Duration::from_millis(100);

        let serve = std::future::pending::<std::io::Result<()>>();

        let driver = tokio::spawn(drive_serve_with_drain(serve, cancel.clone(), drain_timeout));

        // Trigger shutdown immediately; force_close arm starts its drain_timeout countdown.
        cancel.cancel();

        // Before drain_timeout the driver should still be pending.
        tokio::time::advance(Duration::from_millis(50)).await;
        assert!(!driver.is_finished(), "should still be draining");

        // After drain_timeout the force-close arm wins.
        tokio::time::advance(Duration::from_millis(60)).await;
        let res = driver.await.expect("join");
        res.expect("force-close returns Ok");
    }

    #[tokio::test(start_paused = true)]
    async fn drive_serve_propagates_serve_error() {
        let cancel = CancellationToken::new();
        let drain_timeout = Duration::from_secs(10);

        let serve = async {
            Err::<(), std::io::Error>(std::io::Error::new(std::io::ErrorKind::Other, "boom"))
        };

        let res = drive_serve_with_drain(serve, cancel, drain_timeout).await;
        let err = res.expect_err("error from serve propagates");
        assert_eq!(err.to_string(), "boom");
    }

    #[cfg(feature = "sql")]
    #[test]
    fn sql_response_usage_records_serialized_body() {
        use std::sync::Mutex;

        use crate::commercial::{Principal, StreamUsageEvent, UsageReporter, UsageStatus};
        use crate::sql::query::TableItem;

        #[derive(Default)]
        struct RecordingReporter {
            events: Mutex<Vec<StreamUsageEvent>>,
        }

        impl UsageReporter for RecordingReporter {
            fn report(&self, event: StreamUsageEvent) {
                self.events.lock().unwrap().push(event);
            }
        }

        let reporter = Arc::new(RecordingReporter::default());
        let meter = MeterHandle::new(
            Principal {
                account_id: "account".to_string(),
                api_key_id: Some("key".to_string()),
            },
            "request".to_string(),
            Endpoint::SqlQuery,
            "sql".to_string(),
            reporter.clone(),
            3,
        );
        let response = crate::sql::query::SqlQueryResponse {
            query_id: "sql-test".to_string(),
            tables: Vec::<TableItem>::new(),
        };
        let expected = serde_json::to_vec(&response).unwrap().len() as u64;

        record_json_response_usage(meter, &response, "sql");

        let event = reporter.events.lock().unwrap().pop().unwrap();
        assert_eq!(event.endpoint, Endpoint::SqlQuery);
        assert_eq!(event.logical_bytes, expected);
        assert_eq!(event.wire_bytes, expected);
        assert_eq!(event.status, UsageStatus::Completed);
    }
}
