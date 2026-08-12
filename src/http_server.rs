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
    routing::{get, post},
    Extension, RequestExt,
};
use prometheus_client::registry::Registry;
use sentry::integrations::tower as sentry_tower;
use serde_json::json;
use sqd_contract_client::PeerId;
use sqd_primitives::BlockRef;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tower_http::cors::{Any, CorsLayer};
use tower_http::decompression::RequestDecompressionLayer;
use tower_http::request_id::{
    MakeRequestUuid, PropagateRequestIdLayer, RequestId, SetRequestIdLayer,
};
use utoipa_scalar::{Scalar, Servable as _};

use crate::auth::{AuthExt, EndpointExt, Gate, Gated};
use crate::datasets::DatasetConfig;
use crate::endpoints::{
    block_number_by_timestamp::get_blocknumber_by_timestamp,
    stream::{
        run_archival_stream, run_archival_stream_restricted, run_finalized_stream, run_stream,
    },
};
use crate::hotblocks::HotblocksErr;
use crate::openapi::{build_openapi_spec, serve_openapi_spec, BlockHead, StatusResponse};
use crate::types::api_types::AvailableDatasetApiResponse;
use crate::types::Compression;
use crate::utils::conversion::json_lines_to_json;
use crate::{
    config::Config,
    controller::task_manager::TaskManager,
    hotblocks::{traceless_key, HeadMode, HotblocksHandle},
    network::{NetworkClient, NoWorker, NotReady},
    types::{
        coded_response, error_body_response, error_response, ChunkId, DatasetId, ErrorBody,
        ErrorCode, ErrorResponse, ParsedQuery, RequestError, StreamRequest, RETRY_AFTER_FLOOR,
    },
    utils::logging,
};

#[cfg(feature = "sql")]
use crate::sql;
#[cfg(feature = "sql")]
use axum::body;

/// Response headers a cross-origin client must be able to read.
///
/// The Fetch spec hands JavaScript only the CORS-safelisted headers unless the server
/// names the rest here — and that list contains none of ours: not `Retry-After`, the hint
/// INV-26 makes mandatory, nor `x-request-id` (REQ-9), nor the stream metadata IB-2 binds
/// to every 200. Those headers were on the wire all along, which is why no server-side
/// test saw the gap: the filtering happens in the browser.
///
/// Spelled out rather than `Any`, so a header added later is invisible until someone
/// decides it is public — `x-internal-*` above all.
const EXPOSED_HEADERS: [axum::http::HeaderName; 6] = [
    header::RETRY_AFTER,
    logging::X_REQUEST_ID,
    axum::http::HeaderName::from_static(crate::endpoints::stream::DATA_SOURCE_HEADER),
    axum::http::HeaderName::from_static(crate::endpoints::stream::HEAD_NUMBER_HEADER),
    axum::http::HeaderName::from_static(crate::endpoints::stream::FINALIZED_NUMBER_HEADER),
    axum::http::HeaderName::from_static(crate::endpoints::stream::FINALIZED_HASH_HEADER),
];

fn cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS, Method::PUT])
        .allow_headers(Any)
        .allow_origin(Any)
        .expose_headers(EXPOSED_HEADERS)
}

#[allow(deprecated)]
pub async fn run_server(
    task_manager: Arc<TaskManager>,
    network_client: Arc<NetworkClient>,
    metrics_registry: Registry,
    addr: SocketAddr,
    config: Arc<Config>,
    hotblocks: Arc<HotblocksHandle>,
    shutting_down: Arc<AtomicBool>,
    shutdown_signal: CancellationToken,
    show_internal_docs: bool,
    auth_gate: Option<Arc<Gate>>,
) -> anyhow::Result<()> {
    let openapi_spec = build_openapi_spec(show_internal_docs);
    let cors = cors_layer();

    tracing::info!("Starting HTTP server listening on {addr}");
    let app = gated_routes(auth_gate.clone(), &openapi_spec)
        .into_router()
        .layer(Extension(Arc::new(openapi_spec)));

    let drain_timeout = config.drain_timeout;

    let app = app
        .route_layer(axum::middleware::from_fn(logging::middleware))
        .layer(sentry_tower::NewSentryLayer::new_from_top())
        .layer(RequestDecompressionLayer::new())
        .layer(
            // Outside the decompression layer and the router, both of which can answer
            // without reaching `logging::middleware`.
            axum::middleware::from_fn(logging::observe_bypassed),
        )
        .layer(cors)
        .layer(
            // Copies the request id onto every response (REQ-9). Must be added
            // before (= sit inside) SetRequestIdLayer to see the id it sets.
            // Layers attached to the empty Router::new() wrap zero routes, so
            // this must live here, after the routes.
            PropagateRequestIdLayer::x_request_id(),
        )
        .layer(
            // This layer is added here to be applied before the request reaches trace layers
            SetRequestIdLayer::x_request_id(MakeRequestUuid),
        )
        .layer(
            // Outside SetRequestIdLayer: a non-ASCII client id is rejected with a 400 and
            // a generated response correlation id before it can enter logs or handlers.
            axum::middleware::from_fn(logging::reject_non_ascii_request_id),
        )
        .layer(Extension(task_manager))
        .layer(Extension(network_client))
        // `None` without an `auth:` block, so `/ready` behaves exactly as
        // it does on an OSS portal.
        .layer(Extension(auth_gate))
        .layer(Extension(config))
        .layer(Extension(Arc::new(metrics_registry)))
        .layer(Extension(hotblocks))
        .layer(Extension(shutting_down));

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
/// See [ADR-005](../spec/decisions/ADR-005-two-phase-shutdown.md) for the two-phase
/// shutdown decision and drain semantics — whatever remains in flight after the
/// timeout is detached, not awaited.
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

/// Latest archival head
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
        (status = 404, description = "Dataset has no archival data source", body = ErrorResponse),
    ),
    tag = "Streaming",
    extensions(("x-internal" = json!(true))),
)]
async fn get_archival_head(
    Extension(network): Extension<Arc<NetworkClient>>,
    dataset: DatasetConfig,
) -> Response {
    // Prefer network data source to correspond to the /archival-stream behaviour
    if let Some(dataset_id) = dataset.network_id {
        return axum::Json(network.head(&dataset_id)).into_response();
    }

    coded_response(
        ErrorCode::UnknownDataset,
        format!(
            "Dataset {} has no archival data source",
            dataset.default_name
        ),
    )
}

/// Whether the head must be reported as the minimum of the traced and traceless variants.
///
/// They finalize independently and a stream may use either, so advertising the higher head
/// could name a block the other variant can't serve yet. Only finalized heads are affected:
/// the real-time head is served from the traced variant a `/stream` would use.
fn reports_min_finalized_head(dataset: &DatasetConfig, mode: HeadMode) -> bool {
    mode == HeadMode::Finalized
        && dataset
            .hotblocks
            .as_ref()
            .is_some_and(|cfg| cfg.dataset_traceless.is_some())
}

/// Returns the head reported by hotblocks, or `None` if there's no such block yet.
///
/// If either variant of a traceless dataset has no finalized block, neither has the dataset
/// as a whole.
async fn real_time_head(
    hotblocks: &HotblocksHandle,
    dataset: &DatasetConfig,
    mode: HeadMode,
) -> Result<Option<BlockRef>, HotblocksErr> {
    if !reports_min_finalized_head(dataset, mode) {
        return hotblocks.get_head(&dataset.default_name, mode).await;
    }

    let traceless_name = traceless_key(&dataset.default_name);
    let (traced, traceless) = tokio::join!(
        hotblocks.get_head(&dataset.default_name, mode),
        hotblocks.get_head(&traceless_name, mode),
    );
    Ok(match (traced?, traceless?) {
        (Some(traced), Some(traceless)) => Some(if traced.number <= traceless.number {
            traced
        } else {
            traceless
        }),
        _ => None,
    })
}

/// Returns the real-time head of the dataset, falling back to the archival head when
/// the hotblocks database has no blocks yet or can't be reached. This mirrors the
/// `x-sqd-head-number` header of the corresponding streaming endpoints
/// (see `stream_from_network` in endpoints/stream.rs).
async fn head_response(
    hotblocks: &HotblocksHandle,
    network: &NetworkClient,
    dataset: &DatasetConfig,
    mode: HeadMode,
) -> Response {
    if dataset.hotblocks.is_some() {
        let Some(dataset_id) = &dataset.network_id else {
            // Without an archival data source there is nothing to fall back to.
            if reports_min_finalized_head(dataset, mode) {
                return match real_time_head(hotblocks, dataset, mode).await {
                    Ok(head) => axum::Json(head).into_response(),
                    Err(e) => forward_hotblocks_response(&dataset.default_name, Err(e)).await,
                };
            }
            // Pass the hotblocks response through unchanged.
            return forward_hotblocks_response(
                &dataset.default_name,
                hotblocks.request_head(&dataset.default_name, mode).await,
            )
            .await;
        };

        match real_time_head(hotblocks, dataset, mode).await {
            Ok(Some(head)) => return axum::Json(head).into_response(),
            Ok(None) => {}
            Err(e) => tracing::warn!(
                "Couldn't get the real-time head of dataset {}: {e}",
                dataset.default_name
            ),
        }

        // Computing the archival head is not free, so it's only done once the real-time
        // head is known to be unavailable. It never exceeds either real-time head, so
        // it's safe to advertise for traceless datasets too.
        return axum::Json(network.head(dataset_id)).into_response();
    }

    if let Some(dataset_id) = &dataset.network_id {
        return axum::Json(network.head(dataset_id)).into_response();
    }

    coded_response(
        ErrorCode::UnknownDataset,
        format!("Dataset {} has no data sources", dataset.default_name),
    )
}

/// Latest finalized head
///
/// Returns the block number and hash of the highest finalized block.
/// If the dataset has both real-time and archival data sources, the real-time finalized head
/// is returned when available, and the archival one otherwise.
/// Matches `/finalized-stream` head behavior.
#[utoipa::path(
    get,
    path = "/datasets/{dataset}/finalized-head",
    params(
        ("dataset" = String, Path, description = "Dataset name"),
    ),
    responses(
        (status = 200, description = "Finalized head block retrieved", body = Option<BlockHead>),
        (status = 404, description = "Dataset has no data sources", body = ErrorResponse),
    ),
    tag = "Streaming"
)]
async fn get_finalized_head(
    Extension(hotblocks): Extension<Arc<HotblocksHandle>>,
    Extension(network): Extension<Arc<NetworkClient>>,
    dataset: DatasetConfig,
) -> Response {
    head_response(&hotblocks, &network, &dataset, HeadMode::Finalized).await
}

/// Latest head
///
/// Returns the block number and hash of the highest block, including real-time data.
/// If the dataset has both real-time and archival data sources, the real-time head is
/// returned when available, and the archival one otherwise.
/// Matches `/stream` head header behavior.
#[utoipa::path(
    get,
    path = "/datasets/{dataset}/head",
    params(
        ("dataset" = String, Path, description = "Dataset name"),
    ),
    responses(
        (status = 200, description = "Head block retrieved", body = Option<BlockHead>),
        (status = 404, description = "Dataset has no data sources", body = ErrorResponse),
    ),
    tag = "Streaming"
)]
async fn get_head(
    Extension(hotblocks): Extension<Arc<HotblocksHandle>>,
    Extension(network): Extension<Arc<NetworkClient>>,
    dataset: DatasetConfig,
) -> Response {
    head_response(&hotblocks, &network, &dataset, HeadMode::RealTime).await
}

/// Portal Status
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
    tag = "Monitoring",
    extensions(("x-internal" = json!(true))),
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

/// Dataset State
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
        (status = 404, description = "Dataset not found", body = ErrorResponse),
    ),
    tag = "Datasets",
    extensions(("x-internal" = json!(true))),
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
        (status = 404, description = "Dataset not found", body = ErrorResponse),
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

/// Block Debug Info
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
        (status = 404, description = "Dataset or block not found", body = ErrorResponse),
    ),
    tag = "Debug",
    extensions(("x-internal" = json!(true))),
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

/// Worker Inventory
///
/// Returns information about all workers currently visible to the portal.
#[utoipa::path(
    get,
    path = "/debug/workers",
    responses(
        (status = 200, description = "All worker information retrieved", body = serde_json::Value),
    ),
    tag = "Debug",
    extensions(("x-internal" = json!(true))),
)]
async fn get_all_workers(
    Extension(client): Extension<Arc<NetworkClient>>,
) -> axum::Json<serde_json::Value> {
    axum::Json(json!({
        "workers": client.get_all_workers(),
    }))
}

/// Prometheus Metrics
///
/// Returns portal metrics in OpenMetrics text format.
#[utoipa::path(
    get,
    path = "/metrics",
    responses(
        (status = 200, description = "Metrics in OpenMetrics format", content_type = "application/openmetrics-text"),
    ),
    tag = "Monitoring",
    extensions(("x-internal" = json!(true))),
)]
async fn get_metrics(
    Extension(registry): Extension<Arc<Registry>>,
    Extension(auth): Extension<Option<Arc<Gate>>>,
) -> impl IntoResponse {
    // Republished per scrape rather than per exchange: the age of the last
    // successful one has to climb through an outage, not freeze at whatever it
    // reached before the control plane went quiet (OB-13).
    if let Some(gate) = &auth {
        gate.publish_freshness();
    }
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

/// Readiness Probe
///
/// Returns 200 once the portal is ready to serve requests; 503 otherwise (e.g. during shutdown).
#[utoipa::path(
    get,
    path = "/ready",
    responses(
        (status = 200, description = "Portal is ready"),
        (status = 503, description = "Portal is not ready", body = ErrorResponse),
    ),
    tag = "Monitoring",
    extensions(("x-internal" = json!(true))),
)]
async fn get_readiness(
    Extension(client): Extension<Arc<NetworkClient>>,
    Extension(shutting_down): Extension<Arc<AtomicBool>>,
) -> impl IntoResponse {
    static LAST_STATE: AtomicU8 = AtomicU8::new(READY);

    let (state, code, body, reason) =
        readiness_verdict(shutting_down.load(Ordering::Relaxed), client.readiness());

    if LAST_STATE.swap(state, Ordering::Relaxed) != state {
        match (state, &reason) {
            (READY, _) => tracing::info!("readiness check now passing: portal is ready"),
            (SHUTTING_DOWN, _) => tracing::info!("readiness check now failing: shutting down"),
            (_, Some(reason)) => tracing::warn!("readiness check now failing: {reason}"),
            (_, None) => {}
        }
    }

    readiness_response(code, &readiness_detail(reason.as_ref(), body))
}

/// What the probe is told, beyond the status: the live reason when there is one, the
/// category otherwise.
///
/// Separate from [`readiness_response`] because this is the half OB-5 cares about — a
/// probe flip must be attributable without log archaeology — and the half a test can
/// reach without building a `NetworkClient`.
fn readiness_detail(reason: Option<&NotReady>, category: &str) -> String {
    match reason {
        Some(reason) => reason.to_string(),
        None => category.to_owned(),
    }
}

/// IB-6: a declining probe answers with the ADR-011 `not_ready` envelope. 200 stays bare
/// text — there is no error to describe, and probes read the status either way.
fn readiness_response(code: StatusCode, detail: &str) -> Response {
    if code == StatusCode::OK {
        return (code, detail.to_owned()).into_response();
    }
    error_response(code, ErrorCode::NotReady, detail)
}

// Stable discriminant per readiness *category*: `/ready` is polled continuously,
// so only a category change logs. Starts `READY` so a portal that never becomes
// ready still logs the reason on its first probe.
const READY: u8 = 0;
const SHUTTING_DOWN: u8 = 1;
const NO_WORKERS: u8 = 2;
const INSUFFICIENT_CONNECTIONS: u8 = 3;

/// Auth configuration adds no conjunct in either enforcement mode: there is
/// nothing to load before serving, and every replica shares one authority, so a rule
/// keyed on the control plane would empty the fleet during the outage that triggered
/// it — and a cold replica would wait for traffic it is not being sent (INV-31).
/// Every route the portal serves, each stating whether it needs a key — including
/// the routers mounted whole, so the inventory the surface test reads back covers
/// everything `run_server` serves (REQ-51).
#[allow(deprecated)]
fn gated_routes(auth_gate: Option<Arc<Gate>>, openapi_spec: &utoipa::openapi::OpenApi) -> Gated {
    let routes = Gated::new(auth_gate)
        // Portal status
        .route("/status", get(get_status).endpoint("/status").no_auth())
        .route(
            "/datasets",
            get(get_datasets).endpoint("/datasets").no_auth(),
        )
        // Streaming data
        .route(
            "/datasets/:dataset/archival-stream",
            post(run_archival_stream_restricted)
                .endpoint("/archival-stream")
                .auth(),
        )
        .route(
            "/datasets/:dataset/archival-stream/debug",
            post(run_archival_stream)
                .endpoint("/archival-stream/debug")
                .auth(),
        )
        .route(
            "/datasets/:dataset/finalized-stream",
            post(run_finalized_stream)
                .endpoint("/finalized-stream")
                .auth(),
        )
        .route(
            "/datasets/:dataset/stream",
            post(run_stream).endpoint("/stream").auth(),
        )
        // Getting head
        .route(
            "/datasets/:dataset/archival-head",
            get(get_archival_head).endpoint("/archival-head").no_auth(),
        )
        .route(
            "/datasets/:dataset/finalized-head",
            get(get_finalized_head)
                .endpoint("/finalized-head")
                .no_auth(),
        )
        .route(
            "/datasets/:dataset/head",
            get(get_head).endpoint("/head").no_auth(),
        )
        // Dataset info
        .route(
            "/datasets/:dataset/state",
            get(get_dataset_state).endpoint("/state").no_auth(),
        )
        .route(
            "/datasets/:dataset",
            get(get_dataset_metadata).endpoint("/dataset").no_auth(),
        )
        .route(
            "/datasets/:dataset/metadata",
            get(get_dataset_metadata).endpoint("/metadata").no_auth(),
        )
        .route(
            "/datasets/:dataset/timestamps/:timestamp/block",
            get(get_blocknumber_by_timestamp)
                .endpoint("/timestamps/block")
                .auth(),
        )
        // Backward compatibility routes
        .route(
            "/datasets/:dataset/finalized-stream/height",
            get(get_finalized_stream_height)
                .endpoint("/height")
                .no_auth(),
        )
        .route(
            "/datasets/:dataset/archival-stream/height",
            get(get_archival_stream_height)
                .endpoint("/height")
                .no_auth(),
        )
        .route(
            "/datasets/:dataset_id/query/:worker_id",
            post(execute_query).endpoint("/query").auth(),
        )
        .route(
            "/datasets/:dataset/height",
            get(get_height).endpoint("/height").no_auth(),
        )
        .route(
            "/datasets/:dataset/:start_block/worker",
            get(get_worker).endpoint("/worker").no_auth(),
        )
        // Internal routes
        .route(
            "/debug/workers",
            get(get_all_workers).endpoint("/debug/workers").no_auth(),
        )
        .route(
            "/datasets/:dataset/:block/debug",
            get(get_debug_block).endpoint("/block/debug").no_auth(),
        )
        // Ops probes and the served schema: never gated, or a pod that cannot
        // answer its own readiness check leaves rotation.
        .route("/metrics", get(get_metrics).no_auth())
        .route("/ready", get(get_readiness).no_auth())
        .route("/api-docs/openapi.json", get(serve_openapi_spec).no_auth());

    // SQL Query Engine
    //
    // Gated, and therefore measured where usage measurement is on — but what it
    // returns is a worker/chunk *plan*, not result data, so its usage records
    // are plan bytes and are excluded from data-volume analysis at read time by
    // their `/sql/query` endpoint label (ADR-016). Scanned bytes, if they ever
    // matter, are separate work with a separate measurement.
    #[cfg(feature = "sql")]
    let routes = routes
        .route("/sql/query", post(sql_query).endpoint("/sql/query").auth())
        .route(
            "/sql/metadata",
            get(sql_metadata).endpoint("/sql/metadata").no_auth(),
        );

    routes.merge_ungated(
        "the Scalar docs UI renders the same schema on every deployment",
        Scalar::with_url("/docs", openapi_spec.clone())
            .custom_html(include_str!("../docs/openapi/scalar_template.html"))
            .into(),
    )
}

fn readiness_verdict(
    shutting_down: bool,
    network: Result<(), NotReady>,
) -> (u8, StatusCode, &'static str, Option<NotReady>) {
    if shutting_down {
        return (
            SHUTTING_DOWN,
            StatusCode::SERVICE_UNAVAILABLE,
            "Shutting down",
            None,
        );
    }
    if let Err(reason) = network {
        let state = match reason {
            NotReady::NoWorkers => NO_WORKERS,
            NotReady::InsufficientConnections { .. } => INSUFFICIENT_CONNECTIONS,
        };
        return (
            state,
            StatusCode::SERVICE_UNAVAILABLE,
            "Not ready",
            Some(reason),
        );
    }
    (READY, StatusCode::OK, "Ready", None)
}

/// Dataset Height
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
        (status = 404, description = "Dataset not found", body = ErrorResponse),
    ),
    tag = "Streaming",
    extensions(("x-internal" = json!(true))),
)]
#[deprecated]
async fn get_height(
    Extension(network): Extension<Arc<NetworkClient>>,
    Path(dataset): Path<String>,
    dataset_id: DatasetId,
) -> impl IntoResponse {
    height_response(&network, &dataset, &dataset_id)
}

/// Finalized Stream Height
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
        (status = 404, description = "Dataset not found", body = ErrorResponse),
    ),
    tag = "Streaming",
    extensions(("x-internal" = json!(true))),
)]
#[deprecated]
async fn get_finalized_stream_height(
    Extension(network): Extension<Arc<NetworkClient>>,
    Path(dataset): Path<String>,
    dataset_id: DatasetId,
) -> impl IntoResponse {
    height_response(&network, &dataset, &dataset_id)
}

/// Archival Stream Height
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
        (status = 404, description = "Dataset not found", body = ErrorResponse),
    ),
    tag = "Streaming",
    extensions(("x-internal" = json!(true))),
)]
#[deprecated]
async fn get_archival_stream_height(
    Extension(network): Extension<Arc<NetworkClient>>,
    Path(dataset): Path<String>,
    dataset_id: DatasetId,
) -> impl IntoResponse {
    height_response(&network, &dataset, &dataset_id)
}

fn height_response(network: &NetworkClient, dataset: &str, dataset_id: &DatasetId) -> Response {
    match network.get_height(dataset_id) {
        // Bare number, not JSON: the deprecated height endpoints' response contract.
        Some(height) => (StatusCode::OK, height.to_string()).into_response(),
        None => coded_response(
            ErrorCode::UnknownDataset,
            format!("No data for dataset {dataset}"),
        ),
    }
}

/// Worker Info
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
        (status = 404, description = "Dataset not found", body = ErrorResponse),
        (status = 429, description = "Rate limit exceeded", body = ErrorResponse),
        (status = 503, description = "No available workers", body = ErrorResponse),
    ),
    tag = "Debug",
    extensions(("x-internal" = json!(true))),
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
            return coded_response(
                ErrorCode::NoWorkers,
                format!("No available worker for dataset {dataset} block {start_block}"),
            );
        }
        Err(NoWorker::Backoff(retry_at)) => {
            let seconds = retry_at.duration_since(Instant::now()).as_secs() + 1; // +1 for rounding up
            let mut response = error_response(
                StatusCode::TOO_MANY_REQUESTS,
                ErrorCode::Overloaded,
                "Too many requests",
            );
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, seconds.into());
            return response;
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

/// Worker Query
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
        (status = 400, description = "Invalid query", body = ErrorResponse),
        (status = 404, description = "Dataset or worker not found", body = ErrorResponse),
        (status = 503, description = "Service unavailable", body = ErrorResponse),
    ),
    tag = "Streaming",
    extensions(("x-internal" = json!(true))),
)]
#[deprecated]
async fn execute_query(
    Path((dataset_id_encoded, worker_id)): Path<(String, PeerId)>,
    Extension(client): Extension<Arc<NetworkClient>>,
    Extension(req): Extension<RequestId>,
    query: ParsedQuery, // request body
) -> Response {
    let dataset_id = match DatasetId::from_base64(&dataset_id_encoded) {
        Ok(dataset_id) => dataset_id,
        Err(e) => {
            return coded_response(
                ErrorCode::UnknownDataset,
                format!("Couldn't parse dataset id: {e}"),
            )
        }
    };

    let request_id = req.header_value().to_str().unwrap_or("").to_string();

    let Ok(chunk) = client.find_chunk(&dataset_id, query.first_block()) else {
        return RequestError::NoData.into_response();
    };
    let range = query
        .intersect_with(&chunk.block_range())
        .expect("Found chunk should intersect with query");

    let lease = match client.reserve_worker(worker_id) {
        Some(lease) => lease,
        None => {
            return RequestError::BadRequest(format!("Worker {} does not exist", worker_id))
                .into_response()
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
        Err(err) => return RequestError::from_query_error(err, worker_id).into_response(),
    };
    match json_lines_to_json(&result.data) {
        Ok(data) => Response::builder()
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_ENCODING, "gzip")
            .body(Body::from(data))
            .unwrap(),
        Err(e) => RequestError::Internal(format!("Couldn't convert response: {e}")).into_response(),
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
            .ok_or_else(|| coded_response(ErrorCode::UnknownDataset, "not enough arguments"))?;
        let Extension(network) = parts
            .extract::<Extension<Arc<NetworkClient>>>()
            .await
            .map_err(IntoResponse::into_response)?;

        match network.dataset(alias) {
            Some(config) => Ok(config.clone()),
            None => Err(coded_response(
                ErrorCode::UnknownDataset,
                format!("Unknown dataset: {alias}"),
            )),
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
            // The outer validator rejects a non-ASCII id before routing. Keep this
            // defensive fallback for stacks that omit the production layer.
            .unwrap_or_default()
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
                return Err(RequestError::InvalidParam {
                    param: "buffer_size",
                    message: "buffer_size must be greater than 0".to_string(),
                }
                .into_response())
            }
            Some(Ok(value)) => value,
            Some(Err(e)) => {
                return Err(RequestError::InvalidParam {
                    param: "buffer_size",
                    message: format!("Couldn't parse buffer_size: {e}"),
                }
                .into_response())
            }
            None => config.default_buffer_size,
        };
        let timeout_quantile = match params.get("timeout_quantile") {
            Some(value) => match value.parse() {
                Ok(quantile) => quantile,
                Err(e) => {
                    return Err(RequestError::InvalidParam {
                        param: "timeout_quantile",
                        message: format!("Couldn't parse timeout_quantile: {e}"),
                    }
                    .into_response())
                }
            },
            None => config.default_timeout_quantile,
        };
        let retries = match params.get("retries") {
            Some(value) => match value.parse() {
                Ok(value) => value,
                Err(e) => {
                    return Err(RequestError::InvalidParam {
                        param: "retries",
                        message: format!("Couldn't parse retries: {e}"),
                    }
                    .into_response())
                }
            },
            None => config.default_retries,
        };
        let max_chunks = match params.get("max_chunks") {
            Some(value) => match value.parse() {
                Ok(0) => {
                    return Err(RequestError::InvalidParam {
                        param: "max_chunks",
                        message: "max_chunks must be greater than 0".to_string(),
                    }
                    .into_response())
                }
                Ok(value) => Some(value),
                Err(e) => {
                    return Err(RequestError::InvalidParam {
                        param: "max_chunks",
                        message: format!("Couldn't parse max_chunks: {e}"),
                    }
                    .into_response())
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
            None => Err(coded_response(
                ErrorCode::UnknownDataset,
                format!(
                    "Dataset {} doesn't have archival data",
                    dataset.default_name
                ),
            )),
        }
    }
}

pub(crate) async fn forward_hotblocks_response(
    dataset: &str,
    response: Result<reqwest::Response, HotblocksErr>,
) -> Response {
    match response {
        Ok(response) => forward_response(dataset, response).await,
        // Unreachable by construction; a panic here would kill the connection task.
        Err(HotblocksErr::UnknownDataset) => {
            RequestError::Internal("dataset should be known by the hotblocks service".to_owned())
                .into_response()
        }
        Err(HotblocksErr::Request(e)) => {
            // Until this fires, a stalled upstream leaves no trace: the request never
            // returns, so it carries no status. reqwest's Display already names the URL —
            // which is why it stays in the log and out of the body. DC-4 keeps an
            // upstream's own prose unpublished; a transport error names the same internal
            // topology in the same way, and the client can do nothing with either.
            tracing::warn!(
                dataset,
                timed_out = e.is_timeout(),
                error = %e,
                "hotblocks request failed"
            );
            coded_response(
                ErrorCode::UpstreamUnavailable,
                ErrorCode::UpstreamUnavailable.default_message(),
            )
        }
    }
}

/// Marks a header as upstream diagnostics for the portal alone. Matched by prefix so the
/// guarantee holds for headers added later: these name internal topology and must never
/// reach a client.
const INTERNAL_HEADER_PREFIX: &str = "x-internal-";

/// Set by hotblocks to name the replica that answered. The Service load-balances behind a
/// single ClusterIP, so this is the portal's only way to attribute a response to a pod.
const HOTBLOCKS_INSTANCE_HEADER: &str = "x-internal-hotblocks-instance";

/// Proxy a hotblocks response, rewriting error bodies into the portal's envelope: one
/// stream endpoint is served by either data source and must not emit two body shapes.
pub(crate) async fn forward_response(
    dataset: &str,
    mut response: reqwest::Response,
) -> axum::response::Response {
    let status = response.status();
    let instance = || {
        response
            .headers()
            .get(HOTBLOCKS_INSTANCE_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("-")
    };
    if status.is_server_error() {
        tracing::warn!(
            dataset,
            status = status.as_u16(),
            instance = instance(),
            "hotblocks returned a server error"
        );
    }
    // The portal validated this alias against its own catalog before asking, so a 404
    // here is the two deployments disagreeing about what exists, not a client typo — and
    // on the wire and on the metric the two are identical (DC-4, spec/05). Since the
    // upstream's own prose is not published, this log is the only witness.
    if status == StatusCode::NOT_FOUND {
        tracing::error!(
            dataset,
            instance = instance(),
            "hotblocks does not know a dataset the portal advertises: catalog incoherence"
        );
    }

    // 204 and success stream through untouched, keeping x-sqd-finalized-head-*.
    if status == StatusCode::NO_CONTENT || status.is_success() || status.is_redirection() {
        return stream_response(response);
    }

    let (public_status, class) = ErrorCode::classify_upstream(status);
    let headers = response.headers().clone();

    let mut error = ErrorBody::new(class, class.default_message());

    // 409 is the only status whose body is read at all. The upstream's prose is never
    // published (DC-4) and adds nothing to the log — a server error is already recorded
    // above with the pod that served it, and hotblocks logs its own errors in full — so
    // every other status streams straight to drop.
    if status == StatusCode::CONFLICT {
        // Clients walk `previousBlocks` to find a shared ancestor, so it is preserved at
        // the top level beside `error` (IB-5). It is the one public upstream field.
        match conflict_previous_blocks(&mut response).await {
            Some(blocks) => {
                error = error.with_sibling("previousBlocks", serde_json::json!(blocks));
            }
            // Not a legal 409 under IB-5, and unrecoverable for the client: it has
            // nothing to walk, so it re-requests the same range and conflicts again.
            // Indistinguishable from a healthy reorg on the wire and on the metric, so
            // this log is the only witness.
            None => tracing::error!(
                dataset,
                "hotblocks 409 carried no usable previousBlocks: clients cannot resolve the reorg"
            ),
        }
    }

    let mut rewritten = error_body_response(public_status, error);

    // Keep upstream headers (retry-after, x-sqd-*), but not the replaced body's own, and
    // never the internal ones — this path bypasses stream_response's filter.
    let out = rewritten.headers_mut();
    for (key, value) in headers.iter() {
        if key == header::CONTENT_TYPE
            || key == header::CONTENT_LENGTH
            || key == header::CONTENT_ENCODING
            || key == header::TRANSFER_ENCODING
            || key.as_str().starts_with(INTERNAL_HEADER_PREFIX)
        {
            continue;
        }
        out.insert(key, value.clone());
    }

    // INV-26: OVERLOADED always carries a *usable* hint. An upstream value survives only
    // if it reads as seconds at or above the floor — a 0 would invite an immediate retry
    // loop against a source that just shed load, and the portal documents this header in
    // seconds, so the RFC's HTTP-date form is replaced rather than passed on.
    if class == ErrorCode::Overloaded {
        let usable = out
            .get(header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<u64>().ok())
            .is_some_and(|seconds| seconds >= RETRY_AFTER_FLOOR);
        if !usable {
            out.insert(header::RETRY_AFTER, RETRY_AFTER_FLOOR.into());
        }
    }

    rewritten
}

/// A 409's recovery list is a short `{number, hash}` slice by contract, so this bounds a
/// broken upstream rather than budgeting a legitimate one: a body past the cap is not the
/// contract, and is refused instead of truncated.
const MAX_CONFLICT_BODY: usize = 64 * 1024;

/// The 409 recovery list, or `None` if the upstream did not supply a usable one.
///
/// Typed and non-empty rather than forwarded as an opaque value: `previousBlocks` is the
/// one upstream field clients are told to walk, so an upstream must not be able to put
/// something else — a scalar, an empty list, arbitrary internal data — under that name.
/// Read here and nowhere else, bounded, because this is the only upstream body the portal
/// consumes at all.
async fn conflict_previous_blocks(response: &mut reqwest::Response) -> Option<Vec<BlockRef>> {
    #[derive(serde::Deserialize)]
    struct Conflict {
        #[serde(rename = "previousBlocks")]
        previous_blocks: Vec<BlockRef>,
    }

    let mut body = bytes::BytesMut::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) if body.len() + chunk.len() <= MAX_CONFLICT_BODY => {
                body.extend_from_slice(&chunk)
            }
            Ok(Some(_)) => return None,
            Ok(None) => break,
            // A truncated prefix cannot be parsed, and half a chain slice is worse than
            // none: the client would resume from an ancestor that is not the deepest one.
            Err(_) => return None,
        }
    }

    let conflict: Conflict = serde_json::from_slice(&body).ok()?;
    (!conflict.previous_blocks.is_empty()).then_some(conflict.previous_blocks)
}

fn stream_response(response: reqwest::Response) -> axum::response::Response {
    let mut builder = Response::builder().status(response.status());
    for (key, value) in response.headers() {
        if key.as_str().starts_with(INTERNAL_HEADER_PREFIX) {
            continue;
        }
        builder = builder.header(key, value);
    }
    builder
        .body(Body::from_stream(response.bytes_stream()))
        .unwrap()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::server_overloaded;
    use std::time::Duration;

    /// A server-side test can only see that a header is *on* the response — whether a
    /// browser hands it to JavaScript is decided by `access-control-expose-headers`, and
    /// that is why no assertion on a handler could ever have caught its absence. The
    /// names are written out here rather than read from [`EXPOSED_HEADERS`]: a test that
    /// iterates the list it is checking restates the implementation instead of pinning
    /// the contract, and dropping a name would quietly move both.
    #[tokio::test]
    async fn cors_exposes_every_header_a_client_is_told_to_read() {
        use axum::{body::Body, http::Request, routing::get, Router};
        use tower::ServiceExt;

        let app = Router::new()
            .route(
                "/refused",
                get(|| async { RequestError::BusyFor(Duration::from_secs(9)).into_response() }),
            )
            .layer(cors_layer());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/refused")
                    .header(header::ORIGIN, "https://app.example.com")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), server_overloaded());
        assert_eq!(
            response.headers()[header::RETRY_AFTER],
            "10",
            "the hint must be on the wire before exposing it can matter"
        );

        let exposed = response
            .headers()
            .get(header::ACCESS_CONTROL_EXPOSE_HEADERS)
            .expect("a cross-origin response must name what it exposes")
            .to_str()
            .unwrap()
            .to_ascii_lowercase();
        let exposed: Vec<&str> = exposed.split(',').map(str::trim).collect();

        for name in [
            // INV-26 owes an overload a back-off interval; unreadable, it owes nothing.
            "retry-after",
            // REQ-9: the id a user quotes in a ticket.
            "x-request-id",
            // IB-2's stream metadata, which the harness asserts on every 200.
            "x-sqd-data-source",
            "x-sqd-head-number",
            "x-sqd-finalized-head-number",
            "x-sqd-finalized-head-hash",
        ] {
            assert!(
                exposed.contains(&name),
                "{name} is unreadable cross-origin: {exposed:?}"
            );
        }
        assert!(
            !exposed
                .iter()
                .any(|h| *h == "*" || h.starts_with(INTERNAL_HEADER_PREFIX)),
            "the exposed set must stay explicit and free of internal headers: {exposed:?}"
        );
    }

    /// GAP-29: a refusal carrying no `ErrorCode` reads as an unmatched client
    /// error to the normalizing layer and is rewritten to 400. Only the gate and
    /// that layer stacked together show it, so the test sits here.
    #[tokio::test]
    async fn an_auth_refusal_survives_the_middleware_that_normalizes_client_errors() {
        use tower::ServiceExt;
        use tower_http::request_id::{MakeRequestUuid, SetRequestIdLayer};

        use crate::auth::test_support::gate_with;

        let gate = Some(gate_with(crate::auth::Enforcement::Enforce));
        let app = Gated::new(gate)
            .route(
                "/datasets/:dataset/stream",
                post(|| async { "served" }).endpoint("/stream").auth(),
            )
            .into_router()
            .route_layer(axum::middleware::from_fn(logging::middleware))
            .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/datasets/base/stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "the refusal must reach the wire as a 403, not a normalized 400"
        );

        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["type"], "authentication_error");
        assert_eq!(body["error"]["code"], "missing_credential");
    }

    /// The kill switch: with no `auth:` block the gate is never
    /// installed, so an OSS build carries no layer at all.
    #[tokio::test]
    async fn no_route_carries_authorization_without_a_an_auth_config() {
        use tower::ServiceExt;

        let app = Gated::new(None)
            .route(
                "/datasets/:dataset/stream",
                post(|| async { "served" }).auth(),
            )
            .route("/datasets", get(|| async { "served" }).no_auth())
            .into_router();

        for (method, uri) in [
            ("POST", "/datasets/ethereum-mainnet/stream"),
            ("GET", "/datasets"),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(uri)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::OK, "{uri}");
        }
    }

    /// A cold replica is ready before it has ever reached the control plane. The
    /// rule that used to hold it back deadlocked: an exchange happens only on a
    /// client request, and readiness is what decides whether any arrive (INV-31).
    #[test]
    fn readiness_does_not_depend_on_the_control_plane() {
        assert_eq!(readiness_verdict(false, Ok(())).1, StatusCode::OK);
        assert_eq!(
            readiness_verdict(true, Ok(())).1,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            readiness_verdict(false, Err(NotReady::NoWorkers)),
            (
                NO_WORKERS,
                StatusCode::SERVICE_UNAVAILABLE,
                "Not ready",
                Some(NotReady::NoWorkers)
            )
        );
        // Shutdown still outranks everything.
        assert_eq!(readiness_verdict(true, Ok(())).0, SHUTTING_DOWN);
    }

    /// REQ-51: the whole served surface, and which half of it needs a key.
    ///
    /// `Gated::route` already makes an unclassified route a compile error, but it
    /// cannot see inside a merged router and nothing stops a route being added
    /// after `into_router`. This reads the classification back — merged routers
    /// included — so a new data route reaches review as a diff here rather than
    /// as an open endpoint. `run_server` mounts nothing `gated_routes` does not.
    #[test]
    fn every_mounted_route_declares_whether_it_needs_a_key() {
        use crate::auth::Mounted::{self, Gated as G, Merged as M, Open as O};

        let routes = gated_routes(None, &build_openapi_spec(false));
        #[allow(unused_mut)]
        let mut expected: Vec<Mounted> = vec![
            O("/status"),
            O("/datasets"),
            G("/datasets/:dataset/archival-stream"),
            G("/datasets/:dataset/archival-stream/debug"),
            G("/datasets/:dataset/finalized-stream"),
            G("/datasets/:dataset/stream"),
            O("/datasets/:dataset/archival-head"),
            O("/datasets/:dataset/finalized-head"),
            O("/datasets/:dataset/head"),
            O("/datasets/:dataset/state"),
            O("/datasets/:dataset"),
            O("/datasets/:dataset/metadata"),
            G("/datasets/:dataset/timestamps/:timestamp/block"),
            O("/datasets/:dataset/finalized-stream/height"),
            O("/datasets/:dataset/archival-stream/height"),
            G("/datasets/:dataset_id/query/:worker_id"),
            O("/datasets/:dataset/height"),
            O("/datasets/:dataset/:start_block/worker"),
            O("/debug/workers"),
            O("/datasets/:dataset/:block/debug"),
            O("/metrics"),
            O("/ready"),
            O("/api-docs/openapi.json"),
        ];
        #[cfg(feature = "sql")]
        expected.extend([G("/sql/query"), O("/sql/metadata")]);
        expected.push(M(
            "the Scalar docs UI renders the same schema on every deployment",
        ));

        assert_eq!(routes.inventory(), expected.as_slice());
    }

    #[tokio::test]
    async fn forward_response_strips_internal_headers() {
        let upstream = axum::http::Response::builder()
            .status(StatusCode::OK)
            .header(HOTBLOCKS_INSTANCE_HEADER, "hotblocks-db-0")
            .header("x-sqd-finalized-head-number", "42")
            .body(Vec::new())
            .unwrap();

        let forwarded =
            forward_response("polygon-mainnet", reqwest::Response::from(upstream)).await;

        let headers = forwarded.headers();
        assert!(
            !headers
                .keys()
                .any(|k| k.as_str().starts_with(INTERNAL_HEADER_PREFIX)),
            "internal headers must not reach clients: {headers:?}"
        );
        assert_eq!(
            headers.get("x-sqd-finalized-head-number").unwrap(),
            "42",
            "client-facing headers must still be forwarded"
        );
    }

    /// Error responses are rebuilt into the envelope rather than streamed, so they copy
    /// upstream headers on a separate path that has to strip the internal ones too.
    #[tokio::test]
    async fn forward_response_strips_internal_headers_on_errors() {
        let upstream = axum::http::Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header(HOTBLOCKS_INSTANCE_HEADER, "hotblocks-db-0")
            .header(header::RETRY_AFTER, "5")
            .body(Vec::from("upstream is down"))
            .unwrap();

        let forwarded =
            forward_response("polygon-mainnet", reqwest::Response::from(upstream)).await;

        let headers = forwarded.headers();
        assert!(
            !headers
                .keys()
                .any(|k| k.as_str().starts_with(INTERNAL_HEADER_PREFIX)),
            "internal headers must not reach clients: {headers:?}"
        );
        assert_eq!(
            headers.get(header::RETRY_AFTER).unwrap(),
            "5",
            "upstream retry-after must survive the body rewrite"
        );
    }

    // ---------------------------------------------------------------------------
    // CT-5 — interface conformance for the error surface (IB-5, INV-26).
    //
    // The taxonomy has two emitters: locally produced errors and rewritten upstream
    // ones. The invariant that matters is that a client cannot tell them apart, so
    // these assert the *proxied* shape and pin it against the local one. Unit tests
    // per emitter cannot catch a divergence between them.
    // ---------------------------------------------------------------------------

    async fn proxied(status: StatusCode, body: &'static str) -> (StatusCode, serde_json::Value) {
        let upstream = axum::http::Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Vec::from(body))
            .unwrap();
        let response = forward_response("polygon-mainnet", reqwest::Response::from(upstream)).await;
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).expect("proxied error bodies are the JSON envelope")
        };
        (status, json)
    }

    /// Every proxied failure arrives as the envelope on the public status DC-4 fixes,
    /// classified onto the closed vocabulary. A matched status is preserved; an
    /// unmatched 4xx normalizes to 400.
    #[tokio::test]
    async fn proxied_errors_use_the_envelope() {
        let cases = [
            (400, 400, "malformed_request", "invalid_request_error"),
            (403, 400, "malformed_request", "invalid_request_error"),
            (418, 400, "malformed_request", "invalid_request_error"),
            (404, 404, "unknown_dataset", "invalid_request_error"),
            (429, 429, "overloaded", "rate_limit_error"),
            (529, 529, "overloaded", "rate_limit_error"),
            (500, 500, "upstream_unavailable", "availability_error"),
            (502, 502, "upstream_unavailable", "availability_error"),
            // ADR-007: 503 is unavailability, not congestion. Read as an overload it
            // claimed exhausted capacity of a source that may have none running at all.
            (503, 503, "upstream_unavailable", "availability_error"),
        ];

        for (upstream, want_status, want_code, want_type) in cases {
            let upstream = StatusCode::from_u16(upstream).unwrap();
            let (got_status, body) =
                proxied(upstream, "instance hotblocks-db-0: /var/lib oops").await;

            assert_eq!(got_status.as_u16(), want_status, "upstream {upstream}");
            assert_eq!(body["error"]["code"], want_code, "upstream {upstream}");
            assert_eq!(body["error"]["type"], want_type, "upstream {upstream}");
        }
    }

    /// DC-4: the upstream's prose is not public API and can name instances and paths.
    /// No proxied error may echo it.
    /// The other half of DC-4. `a_proxied_error_never_leaks_the_upstream_body` only ever
    /// builds an `Ok(reqwest::Response)`, so the branch where the request never completed
    /// went uncovered — and that is the one whose `reqwest::Error` Display carries the
    /// upstream URL, naming the internal service, namespace and port to any client that
    /// can make hotblocks time out.
    #[tokio::test]
    async fn a_transport_failure_does_not_publish_the_upstream_url() {
        // Port 1 is privileged and unbound, so this refuses immediately without DNS.
        let error = reqwest::Client::new()
            .get("http://127.0.0.1:1/datasets/internal-only-name/stream")
            .send()
            .await
            .expect_err("nothing listens on port 1");
        assert!(
            error.to_string().contains("127.0.0.1:1"),
            "reqwest names the url, which is the whole hazard: {error}"
        );

        let response =
            forward_hotblocks_response("eth-mainnet", Err(HotblocksErr::Request(error))).await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);

        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["code"], "upstream_unavailable");
        assert_eq!(body["error"]["type"], "availability_error");

        let message = body["error"]["message"].as_str().unwrap();
        for leak in ["127.0.0.1", "internal-only-name", "http://", ":1/"] {
            assert!(
                !message.contains(leak),
                "the upstream url must stay in the log: {message}"
            );
        }
    }

    #[tokio::test]
    async fn a_proxied_error_never_leaks_the_upstream_body() {
        let secret = "instance hotblocks-db-0 at /var/lib/hotblocks: shard 7 corrupt";
        for upstream in [400u16, 403, 404, 409, 429, 500, 502, 503] {
            let status = StatusCode::from_u16(upstream).unwrap();
            let (_, body) = proxied(status, secret).await;
            let rendered = body.to_string();
            assert!(
                !rendered.contains("hotblocks-db-0") && !rendered.contains("/var/lib"),
                "upstream {upstream} leaked its body: {rendered}"
            );
            assert!(
                body["error"]["message"]
                    .as_str()
                    .is_some_and(|m| !m.is_empty()),
                "upstream {upstream} must still explain itself"
            );
        }
    }

    /// The bug this class of test exists to catch: 409 is the one status where the
    /// envelope has a top-level sibling, and it is served by both data sources.
    #[tokio::test]
    async fn both_stream_paths_emit_the_same_409_shape() {
        let (proxied_status, proxied_body) = proxied(
            StatusCode::CONFLICT,
            r#"{"previousBlocks":[{"number":42,"hash":"0xdead"}]}"#,
        )
        .await;

        let local = RequestError::BaseBlockMismatch(BlockRef {
            number: 42,
            hash: "0xdead".to_owned(),
        })
        .into_response();
        let local_status = local.status();
        let local_body: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(local.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();

        assert_eq!(proxied_status, local_status);
        for (source, body) in [("proxied", &proxied_body), ("local", &local_body)] {
            assert_eq!(body["error"]["code"], "base_block_mismatch", "{source}");
            assert_eq!(body["error"]["type"], "invalid_request_error", "{source}");
            assert!(
                body["error"]["message"].is_string(),
                "{source} must explain itself"
            );
            // IB-5: the recovery contract stays a top-level sibling, not nested.
            assert_eq!(body["previousBlocks"][0]["number"], 42, "{source}");
            assert_eq!(body["previousBlocks"][0]["hash"], "0xdead", "{source}");
        }
    }

    /// INV-26: an overload refusal must always tell the client how long to wait, even
    /// when the upstream forgot to. Nothing else invents one — a hint on a 503 pointed
    /// the client straight back at a dependency that may be down rather than busy.
    #[tokio::test]
    async fn only_a_proxied_overload_carries_an_invented_retry_hint() {
        for (status, wants_hint) in [(429u16, true), (529, true), (503, false), (500, false)] {
            let upstream = axum::http::Response::builder()
                .status(status)
                .body(Vec::from("busy"))
                .unwrap();
            let forwarded =
                forward_response("polygon-mainnet", reqwest::Response::from(upstream)).await;

            let hint = forwarded.headers().get(header::RETRY_AFTER);
            assert_eq!(hint.is_some(), wants_hint, "{status}");
            if let Some(hint) = hint {
                assert!(
                    hint.to_str().unwrap().parse::<u64>().unwrap() >= RETRY_AFTER_FLOOR,
                    "{status} hint must be at least the floor"
                );
            }
        }
    }

    /// 204 streams through as-is: it is not a failure, so it gets no envelope, no code,
    /// and keeps the head markers clients read off it.
    #[tokio::test]
    async fn proxied_204_is_untouched() {
        let upstream = axum::http::Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header("x-sqd-finalized-head-number", "99")
            .body(Vec::new())
            .unwrap();
        let forwarded =
            forward_response("polygon-mainnet", reqwest::Response::from(upstream)).await;

        assert_eq!(forwarded.status(), StatusCode::NO_CONTENT);
        assert_eq!(forwarded.headers()["x-sqd-finalized-head-number"], "99");
        assert!(
            forwarded.extensions().get::<ErrorCode>().is_none(),
            "a 204 must not be tagged with a taxonomy code"
        );
    }

    /// An upstream error body cannot size our response, however large it is: it is not
    /// copied into the envelope at all.
    #[tokio::test]
    async fn a_huge_upstream_body_does_not_size_the_response() {
        let upstream = axum::http::Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("x".repeat(200 * 1024).into_bytes())
            .unwrap();
        let forwarded =
            forward_response("polygon-mainnet", reqwest::Response::from(upstream)).await;
        let bytes = axum::body::to_bytes(forwarded.into_body(), usize::MAX)
            .await
            .unwrap();

        assert!(
            bytes.len() < 1024,
            "envelope must not carry the upstream body"
        );
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["code"], "upstream_unavailable");
    }

    /// The stronger claim behind the test above: the body is not merely omitted from the
    /// response, it is never read. Nothing consumes it — it is not published (DC-4) and a
    /// server error is already logged with the pod that served it — so buffering it would
    /// let an upstream fault size portal memory. 409 is the sole exception.
    #[tokio::test]
    async fn only_a_409_reads_the_upstream_body() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        async fn forward_watching_the_body(status: StatusCode, payload: &'static str) -> bool {
            let polled = Arc::new(AtomicBool::new(false));
            let flag = polled.clone();
            let body = reqwest::Body::wrap_stream(futures::stream::once(async move {
                flag.store(true, Ordering::SeqCst);
                Ok::<_, std::io::Error>(bytes::Bytes::from_static(payload.as_bytes()))
            }));
            let upstream = axum::http::Response::builder()
                .status(status)
                .body(body)
                .unwrap();

            forward_response("polygon-mainnet", reqwest::Response::from(upstream)).await;
            polled.load(Ordering::SeqCst)
        }

        for status in [400u16, 404, 429, 500, 502, 503] {
            assert!(
                !forward_watching_the_body(StatusCode::from_u16(status).unwrap(), "prose").await,
                "{status} must not read the upstream body"
            );
        }

        assert!(
            forward_watching_the_body(StatusCode::CONFLICT, r#"{"previousBlocks":[]}"#).await,
            "409 must read the body — previousBlocks lives in it"
        );
    }

    /// `previousBlocks` is the one upstream field clients are told to walk, so it is
    /// parsed into a non-empty typed list rather than forwarded as whatever value happens
    /// to sit under that key. Anything else — absent, empty, a scalar, arbitrary internal
    /// data, or a body too large to be the contract — is refused rather than passed on as
    /// a recovery hint the client cannot use.
    #[tokio::test]
    async fn a_409_publishes_only_a_usable_recovery_list() {
        let unusable = [
            ("not json at all", "non-JSON body"),
            (r#"{"code":"conflict"}"#, "key absent"),
            (r#"{"previousBlocks":[]}"#, "empty list"),
            (r#"{"previousBlocks":"0xdead"}"#, "scalar"),
            (
                r#"{"previousBlocks":[{"secret":"/var/lib"}]}"#,
                "wrong shape",
            ),
        ];

        for (upstream_body, case) in unusable {
            let (status, body) = proxied(StatusCode::CONFLICT, upstream_body).await;

            assert_eq!(status, StatusCode::CONFLICT, "{case}");
            assert_eq!(body["error"]["code"], "base_block_mismatch", "{case}");
            assert!(
                body.get("previousBlocks").is_none(),
                "{case}: no recovery hint can be invented, and none may be echoed: {body}"
            );
        }

        let (_, body) = proxied(
            StatusCode::CONFLICT,
            r#"{"previousBlocks":[{"number":42,"hash":"0xdead"}],"internal":"/var/lib"}"#,
        )
        .await;
        assert_eq!(body["previousBlocks"][0]["number"], 42);
        assert!(
            body.get("internal").is_none(),
            "only the recovery list survives: {body}"
        );
    }

    /// A body past the cap is not the short `{number, hash}` slice IB-5 describes, so it
    /// is refused rather than truncated — a partial chain slice would resume the client
    /// from an ancestor that is not the deepest one.
    #[tokio::test]
    async fn an_oversized_409_body_is_refused_not_truncated() {
        let filler = "x".repeat(MAX_CONFLICT_BODY);
        let upstream = axum::http::Response::builder()
            .status(StatusCode::CONFLICT)
            .body(
                format!(
                    r#"{{"previousBlocks":[{{"number":42,"hash":"0xdead"}}],"pad":"{filler}"}}"#
                )
                .into_bytes(),
            )
            .unwrap();
        let forwarded =
            forward_response("polygon-mainnet", reqwest::Response::from(upstream)).await;

        assert_eq!(forwarded.status(), StatusCode::CONFLICT);
        let bytes = axum::body::to_bytes(forwarded.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(body.get("previousBlocks").is_none(), "{body}");
    }

    /// INV-26 asks for a hint a client can act on, not merely a present header. A 0 would
    /// send it straight back at a source that just shed load, and the header is documented
    /// in seconds, so the RFC's date form is replaced rather than forwarded.
    #[tokio::test]
    async fn an_unusable_upstream_retry_hint_is_replaced() {
        for hint in ["0", "-1", "not-a-number", "Wed, 21 Oct 2015 07:28:00 GMT"] {
            let upstream = axum::http::Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .header(header::RETRY_AFTER, hint)
                .body(Vec::from("busy"))
                .unwrap();
            let forwarded =
                forward_response("polygon-mainnet", reqwest::Response::from(upstream)).await;

            let got = forwarded.headers()[header::RETRY_AFTER].to_str().unwrap();
            assert!(
                got.parse::<u64>().is_ok_and(|s| s >= RETRY_AFTER_FLOOR),
                "{hint:?} must not survive as a hint, got {got:?}"
            );
        }

        // A usable one is still preserved.
        let upstream = axum::http::Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .header(header::RETRY_AFTER, "30")
            .body(Vec::from("busy"))
            .unwrap();
        let forwarded =
            forward_response("polygon-mainnet", reqwest::Response::from(upstream)).await;
        assert_eq!(forwarded.headers()[header::RETRY_AFTER], "30");
    }

    /// IB-6: a declining probe answers with the `not_ready` envelope, not bare prose,
    /// and names the live reason so a flip is attributable (OB-5). 200 has no error to
    /// describe and stays plain.
    #[tokio::test]
    async fn readiness_declines_with_the_envelope() {
        // Drive the real `NotReady` through the same selection `get_readiness` uses.
        // Passing the renderer a string of the test's own invention asserted nothing
        // about the wiring: `detail = category` would have satisfied it, and OB-5 turns
        // on exactly that substitution.
        let reason = NotReady::InsufficientConnections {
            active: 2,
            required: 10,
            workers: 12,
        };
        let detail = readiness_detail(Some(&reason), "Not ready");
        let response = readiness_response(StatusCode::SERVICE_UNAVAILABLE, &detail);
        assert_eq!(
            response.extensions().get::<ErrorCode>().copied(),
            Some(ErrorCode::NotReady),
            "a drain must not be counted as an api_error"
        );
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["code"], "not_ready");
        assert_eq!(body["error"]["type"], "availability_error");
        // The live counts, not the category: an operator must be able to tell a
        // bootstrapping portal from one that lost half its connections (OB-5).
        let message = body["error"]["message"].as_str().unwrap();
        assert_eq!(message, reason.to_string());
        for detail in ["2", "10", "12"] {
            assert!(
                message.contains(detail),
                "the reason's counts must survive into the body: {message}"
            );
        }
        assert_ne!(message, "Not ready", "the category is not the reason");

        // The other variant carries no counts, so pin it by identity too.
        assert_eq!(
            readiness_detail(Some(&NotReady::NoWorkers), "Not ready"),
            NotReady::NoWorkers.to_string()
        );
        // Shutting down has no `NotReady` to report; the category is all there is.
        assert_eq!(readiness_detail(None, "Shutting down"), "Shutting down");

        let ready = readiness_response(StatusCode::OK, "Ready");
        assert!(ready.extensions().get::<ErrorCode>().is_none());
        let bytes = axum::body::to_bytes(ready.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&bytes[..], b"Ready");
    }

    /// End-to-end: real TCP listener + axum router + reqwest client. Verifies that
    /// flipping the AtomicBool that `watch_shutdown_signal` flips on SIGTERM actually
    /// changes the response observed by an HTTP client. The handler mirrors
    /// `get_readiness` with `client.is_ready()` stubbed as `true`, since constructing
    /// a real `NetworkClient` is heavy. The "Not ready" branch is pre-existing and
    /// out of scope for this PR.
    #[tokio::test]
    async fn ready_endpoint_flips_to_503_when_shutdown_flag_set() {
        let shutting_down = Arc::new(AtomicBool::new(false));
        let cancel = CancellationToken::new();

        let app = axum::Router::new()
            .route(
                "/ready",
                axum::routing::get(|Extension(sd): Extension<Arc<AtomicBool>>| async move {
                    if sd.load(Ordering::Relaxed) {
                        (StatusCode::SERVICE_UNAVAILABLE, "Shutting down").into_response()
                    } else {
                        (StatusCode::OK, "Ready").into_response()
                    }
                }),
            )
            .layer(Extension(shutting_down.clone()));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let cancel_for_serve = cancel.clone();
        let server = tokio::spawn(
            axum::serve(listener, app)
                .with_graceful_shutdown(async move { cancel_for_serve.cancelled().await })
                .into_future(),
        );

        let client = reqwest::Client::new();
        let url = format!("http://{addr}/ready");

        let resp = client.get(&url).send().await.expect("GET /ready");
        assert_eq!(resp.status(), reqwest::StatusCode::OK);

        // Equivalent to what `run_shutdown_sequence` does on SIGTERM.
        shutting_down.store(true, Ordering::Relaxed);

        let resp = client.get(&url).send().await.expect("GET /ready");
        assert_eq!(resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);

        cancel.cancel();
        server
            .await
            .expect("server join")
            .expect("server drains cleanly");
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
}
