#![allow(dead_code)]

use std::sync::Arc;

use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};

use crate::network::{CurrentEpoch, NetworkClientStatus, Status, Workers};
use crate::types::api_types::AvailableDatasetApiResponse;

/// Status response for the portal
#[derive(Serialize, Clone, Debug, ToSchema)]
pub struct StatusResponse {
    /// Portal version string (semver)
    pub portal_version: String,
    #[serde(flatten)]
    pub status: NetworkClientStatus,
}

/// Block head information
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct BlockHead {
    pub number: u64,
    pub hash: String,
}

/// Body of a `409 Conflict` on a stream request — a slice of the **current
/// canonical chain** at and below the conflict point, most recent first.
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(example = json!({
    "previousBlocks": [
        { "number": 21780872, "hash": "0xf6a96a29..." },
        { "number": 21780871, "hash": "0xab12cd..." }
    ]
}))]
pub struct ConflictResponse {
    /// `{ number, hash }` pairs from the canonical chain, descending from the conflict
    /// point. Guaranteed to contain at least the parent of the requested `fromBlock`.
    pub previous_blocks: Vec<BlockHead>,
}

/// Block number response for timestamp query
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct BlockNumberResponse {
    pub block_number: u64,
}

/// Generic error response
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct ErrorResponse {
    pub message: String,
}

/// Metadata query parameters
#[derive(Deserialize, Debug, ToSchema)]
#[allow(dead_code)]
pub struct MetadataQueryParams {
    #[serde(default, rename = "expand[]")]
    pub expand: Vec<String>,
}

/// Data query request body for stream endpoints.
/// Note: the full data query object accepts additional chain-specific filter fields
/// beyond those documented here. See the data query specification for details.
#[derive(Deserialize, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct StreamRequestBody {
    /// The number of the first block to fetch (required)
    pub from_block: u64,
    /// The number of the last block to fetch, inclusive (optional)
    pub to_block: Option<u64>,
    /// Expected hash of the parent of the first requested block (optional)
    pub parent_block_hash: Option<String>,
}

/// Query execution request
#[derive(Deserialize, Debug, ToSchema)]
#[allow(dead_code)]
pub struct QueryRequest {
    pub query: String,
}

/// Worker information response
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct WorkerInfo {
    pub peers: Vec<String>,
}

/// Dataset state response
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct DatasetStateResponse {
    #[serde(flatten)]
    pub state: serde_json::Value,
}

#[derive(OpenApi)]
#[openapi(
    paths(
        // Datasets
        crate::http_server::get_datasets,
        crate::http_server::get_dataset_metadata,
        crate::http_server::get_dataset_state,
        crate::endpoints::block_number_by_timestamp::get_blocknumber_by_timestamp,
        // Stream — real-time pair, then finalized pair (POST stream before GET head),
        // then archival variants (internal). Deprecated endpoints appended at the end.
        crate::endpoints::stream::run_stream,
        crate::http_server::get_head,
        crate::endpoints::stream::run_finalized_stream,
        crate::http_server::get_finalized_head,
        crate::endpoints::stream::run_archival_stream_restricted,
        crate::endpoints::stream::run_archival_stream,
        crate::http_server::get_archival_head,
        crate::http_server::get_finalized_stream_height,
        crate::http_server::get_archival_stream_height,
        crate::http_server::execute_query,
        // Monitoring (internal)
        crate::http_server::get_status,
        crate::http_server::get_readiness,
        crate::http_server::get_metrics,
        // Debug (internal)
        crate::http_server::get_debug_block,
        crate::http_server::get_all_workers,
    ),
    components(
        schemas(
            StatusResponse,
            NetworkClientStatus,
            CurrentEpoch,
            Workers,
            Status,
            AvailableDatasetApiResponse,
            BlockHead,
            ConflictResponse,
            BlockNumberResponse,
            ErrorResponse,
            MetadataQueryParams,
            StreamRequestBody,
            QueryRequest,
            WorkerInfo,
            DatasetStateResponse,
        )
    ),
    info(
        title = "SQD Portal API",
        description = concat!(
            include_str!("../docs/openapi/01-introduction.md"),
            "\n\n",
            include_str!("../docs/openapi/02-blockchain-forks.md"),
            "\n\n",
            include_str!("../docs/openapi/03-getting-started.md"),
        ),
        version = "0.10.0",
    ),
    servers(
        (url = "http://localhost:8000", description = "Local development server"),
    ),
    tags(
        (name = "Datasets", description = "Dataset information and metadata"),
        (name = "Streaming", description = "Stream blocks and query head — real-time, finalized, and archival"),
        (name = "Monitoring", description = "Portal status, readiness and metrics"),
        (name = "Debug", description = "Debug operations"),
    ),
)]
pub struct ApiDoc;

/// To mark an API operation as internal — so it is pruned from the served
/// OpenAPI spec unless `show_internal` is true — add this extension on its
/// handler:
///
/// ```ignore
/// #[utoipa::path(
///     ...,
///     extensions(("x-internal" = json!(true))),
/// )]
/// ```
///
/// [`build_openapi_spec`] reads this key to decide which operations to drop.
const INTERNAL_EXT_KEY: &str = "x-internal";

/// Builds the served OpenAPI spec from [`ApiDoc`], optionally pruning operations
/// (and now-empty tags) marked internal via the `x-internal` extension.
pub fn build_openapi_spec(show_internal: bool) -> utoipa::openapi::OpenApi {
    let mut spec = ApiDoc::openapi();
    let is_internal = |op: &utoipa::openapi::path::Operation| {
        op.extensions
            .as_ref()
            .and_then(|e| e.get(INTERNAL_EXT_KEY))
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    };

    // Tags that no operation references in the *original* spec are "doc-only"
    // (e.g. "Forks") — they carry markdown content for the sidebar. We must
    // never prune them, regardless of operation visibility.
    let original_op_tags: std::collections::HashSet<String> = spec
        .paths
        .paths
        .values()
        .flat_map(|item| {
            [
                &item.get,
                &item.put,
                &item.post,
                &item.delete,
                &item.options,
                &item.head,
                &item.patch,
                &item.trace,
            ]
        })
        .filter_map(|slot| slot.as_ref())
        .flat_map(|op| op.tags.iter().flatten().cloned())
        .collect();

    spec.paths.paths.retain(|_, item| {
        for slot in [
            &mut item.get,
            &mut item.put,
            &mut item.post,
            &mut item.delete,
            &mut item.options,
            &mut item.head,
            &mut item.patch,
            &mut item.trace,
        ] {
            if let Some(op) = slot.as_mut() {
                if is_internal(op) {
                    if show_internal {
                        // Scalar (and some other renderers) treat `x-internal: true`
                        // as a hint to hide the operation client-side, even if it's
                        // present in the served spec. Strip the marker so the
                        // operation actually renders in /docs.
                        if let Some(ext) = op.extensions.as_mut() {
                            ext.remove(INTERNAL_EXT_KEY);
                        }
                    } else {
                        *slot = None;
                    }
                }
            }
        }
        item.get.is_some()
            || item.put.is_some()
            || item.post.is_some()
            || item.delete.is_some()
            || item.options.is_some()
            || item.head.is_some()
            || item.patch.is_some()
            || item.trace.is_some()
    });

    // Drop tag declarations that no surviving operation references — otherwise
    // Scalar/Swagger render empty groups.
    let used_tags: std::collections::HashSet<String> = spec
        .paths
        .paths
        .values()
        .flat_map(|item| {
            [
                &item.get,
                &item.put,
                &item.post,
                &item.delete,
                &item.options,
                &item.head,
                &item.patch,
                &item.trace,
            ]
        })
        .filter_map(|slot| slot.as_ref())
        .flat_map(|op| op.tags.iter().flatten().cloned())
        .collect();
    if let Some(tags) = spec.tags.as_mut() {
        tags.retain(|t| {
            // Keep tags that still have operations OR tags that were never bound
            // to operations in the first place (doc-only sections).
            used_tags.contains(&t.name) || !original_op_tags.contains(&t.name)
        });
    }
    spec
}

/// Axum handler that serves the pre-built OpenAPI spec injected as an extension.
pub async fn serve_openapi_spec(
    Extension(spec): Extension<Arc<utoipa::openapi::OpenApi>>,
) -> Json<utoipa::openapi::OpenApi> {
    Json((*spec).clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_ops(spec: &utoipa::openapi::OpenApi) -> Vec<&utoipa::openapi::path::Operation> {
        spec.paths
            .paths
            .values()
            .flat_map(|i| {
                [
                    &i.get, &i.put, &i.post, &i.delete, &i.options, &i.head, &i.patch, &i.trace,
                ]
            })
            .filter_map(|slot| slot.as_ref())
            .collect()
    }

    fn has_internal_ext(op: &utoipa::openapi::path::Operation) -> bool {
        op.extensions
            .as_ref()
            .and_then(|e| e.get(INTERNAL_EXT_KEY))
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    #[test]
    fn hide_internal_drops_marked_ops_and_empty_tags() {
        let hidden = build_openapi_spec(false);
        let ops = all_ops(&hidden);
        assert!(
            ops.iter().all(|op| !has_internal_ext(op)),
            "no surviving op should carry x-internal"
        );

        let tag_names: Vec<&str> = hidden
            .tags
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|t| t.name.as_str())
            .collect();
        for empty_tag in ["Monitoring", "Debug"] {
            assert!(
                !tag_names.contains(&empty_tag),
                "tag {empty_tag} should be pruned (all its operations are internal); got {tag_names:?}"
            );
        }
    }

    #[test]
    fn show_internal_keeps_all_ops() {
        let shown = build_openapi_spec(true);
        let full = ApiDoc::openapi();
        assert_eq!(shown.paths.paths.len(), full.paths.paths.len());
        let ops = all_ops(&shown);
        assert!(!ops.is_empty(), "should have at least some operations");
        // Scalar hides operations carrying `x-internal: true` client-side, so the
        // marker must be stripped when we want them rendered.
        assert!(
            ops.iter().all(|op| !has_internal_ext(op)),
            "no surviving op should still carry x-internal when show_internal=true"
        );
    }
}
