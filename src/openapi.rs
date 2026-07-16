#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};

use crate::network::{CurrentEpoch, NetworkClientStatus, Status, Workers};
use crate::types::api_types::AvailableDatasetApiResponse;
use crate::types::{ErrorDetail, ErrorResponse};

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

/// Block number response for timestamp query
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct BlockNumberResponse {
    pub block_number: u64,
}

/// 409 body: the standard error envelope, plus `previousBlocks` at the top level.
// Documentation-only; ErrorDetail borrows 'static strs and cannot Deserialize.
#[derive(Serialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BaseBlockConflictResponse {
    pub error: ErrorDetail,
    /// Canonical-chain blocks to walk back through when looking for a shared ancestor.
    pub previous_blocks: Vec<BlockHead>,
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
        // Stream — real-time pair, then finalized pair (POST stream before GET head).
        // Deprecated endpoints appended at the end of the group.
        crate::endpoints::stream::run_stream,
        crate::http_server::get_head,
        crate::endpoints::stream::run_finalized_stream,
        crate::http_server::get_finalized_head,
        crate::http_server::get_finalized_stream_height,
        crate::http_server::execute_query,
        // Archival stream (internal)
        crate::endpoints::stream::run_archival_stream_restricted,
        crate::endpoints::stream::run_archival_stream,
        crate::http_server::get_archival_head,
        crate::http_server::get_archival_stream_height,
        // Status (internal)
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
            BlockNumberResponse,
            ErrorResponse,
            ErrorDetail,
            BaseBlockConflictResponse,
            MetadataQueryParams,
            StreamRequestBody,
            QueryRequest,
            WorkerInfo,
            DatasetStateResponse,
        )
    ),
    info(
        title = "SQD Portal API",
        description = "API for querying and streaming blockchain data from the SQD network",
        version = "0.10.0",
    ),
    servers(
        (url = "http://localhost:8000", description = "Local development server"),
    ),
    tags(
        (name = "Status", description = "Portal and dataset status operations"),
        (name = "Datasets", description = "Dataset information and metadata"),
        (name = "Stream", description = "Stream blocks and query head — real-time and finalized"),
        (name = "Archival stream", description = "Archival data only: stable historical blocks"),
        (name = "Debug", description = "Debug operations"),
    ),
)]
pub struct ApiDoc;
