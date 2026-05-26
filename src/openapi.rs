use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};

use crate::network::{CurrentEpoch, NetworkClientStatus, Status, Workers};
use crate::types::api_types::AvailableDatasetApiResponse;

#[allow(dead_code)]

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
        crate::http_server::get_status,
        crate::http_server::get_datasets,
        crate::http_server::get_dataset_metadata,
        crate::http_server::get_dataset_state,
        crate::http_server::get_archival_head,
        crate::http_server::get_finalized_head,
        crate::http_server::get_head,
        crate::endpoints::block_number_by_timestamp::get_blocknumber_by_timestamp,
        crate::endpoints::stream::run_archival_stream_restricted,
        crate::endpoints::stream::run_archival_stream,
        crate::endpoints::stream::run_finalized_stream,
        crate::endpoints::stream::run_stream,
        crate::http_server::execute_query,
        crate::http_server::get_readiness,
        crate::http_server::get_metrics,
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
        version = "0.9.1",
    ),
    servers(
        (url = "http://localhost:8000", description = "Local development server"),
    ),
    tags(
        (name = "status", description = "Portal and dataset status operations"),
        (name = "datasets", description = "Dataset information and metadata"),
        (name = "stream", description = "Data streaming operations"),
        (name = "head", description = "Block head information"),
        (name = "query", description = "Query operations"),
        (name = "debug", description = "Debug operations"),
    ),
)]
pub struct ApiDoc;
