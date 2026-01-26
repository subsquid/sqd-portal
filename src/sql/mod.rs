use std::sync::Arc;

use sql_query_plan::plan::{self, TargetPlan};

pub mod metadata;
pub mod query;
mod rewrite_target;
mod extractor;

use metadata::{map_datasets_on_schemas, Metadata, Schema, SchemaErr};
use query::{QueryErr, SqlQueryResponse};

use crate::datasets;
use crate::network::NetworkClient;

use axum::body;
use prost::{DecodeError, Message};

use serde::{Deserialize, Serialize};
use substrait::proto::Plan;
use thiserror;
use tracing;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum SqlErr {
    #[error("cannot transform query: {0}")]
    QueryErr(#[from] QueryErr),
    #[error("cannot serialize metadata: {0}")]
    Metadata(#[from] SchemaErr),
    // #[error("cannot send request: {0}")]
    // Assignment(#[from] AssignErr),
}

pub async fn get_all_metadata(network: Arc<NetworkClient>) -> Result<Metadata, SqlErr> {
    let ds = network
        .datasets()
        .read()
        .iter()
        .cloned()
        .collect::<Vec<datasets::DatasetConfig>>();
    Ok(map_datasets_on_schemas(&ds)?)
}

pub async fn query(
    request: body::Bytes,
    network: &Arc<NetworkClient>
) -> Result<SqlQueryResponse, SqlErr> {
    let query_id = format!("sql-{}", Uuid::new_v4().to_string());
    let mut ctx = plan::TraversalContext::new(
        plan::Options {
            filter_pushdown: plan::FilterPushdownLevel::Extract,
        }
    );
    let tables = Vec::new();
    for src in query::get_sources(
        request,
        &mut ctx,
    )? {
        let sql = query::compile_sql(&src, &ctx)?;
        let blocks = query::unwrap_field_ranges(&src.blocks);
        let dataset_id = metadata::schema_name_to_dataset_id(&src.schema_name);
        let chunks = query::get_chunks(&src, &dataset_id, network)?;
        // get workers
    }

    Ok(SqlQueryResponse { query_id: query_id, tables: tables })
}
