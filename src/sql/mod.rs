use std::ops::Range;
use std::sync::Arc;

use sql_query_plan::plan;

mod extractor;
pub mod metadata;
pub mod query;
mod rewrite_target;

use metadata::{map_datasets_on_schemas, Metadata, SchemaErr};
use query::{QueryErr, SqlQueryResponse, TableItem};

use crate::datasets;
use crate::network::NetworkClient;

use axum::body;

use thiserror;
use tracing;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum SqlErr {
    #[error("cannot transform query: {0}")]
    QueryErr(#[from] QueryErr),
    #[error("cannot serialize metadata: {0}")]
    Metadata(#[from] SchemaErr),
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
    network: &Arc<NetworkClient>,
) -> Result<SqlQueryResponse, SqlErr> {
    let query_id = format!("sql-{}", Uuid::new_v4().to_string());
    let mut ctx = plan::TraversalContext::new(plan::Options::default());
    let mut tables = Vec::new();
    for src in query::get_sources(request, &mut ctx)? {
        let sql = query::compile_sql(&src, &ctx)?;
        tracing::info!("Derived SQL '{sql}'");
        let blocks = query::unwrap_field_ranges(&src.blocks);
        let dataset_id = metadata::schema_name_to_dataset_id(&src.schema_name);
        // no blocks means no filters
        let chunks = if blocks.is_empty() {
            query::get_chunks(
                &dataset_id,
                &vec![Range {
                    start: 0,
                    end: u64::MAX,
                }],
                network,
            )?
        } else {
            query::get_chunks(&dataset_id, &blocks, network)?
        };
        let workers = query::get_workers(&dataset_id, &sql, &chunks, network)?;
        tracing::info!(
            "For table '{}' {} chunks on {} workers",
            src.table_name,
            chunks.len(),
            workers.len(),
        );
        tables.push(TableItem {
            schema_name: src.schema_name.to_string(),
            table_name: src.table_name.to_string(),
            approx_num_rows: metadata::compute_stats(&dataset_id, &src.table_name, &blocks),
            workers: workers,
        });
    }

    Ok(SqlQueryResponse {
        query_id: query_id,
        tables: tables,
    })
}
