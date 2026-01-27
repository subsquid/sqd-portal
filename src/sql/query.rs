use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use axum::body;
use prost::{DecodeError, Message};
use serde::{Deserialize, Serialize};
use substrait::proto::Plan;
use thiserror;

use crate::network::NetworkClient;
use crate::sql::rewrite_target;
use crate::types::{BlockNumber, DatasetId};

use sql_query_plan::plan::{self, Source, TargetPlan};

#[derive(Debug, thiserror::Error)]
pub enum QueryErr {
    #[error("cannot decode query plan: {0}")]
    DecodePlan(#[from] DecodeError),
    #[error("cannot transform plan: {0}")]
    Planning(#[from] plan::PlanErr),
    #[error("cannot compile rewritten plan: {0}")]
    RewriteTarget(#[from] rewrite_target::RewriteTargetErr),
    #[error("no worker available for chunk: {0}")]
    NoWorker(String),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SqlQueryResponse {
    pub query_id: String,
    pub tables: Vec<TableItem>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableItem {
    pub schema_name: String,
    pub table_name: String,
    pub approx_num_rows: u64,
    pub workers: Vec<TableWorker>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableWorker {
    pub peer_id: String,
    pub chunks: Vec<TableChunk>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableChunk {
    pub sql: String,
    pub chunk_id: String,
}

pub fn get_sources(
    request: body::Bytes,
    ctx: &mut plan::TraversalContext,
) -> Result<Vec<Source>, QueryErr> {
    let plan = Plan::decode(request)?;
    let target = plan::traverse_plan::<rewrite_target::RewriteTarget>(&plan, ctx)?;
    let mut sources = target.get_sources();
    target.pushdown_filters(ctx, &mut sources)?;
    let mut res = Vec::new();
    for src in sources {
        if src.sqd {
            res.push(src);
        }
    }
    Ok(res)
}

pub fn get_chunks(
    dataset_id: &DatasetId,
    blocks: &[Range<u64>],
    network: &Arc<NetworkClient>,
) -> Result<HashMap<String, BlockNumber>, QueryErr> {
    let mut chunks = HashMap::new();
    for range in blocks {
        get_chunks_for_range(network, dataset_id, range, &mut chunks)?;
    }
    Ok(chunks)
}

pub fn get_chunks_for_range(
    network: &Arc<NetworkClient>,
    dataset_id: &DatasetId,
    range: &Range<u64>,
    chunks: &mut HashMap<String, BlockNumber>,
) -> Result<(), QueryErr> {
    if let Ok(chunk) = network.find_chunk(dataset_id, range.start) {
        chunks.insert(chunk.to_string(), chunk.first_block);
        let mut chunk_start = chunk.first_block;
        while chunk_start < range.end {
            if let Some(chunk) = network.next_chunk(dataset_id, &chunk) {
                chunk_start = chunk.first_block;
                chunks.insert(chunk.to_string(), chunk.first_block);
            }
        }
    }
    Ok(())
}

pub fn get_workers(
    dataset_id: &DatasetId,
    sql: &str,
    chunks: &HashMap<String, BlockNumber>,
    network: &Arc<NetworkClient>,
) -> Result<Vec<TableWorker>, QueryErr> {
    let mut workers: HashMap<String, Vec<TableChunk>> = HashMap::new();
    for (chunk, block) in chunks.into_iter() {
        // it would be convenient to search for workers directly by chunk;
        // here we search for the chunk again. Should be implemented in NetworkClient.
        if let Ok(peer_id) = network.find_worker(dataset_id, *block, false) {
            let tch = TableChunk {
                sql: sql.to_string(),
                chunk_id: chunk.to_string(),
            };
            workers
                .entry(peer_id.to_string())
                .and_modify(|v| v.push(tch.clone()))
                .or_insert(vec![tch]);
        } else {
            return Err(QueryErr::NoWorker(chunk.to_string()));
        }
    }
    let mut tws = Vec::new();
    for (peer_id, chunks) in workers.into_iter() {
        tws.push(TableWorker {
            peer_id: peer_id.to_string(),
            chunks: chunks,
        });
    }

    Ok(tws)
}

pub fn unwrap_field_ranges(frs: &[plan::FieldRange]) -> Vec<Range<u64>> {
    let mut v = Vec::new();
    for fr in frs {
        match fr {
            plan::FieldRange::BlockNumber(r) => v.push(Range {
                start: r.start as u64,
                end: r.end as u64,
            }),
            _ => continue, // TODO: timestamps to blocks
        }
    }
    v
}

pub fn compile_sql(src: &Source, ctx: &plan::TraversalContext) -> Result<String, QueryErr> {
    Ok(rewrite_target::compile_sql(src, ctx)?)
}
