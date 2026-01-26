use std::ops::Range;

use axum::body;
use prost::{DecodeError, Message};
use serde::{Deserialize, Serialize};
use substrait::proto::Plan;
use thiserror;

use crate::sql::rewrite_target;

use sql_query_plan::plan::{self, Source, TargetPlan};

#[derive(Debug, thiserror::Error)]
pub enum QueryErr {
    #[error("cannot decode query plan: {0}")]
    DecodePlan(#[from] DecodeError),
    #[error("cannot transform plan: {0}")]
    Planning(#[from] plan::PlanErr),
    #[error("cannot compile rewritten plan: {0}")]
    RewriteTarget(#[from] rewrite_target::RewriteTargetErr),
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

// get_chunks
// use find chunk in a loop for block ranges:
// - find chunk
// - advance range behind last block in chunk (can I use next_chunk)?
// - etc.

// get workers
// new function find workers per chunk (no derivation through block)

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

pub fn compile_sql(
    src: &Source, 
    ctx: &plan::TraversalContext,
) -> Result<String, QueryErr> {
    Ok(rewrite_target::compile_sql(src, ctx)?)
}

