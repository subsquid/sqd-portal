use sql_query_plan::plan::{self, TargetPlan};

mod rewrite_target;
mod extractor;

use prost::{DecodeError, Message};

use serde::{Deserialize, Serialize};
use substrait::proto::Plan;
use thiserror;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
enum SqlErr {
    #[error("cannot decode query plan: {0}")]
    DecodePlan(#[from] DecodeError),
    #[error("cannot transform plan: {0}")]
    Planning(#[from] plan::PlanErr),
    #[error("cannot compile rewritten plan: {0}")]
    RewriteTarget(#[from] rewrite_target::RewriteTargetErr),
    // #[error("cannot serialize metadata: {0}")]
    // Metadata(#[from] MetaErr),
    // #[error("cannot send request: {0}")]
    // Assignment(#[from] AssignErr),
}

struct Metadata {
    datasets: Vec<Dataset>,
    schemas: Vec<Schema>,
}

struct Dataset {
    name: String,
    bucket_name: String,
    schema: String,
    stats: Stats,
}

struct Schema {
    name: String,
    table: Vec<Table>,
}

struct Table {
    name: String,
}

// This is dataset-oriented
// but should be generic in the future
struct Stats {
    num_blocks: u64,
    tx_per_block: u64,
    logs_per_block: u64,
    traces_per_block: u64,
    diffs_per_block: u64,
}

impl Metadata {
    fn empty() -> Metadata {
        Metadata {
            datasets: Vec::with_capacity(0),
            schemas: Vec::with_capacity(0),
        }
    } 
}

pub async fn metadata() -> Result<Metadata, SqlErr> {
    Ok(Metadata::empty())
}
