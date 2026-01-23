use std::sync::Arc;

use sql_query_plan::plan::{self, TargetPlan};

pub mod metadata;
mod rewrite_target;
mod extractor;

use metadata::{map_datasets_on_schemas, Metadata, Schema, SchemaErr};

use crate::datasets;
use crate::network::NetworkClient;

use prost::{DecodeError, Message};

use serde::{Deserialize, Serialize};
use substrait::proto::Plan;
use thiserror;
use tracing;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum SqlErr {
    #[error("cannot decode query plan: {0}")]
    DecodePlan(#[from] DecodeError),
    #[error("cannot transform plan: {0}")]
    Planning(#[from] plan::PlanErr),
    #[error("cannot compile rewritten plan: {0}")]
    RewriteTarget(#[from] rewrite_target::RewriteTargetErr),
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
