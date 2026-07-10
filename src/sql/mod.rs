use std::ops::Range;
use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use sql_query_plan::plan::{self, Source};

mod extractor;
pub mod metadata;
pub mod query;
mod rewrite_target;

use metadata::{map_datasets_on_schemas, Metadata, SchemaErr};
use query::{QueryErr, SqlQueryResponse, TableItem};

use crate::datasets;
use crate::network::NetworkClient;
use crate::{
    commercial::{Granted, Rejected},
    types::{DatasetId, GenericError, RequestError},
};

use axum::body;

use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum SqlErr {
    #[error("cannot transform query: {0}")]
    QueryErr(#[from] QueryErr),
    #[error("cannot serialize metadata: {0}")]
    Metadata(#[from] SchemaErr),
    #[error("commercial request rejected: {}", .0.reason)]
    Rejected(Rejected),
}

impl IntoResponse for SqlErr {
    fn into_response(self) -> Response {
        match self {
            SqlErr::QueryErr(err) => err.into_response(),
            SqlErr::Metadata(err) => (
                StatusCode::BAD_REQUEST,
                axum::Json(GenericError {
                    message: err.to_string(),
                }),
            )
                .into_response(),
            SqlErr::Rejected(rejected) => RequestError::from(rejected).into_response(),
        }
    }
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
    grant: Option<&Granted>,
) -> Result<SqlQueryResponse, SqlErr> {
    let query_id = format!("sql-{}", Uuid::new_v4());
    let mut ctx = plan::TraversalContext::new(plan::Options::default());
    let mut tables = Vec::new();
    for ResolvedSqlSource {
        source: src,
        dataset_id,
        ..
    } in resolve_authorized_sources(request, &mut ctx, grant)?
    {
        tracing::trace!("Source: {src:?}");
        let sql = query::compile_sql(&src, &ctx)?;
        tracing::info!("Derived SQL '{sql}'");
        let blocks = query::unwrap_field_ranges(&src.blocks);
        // No blocks means no filters.
        // A method in network client to get
        // all chunks of a dataset would be more efficient.
        let chunks = if blocks.is_empty() {
            query::get_chunks(
                &dataset_id,
                &[Range {
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
            workers,
        });
    }

    Ok(SqlQueryResponse { query_id, tables })
}

#[derive(Debug)]
struct ResolvedSqlSource {
    source: Source,
    dataset_id: DatasetId,
    dataset_slug: String,
}

fn resolve_authorized_sources(
    request: body::Bytes,
    ctx: &mut plan::TraversalContext,
    grant: Option<&Granted>,
) -> Result<Vec<ResolvedSqlSource>, SqlErr> {
    let sources = query::get_sources(request, ctx)?;
    let mut resolved = Vec::with_capacity(sources.len());
    for source in sources {
        resolved.push(ResolvedSqlSource {
            dataset_slug: metadata::schema_name_to_dataset_slug(&source.schema_name),
            dataset_id: metadata::schema_name_to_dataset_id(&source.schema_name),
            source,
        });
    }
    enforce_sql_entitlements(grant, &resolved)?;
    Ok(resolved)
}

fn enforce_sql_entitlements(
    grant: Option<&Granted>,
    sources: &[ResolvedSqlSource],
) -> Result<(), SqlErr> {
    let Some(entitled_chains) = grant.and_then(|grant| grant.entitled_chains.as_ref()) else {
        return Ok(());
    };

    if sources
        .iter()
        .any(|source| !entitled_chains.contains(&source.dataset_slug))
    {
        return Err(SqlErr::Rejected(Rejected {
            reason: "dataset_not_entitled".to_string(),
            http_status: 403,
            message: "Dataset is not enabled for this key".to_string(),
            retry_after_secs: None,
        }));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::{body, response::IntoResponse};
    use prost::Message;
    use substrait::proto::Plan;

    use crate::commercial::{GrantedLimits, OnExceed, Principal};

    use super::*;

    static JSON_PLAN_SOLANA: &str = include_str!("../../resources/block_plain_with_cols.json");

    fn sql_request(json: &str) -> body::Bytes {
        let plan: Plan = serde_json::from_str(json).unwrap();
        let mut bytes = Vec::new();
        plan.encode(&mut bytes).unwrap();
        body::Bytes::from(bytes)
    }

    fn grant_with_entitlements(chains: &[&str]) -> Granted {
        Granted {
            principal: Principal {
                account_id: "account".to_string(),
                api_key_id: Some("key".to_string()),
            },
            tally_account_id: None,
            entitled_chains: Some(chains.iter().map(|chain| (*chain).to_string()).collect()),
            limits: GrantedLimits::default(),
            on_exceed: OnExceed::Reject,
            quota_version: 1,
            quota_remaining_bytes: Some(1_000_000),
            concurrency_permit: None,
        }
    }

    #[test]
    fn sql_entitlement_rejects_unentitled_referenced_dataset() {
        let grant = grant_with_entitlements(&["ethereum-mainnet"]);
        let mut ctx = plan::TraversalContext::new(plan::Options::default());
        let err =
            match resolve_authorized_sources(sql_request(JSON_PLAN_SOLANA), &mut ctx, Some(&grant))
            {
                Ok(_) => panic!("expected SQL entitlement rejection"),
                Err(err) => err,
            };

        match &err {
            SqlErr::Rejected(rejected) => {
                assert_eq!(rejected.reason, "dataset_not_entitled");
                assert_eq!(rejected.http_status, 403);
            }
            other => panic!("expected commercial rejection, got {other:?}"),
        }
        assert_eq!(err.into_response().status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn sql_entitlement_allows_entitled_referenced_dataset() {
        let grant = grant_with_entitlements(&["solana-mainnet"]);
        let mut ctx = plan::TraversalContext::new(plan::Options::default());

        let sources =
            resolve_authorized_sources(sql_request(JSON_PLAN_SOLANA), &mut ctx, Some(&grant))
                .unwrap();

        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].dataset_slug, "solana-mainnet");
        assert_eq!(sources[0].dataset_id.to_string(), "s3://solana-mainnet");
    }

    #[test]
    fn sql_entitlement_allows_oss_no_grant() {
        let mut ctx = plan::TraversalContext::new(plan::Options::default());

        let sources = resolve_authorized_sources(sql_request(JSON_PLAN_SOLANA), &mut ctx, None)
            .expect("OSS SQL requests should be unrestricted");

        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].dataset_slug, "solana-mainnet");
    }
}
