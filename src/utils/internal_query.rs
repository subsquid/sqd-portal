use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::types::ParsedQuery;

#[derive(Debug, Clone, Deserialize, Serialize)]
struct BlockSummary {
    header: SummaryHeader,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct SummaryHeader {
    number: u64,
    timestamp: u64,
}

#[tracing::instrument]
pub fn find_block_in_chunk(ts: u64, js: &str) -> Result<u64, anyhow::Error> {
    for line in js.lines() {
        let summary = serde_json::from_str::<BlockSummary>(line)?;
        if summary.header.timestamp >= ts {
            return Ok(summary.header.number);
        }
    }
    Err(anyhow::anyhow!("cannot find block in chunk"))
}

pub fn build_blocknumber_query(
    kind: &str,
    first_block: u64,
    last_block: u64,
) -> anyhow::Result<ParsedQuery> {
    let query = json!({
        "type": kind.to_string(),
        "includeAllBlocks": true,
        "fromBlock": first_block,
        "toBlock": last_block,
        "fields": {
            "block": {
                "number": true,
                "timestamp": true
            }
        },
    });

    ParsedQuery::try_from(query.to_string()).map_err(|e| {
        tracing::warn!("cannot parse my own query: {:?}", e);
        e
    })
}
