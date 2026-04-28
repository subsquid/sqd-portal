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
/// Find the first streamed block whose timestamp is greater than or equal to `ts`.
///
/// `js` must be JSON Lines ordered by block height/timestamp and each row must
/// contain `header.number` and `header.timestamp`. The archive and HotblocksDB
/// timestamp lookups rely on that ordering contract.
pub fn find_block_in_chunk(ts: u64, js: &str) -> Result<u64, anyhow::Error> {
    for line in js.lines() {
        let summary = serde_json::from_str::<BlockSummary>(line)?;
        if summary.header.timestamp >= ts {
            return Ok(summary.header.number);
        }
    }
    Err(anyhow::anyhow!("cannot find block in chunk"))
}

/// Build the minimal SQD stream query needed for timestamp-to-block lookup.
///
/// The query requests block number and timestamp for every block in the
/// inclusive `[first_block, last_block]` range. The same query shape is used for
/// archive streams and HotblocksDB streams.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn block(number: u64, timestamp: u64) -> String {
        json!({
            "header": {
                "number": number,
                "timestamp": timestamp,
            }
        })
        .to_string()
    }

    fn jsonl(blocks: &[(u64, u64)]) -> String {
        blocks
            .iter()
            .map(|(number, timestamp)| block(*number, *timestamp))
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn find_block_in_chunk_returns_first_block_with_timestamp_gte_target() {
        let chunk = jsonl(&[(10, 100), (11, 112), (12, 124)]);

        assert_eq!(find_block_in_chunk(100, &chunk).unwrap(), 10);
        assert_eq!(find_block_in_chunk(101, &chunk).unwrap(), 11);
        assert_eq!(find_block_in_chunk(112, &chunk).unwrap(), 11);
    }

    #[test]
    fn find_block_in_chunk_returns_error_when_target_is_after_chunk() {
        let chunk = jsonl(&[(10, 100), (11, 112), (12, 124)]);

        assert!(find_block_in_chunk(125, &chunk).is_err());
    }

    #[test]
    fn find_block_in_chunk_requires_ordered_stream_rows() {
        let unordered_chunk = jsonl(&[(12, 124), (10, 100), (11, 112)]);

        assert_eq!(find_block_in_chunk(101, &unordered_chunk).unwrap(), 12);
    }
}
