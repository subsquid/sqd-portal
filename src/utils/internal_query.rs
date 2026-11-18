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

/// A header timestamp below this is in seconds, above it in milliseconds.
///
/// The dataset kind sets the unit: EVM, Solana and Bitcoin headers have seconds, Substrate,
/// Tron and Hyperliquid headers have milliseconds. The two ranges do not overlap for any
/// date after 1973.
const MILLIS_THRESHOLD: u64 = 100_000_000_000;

/// The header timestamp in Unix milliseconds, whatever unit the dataset kind uses.
fn header_millis(timestamp: u64) -> u64 {
    if timestamp < MILLIS_THRESHOLD {
        timestamp.saturating_mul(1000)
    } else {
        timestamp
    }
}

#[tracing::instrument]
/// Find the first streamed block whose timestamp is greater than or equal to `ts_millis`.
///
/// `js` must be JSON Lines ordered by block height/timestamp and each row must
/// contain `header.number` and `header.timestamp`. The archive and HotblocksDB
/// timestamp lookups rely on that ordering contract.
pub fn find_block_in_chunk(ts_millis: u64, js: &str) -> Result<u64, anyhow::Error> {
    for line in js.lines() {
        let summary = serde_json::from_str::<BlockSummary>(line)?;
        if header_millis(summary.header.timestamp) >= ts_millis {
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

    const T: u64 = 1_700_000_000;

    #[test]
    fn find_block_in_chunk_returns_first_block_with_timestamp_gte_target() {
        let chunk = jsonl(&[(10, T), (11, T + 12), (12, T + 24)]);

        assert_eq!(find_block_in_chunk(T * 1000, &chunk).unwrap(), 10);
        assert_eq!(find_block_in_chunk((T + 1) * 1000, &chunk).unwrap(), 11);
        assert_eq!(find_block_in_chunk((T + 12) * 1000, &chunk).unwrap(), 11);
    }

    #[test]
    fn find_block_in_chunk_returns_error_when_target_is_after_chunk() {
        let chunk = jsonl(&[(10, T), (11, T + 12), (12, T + 24)]);

        assert!(find_block_in_chunk((T + 25) * 1000, &chunk).is_err());
    }

    #[test]
    fn find_block_in_chunk_requires_ordered_stream_rows() {
        let unordered_chunk = jsonl(&[(12, T + 24), (10, T), (11, T + 12)]);

        assert_eq!(
            find_block_in_chunk((T + 1) * 1000, &unordered_chunk).unwrap(),
            12
        );
    }

    #[test]
    fn find_block_in_chunk_reads_millisecond_headers() {
        // Polkadot block 20000001 has the timestamp 1711049430000 ms.
        let chunk = jsonl(&[
            (19_999_999, 1_711_049_418_000),
            (20_000_000, 1_711_049_424_000),
            (20_000_001, 1_711_049_430_000),
        ]);

        assert_eq!(
            find_block_in_chunk(1_711_049_430_000, &chunk).unwrap(),
            20_000_001
        );
        assert_eq!(
            find_block_in_chunk(1_711_049_419_000, &chunk).unwrap(),
            20_000_000
        );
    }

    #[test]
    fn find_block_in_chunk_skips_a_zero_timestamp() {
        let chunk = jsonl(&[(10, 0), (11, T)]);

        assert_eq!(find_block_in_chunk(T * 1000, &chunk).unwrap(), 11);
    }
}
