use sqd_query::Query;

use super::{BlockRange, DatasetId};

#[derive(Debug, Clone)]
pub struct StreamRequest {
    pub dataset_id: DatasetId,
    pub dataset_name: String,
    pub query: ParsedQuery,
    pub request_id: String,
    pub buffer_size: usize,
    pub max_stored_results_per_chunk: usize,
    pub max_chunks: Option<usize>,
    pub timeout_quantile: f32,
    pub retries: u8,
    pub compression: Compression,
    pub skip_parent_hash_validation: bool,
    /// The highest block this response may cover, set by the endpoint to the archival head
    /// it reported in the response headers. A stream reads the assignment live, so without
    /// it the stream follows chunks assigned after the headers went out and delivers blocks
    /// above the finalized head it announced (INV-21, INV-24). Never client-supplied.
    pub coverage_limit: Option<u64>,
}

impl StreamRequest {
    /// The part of `range` this response covers: the query's intersection with it,
    /// capped by [`Self::coverage_limit`]. `None` when it covers none of it. The stream
    /// controller both picks chunks and sizes their queries by this alone, so the two
    /// cannot disagree about where coverage ends.
    pub fn intersect_with(&self, range: &BlockRange) -> Option<BlockRange> {
        let range = self.query.intersect_with(range)?;
        let end = self
            .coverage_limit
            .map_or(*range.end(), |limit| limit.min(*range.end()));
        (*range.start() <= end).then_some(*range.start()..=end)
    }
}

#[derive(Debug, Clone)]
pub struct ParsedQuery {
    raw: String,
    without_parent_hash: Option<String>,
    parsed: Query,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    Gzip,
    Zstd,
}

impl ParsedQuery {
    pub fn try_from(str: String) -> anyhow::Result<Self> {
        let query = Query::from_json_bytes(str.as_bytes())?;
        query.validate()?;
        Ok(Self {
            raw: str,
            without_parent_hash: None,
            parsed: query,
        })
    }

    pub fn first_block(&self) -> u64 {
        self.parsed.first_block()
    }

    pub fn last_block(&self) -> Option<u64> {
        self.parsed.last_block()
    }

    /// Returns `true` when the query does not need traces or statediffs,
    /// meaning it can be served by a traceless dataset.
    pub fn is_traceless(&self) -> bool {
        match &self.parsed {
            Query::Eth(q) => !q.requires_traces() && !q.requires_statediffs(),
            _ => false,
        }
    }

    pub fn intersect_with(&self, range: &BlockRange) -> Option<BlockRange> {
        let begin = std::cmp::max(*range.start(), self.first_block());
        let end = if let Some(last_block) = self.last_block() {
            std::cmp::min(*range.end(), last_block)
        } else {
            *range.end()
        };
        (begin <= end).then_some(begin..=end)
    }

    // TODO: consider optimizing by passing a flag to workers
    pub fn without_parent_hash(&mut self) -> String {
        if self.without_parent_hash.is_none() {
            match self.parsed {
                Query::Bitcoin(ref mut q) => {
                    q.parent_block_hash = None;
                }
                Query::Eth(ref mut q) => {
                    q.parent_block_hash = None;
                }
                Query::Solana(ref mut q) => {
                    q.parent_block_hash = None;
                }
                Query::Substrate(ref mut q) => {
                    q.parent_block_hash = None;
                }
                Query::Fuel(ref mut q) => {
                    q.parent_block_hash = None;
                }
                Query::HyperliquidFills(ref mut q) => {
                    q.parent_block_hash = None;
                }
                Query::HyperliquidReplicaCmds(ref mut q) => {
                    q.parent_block_hash = None;
                }
                Query::Tron(ref mut q) => {
                    q.parent_block_hash = None;
                }
            }
            self.without_parent_hash = Some(self.parsed.to_json_string());
        }
        self.without_parent_hash.clone().unwrap()
    }

    pub fn remove_parent_hash(&mut self) {
        self.raw = self.without_parent_hash();
    }

    #[allow(clippy::inherent_to_string)]
    pub fn to_string(&self) -> String {
        self.raw.clone()
    }

    pub fn into_string(self) -> String {
        self.raw
    }

    pub fn _into_parsed(self) -> Query {
        self.parsed
    }
}

impl Compression {
    pub fn content_encoding(&self) -> &'static str {
        match self {
            Compression::Gzip => "gzip",
            Compression::Zstd => "zstd",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(to_block: Option<u64>, coverage_limit: Option<u64>) -> StreamRequest {
        let to_block = to_block.map_or(String::new(), |b| format!(r#", "toBlock": {b}"#));
        let query = format!(r#"{{"type": "evm", "fromBlock": 100{to_block}}}"#);
        StreamRequest {
            dataset_id: DatasetId::from_url("test-dataset"),
            dataset_name: "test-dataset".to_owned(),
            query: ParsedQuery::try_from(query).unwrap(),
            request_id: "test-request".to_owned(),
            buffer_size: 10,
            max_stored_results_per_chunk: 2,
            max_chunks: None,
            timeout_quantile: 0.5,
            retries: 1,
            compression: Compression::Gzip,
            skip_parent_hash_validation: false,
            coverage_limit,
        }
    }

    #[test]
    fn intersect_with_caps_the_range_at_the_coverage_limit() {
        let chunk = 100..=199;
        assert_eq!(request(None, None).intersect_with(&chunk), Some(100..=199));
        assert_eq!(
            request(None, Some(149)).intersect_with(&chunk),
            Some(100..=149)
        );
        assert_eq!(
            request(Some(120), Some(149)).intersect_with(&chunk),
            Some(100..=120)
        );
        assert_eq!(
            request(None, Some(250)).intersect_with(&chunk),
            Some(100..=199)
        );
        // A limit below the first requested block leaves nothing to cover.
        assert_eq!(request(None, Some(99)).intersect_with(&chunk), None);
        // Nor does a chunk that starts past the limit, which ends the stream.
        assert_eq!(request(None, Some(199)).intersect_with(&(200..=299)), None);
    }
}
