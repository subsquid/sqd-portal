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

    pub fn requires_traces(&self) -> bool {
        match &self.parsed {
            Query::Eth(q) => q.requires_traces(),
            _ => false,
        }
    }

    pub fn requires_statediffs(&self) -> bool {
        match &self.parsed {
            Query::Eth(q) => q.requires_statediffs(),
            _ => false,
        }
    }

    pub fn chain_kind(&self) -> &'static str {
        match &self.parsed {
            Query::Eth(_) => "evm",
            Query::Solana(_) => "solana",
            Query::Substrate(_) => "substrate",
            Query::Bitcoin(_) => "bitcoin",
            Query::Fuel(_) => "fuel",
            Query::HyperliquidFills(_) | Query::HyperliquidReplicaCmds(_) => "hyperliquid",
            Query::Tron(_) => "tron",
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
