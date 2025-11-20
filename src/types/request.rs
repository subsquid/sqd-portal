use sqd_query::Query;

use super::{BlockRange, DatasetId};

#[derive(Debug, Clone)]
pub struct ClientRequest {
    pub dataset_id: DatasetId,
    pub dataset_name: String,
    pub query: ParsedQuery,
    pub request_id: String,
    pub buffer_size: usize,
    pub max_chunks: Option<usize>,
    pub timeout_quantile: f32,
    pub retries: u8,
}

#[derive(Debug, Clone)]
pub struct ParsedQuery {
    raw: String,
    without_parent_hash: Option<String>,
    parsed: Query,
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
                Query::Hyperliquid(ref mut q) => {
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
