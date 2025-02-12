use super::{BlockRange, DatasetId};

#[derive(Debug, Clone)]
pub struct ClientRequest {
    pub dataset_id: DatasetId,
    pub dataset_name: String,
    pub query: ParsedQuery,
    pub buffer_size: usize,
    pub max_chunks: Option<usize>,
    pub timeout_quantile: f32,
    pub retries: u8,
}

#[derive(Debug, Clone)]
pub struct ParsedQuery {
    raw: String,
    parsed: sqd_node::Query,
}

impl ParsedQuery {
    pub fn try_from(str: String) -> anyhow::Result<Self> {
        let query = sqd_node::Query::from_json_bytes(str.as_bytes())?;
        query.validate()?;
        Ok(Self {
            raw: str,
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

    // Network query format is incompatible with hotblock queries
    // This should be removed once the network query format is updated
    pub fn prepare_for_network(&mut self) {
        match self.parsed {
            sqd_node::Query::Eth(ref mut q) => {
                q.parent_block_hash = None;
            }
            sqd_node::Query::Solana(ref mut q) => {
                q.parent_block_hash = None;
            }
            sqd_node::Query::Substrate(ref mut q) => {
                q.parent_block_hash = None;
            }
            sqd_node::Query::Fuel(ref mut q) => {
                q.parent_block_hash = None;
            }
        }
        self.raw = self.parsed.to_json_string()
    }

    #[allow(clippy::inherent_to_string)]
    pub fn to_string(&self) -> String {
        self.raw.clone()
    }

    pub fn into_string(self) -> String {
        self.raw
    }

    pub fn into_parsed(self) -> sqd_node::Query {
        self.parsed
    }
}
