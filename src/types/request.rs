use super::{BlockRange, DatasetId};

#[derive(Debug, Clone)]
pub struct ClientRequest {
    pub dataset_id: DatasetId,
    pub dataset_name: String,
    pub query: ParsedQuery,
    pub buffer_size: usize,
    pub max_chunks: Option<usize>,
    pub timeout_quantile: f32,
    pub retries: usize,
}

#[derive(Debug, Clone)]
pub struct ParsedQuery {
    raw: String,
    first_block: u64,
    last_block: Option<u64>,
}

impl ParsedQuery {
    pub fn try_from(query: String) -> Result<Self, anyhow::Error> {
        let json: serde_json::Value = serde_json::from_str(&query)?;
        let first_block = json
            .get("fromBlock")
            .and_then(|v| v.as_u64())
            .ok_or(anyhow::anyhow!("fromBlock is required"))?;
        let last_block = json.get("toBlock").and_then(|v| v.as_u64());
        anyhow::ensure!(
            last_block.is_none() || last_block >= Some(first_block),
            "toBlock must be greater or equal to fromBlock"
        );
        Ok(Self {
            raw: query,
            first_block,
            last_block,
        })
    }

    pub fn first_block(&self) -> u64 {
        self.first_block
    }

    pub fn last_block(&self) -> Option<u64> {
        self.last_block
    }

    pub fn intersect_with(&self, range: &BlockRange) -> Option<BlockRange> {
        let begin = std::cmp::max(*range.start(), self.first_block);
        let end = if let Some(last_block) = self.last_block {
            std::cmp::min(*range.end(), last_block)
        } else {
            *range.end()
        };
        (begin <= end).then_some(begin..=end)
    }

    #[allow(clippy::inherent_to_string)]
    pub fn to_string(&self) -> String {
        self.raw.clone()
    }

    pub fn into_string(self) -> String {
        self.raw
    }
}
