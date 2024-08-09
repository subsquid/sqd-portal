use std::{str::FromStr, time::Duration};

use super::{DataChunk, DatasetId};

pub struct ClientRequest {
    pub dataset_id: DatasetId,
    pub query: ParsedQuery,
    pub buffer_size: usize,
    pub chunk_timeout: Duration,
    pub timeout_quantile: f32,
    pub request_multiplier: usize,
    pub backoff: Duration,
    pub retries: usize,
}

pub struct ParsedQuery {
    json: serde_json::Value,
    first_block: u64,
}

impl ParsedQuery {
    pub fn from_string(query: &str) -> Result<Self, anyhow::Error> {
        let json: serde_json::Value = serde_json::from_str(query)?;
        let first_block = json
            .get("fromBlock")
            .and_then(|v| v.as_u64())
            .ok_or(anyhow::anyhow!("fromBlock is required"))?;
        Ok(Self { json, first_block })
    }

    pub fn first_block(&self) -> u64 {
        self.first_block
    }

    pub fn with_range(
        &self,
        from_block: impl Into<u64>,
        to_block: impl Into<u64>,
    ) -> serde_json::Value {
        let mut json = self.json.clone();
        json["fromBlock"] = serde_json::Value::from(from_block.into());
        json["toBlock"] = serde_json::Value::from(to_block.into());
        json
    }

    pub fn with_set_chunk(&mut self, chunk: &DataChunk) -> String {
        self.json["fromBlock"] = serde_json::Value::from(chunk.first_block());
        self.json["toBlock"] = serde_json::Value::from(chunk.last_block());
        serde_json::to_string(&self.json).expect("Couldn't serialize query")
    }
}

impl FromStr for ParsedQuery {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_string(s)
    }
}