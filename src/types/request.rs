use anyhow::{bail, ensure, Context};
use serde::{de::IgnoredAny, Deserialize};
use serde_json::{Map, Value};
use sqd_query::Query;
use tokio::time::Instant;

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
    /// Set on a paced network stream (REQ-75).
    pub age_limit: Option<AgeLimit>,
}

/// Past `deadline` a stream starts no chunk after its first, sends the ones
/// already started, and ends, the way `max_chunks` ends it (ADR-018).
#[derive(Debug, Clone, Copy)]
pub struct AgeLimit {
    pub deadline: Instant,
    /// The grant's usage state, the label an end is counted under (OB-16).
    pub state: &'static str,
}

#[derive(Debug, Clone)]
pub struct ParsedQuery {
    raw: String,
    without_parent_hash: Option<String>,
    parsed: Parsed,
    last_block_cap: Option<u64>,
}

#[derive(Debug, Clone)]
enum Parsed {
    Legacy(Query),
    WithSets(SetsQuery),
}

/// The worker engine a query runs on. Only the dynamic engine serves sets (REQ-45).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    Legacy,
    Dynamic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    Gzip,
    Zstd,
}

impl ParsedQuery {
    pub fn try_from(str: String) -> anyhow::Result<Self> {
        if declares_sets(&str) {
            let query = SetsQuery::parse(&str)?;
            return Ok(Self {
                raw: str,
                without_parent_hash: None,
                parsed: Parsed::WithSets(query),
                last_block_cap: None,
            });
        }

        let query = Query::from_json_bytes(str.as_bytes())?;
        query.validate()?;
        Ok(Self {
            raw: str,
            without_parent_hash: None,
            parsed: Parsed::Legacy(query),
            last_block_cap: None,
        })
    }

    pub fn engine(&self) -> Engine {
        match self.parsed {
            Parsed::Legacy(_) => Engine::Legacy,
            Parsed::WithSets(_) => Engine::Dynamic,
        }
    }

    pub fn first_block(&self) -> u64 {
        match &self.parsed {
            Parsed::Legacy(q) => q.first_block(),
            Parsed::WithSets(q) => q.first_block,
        }
    }

    pub fn last_block(&self) -> Option<u64> {
        let last_block = match &self.parsed {
            Parsed::Legacy(q) => q.last_block(),
            Parsed::WithSets(q) => q.last_block,
        };
        last_block.into_iter().chain(self.last_block_cap).min()
    }

    /// Stops the stream at `block` without changing the query sent to workers. Endpoints
    /// cap at the head they report, so chunks assigned mid-stream can't exceed it (INV-21).
    pub fn cap_last_block(&mut self, block: u64) {
        self.last_block_cap = Some(block);
    }

    /// Returns `true` when the query does not need traces or statediffs,
    /// meaning it can be served by a traceless dataset.
    pub fn is_traceless(&self) -> bool {
        match &self.parsed {
            Parsed::Legacy(Query::Eth(q)) => !q.requires_traces() && !q.requires_statediffs(),
            // The portal doesn't read a query with sets far enough to know.
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
            let query = match &mut self.parsed {
                Parsed::Legacy(query) => query,
                Parsed::WithSets(query) => {
                    let json = query.without_parent_hash();
                    return self.without_parent_hash.insert(json).clone();
                }
            };
            match *query {
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
            self.without_parent_hash = Some(query.to_json_string());
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
}

/// Top-level keys of a query that hold no item requests.
const NON_ITEM_KEYS: &[&str] = &[
    "type",
    "fromBlock",
    "toBlock",
    "parentBlockHash",
    "fields",
    "includeAllBlocks",
    "sets",
];

/// P-QUERY-MAX-ITEMS.
const MAX_ITEMS: usize = 100;

/// A query that declares `sets` (REQ-45). The legacy parser can't read one, so the portal
/// checks what it acts on and what REQ-7 bounds, and the dynamic engine validates the rest.
#[derive(Debug, Clone)]
struct SetsQuery {
    fields: Map<String, Value>,
    first_block: u64,
    last_block: Option<u64>,
}

impl SetsQuery {
    fn parse(str: &str) -> anyhow::Result<Self> {
        let fields: Map<String, Value> =
            serde_json::from_str(str).context("query must be a JSON object")?;

        ensure!(
            fields.get("type").is_some_and(Value::is_string),
            "\"type\" must be a string"
        );
        ensure!(
            fields.get("sets").is_some_and(Value::is_object),
            "\"sets\" must be an object"
        );
        let parent_hash_valid = fields
            .get("parentBlockHash")
            .is_none_or(|value| value.is_string() || value.is_null());
        ensure!(parent_hash_valid, "\"parentBlockHash\" must be a string");

        // As the legacy parser reads them: a null `fromBlock` is refused, a null `toBlock` is
        // no bound.
        let first_block = match fields.get("fromBlock") {
            None => 0,
            Some(value) => block_number(value, "fromBlock")?,
        };
        let last_block = match fields.get("toBlock") {
            None | Some(Value::Null) => None,
            Some(value) => Some(block_number(value, "toBlock")?),
        };
        if let Some(last_block) = last_block {
            ensure!(first_block <= last_block, "got \"toBlock\" < \"fromBlock\"");
        }

        let items: usize = fields
            .iter()
            .filter(|(key, _)| !NON_ITEM_KEYS.contains(&key.as_str()))
            .filter_map(|(_, value)| value.as_array())
            .map(Vec::len)
            .sum();
        ensure!(
            items <= MAX_ITEMS,
            "query contains {items} item requests, but only {MAX_ITEMS} is allowed"
        );

        Ok(Self {
            fields,
            first_block,
            last_block,
        })
    }

    /// Keeps `sets` and every reference to them: only the parent hash goes.
    fn without_parent_hash(&self) -> String {
        let mut fields = self.fields.clone();
        fields.remove("parentBlockHash");
        Value::Object(fields).to_string()
    }
}

fn block_number(value: &Value, key: &str) -> anyhow::Result<u64> {
    match value.as_u64() {
        Some(number) => Ok(number),
        None => bail!("\"{key}\" must be an unsigned integer"),
    }
}

/// Reads the top level only. A body that is not a JSON object answers `false` and gets the
/// legacy parser's error.
fn declares_sets(str: &str) -> bool {
    #[derive(Deserialize)]
    struct TopLevel {
        sets: Option<IgnoredAny>,
    }

    serde_json::from_str::<TopLevel>(str).is_ok_and(|top| top.sets.is_some())
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

    const WITH_SETS: &str = r#"{
        "type": "evm",
        "fromBlock": 100,
        "toBlock": 200,
        "parentBlockHash": "0xabc",
        "fields": {"log": {"address": true}},
        "logs": [{"address": ["0x01", {"set": "pairs"}]}],
        "sets": {"pairs": {"fuse8": "AAAA", "canaries": ["0x02"]}}
    }"#;

    fn parse(json: &str) -> ParsedQuery {
        ParsedQuery::try_from(json.to_owned()).unwrap()
    }

    fn as_json(query: &str) -> Value {
        serde_json::from_str(query).unwrap()
    }

    #[test]
    fn a_query_with_sets_goes_to_the_dynamic_engine_unchanged() {
        let query = parse(WITH_SETS);

        assert_eq!(query.engine(), Engine::Dynamic);
        assert_eq!(query.first_block(), 100);
        assert_eq!(query.last_block(), Some(200));
        assert!(!query.is_traceless());
        assert_eq!(query.to_string(), WITH_SETS);
    }

    #[test]
    fn later_chunks_drop_only_the_parent_hash() {
        let mut query = parse(WITH_SETS);

        let mut expected = as_json(WITH_SETS);
        expected.as_object_mut().unwrap().remove("parentBlockHash");
        assert_eq!(as_json(&query.without_parent_hash()), expected);
    }

    #[test]
    fn a_query_without_sets_stays_on_the_legacy_engine() {
        let json = r#"{"type": "evm", "fromBlock": 100}"#;
        let query = parse(json);

        assert_eq!(query.engine(), Engine::Legacy);
        assert_eq!(query.to_string(), json);
    }

    #[test]
    fn a_query_with_sets_is_bounded_like_any_other() {
        let refused = [
            (
                r#"{"type": "evm", "fromBlock": 9, "toBlock": 8, "sets": {}}"#,
                "toBlock",
            ),
            (
                r#"{"type": "evm", "fromBlock": "9", "sets": {}}"#,
                "fromBlock",
            ),
            (
                r#"{"type": "evm", "fromBlock": null, "sets": {}}"#,
                "fromBlock",
            ),
            (r#"{"fromBlock": 0, "sets": {}}"#, "type"),
            (r#"{"type": "evm", "sets": []}"#, "sets"),
            (
                r#"{"type": "evm", "parentBlockHash": 1, "sets": {}}"#,
                "parentBlockHash",
            ),
        ];
        for (json, named) in refused {
            let error = ParsedQuery::try_from(json.to_owned())
                .unwrap_err()
                .to_string();
            assert!(error.contains(named), "{json}: {error}");
        }

        let logs = vec![serde_json::json!({}); MAX_ITEMS];
        let at_bound = serde_json::json!({"type": "evm", "logs": logs, "sets": {}});
        assert!(ParsedQuery::try_from(at_bound.to_string()).is_ok());
        let past_bound = serde_json::json!({
            "type": "evm", "logs": logs, "transactions": [{}], "sets": {},
        });
        assert!(ParsedQuery::try_from(past_bound.to_string()).is_err());
    }

    #[test]
    fn a_query_with_sets_starts_at_block_zero_by_default() {
        let query = parse(r#"{"type": "evm", "toBlock": null, "sets": {}}"#);

        assert_eq!(query.first_block(), 0);
        assert_eq!(query.last_block(), None);
    }
}
