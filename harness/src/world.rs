//! The toy world: ground truth every stub serves from and the oracle reads.
//! Deterministic — same seed, same world — so responses are diffable (INV-28).

use serde_json::{json, Value};

#[derive(Clone, Debug)]
pub struct ToyChunk {
    pub first: u64,
    pub last: u64,
}

impl ToyChunk {
    /// Chunk id in the strict format the portal parses:
    /// `NNNNNNNNNN/NNNNNNNNNN-NNNNNNNNNN-<hash5>`.
    pub fn id(&self, ds: &str) -> String {
        format!(
            "0000000000/{:010}-{:010}-{}",
            self.first,
            self.last,
            short_tag(ds, self.last)
        )
    }
}

#[derive(Clone, Debug)]
pub struct ToyDataset {
    /// Portal-facing default name (config key, first alias).
    pub name: String,
    pub aliases: Vec<String>,
    /// Archival identity (`s3://...`) — present iff served from the network.
    pub network_id: Option<String>,
    pub chunks: Vec<ToyChunk>,
    /// Real-time attachment: (head, finalized_head) served by the hotblocks stub.
    pub real_time: Option<(u64, u64)>,
    pub start_block: u64,
}

impl ToyDataset {
    pub fn archival_head(&self) -> Option<u64> {
        self.chunks.last().map(|c| c.last)
    }
}

#[derive(Clone, Debug)]
pub struct ToyWorld {
    pub datasets: Vec<ToyDataset>,
}

impl ToyWorld {
    /// The standard Phase-0 world: one archival dataset (3 chunks, blocks
    /// 0..=99) and one real-time-only dataset (head 49, finalized 45).
    pub fn standard() -> Self {
        ToyWorld {
            datasets: vec![
                ToyDataset {
                    name: "toy".into(),
                    aliases: vec!["toy".into(), "toy-alias".into()],
                    network_id: Some("s3://toy-dataset".into()),
                    chunks: vec![
                        ToyChunk { first: 0, last: 39 },
                        ToyChunk { first: 40, last: 79 },
                        ToyChunk { first: 80, last: 99 },
                    ],
                    real_time: None,
                    start_block: 0,
                },
                ToyDataset {
                    name: "toy-rt".into(),
                    aliases: vec!["toy-rt".into()],
                    network_id: None,
                    chunks: vec![],
                    real_time: Some((49, 45)),
                    start_block: 0,
                },
            ],
        }
    }

    pub fn dataset(&self, name: &str) -> &ToyDataset {
        self.datasets
            .iter()
            .find(|d| d.name == name || d.aliases.iter().any(|a| a == name))
            .unwrap_or_else(|| panic!("unknown toy dataset {name}"))
    }

    pub fn hash(&self, ds: &str, n: u64) -> String {
        block_hash(ds, n)
    }

    pub fn timestamp(&self, n: u64) -> u64 {
        1_700_000_000 + n * 12
    }

    /// One block record — the line every serving stub emits and the oracle expects.
    pub fn record(&self, ds: &str, n: u64) -> Value {
        json!({
            "header": {
                "number": n,
                "hash": block_hash(ds, n),
                "parentHash": if n == 0 { json!(Value::Null) } else { json!(block_hash(ds, n - 1)) },
                "timestamp": self.timestamp(n),
            }
        })
    }

    /// All blocks in `[from, to]` (the `includeAllBlocks=true` shape).
    pub fn records(&self, ds: &str, from: u64, to: u64) -> Vec<Value> {
        (from..=to).map(|n| self.record(ds, n)).collect()
    }

    /// Coverage boundary of `[from, to]` — first and last block only. The header-only
    /// world matches nothing selectively, so this is a chunk's whole selective response
    /// (INV-29); the oracle unions per-chunk boundaries for FV-6.
    pub fn boundary_records(&self, ds: &str, from: u64, to: u64) -> Vec<Value> {
        boundary_numbers(from, to)
            .into_iter()
            .map(|n| self.record(ds, n))
            .collect()
    }

    pub fn jsonl(&self, ds: &str, from: u64, to: u64) -> Vec<u8> {
        to_jsonl(&self.records(ds, from, to))
    }

    /// JSONL of the coverage boundary (the selective, `includeAllBlocks=false` shape).
    pub fn boundary_jsonl(&self, ds: &str, from: u64, to: u64) -> Vec<u8> {
        to_jsonl(&self.boundary_records(ds, from, to))
    }
}

/// First and last of `[from, to]`, collapsed to a single entry when they coincide.
pub fn boundary_numbers(from: u64, to: u64) -> Vec<u64> {
    if from >= to {
        vec![from]
    } else {
        vec![from, to]
    }
}

fn to_jsonl(records: &[Value]) -> Vec<u8> {
    let mut out = Vec::new();
    for r in records {
        out.extend_from_slice(serde_json::to_string(r).unwrap().as_bytes());
        out.push(b'\n');
    }
    out
}

/// Deterministic 64-hex block hash (FNV-1a over dataset + number, stretched).
pub fn block_hash(ds: &str, n: u64) -> String {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in ds.as_bytes().iter().copied().chain(n.to_le_bytes()) {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    let mut out = String::with_capacity(66);
    out.push_str("0x");
    let mut x = h;
    for _ in 0..4 {
        out.push_str(&format!("{:016x}", x));
        x = x.wrapping_mul(0x100000001b3) ^ h;
    }
    out
}

/// 5-char alphanumeric tag for chunk ids.
fn short_tag(ds: &str, n: u64) -> String {
    const ALPHA: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let full = block_hash(ds, n);
    full.as_bytes()[2..]
        .iter()
        .take(5)
        .map(|b| ALPHA[(*b as usize) % ALPHA.len()] as char)
        .collect()
}
