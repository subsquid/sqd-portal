//! The reference model (spec/13 §oracle), specialized to the toy world and the
//! success paths CT-1 smoke exercises. Where the normative docs and this model
//! disagree, the docs win.

use serde_json::Value;

use crate::world::{ToyDataset, ToyWorld};

#[derive(Debug, Clone)]
pub struct StreamReq {
    pub dataset: String,
    pub from: u64,
    pub to: Option<u64>,
    pub finalized: bool,
    /// `false` = selective: nothing matches, only the coverage boundary ships (INV-29).
    pub include_all_blocks: bool,
}

#[derive(Debug)]
pub enum Expect {
    Stream {
        records: Vec<Value>,
        head: u64,
        finalized_head: (u64, String),
        source: &'static str,
    },
    Empty {
        head: u64,
        finalized_head: (u64, String),
    },
}

pub fn model(world: &ToyWorld, req: &StreamReq) -> Expect {
    let ds = world.dataset(&req.dataset);
    let archival_head = ds.archival_head();

    let (head, fin) = match ds.real_time {
        Some((h, f)) => (h, f),
        None => {
            let h = archival_head.expect("dataset serves nothing");
            // ADR-014: archival-only finalized head = archival head.
            (h, h)
        }
    };
    let frontier = if req.finalized { fin } else { head };
    let finalized_head = (fin, world.hash(&ds.name, fin));

    if req.from > frontier {
        return Expect::Empty { head, finalized_head };
    }

    let source = match archival_head {
        Some(ah) if req.from <= ah => "network",
        _ => "real_time",
    };
    let last = req.to.map_or(frontier, |t| t.min(frontier));
    let records = if req.include_all_blocks {
        world.records(&ds.name, req.from, last)
    } else {
        // Selective: only the boundary ships. Network emits it per chunk (FV-6);
        // real-time serves the whole range at once, so its boundary is just first+last.
        match source {
            "network" => network_boundary_records(world, ds, req.from, last),
            _ => world.boundary_records(&ds.name, req.from, last),
        }
    };
    Expect::Stream {
        records,
        head,
        finalized_head,
        source,
    }
}

/// Per-chunk coverage boundary for a selective network stream: each chunk
/// intersecting `[from, last]` contributes its own first+last covered block (FV-6).
fn network_boundary_records(world: &ToyWorld, ds: &ToyDataset, from: u64, last: u64) -> Vec<Value> {
    let mut nums: Vec<u64> = Vec::new();
    for c in &ds.chunks {
        let lo = c.first.max(from);
        let hi = c.last.min(last);
        if lo > hi {
            continue;
        }
        nums.push(lo);
        if hi != lo {
            nums.push(hi);
        }
    }
    nums.sort_unstable();
    nums.dedup();
    nums.into_iter().map(|n| world.record(&ds.name, n)).collect()
}
