//! The reference model (spec/13 §oracle), specialized to the toy world and the
//! success paths CT-1 smoke exercises. Where the normative docs and this model
//! disagree, the docs win.

use serde_json::Value;

use crate::world::ToyWorld;

#[derive(Debug, Clone)]
pub struct StreamReq {
    pub dataset: String,
    pub from: u64,
    pub to: Option<u64>,
    pub finalized: bool,
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
    Expect::Stream {
        records: world.records(&ds.name, req.from, last),
        head,
        finalized_head,
        source,
    }
}
