//! The structural validators of spec/13. Validators 1–5 run on every stream
//! response via `validate_stream`; validator 6 (`validate_error`) covers error
//! envelopes and is not yet exercised by the Phase-0 smoke (no error-path request).
//! `errors` are hard conformance failures; `warnings` cover surfaces the gap
//! register already knows are not integrated on current master (GAP-15 cursor,
//! GAP-16 error envelope, ADR-014's 204 metadata) — reported, not fatal, so
//! the Phase-0 smoke stays green while the gaps stay visible.

use crate::driver::Decoded;
use crate::model::{Expect, StreamReq};
use crate::world::ToyWorld;

#[derive(Debug, Default)]
pub struct Verdict {
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

impl Verdict {
    fn err(&mut self, m: impl Into<String>) {
        self.errors.push(m.into());
    }
    fn warn(&mut self, m: impl Into<String>) {
        self.warnings.push(m.into());
    }
}

const KNOWN_CODES: &[&str] = &[
    "malformed_request",
    "unknown_dataset",
    "not_found",
    "base_block_mismatch",
    "no_data",
    "overloaded",
    "no_workers",
    "retries_exhausted",
    "upstream_unavailable",
    "not_ready",
    "worker_failure",
    "internal_error",
    "unclassified",
];

pub fn validate_stream(
    world: &ToyWorld,
    req: &StreamReq,
    expect: &Expect,
    d: &Decoded,
) -> Verdict {
    let mut v = Verdict::default();

    // 1 — body decodes under its declared encoding, line by line (INV-25).
    for e in &d.decode_errors {
        v.err(format!("validator1: {e}"));
    }

    match expect {
        Expect::Stream { records, head, finalized_head, source } => {
            if d.status != 200 {
                v.err(format!("expected 200, got {}", d.status));
                return v;
            }

            // 2 — records parse; strictly ascending; no duplicates (INV-20).
            let numbers = d.block_numbers();
            if numbers.len() != d.lines.len() {
                v.err("validator2: some records lack header.number".to_string());
            }
            for w in numbers.windows(2) {
                if w[1] <= w[0] {
                    v.err(format!("validator2: not strictly ascending: {} then {}", w[0], w[1]));
                }
            }

            // 3 — every record within [fromBlock, min(toBlock, frontier)] (INV-21).
            let expected_last = records.last().and_then(|r| r["header"]["number"].as_u64());
            for n in &numbers {
                if *n < req.from {
                    v.err(format!("validator3: record {n} below fromBlock {}", req.from));
                }
                if let Some(last) = expected_last {
                    if *n > last {
                        v.err(format!("validator3: record {n} above bound {last}"));
                    }
                }
            }

            // 4 + oracle diff — provenance: delivered records equal the world's
            // records for the covered range, exactly and completely (INV-22, INV-20).
            if d.lines != *records {
                v.err(format!(
                    "oracle: delivered records differ from model (got {} records, want {}; first mismatch at {})",
                    d.lines.len(),
                    records.len(),
                    d.lines
                        .iter()
                        .zip(records.iter())
                        .position(|(a, b)| a != b)
                        .map_or("length".to_string(), |i| format!("index {i}")),
                ));
            }

            // 5 — coherent headers (INV-24, INV-13, DEF-8).
            check_head_headers(&mut v, d, *head, finalized_head, false);
            match d.header("x-sqd-data-source") {
                None => v.err("validator5: missing x-sqd-data-source".to_string()),
                Some(s) if s != *source => {
                    v.err(format!("validator5: source {s}, expected {source}"))
                }
                _ => {}
            }
            if d.header("x-sqd-coverage-cursor").is_none() {
                v.warn("GAP-15: coverage cursor absent on the wire".to_string());
            }
            if d.header("x-internal-hotblocks-instance").is_some() {
                v.err("validator5: x-internal-* header leaked to the client".to_string());
            }
            if d.header("x-request-id").is_none() {
                v.err("validator5: missing x-request-id echo (REQ-9)".to_string());
            }

            let _ = world;
        }
        Expect::Empty { head, finalized_head } => {
            if d.status != 204 {
                v.err(format!("expected 204 EMPTY, got {}", d.status));
                return v;
            }
            if !d.body.is_empty() {
                v.err("validator6: 204 carries a body".to_string());
            }
            // ADR-014: EMPTY carries head metadata. Not yet integrated on master
            // (GAP-16 scope) — warn, don't fail.
            check_head_headers(&mut v, d, *head, finalized_head, true);
        }
    }
    v
}

fn check_head_headers(
    v: &mut Verdict,
    d: &Decoded,
    head: u64,
    finalized_head: &(u64, String),
    warn_only: bool,
) {
    let mut push = |msg: String| {
        if warn_only {
            v.warn(format!("ADR-014/GAP-16: {msg}"));
        } else {
            v.err(format!("validator5: {msg}"));
        }
    };
    match d.header("x-sqd-head-number").map(|s| s.parse::<u64>()) {
        Some(Ok(h)) if h == head => {}
        Some(Ok(h)) => push(format!("head number {h}, expected {head}")),
        Some(Err(_)) => push("head number unparsable".to_string()),
        None => push("missing x-sqd-head-number".to_string()),
    }
    match d.header("x-sqd-finalized-head-number").map(|s| s.parse::<u64>()) {
        Some(Ok(f)) if f == finalized_head.0 => {}
        Some(Ok(f)) => push(format!("finalized head {f}, expected {}", finalized_head.0)),
        Some(Err(_)) => push("finalized head unparsable".to_string()),
        None => push("missing x-sqd-finalized-head-number".to_string()),
    }
    match d.header("x-sqd-finalized-head-hash") {
        Some(h) if h == finalized_head.1 => {}
        Some(h) => push(format!("finalized hash {h}, expected {}", finalized_head.1)),
        None => push("missing x-sqd-finalized-head-hash".to_string()),
    }
    // Coherence is a hard rule regardless of provenance (validator 5).
    if let (Some(Ok(h)), Some(Ok(f))) = (
        d.header("x-sqd-head-number").map(|s| s.parse::<u64>()),
        d.header("x-sqd-finalized-head-number").map(|s| s.parse::<u64>()),
    ) {
        if f > h {
            v.err(format!("validator5: finalized {f} > head {h}"));
        }
    }
}

/// Validator 6 on error responses: ADR-011 envelope, closed vocabulary.
/// GAP-16: current master does not emit the envelope — warnings for now.
pub fn validate_error(d: &Decoded) -> Verdict {
    let mut v = Verdict::default();
    if d.status < 400 {
        v.err(format!("expected an error status, got {}", d.status));
        return v;
    }
    match serde_json::from_slice::<serde_json::Value>(&d.body) {
        Ok(body) => {
            let t = body["error"]["type"].as_str();
            let c = body["error"]["code"].as_str();
            match (t, c) {
                (Some(_), Some(code)) if KNOWN_CODES.contains(&code) => {}
                _ => v.warn(format!(
                    "GAP-16: error body is not the ADR-011 envelope: {}",
                    String::from_utf8_lossy(&d.body)
                )),
            }
        }
        Err(_) => v.warn(format!(
            "GAP-16: error body is not JSON: {}",
            String::from_utf8_lossy(&d.body)
        )),
    }
    v
}
