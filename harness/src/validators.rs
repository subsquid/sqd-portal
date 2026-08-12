//! The structural validators of spec/13. Validators 1–5 run on every stream
//! response via `validate_stream`; validator 6 (`validate_error`) runs on every
//! error response and is exercised by CT-2's fault cases.
//! `errors` are hard conformance failures; `warnings` cover surfaces the gap
//! register already knows are not integrated on current master (ADR-014's 204
//! metadata) — reported, not fatal, so the suites stay green while the gaps stay
//! visible.

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

/// DEF-10's closed vocabulary bound to IB-5: `(code, type, permitted statuses)`.
/// An empty status list means the binding leaves it contextual, and only 5xx is legal.
///
/// Transcribed from the spec rather than imported from the portal: a validator sharing
/// the implementation's table would agree with it by construction, including where both
/// are wrong. `no_data` is absent because a 204 is a success and carries no code.
const TAXONOMY: &[(&str, &str, &[u16])] = &[
    ("malformed_request", "invalid_request_error", &[400]),
    ("method_not_allowed", "invalid_request_error", &[405]),
    ("unknown_dataset", "invalid_request_error", &[404]),
    ("not_found", "invalid_request_error", &[404]),
    ("base_block_mismatch", "invalid_request_error", &[409]),
    // 529 for a Portal-local refusal; a proxied one keeps the upstream's status.
    ("overloaded", "rate_limit_error", &[429, 529]),
    ("no_workers", "availability_error", &[503]),
    ("retries_exhausted", "availability_error", &[503]),
    // 502 locally; a proxied upstream failure retains its own 5xx.
    ("upstream_unavailable", "availability_error", &[]),
    ("not_ready", "availability_error", &[503]),
    ("worker_failure", "api_error", &[500]),
    ("internal_error", "api_error", &[500]),
    ("unclassified", "api_error", &[]),
    // The six auth rungs (IB-9). All 403, so the status line never tells a
    // guesser which guess to keep; the code separates them for whoever already
    // holds the key.
    ("missing_credential", "authentication_error", &[403]),
    ("invalid_credential", "authentication_error", &[403]),
    ("revoked_credential", "authentication_error", &[403]),
    ("expired_credential", "authentication_error", &[403]),
    ("portal_not_allowed", "permission_error", &[403]),
    ("dataset_not_allowed", "permission_error", &[403]),
];

/// The auth rungs, which IB-5 binds to *no* retry hint: retrying with the same
/// credential cannot succeed, and treating a refusal as transient reproduces the
/// ADR-012 storm.
const UNRETRYABLE: &[&str] = &[
    "missing_credential",
    "invalid_credential",
    "revoked_credential",
    "expired_credential",
    "portal_not_allowed",
    "dataset_not_allowed",
];

pub fn validate_stream(world: &ToyWorld, req: &StreamReq, expect: &Expect, d: &Decoded) -> Verdict {
    let mut v = Verdict::default();

    // 1 — body decodes under its declared encoding, line by line (INV-25).
    for e in &d.decode_errors {
        v.err(format!("validator1: {e}"));
    }

    match expect {
        Expect::Stream {
            records,
            head,
            finalized_head,
            source,
        } => {
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
                    v.err(format!(
                        "validator2: not strictly ascending: {} then {}",
                        w[0], w[1]
                    ));
                }
            }

            // 3 — every record within [fromBlock, min(toBlock, frontier)] (INV-21).
            let expected_last = records.last().and_then(|r| r["header"]["number"].as_u64());
            for n in &numbers {
                if *n < req.from {
                    v.err(format!(
                        "validator3: record {n} below fromBlock {}",
                        req.from
                    ));
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

            // INV-29 — last delivered record is the coverage cursor a client resumes
            // from (DEF-8/9). Isolates the resume anchor the full oracle diff subsumes.
            match (records.last(), d.lines.last()) {
                (Some(want), Some(got)) if got == want => {}
                (Some(want), Some(got)) => v.err(format!(
                    "INV-29: last record {} is not the coverage cursor {}",
                    got["header"]["number"], want["header"]["number"]
                )),
                (Some(_), None) => v.err("INV-29: a covered range delivered no record".to_string()),
                (None, _) => {}
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
            if d.header("x-internal-hotblocks-instance").is_some() {
                v.err("validator5: x-internal-* header leaked to the client".to_string());
            }
            if d.header("x-request-id").is_none() {
                v.err("validator5: missing x-request-id echo (REQ-9)".to_string());
            }

            let _ = world;
        }
        Expect::Empty {
            head,
            finalized_head,
        } => {
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
    match d
        .header("x-sqd-finalized-head-number")
        .map(|s| s.parse::<u64>())
    {
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
        d.header("x-sqd-finalized-head-number")
            .map(|s| s.parse::<u64>()),
    ) {
        if f > h {
            v.err(format!("validator5: finalized {f} > head {h}"));
        }
    }
}

/// Validator 6 on error responses: the ADR-011 envelope against the whole of DEF-10 and
/// IB-5 — code in the vocabulary, the type *bound to that code*, a status the binding
/// permits, and the hint rule. Checking the code alone left `type` free-form, so a
/// response could name a type outside the axis, or pair a code with the wrong one, and
/// still pass (INV-26).
pub fn validate_error(d: &Decoded) -> Verdict {
    let mut v = Verdict::default();
    if d.status < 400 {
        v.err(format!("expected an error status, got {}", d.status));
        return v;
    }
    let Ok(body) = serde_json::from_slice::<serde_json::Value>(&d.body) else {
        v.err(format!(
            "validator6: error body is not JSON: {}",
            String::from_utf8_lossy(&d.body)
        ));
        return v;
    };

    let Some(code) = body["error"]["code"].as_str() else {
        v.err(format!(
            "validator6: error body is not the ADR-011 envelope: {}",
            String::from_utf8_lossy(&d.body)
        ));
        return v;
    };
    let Some(&(_, want_type, statuses)) = TAXONOMY.iter().find(|(c, _, _)| *c == code) else {
        v.err(format!(
            "validator6: {code} is outside the DEF-10 vocabulary"
        ));
        return v;
    };

    match body["error"]["type"].as_str() {
        Some(t) if t == want_type => {}
        other => v.err(format!(
            "validator6: {code} is bound to {want_type}, got {other:?}"
        )),
    }
    if !statuses.is_empty() && !statuses.contains(&d.status) {
        v.err(format!(
            "validator6: IB-5 binds {code} to {statuses:?}, got {}",
            d.status
        ));
    } else if statuses.is_empty() && d.status < 500 {
        v.err(format!(
            "validator6: {code} is a contextual 5xx, got {}",
            d.status
        ));
    }
    if body["error"]["message"]
        .as_str()
        .is_none_or(|m| m.is_empty())
    {
        v.err(format!("validator6: {code} does not explain itself"));
    }

    // INV-26: OVERLOADED always says how long to wait, DATA-UNAVAILABLE never does.
    let hint = d.header("retry-after");
    match code {
        "overloaded" => match hint.map(|h| h.trim().parse::<u64>()) {
            Some(Ok(seconds)) if seconds >= 1 => {}
            other => v.err(format!(
                "validator6: overloaded needs a usable hint, got {other:?}"
            )),
        },
        "no_workers" if hint.is_some() => {
            v.err(format!(
                "validator6: no_workers must carry no hint, got {hint:?}"
            ));
        }
        code if UNRETRYABLE.contains(&code) && hint.is_some() => {
            v.err(format!(
                "validator6: {code} is not retryable and must carry no hint, got {hint:?}"
            ));
        }
        _ => {}
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn response(status: u16, body: &str, hint: Option<&str>) -> Decoded {
        let mut headers = HashMap::new();
        if let Some(hint) = hint {
            headers.insert("retry-after".to_owned(), hint.to_owned());
        }
        Decoded {
            status,
            headers,
            body: body.as_bytes().to_vec(),
            lines: Vec::new(),
            decode_errors: Vec::new(),
        }
    }

    fn envelope(t: &str, code: &str) -> String {
        format!(r#"{{"error":{{"type":"{t}","code":"{code}","message":"m"}}}}"#)
    }

    /// The validator is the gate the CI job rests on, so its own holes are invisible:
    /// accepting the code alone let a type outside the axis — or bound to a different
    /// code — pass, and the suite would have stayed green through it.
    #[test]
    fn a_type_outside_its_code_is_rejected() {
        let banana = response(529, &envelope("banana", "overloaded"), Some("1"));
        assert!(!validate_error(&banana).errors.is_empty());

        let crossed = response(500, &envelope("availability_error", "internal_error"), None);
        assert!(!validate_error(&crossed).errors.is_empty());

        let ok = response(500, &envelope("api_error", "internal_error"), None);
        assert!(
            validate_error(&ok).errors.is_empty(),
            "{:?}",
            validate_error(&ok).errors
        );
    }

    /// IB-5 fixes the status per code, so a right code on a wrong status is still a
    /// contract break — and `unclassified` may be any 5xx but never a 4xx.
    #[test]
    fn a_status_outside_its_binding_is_rejected() {
        let wrong = response(
            500,
            &envelope("invalid_request_error", "malformed_request"),
            None,
        );
        assert!(!validate_error(&wrong).errors.is_empty());

        let proxied = response(429, &envelope("rate_limit_error", "overloaded"), Some("30"));
        assert!(validate_error(&proxied).errors.is_empty());

        let contextual = response(503, &envelope("api_error", "unclassified"), None);
        assert!(validate_error(&contextual).errors.is_empty());
        let as_4xx = response(400, &envelope("api_error", "unclassified"), None);
        assert!(!validate_error(&as_4xx).errors.is_empty());
    }

    /// INV-26 is an iff: the overload owes a usable hint, DATA-UNAVAILABLE owes none.
    #[test]
    fn the_hint_rule_runs_both_ways() {
        for hint in [None, Some("0"), Some("Wed, 21 Oct 2015 07:28:00 GMT")] {
            let d = response(529, &envelope("rate_limit_error", "overloaded"), hint);
            assert!(!validate_error(&d).errors.is_empty(), "{hint:?}");
        }
        let with_hint = response(
            503,
            &envelope("availability_error", "no_workers"),
            Some("5"),
        );
        assert!(!validate_error(&with_hint).errors.is_empty());
    }

    #[test]
    fn an_unknown_code_and_a_silent_message_are_rejected() {
        let unknown = response(500, &envelope("api_error", "kaboom"), None);
        assert!(!validate_error(&unknown).errors.is_empty());

        let silent = response(
            500,
            r#"{"error":{"type":"api_error","code":"internal_error","message":""}}"#,
            None,
        );
        assert!(!validate_error(&silent).errors.is_empty());
    }
}
