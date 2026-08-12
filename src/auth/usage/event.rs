//! The record one measured segment of one response produces (DC-9).
//!
//! Every field is either what was served, who it was served to, or when — and
//! nothing here is ever read back by the portal. The wire shape is the contract
//! with the control plane's ingest, so it is pinned by a test rather than
//! left to the derive.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::http::{header, HeaderMap};
use serde::Serialize;

/// How the bytes on the wire were encoded. Kept as a label rather than
/// normalized away because logical size is estimated at read time from
/// (dataset family, encoding), and a gzip ratio applied to zstd traffic is off
/// by about half (ADR-016).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Encoding {
    Gzip,
    Zstd,
    /// No `Content-Encoding`, or an explicit `identity`.
    Identity,
    /// An encoding this build does not name. Unreachable through the portal's
    /// own negotiation, which offers exactly the three above — but recording it
    /// as `identity` would silently claim a ratio of 1 for compressed bytes,
    /// and minting a label per unknown value would be unbounded (HZ-15).
    Other,
}

impl Encoding {
    /// Read off the response, not off the request: what was negotiated and what
    /// was sent can differ, and only the second describes the bytes counted.
    pub fn of(headers: &HeaderMap) -> Self {
        let Some(value) = headers.get(header::CONTENT_ENCODING) else {
            return Self::Identity;
        };
        match value.to_str().map(str::trim) {
            Ok(value) if value.eq_ignore_ascii_case("gzip") => Self::Gzip,
            Ok(value) if value.eq_ignore_ascii_case("zstd") => Self::Zstd,
            Ok(value) if value.eq_ignore_ascii_case("identity") => Self::Identity,
            _ => Self::Other,
        }
    }
}

/// What the segment's end was. Deltas do not overlap and carry no stream id, so
/// this is the only thing that says whether more of the same response follows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// An interim delta: the response was still being written when the record
    /// was cut.
    Open,
    /// The body ended.
    Completed,
    /// The body was dropped before it ended — a client that went away, a
    /// truncation, a shutdown. The bytes measured up to that point still
    /// happened, which is why the record exists at all.
    Disconnected,
}

/// One delta: the bytes yielded to the transport since the last record for this
/// response, and the window they were yielded in.
///
/// **Deltas, never totals** (ADR-016). A completion total plus interim records
/// would double count, and a completion total alone would miss every stream
/// still running — which on this product is most of the bytes. The sum over a
/// (key, endpoint, encoding) group *is* the total for that group.
///
/// Deliberately absent: any stream or request identifier (nothing needs to
/// re-assemble one response, and one would be a join key nobody asked for),
/// `blocks` (only some routes know it, and a zero would conflate "none" with
/// "unmeasured"), and the portal's own identity — the control plane stamps that
/// from the request signature it already verified, where it cannot be forged.
#[derive(Debug, Clone, Serialize)]
pub struct UsageEvent {
    /// Idempotency key for the ingest: the sink retries whole batches, so a
    /// delivery that timed out after the control plane stored it must not count
    /// twice.
    pub event_id: String,
    pub key_id: String,
    /// Absent against a control plane that predates the claim; the read side
    /// joins on the key id instead (REQ-60).
    pub organization_id: Option<String>,
    /// The canonical dataset name, where the route names one.
    pub dataset: Option<String>,
    /// The route label, not the request path — a client-supplied path would
    /// mint a dimension per spelling (HZ-15).
    pub endpoint: String,
    pub encoding: Encoding,
    /// Encoded body bytes, as yielded to the transport. Excludes headers and
    /// HTTP framing (see [`super::tap`] for what that means for accuracy).
    pub wire_bytes: u64,
    /// Unix seconds, fractional, of the moment this window opened — the
    /// response's start for the first record, the previous record's cut for
    /// every one after it.
    pub started_at: f64,
    /// The window's length, so `(wire_bytes, started_at, duration_ms)` is a
    /// self-contained rate observation.
    pub duration_ms: u64,
    pub status: Status,
}

/// Cap on the two claims that travel as free text. Both are system-controlled —
/// `dataset` is the canonical catalog name the gate resolved, `organization_id`
/// a uuid the control plane minted — so this is armor against an upstream that
/// changes its mind, not a live bound either is expected to approach. Bytes
/// rather than characters, on a char boundary, because the ingest's column is
/// bytes and slicing one in half would panic.
const MAX_CLAIM_BYTES: usize = 256;

fn capped(value: &str) -> String {
    let mut end = MAX_CLAIM_BYTES.min(value.len());
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

impl UsageEvent {
    pub fn new(
        attribution: &super::Attribution,
        encoding: Encoding,
        wire_bytes: u64,
        window: Window,
        status: Status,
    ) -> Self {
        Self {
            event_id: uuid::Uuid::new_v4().to_string(),
            key_id: attribution.key_id().to_owned(),
            organization_id: attribution.organization_id().map(capped),
            dataset: attribution.dataset().map(capped),
            endpoint: attribution.endpoint().to_owned(),
            encoding,
            wire_bytes,
            started_at: window.started_at,
            duration_ms: window.duration.as_millis() as u64,
            status,
        }
    }
}

/// The measured interval, resolved to wall clock once per record.
#[derive(Debug, Clone, Copy)]
pub struct Window {
    pub started_at: f64,
    pub duration: Duration,
}

/// Fractional unix seconds. A record's `started_at` is derived from one wall
/// read per response plus a monotone offset, so a clock step mid-stream cannot
/// make two of its records overlap or run backwards.
pub fn unix_seconds(at: SystemTime) -> f64 {
    at.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers(encoding: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(encoding) = encoding {
            headers.insert(
                header::CONTENT_ENCODING,
                HeaderValue::from_str(encoding).unwrap(),
            );
        }
        headers
    }

    #[test]
    fn the_encoding_is_read_off_the_response_header() {
        assert_eq!(Encoding::of(&headers(Some("gzip"))), Encoding::Gzip);
        assert_eq!(Encoding::of(&headers(Some("zstd"))), Encoding::Zstd);
        assert_eq!(Encoding::of(&headers(Some("identity"))), Encoding::Identity);
        // No header at all is what an uncompressed response looks like.
        assert_eq!(Encoding::of(&headers(None)), Encoding::Identity);
    }

    /// Header values are case-insensitive tokens, and a portal behind a proxy
    /// that rewrote the case would otherwise have its whole gzip stream land in
    /// the bucket the ratio table has no row for.
    #[test]
    fn encoding_matching_is_case_insensitive_and_trimmed() {
        assert_eq!(Encoding::of(&headers(Some("GZIP"))), Encoding::Gzip);
        assert_eq!(Encoding::of(&headers(Some(" zstd "))), Encoding::Zstd);
    }

    /// Recording an unknown encoding as `identity` would claim a compression
    /// ratio of 1 for bytes that are compressed — the one error the read-time
    /// estimate cannot detect.
    #[test]
    fn an_encoding_this_build_does_not_name_is_not_called_identity() {
        for value in ["br", "deflate", "gzip, br"] {
            assert_eq!(
                Encoding::of(&headers(Some(value))),
                Encoding::Other,
                "{value}"
            );
        }
    }

    /// The wire shape is the ingest contract. A renamed or dropped field is a
    /// silent data loss on the far side, so the JSON is pinned here rather than
    /// left to whatever the derive happens to produce.
    #[test]
    fn the_json_shape_is_pinned() {
        let event = UsageEvent {
            event_id: "e1".to_owned(),
            key_id: "k1".to_owned(),
            organization_id: Some("org-7".to_owned()),
            dataset: Some("ethereum-mainnet".to_owned()),
            endpoint: "/stream".to_owned(),
            encoding: Encoding::Zstd,
            wire_bytes: 4096,
            started_at: 1_800_000_000.5,
            duration_ms: 30_000,
            status: Status::Open,
        };

        assert_eq!(
            serde_json::to_value(&event).unwrap(),
            serde_json::json!({
                "event_id": "e1",
                "key_id": "k1",
                "organization_id": "org-7",
                "dataset": "ethereum-mainnet",
                "endpoint": "/stream",
                "encoding": "zstd",
                "wire_bytes": 4096,
                "started_at": 1_800_000_000.5,
                "duration_ms": 30_000,
                "status": "open",
            })
        );
    }

    /// The two optional claims are absent, not empty strings: the ingest
    /// distinguishes "no organization was claimed" from one named "".
    #[test]
    fn unclaimed_attribution_serializes_as_null_rather_than_empty() {
        let event = UsageEvent {
            event_id: "e2".to_owned(),
            key_id: "k1".to_owned(),
            organization_id: None,
            dataset: None,
            endpoint: "/sql/query".to_owned(),
            encoding: Encoding::Identity,
            wire_bytes: 0,
            started_at: 0.0,
            duration_ms: 0,
            status: Status::Completed,
        };

        let json = serde_json::to_value(&event).unwrap();
        assert!(json["organization_id"].is_null());
        assert!(json["dataset"].is_null());
    }

    /// Armor rather than a live bound: neither claim is client-supplied, so this
    /// only ever fires if an upstream starts sending something the ingest's
    /// column cannot hold. Multi-byte on purpose — a cap that split a character
    /// would panic on the serving path, which is the one thing measurement may
    /// never do (INV-32).
    #[test]
    fn the_free_text_claims_are_capped_on_a_character_boundary() {
        // Three bytes a character, so the cap does not land on a boundary and
        // the walk back is exercised rather than skipped.
        let long = "€".repeat(400);
        let attribution = super::super::Attribution::new(
            std::sync::Arc::new(crate::auth::cache::CachedGrant {
                key_id: "k1".to_owned(),
                datasets: None,
                organization_id: Some(long.clone()),
                refresh_after: 1,
                expires_at: 2,
            }),
            Some(long),
            std::sync::Arc::from("/stream"),
        );

        let event = UsageEvent::new(
            &attribution,
            Encoding::Identity,
            0,
            Window {
                started_at: 0.0,
                duration: Duration::default(),
            },
            Status::Completed,
        );

        for claim in [&event.organization_id, &event.dataset] {
            let claim = claim.as_deref().expect("both claims were set");
            assert_eq!(claim.len(), 255, "the last whole character inside 256B");
            assert!(claim.chars().all(|c| c == '€'), "no character was split");
        }
    }

    #[test]
    fn every_status_has_a_distinct_wire_spelling() {
        let spellings: Vec<_> = [Status::Open, Status::Completed, Status::Disconnected]
            .iter()
            .map(|status| serde_json::to_value(status).unwrap())
            .collect();

        assert_eq!(spellings, ["open", "completed", "disconnected"]);
    }
}
