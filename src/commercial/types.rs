use serde::{Deserialize, Serialize};

/// Anything the control plane may add later (`suspended`, …) fails the record,
/// which `parse_records` turns into a tombstone — so an unknown status is
/// fail-closed by the same mechanism as any other malformed record, rather
/// than by a second one written just for this enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyStatus {
    Active,
    Revoked,
}

/// One key as published by the control-plane feed. Unknown fields are ignored
/// and every optional field defaults to `None`, so the data plane keeps working
/// against a newer control plane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRecord {
    pub key_id: String,

    #[serde(default)]
    pub organization_id: Option<String>,

    pub status: KeyStatus,

    /// Feed position. Deltas only move a key forward.
    pub seq: u64,

    /// Absent means the record cannot authenticate anyone; such a key is
    /// rejected rather than admitted without a secret check.
    #[serde(default)]
    pub secret_sha256: Option<String>,

    /// `None` means "valid on any portal"; an empty list means none.
    #[serde(default)]
    pub portal_ids: Option<Vec<String>>,

    /// `None` means "all datasets". Entries are canonical dataset names,
    /// matched exactly — aliases are resolved before matching.
    #[serde(default)]
    pub datasets: Option<Vec<String>>,

    #[serde(default)]
    pub expires_at: Option<u64>,
}

impl KeyRecord {
    /// Fail-closed stand-in for a record that arrived malformed but identifiable.
    pub fn tombstone(key_id: String, seq: u64) -> Self {
        Self {
            key_id,
            organization_id: None,
            status: KeyStatus::Revoked,
            seq,
            secret_sha256: None,
            portal_ids: None,
            datasets: None,
            expires_at: None,
        }
    }
}

/// Feed page envelope. The control plane sends all four fields on every page,
/// so each one is required here: an answer missing any of them is a broken
/// answer, not an empty page, and defaulting them would turn any 200 — a wrong
/// route, a half-deployed replica, a proxy with opinions — into a "successful"
/// sync that quietly stops delivering revocations. Unknown fields are still
/// ignored, so a newer control plane keeps working.
#[derive(Debug, Clone, Deserialize)]
pub struct SnapshotPage {
    pub records: Vec<serde_json::Value>,

    pub next_cursor: u64,

    /// Changing epoch is the only full-resync signal: the feed's history was
    /// rebuilt, so cursors from the previous epoch are meaningless.
    pub epoch: String,

    /// The feed's newest sequence number: what a complete read reaches.
    pub head_seq: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_ignores_unknown_fields_and_defaults_optional_ones() {
        let record: KeyRecord = serde_json::from_value(serde_json::json!({
            "key_id": "k1",
            "organization_id": null,
            "status": "active",
            "seq": 7,
            "tier": "gold",
            "limits": {"throughput_bytes_per_sec": 1000},
        }))
        .expect("unknown fields must not break parsing");

        assert_eq!(record.key_id, "k1");
        assert_eq!(record.status, KeyStatus::Active);
        assert_eq!(record.seq, 7);
        assert_eq!(record.secret_sha256, None);
        assert_eq!(record.portal_ids, None);
        assert_eq!(record.datasets, None);
        assert_eq!(record.expires_at, None);
        assert_eq!(record.organization_id, None);
    }

    /// A status this build has never heard of must not admit traffic. It fails
    /// the record, and the snapshot path tombstones records that fail — see
    /// `store::tests::malformed_records_are_tombstoned_and_unidentifiable_ones_fail_the_page`.
    #[test]
    fn an_unknown_status_fails_the_record() {
        let err = serde_json::from_value::<KeyRecord>(serde_json::json!({
            "key_id": "k1",
            "status": "suspended",
            "seq": 1,
        }))
        .expect_err("an unknown status must not parse as a usable record");

        assert!(err.to_string().contains("suspended"), "got {err}");
    }

    #[test]
    fn page_requires_every_envelope_field_and_ignores_extra_ones() {
        let page: SnapshotPage = serde_json::from_value(serde_json::json!({
            "records": [],
            "next_cursor": 12,
            "epoch": "e1",
            "head_seq": 12,
            "reset": true,
        }))
        .expect("extra envelope fields must be ignored");

        assert_eq!(page.next_cursor, 12);
        assert_eq!(page.epoch, "e1");
        assert_eq!(page.head_seq, 12);

        // Each of the four is load-bearing: without it the page cannot be
        // told apart from a broken answer, so its absence is an error.
        for missing in ["records", "next_cursor", "epoch", "head_seq"] {
            let mut body = serde_json::json!({
                "records": [],
                "next_cursor": 12,
                "epoch": "e1",
                "head_seq": 12,
            });
            body.as_object_mut().unwrap().remove(missing);
            let err = serde_json::from_value::<SnapshotPage>(body)
                .expect_err("a missing envelope field must not parse");
            assert!(err.to_string().contains(missing), "got {err}");
        }

        serde_json::from_value::<SnapshotPage>(serde_json::json!({}))
            .expect_err("an empty envelope is not an empty page");
    }

    #[test]
    fn lists_distinguish_absent_from_empty() {
        let record: KeyRecord = serde_json::from_value(serde_json::json!({
            "key_id": "k1",
            "status": "active",
            "seq": 1,
            "portal_ids": [],
            "datasets": ["ethereum-mainnet"],
        }))
        .expect("parse");

        assert_eq!(record.portal_ids, Some(Vec::new()));
        assert_eq!(
            record.datasets.as_deref(),
            Some(&["ethereum-mainnet".to_string()][..])
        );
    }
}
