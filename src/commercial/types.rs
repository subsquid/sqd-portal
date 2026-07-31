use serde::{Deserialize, Deserializer, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyStatus {
    Active,
    Revoked,
}

/// Anything the control plane may add later (`suspended`, …) must not admit
/// traffic on a portal that predates it, and must not fail the whole record
/// either — an unparseable record would leave the previous, possibly active,
/// version of the key in place.
impl<'de> Deserialize<'de> for KeyStatus {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Ok(match raw.as_str() {
            "active" => Self::Active,
            _ => Self::Revoked,
        })
    }
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

/// Feed page envelope. Every field is optional on the wire so the data plane
/// tolerates both older and newer control planes.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SnapshotPage {
    #[serde(default)]
    pub records: Vec<serde_json::Value>,

    #[serde(default)]
    pub next_cursor: u64,

    /// Changing epoch is the only full-resync signal: the feed's history was
    /// rebuilt, so cursors from the previous epoch are meaningless.
    #[serde(default)]
    pub epoch: Option<String>,

    #[serde(default)]
    pub head_seq: Option<u64>,
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

    #[test]
    fn unknown_status_is_read_as_revoked() {
        let record: KeyRecord = serde_json::from_value(serde_json::json!({
            "key_id": "k1",
            "status": "suspended",
            "seq": 1,
        }))
        .expect("unknown status must not break parsing");

        assert_eq!(record.status, KeyStatus::Revoked);
    }

    #[test]
    fn page_tolerates_absent_and_extra_fields() {
        let page: SnapshotPage = serde_json::from_value(serde_json::json!({
            "records": [],
            "next_cursor": 12,
            "reset": true,
        }))
        .expect("extra envelope fields must be ignored");

        assert_eq!(page.next_cursor, 12);
        assert_eq!(page.epoch, None);
        assert_eq!(page.head_seq, None);

        let empty: SnapshotPage =
            serde_json::from_value(serde_json::json!({})).expect("absent fields must default");
        assert!(empty.records.is_empty());
        assert_eq!(empty.next_cursor, 0);
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
