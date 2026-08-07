use serde::Deserialize;

/// The claim vocabulary this build understands. A grant that names any other
/// version is unusable rather than partly usable: reading a newer vocabulary
/// for the fields it happens to recognise is how a restriction the control
/// plane added becomes a permission the portal grants (DC-8, DEF-17).
pub const CLAIMS_VERSION: u32 = 1;

/// Tagged rather than encoded in the status line: if 404 meant "no such key",
/// any proxy or half-deployed replica could turn a dependency failure into a
/// verdict about a customer's key (GAP-34).
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum ExchangeAnswer {
    Granted { grant: Grant },
    Denied { reason: String },
}

/// A short-lived authorization for one credential. Every acted-on field is
/// required, so a truncated answer cannot admit traffic. `datasets` is the
/// exception: absent means unrestricted in this claims version (REQ-53).
#[derive(Debug, Clone, Deserialize)]
pub struct Grant {
    pub claims_version: u32,

    /// Which key the control plane resolved the credential to. Checked against
    /// the id the portal asked about, so an answer about someone else is
    /// discarded rather than acted on.
    pub key_id: String,

    /// `None` means every dataset. Entries are canonical names, matched
    /// exactly — aliases are resolved before matching.
    #[serde(default)]
    pub datasets: Option<Vec<String>>,

    /// Unix seconds. Past this the portal renews, while still serving on the
    /// grant it has.
    pub refresh_after: u64,

    /// Unix seconds. Past this the grant admits nothing, whatever the control
    /// plane's state — the hard bound on how stale an authorization may get.
    pub expires_at: u64,
}

/// Denial reasons this build maps to a specific wire code. Anything else is
/// still a denial — it just cannot be reported more precisely than
/// `invalid_credential`, which is the fail-closed direction.
pub mod denial {
    pub const UNKNOWN_KEY: &str = "unknown_key";
    pub const INVALID_SECRET: &str = "invalid_secret";
    pub const REVOKED: &str = "revoked";
    pub const EXPIRED: &str = "expired";
    pub const PORTAL_NOT_ALLOWED: &str = "portal_not_allowed";
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(value: serde_json::Value) -> serde_json::Result<ExchangeAnswer> {
        serde_json::from_value(value)
    }

    #[test]
    fn a_grant_parses_and_ignores_fields_this_build_does_not_know() {
        let answer = parse(serde_json::json!({
            "result": "granted",
            "grant": {
                "claims_version": 1,
                "key_id": "k1",
                "refresh_after": 1_800_000_300u64,
                "expires_at": 1_800_000_900u64,
                "tier": "gold",
                "limits": {"throughput_bytes_per_sec": 1000},
            },
        }))
        .expect("unknown fields must not break parsing");

        let ExchangeAnswer::Granted { grant } = answer else {
            panic!("expected a grant");
        };
        assert_eq!(grant.key_id, "k1");
        assert_eq!(grant.datasets, None, "absent scope means unrestricted");
    }

    #[test]
    fn a_grant_missing_a_field_the_portal_acts_on_does_not_parse() {
        for missing in ["claims_version", "key_id", "refresh_after", "expires_at"] {
            let mut grant = serde_json::json!({
                "claims_version": 1,
                "key_id": "k1",
                "refresh_after": 1u64,
                "expires_at": 2u64,
            });
            grant.as_object_mut().unwrap().remove(missing);

            let err = parse(serde_json::json!({"result": "granted", "grant": grant}))
                .expect_err("a missing required claim must not parse");
            assert!(err.to_string().contains(missing), "got {err}");
        }
    }

    #[test]
    fn scope_lists_distinguish_absent_from_empty() {
        let with_empty = serde_json::json!({
            "result": "granted",
            "grant": {
                "claims_version": 1,
                "key_id": "k1",
                "datasets": [],
                "refresh_after": 1u64,
                "expires_at": 2u64,
            },
        });
        let ExchangeAnswer::Granted { grant } = parse(with_empty).expect("parse") else {
            panic!("expected a grant");
        };
        assert_eq!(
            grant.datasets,
            Some(Vec::new()),
            "an empty list means no dataset, which is not the same as no list"
        );
    }

    /// A reason string rather than an enum: an unrecognised denial is still a
    /// denial, and refusing to parse one would turn it into a retryable
    /// dependency failure — the fail-*open* direction.
    #[test]
    fn a_denial_carries_its_reason_verbatim() {
        let answer = parse(serde_json::json!({
            "result": "denied",
            "reason": "some_reason_from_a_newer_control_plane",
        }))
        .expect("an unknown denial reason must still parse as a denial");

        let ExchangeAnswer::Denied { reason } = answer else {
            panic!("expected a denial");
        };
        assert_eq!(reason, "some_reason_from_a_newer_control_plane");
    }

    #[test]
    fn an_untagged_or_unknown_answer_does_not_parse() {
        parse(serde_json::json!({"grant": {}})).expect_err("an untagged answer is not an answer");
        parse(serde_json::json!({"result": "maybe"})).expect_err("only two results exist");
    }
}
