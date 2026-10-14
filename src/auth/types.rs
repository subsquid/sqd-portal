use serde::Deserialize;
use serde_json::{Map, Value};

/// The claim vocabularies this build understands, offered in every exchange
/// (REQ-57). Any other version is unusable rather than partly usable: reading a
/// newer one for the fields it recognises is how an added restriction becomes a
/// granted permission (DC-8, DEF-17).
pub const CLAIMS_VERSIONS: [u32; 2] = [1, 2];

/// The version that carries the usage claim (ADR-018).
const USAGE_CLAIMS_VERSION: u32 = 2;

/// P-PACING-MIN-RATE. A lower rate is a units error rather than a floor: any
/// plan rate sent in KB/s instead of bytes reads below it (ADR-018).
pub const MIN_PACING_RATE: u64 = 65_536;

/// Tagged rather than encoded in the status line: if 404 meant "no such key",
/// a proxy could turn a dependency failure into a verdict about a key
/// (GAP-34).
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum ExchangeAnswer {
    Granted { grant: Grant },
    Denied { reason: String },
}

/// A short-lived authorization for one credential. Every acted-on field is
/// required, so a truncated answer cannot admit traffic — except `datasets`,
/// where absent means unrestricted in this claims version (REQ-53).
#[derive(Debug, Clone, Deserialize)]
pub struct Grant {
    pub claims_version: u32,

    /// Checked against the id the portal asked about, so an answer about
    /// someone else is discarded rather than acted on.
    pub key_id: String,

    /// `None` means every dataset. Entries are canonical names, matched
    /// exactly — aliases are resolved before matching.
    #[serde(default)]
    pub datasets: Option<Vec<String>>,

    /// Who the key belongs to, recorded on usage events and never read by the
    /// ladder (REQ-60). Optional because a control plane that predates the
    /// claim omits it, and because a portal that acted on it would be enforcing
    /// on a field this vocabulary does not authorize anything with — which is
    /// also why adding it was not a claims-version bump: recording a claim is
    /// not acting on it (DC-8, DEF-17).
    #[serde(default, deserialize_with = "organization_id")]
    pub organization_id: Option<String>,

    /// Unix seconds. Past this the portal renews, still serving meanwhile.
    pub refresh_after: u64,

    /// Unix seconds. Past this the grant admits nothing, whatever the control
    /// plane's state.
    pub expires_at: u64,

    /// Kept raw: a bad usage claim decides speed, not access, so it must not
    /// fail the grant the way a bad access claim does (REQ-70). Read through
    /// [`Grant::usage_claim`].
    #[serde(default)]
    pub usage: Option<Value>,
}

impl Grant {
    /// The usage claim as the pod acts on it. `Ok(None)` on a version without
    /// one; `Err` names the first fault of a version-2 claim that is read as
    /// absent, which the caller counts (OB-16).
    pub fn usage_claim(&self) -> Result<Option<UsageClaim>, &'static str> {
        if self.claims_version != USAGE_CLAIMS_VERSION {
            return Ok(None);
        }
        let Some(Value::Object(claim)) = &self.usage else {
            return Err("usage");
        };
        UsageClaim::read(claim).map(Some)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageState {
    Within,
    Over,
    Unmetered,
}

impl UsageState {
    /// The wire spelling, which is also the metric label and the header value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Within => "within",
            Self::Over => "over",
            Self::Unmetered => "unmetered",
        }
    }
}

/// Grant v2's usage claim (DEF-17). The pod paces with the rate, reads the
/// state only to cap read-ahead, and copies the rest into headers; it derives
/// nothing (ADR-018). Times are unix seconds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageClaim {
    pub state: UsageState,
    /// `None` is unpaced.
    pub stream_bytes_per_sec: Option<u64>,
    /// `None` is no floor.
    pub floor_bytes_per_sec: Option<u64>,
    /// `None` is uncapped.
    pub allowance_bytes: Option<u64>,
    pub used_bytes: u64,
    pub period_end: u64,
    /// `None` until the control plane has computed the period's usage.
    pub as_of: Option<u64>,
}

impl UsageClaim {
    /// Every field is required and typed; a nullable one may be null but not
    /// absent. Any fault discards the whole claim (ADR-018).
    fn read(claim: &Map<String, Value>) -> Result<Self, &'static str> {
        let field = |name: &'static str| claim.get(name).ok_or(name);
        let count = |name: &'static str| field(name)?.as_u64().ok_or(name);
        let nullable = |name: &'static str| match field(name)? {
            Value::Null => Ok(None),
            value => value.as_u64().map(Some).ok_or(name),
        };
        let state = match field("state")?.as_str() {
            Some("within") => UsageState::Within,
            Some("over") => UsageState::Over,
            Some("unmetered") => UsageState::Unmetered,
            _ => return Err("state"),
        };
        let stream_bytes_per_sec = nullable("stream_bytes_per_sec")?;
        if stream_bytes_per_sec.is_some_and(|rate| rate < MIN_PACING_RATE) {
            return Err("stream_bytes_per_sec");
        }
        Ok(Self {
            state,
            stream_bytes_per_sec,
            floor_bytes_per_sec: nullable("floor_bytes_per_sec")?,
            allowance_bytes: nullable("allowance_bytes")?,
            used_bytes: count("used_bytes")?,
            period_end: count("period_end")?,
            as_of: nullable("as_of")?,
        })
    }
}

// Attribution is optional metadata. An unexpected shape must not invalidate
// the authorization claims, including when usage measurement is disabled.
//
// Logged because the silent path is the dangerous one: a control plane that
// regresses this field's shape would otherwise strip the organization from
// every record with nothing to read it off. The type only — the value is the
// control plane's payload and does not belong in a log (INV-38).
fn organization_id<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    Ok(match serde_json::Value::deserialize(deserializer)? {
        serde_json::Value::String(id) => Some(id),
        serde_json::Value::Null => None,
        other => {
            let kind = match other {
                serde_json::Value::Bool(_) => "bool",
                serde_json::Value::Number(_) => "number",
                serde_json::Value::Array(_) => "array",
                serde_json::Value::Object(_) => "object",
                // `String` and `Null` are answered above.
                _ => "unknown",
            };
            tracing::warn!(
                kind,
                "grant carried a non-string organization_id; recording it as absent"
            );
            None
        }
    })
}

/// Denial reasons this build maps to a specific wire code. Anything else is
/// still a denial, just reported no more precisely than `invalid_credential`.
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

    /// The claim the commercial control plane added (#642). A portal talking to
    /// one that predates it reads `None` and reports events without an owner —
    /// the read-time join on the key id is the fallback, and refusing to parse
    /// the grant would turn a missing attribution field into an outage.
    #[test]
    fn an_organization_is_recorded_when_the_control_plane_names_one() {
        let grant = |value: serde_json::Value| {
            let mut claims = serde_json::json!({
                "claims_version": 1,
                "key_id": "k1",
                "refresh_after": 1u64,
                "expires_at": 2u64,
            });
            claims["organization_id"] = value;
            let ExchangeAnswer::Granted { grant } =
                parse(serde_json::json!({"result": "granted", "grant": claims}))
                    .expect("the grant parses")
            else {
                panic!("expected a grant");
            };
            grant.organization_id
        };

        assert_eq!(grant(serde_json::json!("org-7")), Some("org-7".to_owned()));
        assert_eq!(
            grant(serde_json::Value::Null),
            None,
            "no organization was claimed"
        );
        for value in [
            serde_json::json!(7),
            serde_json::json!(true),
            serde_json::json!(["org-7"]),
            serde_json::json!({"id": "org-7"}),
        ] {
            assert_eq!(
                grant(value),
                None,
                "malformed attribution must not reject a grant"
            );
        }
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

    /// The claim portal-api issues (saas-rest-api `credential-exchange.service.ts`).
    fn usage() -> Value {
        serde_json::json!({
            "state": "over",
            "stream_bytes_per_sec": 500_000,
            "floor_bytes_per_sec": 500_000,
            "allowance_bytes": 400_000_000_000u64,
            "used_bytes": 400_000_000_000u64,
            "period_end": 1_800_000_000u64,
            "as_of": 1_799_000_000u64,
        })
    }

    fn grant_v(claims_version: u32, usage: Option<Value>) -> Grant {
        let mut grant = serde_json::json!({
            "claims_version": claims_version,
            "key_id": "k1",
            "refresh_after": 1u64,
            "expires_at": 2u64,
        });
        if let Some(usage) = usage {
            grant["usage"] = usage;
        }
        let ExchangeAnswer::Granted { grant } =
            parse(serde_json::json!({"result": "granted", "grant": grant})).expect("parse")
        else {
            panic!("expected a grant");
        };
        grant
    }

    #[test]
    fn a_v2_usage_claim_is_read_whole() {
        assert_eq!(
            grant_v(2, Some(usage())).usage_claim(),
            Ok(Some(UsageClaim {
                state: UsageState::Over,
                stream_bytes_per_sec: Some(500_000),
                floor_bytes_per_sec: Some(500_000),
                allowance_bytes: Some(400_000_000_000),
                used_bytes: 400_000_000_000,
                period_end: 1_800_000_000,
                as_of: Some(1_799_000_000),
            }))
        );

        let mut unmetered = usage();
        for nullable in [
            "stream_bytes_per_sec",
            "floor_bytes_per_sec",
            "allowance_bytes",
            "as_of",
        ] {
            unmetered[nullable] = Value::Null;
        }
        unmetered["state"] = "unmetered".into();
        let claim = grant_v(2, Some(unmetered)).usage_claim().unwrap().unwrap();
        assert_eq!(claim.state, UsageState::Unmetered);
        assert_eq!(claim.stream_bytes_per_sec, None, "null is unpaced");
    }

    /// A v1 grant is never paced, whatever it carries.
    #[test]
    fn a_v1_grant_has_no_usage_claim() {
        assert_eq!(grant_v(1, Some(usage())).usage_claim(), Ok(None));
        assert_eq!(grant_v(1, None).usage_claim(), Ok(None));
    }

    /// Each fault discards the whole claim and names itself (REQ-70).
    #[test]
    fn a_bad_usage_claim_is_read_as_absent() {
        let with = |field: &str, value: Option<Value>| {
            let mut claim = usage();
            match value {
                Some(value) => claim[field] = value,
                None => {
                    claim.as_object_mut().unwrap().remove(field);
                }
            }
            claim
        };
        let cases: Vec<(&str, Option<Value>, &str)> = vec![
            ("missing", None, "usage"),
            ("not an object", Some(serde_json::json!([1])), "usage"),
            ("null", Some(Value::Null), "usage"),
            ("state absent", Some(with("state", None)), "state"),
            (
                "state unknown",
                Some(with("state", Some("slowed".into()))),
                "state",
            ),
            (
                "rate absent",
                Some(with("stream_bytes_per_sec", None)),
                "stream_bytes_per_sec",
            ),
            (
                "rate negative",
                Some(with("stream_bytes_per_sec", Some((-1).into()))),
                "stream_bytes_per_sec",
            ),
            (
                "rate fractional",
                Some(with("stream_bytes_per_sec", Some(500_000.5.into()))),
                "stream_bytes_per_sec",
            ),
            (
                "rate a string",
                Some(with("stream_bytes_per_sec", Some("500000".into()))),
                "stream_bytes_per_sec",
            ),
            (
                "rate zero",
                Some(with("stream_bytes_per_sec", Some(0.into()))),
                "stream_bytes_per_sec",
            ),
            (
                "rate below the minimum",
                Some(with(
                    "stream_bytes_per_sec",
                    Some((MIN_PACING_RATE - 1).into()),
                )),
                "stream_bytes_per_sec",
            ),
            (
                "floor absent",
                Some(with("floor_bytes_per_sec", None)),
                "floor_bytes_per_sec",
            ),
            (
                "allowance negative",
                Some(with("allowance_bytes", Some((-5).into()))),
                "allowance_bytes",
            ),
            (
                "used null",
                Some(with("used_bytes", Some(Value::Null))),
                "used_bytes",
            ),
            (
                "period end null",
                Some(with("period_end", Some(Value::Null))),
                "period_end",
            ),
            ("as of absent", Some(with("as_of", None)), "as_of"),
        ];
        for (case, claim, fault) in cases {
            assert_eq!(grant_v(2, claim).usage_claim(), Err(fault), "{case}");
        }

        // The minimum applies to the rate only; the floor is copied.
        let mut low_floor = usage();
        low_floor["floor_bytes_per_sec"] = 1.into();
        low_floor["stream_bytes_per_sec"] = MIN_PACING_RATE.into();
        assert!(grant_v(2, Some(low_floor)).usage_claim().is_ok());
    }

    #[test]
    fn an_untagged_or_unknown_answer_does_not_parse() {
        parse(serde_json::json!({"grant": {}})).expect_err("an untagged answer is not an answer");
        parse(serde_json::json!({"result": "maybe"})).expect_err("only two results exist");
    }
}
