//! DC-8 stub: the control-plane credential exchange (IB-7), and DC-9's usage
//! ingest beside it.
//!
//! Verifies the signing contract a real control plane must verify — exactly one
//! of each header, an attributable portal, a timestamp inside the skew window,
//! and an Ed25519 signature over the canonical binding — then answers whatever
//! the test programmed. Verification is real rather than assumed: a stub that
//! trusted the headers would prove nothing about what the portal sends.
//!
//! Everything it was asked about lands in the ledger, which is what lets CT-10
//! assert an exchange did *not* happen (INV-14) and that the presented secret
//! reached exactly one place (INV-38). The usage endpoint keeps every batch it
//! accepted, whole and in order, so CT-11 can assert what was reported, that it
//! arrived batched, and — with the endpoint refusing — that a portal whose sink
//! is down serves exactly what it served when the sink was up.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as BASE64URL, Engine};
use libp2p_identity::PublicKey;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::Ledger;

/// Where this stub mounts the endpoint. The prefix is its own — the portal
/// appends `v1/exchange` to whatever base it is configured with — but the
/// signature covers the whole path, so the two have to agree exactly.
pub const EXCHANGE_PATH: &str = "/authority/v1/auth/exchange";

/// The usage ingest, mounted beside the exchange under the same base — a real
/// control plane routes both from one mount point (DC-9).
pub const USAGE_PATH: &str = "/authority/v1/auth/usage";

/// Both sides reimplement the canonical form, so its bytes are the contract.
/// A drift here is what the scheme tag exists to make loud.
const SCHEME: &str = "sqd-portal-v1";

/// The claim vocabulary the portal understands (`auth::types::CLAIMS_VERSION`).
pub const CLAIMS_VERSION: u32 = 1;

/// P-SIGNATURE-MAX-SKEW.
const MAX_SKEW_SECS: u64 = 300;

/// Token layouts the control plane mints, mirrored so the stub can answer per
/// key id (IB-9).
const TOKEN_PREFIXES: [&str; 2] = ["sqd_portal_", "prt_"];

/// What the control plane says about one credential. The last two are the DC-8
/// error table's fault rows: a status carries no answer at all, and a raw body
/// is how the unreadable answer, the unknown claims version and the answer about
/// somebody else are all expressed.
#[derive(Clone, Debug)]
pub enum Answer {
    Grant {
        /// `None` is an unrestricted grant; the portal reads an absent list and
        /// an empty one differently, so this maps straight onto the wire.
        datasets: Option<Vec<String>>,
        refresh_in: u64,
        expires_in: u64,
    },
    /// An unrestricted grant that also names the key's owner.
    GrantOwned { organization: String },
    Deny(String),
    Status(u16),
    Raw(Value),
}

impl Answer {
    /// Fresh for longer than any test runs, so nothing renews by accident.
    pub fn grant() -> Self {
        Answer::Grant {
            datasets: None,
            refresh_in: 300,
            expires_in: 900,
        }
    }

    pub fn grant_for(datasets: &[&str]) -> Self {
        Answer::Grant {
            datasets: Some(datasets.iter().map(|d| (*d).to_owned()).collect()),
            refresh_in: 300,
            expires_in: 900,
        }
    }

    /// A grant naming the key's owner, as a control plane that carries the
    /// claim does. The portal records it and acts on none of it (REQ-60).
    pub fn grant_owned_by(organization: &str) -> Self {
        Answer::GrantOwned {
            organization: organization.to_owned(),
        }
    }

    /// Lifetimes are relative to the timestamp the portal signed, so the answer
    /// lands on the portal's clock without the harness owning one.
    fn render(&self, key_id: &str, now: u64) -> Response {
        match self {
            Answer::Grant {
                datasets,
                refresh_in,
                expires_in,
            } => Json(json!({
                "result": "granted",
                "grant": {
                    "claims_version": CLAIMS_VERSION,
                    "key_id": key_id,
                    "datasets": datasets,
                    "refresh_after": now + refresh_in,
                    "expires_at": now + expires_in,
                },
            }))
            .into_response(),
            Answer::GrantOwned { organization } => Json(json!({
                "result": "granted",
                "grant": {
                    "claims_version": CLAIMS_VERSION,
                    "key_id": key_id,
                    "organization_id": organization,
                    "refresh_after": now + 300,
                    "expires_at": now + 900,
                },
            }))
            .into_response(),
            Answer::Deny(reason) => {
                Json(json!({ "result": "denied", "reason": reason })).into_response()
            }
            Answer::Status(code) => StatusCode::from_u16(*code)
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
                .into_response(),
            Answer::Raw(value) => Json(value.clone()).into_response(),
        }
    }
}

/// One accepted exchange, whole. The token is kept so a test can prove the
/// secret reached the exchange and nowhere else (INV-38).
#[derive(Clone, Debug)]
pub struct Seen {
    pub key_id: String,
    pub token: String,
    pub portal_id: String,
    pub timestamp: u64,
}

struct Inner {
    portal_id: Mutex<String>,
    public_key: PublicKey,
    answers: Mutex<HashMap<String, Answer>>,
    default_answer: Mutex<Answer>,
    delay: Mutex<Duration>,
    seen: Mutex<Vec<Seen>>,
    /// One entry per accepted delivery, in order, so a test can tell four
    /// records arriving together from four arriving alone (DC-9).
    usage: Mutex<Vec<Vec<Value>>>,
    /// What the usage endpoint answers from now on. `None` accepts.
    usage_status: Mutex<Option<u16>>,
    ledger: Ledger,
}

pub struct ControlPlane {
    pub ledger: Ledger,
    inner: Arc<Inner>,
}

impl ControlPlane {
    /// Exchanges the control plane actually answered — the INV-14 denominator.
    pub fn exchanges(&self) -> usize {
        self.ledger.count_with_prefix("exchange ")
    }

    /// Requests refused before they were an exchange at all: the signing
    /// contract's own failures, each tagged with which clause failed.
    pub fn rejections(&self) -> Vec<String> {
        self.ledger
            .entries()
            .into_iter()
            .filter(|e| e.starts_with("reject "))
            .collect()
    }

    pub fn seen(&self) -> Vec<Seen> {
        self.inner.seen.lock().unwrap().clone()
    }

    /// Every credential presented to the exchange, verbatim.
    pub fn tokens(&self) -> Vec<String> {
        self.seen().into_iter().map(|s| s.token).collect()
    }

    /// What this key id gets from now on.
    pub fn answer(&self, key_id: &str, answer: Answer) {
        self.inner
            .answers
            .lock()
            .unwrap()
            .insert(key_id.to_owned(), answer);
    }

    /// What a key id with no specific answer gets. Starts as a plain grant.
    pub fn default_answer(&self, answer: Answer) {
        *self.inner.default_answer.lock().unwrap() = answer;
    }

    /// Held before answering. Longer than the portal's exchange deadline is the
    /// DC-8 timeout row; shorter is how a burst is made to overlap.
    pub fn set_delay(&self, delay: Duration) {
        *self.inner.delay.lock().unwrap() = delay;
    }

    /// Makes every later exchange unattributable, as a control plane that does
    /// not know this portal would.
    pub fn expect_portal_id(&self, portal_id: &str) {
        *self.inner.portal_id.lock().unwrap() = portal_id.to_owned();
    }

    /// Every accepted delivery, in order — the batching evidence.
    pub fn usage_batches(&self) -> Vec<Vec<Value>> {
        self.inner.usage.lock().unwrap().clone()
    }

    /// Every reported record, flattened.
    pub fn usage_events(&self) -> Vec<Value> {
        self.usage_batches().into_iter().flatten().collect()
    }

    /// The records attributed to one key. Events carry no request id by design
    /// (DC-9), so a test separates traffic by the key that caused it.
    pub fn usage_events_for(&self, key_id: &str) -> Vec<Value> {
        self.usage_events()
            .into_iter()
            .filter(|event| event["key_id"] == key_id)
            .collect()
    }

    /// Takes the usage endpoint down without touching the exchange: the DC-9
    /// outage row, which must be invisible to every client.
    pub fn refuse_usage(&self, status: u16) {
        *self.inner.usage_status.lock().unwrap() = Some(status);
    }

    pub fn accept_usage(&self) {
        *self.inner.usage_status.lock().unwrap() = None;
    }

    /// Deliveries the endpoint was asked for, accepted or refused.
    pub fn usage_attempts(&self) -> usize {
        self.ledger.count_with_prefix("usage ")
    }
}

pub async fn start(
    port: u16,
    portal_id: &str,
    public_key: PublicKey,
) -> anyhow::Result<ControlPlane> {
    let ledger = Ledger::default();
    let inner = Arc::new(Inner {
        portal_id: Mutex::new(portal_id.to_owned()),
        public_key,
        answers: Mutex::new(HashMap::new()),
        default_answer: Mutex::new(Answer::grant()),
        delay: Mutex::new(Duration::ZERO),
        seen: Mutex::new(Vec::new()),
        usage: Mutex::new(Vec::new()),
        usage_status: Mutex::new(None),
        ledger: ledger.clone(),
    });
    let app = Router::new()
        .route(EXCHANGE_PATH, post(exchange))
        .route(USAGE_PATH, post(usage))
        .with_state(inner.clone());
    super::serve(app, port).await?;
    Ok(ControlPlane { ledger, inner })
}

async fn exchange(State(s): State<Arc<Inner>>, headers: HeaderMap, body: Bytes) -> Response {
    let timestamp = match s.verify(EXCHANGE_PATH, &headers, &body) {
        Ok(timestamp) => timestamp,
        Err(why) => {
            s.ledger.push(format!("reject {why}"));
            return (StatusCode::UNAUTHORIZED, Json(json!({ "error": why }))).into_response();
        }
    };

    let token = match serde_json::from_slice::<Value>(&body) {
        Ok(value) => value["credential"].as_str().unwrap_or_default().to_owned(),
        Err(_) => {
            s.ledger.push("reject unparseable-body");
            return StatusCode::BAD_REQUEST.into_response();
        }
    };
    let Some(key_id) = key_id_of(&token) else {
        s.ledger.push("reject ungrammatical-token");
        return StatusCode::BAD_REQUEST.into_response();
    };

    // Ledgered before the answer, so a call the portal abandoned still counts
    // as one it made.
    s.ledger.push(format!("exchange {key_id}"));
    s.seen.lock().unwrap().push(Seen {
        key_id: key_id.clone(),
        token,
        portal_id: s.portal_id.lock().unwrap().clone(),
        timestamp,
    });

    let delay = *s.delay.lock().unwrap();
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }

    let answer = s
        .answers
        .lock()
        .unwrap()
        .get(&key_id)
        .cloned()
        .unwrap_or_else(|| s.default_answer.lock().unwrap().clone());
    answer.render(&key_id, timestamp)
}

/// DC-9's ingest: the same signing contract as the exchange, because it is the
/// signature that says which portal these records belong to — an event carries
/// no portal identity of its own, and a field would be one a portal could forge.
async fn usage(State(s): State<Arc<Inner>>, headers: HeaderMap, body: Bytes) -> Response {
    if let Err(why) = s.verify(USAGE_PATH, &headers, &body) {
        s.ledger.push(format!("usage reject {why}"));
        return (StatusCode::UNAUTHORIZED, Json(json!({ "error": why }))).into_response();
    }
    let events = match serde_json::from_slice::<Value>(&body) {
        Ok(value) => value["events"].as_array().cloned().unwrap_or_default(),
        Err(_) => {
            s.ledger.push("usage reject unparseable-body");
            return StatusCode::BAD_REQUEST.into_response();
        }
    };
    // Ledgered before the verdict: a delivery the stub refuses is still one the
    // portal made, and the outage case is about exactly those.
    s.ledger.push(format!("usage {}", events.len()));

    if let Some(status) = *s.usage_status.lock().unwrap() {
        return StatusCode::from_u16(status)
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
            .into_response();
    }
    s.usage.lock().unwrap().push(events);
    StatusCode::ACCEPTED.into_response()
}

impl Inner {
    /// The DC-8 signing contract, clause by clause, returning the timestamp the
    /// request was signed at. Each failure names its clause so a test can assert
    /// *why* a request was refused. The path is a parameter because the
    /// signature covers it: a signature made for the exchange must not verify
    /// against the usage endpoint, and vice versa.
    fn verify(&self, path: &str, headers: &HeaderMap, body: &[u8]) -> Result<u64, String> {
        let exactly_one = |name: &str| -> Result<String, String> {
            let mut values = headers.get_all(name).iter();
            let first = values.next().ok_or_else(|| format!("missing-{name}"))?;
            if values.next().is_some() {
                return Err(format!("repeated-{name}"));
            }
            first
                .to_str()
                .map(str::to_owned)
                .map_err(|_| format!("malformed-{name}"))
        };

        let portal_id = exactly_one("x-portal-id")?;
        let timestamp = exactly_one("x-signature-timestamp")?;
        let signature = exactly_one("x-signature")?;

        if portal_id != *self.portal_id.lock().unwrap() {
            return Err("unattributable-portal".into());
        }
        let timestamp: u64 = timestamp
            .parse()
            .map_err(|_| "malformed-timestamp".to_owned())?;
        // Both directions: a portal running fast is as unverifiable as one
        // running slow, and only one of the two is a replay.
        if unix_now().abs_diff(timestamp) > MAX_SKEW_SECS {
            return Err("skewed-timestamp".into());
        }

        let signature = BASE64URL
            .decode(&signature)
            .map_err(|_| "malformed-signature".to_owned())?;
        let payload = canonical(&portal_id, timestamp, "POST", path, body);
        if !self.public_key.verify(payload.as_bytes(), &signature) {
            return Err("bad-signature".into());
        }
        Ok(timestamp)
    }
}

/// The canonical binding of `auth::signing`, reimplemented rather than
/// imported: a stub sharing the portal's code could not catch the portal
/// changing it.
fn canonical(portal_id: &str, timestamp: u64, method: &str, path: &str, body: &[u8]) -> String {
    format!(
        "{SCHEME}\n{portal_id}\n{timestamp}\n{method}\n{path}\n{}",
        hex::encode(Sha256::digest(body))
    )
}

fn key_id_of(token: &str) -> Option<String> {
    let rest = TOKEN_PREFIXES
        .iter()
        .find_map(|prefix| token.strip_prefix(prefix))?;
    let (key_id, secret) = rest.split_once('_')?;
    (!key_id.is_empty() && !secret.is_empty()).then(|| key_id.to_owned())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
