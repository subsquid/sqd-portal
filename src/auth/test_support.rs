//! Scaffolding shared by the module's unit tests: a control plane that answers
//! whatever a test told it to, and the constructors that wire a cache or a gate
//! to it exactly as `auth::build` does.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex, MutexGuard,
    },
    time::Duration,
};

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
    Json, Router,
};

use super::{extractor::Credential, *};

pub const KEY_ID: &str = "k1";
pub const SECRET: &str = "theverysecretvalue";
pub const TOKEN: &str = "sqd_portal_k1_theverysecretvalue";

/// Serializes the tests that read or write process-wide environment.
pub fn env_guard() -> MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|err| err.into_inner())
}

pub fn credential() -> Credential {
    super::extractor::parse_token_for_test(TOKEN).expect("the test token parses")
}

struct NoCatalog;

impl DatasetCatalog for NoCatalog {
    fn canonical_name(&self, _alias: &str) -> Option<String> {
        None
    }
}

/// A gate whose control plane is a port nothing listens on. Enough for the
/// tests that ask whether a route is wrapped at all; the ones about the
/// ladder want [`MockControlPlane`].
pub fn gate_with(enforcement: Enforcement) -> Arc<Gate> {
    let config = ResolvedAuth {
        control_plane_url: "http://127.0.0.1:1/authority".parse().unwrap(),
        portal_id: "portal-premium-eu".to_string(),
        key: None,
        enforcement,
        limits: config::Limits::default(),
    };
    let signer = config.signer(Keypair::generate_ed25519()).unwrap();
    let cache = GrantCache::new(
        ControlPlaneClient::new(&config, signer).unwrap(),
        config.limits.clone(),
        config.enforcement,
    );
    Arc::new(Gate::new(&config, cache, Arc::new(NoCatalog)))
}

pub async fn cache_for(control_plane: &MockControlPlane) -> Arc<GrantCache> {
    cache_with(control_plane, Enforcement::Enforce).await
}

pub async fn cache_with(
    control_plane: &MockControlPlane,
    enforcement: Enforcement,
) -> Arc<GrantCache> {
    let config = ResolvedAuth {
        enforcement,
        ..control_plane.config()
    };
    let signer = config.signer(Keypair::generate_ed25519()).unwrap();
    GrantCache::new(
        ControlPlaneClient::new(&config, signer).expect("client should build"),
        config.limits.clone(),
        config.enforcement,
    )
}

/// A control plane that answers whatever a test told it to, and remembers
/// what it was asked. Its default answer is a failure rather than a denial:
/// nothing should silently read "we never configured this key" as "the
/// authority says no".
pub struct MockControlPlane {
    addr: SocketAddr,
    state: Arc<MockState>,
}

#[derive(Default)]
struct MockState {
    answers: Mutex<HashMap<String, serde_json::Value>>,
    statuses: Mutex<HashMap<String, u16>>,
    exchanges: AtomicUsize,
    signatures: Mutex<Vec<(String, String, String)>>,
    stopped: AtomicBool,
    delay_ms: AtomicUsize,
}

impl MockControlPlane {
    pub async fn spawn() -> Self {
        let state = Arc::new(MockState::default());
        let app = Router::new()
            .route("/authority/v1/auth/exchange", post(exchange))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self { addr, state }
    }

    pub fn config(&self) -> ResolvedAuth {
        let limits = config::Limits {
            // Small enough that an eviction test does not have to fill 65k
            // entries to prove the bound exists.
            grant_cache_capacity: 32,
            ..config::Limits::default()
        };
        ResolvedAuth {
            control_plane_url: format!("http://{}/authority", self.addr).parse().unwrap(),
            portal_id: "portal-premium-eu".to_string(),
            key: None,
            enforcement: Enforcement::Enforce,
            limits,
        }
    }

    pub fn grant(
        &self,
        key_id: &str,
        datasets: Option<Vec<String>>,
        refresh_after: u64,
        expires_at: u64,
    ) {
        self.raw(
            key_id,
            serde_json::json!({
                "result": "granted",
                "grant": {
                    "claims_version": types::CLAIMS_VERSION,
                    "key_id": key_id,
                    "datasets": datasets,
                    "refresh_after": refresh_after,
                    "expires_at": expires_at,
                },
            }),
        );
    }

    pub fn deny(&self, key_id: &str, reason: &str) {
        self.raw(
            key_id,
            serde_json::json!({"result": "denied", "reason": reason}),
        );
    }

    /// An answer the typed constructors cannot express.
    pub fn raw(&self, key_id: &str, body: serde_json::Value) {
        self.state
            .answers
            .lock()
            .unwrap()
            .insert(key_id.to_string(), body);
    }

    pub fn status(&self, key_id: &str, status: u16) {
        self.state
            .statuses
            .lock()
            .unwrap()
            .insert(key_id.to_string(), status);
    }

    pub fn clear_status(&self, key_id: &str) {
        self.state.statuses.lock().unwrap().remove(key_id);
    }

    /// Refuses every exchange from here on, the way an unreachable control
    /// plane does — but without waiting out a connect timeout.
    pub fn stop(&self) {
        self.state.stopped.store(true, Ordering::SeqCst);
    }

    pub fn delay(&self, delay: Duration) {
        self.state
            .delay_ms
            .store(delay.as_millis() as usize, Ordering::SeqCst);
    }

    pub fn exchanges(&self) -> usize {
        self.state.exchanges.load(Ordering::SeqCst)
    }

    /// `(portal_id, timestamp, signature)` per exchange, in order.
    pub fn signatures(&self) -> Vec<(String, String, String)> {
        self.state.signatures.lock().unwrap().clone()
    }
}

async fn exchange(
    State(state): State<Arc<MockState>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    state.exchanges.fetch_add(1, Ordering::SeqCst);

    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned()
    };
    state.signatures.lock().unwrap().push((
        header(signing::PORTAL_ID_HEADER),
        header(signing::TIMESTAMP_HEADER),
        header(signing::SIGNATURE_HEADER),
    ));
    if header(signing::SIGNATURE_HEADER).is_empty() {
        return Err(StatusCode::UNAUTHORIZED);
    }
    if state.stopped.load(Ordering::SeqCst) {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    let delay = state.delay_ms.load(Ordering::SeqCst);
    if delay > 0 {
        tokio::time::sleep(Duration::from_millis(delay as u64)).await;
    }

    // The mock speaks the portal's own token grammar so a test can drive it
    // with a real credential rather than a key id.
    let key_id = body
        .get("credential")
        .and_then(serde_json::Value::as_str)
        .and_then(|token| token.strip_prefix("sqd_portal_"))
        .and_then(|rest| rest.split_once('_'))
        .map(|(key_id, _)| key_id.to_owned())
        .unwrap_or_default();

    if let Some(status) = state.statuses.lock().unwrap().get(&key_id) {
        return Err(StatusCode::from_u16(*status).unwrap());
    }
    state
        .answers
        .lock()
        .unwrap()
        .get(&key_id)
        .cloned()
        .map(Json)
        // Not a denial: a control plane that was never told about this key
        // has not said anything about it.
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)
}
