//! Portal-side half of commercial access control: the portal mirrors the
//! control plane's key set and answers, per request, whether the presented key
//! may be served here. Phase 1 is authentication plus coarse portal/dataset
//! scoping only — an admitted key streams unrestricted.
//!
//! The whole module is inert unless `commercial:` is present in the config.

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio_util::sync::CancellationToken;

mod client;
mod config;
mod evaluate;
mod extractor;
mod routes;
mod store;
mod types;

pub use config::{CommercialConfig, Enforcement, GatedRoutes};
pub use extractor::{middleware, DatasetCatalog, DatasetSource, Gate, RouteClass};
#[cfg(test)]
pub use routes::{classify as classify_for_test, is_classified as is_classified_for_test, Gating};

use store::SnapshotStore;

/// Starts the snapshot sync loop and returns the gate the router wraps its
/// data endpoints in. The loop stops when `cancel` fires.
pub fn build(
    config: &CommercialConfig,
    catalog: Arc<dyn DatasetCatalog>,
    cancel: CancellationToken,
) -> anyhow::Result<Arc<Gate>> {
    let store = SnapshotStore::new(config)?;
    store.spawn_sync(cancel);
    // The mode itself is logged by `log_authorization_mode` at startup, which
    // covers the disabled case too — this path only exists when it is on.
    Ok(Arc::new(Gate::new(config, store, catalog)))
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
pub mod test_support {
    use std::{
        collections::{HashMap, VecDeque},
        net::SocketAddr,
        sync::{Mutex, MutexGuard, OnceLock},
    };

    use axum::{
        extract::{Query, State},
        http::{HeaderMap, StatusCode},
        routing::{get, post},
        Json, Router,
    };
    use serde::Deserialize;

    use super::*;
    use crate::commercial::types::{KeyRecord, KeyStatus};

    pub const SECRET: &str = "theverysecretvalue";
    pub const SECRET_SHA256: &str =
        "ef84baff4bd11c9ed15cbaa2aa670ef4d02b6668ed7e98c5ff6d3838412640ef";
    const SERVICE_TOKEN: &str = "test-service-token";
    const TEST_TOKEN_ENV: &str = "SQD_COMMERCIAL_TEST_TOKEN";

    /// Serializes the tests that read or write process-wide environment.
    pub fn env_guard() -> MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        LOCK.lock().unwrap_or_else(|err| err.into_inner())
    }

    fn service_token_env() -> String {
        static INIT: OnceLock<()> = OnceLock::new();
        INIT.get_or_init(|| {
            let _guard = env_guard();
            std::env::set_var(TEST_TOKEN_ENV, SERVICE_TOKEN);
        });
        TEST_TOKEN_ENV.to_string()
    }

    pub fn key_record(key_id: &str, seq: u64) -> KeyRecord {
        KeyRecord {
            key_id: key_id.to_string(),
            organization_id: Some("11111111-1111-1111-1111-111111111111".to_string()),
            status: KeyStatus::Active,
            seq,
            secret_sha256: Some(SECRET_SHA256.to_string()),
            portal_ids: None,
            datasets: None,
            expires_at: None,
        }
    }

    /// A well-formed feed page: the control plane sends all four envelope
    /// fields on every page, and so does this.
    pub fn page(
        records: Vec<KeyRecord>,
        next_cursor: u64,
        epoch: &str,
        head_seq: u64,
    ) -> serde_json::Value {
        serde_json::json!({
            "records": records,
            "next_cursor": next_cursor,
            "epoch": epoch,
            "head_seq": head_seq,
        })
    }

    /// A config pointing at a port nothing listens on, so a store built from it
    /// never reaches a control plane.
    pub fn offline_config() -> CommercialConfig {
        CommercialConfig {
            control_plane_url: "http://127.0.0.1:1/".parse().unwrap(),
            service_token_env: service_token_env(),
            portal_id: "portal-premium-eu".to_string(),
            enforcement: Enforcement::Enforce,
            gated_routes: GatedRoutes::Data,
            sync_interval_secs: 10,
        }
    }

    /// A store preloaded with `records`, backed by a control plane that knows
    /// nothing else. A miss therefore resolves to the authoritative "no such
    /// key" — which is a different outcome from a portal that could not reach
    /// anyone, and tests of the ladder want the former (REQ-54).
    pub async fn store_with(records: Vec<KeyRecord>) -> Arc<SnapshotStore> {
        let control_plane = MockControlPlane::spawn().await;
        let store = SnapshotStore::new(&control_plane.config()).expect("store should build");
        store.install_for_test(records);
        store
    }

    /// A store whose control plane is unreachable, preloaded with `records`.
    pub fn offline_store(records: Vec<KeyRecord>) -> Arc<SnapshotStore> {
        let store = SnapshotStore::new(&offline_config()).expect("store should build");
        store.install_for_test(records);
        store
    }

    struct NoCatalog;

    impl DatasetCatalog for NoCatalog {
        fn canonical_name(&self, _alias: &str) -> Option<String> {
            None
        }

        fn canonical_name_for_id(&self, _id: &crate::types::DatasetId) -> Option<String> {
            None
        }
    }

    /// A gate whose snapshot store has synced, or one that never has.
    pub fn gate_with_readiness(ready: bool) -> Arc<Gate> {
        gate_with(Enforcement::Enforce, ready)
    }

    pub fn gate_with(enforcement: Enforcement, ready: bool) -> Arc<Gate> {
        let config = CommercialConfig {
            enforcement,
            ..offline_config()
        };
        let store = SnapshotStore::new(&config).expect("store should build");
        if ready {
            store.install_for_test(Vec::new());
        }
        Arc::new(Gate::new(&config, store, Arc::new(NoCatalog)))
    }

    #[derive(Clone)]
    pub struct MockControlPlane {
        addr: SocketAddr,
        state: Arc<MockState>,
    }

    /// The largest page the control plane will serve. Asking for more is a
    /// 400, never a smaller page: a clamped page reads as short, and a short
    /// page is how the reader knows it has reached the end of the feed.
    pub const MAX_PAGE_LIMIT: u16 = 1000;

    #[derive(Default)]
    struct MockState {
        pages: Mutex<VecDeque<(u64, serde_json::Value)>>,
        /// Epoch of the last queued page served, so the empty pages this
        /// answers with once the queue drains do not read as a feed rebuild.
        epoch: Mutex<Option<String>>,
        snapshot_cursors: Mutex<Vec<u64>>,
        snapshot_limits: Mutex<Vec<u16>>,
        fail_snapshots: Mutex<bool>,
        authorize: Mutex<HashMap<String, serde_json::Value>>,
        authorize_statuses: Mutex<HashMap<String, u16>>,
        authorize_calls: Mutex<Vec<String>>,
    }

    #[derive(Deserialize)]
    struct CursorQuery {
        cursor: u64,
        limit: Option<u16>,
    }

    impl MockControlPlane {
        pub async fn spawn() -> Self {
            let state = Arc::new(MockState::default());
            let app = Router::new()
                .route("/internal/portal/v1/snapshots", get(snapshots))
                .route("/internal/portal/v1/authorize", post(authorize))
                .with_state(state.clone());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            Self { addr, state }
        }

        pub fn config(&self) -> CommercialConfig {
            CommercialConfig {
                control_plane_url: format!("http://{}", self.addr).parse().unwrap(),
                service_token_env: service_token_env(),
                portal_id: "portal-premium-eu".to_string(),
                enforcement: Enforcement::Enforce,
                gated_routes: GatedRoutes::Data,
                sync_interval_secs: 10,
            }
        }

        /// Queues a page served to the first poll whose cursor is at least
        /// `min_cursor`. Unqueued polls answer with an empty page.
        pub fn push_page(&self, min_cursor: u64, page: serde_json::Value) {
            self.state
                .pages
                .lock()
                .unwrap()
                .push_back((min_cursor, page));
        }

        pub fn snapshot_cursors(&self) -> Vec<u64> {
            self.state.snapshot_cursors.lock().unwrap().clone()
        }

        /// The page size each poll asked for.
        pub fn snapshot_limits(&self) -> Vec<u16> {
            self.state.snapshot_limits.lock().unwrap().clone()
        }

        pub fn fail_snapshots(&self, fail: bool) {
            *self.state.fail_snapshots.lock().unwrap() = fail;
        }

        pub fn authorize_with(&self, key_id: &str, record: Option<KeyRecord>) {
            let Some(record) = record else {
                self.state.authorize.lock().unwrap().remove(key_id);
                return;
            };
            self.authorize_raw(key_id, serde_json::to_value(record).unwrap());
        }

        /// An answer the `KeyRecord` type cannot express, such as a status this
        /// build predates.
        pub fn authorize_raw(&self, key_id: &str, body: serde_json::Value) {
            self.state
                .authorize
                .lock()
                .unwrap()
                .insert(key_id.to_string(), body);
        }

        pub fn authorize_status(&self, key_id: &str, status: u16) {
            self.state
                .authorize_statuses
                .lock()
                .unwrap()
                .insert(key_id.to_string(), status);
        }

        pub fn authorize_calls(&self) -> Vec<String> {
            self.state.authorize_calls.lock().unwrap().clone()
        }
    }

    fn authorized(headers: &HeaderMap) -> bool {
        headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            == Some(&format!("Bearer {SERVICE_TOKEN}"))
    }

    async fn snapshots(
        State(state): State<Arc<MockState>>,
        headers: HeaderMap,
        Query(query): Query<CursorQuery>,
    ) -> Result<Json<serde_json::Value>, StatusCode> {
        if !authorized(&headers) {
            return Err(StatusCode::UNAUTHORIZED);
        }
        state.snapshot_cursors.lock().unwrap().push(query.cursor);
        let limit = query.limit.unwrap_or(MAX_PAGE_LIMIT);
        state.snapshot_limits.lock().unwrap().push(limit);
        if limit > MAX_PAGE_LIMIT {
            return Err(StatusCode::BAD_REQUEST);
        }
        if *state.fail_snapshots.lock().unwrap() {
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
        let mut pages = state.pages.lock().unwrap();
        let index = pages
            .iter()
            .position(|(min_cursor, _)| query.cursor >= *min_cursor);
        let Some(mut page) = index
            .and_then(|index| pages.remove(index))
            .map(|(_, page)| page)
        else {
            let epoch = state.epoch.lock().unwrap().clone();
            return Ok(Json(serde_json::json!({
                "records": [],
                "next_cursor": query.cursor,
                "epoch": epoch.as_deref().unwrap_or("e1"),
                "head_seq": query.cursor,
            })));
        };
        if let Some(epoch) = page.get("epoch").and_then(serde_json::Value::as_str) {
            *state.epoch.lock().unwrap() = Some(epoch.to_owned());
        }
        // No page is ever longer than the one that was asked for.
        if let Some(records) = page.get_mut("records").and_then(|r| r.as_array_mut()) {
            records.truncate(usize::from(limit));
        }
        Ok(Json(page))
    }

    async fn authorize(
        State(state): State<Arc<MockState>>,
        headers: HeaderMap,
        Json(body): Json<serde_json::Value>,
    ) -> Result<Json<serde_json::Value>, StatusCode> {
        if !authorized(&headers) {
            return Err(StatusCode::UNAUTHORIZED);
        }
        let key_id = body
            .get("key_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string();
        state.authorize_calls.lock().unwrap().push(key_id.clone());

        if let Some(status) = state.authorize_statuses.lock().unwrap().get(&key_id) {
            return Err(StatusCode::from_u16(*status).unwrap());
        }
        state
            .authorize
            .lock()
            .unwrap()
            .get(&key_id)
            .cloned()
            .map(Json)
            .ok_or(StatusCode::NOT_FOUND)
    }
}
