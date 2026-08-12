//! DC-2 stub: serves the network-state document and the assignment artifact.

use axum::{extract::State, response::IntoResponse, routing::get, Router};
use serde_json::json;

use super::Ledger;

#[derive(Clone)]
struct PublisherState {
    network_state: String,
    artifact_gz: Vec<u8>,
    portal_artifact_gz: Vec<u8>,
    ledger: Ledger,
}

/// Publishes both artifacts, which is the state the scheduler holds throughout the migration —
/// so which one the portal reads is decided by its config alone, never by what is on offer.
pub fn network_state_json(port: u16, assignment_id: &str, effective_from: u64) -> String {
    json!({
        "network": "tethys",
        "assignment": {
            "id": assignment_id,
            "effective_from": effective_from,
            "fb_url_v1": format!("http://127.0.0.1:{port}/assignment.fb.gz"),
        },
        "portal_assignment": {
            "id": assignment_id,
            "effective_from": effective_from,
            "fb_url_v1": format!("http://127.0.0.1:{port}/portal-assignment.fb.gz"),
        }
    })
    .to_string()
}

pub async fn start(
    port: u16,
    network_state: String,
    artifact_gz: Vec<u8>,
    portal_artifact_gz: Vec<u8>,
) -> anyhow::Result<Ledger> {
    let ledger = Ledger::default();
    let state = PublisherState {
        network_state,
        artifact_gz,
        portal_artifact_gz,
        ledger: ledger.clone(),
    };
    let app = Router::new()
        .route(
            "/network-state-tethys.json",
            get(|State(s): State<PublisherState>| async move {
                s.ledger.push("network-state");
                (
                    [("content-type", "application/json")],
                    s.network_state.clone(),
                )
            }),
        )
        .route(
            "/assignment.fb.gz",
            get(|State(s): State<PublisherState>| async move {
                s.ledger.push("artifact");
                s.artifact_gz.clone().into_response()
            }),
        )
        .route(
            // Prefixed "artifact" so a fetch of either format satisfies the same ledger check.
            "/portal-assignment.fb.gz",
            get(|State(s): State<PublisherState>| async move {
                s.ledger.push("artifact-portal");
                s.portal_artifact_gz.clone().into_response()
            }),
        )
        .with_state(state);
    super::serve(app, port).await?;
    Ok(ledger)
}
