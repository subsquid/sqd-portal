//! DC-5 stub: the contract client's dummy-data file. Both the portal and the
//! stub worker load it via `DUMMY_CLIENT_FILE_PATH`, which also seeds the p2p
//! whitelist on both sides and satisfies `build_worker`'s registration gate.

use libp2p_identity::PeerId;
use serde_json::{json, Value};

pub fn dummy_data_json(workers: &[PeerId], portal: PeerId) -> String {
    let portal = portal.to_string();
    let worker_entries: Vec<Value> = workers
        .iter()
        .enumerate()
        .map(|(i, w)| {
            json!({
                "peer_id": w.to_string(),
                "onchain_id": (i + 1).to_string(),
                "address": format!("0x{:040x}", i + 1),
                "bond": "100000000000000000000",
                // Far in the past so the stub workers' registration wait is a no-op.
                "registered_at": 1_690_000_000u64,
                "deregistered_at": null,
            })
        })
        .collect();
    let worker_ids: serde_json::Map<String, Value> = workers
        .iter()
        .enumerate()
        .map(|(i, w)| (w.to_string(), json!((i + 1).to_string())))
        .collect();
    serde_json::to_string_pretty(&json!({
        "current_epoch": 100,
        "current_epoch_start": 1_700_000_000u64,
        "epoch_length_secs": 3600,
        "workers": worker_entries,
        "portals": [portal.clone()],
        "portal_clusters": [],
        "worker_ids": worker_ids,
        "portal_compute_units": { portal.clone(): 1_000_000u64 },
        "portal_uses_default_strategy": { portal.clone(): true },
        "portal_sqd_locked": {
            portal: ["0x0000000000000000000000000000000000000002", 100000.0]
        },
    }))
    .expect("dummy data serializes")
}
