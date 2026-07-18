//! IB-7 dependency stubs. Each records every interaction in a [`Ledger`] —
//! the harness's ground truth for provenance and call-count assertions.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

pub mod hotblocks;
pub mod publisher;
pub mod registry;
pub mod worker;

#[derive(Clone, Default)]
pub struct Ledger(Arc<Mutex<Vec<String>>>);

impl Ledger {
    pub fn push(&self, entry: impl Into<String>) {
        self.0.lock().unwrap().push(entry.into());
    }

    pub fn entries(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }

    pub fn count_with_prefix(&self, prefix: &str) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.starts_with(prefix))
            .count()
    }
}

/// Serve an axum router on a fixed loopback port in a background task.
pub async fn serve(router: axum::Router, port: u16) -> anyhow::Result<SocketAddr> {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    Ok(addr)
}
