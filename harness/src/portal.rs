//! Black-box portal process: config generation, spawn, readiness wait,
//! SIGTERM on drop (ADR-005 two-phase shutdown, shortened for tests).

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::Context;

use crate::world::ToyWorld;

pub struct Endpoints {
    pub publisher_port: u16,
    pub registry_port: u16,
    pub hotblocks_port: u16,
    pub http_port: u16,
}

pub fn write_config(scratch: &Path, world: &ToyWorld, e: &Endpoints) -> anyhow::Result<PathBuf> {
    let mut datasets = String::new();
    for ds in &world.datasets {
        datasets.push_str(&format!("  {}:\n", ds.name));
        let extra_aliases: Vec<&String> =
            ds.aliases.iter().filter(|a| **a != ds.name).collect();
        if !extra_aliases.is_empty() {
            let quoted: Vec<String> =
                extra_aliases.iter().map(|a| format!("\"{a}\"")).collect();
            datasets.push_str(&format!("    aliases: [{}]\n", quoted.join(", ")));
        }
        datasets.push_str("    kind: \"evm\"\n");
        if ds.real_time.is_some() {
            datasets.push_str(&format!(
                "    real_time:\n      url: http://127.0.0.1:{}/\n",
                e.hotblocks_port
            ));
        }
    }

    let config = format!(
        r#"hostname: http://127.0.0.1:{http}
max_parallel_streams: 64
transport_timeout_sec: 10
pre_drain_grace_period_sec: 1
drain_timeout_sec: 2
assignments_url: http://127.0.0.1:{publisher}
assignments_update_interval_sec: 1
datasets_update_interval_sec: 600
chain_update_interval_sec: 60
send_logs: false
sentry_is_enabled: false
verify_worker_responses: true
# Fast penalty decay: an early dial race must not wedge the tiny toy pool for
# minutes. Penalty behavior itself is CT-2's subject, not the smoke's.
priorities:
  max_queries_per_worker: 1
  window_errors_secs: 1
  window_timeouts_secs: 1
sqd_network:
  datasets: http://127.0.0.1:{registry}/datasets.yml
  metadata: http://127.0.0.1:{registry}/metadata.yml
  serve: "manual"
datasets:
{datasets}"#,
        http = e.http_port,
        publisher = e.publisher_port,
        registry = e.registry_port,
        datasets = datasets,
    );
    let path = scratch.join("portal.config.yml");
    std::fs::write(&path, config)?;
    Ok(path)
}

pub struct PortalProcess {
    child: Child,
    pub base_url: String,
    pub log_path: PathBuf,
}

pub fn default_binary() -> PathBuf {
    if let Ok(p) = std::env::var("PORTAL_BIN") {
        return PathBuf::from(p);
    }
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/debug/sqd-portal")
}

#[allow(clippy::too_many_arguments)]
pub fn spawn(
    scratch: &Path,
    config: &Path,
    portal_key: &Path,
    dummy_client: &Path,
    boot_nodes: &str,
    e: &Endpoints,
) -> anyhow::Result<PortalProcess> {
    let log_path = scratch.join("portal.log");
    let log = std::fs::File::create(&log_path)?;
    let bin = default_binary();
    anyhow::ensure!(
        bin.exists(),
        "portal binary not found at {} — run `cargo build` in the portal repo or set PORTAL_BIN",
        bin.display()
    );

    let child = Command::new(&bin)
        .current_dir(scratch) // keeps the repo's .env out of dotenv's reach
        .env("CONFIG", config)
        .env("HTTP_LISTEN_ADDR", format!("127.0.0.1:{}", e.http_port))
        .env("KEY_PATH", portal_key)
        .env("RPC_URL", "http://127.0.0.1:9/")
        .env("L1_RPC_URL", "http://127.0.0.1:9/")
        .env("NETWORK", "tethys")
        .env("DUMMY_CLIENT_FILE_PATH", dummy_client)
        .env("BOOT_NODES", boot_nodes)
        .env("PRIVATE_NETWORK", "1")
        .env("RUST_LOG", "info,sqd_portal=debug")
        .env_remove("P2P_LISTEN_ADDRS")
        .env_remove("P2P_PUBLIC_ADDRS")
        .env_remove("SENTRY_DSN")
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .spawn()
        .context("spawning portal")?;

    Ok(PortalProcess {
        child,
        base_url: format!("http://127.0.0.1:{}", e.http_port),
        log_path,
    })
}

impl PortalProcess {
    pub async fn wait_ready(&mut self, timeout: Duration) -> anyhow::Result<()> {
        let client = reqwest::Client::new();
        let url = format!("{}/ready", self.base_url);
        let start = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait()? {
                anyhow::bail!(
                    "portal exited early ({status}); log tail:\n{}",
                    self.log_tail(60)
                );
            }
            if let Ok(resp) = client.get(&url).send().await {
                if resp.status().as_u16() == 200 {
                    return Ok(());
                }
            }
            if start.elapsed() > timeout {
                anyhow::bail!(
                    "portal not ready within {timeout:?}; log tail:\n{}",
                    self.log_tail(60)
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    pub fn log_tail(&self, lines: usize) -> String {
        match std::fs::read_to_string(&self.log_path) {
            Ok(s) => {
                let all: Vec<&str> = s.lines().collect();
                let start = all.len().saturating_sub(lines);
                all[start..].join("\n")
            }
            Err(e) => format!("<no log: {e}>"),
        }
    }

    pub fn terminate(&mut self) {
        let pid = self.child.id().to_string();
        let _ = Command::new("kill").args(["-TERM", &pid]).status();
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            if let Ok(Some(_)) = self.child.try_wait() {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for PortalProcess {
    fn drop(&mut self) {
        self.terminate();
    }
}
