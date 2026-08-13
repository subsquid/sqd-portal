//! Black-box portal process: config generation, spawn, readiness wait,
//! SIGTERM on drop (ADR-005 two-phase shutdown, shortened for tests).

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::Context;

use crate::artifact::AssignmentSource;
use crate::world::ToyWorld;

pub struct Endpoints {
    pub publisher_port: u16,
    pub registry_port: u16,
    pub hotblocks_port: u16,
    pub http_port: u16,
    /// `Some` turns authorization on: presence of the block is the switch
    /// (REQ-56), so absence has to stay expressible.
    pub control_plane_port: Option<u16>,
}

/// The `auth:` block a CT-10 fixture writes. Limits are spelled as raw
/// YAML lines because each case bounds a different one and the portal defaults
/// the rest.
pub struct Auth {
    pub portal_id: String,
    pub enforcement: &'static str,
    pub limits: Vec<String>,
    /// Sign with `auth.key_path`. The fixture then registers that key and only
    /// that key, so signing with the network identity is refused.
    pub dedicated_key: bool,
}

/// Where the fixture writes the dedicated key and where `auth.key_path` points.
pub const AUTH_KEY_FILE: &str = "auth.key";

/// Optional portal tuning a test threads through the fixture, for classes whose
/// property depends on configuration rather than on the request (like CT-10's
/// `auth:` block). Absent, the smoke's defaults hold; present, it can pin the
/// congestion window and lengthen the transport timeout so a slow worker read
/// holds a scheduler slot past the worker's 60s freshness bound.
#[derive(Clone)]
pub struct Tuning {
    /// The portal's per-request transport timeout. Must exceed a stall a test
    /// wants a worker to hold, or the portal tears the query down first.
    pub transport_timeout_sec: u64,
    /// `Some` writes a `congestion:` block; `None` leaves the portal's default
    /// (an AIMD window seeded at 10..=500).
    pub congestion: Option<Congestion>,
}

/// The subset of `CongestionConfig` (src/config.rs) a test pins. Field names
/// match the portal struct; the rest keep their `#[serde(default)]` values.
#[derive(Clone)]
pub struct Congestion {
    pub enabled: bool,
    pub min_window: u32,
    pub max_window: u32,
    /// Per-read deadline inside `read_response_with_permits`; a stalled body read
    /// holds its scheduler slot up to this long.
    pub read_timeout_sec: u64,
}

impl Default for Tuning {
    fn default() -> Self {
        Self {
            transport_timeout_sec: 10,
            congestion: None,
        }
    }
}

impl Tuning {
    /// A single-slot congestion window (`min = max = 1`) with a transport timeout
    /// and read timeout long enough to hold that slot across a multi-second stall.
    pub fn single_slot_congestion(transport_timeout_sec: u64, read_timeout_sec: u64) -> Self {
        Self {
            transport_timeout_sec,
            congestion: Some(Congestion {
                enabled: true,
                min_window: 1,
                max_window: 1,
                read_timeout_sec,
            }),
        }
    }
}

impl Auth {
    pub fn new(portal_id: impl Into<String>) -> Self {
        Self {
            portal_id: portal_id.into(),
            enforcement: "enforce",
            limits: Vec::new(),
            dedicated_key: false,
        }
    }

    pub fn dedicated_key(mut self) -> Self {
        self.dedicated_key = true;
        self
    }

    pub fn enforcement(mut self, mode: &'static str) -> Self {
        self.enforcement = mode;
        self
    }

    pub fn limit(mut self, key: &str, value: impl std::fmt::Display) -> Self {
        self.limits.push(format!("    {key}: {value}\n"));
        self
    }
}

pub fn write_config(
    scratch: &Path,
    world: &ToyWorld,
    e: &Endpoints,
    auth: Option<&Auth>,
    assignment_source: AssignmentSource,
    tuning: Option<&Tuning>,
) -> anyhow::Result<PathBuf> {
    let mut datasets = String::new();
    for ds in &world.datasets {
        datasets.push_str(&format!("  {}:\n", ds.name));
        let extra_aliases: Vec<&String> = ds.aliases.iter().filter(|a| **a != ds.name).collect();
        if !extra_aliases.is_empty() {
            let quoted: Vec<String> = extra_aliases.iter().map(|a| format!("\"{a}\"")).collect();
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

    // Loopback keeps the `https` requirement satisfied without a certificate:
    // the block carries client credentials, so the portal refuses plaintext
    // anywhere else.
    let auth_block = match (auth, e.control_plane_port) {
        (Some(c), Some(port)) => format!(
            "auth:\n  \
             control_plane_url: http://127.0.0.1:{port}/authority\n  \
             portal_id: {id}\n  \
             enforcement: {mode}\n{key_path}{limits}",
            id = c.portal_id,
            mode = c.enforcement,
            key_path = if c.dedicated_key {
                format!("  key_path: {}\n", scratch.join(AUTH_KEY_FILE).display())
            } else {
                String::new()
            },
            limits = if c.limits.is_empty() {
                String::new()
            } else {
                format!("  limits:\n{}", c.limits.concat())
            },
        ),
        _ => String::new(),
    };

    let default_tuning = Tuning::default();
    let tuning = tuning.unwrap_or(&default_tuning);
    let congestion_block = match &tuning.congestion {
        Some(c) => format!(
            "congestion:\n  \
             enabled: {enabled}\n  \
             min_window: {min}\n  \
             max_window: {max}\n  \
             read_timeout_sec: {read}\n",
            enabled = c.enabled,
            min = c.min_window,
            max = c.max_window,
            read = c.read_timeout_sec,
        ),
        None => String::new(),
    };

    let config = format!(
        r#"hostname: http://127.0.0.1:{http}
max_parallel_streams: 64
transport_timeout_sec: {transport_timeout}
pre_drain_grace_period_sec: 1
drain_timeout_sec: 2
assignments_url: http://127.0.0.1:{publisher}
assignments_update_interval_sec: 1
assignment_source: {assignment_source}
datasets_update_interval_sec: 600
chain_update_interval_sec: 60
send_logs: false
sentry_is_enabled: false
verify_worker_responses: true
{congestion_block}# Fast penalty decay: an early dial race must not wedge the tiny toy pool for
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
{datasets}{auth_block}"#,
        http = e.http_port,
        publisher = e.publisher_port,
        assignment_source = assignment_source.as_str(),
        registry = e.registry_port,
        datasets = datasets,
        auth_block = auth_block,
        transport_timeout = tuning.transport_timeout_sec,
        congestion_block = congestion_block,
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
        // Both override the config value, so an inherited one would silently
        // make every exchange unattributable or sign it with an unregistered key.
        .env_remove("PORTAL_ID")
        .env_remove("AUTH_KEY_PATH")
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

    /// The whole log. An audit that greps for a secret has to read every line
    /// of it, not the tail (INV-38).
    pub fn log_all(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
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
