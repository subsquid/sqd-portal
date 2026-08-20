//! Boot the whole IB-7 stub world plus the portal-under-test, so a conformance
//! class is a file of assertions rather than a file of setup.
//!
//! Every CT class needs the same thing: stubs with ledgers, a toy world, a
//! portal process, and a way to dump state when a run fails. CT-1 grew that
//! inline; anything past it shares this.

use std::time::Duration;

use anyhow::Context;
use tempfile::TempDir;

use crate::artifact::AssignmentType;
use crate::portal::{Auth, Endpoints, PortalProcess};
use crate::stubs::control_plane::ControlPlane;
use crate::stubs::worker::{WorkerFaults, WorkerStub};
use crate::{artifact, driver, dummy_chain, keys, portal, stubs, ToyWorld};

pub struct Fixture {
    pub world: ToyWorld,
    pub base: String,
    pub http: reqwest::Client,
    pub portal: PortalProcess,
    pub worker_ledgers: Vec<stubs::Ledger>,
    /// Shared by every stub worker: a queued fault lands on whichever one the
    /// portal picks, so tests don't depend on the routing choice (FV-1).
    pub worker_faults: WorkerFaults,
    pub hotblocks_ledger: stubs::Ledger,
    pub publisher_ledger: stubs::Ledger,
    /// Present only on an authorizing fixture — absent, DC-8 is vacuous (REQ-56).
    pub control_plane: Option<ControlPlane>,
    _workers: Vec<WorkerStub>,
    scratch: Option<TempDir>,
}

/// Which artifacts the publisher offers, and which one the portal is pointed at. Defaults to
/// the migration window: both published, the portal following the state. Separating the two
/// is what lets a test put the portal on an artifact that is not on offer.
pub struct Assignments {
    /// `None` follows the `assignment_type` the state names, which is the portal's default.
    pub source: Option<AssignmentType>,
    pub published: Vec<AssignmentType>,
}

impl Default for Assignments {
    fn default() -> Self {
        Self {
            source: None,
            published: vec![AssignmentType::Legacy, AssignmentType::Split],
        }
    }
}

impl Fixture {
    /// `workers` stub workers on the toy world. The portal pre-leases
    /// 1 + retries distinct workers per chunk, so two is the minimum that lets
    /// a reroute actually find somewhere to go.
    pub async fn start(world: ToyWorld, workers: usize) -> anyhow::Result<Self> {
        Self::start_with(world, workers, None, Assignments::default()).await
    }

    /// The same world with an `auth:` block and the DC-8 stub behind it.
    /// The control plane is booted before the portal and verifies against the
    /// portal's own identity key, so signatures are checked for real.
    pub async fn start_with_auth(
        world: ToyWorld,
        workers: usize,
        auth: Auth,
    ) -> anyhow::Result<Self> {
        Self::start_with(world, workers, Some(auth), Assignments::default()).await
    }

    /// A fixture whose publisher shape and configured source are set by the caller — the DC-2
    /// source-selection cases. Note this does not wait for readiness: a portal pointed at an
    /// artifact nobody publishes never becomes ready, which is the point of those cases.
    pub async fn start_with_assignments(
        world: ToyWorld,
        workers: usize,
        assignments: Assignments,
    ) -> anyhow::Result<Self> {
        Self::start_with(world, workers, None, assignments).await
    }

    async fn start_with(
        world: ToyWorld,
        workers: usize,
        auth: Option<Auth>,
        assignments: Assignments,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(workers >= 1, "need at least one worker");
        // Loopback p2p addresses are filtered as unreachable unless this is set.
        std::env::set_var("PRIVATE_NETWORK", "1");
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "info".into()),
            )
            .try_init();

        let scratch_dir = tempfile::tempdir()?;
        let scratch = scratch_dir.path().to_path_buf();

        let endpoints = Endpoints {
            publisher_port: crate::free_tcp_port(),
            registry_port: crate::free_tcp_port(),
            hotblocks_port: crate::free_tcp_port(),
            http_port: crate::free_tcp_port(),
            control_plane_port: auth.as_ref().map(|_| crate::free_tcp_port()),
        };
        let worker_udp_ports: Vec<u16> = (0..workers).map(|_| crate::free_udp_port()).collect();

        let worker_ids = (0..workers)
            .map(|i| keys::generate(&scratch.join(format!("worker-{i}.key"))))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let portal_id = keys::generate(&scratch.join("portal.key"))?;
        let worker_peers: Vec<_> = worker_ids.iter().map(|w| w.peer_id).collect();
        let dummy_path = scratch.join("dummy_client.json");
        std::fs::write(
            &dummy_path,
            dummy_chain::dummy_data_json(&worker_peers, portal_id.peer_id),
        )?;

        let publisher_ledger = stubs::publisher::start(
            endpoints.publisher_port,
            stubs::publisher::network_state_json_publishing(
                endpoints.publisher_port,
                "toy-assignment-1",
                0,
                &assignments.published,
            ),
            artifact::build_gzipped(&world, &worker_peers)?,
            artifact::build_portal_gzipped(&world, &worker_peers)?,
        )
        .await?;
        let _registry_ledger = stubs::registry::start(endpoints.registry_port, &world).await?;
        let hotblocks_ledger =
            stubs::hotblocks::start(endpoints.hotblocks_port, world.clone()).await?;

        let worker_faults = WorkerFaults::none();
        let mut worker_ledgers = Vec::new();
        let mut stub_workers = Vec::new();
        for (i, id) in worker_ids.iter().enumerate() {
            let stub = stubs::worker::start(
                world.clone(),
                &scratch.join(format!("worker-{i}.key")),
                id.keypair.clone(),
                &dummy_path,
                worker_udp_ports[i],
                worker_faults.clone(),
            )
            .await
            .with_context(|| format!("start stub worker {i}"))?;
            worker_ledgers.push(stub.ledger.clone());
            stub_workers.push(stub);
        }
        tokio::time::sleep(Duration::from_millis(500)).await; // QUIC listeners up

        // Booted before the portal: the first gated request must find it up,
        // and it verifies against the identity the portal will sign with —
        // which is the whole claim when `key_path` names a second key.
        let control_plane = match (&auth, endpoints.control_plane_port) {
            (Some(c), Some(port)) => {
                let signing_key = if c.dedicated_key {
                    keys::generate(&scratch.join(portal::AUTH_KEY_FILE))?.keypair
                } else {
                    portal_id.keypair.clone()
                };
                Some(stubs::control_plane::start(port, &c.portal_id, signing_key.public()).await?)
            }
            _ => None,
        };

        let boot_nodes = worker_ids
            .iter()
            .zip(&worker_udp_ports)
            .map(|(id, port)| format!("{} /ip4/127.0.0.1/udp/{port}/quic-v1", id.peer_id))
            .collect::<Vec<_>>()
            .join(",");
        let config = portal::write_config(&scratch, &world, &endpoints, auth.as_ref())?;
        let portal = portal::spawn(
            &scratch,
            &config,
            &scratch.join("portal.key"),
            &dummy_path,
            &boot_nodes,
            &endpoints,
            assignments.source,
        )?;

        Ok(Self {
            base: portal.base_url.clone(),
            world,
            http: driver::client(),
            portal,
            worker_ledgers,
            worker_faults,
            hotblocks_ledger,
            publisher_ledger,
            control_plane,
            _workers: stub_workers,
            scratch: Some(scratch_dir),
        })
    }

    pub async fn wait_ready(&mut self, timeout: Duration) -> anyhow::Result<()> {
        self.portal.wait_ready(timeout).await
    }

    /// The DC-8 stub. Panics on a fixture without authorization, where asking about it
    /// is the test's own mistake.
    pub fn cp(&self) -> &ControlPlane {
        self.control_plane
            .as_ref()
            .expect("this fixture has no `auth:` block")
    }

    /// The keyless `/metrics` scrape, as OpenMetrics text.
    pub async fn scrape(&self) -> anyhow::Result<String> {
        let d = driver::get(&self.http, &format!("{}/metrics", self.base)).await?;
        Ok(String::from_utf8_lossy(&d.body).into_owned())
    }

    /// Total stub-worker queries answered, across every worker.
    pub fn queries_answered(&self) -> usize {
        self.worker_ledgers
            .iter()
            .map(|l| l.count_with_prefix("query "))
            .sum()
    }

    /// Tear down, dumping the portal log and keeping the scratch dir when the
    /// run failed — a conformance failure is worth more than a clean temp dir.
    pub fn finish<T>(mut self, result: anyhow::Result<T>) -> anyhow::Result<T> {
        if result.is_err() {
            eprintln!("=== portal log tail ===\n{}", self.portal.log_tail(80));
            if let Some(dir) = self.scratch.take() {
                eprintln!("=== scratch kept at {} ===", dir.keep().display());
            }
        }
        self.portal.terminate();
        result
    }
}
