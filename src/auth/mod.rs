//! Portal-side half of access control: each presented credential is
//! exchanged for a short-lived grant, which then answers per request. No quota
//! or limit — an admitted key streams unrestricted.
//!
//! What an admitted key streams is *measured*, where the operator asked for it
//! (`auth.usage:`, REQ-60): shadow accounting that records bytes and can never
//! withhold them. Measurement is not metering, and the distance between the two
//! is the whole of [`usage`]'s design.
//!
//! Inert unless `auth:` is present in the config.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use libp2p_identity::ed25519;
use sqd_network_transport::Keypair;

mod cache;
mod client;
mod config;
mod evaluate;
mod extractor;
mod routes;
mod signing;
mod singleflight;
mod types;
mod usage;

#[cfg(test)]
pub mod test_support;

pub use config::{AuthConfig, Enforcement, ResolvedAuth};
pub use usage::UsageConfig;

use config::KeySource;
pub use extractor::{DatasetCatalog, Gate};
pub use routes::Mounted;
pub use routes::{AuthExt, EndpointExt, Gated};

use cache::GrantCache;
use client::ControlPlaneClient;

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// The key the exchange is signed with: the one `auth.key_path` names, or the
/// network identity when it names none (DC-8). Neither is ever generated —
/// hence not `get_keypair`: a minted key is registered with no control plane,
/// so the replica would read as revoked for its life instead of failing here.
pub async fn load_keypair(
    config: &ResolvedAuth,
    network_key_path: PathBuf,
) -> anyhow::Result<Keypair> {
    let key = config.key.clone().unwrap_or(KeySource {
        path: network_key_path,
        knob: "KEY_PATH",
    });
    let keypair = read_keypair(&key.path, key.knob).await?;
    tracing::info!(
        key_path = %key.path.display(),
        knob = key.knob,
        "credential exchange signing key loaded"
    );
    Ok(keypair)
}

/// 32B secret ‖ 32B public, so one file serves as either identity.
async fn read_keypair(path: &Path, knob: &str) -> anyhow::Result<Keypair> {
    // Read blocks forever on a fifo and never ends on a character device, and
    // either is a plausible mount typo — a hang here is before the listener
    // binds, so it has no log line and no readiness endpoint to explain it.
    if let Ok(metadata) = tokio::fs::metadata(path).await {
        anyhow::ensure!(
            metadata.is_file(),
            "{knob}: the signing key at {} is not a regular file",
            path.display()
        );
    }
    let mut bytes = tokio::fs::read(path).await.map_err(|err| {
        anyhow::anyhow!(
            "{knob}: cannot read the signing key at {}: {err}. It is never generated — \
             a fresh key is registered with no control plane",
            path.display()
        )
    })?;
    let len = bytes.len();
    ed25519::Keypair::try_from_bytes(&mut bytes)
        .map(Keypair::from)
        .map_err(|err| {
            anyhow::anyhow!(
                "{knob}: {} is not an ed25519 key file ({len} bytes, expected 64): {err}",
                path.display()
            )
        })
}

/// The gate, and the usage reporter behind it where one was configured.
///
/// The reporter is handed back rather than detached because *this* is what
/// stops it: it deliberately does not listen to the process's cancellation
/// token, which fires when the drain begins rather than when serving ends, so
/// the only thing that ends its run loop is [`Started::finish`] — called after
/// the listener has stopped (ADR-005's second phase). Nothing on a request path
/// ever holds this.
pub struct Started {
    pub gate: Arc<Gate>,
    reporter: Option<usage::Reporting>,
}

impl Started {
    /// Stops the reporter and awaits its bounded finalize. That budget is
    /// internal and covers the whole post-stop path, so this cannot outlast it
    /// however unreachable the sink is; a reporter that panicked is likewise
    /// nothing to fail shutdown over.
    pub async fn finish(self) {
        if let Some(reporter) = self.reporter {
            reporter.finish().await;
        }
    }
}

/// Builds the gate the router wraps its data endpoints in. No authorization
/// state is started: it is learned from the requests that need it (LIV-5). A
/// configured usage sink does start a task — it owns a queue nothing on the
/// request path may wait on, which is precisely why it is a task.
pub fn build(
    config: &ResolvedAuth,
    keypair: Keypair,
    catalog: Arc<dyn DatasetCatalog>,
) -> anyhow::Result<Started> {
    let signer = config.signer(keypair.clone())?;
    // The public key, not the peer id: that is what the registration carries,
    // logged so an operator can read it off a replica rather than derive it.
    tracing::info!(
        portal_id = config.portal_id,
        peer_id = %signer.peer_id(),
        public_key = signer.public_key_base64()?,
        "credential exchange signing identity"
    );
    let cache = GrantCache::new(
        ControlPlaneClient::new(config, signer)?,
        config.limits.clone(),
        config.enforcement,
    );
    // Signed with the same identity as the exchange, so the control plane
    // attributes both to one registration and stamps the portal onto every
    // record from the signature rather than from a field it would have to trust.
    let usage = config
        .usage
        .as_ref()
        .map(|settings| usage::start(config, settings, config.signer(keypair)?))
        .transpose()?;
    let (sink, reporter) = match usage {
        Some((sink, reporter)) => (Some(sink), Some(reporter)),
        None => (None, None),
    };
    // The mode itself is logged at startup, which covers the disabled case.
    Ok(Started {
        gate: Arc::new(Gate::new(config, cache, catalog, sink)),
        reporter,
    })
}

// Which knob named the key is settled by `AuthConfig::resolve`, so nothing here
// reads the environment: these tests are about what happens to the path once it
// is resolved.
#[cfg(test)]
mod tests {
    use super::*;

    /// A key file in the format `get_keypair` reads, so the same file works in
    /// either role.
    fn write_key(dir: &std::path::Path, name: &str, seed: u8) -> (PathBuf, Keypair) {
        std::fs::create_dir_all(dir).unwrap();
        let path = dir.join(name);
        let keypair = Keypair::ed25519_from_bytes([seed; 32]).unwrap();
        let bytes = keypair.clone().try_into_ed25519().unwrap().to_bytes();
        std::fs::write(&path, bytes).unwrap();
        (path, keypair)
    }

    fn scratch() -> tempfile::TempDir {
        tempfile::tempdir().expect("a scratch dir")
    }

    fn config_with(key: Option<KeySource>) -> ResolvedAuth {
        ResolvedAuth {
            control_plane_url: "https://cp.example/authority".parse().unwrap(),
            portal_id: "portal-premium-eu".to_string(),
            key,
            enforcement: Enforcement::Enforce,
            limits: config::Limits::default(),
            usage: None,
        }
    }

    fn from_file(path: PathBuf) -> Option<KeySource> {
        Some(KeySource {
            path,
            knob: "auth.key_path",
        })
    }

    #[tokio::test]
    async fn the_network_key_signs_when_no_auth_key_is_configured() {
        let dir = scratch();
        let (network_key, expected) = write_key(dir.path(), "network.key", 3);

        let loaded = load_keypair(&config_with(None), network_key).await.unwrap();

        assert_eq!(loaded.public(), expected.public());
    }

    #[tokio::test]
    async fn a_configured_auth_key_signs_instead_of_the_network_key() {
        let dir = scratch();
        let (network_key, network) = write_key(dir.path(), "network.key", 3);
        let (auth_key, expected) = write_key(dir.path(), "auth.key", 9);

        let loaded = load_keypair(&config_with(from_file(auth_key)), network_key)
            .await
            .unwrap();

        assert_eq!(loaded.public(), expected.public());
        assert_ne!(loaded.public(), network.public());
    }

    /// Generating one would sign every exchange with a key the control plane
    /// has never seen, which reads as a portal it no longer recognises.
    #[tokio::test]
    async fn a_missing_auth_key_fails_rather_than_minting_one() {
        let dir = scratch();
        let (network_key, _) = write_key(dir.path(), "network.key", 3);
        let absent = dir.path().join("nope.key");

        let err = load_keypair(&config_with(from_file(absent.clone())), network_key)
            .await
            .expect_err("a missing signing key must not be generated");

        assert!(err.to_string().contains("auth.key_path"), "got {err}");
        assert!(!absent.exists(), "nothing may be written to the path");
    }

    /// The same trap one level down: with auth on, the network key is the
    /// registered identity, so an absent one must not be invented either.
    #[tokio::test]
    async fn a_missing_network_key_fails_rather_than_minting_one() {
        let dir = scratch();
        let absent = dir.path().join("network.key");

        let err = load_keypair(&config_with(None), absent.clone())
            .await
            .expect_err("a missing network key must not be generated");

        assert!(err.to_string().contains("KEY_PATH"), "got {err}");
        assert!(!absent.exists(), "nothing may be written to the path");
    }

    /// The likelier failure is a wrong file, not an absent one — a directory, a
    /// PEM where raw bytes belong — and it reads as a bug in the network key
    /// unless the error names the knob.
    #[tokio::test]
    async fn an_unusable_auth_key_names_the_knob_and_the_path() {
        let dir = scratch();
        let (network_key, _) = write_key(dir.path(), "network.key", 3);

        let malformed = dir.path().join("pem.key");
        std::fs::write(&malformed, b"-----BEGIN PRIVATE KEY-----\n").unwrap();
        let err = load_keypair(
            &config_with(from_file(malformed.clone())),
            network_key.clone(),
        )
        .await
        .expect_err("a malformed key must not load");
        assert!(err.to_string().contains("auth.key_path"), "got {err}");
        assert!(err.to_string().contains("pem.key"), "got {err}");

        // The mount directory instead of the file inside it.
        let err = load_keypair(&config_with(from_file(dir.path().to_owned())), network_key)
            .await
            .expect_err("a directory must not load");
        assert!(err.to_string().contains("auth.key_path"), "got {err}");
    }

    /// The override wins silently, so an error blaming the file sends an
    /// operator to edit something that changes nothing. Which knob won is
    /// settled in `resolve`; this is only about carrying it into the failure.
    #[tokio::test]
    async fn the_knob_that_named_the_key_is_the_one_the_failure_names() {
        let dir = scratch();
        let (network_key, _) = write_key(dir.path(), "network.key", 3);
        let absent = dir.path().join("from-env.key");
        let from_env = Some(KeySource {
            path: absent,
            knob: config::AUTH_KEY_PATH_ENV,
        });

        let err = load_keypair(&config_with(from_env), network_key)
            .await
            .expect_err("a missing signing key must not be generated");

        assert!(err.to_string().contains("AUTH_KEY_PATH"), "got {err}");
        assert!(err.to_string().contains("from-env.key"), "got {err}");
    }
}
