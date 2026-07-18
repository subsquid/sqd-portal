//! libp2p identities: 64-byte raw ed25519 keypair files, the format
//! `sqd-network-transport`'s `get_keypair` reads.

use std::path::Path;

use libp2p_identity::{Keypair, PeerId};

pub struct Identity {
    pub keypair: Keypair,
    pub peer_id: PeerId,
}

/// Generate a fresh identity and persist it at `path` (32B secret ‖ 32B public).
pub fn generate(path: &Path) -> anyhow::Result<Identity> {
    let keypair = Keypair::generate_ed25519();
    let ed = keypair
        .clone()
        .try_into_ed25519()
        .map_err(|e| anyhow::anyhow!("ed25519 conversion: {e}"))?;
    std::fs::write(path, ed.to_bytes())?;
    let peer_id = keypair.public().to_peer_id();
    Ok(Identity { keypair, peer_id })
}
