//! How the portal proves to the control plane which portal is asking (ADR-018).
//!
//! A shared bearer token authenticates whoever holds the header, not the
//! request: read once from a log line or a proxy, it authorizes every future
//! exchange from anywhere. A signature over the request binds the two together,
//! so a captured header cannot be replayed against a different credential, and
//! the control plane stores a public key rather than a secret.

use axum::http::HeaderValue;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use sha2::{Digest, Sha256};
use sqd_network_transport::{Keypair, PeerId};

pub const PORTAL_ID_HEADER: &str = "x-portal-id";
pub const TIMESTAMP_HEADER: &str = "x-timestamp";
pub const SIGNATURE_HEADER: &str = "x-signature";

/// Bumped only if the canonical form below changes shape, so a portal and a
/// control plane that disagree fail loudly instead of failing verification.
const SCHEME: &str = "sqd-portal-v1";

pub struct RequestSigner {
    keypair: Keypair,
    portal_id: String,
}

impl RequestSigner {
    /// `portal_id` must already be validated free of newlines — the canonical
    /// form separates fields with them (`CommercialConfig::validate`).
    pub fn new(keypair: Keypair, portal_id: String) -> Self {
        Self { keypair, portal_id }
    }

    pub fn peer_id(&self) -> PeerId {
        self.keypair.public().to_peer_id()
    }

    /// The three headers the control plane needs to rebuild and check the
    /// binding. `timestamp_secs` is passed in rather than read here so the
    /// canonical form stays a pure function of its inputs.
    pub fn headers(
        &self,
        method: &str,
        path: &str,
        body: &[u8],
        timestamp_secs: u64,
    ) -> anyhow::Result<[(&'static str, HeaderValue); 3]> {
        let payload = canonical(&self.portal_id, timestamp_secs, method, path, body);
        let signature = self.keypair.sign(payload.as_bytes())?;
        Ok([
            (PORTAL_ID_HEADER, HeaderValue::from_str(&self.portal_id)?),
            (
                TIMESTAMP_HEADER,
                HeaderValue::from_str(&timestamp_secs.to_string())?,
            ),
            (
                SIGNATURE_HEADER,
                HeaderValue::from_str(&BASE64.encode(signature))?,
            ),
        ])
    }
}

/// What is actually signed. The body is bound by digest, which is what makes a
/// captured signature useless against a different credential; the timestamp is
/// what bounds how long it stays useful against the same one. No field can
/// contain a newline — the portal id is validated, the rest are hex, digits or
/// a URL path — so no two distinct requests share a canonical form.
fn canonical(
    portal_id: &str,
    timestamp_secs: u64,
    method: &str,
    path: &str,
    body: &[u8],
) -> String {
    format!(
        "{SCHEME}\n{portal_id}\n{timestamp_secs}\n{method}\n{path}\n{}",
        hex::encode(Sha256::digest(body))
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signer() -> RequestSigner {
        RequestSigner::new(Keypair::generate_ed25519(), "portal-premium-eu".to_string())
    }

    fn signature_of(signer: &RequestSigner, body: &[u8], timestamp: u64) -> String {
        let headers = signer
            .headers("POST", "/internal/portal/v1/exchange", body, timestamp)
            .expect("signing should succeed");
        headers
            .iter()
            .find(|(name, _)| *name == SIGNATURE_HEADER)
            .map(|(_, value)| value.to_str().unwrap().to_owned())
            .expect("a signature header")
    }

    #[test]
    fn the_signature_verifies_against_the_portals_public_key() {
        let signer = signer();
        let body = br#"{"credential":"sqd_portal_k1_secret"}"#;
        let encoded = signature_of(&signer, body, 1_800_000_000);

        let payload = canonical(
            "portal-premium-eu",
            1_800_000_000,
            "POST",
            "/internal/portal/v1/exchange",
            body,
        );
        assert!(signer
            .keypair
            .public()
            .verify(payload.as_bytes(), &BASE64.decode(encoded).unwrap()));
    }

    /// The property the whole scheme exists for: a captured header cannot be
    /// pointed at a credential of the attacker's choosing.
    #[test]
    fn a_different_body_produces_a_different_signature() {
        let signer = signer();
        let mine = signature_of(&signer, br#"{"credential":"sqd_portal_k1_mine"}"#, 1);
        let theirs = signature_of(&signer, br#"{"credential":"sqd_portal_k2_theirs"}"#, 1);

        assert_ne!(mine, theirs);
    }

    #[test]
    fn a_different_timestamp_produces_a_different_signature() {
        let signer = signer();
        let body = br#"{"credential":"sqd_portal_k1_secret"}"#;

        assert_ne!(
            signature_of(&signer, body, 1),
            signature_of(&signer, body, 2)
        );
    }

    /// The canonical form is what a control plane reimplements, so its exact
    /// bytes are the contract. A change here that is not also a `SCHEME` bump
    /// silently breaks every deployment.
    #[test]
    fn the_canonical_form_is_pinned() {
        assert_eq!(
            canonical("portal-premium-eu", 1_800_000_000, "POST", "/x", b""),
            "sqd-portal-v1\nportal-premium-eu\n1800000000\nPOST\n/x\n\
             e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn every_header_is_present_and_ascii() {
        let headers = signer()
            .headers("POST", "/x", b"body", 1_800_000_000)
            .expect("signing should succeed");
        let names: Vec<_> = headers.iter().map(|(name, _)| *name).collect();

        assert_eq!(
            names,
            [PORTAL_ID_HEADER, TIMESTAMP_HEADER, SIGNATURE_HEADER]
        );
        for (name, value) in headers {
            assert!(!value.is_empty(), "{name} must carry a value");
        }
    }
}
