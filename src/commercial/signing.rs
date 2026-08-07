//! How the portal proves to the control plane which portal is asking (ADR-018).
//!
//! A shared bearer token authenticates whoever holds the header, not the
//! request: read once from a log line or a proxy, it authorizes every future
//! exchange from anywhere. A signature over the request binds the two together,
//! so a captured header cannot be replayed against a different credential, and
//! the control plane stores a public key rather than a secret.

use axum::http::HeaderValue;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as BASE64URL, Engine};
use sha2::{Digest, Sha256};
use sqd_network_transport::{Keypair, PeerId};

pub const PORTAL_ID_HEADER: &str = "x-portal-id";
pub const TIMESTAMP_HEADER: &str = "x-timestamp";
pub const SIGNATURE_HEADER: &str = "x-signature";

/// Bumped only if the canonical form below changes shape, so a portal and a
/// control plane that disagree fail loudly instead of failing verification.
const SCHEME: &str = "sqd-portal-v1";

/// Both encoded values use **unpadded base64url**, so every byte of them is
/// `[A-Za-z0-9_-]`. Standard base64 would also be a legal header value — `+`,
/// `/` and `=` are all VCHAR — but those three are exactly the characters that
/// change meaning when a value passes through a query string, a form encoder or
/// a parser that splits on `=`. A signature that survives the wire only most of
/// the time is worse than no signature, and the JWK `x` member a verifier feeds
/// the key into is base64url anyway.
const _: () = ();

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

    /// The raw 32-byte Ed25519 public key, base64url. This — not the peer id — is
    /// what the control plane registers against `portal_id`: a peer id wraps the
    /// key in protobuf inside a multihash, so verifying one means depending on a
    /// libp2p implementation, while these 32 bytes go straight into any standard
    /// library's Ed25519 verifier — and, being base64url already, it drops
    /// straight into a JWK `x` member with no re-encoding.
    pub fn public_key_base64(&self) -> anyhow::Result<String> {
        let key = self
            .keypair
            .public()
            .try_into_ed25519()
            .map_err(|_| anyhow::anyhow!("the portal's identity key is not Ed25519"))?;
        Ok(BASE64URL.encode(key.to_bytes()))
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
                HeaderValue::from_str(&BASE64URL.encode(signature))?,
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

/// Signs exactly as the client does, so a test can compare what arrived over
/// the wire against what should have been sent.
#[cfg(test)]
pub(super) fn sign_for_test(
    config: &super::config::CommercialConfig,
    credential: &super::extractor::Credential,
    now_secs: u64,
) -> String {
    let body = serde_json::to_vec(&serde_json::json!({
        "credential": credential.token.expose(),
    }))
    .expect("a credential serializes");
    let signer = RequestSigner::new(test_keypair(), config.portal_id());
    signer
        .headers("POST", "/internal/portal/v1/exchange", &body, now_secs)
        .expect("signing should succeed")
        .into_iter()
        .find(|(name, _)| *name == SIGNATURE_HEADER)
        .map(|(_, value)| value.to_str().expect("base64url is ASCII").to_owned())
        .expect("a signature header")
}

/// One fixed keypair for the whole test run, so the client and the assertion
/// that checks it are signing with the same identity.
#[cfg(test)]
pub(super) fn test_keypair() -> Keypair {
    Keypair::ed25519_from_bytes([11u8; 32]).expect("a 32-byte seed")
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
            .verify(payload.as_bytes(), &BASE64URL.decode(encoded).unwrap()));
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

    /// A fixed vector the other side of this contract can be tested against
    /// without running the portal. Ed25519 over the canonical string; the
    /// signature is the raw 64 bytes and the key the raw 32, both unpadded
    /// base64url. Node verifies it with no dependencies:
    ///
    /// ```js
    /// const key = crypto.createPublicKey({ format: 'jwk',
    ///   key: { kty: 'OKP', crv: 'Ed25519', x: PUBLIC_KEY } });
    /// crypto.verify(null, Buffer.from(canonical), key,
    ///               Buffer.from(SIGNATURE, 'base64url'));
    /// ```
    #[test]
    fn the_wire_format_matches_its_published_vector() {
        use sqd_network_transport::Keypair;

        let keypair = Keypair::ed25519_from_bytes([7u8; 32]).expect("a 32-byte seed");
        let signer = RequestSigner::new(keypair, "portal-premium-eu".to_string());
        let body = br#"{"credential":"sqd_portal_k1_theverysecretvalue"}"#;

        assert_eq!(
            signer.public_key_base64().unwrap(),
            "6kpsY-KcUgq-9VB7Ey7F-ZVHdq6-vnuSQh7qaRRG0iw"
        );
        assert_eq!(
            canonical(
                "portal-premium-eu",
                1_800_000_000,
                "POST",
                "/internal/portal/v1/exchange",
                body
            ),
            "sqd-portal-v1\nportal-premium-eu\n1800000000\nPOST\n\
             /internal/portal/v1/exchange\n\
             30650daa6d3b90517f572c1154da8fcfb1ebf678003a3e74eaa7a33826d6dd56"
        );
        assert_eq!(
            signature_of(&signer, body, 1_800_000_000),
            "_ThgFiJAgcGa7dKAE_EOMDYaL_myOdX2BIlz6RT66ziWZtzCFrFbQ6e9hcvV0zsb0A0bYhyC5GK6Kj6EU36eCA"
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
