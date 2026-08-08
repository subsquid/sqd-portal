use url::Url;

use super::{
    config::ResolvedAuth,
    signing::RequestSigner,
    types::{ExchangeAnswer, Grant, CLAIMS_VERSION},
};
use crate::auth::extractor::Credential;

const EXCHANGE_PATH: [&str; 4] = ["internal", "portal", "v1", "exchange"];

/// What one exchange established. Anything else propagates as an error, which
/// the caller turns into a retryable refusal rather than a verdict.
#[derive(Debug)]
pub enum Exchanged {
    Granted(Grant),
    /// Verbatim, so an unrecognised reason still reaches the log.
    Denied(String),
}

pub struct ControlPlaneClient {
    http: reqwest::Client,
    exchange_url: Url,
    signer: RequestSigner,
}

impl ControlPlaneClient {
    pub fn new(config: &ResolvedAuth, signer: RequestSigner) -> anyhow::Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(config.exchange_timeout())
                // Carries a credential, so a redirect is an error: it would
                // hand the secret somewhere unconfigured.
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            exchange_url: endpoint_url(&config.control_plane_url, &EXCHANGE_PATH)?,
            signer,
        })
    }

    /// Hands the credential to the authority. `now_secs` stamps the signature;
    /// refusing a stale one is what bounds replay (DC-8).
    pub async fn exchange(
        &self,
        credential: &Credential,
        now_secs: u64,
    ) -> anyhow::Result<Exchanged> {
        // Once: the signature binds the bytes actually sent.
        let body = serde_json::to_vec(&serde_json::json!({
            "credential": credential.token.expose(),
        }))?;
        let path = self.exchange_url.path().to_owned();
        let headers = self.signer.headers("POST", &path, &body, now_secs)?;

        let mut request = self
            .http
            .post(self.exchange_url.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/json");
        for (name, value) in headers {
            request = request.header(name, value);
        }

        let response = request.body(body).send().await?;
        let status = response.status();
        anyhow::ensure!(status.is_success(), "exchange returned status {status}");

        match response.json::<ExchangeAnswer>().await? {
            ExchangeAnswer::Denied { reason } => Ok(Exchanged::Denied(reason)),
            ExchangeAnswer::Granted { grant } => {
                anyhow::ensure!(
                    grant.claims_version == CLAIMS_VERSION,
                    "grant claims version {} is not {CLAIMS_VERSION}",
                    grant.claims_version
                );
                // Acting on it would admit one caller on another's
                // entitlements.
                anyhow::ensure!(
                    grant.key_id == credential.key_id,
                    "exchange answered about a different key"
                );
                anyhow::ensure!(
                    grant.expires_at > now_secs,
                    "grant is already expired on arrival"
                );
                Ok(Exchanged::Granted(grant))
            }
        }
    }
}

/// Appends the internal API path to a base that may carry a prefix of its own.
fn endpoint_url(base: &Url, segments: &[&str]) -> anyhow::Result<Url> {
    let mut url = base.clone();
    url.set_query(None);
    url.set_fragment(None);
    {
        let mut path = url
            .path_segments_mut()
            .map_err(|()| anyhow::anyhow!("auth.control_plane_url cannot be a base"))?;
        path.pop_if_empty();
        path.extend(segments);
    }
    Ok(url)
}

#[cfg(test)]
mod tests {
    use axum::{response::Redirect, routing::post, Json, Router};

    use super::*;
    use crate::auth::signing;
    use crate::auth::test_support::{credential, MockControlPlane, KEY_ID};

    const NOW: u64 = 1_800_000_000;

    async fn client_for(config: &ResolvedAuth) -> ControlPlaneClient {
        ControlPlaneClient::new(config, config.signer(signing::test_keypair()).unwrap()).unwrap()
    }

    #[tokio::test]
    async fn a_grant_comes_back_with_its_claims() {
        let cp = MockControlPlane::spawn().await;
        cp.grant(KEY_ID, None, NOW + 300, NOW + 900);

        let client = client_for(&cp.config()).await;
        let Exchanged::Granted(grant) = client.exchange(&credential(), NOW).await.unwrap() else {
            panic!("expected a grant");
        };

        assert_eq!(grant.key_id, KEY_ID);
        assert_eq!(grant.expires_at, NOW + 900);
    }

    #[tokio::test]
    async fn a_denial_comes_back_as_a_denial_rather_than_an_error() {
        let cp = MockControlPlane::spawn().await;
        cp.deny(KEY_ID, "revoked");

        let client = client_for(&cp.config()).await;
        let Exchanged::Denied(reason) = client.exchange(&credential(), NOW).await.unwrap() else {
            panic!("expected a denial");
        };

        assert_eq!(reason, "revoked");
    }

    /// Every one of these is the control plane failing to answer, not answering
    /// "no". Collapsing them onto a denial is the defect GAP-34 was opened for.
    #[tokio::test]
    async fn an_unusable_answer_is_an_error_and_never_a_denial() {
        let cp = MockControlPlane::spawn().await;
        let client = client_for(&cp.config()).await;

        cp.status(KEY_ID, 500);
        assert!(client.exchange(&credential(), NOW).await.is_err());

        cp.status(KEY_ID, 404);
        assert!(
            client.exchange(&credential(), NOW).await.is_err(),
            "a 404 is a routing accident, not an authoritative unknown key"
        );

        cp.clear_status(KEY_ID);
        cp.raw(
            KEY_ID,
            serde_json::json!({"result": "granted", "grant": {}}),
        );
        assert!(client.exchange(&credential(), NOW).await.is_err());

        cp.raw(
            KEY_ID,
            serde_json::json!({"result": "granted", "grant": {
                "claims_version": CLAIMS_VERSION + 1,
                "key_id": KEY_ID,
                "refresh_after": NOW + 300,
                "expires_at": NOW + 900,
            }}),
        );
        let err = client.exchange(&credential(), NOW).await.unwrap_err();
        assert!(err.to_string().contains("claims version"), "got {err}");

        cp.raw(
            KEY_ID,
            serde_json::json!({"result": "granted", "grant": {
                "claims_version": CLAIMS_VERSION,
                "key_id": "someone-else",
                "refresh_after": NOW + 300,
                "expires_at": NOW + 900,
            }}),
        );
        let err = client.exchange(&credential(), NOW).await.unwrap_err();
        assert!(err.to_string().contains("different key"), "got {err}");

        cp.grant(KEY_ID, None, NOW - 10, NOW - 1);
        let err = client.exchange(&credential(), NOW).await.unwrap_err();
        assert!(err.to_string().contains("already expired"), "got {err}");
    }

    #[tokio::test]
    async fn the_exchange_carries_a_signature_the_control_plane_can_attribute() {
        let cp = MockControlPlane::spawn().await;
        cp.grant(KEY_ID, None, NOW + 300, NOW + 900);

        let client = client_for(&cp.config()).await;
        client.exchange(&credential(), NOW).await.unwrap();

        let seen = cp.signatures();
        assert_eq!(seen.len(), 1);
        let (portal_id, timestamp, signature) = &seen[0];
        assert_eq!(portal_id, "portal-premium-eu");
        assert_eq!(timestamp, &NOW.to_string());
        // Byte-for-byte across a real HTTP hop: a signature is worthless if the
        // alphabet it is encoded in survives the wire only most of the time.
        assert_eq!(
            signature,
            &signing::sign_for_test(&cp.config(), &credential(), NOW)
        );
        assert!(signature
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'));
    }

    /// A redirect sends a client's credential somewhere the operator did not
    /// configure, and a redirected answer is not the control plane's.
    #[tokio::test]
    async fn the_exchange_does_not_follow_redirects() {
        let app = Router::new()
            .route(
                "/internal/portal/v1/exchange",
                post(|| async { Redirect::temporary("/elsewhere") }),
            )
            .route(
                "/elsewhere",
                post(|| async {
                    Json(serde_json::json!({"result": "denied", "reason": "unknown_key"}))
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let cp = MockControlPlane::spawn().await;
        let mut config = cp.config();
        config.control_plane_url = format!("http://{addr}").parse().unwrap();

        let err = client_for(&config)
            .await
            .exchange(&credential(), NOW)
            .await
            .expect_err("a redirected exchange must not be treated as an answer");

        assert!(err.to_string().contains("307"), "got {err}");
    }

    fn url_for(base: &str) -> String {
        endpoint_url(&base.parse().unwrap(), &EXCHANGE_PATH)
            .unwrap()
            .to_string()
    }

    #[test]
    fn endpoint_url_appends_to_bases_with_and_without_trailing_slash() {
        assert_eq!(
            url_for("https://cp.example"),
            "https://cp.example/internal/portal/v1/exchange"
        );
        assert_eq!(
            url_for("https://cp.example/"),
            "https://cp.example/internal/portal/v1/exchange"
        );
        assert_eq!(
            url_for("https://cp.example/saas/"),
            "https://cp.example/saas/internal/portal/v1/exchange"
        );
        assert_eq!(
            url_for("https://cp.example/saas?x=1#f"),
            "https://cp.example/saas/internal/portal/v1/exchange"
        );
    }

    #[test]
    fn endpoint_url_rejects_a_url_that_cannot_be_a_base() {
        let base: Url = "mailto:ops@example.com".parse().unwrap();
        assert!(endpoint_url(&base, &EXCHANGE_PATH).is_err());
    }
}
