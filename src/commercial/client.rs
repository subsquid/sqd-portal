use std::time::Duration;

use url::Url;

use super::{config::CommercialConfig, types::SnapshotPage};

const SNAPSHOTS_PATH: [&str; 4] = ["internal", "portal", "v1", "snapshots"];
const AUTHORIZE_PATH: [&str; 4] = ["internal", "portal", "v1", "authorize"];

/// Page size requested from the feed. A short page ends the bootstrap loop, so
/// this value also defines what "short" means.
pub const PAGE_LIMIT: u16 = 1000;

/// Kept below the sync interval so a hung control plane cannot stack ticks.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

pub enum Authorized {
    Found(serde_json::Value),
    /// The control plane positively knows nothing about this key.
    Unknown,
}

pub struct ControlPlaneClient {
    http: reqwest::Client,
    snapshots_url: Url,
    authorize_url: Url,
    service_token: String,
}

impl ControlPlaneClient {
    pub fn new(config: &CommercialConfig) -> anyhow::Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(REQUEST_TIMEOUT)
                // These requests carry the portal's service token and their
                // answer is the key set itself. Both belong to the configured
                // control plane and nowhere else, so a redirect is an error
                // rather than an instruction. `http://` stays legal: local dev
                // runs the control plane without TLS.
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            snapshots_url: endpoint_url(&config.control_plane_url, &SNAPSHOTS_PATH)?,
            authorize_url: endpoint_url(&config.control_plane_url, &AUTHORIZE_PATH)?,
            service_token: config.service_token()?,
        })
    }

    pub async fn fetch_page(&self, cursor: u64) -> anyhow::Result<SnapshotPage> {
        let mut url = self.snapshots_url.clone();
        url.query_pairs_mut()
            .append_pair("cursor", &cursor.to_string())
            .append_pair("limit", &PAGE_LIMIT.to_string());

        let response = self
            .http
            .get(url)
            .bearer_auth(&self.service_token)
            .send()
            .await?;
        let status = response.status();
        anyhow::ensure!(
            status.is_success(),
            "snapshot feed returned status {status}"
        );

        Ok(response.json().await?)
    }

    pub async fn authorize(&self, key_id: &str) -> anyhow::Result<Authorized> {
        let response = self
            .http
            .post(self.authorize_url.clone())
            .bearer_auth(&self.service_token)
            .json(&serde_json::json!({ "key_id": key_id }))
            .send()
            .await?;

        match response.status() {
            status if status.is_success() => Ok(Authorized::Found(response.json().await?)),
            reqwest::StatusCode::NOT_FOUND => Ok(Authorized::Unknown),
            status => anyhow::bail!("authorize returned status {status}"),
        }
    }
}

/// Appends the internal API path to a configured base that may itself carry a
/// path prefix, with or without a trailing slash.
fn endpoint_url(base: &Url, segments: &[&str]) -> anyhow::Result<Url> {
    let mut url = base.clone();
    url.set_query(None);
    url.set_fragment(None);
    {
        let mut path = url
            .path_segments_mut()
            .map_err(|()| anyhow::anyhow!("commercial.control_plane_url cannot be a base"))?;
        path.pop_if_empty();
        path.extend(segments);
    }
    Ok(url)
}

#[cfg(test)]
mod tests {
    use axum::{response::Redirect, routing::get, Json, Router};

    use super::*;
    use crate::commercial::test_support::MockControlPlane;

    /// The feed request carries the portal's service token. A redirect sends
    /// that request somewhere the operator did not configure, and a redirected
    /// key set is not the control plane's answer — refuse both.
    #[tokio::test]
    async fn the_feed_client_does_not_follow_redirects() {
        let app = Router::new()
            .route(
                "/internal/portal/v1/snapshots",
                get(|| async { Redirect::temporary("/elsewhere") }),
            )
            .route(
                "/elsewhere",
                get(|| async { Json(serde_json::json!({ "records": [], "next_cursor": 0 })) }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        // Borrows the mock's service-token plumbing; only the URL differs.
        let cp = MockControlPlane::spawn().await;
        let mut config = cp.config();
        config.control_plane_url = format!("http://{addr}").parse().unwrap();
        let client = ControlPlaneClient::new(&config).unwrap();

        let err = client
            .fetch_page(0)
            .await
            .expect_err("a redirected feed must not be treated as an answer");

        assert!(err.to_string().contains("307"), "got {err}");
    }

    fn url_for(base: &str) -> String {
        endpoint_url(&base.parse().unwrap(), &SNAPSHOTS_PATH)
            .unwrap()
            .to_string()
    }

    #[test]
    fn endpoint_url_appends_to_bases_with_and_without_trailing_slash() {
        assert_eq!(
            url_for("https://cp.example"),
            "https://cp.example/internal/portal/v1/snapshots"
        );
        assert_eq!(
            url_for("https://cp.example/"),
            "https://cp.example/internal/portal/v1/snapshots"
        );
        assert_eq!(
            url_for("https://cp.example/saas/"),
            "https://cp.example/saas/internal/portal/v1/snapshots"
        );
        assert_eq!(
            url_for("https://cp.example/saas?x=1#f"),
            "https://cp.example/saas/internal/portal/v1/snapshots"
        );
    }

    #[test]
    fn endpoint_url_rejects_a_url_that_cannot_be_a_base() {
        let base: Url = "mailto:ops@example.com".parse().unwrap();
        assert!(endpoint_url(&base, &SNAPSHOTS_PATH).is_err());
    }
}
