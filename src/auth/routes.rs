//! How a route says whether it needs a key.
//!
//! Both [`AuthExt`] methods return a [`Classified`], and [`Gated::route`] takes
//! nothing else — so a route that says neither does not compile. A wrapper you
//! can forget to write is open by omission.
//!
//! That covers routes added *here*. [`Gated::merge_ungated`] mounts a router
//! whole and cannot see inside it, so [`Gated::inventory`] records what was
//! mounted either way and a test asserts the whole surface against it (REQ-51).

use std::sync::Arc;

use axum::{routing::MethodRouter, Router};

use super::{extractor, Gate};
use crate::utils::logging::EndpointAnnotationLayer;

/// A method router that has declared whether it needs a key.
pub struct Classified {
    router: MethodRouter,
    gated: bool,
    endpoint: Option<String>,
}

/// A method router carrying the name its metrics go under. The annotation layer
/// is applied by [`Gated::route`], outermost, so a refusal the gate mints is
/// labelled like any other response — the metrics fall back to the raw request
/// path otherwise, one series per spelling an unauthenticated client invents.
pub struct Named {
    router: MethodRouter,
    endpoint: String,
}

pub trait EndpointExt {
    /// Names the route for the HTTP metrics (INV-30).
    fn endpoint(self, endpoint: impl Into<String>) -> Named;
}

impl EndpointExt for MethodRouter {
    fn endpoint(self, endpoint: impl Into<String>) -> Named {
        Named {
            router: self,
            endpoint: endpoint.into(),
        }
    }
}

/// Declared at the route, like [`EndpointExt::endpoint`].
pub trait AuthExt {
    /// Needs a key on a portal with `auth` configured.
    fn auth(self) -> Classified;

    /// Served to anyone. Spelled out, not defaulted: leaving a route open
    /// should be a sentence somebody wrote.
    fn no_auth(self) -> Classified;
}

impl AuthExt for Named {
    fn auth(self) -> Classified {
        Classified {
            router: self.router,
            gated: true,
            endpoint: Some(self.endpoint),
        }
    }

    fn no_auth(self) -> Classified {
        Classified {
            router: self.router,
            gated: false,
            endpoint: Some(self.endpoint),
        }
    }
}

impl AuthExt for MethodRouter {
    fn auth(self) -> Classified {
        Classified {
            router: self,
            gated: true,
            endpoint: None,
        }
    }

    fn no_auth(self) -> Classified {
        Classified {
            router: self,
            gated: false,
            endpoint: None,
        }
    }
}

/// What a path was mounted as, for the test that asserts the whole surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mounted {
    Gated(&'static str),
    Open(&'static str),
    /// A router mounted whole, with the reason it answers without a key.
    Merged(&'static str),
}

/// A router whose only way to add a route demands that the route say so.
pub struct Gated {
    router: Router,
    gate: Option<Arc<Gate>>,
    inventory: Vec<Mounted>,
}

impl Gated {
    /// Without an `auth:` block there is no gate and nothing is wrapped.
    pub fn new(gate: Option<Arc<Gate>>) -> Self {
        Self {
            router: Router::new(),
            gate,
            inventory: Vec::new(),
        }
    }

    /// Every path this router mounted, in declaration order.
    pub fn inventory(&self) -> &[Mounted] {
        &self.inventory
    }

    pub fn route(mut self, path: &'static str, classified: Classified) -> Self {
        let Classified {
            router,
            gated,
            endpoint,
        } = classified;
        self.inventory.push(if gated {
            Mounted::Gated(path)
        } else {
            Mounted::Open(path)
        });
        let router = match (self.gate.clone(), gated) {
            (Some(gate), true) => {
                let dataset = dataset_param(path);
                // `layer`, not `route_layer`: the latter skips methods the
                // route does not declare, so a keyless method mismatch would
                // answer 405 without ever entering the OB-12 accounting. The
                // refusal still carries `Allow` — axum appends it outside
                // every layer — but the method map is public in the served
                // schema anyway; what this buys is the accounting and the
                // uniform status.
                router.layer(axum::middleware::from_fn(move |req, next| {
                    extractor::middleware(gate.clone(), dataset, req, next)
                }))
            }
            _ => router,
        };
        // Applied after the gate, hence outside it, so a refusal the gate
        // short-circuits still carries its endpoint name.
        let router = match endpoint {
            Some(endpoint) => router.layer(EndpointAnnotationLayer::new(endpoint)),
            None => router,
        };
        self.router = self.router.route(path, router);
        self
    }

    /// A router mounted whole. Nothing inside passes through [`Self::route`], so
    /// its routes answer without a key — hence the reason, which is recorded
    /// rather than discarded so the surface test can account for it.
    pub fn merge_ungated(mut self, why: &'static str, other: Router) -> Self {
        self.inventory.push(Mounted::Merged(why));
        self.router = self.router.merge(other);
        self
    }

    pub fn into_router(self) -> Router {
        self.router
    }
}

/// Whether the path names the dataset in the form a grant's claims use — the
/// canonical name.
///
/// The deprecated worker-query route carries a base64 `DatasetId` under
/// `:dataset_id`. `Datasets::default_name` would resolve it, so this is a choice
/// rather than a limit: the route is NG7 — unspecified and not to be pinned — and
/// teaching the gate a second dataset spelling would extend authorization onto a
/// surface the binding does not describe. Until it is either specified or removed,
/// it names no dataset and a dataset-scoped key is refused there, which is the
/// fail-closed direction (REQ-53, NG7, GAP-36).
fn dataset_param(path: &str) -> bool {
    let mut segments = path.trim_start_matches('/').split('/');
    matches!(
        (segments.next(), segments.next()),
        (Some("datasets"), Some(":dataset"))
    )
}

#[cfg(test)]
mod tests {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        routing::get,
    };
    use tower::ServiceExt;

    use super::*;
    use crate::auth::{test_support::gate_with, Enforcement};

    async fn status(gate: Option<Arc<Gate>>, classified: Classified) -> StatusCode {
        Gated::new(gate)
            .route("/probe", classified)
            .into_router()
            .oneshot(
                Request::builder()
                    .uri("/probe")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn a_gated_route_needs_a_key_and_an_open_one_does_not() {
        let gate = Some(gate_with(Enforcement::Enforce));

        let gated = status(gate.clone(), get(|| async { "served" }).auth()).await;
        assert_eq!(gated, StatusCode::FORBIDDEN);

        let open = status(gate, get(|| async { "served" }).no_auth()).await;
        assert_eq!(open, StatusCode::OK);
    }

    /// The kill switch: with no `auth:` block the gate is never
    /// installed, so an OSS build runs no authorization middleware at all.
    #[tokio::test]
    async fn nothing_is_gated_without_a_an_auth_config() {
        assert_eq!(
            status(None, get(|| async { "served" }).auth()).await,
            StatusCode::OK
        );
    }

    /// A method the route does not serve is still gated. `route_layer` would
    /// skip the middleware and answer 405, so a keyless client could map the
    /// gated surface — which paths exist, which methods they take —
    /// without ever appearing in the authorization accounting.
    #[tokio::test]
    async fn a_method_mismatch_is_refused_before_it_is_a_405() {
        let app = Gated::new(Some(gate_with(Enforcement::Enforce)))
            .route("/probe", axum::routing::post(|| async { "served" }).auth())
            .into_router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/probe")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    /// A refusal minted by the gate must still carry the endpoint name, or the
    /// HTTP metrics fall back to the raw request path — an unbounded series
    /// per spelling, minted by whoever sends keyless traffic (INV-30).
    #[tokio::test]
    async fn a_gate_refusal_is_labelled_with_its_endpoint_name() {
        use crate::utils::logging::EndpointName;

        let app = Gated::new(Some(gate_with(Enforcement::Enforce)))
            .route(
                "/probe",
                get(|| async { "served" }).endpoint("/probe-name").auth(),
            )
            .into_router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/probe")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            response
                .extensions()
                .get::<EndpointName>()
                .map(|endpoint| endpoint.0.as_str()),
            Some("/probe-name"),
        );
    }

    #[test]
    fn only_a_dataset_segment_counts_as_naming_one() {
        assert!(dataset_param("/datasets/:dataset/stream"));
        assert!(dataset_param(
            "/datasets/:dataset/timestamps/:timestamp/block"
        ));

        assert!(!dataset_param("/sql/query"));
        assert!(!dataset_param("/datasets"));
        assert!(!dataset_param("/metrics"));
        assert!(!dataset_param("/datasets/:other/stream"));
        // Deliberate, not incidental: the segment is a base64 `DatasetId`, not a
        // name a claim can be matched against, so a scoped key is refused here.
        assert!(!dataset_param("/datasets/:dataset_id/query/:worker_id"));
    }
}
