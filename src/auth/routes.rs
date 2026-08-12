//! How a route says whether it needs a key.
//!
//! [`Gated::route`] takes only a [`Classified`], so a route that says neither
//! does not compile — a wrapper you can forget to write is open by omission.
//! [`Gated::merge_ungated`] cannot see inside what it mounts, so
//! [`Gated::inventory`] records both kinds and a test asserts the surface
//! against it (REQ-51).

use std::sync::Arc;

use axum::{routing::MethodRouter, Router};

use super::{extractor, usage, Gate};
use crate::utils::logging::EndpointAnnotationLayer;

/// A method router that has declared whether it needs a key.
pub struct Classified {
    router: MethodRouter,
    gated: bool,
    endpoint: Option<String>,
}

/// A method router carrying the name its metrics go under. [`Gated::route`]
/// applies the layer outermost, so a refusal the gate mints is labelled too —
/// otherwise the metrics fall back to the raw path, one series per spelling an
/// unauthenticated client invents.
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
                // The label a usage record carries. Fixed at mount time and
                // shared by every request to the route, so the tap copies a
                // pointer rather than a string.
                let label: Arc<str> = Arc::from(endpoint.as_deref().unwrap_or(path));
                // The egress tap goes on *inside* the gate: it reads the
                // attribution the gate deposits on the request, which only an
                // inner layer can see. With no `usage:` block there is no sink
                // and nothing is wrapped at all — the route is byte-for-byte
                // what it is on a portal that never heard of measurement.
                let router = match gate.usage() {
                    Some(sink) => router.layer(usage::tap_layer(sink)),
                    None => router,
                };
                // `layer`, not `route_layer`: the latter skips undeclared
                // methods, so a keyless method mismatch would answer 405
                // without entering the OB-12 accounting.
                router.layer(axum::middleware::from_fn(move |req, next| {
                    extractor::middleware(gate.clone(), dataset, label.clone(), req, next)
                }))
            }
            _ => router,
        };
        // Outside the gate, so a short-circuited refusal keeps its name.
        let router = match endpoint {
            Some(endpoint) => router.layer(EndpointAnnotationLayer::new(endpoint)),
            None => router,
        };
        self.router = self.router.route(path, router);
        self
    }

    /// Nothing inside passes through [`Self::route`], so its routes answer
    /// without a key — hence the reason, recorded for the surface test.
    pub fn merge_ungated(mut self, why: &'static str, other: Router) -> Self {
        self.inventory.push(Mounted::Merged(why));
        self.router = self.router.merge(other);
        self
    }

    pub fn into_router(self) -> Router {
        self.router
    }
}

/// Whether the path names the dataset the way a grant's claims do.
///
/// The deprecated worker-query route carries a base64 `DatasetId` instead. That
/// is resolvable, so excluding it is a choice: the route is NG7, and teaching
/// the gate a second spelling would extend authorization onto a surface the
/// binding does not describe. It names no dataset, so a scoped key is refused
/// there — the fail-closed direction (REQ-53, NG7, GAP-36).
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
