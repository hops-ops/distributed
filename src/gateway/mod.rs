//! Portable application gateway contracts (`gateway` feature).
//!
//! Configuration selects capabilities; linking this module starts no listeners,
//! executors, projectors or delivery coordinators. [`Gateway::dispatch`] fixes
//! route ownership before admission and execution. Native/Worker adapters own
//! HTTP bodies, credentials, streams, cancellation and connection lifetimes.
//!
//! ```
//! use distributed::gateway::*;
//! let gateway = GatewayConfig {
//!     bindings: vec![Binding::new("assets", BindingKind::Assets)],
//!     routes: vec![Route::new("ui", RoutePath::prefix("/"), "assets")],
//! }.build()?;
//! assert_eq!(gateway.select("GET", "/about")?.unwrap().route().id, "ui");
//! # Ok::<(), GatewayError>(())
//! ```

#![deny(missing_docs)]

mod auth;
mod config;
mod context;
mod extension;
mod route;

pub use config::{
    Binding, BindingKind, DeliveryCapabilities, Gateway, GatewayConfig, GatewayError,
    GraphqlCapabilities, GraphqlExecutor, MAX_BINDINGS, MAX_ID_BYTES, MAX_ROUTES,
};
pub use extension::{GatewayAdapter, Rejection};
pub use route::{Methods, Route, RoutePath, SelectedRoute, MAX_ADMISSIONS, MAX_PATH_BYTES};

pub use auth::{is_untrusted_identity_header, Admission, AuthError, AuthProvider};
pub use context::{BackendCredential, Credentials, Identity, RequestContext};

/// Native HTTP routing, streaming proxy and static asset adapter.
#[cfg(feature = "gateway-native")]
pub mod native;

/// Portable GraphQL operation selection and capability admission.
#[cfg(feature = "gateway-graphql")]
pub mod graphql;

/// Portable authenticated delivery identity and freshness contracts.
#[cfg(feature = "gateway-delivery")]
pub mod delivery;
