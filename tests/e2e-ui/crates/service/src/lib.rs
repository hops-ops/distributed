//! e2e-ui service library — application modules + host.
//!
//! # Authoring story
//!
//! - [`application`] — application name and client surfaces
//! - [`modules`] — todo / chat / blob / identity mounts + compose + GraphQL
//! - [`host`] — one-screen process bootstrap (`run`)
//! - [`handlers`] — command bodies of the Zitadel ingress (domain-adjacent)
//!
//! Domain crates stay pure aggregates; read models live in `e2e-readmodels`;
//! portable projections in `e2e-projections`.

mod application;
pub mod handlers;
mod host;
mod http;
pub mod modules;

pub use application::{
    ClientSurface, ADMIN_CLIENT_SURFACE, CLIENT_SURFACE, CLIENT_SURFACES, E2E_UI_APPLICATION,
    PUBLIC_CLIENT_SURFACE,
};
pub use e2e_readmodels::read_models;
pub use handlers::ingestors::zitadel::{spawn_scrape_loop, ZitadelScrapeConfig};
pub use host::{run, HostOptions};
pub use http::{gateway_router, serve};
pub use modules::compose::build_service;
pub use modules::graphql::{
    application, application_manifest, build_graphql_engine, dev_identity,
    distributed_admin_client_surface, distributed_client_surface,
    distributed_public_client_surface, identity_from_env, oidc_bearer_config,
};
