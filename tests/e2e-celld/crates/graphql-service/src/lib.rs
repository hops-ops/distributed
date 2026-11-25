//! GraphQL process for the celld example (sibling of e2e-ui, not `make run`).
//!
//! Todo create/complete wait-dispatch to celld. Chat, Blob, and identity stay
//! in-process via their service crates. Domain crates are the e2e-ui ones.

mod application;
mod host;
mod http;
pub mod modules;

pub use application::{
    ClientSurface, ADMIN_CLIENT_SURFACE, CLIENT_SURFACE, CLIENT_SURFACES, E2E_UI_APPLICATION,
    PUBLIC_CLIENT_SURFACE,
};
pub use e2e_celld_identity::{spawn_scrape_loop, ZitadelScrapeConfig};
pub use e2e_readmodels::read_models;
pub use host::{run, HostOptions};
pub use modules::compose::build_service;
pub use modules::graphql::{
    application, application_manifest, build_graphql_engine, dev_identity,
    distributed_admin_client_surface, distributed_client_surface,
    distributed_public_client_surface, identity_from_env, oidc_bearer_config,
};
