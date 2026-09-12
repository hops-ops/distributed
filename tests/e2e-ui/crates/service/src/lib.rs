//! e2e-ui service library — application modules + host.
//!
//! # Authoring story
//!
//! - [`application`] — surface names and module inventory
//! - [`modules`] — todo / chat / blob mounts + compose + GraphQL
//! - [`host`] — one-screen process bootstrap (`run`)
//! - [`handlers`] — command/event bodies (domain-adjacent, not infrastructure)
//!
//! Domain crates stay pure aggregates; read models live in `e2e-readmodels`;
//! portable projections in `e2e-projections`.

mod application;
mod bounds;
mod deps;
pub mod handlers;
mod host;
mod http;
pub mod modules;

pub use application::{
    DISTRIBUTED_ADMIN_CLIENT_SURFACE, DISTRIBUTED_CLIENT_SURFACE,
    DISTRIBUTED_PUBLIC_CLIENT_SURFACE, E2E_UI_APPLICATION, E2E_UI_MODULE_IDS,
};
pub use e2e_readmodels::distributed_manifest;
pub use handlers::ingestors::zitadel::{
    scrape_users_to_outbox, spawn_scrape_loop, ScrapeReport, ZitadelScrapeConfig,
};
pub use host::{run, HostOptions};
pub use http::{gateway_router, serve};
pub use modules::compose::build_service;
pub use modules::graphql::{
    application_manifest, build_graphql_engine, dev_identity, distributed_admin_client_surface,
    distributed_client_surface, distributed_public_client_surface, identity_from_env,
    oidc_bearer_config,
};
