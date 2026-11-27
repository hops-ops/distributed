//! Zitadel Action/HTTP ingress + Management API scrape → user profiles.
//!
//! Teaching fixture (simplified from gitkb domain-service):
//! 1. Authenticity (`auth`) — shared secret header, checked by the command's
//!    HTTP edge; it vouches for the ingestor identity the commands run as
//! 2. Map (`map`) — Action payload → the `zitadel.*.v1` event it is and the
//!    user's [`ZitadelUserProfile`](e2e_projections::ZitadelUserProfile)
//! 3. Publish — `publish_external`: the profile as an external fact,
//!    committed with the command's ledger receipt (no aggregate)
//! 4. Projection (`e2e_projections::AUTH_USERS`) — upserts `auth_users` for
//!    GraphQL joins
//! 5. Scrape (`scrape`) — periodic Management API reconcile for missed events
//!
//! See `docs/zitadel-ingestor.md`.

mod auth;
mod handle;
mod map;
pub mod scrape;

pub use auth::{
    allow_action_events, configured_secret, ingestor_principal, ingestor_session,
    verify_authenticity, ALLOW_ACTION_EVENTS_ENV, INGESTOR_ROLE, INGESTOR_USER_ID, SECRET_ENV,
    SECRET_HEADER,
};
pub use handle::{command, handle, http_edge, ingress_command_id, ZitadelIngressPayload, COMMAND};
pub use map::{
    looks_like_action_event, map_action_delivery, normalize_ingress_body, user_profile,
    MappedDelivery, ZitadelIngressEmail, ZitadelIngressInput, HUMAN_CREATED, HUMAN_DEACTIVATED,
    HUMAN_REACTIVATED, HUMAN_UPDATED, MACHINE_CREATED,
};
pub use scrape::{
    spawn_scrape_from_env, spawn_scrape_loop, ZitadelScrapeConfig, API_URL_ENV, INTERVAL_ENV,
    ON_START_ENV, TOKEN_ENV,
};
