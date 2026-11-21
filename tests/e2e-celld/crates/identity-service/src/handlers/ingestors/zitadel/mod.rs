//! Zitadel Action/HTTP ingress + Management API scrape → provider messages only.
//!
//! Teaching fixture (simplified from gitkb domain-service):
//! 1. Authenticity (`auth`) — shared secret header, checked at the HTTP edge;
//!    it vouches for the ingestor identity the commands run as
//! 2. Map (`map`) — Action payload → typed `zitadel.*.v1` subjects
//! 3. Publish (`publish`) — outbox provider message, committed with the
//!    command's ledger receipt (no aggregate)
//! 4. Projector (`project_auth_user`) — upserts `auth_users` for GraphQL joins
//! 5. Scrape (`scrape`) — periodic Management API reconcile for missed events
//!
//! See `docs/zitadel-ingestor.md`.

mod auth;
mod handle;
mod map;
mod publish;
pub mod scrape;

pub use auth::{
    allow_action_events, configured_secret, ingestor_principal, ingestor_session,
    verify_authenticity, ALLOW_ACTION_EVENTS_ENV, INGESTOR_ROLE, INGESTOR_USER_ID, SECRET_ENV,
    SECRET_HEADER,
};
pub use handle::{command, handle, ingress_command_id, receive, ZitadelIngressPayload, COMMAND};
pub use map::{
    looks_like_action_event, map_action_delivery, normalize_ingress_body, MappedDelivery,
    ZitadelIngressEmail, ZitadelIngressInput, HUMAN_CREATED, HUMAN_DEACTIVATED, HUMAN_REACTIVATED,
    HUMAN_UPDATED, MACHINE_CREATED,
};
pub use publish::provider_message;
pub use scrape::{
    spawn_scrape_loop, ZitadelScrapeConfig, API_URL_ENV, INTERVAL_ENV, ON_START_ENV, TOKEN_ENV,
};

/// Provider event names published by this ingestor (never domain forgeries).
pub fn is_provider_message_name(name: &str) -> bool {
    name.starts_with("zitadel.")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_names_are_zitadel_prefixed() {
        for name in [
            HUMAN_CREATED,
            HUMAN_UPDATED,
            HUMAN_DEACTIVATED,
            HUMAN_REACTIVATED,
            MACHINE_CREATED,
        ] {
            assert!(is_provider_message_name(name), "{name}");
        }
    }
}
