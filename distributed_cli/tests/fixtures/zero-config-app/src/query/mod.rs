//! GraphQL query exposure (deny-by-default permissions).
//!
//! One module per exposed read model. Command mutations are derived from the
//! executable service's typed causal inventory.

pub mod roles;
pub mod zero_config_app;

use distributed::graphql::{GraphqlBuildError, GraphqlEngine, GraphqlPoolSource};
use distributed::microsvc::Service;

/// Build the GraphQL engine for this service.
///
/// `DATABASE_URL` is used by `service::build_with_graphql`; defaults to an
/// in-memory SQLite database when unset (dev only).
pub fn build_engine(
    source: impl Into<GraphqlPoolSource>,
    service: &Service,
    protocol_token_key: [u8; 32],
) -> Result<GraphqlEngine, GraphqlBuildError> {
    // Tighten grants by replacing grant_all with one
    //   .model::<module::Model>(module::permissions())
    // per model (zero_config_app) after filling permissions() in each model module.
    // GraphiQL policy lives in `distributed::graphql::graphiql_enabled_from_env`
    // (GRAPHIQL override; RUST_ENV/ENV/APP_ENV production → off; else on).
    let graphiql = distributed::graphql::graphiql_enabled_from_env();
    // Public GraphQL identity (D6/D7): always OidcBearer + require_auth=true.
    // Set OIDC_ISSUER + OIDC_AUDIENCE (or OIDC_CLIENT_ID). Unset → placeholder
    // issuer (still OidcBearer; ambient headers never trusted). For local
    // GraphiQL ambient headers only, pass `IdentityConfig::DevHeaders` explicitly.
    let identity = distributed::graphql::public_oidc_identity_from_env();
    GraphqlEngine::from_schema_catalog(&crate::read_model_catalog(), source)?
        .service(service)
        .protocol_token_key(protocol_token_key)
        .roles(roles::ALL)
        .grant_all(roles::USER)
        .graphiql(graphiql)
        .identity(identity)
        .build()
}
