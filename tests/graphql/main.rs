//! The GraphQL engine, its HTTP and subscription transports, identity and OIDC providers.
//!
//! One test binary for the area: each former `tests/<name>` target is a
//! module, so `cargo test --test graphql <name>::` runs what `--test <name>` ran.

#[cfg(all(feature = "graphql", feature = "sqlite"))]
mod graphql_causal_transport;
#[cfg(feature = "graphql")]
mod graphql_commands;
#[cfg(all(feature = "graphql", feature = "sqlite"))]
mod graphql_compile;
#[cfg(all(feature = "graphql", feature = "sqlite"))]
mod graphql_engine;
#[cfg(all(feature = "graphql", feature = "sqlite"))]
mod graphql_harden;
#[cfg(all(feature = "graphql", feature = "sqlite"))]
mod graphql_http;
#[cfg(all(feature = "graphql", feature = "sqlite"))]
mod graphql_identity;
#[cfg(all(feature = "graphql", any(feature = "sqlite", feature = "postgres")))]
mod graphql_literal_text;
#[cfg(all(feature = "graphql", feature = "sqlite"))]
mod graphql_oidc_authentik;
#[cfg(all(feature = "graphql", feature = "sqlite"))]
mod graphql_oidc_keycloak;
#[cfg(all(feature = "graphql", feature = "sqlite"))]
mod graphql_oidc_zitadel;
// `load` and most helpers drive the native gateway.
#[cfg(all(feature = "gateway-graphql-native", feature = "sqlite"))]
mod graphql_query_protocol;
mod graphql_sdl;
#[cfg(all(feature = "graphql", feature = "sqlite"))]
mod graphql_sqlite;
#[cfg(all(feature = "graphql", feature = "sqlite"))]
mod graphql_subscriptions_sqlite;
#[cfg(all(feature = "graphql", feature = "sqlite"))]
mod graphql_unique_key_live;
