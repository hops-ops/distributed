//! The gateway: routing, auth, extensions, UI, GraphQL operations and edge query delivery.
//!
//! One test binary for the area: each former `tests/<name>` target is a
//! module, so `cargo test --test gateway <name>::` runs what `--test <name>` ran.
//!
//! `edge_query_delivery`, `gateway_auth`, `gateway_extensions`,
//! `gateway_graphql_operation`, `gateway_routing` and `gateway_ui` are also the
//! crate roots of the `tests/gateway-portable` and `tests/gateway-native`
//! fixtures, so they keep their own `#![cfg]`. Those that declare
//! `mod gateway_support;` are loaded with `#[path]`, which makes that
//! declaration resolve to `tests/gateway/gateway_support/` in both builds.

#[cfg(feature = "gateway")]
mod edge_query_delivery;
#[cfg(all(feature = "gateway", feature = "graphql", feature = "postgres"))]
mod edge_query_delivery_postgres;
#[cfg(feature = "gateway")]
#[path = "gateway_auth.rs"]
mod gateway_auth;
#[cfg(feature = "gateway")]
#[path = "gateway_extensions.rs"]
mod gateway_extensions;
#[cfg(all(feature = "gateway-graphql-native", feature = "sqlite"))]
mod gateway_graphql;
#[cfg(feature = "gateway")]
mod gateway_graphql_operation;
#[cfg(feature = "gateway")]
#[path = "gateway_routing.rs"]
mod gateway_routing;
#[cfg(feature = "gateway-native")]
mod gateway_ui;
