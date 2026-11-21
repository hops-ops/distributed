//! microsvc integration tests.

#[path = "../support/commands.rs"]
mod commands;

mod basic;
mod convention;
mod handlers;
mod models;
mod session;
mod transport_listen;
mod transport_subscribe;

#[cfg(feature = "http")]
mod transport_http;

#[cfg(feature = "grpc")]
mod transport_grpc;
