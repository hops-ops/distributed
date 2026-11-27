//! The e2e-ui application on another command host.
//!
//! The modules, projections, GraphQL surfaces and identity ingestor are
//! `e2e-service`'s, mounted unchanged through `build_service`. This crate adds
//! only what celld changes: Todo and Chat commands wait-dispatch to their
//! cells ([`CelldCommandHost::route_aggregate`]), the bus is NATS, and the
//! cells' Queue relay is an internal route.
//!
//! [`CelldCommandHost::route_aggregate`]: distributed::microsvc::cell_host::CelldCommandHost::route_aggregate

mod host;
mod http;

pub use e2e_service::{application_manifest, identity_from_env};
pub use host::{run, HostOptions};
