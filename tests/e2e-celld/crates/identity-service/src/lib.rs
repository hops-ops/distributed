//! Identity service crate: Zitadel ingress/scrape + AuthUsers projection.
//!
//! Not a cell and no aggregate. Chat and Blob stay in their own crates; this
//! only owns IdP import so AuthUsers joins are not mounted on the chat aggregate.

mod bounds;
mod deps;
pub mod handlers;
mod routes;

pub use handlers::ingestors::zitadel::{spawn_scrape_loop, ZitadelScrapeConfig};
pub use routes::{routes, COMMAND_PREFIX, MODULE_ID};
