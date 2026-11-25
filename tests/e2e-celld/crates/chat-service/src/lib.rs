//! Chat service crate: lobby messages.
//!
//! SOA mounts stay in-process. The optional celld host wait-dispatches
//! `chat.post` through [`distributed::cell_host::CelldCommandHost`]. GraphQL
//! `@live` stays here.

mod host;
mod routes;

pub use host::celld_route;
pub use routes::routes;
