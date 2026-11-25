//! e2e-ui projection mounts — product declaration only.
//!
//! Each projection is mounted from its `projection!` descriptor, which names
//! it, pins its epoch and its model. Topology, catalog activation, and Surface
//! packaging come from [`distributed::LocalProjectionMounts`].

use distributed::LocalProjectionMounts;
use e2e_projections::{AUTH_USERS, BLOB_GAMES, CHAT_MESSAGES, TODOS};

/// Every projection of the application, mounted for one local process.
pub fn projections() -> LocalProjectionMounts {
    LocalProjectionMounts::builder("e2e-ui", "ordered-domain-events")
        .eventual(TODOS)
        .eventual(CHAT_MESSAGES)
        .direct(BLOB_GAMES)
        .eventual(AUTH_USERS)
        .build()
        .expect("e2e-ui projection catalog")
}
