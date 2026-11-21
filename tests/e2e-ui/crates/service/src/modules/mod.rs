//! Bounded-context application modules for e2e-ui.
//!
//! Each module owns its command/projection mounts. [`compose`] lists them
//! into one Service; [`graphql`] owns surfaces and the query engine.

pub mod blob;
pub mod chat;
pub mod compose;
pub mod graphql;
pub mod identity;
pub mod projections;
pub mod todo;
