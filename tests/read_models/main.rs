//! Read models, projections and the distributed read-model services.
//!
//! One test binary for the area: each former `tests/<name>` target is a
//! module, so `cargo test --test read_models <name>::` runs what `--test <name>` ran.

mod distributed_read_model;
mod distributed_read_model_board;
mod domain_event_projection_server;
mod named_read_models;
mod read_model_keyset;
mod read_model_metadata;
mod read_model_relationship_includes;
mod read_model_schema_bootstrap;
mod read_model_session;
