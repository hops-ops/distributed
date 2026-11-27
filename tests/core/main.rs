//! Aggregates, commands, typed commands, event sourcing, snapshots, upcasting and replay.
//!
//! One test binary for the area: each former `tests/<name>` target is a
//! module, so `cargo test --test core <name>::` runs what `--test <name>` ran.

mod aggregate_command;
mod aggregate_less_commands;
mod application_composition;
#[cfg(feature = "graphql")]
mod causal_public_invoke;
#[cfg(all(feature = "graphql", feature = "http"))]
mod causal_wait_path;
mod core_command_contract;
mod replay_event_version;
mod replay_property;
mod snapshots;
mod sourced;
mod sourced_snapshot;
#[cfg(all(feature = "graphql", feature = "sqlite"))]
mod typed_commands;
mod upcasting;
