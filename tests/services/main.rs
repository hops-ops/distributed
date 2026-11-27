//! Services built on the framework: microsvc, sagas, example domains and the live celld host.
//!
//! One test binary for the area: each former `tests/<name>` target is a
//! module, so `cargo test --test services <name>::` runs what `--test <name>` ran.

mod bomberman;
mod celld;
mod e2e_ui_celld_nats_profile;
mod microsvc;
#[cfg(feature = "runtime-tokio")]
mod saga_transfer;
mod sagas;
mod todos;
