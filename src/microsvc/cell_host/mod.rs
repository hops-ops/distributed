//! Second command host: a celld Durable Object class analogue.
//!
//! Domain crates keep `CausalCommandContext` / `ctx.repo()`. This module is
//! the host adapter: one named cell per shard, private stream store, same
//! [`PortableCommand`] mounts as SOA `Routes`. It is **not** a sqlx dialect
//! and must not be gated behind `feature = "celld"` (`PCH-DEC-005`).
//!
//! Live celld fleet / CI is not required (`PCH-AC-006.1`). A workers-rs
//! `Send` tax stays in this adapter; cell types do not leak into domain
//! command declarations.
//!
//! The GraphQL wait-path (`CelldCommandHost`) is the same for every aggregate:
//! routes only supply kind, shard, and payload. Aggregate Workers own outbox
//! delivery through celld Queue.
//!
//! Durable storage is one SQLite database per cell: Durable Object
//! `storage.sql` on celld (`workers-rs`/wasm32, `sql_store.rs`). Durable
//! cells run on celld only, for local development and deployment alike;
//! there is no native durable cell backend. Native builds have the
//! in-process reference host, for unit tests (the cell host itself stays
//! ungated, `PCH-DEC-005`).

pub(crate) mod alarm;
pub(crate) mod causal;
mod cell;
#[cfg(feature = "workers-rs")]
mod celld_outbox;
#[cfg(feature = "graphql")]
mod command;
mod internal_auth;
#[cfg(all(feature = "workers-rs", target_arch = "wasm32"))]
mod sql_executor;
#[cfg(all(feature = "workers-rs", target_arch = "wasm32"))]
mod sql_store;
mod store;
mod wire;

// Schedule types under their old cell-host paths, for the host's own code.
#[allow(unused_imports)]
pub(crate) use crate::schedule::{
    schedule_session, ArmedSchedule, FailedSchedule, FiredSchedule, Schedule, ScheduleError,
    ScheduleFireReport, ScheduleWrite, HOST_NOW_CLAIM, MAX_SCHEDULE_WRITES_PER_COMMAND,
};
pub use causal::{
    CellCommandIdentity, CellDispatchError, CellDispatchResult, CELL_CAUSATION_ID_HEADER,
    CELL_PRINCIPAL_PARTITION_HEADER, CELL_SERVICE_ID_HEADER,
};
pub use cell::{instance_name, parent_cell_name, AggregateCell, CellBatchCommand, CellNamespace};
#[cfg(feature = "workers-rs")]
pub use celld_outbox::{
    CelldAlarmOutcome, CelldOutbox, CelldOutboxDrainOutcome, CELLD_OUTBOX_DEFAULT_BATCH_SIZE,
    CELLD_OUTBOX_DEFAULT_BINDING, CELLD_OUTBOX_DEFAULT_DRAIN_INTERVAL_MS,
    CELLD_OUTBOX_DEFAULT_LEASE, CELLD_OUTBOX_DEFAULT_MAX_ATTEMPTS, CELLD_OUTBOX_DRAIN_INTERVAL_ENV,
};
#[cfg(feature = "graphql")]
pub use command::{CelldCommandHost, CelldRoute};
pub use internal_auth::{
    InternalHttpSecret, CELL_INTERNAL_SECRET_ENV, CELL_INTERNAL_SECRET_HEADER,
};
pub use store::CellStreamStore;
#[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
pub use store::{
    DurableAggregateCellState, DurableCellCommand, DurableCellEvents, DurableCellSnapshot,
    SealedRowReducer, DURABLE_AGGREGATE_CELL_STATE_VERSION,
};
pub(crate) use wire::validate_cell_projection_events;
pub use wire::{
    cell_projection_event_evidence, parse_cell_projection_events, CellProjectionEventWireItem,
    CellWaitPathRequest, MAX_CELL_PROJECTION_EVENTS, MAX_CELL_PROJECTION_EVENT_PAYLOAD_BYTES,
    MAX_CELL_PROJECTION_EVENT_WIRE_BYTES,
};

#[cfg(test)]
mod tests;
