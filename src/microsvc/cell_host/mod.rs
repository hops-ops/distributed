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
//! Durable storage is one SQLite database per cell on both targets, through
//! one shared store (`sql_store.rs`): Durable Object `storage.sql` on
//! `workers-rs`/wasm32, and a synchronous rusqlite file per cell on native
//! hosts with the `cell-sqlite` feature (`CellSqlitePath`,
//! `AggregateCell::open_sqlite`). `cell-sqlite` selects a storage backend
//! only; the cell host itself stays ungated (`PCH-DEC-005`). Without it,
//! native cells use the in-process reference host.

pub(crate) mod causal;
mod cell;
#[cfg(feature = "workers-rs")]
mod celld_outbox;
#[cfg(feature = "graphql")]
mod command;
mod internal_auth;
#[cfg(all(feature = "cell-sqlite", not(target_arch = "wasm32")))]
mod native_sqlite;
#[cfg(all(feature = "workers-rs", target_arch = "wasm32"))]
mod sql_executor;
// Native durable cells reuse the Durable Object store over a rusqlite executor
// with the same `read`/`transaction` surface (see `native_sql_executor.rs`).
#[cfg(all(feature = "cell-sqlite", not(target_arch = "wasm32")))]
#[path = "native_sql_executor.rs"]
mod sql_executor;
#[cfg(any(
    all(feature = "workers-rs", target_arch = "wasm32"),
    all(feature = "cell-sqlite", not(target_arch = "wasm32"))
))]
mod sql_store;
mod store;
mod wire;

pub use causal::{
    CellCommandIdentity, CellDispatchError, CellDispatchResult, CELL_CAUSATION_ID_HEADER,
    CELL_PRINCIPAL_PARTITION_HEADER, CELL_SERVICE_ID_HEADER,
};
pub use cell::{instance_name, parent_cell_name, AggregateCell, CellNamespace};
#[cfg(feature = "workers-rs")]
pub use celld_outbox::{
    CelldOutbox, CelldOutboxDrainOutcome, CELLD_OUTBOX_DEFAULT_BATCH_SIZE,
    CELLD_OUTBOX_DEFAULT_BINDING, CELLD_OUTBOX_DEFAULT_DRAIN_INTERVAL_MS,
    CELLD_OUTBOX_DEFAULT_LEASE, CELLD_OUTBOX_DEFAULT_MAX_ATTEMPTS, CELLD_OUTBOX_DRAIN_INTERVAL_ENV,
};
#[cfg(feature = "graphql")]
pub use command::{CelldCommandHost, CelldRoute};
pub use internal_auth::{
    InternalHttpSecret, CELL_INTERNAL_SECRET_ENV, CELL_INTERNAL_SECRET_HEADER,
};
#[cfg(all(feature = "cell-sqlite", not(target_arch = "wasm32")))]
pub use native_sqlite::{CellOutboxStore, CellSqlitePath, NATIVE_CELL_SQLITE_EXTENSION};
#[cfg(all(feature = "cell-sqlite", not(target_arch = "wasm32")))]
pub use sql_store::CellSqlOutboxStore;
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
