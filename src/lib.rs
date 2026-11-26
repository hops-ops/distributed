#![allow(clippy::module_inception)]
#![doc = include_str!("../README.md")]

//! # Public paths
//!
//! Every public item has exactly one path:
//!
//! - The crate root holds the event-sourcing core: [`Entity`] and its event
//!   records, [`Aggregate`] and [`AggregateRepository`], the [`Repository`]
//!   trait family and its backends, snapshots, and the attribute and
//!   function-like macros ([`sourced`], [`command`](macro@command), [`mutation!`],
//!   [`projection!`]).
//! - Everything else lives in its module (`distributed::command`,
//!   `distributed::microsvc`, ...). A derive lives with the trait it
//!   implements, as `serde::Serialize` does: `read_model::ReadModel`,
//!   `domain_event::{DomainEvent, DomainState}`, `table::JsonType`,
//!   `command::{CommandInput, CommandOutput}`.
//! - [`prelude`] re-exports what a typical aggregate, command or read-model
//!   file needs, for `use distributed::prelude::*;`.

// Allow proc-macros to reference this crate by name even when used internally
extern crate self as distributed;

/// Macro implementation support. External packages may rename the
/// distributed dependency and re-export it under the generated path; the
/// generated code must not require a direct serde dependency of its own.
#[doc(hidden)]
pub mod __private {
    pub use serde;
}

/// Service guard shared with the integration tests.
#[cfg(test)]
pub(crate) use common::env as test_env;

mod time;

// The event-sourcing core. These modules are private: their items are
// exported at the crate root below.
mod aggregate;
mod entity;
mod in_memory_repo;
#[cfg(feature = "postgres")]
mod postgres_repo;
mod queued_repo;
mod repository;
mod snapshot;
#[cfg(feature = "sqlite")]
mod sqlite_repo;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
mod sqlx_repo;

pub mod application;
pub mod bus;
pub mod clock;
pub mod command;
pub mod command_dispatch;
pub(crate) mod command_ledger;
pub mod domain_event;
#[cfg(feature = "gateway")]
pub mod gateway;
pub mod graphql;
pub mod lease;
pub mod lock;
#[cfg(feature = "metrics")]
pub mod metrics;
pub mod microsvc;
pub mod mutation;
pub mod outbox;
pub mod outbox_worker;
pub mod prelude;
pub mod projection;
pub mod projection_protocol;
pub mod read_model;
pub mod saga;
pub mod schedule;
pub mod table;
mod telemetry;
pub mod trace_context;

pub use aggregate::{hydrate, Aggregate, AggregateBuilder, AggregateRepository};
pub use entity::{
    upcast_events, upcast_events_for_replay, upcast_payload, BitcodePayloadCodec, Entity,
    EventRecord, EventRecordError, EventUpcaster, Recorded, UpcastError, BITCODE_PAYLOAD_CODEC,
    BITCODE_PAYLOAD_CODEC_VERSION,
};
pub use in_memory_repo::{InMemoryOutboxStore, InMemoryRepository};
#[cfg(feature = "postgres")]
pub use postgres_repo::{PostgresOutboxStore, PostgresRepository};
pub use queued_repo::{Queueable, QueuedRepository, ReadOpts};
pub use repository::{
    CommitBatch, GetStream, InboxReceipt, InboxStore, PreparedEventAppend, ReadModelWritePlanStore,
    RelationalReadModelQueryStore, Repository, RepositoryError, SnapshotStore, SnapshotWrite,
    StreamIdentity, StreamWrite, TransactionalCommit,
};
pub use snapshot::{hydrate_from_snapshot, InMemorySnapshotStore, SnapshotRecord, Snapshottable};
#[cfg(feature = "sqlite")]
pub use sqlite_repo::{SqliteOutboxStore, SqliteRepository};

pub type SourcedResult<T = ()> = std::result::Result<T, EventRecordError>;

pub use distributed_macros::{
    command, command_input_defaults, mutation, mutation_file, sourced, Snapshot,
};
