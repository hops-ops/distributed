mod error;
mod identity;
mod inbox;
#[cfg(any(
    feature = "sqlite",
    feature = "postgres",
    all(feature = "workers-rs", target_arch = "wasm32")
))]
pub(crate) mod migrations;
pub(crate) mod sql;
pub(crate) mod sqlite_codec;
mod traits;
mod validation;

pub use error::RepositoryError;
pub(crate) use error::CONCURRENT_WRITE_RETRIES;
pub use identity::StreamIdentity;
pub use inbox::{InboxOutcome, InboxReceipt};
pub use traits::{
    CommitBatch, GetStream, InboxStore, PreparedEventAppend, ReadModelWritePlanStore,
    RelationalReadModelQueryStore, Repository, SnapshotStore, SnapshotWrite, StreamWrite,
    TransactionalCommit,
};
pub(crate) use validation::{
    require_captured_events_are_published, validate_commit_batch, validate_snapshot_identity,
};
