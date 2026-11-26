mod error;
mod identity;
mod inbox;
#[cfg(any(
    feature = "sqlite",
    feature = "postgres",
    all(feature = "cloudflare-workers", target_arch = "wasm32")
))]
pub(crate) mod migrations;
#[cfg(any(
    feature = "sqlite",
    feature = "postgres",
    all(feature = "cloudflare-workers", target_arch = "wasm32")
))]
pub(crate) mod sql;
#[cfg(any(
    feature = "sqlite",
    feature = "postgres",
    all(feature = "cloudflare-workers", target_arch = "wasm32")
))]
pub(crate) mod sqlite_codec;
mod traits;
mod validation;

pub use error::RepositoryError;
pub(crate) use error::CONCURRENT_WRITE_RETRIES;
pub use identity::StreamIdentity;
pub use inbox::InboxReceipt;
pub use traits::{
    CommitBatch, GetStream, InboxStore, PreparedEventAppend, ReadModelWritePlanStore,
    RelationalReadModelQueryStore, Repository, SnapshotStore, SnapshotWrite, StreamWrite,
    TransactionalCommit,
};
pub(crate) use validation::{validate_commit_batch, validate_snapshot_identity};
