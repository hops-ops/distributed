mod aggregate;
mod repository;

pub(crate) use aggregate::replay_into;
pub use aggregate::{hydrate, Aggregate};
pub(crate) use repository::{stream_identity_for, SnapshotPolicy, StreamReads};
pub use repository::{AggregateBuilder, AggregateRepository};
