mod aggregate;
mod repository;

pub use aggregate::{hydrate, Aggregate};
pub use repository::{AggregateBuilder, AggregateRepository};
pub(crate) use repository::{SnapshotPolicy, StreamReads};
