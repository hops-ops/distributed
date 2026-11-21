use std::sync::Arc;

use super::{Lock, LockError};

/// Factory for per-entity (or per-key) [`Lock`]s.
///
/// `QueuedRepository` uses a [`LockManager`] to obtain a lock for each
/// aggregate stream. The default [`InMemoryLockManager`](super::InMemoryLockManager)
/// stores locks in a `HashMap`; distributed implementations can talk to Redis,
/// Postgres advisory locks, or another lease backend.
pub trait LockManager: Send + Sync {
    /// The concrete lock type returned by this manager.
    type Lock: Lock + 'static;

    /// Get (or create) a lock for the given identifier.
    ///
    /// Repeated calls with the same `id` must return the same logical lock
    /// (i.e. the same `Arc` for in-memory, or the same distributed key).
    fn get_lock(&self, id: &str) -> Result<Arc<Self::Lock>, LockError>;
}
