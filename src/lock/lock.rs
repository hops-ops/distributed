use std::future::Future;
use std::sync::Arc;

use super::LockError;

/// A single lock instance whose operations yield to the executor instead of
/// blocking the OS thread.
///
/// Every operation is asynchronous because a lock may be backed by I/O: the
/// in-memory lock resolves immediately, but a durable lock (e.g. the SQLx
/// lease-table [`PostgresLock`](super::PostgresLock) /
/// [`SqliteLock`](super::SqliteLock)) acquires and releases by talking to the
/// database. `lock` awaits until the lock becomes free; `try_lock` attempts a
/// single non-blocking acquire; `unlock` releases the lock and (for in-memory)
/// wakes any waiters so they can re-contend.
///
/// All returned futures are `Send` so an async `QueuedRepository` built on this
/// lock keeps its repository futures `Send` (required by the async repo traits).
pub trait Lock: Send + Sync {
    /// Acquire the lock, awaiting until it becomes available.
    fn lock(&self) -> impl Future<Output = Result<(), LockError>> + Send + '_;

    /// Try to acquire the lock without waiting.
    ///
    /// Returns `Ok(true)` if acquired, `Ok(false)` if already held.
    fn try_lock(&self) -> impl Future<Output = Result<bool, LockError>> + Send + '_;

    /// Release the lock, waking any waiters so they can re-contend.
    ///
    /// Idempotent: releasing a lock this caller no longer holds (e.g. an
    /// expired distributed lease that was stolen) is a no-op success.
    fn unlock(&self) -> impl Future<Output = Result<(), LockError>> + Send + '_;

    /// Release from a `Drop`, which cannot await. Must not block. A lock
    /// that releases by I/O hands the release to a background task, or
    /// leaves it to its own expiry when there is nowhere to run one.
    fn release_detached(self: Arc<Self>);
}

/// A held [`Lock`]. It is the only thing that releases the lock: by
/// [`release`](Self::release), or when it is dropped.
pub struct LockGuard<L: Lock> {
    lock: Option<Arc<L>>,
}

impl<L: Lock> LockGuard<L> {
    /// Wait for `lock` and hold it.
    pub async fn acquire(lock: Arc<L>) -> Result<Self, LockError> {
        lock.lock().await?;
        Ok(Self { lock: Some(lock) })
    }

    /// Release and wait for the release to finish.
    pub async fn release(mut self) -> Result<(), LockError> {
        match self.lock.take() {
            Some(lock) => lock.unlock().await,
            None => Ok(()),
        }
    }
}

impl<L: Lock> Drop for LockGuard<L> {
    fn drop(&mut self) {
        if let Some(lock) = self.lock.take() {
            lock.release_detached();
        }
    }
}
