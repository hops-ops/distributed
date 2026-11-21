//! Lock - Pluggable locking abstractions
//!
//! This module provides traits and implementations for per-entity locking,
//! used by `QueuedRepository` to serialize aggregate access.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │              LockManager (per repository)                    │
//! │  - get_lock(id) → Arc<Lock>                                 │
//! └─────────────────────────────────────────────────────────────┘
//!                            │
//!                            ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │                     Lock Trait                               │
//! │  lock() / try_lock() / unlock()                              │
//! └─────────────────────────────────────────────────────────────┘
//!          │                  │                     │
//!          ▼                  ▼                     ▼
//! ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
//! │  InMemoryLock   │  │   PostgresLock   │  │    SqliteLock    │
//! │   (process-local)│  │ (durable lease,  │  │ (durable lease,  │
//! │                  │  │  feature=postgres│  │  feature=sqlite) │
//! └──────────────────┘  └──────────────────┘  └──────────────────┘
//! ```
//!
//! The SQLx locks ([`PostgresLock`]/[`SqliteLock`]) persist a per-stream lease in
//! the `aggregate_locks` table so a `QueuedRepository` can serialize aggregate
//! access across processes.

mod error;
mod in_memory;
mod lock;
mod manager;
#[cfg(feature = "postgres")]
mod postgres_lock;
#[cfg(feature = "sqlite")]
mod sqlite_lock;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
mod sqlx_common;

pub use error::{LockError, RetryClass};
pub use in_memory::{InMemoryLock, InMemoryLockFuture, InMemoryLockManager};
pub use lock::{Lock, LockGuard};
pub use manager::LockManager;
#[cfg(feature = "postgres")]
pub use postgres_lock::{PostgresDialect, PostgresLock, PostgresLockManager};
#[cfg(feature = "sqlite")]
pub use sqlite_lock::{SqliteDialect, SqliteLock, SqliteLockManager};
#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub use sqlx_common::{LockDialect, SqlxLock, SqlxLockManager};
