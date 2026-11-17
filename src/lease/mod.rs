//! Lease - TTL leases with monotonic fencing tokens.
//!
//! A [`Lease`] grants one *holder* exclusive ownership of a *key* (a boss
//! controller, a play-session item, an encounter settlement, ...) until its
//! TTL elapses. Holders [`renew`](LeaseStore::renew) to keep it and
//! [`release`](LeaseStore::release) to hand it back early. A crashed holder's
//! lease simply expires and becomes acquirable by someone else.
//!
//! ## Fencing
//!
//! A TTL alone cannot stop a paused/partitioned former holder from writing
//! after its lease was taken over. Every successful acquisition therefore
//! carries a [`FencingToken`] that is **strictly greater** than every token
//! previously issued for that key — the counter survives release and expiry.
//! Renewal keeps the token (same acquisition). Writers attach the token to
//! every protected write, and the resource rejects tokens older than the
//! newest it has seen:
//!
//! - embed a [`Fence`] in the protected aggregate/state and call
//!   [`Fence::admit`] before applying a write (the robust pattern: the check
//!   is atomic with the write because it lives in the same cell), or
//! - ask the store with [`LeaseStore::check_fence`] (a point-in-time check —
//!   useful for diagnostics and cheap early rejection, but not atomic with a
//!   write elsewhere).
//!
//! ## Why a sibling of [`crate::lock`]
//!
//! The [`lock`](crate::lock) module is an internal mutual-exclusion
//! optimisation behind `QueuedRepository`: its `Lock` trait is a
//! lock/unlock gate with no holder identity, no renewal and no token, and its
//! `aggregate_locks` rows are deleted on release (so a token stored there
//! would reset). Bolting fencing onto that trait would change its contract for
//! every repository; this module keeps a separate, explicit API and a separate
//! `coordination_leases` table whose rows are retained after release so the
//! token stays monotonic.
//!
//! ## Backends
//!
//! - [`InMemoryLeaseStore`] — process-local, driven by an injectable
//!   [`Clock`](crate::clock::Clock) (use a
//!   [`ManualClock`](crate::clock::ManualClock) in tests).
//! - `SqliteLeaseStore` / `PostgresLeaseStore` (features `sqlite` /
//!   `postgres`) — durable, cross-process, evaluated against the **database
//!   clock** so processes never disagree about expiry. The table is created
//!   by the repository migrations (`0010_coordination_leases`) or standalone
//!   with `migrate`.
//!
//! ```ignore
//! use distributed::lease::{InMemoryLeaseStore, LeaseStore, Fence};
//! use std::time::Duration;
//!
//! let leases = InMemoryLeaseStore::new();
//! let lease = leases.acquire("boss:hydra", "zone-7", Duration::from_secs(15)).await?;
//! // ... every 5s:
//! let lease = leases.renew(&lease, Duration::from_secs(15)).await?;
//! // the protected state rejects writes from a superseded controller:
//! boss.fence.admit("boss:hydra", lease.token)?;
//! ```

mod fence;
mod in_memory;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
mod sqlx_store;

pub use fence::Fence;
pub use in_memory::InMemoryLeaseStore;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub use sqlx_store::{LeaseDialect, SqlxLeaseStore};
#[cfg(feature = "postgres")]
pub use sqlx_store::{PostgresLeaseDialect, PostgresLeaseStore};
#[cfg(feature = "sqlite")]
pub use sqlx_store::{SqliteLeaseDialect, SqliteLeaseStore};

use std::fmt;
use std::future::Future;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use crate::lock::RetryClass;

/// Monotonic per-key fencing token. Every acquisition of a key gets a token
/// strictly greater than all earlier acquisitions of that key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FencingToken(u64);

impl FencingToken {
    /// Wrap a raw token value (e.g. one carried in a command payload).
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// The raw token value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for FencingToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A held lease: `holder` owns `key` until `expires_at`, fenced by `token`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lease {
    pub key: String,
    pub holder: String,
    pub token: FencingToken,
    /// Expiry as seen by the store's clock (the database clock for SQL stores).
    pub expires_at: SystemTime,
}

impl Lease {
    /// Whether the lease has expired at `now` (by the caller's clock — the
    /// store's own check is authoritative).
    pub fn is_expired_at(&self, now: SystemTime) -> bool {
        self.expires_at <= now
    }
}

/// Lease operation failure.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum LeaseError {
    /// `acquire`: another holder owns an unexpired lease on the key.
    Held {
        key: String,
        holder: String,
        expires_at: SystemTime,
    },
    /// `renew`/`check_fence`: the presented lease is no longer the live
    /// lease for the key (expired, released, or superseded).
    Lost { key: String },
    /// A write carried a fencing token older than the newest one seen.
    StaleFence {
        key: String,
        presented: FencingToken,
        current: FencingToken,
    },
    /// Invalid arguments (empty key/holder, zero TTL).
    Invalid(String),
    /// A transient backend failure (I/O, `SQLITE_BUSY`, pool timeout).
    Backend(String),
}

impl LeaseError {
    /// Retry classification, shared vocabulary with [`crate::lock::LockError`].
    pub fn kind(&self) -> RetryClass {
        match self {
            LeaseError::Held { .. } | LeaseError::Backend(_) => RetryClass::Retryable,
            LeaseError::Lost { .. } | LeaseError::StaleFence { .. } | LeaseError::Invalid(_) => {
                RetryClass::Permanent
            }
        }
    }

    /// Whether a retry may succeed.
    pub fn is_retryable(&self) -> bool {
        self.kind().is_retryable()
    }
}

impl fmt::Display for LeaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LeaseError::Held { key, holder, .. } => {
                write!(f, "lease `{key}` is held by `{holder}`")
            }
            LeaseError::Lost { key } => write!(f, "lease `{key}` is no longer held"),
            LeaseError::StaleFence {
                key,
                presented,
                current,
            } => write!(
                f,
                "stale fencing token {presented} for `{key}` (current {current})"
            ),
            LeaseError::Invalid(message) => write!(f, "invalid lease request: {message}"),
            LeaseError::Backend(message) => write!(f, "lease backend error: {message}"),
        }
    }
}

impl std::error::Error for LeaseError {}

/// Store of TTL leases with monotonic fencing tokens.
pub trait LeaseStore: Send + Sync {
    /// Acquire `key` for `holder` for `ttl`.
    ///
    /// - free, expired or released key: a new lease with the next token;
    /// - live lease already held by `holder`: that lease, unchanged
    ///   (idempotent on a lost-response retry — call [`renew`](Self::renew)
    ///   to extend it);
    /// - live lease held by someone else: [`LeaseError::Held`].
    fn acquire<'a>(
        &'a self,
        key: &'a str,
        holder: &'a str,
        ttl: Duration,
    ) -> impl Future<Output = Result<Lease, LeaseError>> + Send + 'a;

    /// Extend a live lease to `now + ttl`, keeping its token. Fails with
    /// [`LeaseError::Lost`] if it expired or was superseded — the holder must
    /// stop acting and re-acquire (getting a new token).
    fn renew<'a>(
        &'a self,
        lease: &'a Lease,
        ttl: Duration,
    ) -> impl Future<Output = Result<Lease, LeaseError>> + Send + 'a;

    /// Release a lease early. Returns `false` (not an error) when it was no
    /// longer held. The token counter is retained.
    fn release<'a>(
        &'a self,
        lease: &'a Lease,
    ) -> impl Future<Output = Result<bool, LeaseError>> + Send + 'a;

    /// The live lease on `key`, if any.
    fn current<'a>(
        &'a self,
        key: &'a str,
    ) -> impl Future<Output = Result<Option<Lease>, LeaseError>> + Send + 'a;

    /// Point-in-time fence check: `Ok` iff `token` is the live lease's token.
    fn check_fence<'a>(
        &'a self,
        key: &'a str,
        token: FencingToken,
    ) -> impl Future<Output = Result<(), LeaseError>> + Send + 'a {
        async move {
            match self.current(key).await? {
                Some(lease) if lease.token == token => Ok(()),
                Some(lease) if lease.token > token => Err(LeaseError::StaleFence {
                    key: key.to_string(),
                    presented: token,
                    current: lease.token,
                }),
                _ => Err(LeaseError::Lost {
                    key: key.to_string(),
                }),
            }
        }
    }
}

pub(crate) fn validate_request(key: &str, holder: &str, ttl: Duration) -> Result<(), LeaseError> {
    if key.is_empty() {
        return Err(LeaseError::Invalid("lease key must not be empty".into()));
    }
    if holder.is_empty() {
        return Err(LeaseError::Invalid("lease holder must not be empty".into()));
    }
    if ttl.is_zero() {
        return Err(LeaseError::Invalid("lease ttl must be positive".into()));
    }
    Ok(())
}
