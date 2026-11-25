//! The durable lock-manager contract (database leases behind the in-process
//! gate): one case list, run on every SQL lock manager by
//! [`lock_conformance_tests!`](crate::lock_conformance_tests).
//!
//! Every manager a backend makes is a separate "process" over one database.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use distributed::{
    sourced, AggregateBuilder, Entity, InMemoryRepository, Lock, LockError, LockManager, Queueable,
};

/// How a backend configures a manager; `None` keeps the manager default.
#[derive(Clone, Copy, Debug, Default)]
pub struct LockOptions {
    pub lease_ttl: Option<Duration>,
    pub max_wait: Option<Duration>,
}

/// A lock manager under test.
pub trait LockBackend: Sized {
    type Manager: LockManager + Send + Sync + 'static;

    async fn open(case: &str) -> Option<Self>;
    /// A new manager over the backend's database.
    async fn manager(&self, options: LockOptions) -> Self::Manager;
    /// Delete expired lease rows, returning how many.
    async fn sweep_expired(manager: &Self::Manager) -> u64;
}

async fn manager<B: LockBackend>(backend: &B) -> B::Manager {
    backend.manager(LockOptions::default()).await
}

async fn with_ttl<B: LockBackend>(backend: &B, ttl: Duration) -> B::Manager {
    backend
        .manager(LockOptions {
            lease_ttl: Some(ttl),
            ..LockOptions::default()
        })
        .await
}

#[derive(Default)]
struct Counter {
    entity: Entity,
}

#[sourced(entity, aggregate_type = "sqllock.counter")]
impl Counter {
    #[event("initialized")]
    fn create(&mut self, id: String) {
        self.entity.set_id(&id);
    }
}

/// Fail (rather than hang) if a lock operation that should be quick blocks.
async fn within<T>(fut: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), fut)
        .await
        .expect("lock operation timed out")
}

/// Margin added to a holder's lease TTL before asserting expiry, generous
/// enough for a loaded machine.
const EXPIRY_MARGIN: Duration = Duration::from_millis(700);

/// The TTL of a lease the scenarios let expire. The "not stealable yet" check
/// runs within it, so it must comfortably exceed one database round trip.
const SHORT_TTL: Duration = Duration::from_millis(400);

/// Acquire holds the key; a second handle cannot acquire until release.
pub async fn acquire_contend_release<B: LockBackend>(backend: &B) {
    let manager = manager(backend).await;
    let a = manager.get_lock("agg-1").unwrap();
    within(a.lock()).await.unwrap();

    let b = manager.get_lock("agg-1").unwrap();
    assert!(
        !within(b.try_lock()).await.unwrap(),
        "held key must not be acquirable"
    );

    within(a.unlock()).await.unwrap();
    assert!(
        within(b.try_lock()).await.unwrap(),
        "released key must be acquirable again"
    );
    within(b.unlock()).await.unwrap();
}

/// Distinct keys never contend.
pub async fn distinct_keys_do_not_contend<B: LockBackend>(backend: &B) {
    let manager = manager(backend).await;
    let a = manager.get_lock("agg-1").unwrap();
    let b = manager.get_lock("agg-2").unwrap();
    within(a.lock()).await.unwrap();
    within(b.lock()).await.unwrap();
    within(a.unlock()).await.unwrap();
    within(b.unlock()).await.unwrap();
}

/// `get_lock` returns the same cached handle for the same key.
pub async fn same_handle_per_key<B: LockBackend>(backend: &B) {
    let manager = manager(backend).await;
    let a1 = manager.get_lock("agg-1").unwrap();
    let a2 = manager.get_lock("agg-1").unwrap();
    let b = manager.get_lock("agg-2").unwrap();
    assert!(Arc::ptr_eq(&a1, &a2));
    assert!(!Arc::ptr_eq(&a1, &b));
}

/// Two managers over one database serialize: the second blocks on the held
/// database lease until the first releases.
pub async fn two_managers_serialize<B: LockBackend>(backend: &B) {
    let m1 = manager(backend).await;
    let m2 = Arc::new(manager(backend).await);
    let l1 = m1.get_lock("shared").unwrap();
    within(l1.lock()).await.unwrap();

    let acquired = Arc::new(AtomicBool::new(false));
    let waiter = {
        let m2 = Arc::clone(&m2);
        let flag = Arc::clone(&acquired);
        tokio::spawn(async move {
            let l2 = m2.get_lock("shared").unwrap();
            l2.lock().await.unwrap();
            flag.store(true, Ordering::SeqCst);
            l2
        })
    };

    // The lease is held by m1 with the default (long) TTL, so a second
    // process blocking here cannot be a timing artefact.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !acquired.load(Ordering::SeqCst),
        "second manager must block on the held lease"
    );

    within(l1.unlock()).await.unwrap();
    let l2 = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("second manager did not acquire after release")
        .unwrap();
    assert!(acquired.load(Ordering::SeqCst));
    within(l2.unlock()).await.unwrap();
}

/// An expired lease (crashed or overrunning holder) becomes reclaimable by
/// another manager.
pub async fn expired_lease_reclaim<B: LockBackend>(backend: &B) {
    let holder = with_ttl(backend, SHORT_TTL).await;
    let other = manager(backend).await;
    let l1 = holder.get_lock("expiring").unwrap();
    // Taken before the lock: the lease's database-clock expiry is later than
    // this instant plus the TTL, so any steal observed before that is a bug.
    let requested = std::time::Instant::now();
    within(l1.lock()).await.unwrap(); // acquired, never released

    let l2 = other.get_lock("expiring").unwrap();
    let stolen = within(l2.try_lock()).await.unwrap();
    // On a loaded machine the round trips can outlast the TTL; the lease may
    // then have expired legitimately, and only an early steal is a failure.
    if requested.elapsed() < SHORT_TTL {
        assert!(!stolen, "lease is still valid; must not be stealable yet");
    } else if stolen {
        within(l2.unlock()).await.unwrap();
    }

    tokio::time::sleep(SHORT_TTL + EXPIRY_MARGIN).await;
    assert!(
        within(l2.try_lock()).await.unwrap(),
        "expired lease should be reclaimable"
    );
    within(l2.unlock()).await.unwrap();
}

/// Release is owner-token scoped: after a lease is stolen on expiry, the
/// original holder's `unlock` is a no-op and does not free the new holder's
/// lease.
pub async fn release_is_owner_scoped<B: LockBackend>(backend: &B) {
    let m1 = with_ttl(backend, SHORT_TTL).await;
    let m2 = manager(backend).await;
    let l1 = m1.get_lock("stolen").unwrap();
    within(l1.lock()).await.unwrap();

    tokio::time::sleep(SHORT_TTL + EXPIRY_MARGIN).await;

    let l2 = m2.get_lock("stolen").unwrap();
    assert!(
        within(l2.try_lock()).await.unwrap(),
        "m2 steals the expired lease"
    );

    // m1 releases its (now-stolen) lease — must NOT delete m2's row.
    within(l1.unlock()).await.unwrap();
    assert!(
        !within(l1.try_lock()).await.unwrap(),
        "owner-scoped release must not free another manager's lease"
    );
    within(l2.unlock()).await.unwrap();
}

/// A `QueuedRepository` backed by the manager serializes per-aggregate access
/// and dropping a loaded aggregate releases the held lease.
pub async fn queued_abort_releases<B: LockBackend>(backend: &B) {
    let repo = InMemoryRepository::new()
        .queued_with(manager(backend).await)
        .aggregate::<Counter>();

    let mut seed = Counter::default();
    seed.create("c1".into()).unwrap();
    repo.commit(&mut seed).await.unwrap();

    let held = repo.get("c1").await.unwrap().unwrap();
    drop(held);

    let reloaded = tokio::time::timeout(Duration::from_secs(5), repo.get("c1"))
        .await
        .expect("load after abort must not block")
        .unwrap()
        .expect("counter should exist");
    drop(reloaded);
}

/// Two managers race `try_lock` for the same free key: exactly one wins the
/// database upsert, not just the in-process gate.
pub async fn two_managers_race_free_key<B: LockBackend>(backend: &B) {
    let l1 = manager(backend).await.get_lock("race").unwrap();
    let l2 = manager(backend).await.get_lock("race").unwrap();
    let t1 = tokio::spawn(async move {
        let won = l1.try_lock().await.unwrap();
        (won, l1)
    });
    let t2 = tokio::spawn(async move {
        let won = l2.try_lock().await.unwrap();
        (won, l2)
    });
    let (won1, l1) = t1.await.unwrap();
    let (won2, l2) = t2.await.unwrap();
    assert!(
        won1 ^ won2,
        "exactly one manager must win the race for a free key (got {won1}, {won2})"
    );
    let winner = if won1 { l1 } else { l2 };
    within(winner.unlock()).await.unwrap();
}

/// `lock()` on a manager with `max_wait` returns `AcquireFailed` (rather than
/// blocking forever) when the lease stays held past the deadline.
pub async fn max_wait_timeout<B: LockBackend>(backend: &B) {
    let holder = manager(backend).await;
    let waiter = backend
        .manager(LockOptions {
            max_wait: Some(Duration::from_millis(150)),
            ..LockOptions::default()
        })
        .await;
    let held = holder.get_lock("busy").unwrap();
    within(held.lock()).await.unwrap();

    let w = waiter.get_lock("busy").unwrap();
    let result = tokio::time::timeout(Duration::from_secs(3), w.lock())
        .await
        .expect("lock() with max_wait must return, not hang");
    assert!(
        matches!(result, Err(LockError::AcquireFailed(_))),
        "expected AcquireFailed on max_wait timeout, got {result:?}"
    );
    within(held.unlock()).await.unwrap();
}

/// `sweep_expired` deletes a lease row whose holder never released it.
pub async fn sweep_expired_reclaims_rows<B: LockBackend>(backend: &B) {
    let manager = with_ttl(backend, SHORT_TTL).await;
    let lock = manager.get_lock("sweepme").unwrap();
    within(lock.lock()).await.unwrap();
    tokio::time::sleep(SHORT_TTL + EXPIRY_MARGIN).await;
    let reclaimed = B::sweep_expired(&manager).await;
    assert!(
        reclaimed >= 1,
        "sweep_expired should delete the expired lease, got {reclaimed}"
    );
}

/// Cancellation safety: a `lock()` dropped while it holds the in-process gate
/// and polls the held database lease still releases the gate.
pub async fn cancelled_acquire_releases_gate<B: LockBackend>(backend: &B) {
    let holder = manager(backend).await;
    let other = manager(backend).await;
    let h = holder.get_lock("cancelme").unwrap();
    within(h.lock()).await.unwrap();

    let o = other.get_lock("cancelme").unwrap();
    let cancelled = tokio::time::timeout(Duration::from_millis(150), o.lock()).await;
    assert!(
        cancelled.is_err(),
        "the contended acquire must still be waiting when cancelled"
    );

    // Had the cancelled acquire leaked its gate, this `lock()` would hang.
    within(h.unlock()).await.unwrap();
    within(o.lock()).await.unwrap();
    within(o.unlock()).await.unwrap();
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn configured<D: distributed::lock::LockDialect>(
    manager: distributed::lock::SqlxLockManager<D>,
    options: LockOptions,
) -> distributed::lock::SqlxLockManager<D> {
    let manager = manager.with_max_wait(options.max_wait);
    match options.lease_ttl {
        Some(ttl) => manager.with_lease_ttl(ttl),
        None => manager,
    }
}

#[cfg(feature = "sqlite")]
pub use self::sqlite::Sqlite;

#[cfg(feature = "sqlite")]
mod sqlite {
    use distributed::SqliteLockManager;

    use super::{configured, LockBackend, LockOptions};
    use crate::sqlite::TempDb;

    /// SQLite: a temp-file database, so several managers share its
    /// `aggregate_locks` table.
    pub struct Sqlite(TempDb);

    impl LockBackend for Sqlite {
        type Manager = SqliteLockManager;

        async fn open(case: &str) -> Option<Self> {
            Some(Self(TempDb::new(&format!("lock_{case}"))))
        }

        async fn manager(&self, options: LockOptions) -> SqliteLockManager {
            let pool = self.0.pool().await;
            SqliteLockManager::migrate(&pool)
                .await
                .expect("migrate aggregate_locks");
            configured(SqliteLockManager::new(pool), options)
        }

        async fn sweep_expired(manager: &SqliteLockManager) -> u64 {
            manager.sweep_expired().await.unwrap()
        }
    }
}

#[cfg(feature = "postgres")]
pub use self::postgres::Postgres;

#[cfg(feature = "postgres")]
mod postgres {
    use distributed::PostgresLockManager;

    use super::{configured, LockBackend, LockOptions};
    use crate::postgres::PostgresTestSchema;

    /// Postgres: a fresh schema of `DATABASE_URL` per test.
    pub struct Postgres(PostgresTestSchema);

    impl LockBackend for Postgres {
        type Manager = PostgresLockManager;

        async fn open(case: &str) -> Option<Self> {
            PostgresTestSchema::create_from_env("locks", case)
                .await
                .map(Self)
        }

        async fn manager(&self, options: LockOptions) -> PostgresLockManager {
            let pool = self.0.repository().await.pool().clone();
            configured(PostgresLockManager::new(pool), options)
        }

        async fn sweep_expired(manager: &PostgresLockManager) -> u64 {
            manager.sweep_expired().await.unwrap()
        }
    }
}

/// Expands every lock-manager conformance case as a `#[tokio::test]` for
/// `$backend`, a [`LockBackend`].
///
/// ```ignore
/// common::lock_conformance_tests!(common::lock::Sqlite);
/// ```
#[macro_export]
macro_rules! lock_conformance_tests {
    ($backend:ty $(,)?) => {
        $crate::conformance_cases! { $crate::lock::LockBackend, $backend, open;
            #[tokio::test]
            acquire_contend_release(b) => $crate::lock::acquire_contend_release(&b);
            #[tokio::test]
            distinct_keys_do_not_contend(b) => $crate::lock::distinct_keys_do_not_contend(&b);
            #[tokio::test]
            same_handle_per_key(b) => $crate::lock::same_handle_per_key(&b);
            #[tokio::test]
            two_managers_serialize(b) => $crate::lock::two_managers_serialize(&b);
            #[tokio::test]
            expired_lease_reclaim(b) => $crate::lock::expired_lease_reclaim(&b);
            #[tokio::test]
            release_is_owner_scoped(b) => $crate::lock::release_is_owner_scoped(&b);
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            queued_abort_releases(b) => $crate::lock::queued_abort_releases(&b);
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            two_managers_race_free_key(b) => $crate::lock::two_managers_race_free_key(&b);
            #[tokio::test]
            max_wait_timeout(b) => $crate::lock::max_wait_timeout(&b);
            #[tokio::test]
            sweep_expired_reclaims_rows(b) => $crate::lock::sweep_expired_reclaims_rows(&b);
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            cancelled_acquire_releases_gate(b) => $crate::lock::cancelled_acquire_releases_gate(&b);
        }
    };
}
