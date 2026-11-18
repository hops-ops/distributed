//! Durable SQLx lease-lock conformance: `PostgresLockManager` / `SqliteLockManager`.
//!
//! The generic `scenario_*` helpers run against any `LockManager`. The
//! SQLite suite runs unconditionally (temp-file databases, no server). The
//! Postgres suite skips when `DATABASE_URL` is unset, mirroring the existing
//! Postgres integration tests.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

#[cfg(feature = "sqlite")]
#[path = "../support/sqlite.rs"]
mod sqlite_support;

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use distributed::{
    sourced, AggregateBuilder, Entity, InMemoryRepository, Lock, LockError, LockManager, Queueable,
};

#[derive(Default)]
struct Counter {
    entity: Entity,
    value: i32,
}

#[sourced(entity, aggregate_type = "sqllock.counter")]
impl Counter {
    #[event("initialized")]
    fn create(&mut self, id: String) {
        self.entity.set_id(&id);
    }

    #[event("incremented")]
    fn increment(&mut self, id: String, by: i32) {
        self.entity.set_id(&id);
        self.value += by;
    }
}

/// Fail (rather than hang) if a lock operation that should be quick blocks.
async fn within<T>(fut: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), fut)
        .await
        .expect("lock operation timed out")
}

// ===========================================================================
// Generic scenarios — run against any LockManager backend.
// ===========================================================================

/// Acquire holds the key; a second handle cannot acquire until release.
async fn scenario_acquire_contend_release<M: LockManager>(manager: &M) {
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
async fn scenario_distinct_keys_do_not_contend<M: LockManager>(manager: &M) {
    let a = manager.get_lock("agg-1").unwrap();
    let b = manager.get_lock("agg-2").unwrap();
    within(a.lock()).await.unwrap();
    // A different key acquires without waiting on `a`.
    within(b.lock()).await.unwrap();
    within(a.unlock()).await.unwrap();
    within(b.unlock()).await.unwrap();
}

/// `get_lock` returns the same cached handle for the same key.
async fn scenario_same_handle_per_key<M: LockManager>(manager: &M) {
    let a1 = manager.get_lock("agg-1").unwrap();
    let a2 = manager.get_lock("agg-1").unwrap();
    let b = manager.get_lock("agg-2").unwrap();
    assert!(Arc::ptr_eq(&a1, &a2));
    assert!(!Arc::ptr_eq(&a1, &b));
}

/// Two managers over one database (simulated processes) serialize: the second
/// blocks on the held DB lease until the first releases.
async fn scenario_two_managers_serialize<M>(m1: M, m2: M)
where
    M: LockManager + 'static,
{
    let l1 = m1.get_lock("shared").unwrap();
    within(l1.lock()).await.unwrap();

    let acquired = Arc::new(AtomicBool::new(false));
    let m2 = Arc::new(m2);
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

    // The DB lease is held by m1, so the second process must block.
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

/// Margin added to a holder's lease TTL before asserting expiry, generous enough
/// to stay robust on slow/loaded CI (the negative "not stealable yet" check runs
/// within the TTL window, so the TTL itself must comfortably exceed one DB round
/// trip — callers pass a TTL of a few hundred ms).
const EXPIRY_MARGIN: Duration = Duration::from_millis(700);

/// An expired lease (crashed/overran holder) becomes reclaimable by another
/// manager. `m_holder` must be configured with lease TTL `ttl` (passed in so the
/// wait scales with it).
async fn scenario_expired_lease_reclaim<M: LockManager>(m_holder: &M, m_other: &M, ttl: Duration) {
    let l1 = m_holder.get_lock("expiring").unwrap();
    within(l1.lock()).await.unwrap(); // acquired, never released

    let l2 = m_other.get_lock("expiring").unwrap();
    assert!(
        !within(l2.try_lock()).await.unwrap(),
        "lease is still valid; must not be stealable yet"
    );

    // Wait past the holder's TTL, then the lease is reclaimable.
    tokio::time::sleep(ttl + EXPIRY_MARGIN).await;
    assert!(
        within(l2.try_lock()).await.unwrap(),
        "expired lease should be reclaimable"
    );
    within(l2.unlock()).await.unwrap();
}

/// Release is owner-token scoped: after a lease is stolen on expiry, the original
/// holder's `unlock` is a no-op and does not free the new holder's lease.
async fn scenario_release_is_owner_scoped<M: LockManager>(m1: &M, m2: &M, ttl: Duration) {
    let l1 = m1.get_lock("stolen").unwrap();
    within(l1.lock()).await.unwrap(); // m1 holds with TTL `ttl`, never releases cleanly

    tokio::time::sleep(ttl + EXPIRY_MARGIN).await; // m1's lease expires

    let l2 = m2.get_lock("stolen").unwrap();
    assert!(
        within(l2.try_lock()).await.unwrap(),
        "m2 steals the expired lease"
    );

    // m1 releases its (now-stolen) lease — must NOT delete m2's row.
    within(l1.unlock()).await.unwrap();

    // m2 still holds it: a fresh acquire by m1 fails (m2's lease is valid).
    assert!(
        !within(l1.try_lock()).await.unwrap(),
        "owner-scoped release must not free another manager's lease"
    );
    within(l2.unlock()).await.unwrap();
}

/// A `QueuedRepository` backed by the manager serializes per-aggregate access and
/// `abort` releases the held lease.
async fn scenario_queued_abort_releases<M>(manager: M)
where
    M: LockManager + 'static,
{
    let repo = InMemoryRepository::new()
        .queued_with(manager)
        .aggregate::<Counter>();

    let mut seed = Counter::default();
    seed.create("c1".into()).unwrap();
    repo.commit(&mut seed).await.unwrap();

    // `get` acquires and holds the lease.
    let held = repo.get("c1").await.unwrap().unwrap();
    // `abort` releases it.
    repo.abort(&held).await.unwrap();

    // A subsequent locking load must not block.
    let reloaded = tokio::time::timeout(Duration::from_secs(5), repo.get("c1"))
        .await
        .expect("load after abort must not block")
        .unwrap()
        .expect("counter should exist");
    repo.abort(&reloaded).await.unwrap();
}

/// Two managers (independent gates) race `try_lock` for the SAME initially-free
/// key concurrently: exactly one must win the atomic DB upsert. This exercises
/// the lease collision at the database level, not just the in-process gate.
async fn scenario_two_managers_race_free_key<M>(m1: M, m2: M)
where
    M: LockManager + 'static,
{
    let l1 = m1.get_lock("race").unwrap();
    let l2 = m2.get_lock("race").unwrap();
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
    if won1 {
        within(l1.unlock()).await.unwrap();
    } else {
        within(l2.unlock()).await.unwrap();
    }
}

/// `lock()` on a manager configured with `max_wait` returns `AcquireFailed`
/// (rather than blocking forever) when the lease stays held past the deadline.
async fn scenario_max_wait_timeout<M: LockManager>(holder: &M, waiter: &M) {
    let held = holder.get_lock("busy").unwrap();
    within(held.lock()).await.unwrap(); // held with the default (long) TTL

    let w = waiter.get_lock("busy").unwrap();
    // Guard against a hang: the inner max_wait is short, so this must resolve.
    let result = tokio::time::timeout(Duration::from_secs(3), w.lock())
        .await
        .expect("lock() with max_wait must return, not hang");
    assert!(
        matches!(result, Err(LockError::AcquireFailed(_))),
        "expected AcquireFailed on max_wait timeout, got {result:?}"
    );
    within(held.unlock()).await.unwrap();
}

/// Cancellation safety: if an in-progress `lock()` is dropped while it holds the
/// in-process gate and is polling the (held) DB lease, the gate must still be
/// released — otherwise the key wedges for every later same-process acquire.
async fn scenario_cancelled_acquire_releases_gate<M: LockManager>(holder: &M, other: &M) {
    let h = holder.get_lock("cancelme").unwrap();
    within(h.lock()).await.unwrap(); // holder owns the DB lease

    // `other` acquires its own gate, then polls the held DB lease. Cancel it
    // there by letting the timeout drop the future while it is still waiting.
    let o = other.get_lock("cancelme").unwrap();
    let cancelled = tokio::time::timeout(Duration::from_millis(150), o.lock()).await;
    assert!(
        cancelled.is_err(),
        "the contended acquire must still be waiting when cancelled"
    );

    // Release the holder; `other` must now acquire. If the cancelled acquire had
    // leaked its gate, this second `lock()` would hang on the wedged gate and
    // `within` would time out.
    within(h.unlock()).await.unwrap();
    within(o.lock()).await.unwrap();
    within(o.unlock()).await.unwrap();
}

// ===========================================================================
// SQLite backend (runs unconditionally — temp-file databases, no server).
// ===========================================================================

#[cfg(feature = "sqlite")]
mod sqlite_backend {
    use super::sqlite_support::TempDb;
    use super::*;
    use distributed::SqliteLockManager;
    use sqlx::SqlitePool;

    /// A pool over the shared temp-file database (so multiple managers see the
    /// same `aggregate_locks` table, cross-"process") with the lease table
    /// migrated.
    async fn pool(db: &TempDb) -> SqlitePool {
        let pool = db.pool().await;
        SqliteLockManager::migrate(&pool)
            .await
            .expect("migrate aggregate_locks");
        pool
    }

    fn temp_db() -> TempDb {
        TempDb::new("sourced_lock_test")
    }

    async fn manager() -> (TempDb, SqliteLockManager) {
        let db = temp_db();
        let manager = SqliteLockManager::new(pool(&db).await);
        (db, manager)
    }

    async fn managers() -> (TempDb, SqliteLockManager, SqliteLockManager) {
        let db = temp_db();
        let m1 = SqliteLockManager::new(pool(&db).await);
        let m2 = SqliteLockManager::new(pool(&db).await);
        (db, m1, m2)
    }

    macro_rules! single_manager_test {
        ($name:ident, $scenario:ident) => {
            #[tokio::test]
            async fn $name() {
                let (_db, manager) = manager().await;
                $scenario(&manager).await;
            }
        };
    }

    single_manager_test!(acquire_contend_release, scenario_acquire_contend_release);
    single_manager_test!(
        distinct_keys_do_not_contend,
        scenario_distinct_keys_do_not_contend
    );
    single_manager_test!(same_handle_per_key, scenario_same_handle_per_key);

    #[tokio::test]
    async fn two_managers_serialize() {
        let (db, m1, m2) = managers().await;
        scenario_two_managers_serialize(m1, m2).await;
        drop(db);
    }

    #[tokio::test]
    async fn expired_lease_reclaim() {
        let ttl = Duration::from_millis(400);
        let db = temp_db();
        let holder = SqliteLockManager::new(pool(&db).await).with_lease_ttl(ttl);
        let other = SqliteLockManager::new(pool(&db).await);
        scenario_expired_lease_reclaim(&holder, &other, ttl).await;
        drop(db);
    }

    #[tokio::test]
    async fn release_is_owner_scoped() {
        let ttl = Duration::from_millis(400);
        let db = temp_db();
        let m1 = SqliteLockManager::new(pool(&db).await).with_lease_ttl(ttl);
        let m2 = SqliteLockManager::new(pool(&db).await);
        scenario_release_is_owner_scoped(&m1, &m2, ttl).await;
        drop(db);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_abort_releases() {
        let (db, manager) = manager().await;
        scenario_queued_abort_releases(manager).await;
        drop(db);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_managers_race_free_key() {
        let (db, m1, m2) = managers().await;
        scenario_two_managers_race_free_key(m1, m2).await;
        drop(db);
    }

    #[tokio::test]
    async fn max_wait_timeout() {
        let db = temp_db();
        let holder = SqliteLockManager::new(pool(&db).await);
        let waiter =
            SqliteLockManager::new(pool(&db).await).with_max_wait(Some(Duration::from_millis(150)));
        scenario_max_wait_timeout(&holder, &waiter).await;
        drop(db);
    }

    #[tokio::test]
    async fn sweep_expired_reclaims_rows() {
        let ttl = Duration::from_millis(200);
        let db = temp_db();
        let manager = SqliteLockManager::new(pool(&db).await).with_lease_ttl(ttl);
        let lock = manager.get_lock("sweepme").unwrap();
        within(lock.lock()).await.unwrap(); // writes a lease row, never released
        tokio::time::sleep(ttl + EXPIRY_MARGIN).await; // lease expires
        let reclaimed = manager.sweep_expired().await.unwrap();
        assert!(
            reclaimed >= 1,
            "sweep_expired should delete the expired lease, got {reclaimed}"
        );
        drop(db);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_acquire_releases_gate() {
        let (db, holder, other) = managers().await;
        scenario_cancelled_acquire_releases_gate(&holder, &other).await;
        drop(db);
    }
}

// ===========================================================================
// Postgres backend (skips when DATABASE_URL is unset).
// ===========================================================================

#[cfg(feature = "postgres")]
#[path = "../support/postgres.rs"]
mod postgres_support;

#[cfg(feature = "postgres")]
mod postgres_backend {
    use super::*;
    use crate::postgres_support::PostgresTestSchema;
    use distributed::PostgresLockManager;

    const SKIP: &str = "postgres lock test";

    async fn schema() -> Option<PostgresTestSchema> {
        PostgresTestSchema::create_from_env("locks", SKIP).await
    }

    async fn manager() -> Option<(PostgresTestSchema, PostgresLockManager)> {
        let schema = schema().await?;
        let manager = PostgresLockManager::new(schema.repository().await.pool().clone());
        Some((schema, manager))
    }

    async fn managers() -> Option<(PostgresTestSchema, PostgresLockManager, PostgresLockManager)> {
        let schema = schema().await?;
        let m1 = PostgresLockManager::new(schema.repository().await.pool().clone());
        let m2 = PostgresLockManager::new(schema.repository().await.pool().clone());
        Some((schema, m1, m2))
    }

    macro_rules! single_manager_test {
        ($name:ident, $scenario:ident) => {
            #[tokio::test]
            async fn $name() {
                let Some((_schema, manager)) = manager().await else {
                    return;
                };
                $scenario(&manager).await;
            }
        };
    }

    single_manager_test!(acquire_contend_release, scenario_acquire_contend_release);
    single_manager_test!(
        distinct_keys_do_not_contend,
        scenario_distinct_keys_do_not_contend
    );
    single_manager_test!(same_handle_per_key, scenario_same_handle_per_key);

    #[tokio::test]
    async fn two_managers_serialize() {
        let Some((_schema, m1, m2)) = managers().await else {
            return;
        };
        scenario_two_managers_serialize(m1, m2).await;
    }

    #[tokio::test]
    async fn expired_lease_reclaim() {
        let Some(schema) = schema().await else {
            return;
        };
        let ttl = Duration::from_millis(400);
        let holder =
            PostgresLockManager::new(schema.repository().await.pool().clone()).with_lease_ttl(ttl);
        let other = PostgresLockManager::new(schema.repository().await.pool().clone());
        scenario_expired_lease_reclaim(&holder, &other, ttl).await;
    }

    #[tokio::test]
    async fn release_is_owner_scoped() {
        let Some(schema) = schema().await else {
            return;
        };
        let ttl = Duration::from_millis(400);
        let m1 =
            PostgresLockManager::new(schema.repository().await.pool().clone()).with_lease_ttl(ttl);
        let m2 = PostgresLockManager::new(schema.repository().await.pool().clone());
        scenario_release_is_owner_scoped(&m1, &m2, ttl).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_abort_releases() {
        let Some((_schema, manager)) = manager().await else {
            return;
        };
        scenario_queued_abort_releases(manager).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_managers_race_free_key() {
        let Some((_schema, m1, m2)) = managers().await else {
            return;
        };
        scenario_two_managers_race_free_key(m1, m2).await;
    }

    #[tokio::test]
    async fn max_wait_timeout() {
        let Some(schema) = schema().await else {
            return;
        };
        let holder = PostgresLockManager::new(schema.repository().await.pool().clone());
        let waiter = PostgresLockManager::new(schema.repository().await.pool().clone())
            .with_max_wait(Some(Duration::from_millis(150)));
        scenario_max_wait_timeout(&holder, &waiter).await;
    }

    #[tokio::test]
    async fn sweep_expired_reclaims_rows() {
        let Some(schema) = schema().await else {
            return;
        };
        let ttl = Duration::from_millis(200);
        let manager =
            PostgresLockManager::new(schema.repository().await.pool().clone()).with_lease_ttl(ttl);
        let lock = manager.get_lock("sweepme").unwrap();
        within(lock.lock()).await.unwrap();
        tokio::time::sleep(ttl + EXPIRY_MARGIN).await;
        let reclaimed = manager.sweep_expired().await.unwrap();
        assert!(
            reclaimed >= 1,
            "sweep_expired should delete the expired lease, got {reclaimed}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_acquire_releases_gate() {
        let Some((_schema, holder, other)) = managers().await else {
            return;
        };
        scenario_cancelled_acquire_releases_gate(&holder, &other).await;
    }
}
