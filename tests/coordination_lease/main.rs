//! Lease + fencing-token conformance for `InMemoryLeaseStore`,
//! `SqliteLeaseStore` and `PostgresLeaseStore`.
//!
//! Generic `scenario_*` helpers run against any `LeaseStore`. Expiry is driven
//! by a `ManualClock` for the in-memory store (no sleeps) and by short TTLs on
//! the database clock for the SQL stores. Postgres skips without
//! `DATABASE_URL`.

#[cfg(feature = "sqlite")]
#[path = "../support/sqlite.rs"]
mod sqlite_support;

use std::sync::Arc;
use std::time::Duration;

use distributed::lease::{Fence, FencingToken, Lease, LeaseError, LeaseStore};
use distributed::{InMemoryLeaseStore, ManualClock};

const TTL: Duration = Duration::from_secs(30);

// ===========================================================================
// Generic scenarios
// ===========================================================================

/// Acquire, contend, renew (same token), release, re-acquire (bigger token).
async fn scenario_lifecycle<L: LeaseStore>(store: &L) {
    let first = store.acquire("boss:hydra", "zone-a", TTL).await.unwrap();
    assert_eq!(first.holder, "zone-a");

    match store.acquire("boss:hydra", "zone-b", TTL).await {
        Err(LeaseError::Held { holder, .. }) => assert_eq!(holder, "zone-a"),
        other => panic!("contended acquire must be Held, got {other:?}"),
    }

    // Same holder re-acquiring a live lease is an idempotent retry.
    let again = store.acquire("boss:hydra", "zone-a", TTL).await.unwrap();
    assert_eq!(again.token, first.token);

    let renewed = store.renew(&first, TTL).await.unwrap();
    assert_eq!(renewed.token, first.token, "renewal keeps the token");
    assert!(renewed.expires_at >= first.expires_at);

    assert_eq!(
        store.current("boss:hydra").await.unwrap(),
        Some(renewed.clone())
    );
    store.check_fence("boss:hydra", first.token).await.unwrap();

    assert!(store.release(&renewed).await.unwrap());
    assert!(
        !store.release(&renewed).await.unwrap(),
        "double release is a no-op"
    );
    assert_eq!(store.current("boss:hydra").await.unwrap(), None);
    assert!(matches!(
        store.renew(&renewed, TTL).await,
        Err(LeaseError::Lost { .. })
    ));

    let next = store.acquire("boss:hydra", "zone-b", TTL).await.unwrap();
    assert!(
        next.token > first.token,
        "tokens stay monotonic across release: {} then {}",
        first.token,
        next.token
    );
}

/// A superseded holder's token is rejected by both the store check and a
/// resource-side `Fence`.
async fn scenario_stale_fence_rejected<L: LeaseStore>(store: &L) {
    let old = store.acquire("item:sword", "session-1", TTL).await.unwrap();
    let mut fence = Fence::new();
    fence.admit("item:sword", old.token).unwrap();

    store.release(&old).await.unwrap();
    let new = store.acquire("item:sword", "session-2", TTL).await.unwrap();
    assert!(new.token > old.token);
    fence.admit("item:sword", new.token).unwrap();

    // The paused former holder wakes up and tries to write.
    match fence.admit("item:sword", old.token) {
        Err(LeaseError::StaleFence {
            presented, current, ..
        }) => {
            assert_eq!(presented, old.token);
            assert_eq!(current, new.token);
        }
        other => panic!("stale token must be rejected, got {other:?}"),
    }
    assert!(matches!(
        store.check_fence("item:sword", old.token).await,
        Err(LeaseError::StaleFence { .. })
    ));
    assert!(matches!(
        store.renew(&old, TTL).await,
        Err(LeaseError::Lost { .. })
    ));
    store.check_fence("item:sword", new.token).await.unwrap();
}

/// Many holders race for one key: exactly one wins, the rest see `Held`.
async fn scenario_concurrent_acquire<L: LeaseStore + Clone + 'static>(store: &L) {
    let contenders = 16;
    let mut tasks = Vec::new();
    for n in 0..contenders {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            store
                .acquire("encounter:42", &format!("worker-{n}"), TTL)
                .await
        }));
    }
    let mut winners: Vec<Lease> = Vec::new();
    for task in tasks {
        match task.await.unwrap() {
            Ok(lease) => winners.push(lease),
            Err(LeaseError::Held { .. }) => {}
            Err(other) => panic!("unexpected acquire error: {other}"),
        }
    }
    assert_eq!(winners.len(), 1, "exactly one contender wins: {winners:?}");
    assert_eq!(
        store.current("encounter:42").await.unwrap().unwrap(),
        winners[0]
    );
}

fn assert_invalid_inputs_rejected(result: Result<Lease, LeaseError>) {
    assert!(matches!(result, Err(LeaseError::Invalid(_))), "{result:?}");
}

// ===========================================================================
// In-memory (ManualClock: no sleeps)
// ===========================================================================

mod in_memory {
    use super::*;

    fn store() -> (ManualClock, InMemoryLeaseStore<ManualClock>) {
        let clock = ManualClock::starting_now();
        (clock.clone(), InMemoryLeaseStore::with_clock(clock))
    }

    #[tokio::test]
    async fn lifecycle() {
        scenario_lifecycle(&store().1).await;
    }

    #[tokio::test]
    async fn stale_fence_rejected() {
        scenario_stale_fence_rejected(&store().1).await;
    }

    #[tokio::test]
    async fn concurrent_acquire_has_one_winner() {
        scenario_concurrent_acquire(&store().1).await;
    }

    #[tokio::test]
    async fn invalid_inputs_rejected() {
        let (_, store) = store();
        assert_invalid_inputs_rejected(store.acquire("", "h", TTL).await);
        assert_invalid_inputs_rejected(store.acquire("k", "", TTL).await);
        assert_invalid_inputs_rejected(store.acquire("k", "h", Duration::ZERO).await);
    }

    #[tokio::test]
    async fn expiry_takeover_bumps_token_and_fences_old_holder() {
        let (clock, store) = store();
        let ttl = Duration::from_secs(15);
        let old = store.acquire("boss:lich", "zone-a", ttl).await.unwrap();

        // Renewed every 5s it never lapses.
        for _ in 0..4 {
            clock.advance(Duration::from_secs(5));
            store.renew(&old, ttl).await.unwrap();
        }
        assert!(matches!(
            store.acquire("boss:lich", "zone-b", ttl).await,
            Err(LeaseError::Held { .. })
        ));

        // The holder stalls past its TTL; another controller takes over.
        clock.advance(ttl);
        assert_eq!(store.current("boss:lich").await.unwrap(), None);
        let new = store.acquire("boss:lich", "zone-b", ttl).await.unwrap();
        assert_eq!(new.token, FencingToken::new(old.token.get() + 1));
        assert!(matches!(
            store.renew(&old, ttl).await,
            Err(LeaseError::Lost { .. })
        ));
        assert!(!store.release(&old).await.unwrap());
        assert!(matches!(
            store.check_fence("boss:lich", old.token).await,
            Err(LeaseError::StaleFence { .. })
        ));
    }

    #[tokio::test]
    async fn concurrent_takeover_after_expiry_has_one_winner() {
        let (clock, store) = store();
        let ttl = Duration::from_secs(10);
        let first = store.acquire("k", "a", ttl).await.unwrap();
        clock.advance(ttl);
        let store = Arc::new(store);
        let mut tasks = Vec::new();
        for n in 0..8 {
            let store = Arc::clone(&store);
            tasks.push(tokio::spawn(async move {
                store.acquire("k", &format!("h{n}"), ttl).await
            }));
        }
        let mut tokens = Vec::new();
        for task in tasks {
            if let Ok(lease) = task.await.unwrap() {
                tokens.push(lease.token);
            }
        }
        assert_eq!(tokens, vec![FencingToken::new(first.token.get() + 1)]);
    }
}

// ===========================================================================
// SQLite (shared temp-file database; database clock)
// ===========================================================================

#[cfg(feature = "sqlite")]
mod sqlite_backend {
    use super::*;
    use crate::sqlite_support::TempDb;
    use distributed::{SqliteLeaseStore, SqliteRepository};

    async fn store(db: &TempDb) -> SqliteLeaseStore {
        let pool = db.pool().await;
        // The repository migrations create `coordination_leases`.
        SqliteRepository::migrate_pool(&pool).await.unwrap();
        SqliteLeaseStore::new(pool)
    }

    #[tokio::test]
    async fn lifecycle() {
        let db = TempDb::new("lease_lifecycle");
        scenario_lifecycle(&store(&db).await).await;
    }

    #[tokio::test]
    async fn stale_fence_rejected() {
        let db = TempDb::new("lease_fence");
        scenario_stale_fence_rejected(&store(&db).await).await;
    }

    #[tokio::test]
    async fn concurrent_acquire_across_pools_has_one_winner() {
        let db = TempDb::new("lease_race");
        let a = store(&db).await;
        scenario_concurrent_acquire(&a).await;
        // A second "process" (pool) sees the same holder.
        let b = SqliteLeaseStore::new(db.pool().await);
        assert!(matches!(
            b.acquire("encounter:42", "other-process", TTL).await,
            Err(LeaseError::Held { .. })
        ));
    }

    #[tokio::test]
    async fn standalone_migrate_is_idempotent() {
        let db = TempDb::new("lease_migrate");
        let pool = db.pool().await;
        SqliteLeaseStore::migrate(&pool).await.unwrap();
        SqliteLeaseStore::migrate(&pool).await.unwrap();
        let store = SqliteLeaseStore::new(pool);
        store.acquire("k", "h", TTL).await.unwrap();
    }

    #[tokio::test]
    async fn expiry_takeover_bumps_token_across_processes() {
        let db = TempDb::new("lease_expiry");
        let a = store(&db).await;
        let b = SqliteLeaseStore::new(db.pool().await);
        let ttl = Duration::from_millis(200);
        let old = a.acquire("boss:lich", "zone-a", ttl).await.unwrap();
        tokio::time::sleep(Duration::from_millis(350)).await;
        let new = b
            .acquire("boss:lich", "zone-b", Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(new.token, FencingToken::new(old.token.get() + 1));
        assert!(matches!(
            a.renew(&old, ttl).await,
            Err(LeaseError::Lost { .. })
        ));
        assert!(matches!(
            a.check_fence("boss:lich", old.token).await,
            Err(LeaseError::StaleFence { .. })
        ));
    }
}

// ===========================================================================
// Postgres (skips when DATABASE_URL is unset)
// ===========================================================================

#[cfg(feature = "postgres")]
#[path = "../support/postgres.rs"]
mod postgres_support;

#[cfg(feature = "postgres")]
mod postgres_backend {
    use super::*;
    use crate::postgres_support::PostgresTestSchema;
    use distributed::PostgresLeaseStore;

    async fn store() -> Option<(PostgresTestSchema, PostgresLeaseStore)> {
        let schema =
            PostgresTestSchema::create_from_env("leases", "skipping postgres lease test").await?;
        let store = PostgresLeaseStore::new(schema.repository().await.pool().clone());
        Some((schema, store))
    }

    #[tokio::test]
    async fn lifecycle() {
        let Some((_schema, store)) = store().await else {
            return;
        };
        scenario_lifecycle(&store).await;
    }

    #[tokio::test]
    async fn stale_fence_rejected() {
        let Some((_schema, store)) = store().await else {
            return;
        };
        scenario_stale_fence_rejected(&store).await;
    }

    #[tokio::test]
    async fn concurrent_acquire_has_one_winner() {
        let Some((_schema, store)) = store().await else {
            return;
        };
        scenario_concurrent_acquire(&store).await;
    }

    #[tokio::test]
    async fn expiry_takeover_bumps_token() {
        let Some((_schema, store)) = store().await else {
            return;
        };
        let ttl = Duration::from_millis(200);
        let old = store.acquire("boss:lich", "zone-a", ttl).await.unwrap();
        tokio::time::sleep(Duration::from_millis(350)).await;
        let new = store.acquire("boss:lich", "zone-b", TTL).await.unwrap();
        assert_eq!(new.token, FencingToken::new(old.token.get() + 1));
        assert!(matches!(
            store.renew(&old, ttl).await,
            Err(LeaseError::Lost { .. })
        ));
    }
}
