//! The lease-store contract (leases with fencing tokens): one case list, run
//! on every store by [`lease_conformance_tests!`](crate::lease_conformance_tests).
//!
//! Expiry is driven through [`LeaseBackend::elapse`]: the in-memory store
//! advances a [`ManualClock`], the SQL stores (which use the database clock)
//! wait in real time past a short TTL. Waiting longer than a TTL always
//! expires the lease, so the wall-clock backends stay deterministic; only a
//! claim that a lease is still alive needs a controlled clock.

use std::time::Duration;

use distributed::clock::ManualClock;
use distributed::lease::InMemoryLeaseStore;
use distributed::lease::{Fence, FencingToken, Lease, LeaseError, LeaseStore};

/// The TTL of a lease a test expects to stay alive: longer than any test
/// runs. The SQL stores expire on the database clock, so a shorter TTL is a
/// race with the host's load: `second_process` migrates a fresh schema, and
/// with a 30s TTL a 157s run on a loaded host saw the lease expired where it
/// asserted `Held`. Expiry itself is tested with [`LeaseBackend::short_ttl`].
const TTL: Duration = Duration::from_secs(3600);

/// Racers per race: as many as one store's pool can put in the database at
/// once, so the database decides the race. A racer beyond that only queues in
/// the pool, and on a host slow enough that the racers ahead of it hold every
/// connection past the pool's acquire timeout (30s), it fails with
/// `pool timed out` instead of `Held`.
const RACERS: usize = crate::POOL_CONNECTIONS as usize;

/// A lease store under test, on storage another "process" can reach.
pub trait LeaseBackend: Sized {
    type Store: LeaseStore + Clone + Send + Sync + 'static;

    async fn open(case: &str) -> Option<Self>;
    fn store(&self) -> Self::Store;
    /// Another store over the same storage, as a second process.
    async fn second_process(&self) -> Self::Store;
    /// A TTL [`LeaseBackend::elapse`] can outlast quickly.
    fn short_ttl(&self) -> Duration;
    /// Let at least `ttl` pass on the store's clock.
    async fn elapse(&self, ttl: Duration);
}

/// Capability `controlled_clock`: time passes only through
/// [`LeaseBackend::elapse`], so a test may also claim a lease is still alive.
pub trait ControlledClock: LeaseBackend {}

/// Capability `standalone_migration`: the store creates its own table.
pub trait StandaloneMigration: LeaseBackend {
    /// A store over fresh storage whose table only the store's standalone
    /// migration created, run twice.
    async fn migrated_twice(&self) -> Self::Store;
}

/// Acquire, contend, renew (same token), release, re-acquire (bigger token).
pub async fn lifecycle<B: LeaseBackend>(backend: &B) {
    let store = backend.store();
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
pub async fn stale_fence_rejected<B: LeaseBackend>(backend: &B) {
    let store = backend.store();
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

/// Many holders race for one key: exactly one wins, the rest see `Held`,
/// and a second process sees the same holder.
pub async fn concurrent_acquire_has_one_winner<B: LeaseBackend>(backend: &B) {
    let store = backend.store();
    let mut tasks = Vec::new();
    for n in 0..RACERS {
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

    let other = backend.second_process().await;
    assert!(matches!(
        other.acquire("encounter:42", "other-process", TTL).await,
        Err(LeaseError::Held { .. })
    ));
}

pub async fn invalid_inputs_rejected<B: LeaseBackend>(backend: &B) {
    let store = backend.store();
    for result in [
        store.acquire("", "h", TTL).await,
        store.acquire("k", "", TTL).await,
        store.acquire("k", "h", Duration::ZERO).await,
    ] {
        assert!(matches!(result, Err(LeaseError::Invalid(_))), "{result:?}");
    }
}

/// A holder stalls past its TTL; another process takes over with the next
/// token, and the old holder can neither renew, release nor pass the fence.
pub async fn expiry_takeover_bumps_token_and_fences_old_holder<B: LeaseBackend>(backend: &B) {
    let store = backend.store();
    let ttl = backend.short_ttl();
    let old = store.acquire("boss:lich", "zone-a", ttl).await.unwrap();

    backend.elapse(ttl).await;
    assert_eq!(store.current("boss:lich").await.unwrap(), None);
    let new = backend
        .second_process()
        .await
        .acquire("boss:lich", "zone-b", TTL)
        .await
        .unwrap();
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

/// Renewed within its TTL, a lease outlives that TTL many times over.
pub async fn renewal_keeps_the_lease_past_its_ttl<B: ControlledClock>(backend: &B) {
    let store = backend.store();
    let ttl = backend.short_ttl();
    let lease = store.acquire("boss:lich", "zone-a", ttl).await.unwrap();
    for _ in 0..4 {
        backend.elapse(ttl / 3).await;
        store.renew(&lease, ttl).await.unwrap();
    }
    assert!(matches!(
        store.acquire("boss:lich", "zone-b", ttl).await,
        Err(LeaseError::Held { .. })
    ));
}

/// Racers for an expired lease: exactly one takes it, with the next token.
pub async fn concurrent_takeover_after_expiry_has_one_winner<B: LeaseBackend>(backend: &B) {
    let store = backend.store();
    let ttl = backend.short_ttl();
    let first = store.acquire("k", "a", ttl).await.unwrap();
    backend.elapse(ttl).await;
    let mut tasks = Vec::new();
    for n in 0..RACERS {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            store.acquire("k", &format!("h{n}"), TTL).await
        }));
    }
    let mut tokens = Vec::new();
    for task in tasks {
        match task.await.unwrap() {
            Ok(lease) => tokens.push(lease.token),
            Err(LeaseError::Held { .. }) => {}
            Err(other) => panic!("unexpected acquire error: {other}"),
        }
    }
    assert_eq!(tokens, vec![FencingToken::new(first.token.get() + 1)]);
}

pub async fn standalone_migration_is_idempotent<B: StandaloneMigration>(backend: &B) {
    let store = backend.migrated_twice().await;
    store.acquire("k", "h", TTL).await.unwrap();
}

/// The in-memory store on a [`ManualClock`].
pub struct InMemory {
    clock: ManualClock,
    store: InMemoryLeaseStore<ManualClock>,
}

impl LeaseBackend for InMemory {
    type Store = InMemoryLeaseStore<ManualClock>;

    async fn open(_case: &str) -> Option<Self> {
        let clock = ManualClock::starting_now();
        Some(Self {
            store: InMemoryLeaseStore::with_clock(clock.clone()),
            clock,
        })
    }

    fn store(&self) -> Self::Store {
        self.store.clone()
    }

    /// The in-memory store is shared by its clones; a process boundary does
    /// not exist for it.
    async fn second_process(&self) -> Self::Store {
        self.store.clone()
    }

    fn short_ttl(&self) -> Duration {
        Duration::from_secs(15)
    }

    async fn elapse(&self, ttl: Duration) {
        self.clock.advance(ttl);
    }
}

impl ControlledClock for InMemory {}

/// Wait out `ttl` on the database clock, with a margin for the gap between
/// this host's clock and the database's.
#[cfg(any(feature = "sqlite", feature = "postgres"))]
async fn wait_out(ttl: Duration) {
    tokio::time::sleep(ttl + Duration::from_millis(150)).await;
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
const SQL_SHORT_TTL: Duration = Duration::from_millis(200);

#[cfg(feature = "sqlite")]
pub use self::sqlite::Sqlite;

#[cfg(feature = "sqlite")]
mod sqlite {
    use std::time::Duration;

    use distributed::lease::SqliteLeaseStore;
    use distributed::SqliteRepository;

    use super::{wait_out, LeaseBackend, StandaloneMigration, SQL_SHORT_TTL};
    use crate::sqlite::TempDb;

    /// SQLite: a temp-file database shared by every pool over it.
    pub struct Sqlite {
        db: TempDb,
        store: SqliteLeaseStore,
        /// Fresh, unmigrated storage for [`StandaloneMigration`].
        bare: TempDb,
    }

    impl LeaseBackend for Sqlite {
        type Store = SqliteLeaseStore;

        async fn open(case: &str) -> Option<Self> {
            let db = TempDb::new(&format!("lease_{case}"));
            let pool = db.pool().await;
            // The repository migrations create `coordination_leases`.
            SqliteRepository::migrate_pool(&pool).await.unwrap();
            Some(Self {
                store: SqliteLeaseStore::new(pool),
                db,
                bare: TempDb::new(&format!("lease_bare_{case}")),
            })
        }

        fn store(&self) -> SqliteLeaseStore {
            self.store.clone()
        }

        async fn second_process(&self) -> SqliteLeaseStore {
            SqliteLeaseStore::new(self.db.pool().await)
        }

        fn short_ttl(&self) -> Duration {
            SQL_SHORT_TTL
        }

        async fn elapse(&self, ttl: Duration) {
            wait_out(ttl).await;
        }
    }

    impl StandaloneMigration for Sqlite {
        async fn migrated_twice(&self) -> SqliteLeaseStore {
            let pool = self.bare.pool().await;
            SqliteLeaseStore::migrate(&pool).await.unwrap();
            SqliteLeaseStore::migrate(&pool).await.unwrap();
            SqliteLeaseStore::new(pool)
        }
    }
}

#[cfg(feature = "postgres")]
pub use self::postgres::Postgres;

#[cfg(feature = "postgres")]
mod postgres {
    use std::time::Duration;

    use distributed::lease::PostgresLeaseStore;

    use super::{wait_out, LeaseBackend, StandaloneMigration, SQL_SHORT_TTL};
    use crate::postgres::PostgresTestSchema;

    /// Postgres: a fresh schema of `DATABASE_URL` per test.
    pub struct Postgres {
        schema: PostgresTestSchema,
        store: PostgresLeaseStore,
        /// Fresh, unmigrated storage for [`StandaloneMigration`].
        bare: PostgresTestSchema,
    }

    impl LeaseBackend for Postgres {
        type Store = PostgresLeaseStore;

        async fn open(case: &str) -> Option<Self> {
            let schema = PostgresTestSchema::create_from_env("leases", case).await?;
            let bare = PostgresTestSchema::create_from_env("leases_bare", case).await?;
            let store = PostgresLeaseStore::new(schema.repository().await.pool().clone());
            Some(Self {
                schema,
                store,
                bare,
            })
        }

        fn store(&self) -> PostgresLeaseStore {
            self.store.clone()
        }

        async fn second_process(&self) -> PostgresLeaseStore {
            PostgresLeaseStore::new(self.schema.repository().await.pool().clone())
        }

        fn short_ttl(&self) -> Duration {
            SQL_SHORT_TTL
        }

        async fn elapse(&self, ttl: Duration) {
            wait_out(ttl).await;
        }
    }

    impl StandaloneMigration for Postgres {
        async fn migrated_twice(&self) -> PostgresLeaseStore {
            let pool = self.bare.repository_unmigrated().await.pool().clone();
            PostgresLeaseStore::migrate(&pool).await.unwrap();
            PostgresLeaseStore::migrate(&pool).await.unwrap();
            PostgresLeaseStore::new(pool)
        }
    }
}

/// Expands every lease-store conformance case as a `#[tokio::test]` for
/// `$backend`, a [`LeaseBackend`]. Each capability must be stated.
///
/// ```ignore
/// common::lease_conformance_tests!(common::lease::Sqlite, controlled_clock: false, standalone_migration: true);
/// ```
#[macro_export]
macro_rules! lease_conformance_tests {
    ($backend:ty, controlled_clock: $clock:tt, standalone_migration: $migration:tt $(,)?) => {
        $crate::conformance_cases! { $crate::lease::LeaseBackend, $backend, open;
            #[tokio::test]
            lifecycle(b) => $crate::lease::lifecycle(&b);
            #[tokio::test]
            stale_fence_rejected(b) => $crate::lease::stale_fence_rejected(&b);
            #[tokio::test]
            concurrent_acquire_has_one_winner(b) => $crate::lease::concurrent_acquire_has_one_winner(&b);
            #[tokio::test]
            invalid_inputs_rejected(b) => $crate::lease::invalid_inputs_rejected(&b);
            #[tokio::test]
            expiry_takeover_bumps_token_and_fences_old_holder(b) =>
                $crate::lease::expiry_takeover_bumps_token_and_fences_old_holder(&b);
            #[tokio::test]
            concurrent_takeover_after_expiry_has_one_winner(b) =>
                $crate::lease::concurrent_takeover_after_expiry_has_one_winner(&b);
        }
        $crate::conformance_cases! { @if $clock $crate::lease::LeaseBackend, $backend, open;
            #[tokio::test]
            renewal_keeps_the_lease_past_its_ttl(b) => $crate::lease::renewal_keeps_the_lease_past_its_ttl(&b);
        }
        $crate::conformance_cases! { @if $migration $crate::lease::LeaseBackend, $backend, open;
            #[tokio::test]
            standalone_migration_is_idempotent(b) => $crate::lease::standalone_migration_is_idempotent(&b);
        }
    };
}
