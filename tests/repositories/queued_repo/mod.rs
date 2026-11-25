//! `QueuedRepository` — per-aggregate serialization over the repository
//! surface. Proves `.queued().aggregate::<T>()` engages the async lock on
//! `get`/`commit`, plus per-aggregate granularity, the `no_lock` opt-out,
//! and release on drop.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use distributed::{
    sourced, Aggregate, AggregateBuilder, AggregateRepository, Entity, GetStream,
    InMemoryLockManager, InMemoryRepository, Queueable, StreamIdentity,
};
use tokio::sync::Barrier;

#[derive(Default)]
struct Counter {
    entity: Entity,
    value: i32,
}

#[sourced(entity, aggregate_type = "queued.counter")]
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

type QueuedCounterRepo = AggregateRepository<
    distributed::QueuedRepository<InMemoryRepository, InMemoryLockManager>,
    Counter,
>;

fn queued_repo() -> Arc<QueuedCounterRepo> {
    Arc::new(InMemoryRepository::new().queued().aggregate::<Counter>())
}

async fn seed(repo: &QueuedCounterRepo, id: &str) {
    let mut counter = Counter::default();
    counter.create(id.into()).unwrap();
    // A bare commit (no prior locking load) takes the lock handle and releases
    // it on success — it does not deadlock and leaves the lock free.
    repo.commit(&mut counter).await.unwrap();
}

// Paused time: the runtime advances the clock only once every task is idle,
// so after the sleep task 2 has provably run as far as it can — it is parked
// on the lock, not merely not yet scheduled.
#[tokio::test(start_paused = true)]
async fn second_load_waits_until_first_commits() {
    let repo = queued_repo();
    seed(&repo, "c1").await;

    // Task 1 loads (acquires + HOLDS the per-stream lock).
    let mut held = repo.get("c1").await.unwrap().unwrap();
    held.increment("c1".into(), 5).unwrap();

    // Task 2 tries to load the same aggregate; it must park on the held lock.
    let acquired = Arc::new(AtomicBool::new(false));
    let task_repo = Arc::clone(&repo);
    let task_flag = Arc::clone(&acquired);
    let task2 = tokio::spawn(async move {
        let loaded = task_repo.get("c1").await.unwrap().unwrap();
        task_flag.store(true, Ordering::SeqCst);
        loaded
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !acquired.load(Ordering::SeqCst) && !task2.is_finished(),
        "the second load must block while the first holds the lock"
    );

    // Committing the first load releases the lock, unblocking task 2.
    repo.commit(&mut held).await.unwrap();
    let loaded = task2.await.unwrap();

    assert!(acquired.load(Ordering::SeqCst));
    // Task 2 loaded AFTER the commit, so it observes the first increment.
    assert_eq!(
        loaded.value, 5,
        "serialized load sees the prior writer's committed state"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distinct_aggregates_do_not_block_each_other() {
    let repo = queued_repo();
    seed(&repo, "a").await;
    seed(&repo, "b").await;

    // Hold the lock on "a".
    let _held_a = repo.get("a").await.unwrap().unwrap();

    // Loading a different aggregate must not wait on "a"'s lock.
    let got_b = tokio::time::timeout(Duration::from_millis(500), repo.get("b"))
        .await
        .expect("loading a distinct aggregate must not block")
        .unwrap();
    assert!(got_b.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peek_reads_without_acquiring_the_lock() {
    let repo = queued_repo();
    seed(&repo, "c1").await;

    // Hold the lock on "c1".
    let _held = repo.get("c1").await.unwrap().unwrap();

    // A no-lock peek of the same aggregate must not block.
    let peeked = tokio::time::timeout(Duration::from_millis(500), repo.peek("c1"))
        .await
        .expect("no_lock peek must not block on a held lock")
        .unwrap();
    assert!(peeked.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_repo_n_writers_commit_in_fifo_order() {
    // N writers race to increment the SAME aggregate. The queued repo serializes
    // load→commit under a per-stream async lock, so the writes must apply one at
    // a time. We pin two invariants:
    //   1. The final version is exactly N (no lost updates, no double-applies).
    //   2. The committed event order equals the lock-grant order — each writer
    //      records the order in which it *acquired* the lock (the instant its
    //      `get` returned), and stamps that grant index into its event payload.
    //      Replaying the stream must then reproduce that exact grant order.
    const WRITERS: usize = 10;

    let base = InMemoryRepository::new();
    let reader = base.clone();
    let repo: Arc<QueuedCounterRepo> = Arc::new(base.queued().aggregate::<Counter>());

    // Seed the aggregate so every writer takes the load-modify-commit path.
    {
        let mut counter = Counter::default();
        counter.create("fifo".into()).unwrap();
        repo.commit(&mut counter).await.unwrap();
    }

    // Shared log of the order in which writers were granted the lock.
    let grant_order = Arc::new(Mutex::new(Vec::<usize>::new()));
    // Release all writers together so they genuinely contend for the lock.
    let barrier = Arc::new(Barrier::new(WRITERS));

    let mut handles = Vec::with_capacity(WRITERS);
    for writer in 0..WRITERS {
        let repo = Arc::clone(&repo);
        let grant_order = Arc::clone(&grant_order);
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            // `get` parks on the held lock until granted; the instant it returns
            // is this writer's place in the grant order.
            let mut counter = repo.get("fifo").await.unwrap().unwrap();
            let grant_index = {
                let mut order = grant_order.lock().unwrap();
                order.push(writer);
                order.len() - 1
            };
            // Stamp the grant index into the event so the stream records order.
            counter
                .increment("fifo".into(), grant_index as i32)
                .unwrap();
            repo.commit(&mut counter).await.unwrap();
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }

    // Invariant 1: exactly N increments landed.
    let identity = StreamIdentity::new(Counter::aggregate_type(), "fifo").unwrap();
    let entity = reader
        .get_stream(&identity)
        .await
        .unwrap()
        .expect("stream should exist");
    // version = 1 (create) + WRITERS increments.
    assert_eq!(
        entity.committed_version() as usize,
        WRITERS + 1,
        "every writer's increment must land exactly once (no lost or doubled writes)"
    );

    // Invariant 2: the increment events, in stream order, carry grant indices in
    // ascending 0..WRITERS — i.e. the stream order equals the lock-grant order.
    let grant_indices: Vec<i32> = entity
        .events()
        .iter()
        .filter(|event| event.event_name == "incremented")
        .map(|event| {
            let (_, by): (String, i32) =
                bitcode::deserialize(&event.payload).expect("payload should decode");
            by
        })
        .collect();
    let expected: Vec<i32> = (0..WRITERS as i32).collect();
    assert_eq!(
        grant_indices, expected,
        "committed event order must match the lock-grant order (strict FIFO serialization)"
    );

    let recorded_grant_order = grant_order.lock().unwrap().clone();
    assert_eq!(
        recorded_grant_order.len(),
        WRITERS,
        "every writer must have been granted the lock exactly once"
    );
}

/// A load that must not block, or the test fails instead of hanging.
async fn load_now(repo: &QueuedCounterRepo, id: &str, why: &str) -> Option<Counter> {
    tokio::time::timeout(Duration::from_millis(500), repo.get(id))
        .await
        .expect(why)
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_a_loaded_aggregate_releases_its_lock() {
    let repo = queued_repo();
    seed(&repo, "c1").await;

    // A handler that loads and then returns early (an error, a `?`) drops
    // the aggregate without committing.
    let held = repo.get("c1").await.unwrap().unwrap();
    drop(held);

    assert!(
        load_now(&repo, "c1", "a dropped load must not leak its lock")
            .await
            .is_some()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loading_a_missing_aggregate_holds_no_lock() {
    let repo = queued_repo();

    assert!(repo.get("absent").await.unwrap().is_none());

    assert!(
        load_now(&repo, "absent", "a miss must not leave the lock held")
            .await
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_commit_releases_only_the_lock_its_own_load_took() {
    let repo = queued_repo();
    seed(&repo, "c1").await;

    // Writer A holds the lock.
    let _held = repo.get("c1").await.unwrap().unwrap();

    // Writer B never took the lock (a peek). Its commit, successful or not,
    // must not release A's lock.
    let mut peeked = repo.peek("c1").await.unwrap().unwrap();
    peeked.increment("c1".into(), 1).unwrap();
    repo.commit(&mut peeked).await.unwrap();

    let blocked = tokio::time::timeout(Duration::from_millis(100), repo.get("c1")).await;
    assert!(
        blocked.is_err(),
        "the lock is still held by the writer that loaded first"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_commit_keeps_the_lock_until_the_aggregate_is_dropped() {
    let repo = queued_repo();
    seed(&repo, "c1").await;

    let mut held = repo.get("c1").await.unwrap().unwrap();
    held.increment("c1".into(), 1).unwrap();
    // Someone commits underneath without the lock, so this commit conflicts.
    let mut other = repo.peek("c1").await.unwrap().unwrap();
    other.increment("c1".into(), 7).unwrap();
    repo.commit(&mut other).await.unwrap();
    repo.commit(&mut held)
        .await
        .expect_err("the stale commit conflicts");

    let blocked = tokio::time::timeout(Duration::from_millis(100), repo.get("c1")).await;
    assert!(blocked.is_err(), "the failed writer still holds the lock");
    drop(held);
    assert!(load_now(&repo, "c1", "dropping the failed writer releases")
        .await
        .is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_all_holds_only_the_streams_it_returned() {
    let repo = queued_repo();
    seed(&repo, "a").await;

    let loaded = repo.get_all(&["a", "missing"]).await.unwrap();
    assert_eq!(loaded.len(), 1);

    assert!(load_now(&repo, "missing", "a missing id holds no lock")
        .await
        .is_none());
    let blocked = tokio::time::timeout(Duration::from_millis(100), repo.get("a")).await;
    assert!(blocked.is_err(), "the returned aggregate holds its lock");
    drop(loaded);
    assert!(load_now(&repo, "a", "dropping the batch releases")
        .await
        .is_some());
}

// ===========================================================================
// Stale-lease fencing (durable lock manager): a writer whose lease expired and
// was stolen must NOT be able to overwrite the stealing writer's commit.
// ===========================================================================

#[cfg(feature = "sqlite")]
use common::sqlite as sqlite_support;

#[cfg(feature = "sqlite")]
mod stale_lease {
    use super::*;
    use crate::queued_repo::sqlite_support::TempDb;
    use distributed::{QueuedRepository, RepositoryError, SqliteLockManager};

    /// Writer A loads (holds the lease), the lease TTL expires, writer B steals
    /// the lease and commits, then A commits its stale load. The queue lock can
    /// no longer protect A — the optimistic-concurrency check in the repository
    /// is the backstop, and A must observe `ConcurrentWrite`, not silently
    /// overwrite B.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_lease_holder_commit_is_fenced_by_optimistic_concurrency() {
        let db = TempDb::new("queued_stale_lease");
        let pool = db.pool().await;
        SqliteLockManager::migrate(&pool)
            .await
            .expect("migrate aggregate_locks");
        let ttl = Duration::from_millis(300);

        // Two "processes": separate lock managers over the same lease table,
        // one shared underlying repository.
        let inner = InMemoryRepository::new();
        let repo_a = QueuedRepository::with_lock_manager(
            inner.clone(),
            SqliteLockManager::new(pool.clone())
                .with_lease_ttl(ttl)
                .with_owner_id("writer-a"),
        )
        .aggregate::<Counter>();
        let repo_b = QueuedRepository::with_lock_manager(
            inner.clone(),
            SqliteLockManager::new(pool)
                .with_lease_ttl(ttl)
                .with_owner_id("writer-b"),
        )
        .aggregate::<Counter>();

        // Seed at version 1 (commit releases A's lock).
        let mut seed = Counter::default();
        seed.create("c1".into()).unwrap();
        repo_a.commit(&mut seed).await.unwrap();

        // A loads and HOLDS the lease, then overruns its TTL.
        let mut stale = repo_a.get("c1").await.unwrap().unwrap();
        stale.increment("c1".into(), 10).unwrap();

        // B's locking load parks on A's lease until it expires, then steals it
        // and commits — advancing the stream past A's loaded version.
        let mut winner = tokio::time::timeout(ttl + Duration::from_secs(5), repo_b.get("c1"))
            .await
            .expect("B must acquire once A's lease expires")
            .unwrap()
            .expect("counter should exist");
        winner.increment("c1".into(), 20).unwrap();
        repo_b.commit(&mut winner).await.unwrap();

        // A wakes up late and commits its stale load: the optimistic version
        // check must reject it.
        let err = repo_a
            .commit(&mut stale)
            .await
            .expect_err("a stale-lease holder must not overwrite the stealing writer");
        assert!(
            matches!(err, RepositoryError::ConcurrentWrite { .. }),
            "expected ConcurrentWrite, got {err:?}"
        );

        // B's write survived; A's did not.
        let current = repo_b.peek("c1").await.unwrap().expect("counter exists");
        assert_eq!(
            current.value, 20,
            "the stealing writer's increment is the surviving state"
        );
    }
}
