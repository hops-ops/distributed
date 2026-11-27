//! SQLite-backed hardening tests for snapshot loading.
//!
//! These cover the two snapshot hardening changes against a *real* queryable
//! store (so the `WHERE sequence > $version` tail fetch and the schema-version
//! gate are exercised end to end, not just the in-memory path):
//!
//! 1. A snapshot-hydrated load must fetch only the post-snapshot tail — it must
//!    not read pre-snapshot rows. Proven by deleting the pre-snapshot rows and
//!    showing the load still succeeds with correct state (a full replay would be
//!    impossible once those rows are gone).
//! 2. A snapshot whose stored schema version does not match the aggregate's
//!    current `SNAPSHOT_VERSION` (e.g. written from an older, differently shaped
//!    struct) must NOT be decoded. It is treated as a cache miss and the
//!    aggregate is rebuilt by full replay to the correct final state — never
//!    silently wrong.

use distributed::{
    sourced, Aggregate, AggregateBuilder, Entity, Snapshot, SnapshotRecord, SnapshotStore,
    Snapshottable, SqliteRepository, StreamIdentity,
};
use serde::{Deserialize, Serialize};

// Aggregate under test. `SNAPSHOT_VERSION` defaults to 1 (no override).
#[derive(Default, Snapshot)]
struct Counter {
    pub entity: Entity,
    pub total: i64,
}

#[sourced(entity, aggregate_type = "sqlite.snap.counter")]
impl Counter {
    #[event("added")]
    fn add(&mut self, id: String, amount: i64) {
        if self.entity.id().is_empty() {
            self.entity.set_id(&id);
        }
        self.total += amount;
    }
}

async fn repository() -> SqliteRepository {
    SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .expect("sqlite in-memory repository should migrate")
}

fn identity(id: &str) -> StreamIdentity {
    StreamIdentity::new(Counter::aggregate_type(), id).unwrap()
}

/// Build a stream of `n` `added(+1)` events and a snapshot covering all of them,
/// returning the repository with both events and snapshot persisted.
async fn seed_counter(repo: &SqliteRepository, id: &str, n: i64) {
    // Snapshot every event so a snapshot covering the whole stream exists.
    let counter_repo = repo.clone().aggregate::<Counter>().with_snapshots(1);
    let mut counter = Counter::default();
    for _ in 0..n {
        counter.add(id.into(), 1).unwrap();
        counter_repo.commit(&mut counter).await.unwrap();
        // reload so each commit advances snapshot_version correctly
        counter = counter_repo.get(id).await.unwrap().unwrap();
    }
}

async fn count_event_rows(repo: &SqliteRepository, id: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM aggregate_events WHERE aggregate_type = ? AND aggregate_id = ?",
    )
    .bind(Counter::aggregate_type())
    .bind(id)
    .fetch_one(repo.pool())
    .await
    .unwrap()
}

#[tokio::test]
async fn snapshot_hydrated_load_does_not_read_pre_snapshot_rows() {
    let repo = repository().await;
    let id = "c-tail";

    // 5 events, snapshot at version 5 (total = 5).
    seed_counter(&repo, id, 5).await;

    let snap = repo.get_snapshot(&identity(id)).await.unwrap().unwrap();
    assert_eq!(snap.version, 5, "snapshot should cover the whole stream");

    // Physically delete every row before the last. After this, a *full* replay
    // is impossible (the early events are gone); only a snapshot+tail load can
    // reconstruct correct state. The last row stays: a stream with no rows is
    // missing, whatever snapshot lingers for it.
    sqlx::query("DELETE FROM aggregate_events WHERE aggregate_type = ? AND aggregate_id = ? AND sequence < ?")
        .bind(Counter::aggregate_type())
        .bind(id)
        .bind(5_i64)
        .execute(repo.pool())
        .await
        .unwrap();
    assert_eq!(
        count_event_rows(&repo, id).await,
        1,
        "only the row the snapshot ends on remains"
    );

    // Load via snapshot + (empty) tail. The total comes entirely from the
    // snapshot; no pre-snapshot row is read.
    let counter_repo = repo.clone().aggregate::<Counter>().with_snapshots(1);
    let loaded = counter_repo.get(id).await.unwrap().unwrap();
    assert_eq!(loaded.total, 5, "state reconstructed from snapshot alone");
    assert_eq!(
        loaded.entity.version(),
        5,
        "version reflects true stream position despite no rows in memory"
    );
    assert_eq!(loaded.entity.snapshot_version(), 5);
    assert!(
        loaded.entity.events().is_empty(),
        "snapshot-hydrated entity holds only the tail (empty here)"
    );
}

#[tokio::test]
async fn snapshot_plus_tail_only_reads_post_snapshot_rows() {
    let repo = repository().await;
    let id = "c-partial";

    // 3 events + snapshot at version 3 (total = 3).
    seed_counter(&repo, id, 3).await;
    assert_eq!(
        repo.get_snapshot(&identity(id))
            .await
            .unwrap()
            .unwrap()
            .version,
        3
    );

    // Append 2 more events WITHOUT updating the snapshot (commit via a
    // snapshotless repo so the snapshot stays at version 3).
    let plain_repo = repo.clone().aggregate::<Counter>();
    let mut counter = plain_repo.get(id).await.unwrap().unwrap();
    counter.add(id.into(), 1).unwrap();
    counter.add(id.into(), 1).unwrap();
    plain_repo.commit(&mut counter).await.unwrap();

    // Delete the pre-snapshot rows (sequence <= 3). A full replay can no longer
    // succeed; only snapshot(v3) + tail(seq 4,5) can.
    sqlx::query("DELETE FROM aggregate_events WHERE aggregate_type = ? AND aggregate_id = ? AND sequence <= ?")
        .bind(Counter::aggregate_type())
        .bind(id)
        .bind(3_i64)
        .execute(repo.pool())
        .await
        .unwrap();
    assert_eq!(
        count_event_rows(&repo, id).await,
        2,
        "only the tail rows remain"
    );

    let snap_repo = repo.clone().aggregate::<Counter>().with_snapshots(1);
    let loaded = snap_repo.get(id).await.unwrap().unwrap();
    assert_eq!(loaded.total, 5, "snapshot(3) + tail(2) = 5");
    assert_eq!(loaded.entity.version(), 5);
    assert_eq!(
        loaded.entity.events().len(),
        2,
        "only the post-snapshot tail is held in memory"
    );
    assert_eq!(
        loaded
            .entity
            .events()
            .iter()
            .map(|e| e.sequence)
            .collect::<Vec<_>>(),
        vec![4, 5],
        "the in-memory tail is exactly the post-snapshot rows"
    );
}

// An older snapshot payload shape. Field order/types differ from the current
// `CounterSnapshot { id, total }`, so decoding these bytes as the current type
// would silently mis-read (bitcode is positional). We persist a record with a
// *mismatched* schema version so the gate refuses to decode it.
#[derive(Serialize, Deserialize)]
struct LegacyCounterSnapshot {
    // A different layout: an extra leading field and a swapped tail. Decoding
    // this as the current snapshot would either error or, worse, decode into
    // the wrong state.
    pub legacy_flag: bool,
    pub label: String,
    pub total: i64,
    pub id: String,
}

#[tokio::test]
async fn stale_schema_snapshot_falls_back_to_replay_with_correct_state() {
    let repo = repository().await;
    let id = "c-stale";

    // Real event history: total should be 1 + 2 + 3 = 6 by replay.
    let plain_repo = repo.clone().aggregate::<Counter>();
    let mut counter = Counter::default();
    counter.add(id.into(), 1).unwrap();
    counter.add(id.into(), 2).unwrap();
    counter.add(id.into(), 3).unwrap();
    plain_repo.commit(&mut counter).await.unwrap();

    // Persist a snapshot from the LEGACY shape with a wrong `total` (999) and a
    // mismatched schema version (2 != Counter::SNAPSHOT_VERSION == 1). If the
    // gate were absent and bytes happened to decode, state would be wrong.
    let legacy = LegacyCounterSnapshot {
        legacy_flag: true,
        label: "old".into(),
        total: 999,
        id: id.into(),
    };
    let payload = bitcode::serialize(&legacy).unwrap();
    assert_ne!(
        Counter::SNAPSHOT_VERSION,
        2,
        "test assumes the current schema version is not 2"
    );
    repo.save_snapshot(
        &identity(id),
        SnapshotRecord::new(
            Counter::aggregate_type(),
            id,
            3,
            2, // stored schema version — deliberately != current
            payload,
        ),
    )
    .await
    .unwrap();

    // Load with the CURRENT aggregate. The schema-version mismatch is a cache
    // miss → full replay → correct total 6, never the snapshot's 999.
    let snap_repo = repo.clone().aggregate::<Counter>().with_snapshots(1);
    let loaded = snap_repo.get(id).await.unwrap().unwrap();
    assert_eq!(
        loaded.total, 6,
        "stale-schema snapshot must not be decoded; replay yields correct state"
    );
    assert_eq!(loaded.entity.version(), 3);
}

#[tokio::test]
async fn snapshot_hydrated_state_equals_full_replay_state() {
    let repo = repository().await;
    let id = "c-equiv";

    // Build a stream and a covering snapshot at version 4.
    seed_counter(&repo, id, 4).await;
    // Append two more events without refreshing the snapshot.
    let plain_repo = repo.clone().aggregate::<Counter>();
    let mut counter = plain_repo.get(id).await.unwrap().unwrap();
    counter.add(id.into(), 10).unwrap();
    counter.add(id.into(), 100).unwrap();
    plain_repo.commit(&mut counter).await.unwrap();

    // Full replay (snapshotless repo) vs snapshot-hydrated load.
    let replayed = plain_repo.get(id).await.unwrap().unwrap();
    let snap_repo = repo.clone().aggregate::<Counter>().with_snapshots(1);
    let hydrated = snap_repo.get(id).await.unwrap().unwrap();

    assert_eq!(replayed.total, 4 + 10 + 100);
    assert_eq!(hydrated.total, replayed.total, "states must match exactly");
    assert_eq!(hydrated.entity.version(), replayed.entity.version());
}
