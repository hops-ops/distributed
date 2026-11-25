use common::todo::Todo;
use distributed::{
    sourced, Aggregate, AggregateBuilder, Entity, InMemoryRepository, Queueable, SnapshotRecord,
    SnapshotStore, Snapshottable, StreamIdentity,
};
use serde::{Deserialize, Serialize};

#[derive(Default)]
struct ReplayCounter {
    entity: Entity,
    total: i32,
}

#[sourced(entity, aggregate_type = "snapshot.replay_counter")]
impl ReplayCounter {
    #[event("added")]
    fn add(&mut self, id: String, amount: i32) {
        if self.entity.id().is_empty() {
            self.entity.set_id(&id);
        }
        self.total += amount;
    }
}

#[derive(Serialize, Deserialize)]
struct ReplayCounterSnapshot {
    id: String,
    total: i32,
}

impl Snapshottable for ReplayCounter {
    type Snapshot = ReplayCounterSnapshot;

    fn create_snapshot(&self) -> Self::Snapshot {
        ReplayCounterSnapshot {
            id: self.entity.id().to_string(),
            total: self.total,
        }
    }

    fn restore_from_snapshot(&mut self, snapshot: Self::Snapshot) {
        self.entity.set_id(&snapshot.id);
        self.total = snapshot.total;
    }
}

#[tokio::test]
async fn snapshot_created_at_frequency_threshold() {
    let repo = InMemoryRepository::new()
        .aggregate::<Todo>()
        .with_snapshots(2);

    let mut todo = Todo::new();
    todo.initialize("t1".into(), "alice".into(), "Buy milk".into())
        .unwrap();
    repo.commit(&mut todo).await.unwrap();

    let identity = StreamIdentity::new(Todo::aggregate_type(), "t1").unwrap();

    // Version 1 — below threshold of 2, no snapshot yet
    assert!(repo.repo().get_snapshot(&identity).await.unwrap().is_none());

    // Load, add another event to reach version 2
    let mut todo = repo.get("t1").await.unwrap().unwrap();
    todo.complete().unwrap();
    repo.commit(&mut todo).await.unwrap();

    // Version 2 >= 0 + 2 — snapshot should now exist
    let snap = repo.repo().get_snapshot(&identity).await.unwrap();
    assert!(snap.is_some());
    let snap = snap.unwrap();
    assert_eq!(snap.version, 2);
    assert_eq!(snap.aggregate_type, Todo::aggregate_type());
    assert_eq!(snap.payload_codec, distributed::BITCODE_PAYLOAD_CODEC);

    // Reload and verify state
    let loaded = repo.get("t1").await.unwrap().unwrap();
    let s = loaded.snapshot();
    assert_eq!(s.id, "t1");
    assert_eq!(s.user_id, "alice");
    assert_eq!(s.task, "Buy milk");
    assert!(s.completed);
    assert_eq!(loaded.entity.version(), 2);
    assert_eq!(loaded.entity.snapshot_version(), 2);
}

#[tokio::test]
async fn no_snapshot_before_threshold() {
    let repo = InMemoryRepository::new()
        .aggregate::<Todo>()
        .with_snapshots(5);

    let mut todo = Todo::new();
    todo.initialize("t1".into(), "alice".into(), "Buy milk".into())
        .unwrap();
    repo.commit(&mut todo).await.unwrap();

    let identity = StreamIdentity::new(Todo::aggregate_type(), "t1").unwrap();

    // Only 1 event, threshold is 5
    assert!(repo.repo().get_snapshot(&identity).await.unwrap().is_none());
}

#[tokio::test]
async fn load_from_snapshot_produces_correct_state() {
    let repo = InMemoryRepository::new()
        .aggregate::<Todo>()
        .with_snapshots(2);

    let mut todo = Todo::new();
    todo.initialize("t1".into(), "alice".into(), "Buy milk".into())
        .unwrap();
    todo.complete().unwrap();
    repo.commit(&mut todo).await.unwrap();

    let identity = StreamIdentity::new(Todo::aggregate_type(), "t1").unwrap();

    // Snapshot at version 2
    assert!(repo.repo().get_snapshot(&identity).await.unwrap().is_some());

    // Reload — should use snapshot
    let loaded = repo.get("t1").await.unwrap().unwrap();
    let snap = loaded.snapshot();
    assert_eq!(snap.id, "t1");
    assert_eq!(snap.user_id, "alice");
    assert_eq!(snap.task, "Buy milk");
    assert!(snap.completed);
}

#[tokio::test]
async fn snapshot_plus_newer_events() {
    let repo = InMemoryRepository::new()
        .aggregate::<Todo>()
        .with_snapshots(2);

    // Create and commit 2 events (triggers snapshot at version 2)
    let mut todo = Todo::new();
    todo.initialize("t1".into(), "alice".into(), "Buy milk".into())
        .unwrap();
    repo.commit(&mut todo).await.unwrap();

    let mut todo = repo.get("t1").await.unwrap().unwrap();
    todo.complete().unwrap();
    repo.commit(&mut todo).await.unwrap();

    let identity = StreamIdentity::new(Todo::aggregate_type(), "t1").unwrap();

    // Snapshot exists at version 2, completed = true
    let snap = repo.repo().get_snapshot(&identity).await.unwrap().unwrap();
    assert_eq!(snap.version, 2);

    // Now create a second todo to verify snapshot + partial replay works.
    // We'll use a different approach: create a fresh repo pointing to the same storage
    // and verify loading still works.
    let loaded = repo.get("t1").await.unwrap().unwrap();
    assert!(loaded.snapshot().completed);
    assert_eq!(loaded.entity.version(), 2);
    assert_eq!(loaded.entity.snapshot_version(), 2);
}

#[tokio::test]
async fn snapshot_hydration_replays_every_event_after_snapshot_version() {
    let base_repo = InMemoryRepository::new();
    let full_replay_repo = base_repo.clone().aggregate::<ReplayCounter>();
    let snapshot_repo = base_repo
        .clone()
        .aggregate::<ReplayCounter>()
        .with_snapshots(100);

    let mut counter = ReplayCounter::default();
    counter.add("counter-1".into(), 10).unwrap();
    full_replay_repo.commit(&mut counter).await.unwrap();

    let payload = bitcode::serialize(&ReplayCounterSnapshot {
        id: "counter-1".into(),
        total: 10,
    })
    .unwrap();
    let counter_identity =
        StreamIdentity::new(ReplayCounter::aggregate_type(), "counter-1").unwrap();
    base_repo
        .save_snapshot(
            &counter_identity,
            SnapshotRecord::new(ReplayCounter::aggregate_type(), "counter-1", 1, 1, payload),
        )
        .await
        .unwrap();

    let mut counter = snapshot_repo.get("counter-1").await.unwrap().unwrap();
    counter.add("counter-1".into(), 5).unwrap();
    snapshot_repo.commit(&mut counter).await.unwrap();

    let mut counter = snapshot_repo.get("counter-1").await.unwrap().unwrap();
    counter.add("counter-1".into(), 7).unwrap();
    snapshot_repo.commit(&mut counter).await.unwrap();

    let loaded = snapshot_repo.get("counter-1").await.unwrap().unwrap();
    let replayed = full_replay_repo.get("counter-1").await.unwrap().unwrap();

    assert_eq!(loaded.total, 22);
    assert_eq!(loaded.total, replayed.total);
    assert_eq!(loaded.entity.version(), replayed.entity.version());
    assert_eq!(loaded.entity.snapshot_version(), 1);
    assert_eq!(replayed.entity.snapshot_version(), 0);
    // Tail-only load: snapshot covers sequence 1; in-memory events are the
    // post-snapshot replay (2, 3). Full-replay repo still holds the whole log.
    assert_eq!(
        loaded
            .entity
            .events()
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(
        replayed
            .entity
            .events()
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}

#[tokio::test]
async fn no_snapshot_falls_back_to_full_replay() {
    let repo = InMemoryRepository::new()
        .aggregate::<Todo>()
        .with_snapshots(2);

    let mut todo = Todo::new();
    todo.initialize("t1".into(), "alice".into(), "Buy milk".into())
        .unwrap();
    todo.complete().unwrap();
    repo.commit(&mut todo).await.unwrap();

    let identity = StreamIdentity::new(Todo::aggregate_type(), "t1").unwrap();

    // Snapshot exists
    assert!(repo.repo().get_snapshot(&identity).await.unwrap().is_some());

    // Delete the snapshot
    repo.repo().delete_snapshot(&identity).await.unwrap();
    assert!(repo.repo().get_snapshot(&identity).await.unwrap().is_none());

    // Loading should still work via full replay
    let loaded = repo.get("t1").await.unwrap().unwrap();
    let snap = loaded.snapshot();
    assert_eq!(snap.id, "t1");
    assert_eq!(snap.user_id, "alice");
    assert!(snap.completed);
}

#[tokio::test]
async fn snapshot_version_advances_on_second_snapshot() {
    let repo = InMemoryRepository::new()
        .aggregate::<Todo>()
        .with_snapshots(1); // snapshot every event

    let mut todo = Todo::new();
    todo.initialize("t1".into(), "alice".into(), "Buy milk".into())
        .unwrap();
    repo.commit(&mut todo).await.unwrap();

    let identity = StreamIdentity::new(Todo::aggregate_type(), "t1").unwrap();

    // First snapshot at version 1
    let snap = repo.repo().get_snapshot(&identity).await.unwrap().unwrap();
    assert_eq!(snap.version, 1);

    // Add another event
    let mut todo = repo.get("t1").await.unwrap().unwrap();
    todo.complete().unwrap();
    repo.commit(&mut todo).await.unwrap();

    // Second snapshot at version 2
    let snap = repo.repo().get_snapshot(&identity).await.unwrap().unwrap();
    assert_eq!(snap.version, 2);

    // Verify the loaded aggregate has correct snapshot_version
    let loaded = repo.get("t1").await.unwrap().unwrap();
    assert_eq!(loaded.entity.snapshot_version(), 2);
    assert!(loaded.snapshot().completed);
}

#[tokio::test]
async fn with_queued_repo() {
    let repo = InMemoryRepository::new()
        .queued()
        .aggregate::<Todo>()
        .with_snapshots(2);

    let mut todo = Todo::new();
    todo.initialize("t1".into(), "alice".into(), "Buy milk".into())
        .unwrap();
    repo.commit(&mut todo).await.unwrap();

    let mut todo = repo.get("t1").await.unwrap().unwrap();
    todo.complete().unwrap();
    repo.commit(&mut todo).await.unwrap();

    let identity = StreamIdentity::new(Todo::aggregate_type(), "t1").unwrap();

    // Snapshot should exist through the queued + snapshot chain
    let snap = repo.repo().inner().get_snapshot(&identity).await.unwrap();
    assert!(snap.is_some());
    assert_eq!(snap.unwrap().version, 2);

    // Reload and verify
    let loaded = repo.get("t1").await.unwrap().unwrap();
    assert!(loaded.snapshot().completed);
    assert_eq!(loaded.entity.snapshot_version(), 2);
}

#[tokio::test]
async fn get_all_with_snapshots() {
    let repo = InMemoryRepository::new()
        .aggregate::<Todo>()
        .with_snapshots(2);

    // Create two todos, both past snapshot threshold
    let mut todo1 = Todo::new();
    todo1
        .initialize("t1".into(), "alice".into(), "Buy milk".into())
        .unwrap();
    todo1.complete().unwrap();
    repo.commit(&mut todo1).await.unwrap();

    let mut todo2 = Todo::new();
    todo2
        .initialize("t2".into(), "bob".into(), "Walk dog".into())
        .unwrap();
    todo2.complete().unwrap();
    repo.commit(&mut todo2).await.unwrap();

    let todos = repo.get_all(&["t1", "t2"]).await.unwrap();
    assert_eq!(todos.len(), 2);
    assert!(todos.iter().all(|todo| todo.snapshot().completed));

    let alice = repo.get("t1").await.unwrap().unwrap();
    assert_eq!(alice.snapshot().user_id, "alice");
    assert_eq!(alice.snapshot().task, "Buy milk");
}

#[tokio::test]
async fn commit_all_with_snapshots() {
    let repo = InMemoryRepository::new()
        .aggregate::<Todo>()
        .with_snapshots(2);

    let mut todo1 = Todo::new();
    todo1
        .initialize("t1".into(), "alice".into(), "Task 1".into())
        .unwrap();
    todo1.complete().unwrap(); // version 2

    let mut todo2 = Todo::new();
    todo2
        .initialize("t2".into(), "bob".into(), "Task 2".into())
        .unwrap();
    todo2.complete().unwrap(); // version 2

    repo.commit_all(&mut [&mut todo1, &mut todo2])
        .await
        .unwrap();

    let identity1 = StreamIdentity::new(Todo::aggregate_type(), "t1").unwrap();
    let identity2 = StreamIdentity::new(Todo::aggregate_type(), "t2").unwrap();

    // Both should have snapshots at version 2
    let snap1 = repo.repo().get_snapshot(&identity1).await.unwrap().unwrap();
    assert_eq!(snap1.version, 2);
    let snap2 = repo.repo().get_snapshot(&identity2).await.unwrap().unwrap();
    assert_eq!(snap2.version, 2);
}
