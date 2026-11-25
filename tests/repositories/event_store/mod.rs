use distributed::{
    CommitBatch, Entity, GetStream, InMemoryRepository, StreamIdentity, StreamWrite,
    TransactionalCommit,
};

/// Fixed aggregate type used to key every event stream in this crate.
///
/// The synchronous, id-only repository API (`Commit`/`GetOne`) is being removed,
/// so these event-store semantics tests now run on the async stream API
/// (`get_stream`/`commit_batch`). The async path keys streams by full
/// `StreamIdentity` (aggregate type + id), so every entity is committed and
/// loaded under this single aggregate type to mirror the old id-only behavior.
const AGGREGATE_TYPE: &str = "event_store_test";

fn identity(id: &str) -> StreamIdentity {
    StreamIdentity::new(AGGREGATE_TYPE, id).unwrap()
}

/// Equivalent of the old `repo.get_one(id)`.
async fn get_one(repo: &InMemoryRepository, id: &str) -> Option<Entity> {
    repo.get_stream(&identity(id)).await.unwrap()
}

/// Equivalent of the old `repo.commit(&mut entity)` for a single entity.
async fn commit_one(
    repo: &InMemoryRepository,
    entity: &mut Entity,
) -> Result<(), distributed::RepositoryError> {
    let id = entity.id().to_string();
    let stream = StreamWrite::new(identity(&id), entity);
    repo.commit_batch(CommitBatch::new(vec![stream])).await
}

/// Equivalent of the old `repo.commit(&mut [&mut a, &mut b])` for many entities.
async fn commit_many(
    repo: &InMemoryRepository,
    entities: &mut [&mut Entity],
) -> Result<(), distributed::RepositoryError> {
    let mut streams = Vec::with_capacity(entities.len());
    for entity in entities.iter_mut() {
        let id = entity.id().to_string();
        streams.push(StreamWrite::new(identity(&id), entity));
    }
    repo.commit_batch(CommitBatch::new(streams)).await
}

// --- Event Accumulation ---

#[test]
fn digest_adds_events_with_correct_sequences() {
    let mut entity = Entity::with_id("e1");
    entity.digest("initialized", &"data1").unwrap();
    entity.digest("updated", &"data2").unwrap();
    entity.digest("updated", &"data3").unwrap();

    assert_eq!(entity.events().len(), 3);
    assert_eq!(entity.events()[0].sequence, 1);
    assert_eq!(entity.events()[1].sequence, 2);
    assert_eq!(entity.events()[2].sequence, 3);
}

#[tokio::test]
async fn multiple_load_modify_commit_cycles_accumulate_all_events() {
    let repo = InMemoryRepository::new();

    // Cycle 1: create and commit
    let mut entity = Entity::with_id("e1");
    entity.digest("initialized", &"v1").unwrap();
    commit_one(&repo, &mut entity).await.unwrap();

    // Cycle 2: load, modify, commit
    let mut entity = get_one(&repo, "e1").await.unwrap();
    assert_eq!(entity.events().len(), 1);
    entity.digest("updated", &"v2").unwrap();
    commit_one(&repo, &mut entity).await.unwrap();

    // Cycle 3: load, modify, commit
    let mut entity = get_one(&repo, "e1").await.unwrap();
    assert_eq!(entity.events().len(), 2);
    entity.digest("updated", &"v3").unwrap();
    commit_one(&repo, &mut entity).await.unwrap();

    // Verify all events accumulated
    let entity = get_one(&repo, "e1").await.unwrap();
    assert_eq!(entity.events().len(), 3);
    assert_eq!(entity.events()[0].event_name, "initialized");
    assert_eq!(entity.events()[1].event_name, "updated");
    assert_eq!(entity.events()[2].event_name, "updated");
    assert_eq!(entity.version(), 3);
}

// --- Append Semantics ---

#[tokio::test]
async fn commit_appends_only_new_events() {
    let repo = InMemoryRepository::new();

    let mut entity = Entity::with_id("e1");
    entity.digest("initialized", &"v1").unwrap();
    commit_one(&repo, &mut entity).await.unwrap();

    // Reload and add one more event
    let mut entity = get_one(&repo, "e1").await.unwrap();
    entity.digest("updated", &"v2").unwrap();
    commit_one(&repo, &mut entity).await.unwrap();

    // Verify via get_one: exactly 2 events
    let loaded = get_one(&repo, "e1").await.unwrap();
    assert_eq!(loaded.events().len(), 2);
    assert_eq!(loaded.events()[0].event_name, "initialized");
    assert_eq!(loaded.events()[1].event_name, "updated");
}

#[tokio::test]
async fn empty_commit_is_idempotent() {
    let repo = InMemoryRepository::new();

    // Create initial state
    let mut entity = Entity::with_id("e1");
    entity.digest("initialized", &"v1").unwrap();
    commit_one(&repo, &mut entity).await.unwrap();

    // Load and commit without changes
    let mut entity = get_one(&repo, "e1").await.unwrap();
    assert!(entity.new_events().is_empty());
    commit_one(&repo, &mut entity).await.unwrap();

    // Storage unchanged
    let loaded = get_one(&repo, "e1").await.unwrap();
    assert_eq!(loaded.events().len(), 1);
}

#[tokio::test]
async fn events_grow_monotonically() {
    let repo = InMemoryRepository::new();

    for i in 0..5 {
        let mut entity = if i == 0 {
            Entity::with_id("e1")
        } else {
            get_one(&repo, "e1").await.unwrap()
        };
        entity.digest("happened", &format!("v{}", i)).unwrap();
        commit_one(&repo, &mut entity).await.unwrap();

        let loaded = get_one(&repo, "e1").await.unwrap();
        assert_eq!(loaded.events().len(), i + 1);
    }
}

// --- Optimistic Concurrency ---

#[tokio::test]
async fn concurrent_writes_detected() {
    let repo = InMemoryRepository::new();

    // Create initial state
    let mut entity = Entity::with_id("e1");
    entity.digest("initialized", &"v1").unwrap();
    commit_one(&repo, &mut entity).await.unwrap();

    // Two readers load the same version
    let mut reader1 = get_one(&repo, "e1").await.unwrap();
    let mut reader2 = get_one(&repo, "e1").await.unwrap();

    assert_eq!(reader1.committed_version(), 1);
    assert_eq!(reader2.committed_version(), 1);

    // Both modify
    reader1.digest("updated_by_r1", &"r1").unwrap();
    reader2.digest("updated_by_r2", &"r2").unwrap();

    // First commit succeeds
    commit_one(&repo, &mut reader1).await.unwrap();

    // Second commit fails with ConcurrentWrite
    let err = commit_one(&repo, &mut reader2).await.unwrap_err();
    match err {
        distributed::RepositoryError::ConcurrentWrite {
            id,
            expected,
            actual,
        } => {
            // Stream commits key by full stream identity, so the
            // conflict id is reported as "<aggregate_type>:<id>".
            assert_eq!(id, format!("{}:e1", AGGREGATE_TYPE));
            assert_eq!(expected, 1); // reader2 loaded at version 1
            assert_eq!(actual, 2); // storage now has 2 events
        }
        other => panic!("expected ConcurrentWrite, got: {:?}", other),
    }
}

#[tokio::test]
async fn partial_conflict_rolls_back_entire_commit() {
    let repo = InMemoryRepository::new();

    // Create two entities
    let mut e1 = Entity::with_id("e1");
    e1.digest("initialized", &"v1").unwrap();
    let mut e2 = Entity::with_id("e2");
    e2.digest("initialized", &"v1").unwrap();
    commit_many(&repo, &mut [&mut e1, &mut e2]).await.unwrap();

    // Load both entities at version 1
    let mut e1_a = get_one(&repo, "e1").await.unwrap();
    let mut e2_a = get_one(&repo, "e2").await.unwrap();

    // Concurrently modify e2 from another "session"
    let mut e2_b = get_one(&repo, "e2").await.unwrap();
    e2_b.digest("conflicted", &"b").unwrap();
    commit_one(&repo, &mut e2_b).await.unwrap();

    // Try to commit both e1_a and e2_a together
    // e1 would be fine, but e2 has a version conflict
    e1_a.digest("updated", &"a").unwrap();
    e2_a.digest("updated", &"a").unwrap();
    let err = commit_many(&repo, &mut [&mut e1_a, &mut e2_a])
        .await
        .unwrap_err();
    match err {
        distributed::RepositoryError::ConcurrentWrite { id, .. } => {
            // Stream commits report the conflicting stream by full identity.
            assert_eq!(id, format!("{}:e2", AGGREGATE_TYPE));
        }
        other => panic!("expected ConcurrentWrite, got: {:?}", other),
    }

    // e1 should NOT have been modified (atomic rollback - phase 1 validates all before writing)
    let e1_loaded = get_one(&repo, "e1").await.unwrap();
    assert_eq!(e1_loaded.events().len(), 1);
    let e2_loaded = get_one(&repo, "e2").await.unwrap();
    assert_eq!(e2_loaded.events().len(), 2);
}

// --- Version Tracking ---

#[test]
fn new_entity_has_zero_versions() {
    let entity = Entity::with_id("e1");
    assert_eq!(entity.committed_version(), 0);
    assert_eq!(entity.snapshot_version(), 0);
    assert!(entity.new_events().is_empty());
}

#[tokio::test]
async fn load_from_history_sets_committed_version() {
    let repo = InMemoryRepository::new();

    let mut entity = Entity::with_id("e1");
    entity.digest("initialized", &"v1").unwrap();
    entity.digest("updated", &"v2").unwrap();
    commit_one(&repo, &mut entity).await.unwrap();

    let loaded = get_one(&repo, "e1").await.unwrap();
    assert_eq!(loaded.committed_version(), 2);
    assert_eq!(loaded.snapshot_version(), 0);
    assert_eq!(loaded.version(), 2);
    assert!(loaded.new_events().is_empty());
}

#[tokio::test]
async fn commit_updates_committed_version() {
    let repo = InMemoryRepository::new();

    let mut entity = Entity::with_id("e1");
    entity.digest("initialized", &"v1").unwrap();
    assert_eq!(entity.committed_version(), 0);

    commit_one(&repo, &mut entity).await.unwrap();
    assert_eq!(entity.committed_version(), 1);
    assert_eq!(entity.snapshot_version(), 0);
    assert!(entity.new_events().is_empty());

    // Add more events and commit again
    entity.digest("updated", &"v2").unwrap();
    assert_eq!(entity.new_events().len(), 1);

    commit_one(&repo, &mut entity).await.unwrap();
    assert_eq!(entity.committed_version(), 2);
    assert_eq!(entity.snapshot_version(), 0);
    assert!(entity.new_events().is_empty());
}

#[tokio::test]
async fn new_events_returns_only_uncommitted() {
    let repo = InMemoryRepository::new();

    let mut entity = Entity::with_id("e1");
    entity.digest("first_recorded", &"a").unwrap();
    entity.digest("second_recorded", &"b").unwrap();
    assert_eq!(entity.new_events().len(), 2);

    commit_one(&repo, &mut entity).await.unwrap();
    assert!(entity.new_events().is_empty());

    entity.digest("third_recorded", &"c").unwrap();
    assert_eq!(entity.new_events().len(), 1);
    assert_eq!(entity.new_events()[0].event_name, "third_recorded");
}
