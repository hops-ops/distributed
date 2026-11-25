mod aggregate;

use aggregate::{TodoV1, TodoV2, TodoV3};
use distributed::{
    hydrate, hydrate_from_snapshot, upcast_events, Aggregate, AggregateBuilder, Entity,
    EventRecord, EventUpcaster, InMemoryRepository, RepositoryError, SnapshotRecord, SnapshotStore,
    StreamIdentity, TransactionalCommit, UpcastError,
};

fn identity_payload(event: &EventRecord) -> Result<Vec<u8>, UpcastError> {
    Ok(event.payload.clone())
}

#[derive(Debug, Default)]
struct SameVersionUpcasterAggregate {
    entity: Entity,
}

impl Aggregate for SameVersionUpcasterAggregate {
    type ReplayError = String;

    fn entity(&self) -> &Entity {
        &self.entity
    }

    fn entity_mut(&mut self) -> &mut Entity {
        &mut self.entity
    }

    fn replay_event(&mut self, _event: &EventRecord) -> Result<(), Self::ReplayError> {
        Ok(())
    }

    fn upcasters() -> &'static [EventUpcaster] {
        static UPCASTERS: &[EventUpcaster] = &[EventUpcaster {
            event_type: "looped",
            from_version: 1,
            to_version: 1,
            transform: identity_payload,
        }];
        UPCASTERS
    }
}

// =============================================================================
// EventRecord version field
// =============================================================================

#[test]
fn event_record_defaults_to_version_1() {
    let record = EventRecord::new("tested", vec![], 1);
    assert_eq!(record.event_version, 1);
}

#[test]
fn event_record_new_versioned() {
    let record = EventRecord::new_versioned("tested", vec![], 1, 3);
    assert_eq!(record.event_version, 3);
    assert_eq!(record.event_name, "tested");
}

#[test]
fn event_version_serializes_cleanly_when_v1() {
    let record = EventRecord::new("tested", vec![], 1);
    let json = serde_json::to_string(&record).unwrap();
    // Version 1 is skipped in serialization
    assert!(!json.contains("event_version"));
}

#[test]
fn event_version_serializes_when_not_v1() {
    let record = EventRecord::new_versioned("tested", vec![], 1, 2);
    let json = serde_json::to_string(&record).unwrap();
    assert!(json.contains("\"event_version\":2"));
}

#[test]
fn v1_event_version_survives_a_round_trip_that_omits_it() {
    let record = EventRecord::new("tested", vec![1, 2, 3], 1);
    let json = serde_json::to_string(&record).unwrap();
    let deserialized: EventRecord = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.event_version, 1);
    assert_eq!(deserialized, record);
}

#[test]
fn event_version_round_trips_through_serde() {
    let record = EventRecord::new_versioned("tested", vec![1, 2, 3], 1, 5);
    let json = serde_json::to_string(&record).unwrap();
    let deserialized: EventRecord = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.event_version, 5);
    assert_eq!(deserialized.event_name, "tested");
    assert_eq!(deserialized.sequence, 1);
}

// =============================================================================
// digest version = N macro
// =============================================================================

#[test]
fn digest_v_creates_events_at_specified_version() {
    let mut todo = TodoV2::default();
    todo.initialize("t1".into(), "alice".into(), "Buy milk".into(), 3)
        .unwrap();

    let events = todo.entity.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_version, 2);
    assert_eq!(events[0].event_name, "initialized");
}

#[test]
fn digest_without_version_creates_v1_events() {
    let mut todo = TodoV1::default();
    todo.initialize("t1".into(), "alice".into(), "Buy milk".into())
        .unwrap();

    let events = todo.entity.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_version, 1);
}

// =============================================================================
// Upcaster: single v1 → v2
// =============================================================================

#[test]
fn hydrate_v2_from_v1_events() {
    // Create events using the v1 aggregate
    let mut v1 = TodoV1::default();
    v1.initialize("t1".into(), "alice".into(), "Buy milk".into())
        .unwrap();
    v1.complete().unwrap();

    // Simulate loading into the v2 aggregate by transferring the entity
    let mut entity = Entity::new();
    entity.load_from_history(v1.entity.events().to_vec());

    let v2: TodoV2 = hydrate(entity).unwrap();
    assert_eq!(v2.entity.id(), "t1");
    assert_eq!(v2.user_id, "alice");
    assert_eq!(v2.task, "Buy milk");
    assert_eq!(v2.priority, 0); // default from upcaster
    assert!(v2.completed);
}

// =============================================================================
// Chained upcasters: v1 → v2 → v3
// =============================================================================

#[test]
fn hydrate_v3_from_v1_events_chains_upcasters() {
    // Create events using v1 aggregate
    let mut v1 = TodoV1::default();
    v1.initialize("t1".into(), "bob".into(), "Walk dog".into())
        .unwrap();

    let mut entity = Entity::new();
    entity.load_from_history(v1.entity.events().to_vec());

    let v3: TodoV3 = hydrate(entity).unwrap();
    assert_eq!(v3.entity.id(), "t1");
    assert_eq!(v3.user_id, "bob");
    assert_eq!(v3.task, "Walk dog");
    assert_eq!(v3.priority, 0); // from v1→v2 upcaster
    assert_eq!(v3.due_date, ""); // from v2→v3 upcaster
    assert!(!v3.completed);
}

#[test]
fn hydrate_v3_from_v2_events_applies_single_upcaster() {
    // Create events using v2 aggregate
    let mut v2 = TodoV2::default();
    v2.initialize("t1".into(), "carol".into(), "Read book".into(), 5)
        .unwrap();

    let mut entity = Entity::new();
    entity.load_from_history(v2.entity.events().to_vec());

    let v3: TodoV3 = hydrate(entity).unwrap();
    assert_eq!(v3.entity.id(), "t1");
    assert_eq!(v3.user_id, "carol");
    assert_eq!(v3.task, "Read book");
    assert_eq!(v3.priority, 5); // preserved from v2
    assert_eq!(v3.due_date, ""); // from v2→v3 upcaster
}

#[test]
fn hydrate_v3_from_native_v3_events_no_upcasting_needed() {
    let mut v3 = TodoV3::default();
    v3.initialize(
        "t1".into(),
        "dave".into(),
        "Cook dinner".into(),
        2,
        "2025-12-31".into(),
    )
    .unwrap();

    let mut entity = Entity::new();
    entity.load_from_history(v3.entity.events().to_vec());

    let loaded: TodoV3 = hydrate(entity).unwrap();
    assert_eq!(loaded.priority, 2);
    assert_eq!(loaded.due_date, "2025-12-31");
}

// =============================================================================
// Mixed events: some need upcasting, some don't
// =============================================================================

#[test]
fn mixed_events_v1_init_and_v1_complete() {
    // Completed events are always v1 — no upcaster needed
    let mut v1 = TodoV1::default();
    v1.initialize("t1".into(), "eve".into(), "Test".into())
        .unwrap();
    v1.complete().unwrap();

    let mut entity = Entity::new();
    entity.load_from_history(v1.entity.events().to_vec());

    let v2: TodoV2 = hydrate(entity).unwrap();
    assert_eq!(v2.priority, 0);
    assert!(v2.completed);
}

// =============================================================================
// Repository round-trip with upcasting
// =============================================================================

#[tokio::test]
async fn repo_roundtrip_v1_to_v2() {
    // Store using v1
    let base_repo = InMemoryRepository::new();
    let mut v1 = TodoV1::default();
    v1.initialize("t1".into(), "frank".into(), "Shop".into())
        .unwrap();
    base_repo
        .clone()
        .aggregate::<TodoV1>()
        .commit(&mut v1)
        .await
        .unwrap();

    // Load using v2 (same storage)
    let v2_repo = base_repo.aggregate::<TodoV2>();
    let loaded = v2_repo.get("t1").await.unwrap().unwrap();
    assert_eq!(loaded.user_id, "frank");
    assert_eq!(loaded.task, "Shop");
    assert_eq!(loaded.priority, 0);
}

// =============================================================================
// upcast_events standalone function
// =============================================================================

#[test]
fn upcast_events_standalone() {
    let payload_v1 =
        bitcode::serialize(&("id1".to_string(), "user1".to_string(), "task1".to_string())).unwrap();
    let event = EventRecord::new("initialized", payload_v1, 1);

    let result = upcast_events(vec![event], TodoV2::upcasters()).unwrap();
    assert_eq!(result[0].event_version, 2);

    let (id, user, task, priority): (String, String, String, u8) =
        bitcode::deserialize(&result[0].payload).unwrap();
    assert_eq!(id, "id1");
    assert_eq!(user, "user1");
    assert_eq!(task, "task1");
    assert_eq!(priority, 0);
}

#[test]
fn hydrate_rejects_invalid_same_version_upcaster() {
    let mut entity = Entity::new();
    entity.load_from_history(vec![EventRecord::new("looped", vec![], 1)]);

    let err = hydrate::<SameVersionUpcasterAggregate>(entity).unwrap_err();

    match err {
        RepositoryError::Replay(message) => {
            assert!(message.contains("does not advance version 1"));
        }
        other => panic!("expected replay error, got {other:?}"),
    }
}

#[test]
fn hydrate_returns_replay_error_when_typed_upcaster_decode_fails() {
    let mut entity = Entity::new();
    entity.load_from_history(vec![EventRecord::new("initialized", vec![0xff], 1)]);

    match hydrate::<TodoV2>(entity) {
        Err(RepositoryError::Replay(message)) => {
            assert!(message.contains("failed to upcast event initialized"));
        }
        Err(other) => panic!("expected replay error, got {other:?}"),
        Ok(_) => panic!("expected replay error"),
    }
}

// =============================================================================
// Snapshot + upcasting
// =============================================================================

#[tokio::test]
async fn snapshot_plus_upcasting_post_snapshot_events() {
    // TodoV2 implements Snapshottable in aggregate.rs
    let repo = InMemoryRepository::new()
        .aggregate::<TodoV2>()
        .with_snapshots(1);

    // Create using native v2 with a specific priority
    let mut todo = TodoV2::default();
    todo.initialize("t1".into(), "grace".into(), "Run".into(), 7)
        .unwrap();
    repo.commit(&mut todo).await.unwrap();

    // Snapshot should now exist at version 1
    let snapshot_identity = StreamIdentity::new(TodoV2::aggregate_type(), "t1").unwrap();
    assert!(repo
        .repo()
        .get_snapshot(&snapshot_identity)
        .await
        .unwrap()
        .is_some());

    // Add another event; this triggers snapshot + partial replay path
    let mut todo = repo.get("t1").await.unwrap().unwrap();
    todo.complete().unwrap();
    repo.commit(&mut todo).await.unwrap();

    let loaded = repo.get("t1").await.unwrap().unwrap();
    assert_eq!(loaded.priority, 7);
    assert!(loaded.completed);
}

#[tokio::test]
async fn snapshot_repo_with_v1_events_upcasted_on_hydrate() {
    // Store v1 events, then load with v2 snapshot repo
    let base_repo = InMemoryRepository::new();

    // Create a v1 todo
    let mut v1 = TodoV1::default();
    v1.initialize("t1".into(), "hank".into(), "Sweep".into())
        .unwrap();
    v1.complete().unwrap();
    base_repo
        .clone()
        .aggregate::<TodoV1>()
        .commit(&mut v1)
        .await
        .unwrap();

    // Load via a v2 snapshot-aware repo (no snapshot exists, so full replay with upcasting)
    let repo = base_repo.aggregate::<TodoV2>().with_snapshots(5);

    let loaded = repo.get("t1").await.unwrap().unwrap();
    assert_eq!(loaded.user_id, "hank");
    assert_eq!(loaded.task, "Sweep");
    assert_eq!(loaded.priority, 0); // upcasted default
    assert!(loaded.completed);
}

// =============================================================================
// Unknown event types fail hydration with a clear, named error
// =============================================================================

#[test]
fn unknown_event_type_in_stream_fails_hydration_with_clear_error() {
    // Simulate a stream that contains an event whose name no version of the
    // aggregate knows how to replay — e.g. a producer wrote an event type that
    // was later renamed/removed, or a stream was mis-keyed under the wrong
    // aggregate type. Hydration must fail loudly (not silently skip) with a
    // Replay error that names the offending event so an operator can diagnose it.
    let unknown_name = "todo.relabelled_in_a_future_we_never_shipped";

    let mut entity = Entity::with_id("t1");
    // A valid v1 initialized event so the rest of the stream is well-formed...
    let init_payload = bitcode::serialize(&(
        "t1".to_string(),
        "alice".to_string(),
        "Buy milk".to_string(),
    ))
    .unwrap();
    let mut events = vec![EventRecord::new("initialized", init_payload, 1)];
    // ...followed by an event the aggregate has no handler for.
    let mut unknown = EventRecord::new(unknown_name, vec![], 2);
    unknown.sequence = 2;
    events.push(unknown);
    entity.load_from_history(events);

    match hydrate::<TodoV1>(entity) {
        Err(RepositoryError::Replay(message)) => {
            assert!(
                message.contains(unknown_name),
                "replay error must name the unknown event, got: {message}"
            );
        }
        Err(other) => panic!("expected a Replay error, got {other:?}"),
        Ok(_) => panic!("hydration must not silently accept an unknown event type"),
    }
}

#[tokio::test]
async fn unknown_event_type_in_repo_stream_fails_get_with_named_replay_error() {
    // The same guarantee through the repository load path: a raw stream carrying
    // an unregistered event name must make `get` fail with a named Replay error,
    // not panic and not return a half-built aggregate.
    let unknown_name = "todo.never_registered_event";

    let repo = InMemoryRepository::new();
    let identity = StreamIdentity::new(TodoV1::aggregate_type(), "t-unknown").unwrap();
    // Build a fresh stream containing a valid v1 event followed by an event with
    // an unregistered name, and persist it in one append (both events are new).
    let mut raw = Entity::with_id("t-unknown");
    raw.digest(
        "initialized",
        &(
            "t-unknown".to_string(),
            "bob".to_string(),
            "Walk dog".to_string(),
        ),
    )
    .unwrap();
    raw.digest_empty(unknown_name).unwrap();
    repo.commit_batch(distributed::CommitBatch::new(vec![
        distributed::StreamWrite::new(identity, &mut raw),
    ]))
    .await
    .expect("raw stream with an unknown event name still persists");

    match repo.aggregate::<TodoV1>().get("t-unknown").await {
        Err(RepositoryError::Replay(message)) => {
            assert!(
                message.contains(unknown_name),
                "replay error must name the unknown event, got: {message}"
            );
        }
        Err(other) => panic!("expected a Replay error, got {other:?}"),
        Ok(_) => panic!("loading a stream with an unknown event type must fail"),
    }
}

#[test]
fn hydrate_from_snapshot_returns_replay_error_when_post_snapshot_upcaster_decode_fails() {
    let snapshot = SnapshotRecord::new(
        TodoV2::aggregate_type(),
        "t1",
        1,
        1,
        bitcode::serialize(&aggregate::TodoV2Snapshot {
            id: "t1".to_string(),
            user_id: "iris".to_string(),
            task: "Plan".to_string(),
            priority: 1,
            completed: false,
        })
        .unwrap(),
    );
    let mut invalid_event = EventRecord::new("initialized", vec![0xff], 2);
    invalid_event.sequence = 2;

    let mut entity = Entity::with_id("t1");
    entity.load_from_history(vec![invalid_event]);

    match hydrate_from_snapshot::<TodoV2>(entity, snapshot) {
        Err(RepositoryError::Replay(message)) => {
            assert!(message.contains("failed to upcast event initialized"));
        }
        Err(other) => panic!("expected replay error, got {other:?}"),
        Ok(_) => panic!("expected replay error"),
    }
}
