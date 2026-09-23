use distributed::{
    hydrate, hydrate_from_snapshot, Aggregate, AggregateRepository, CommitBatch, Entity,
    EventRecord, RepositoryError, SnapshotRecord, SnapshotStore, Snapshottable, StreamIdentity,
    StreamWrite, TransactionalCommit,
};
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize, distributed::Snapshot)]
struct Versioned {
    entity: Entity,
    value: String,
    calls: usize,
}

#[distributed::sourced(entity)]
impl Versioned {
    #[event("renamed", version = 2)]
    fn rename(&mut self, value: String) {
        self.value = value;
        self.calls += 1;
    }

    #[event("cleared", version = 2)]
    fn clear(&mut self) {
        self.value.clear();
        self.calls += 1;
    }
}

#[derive(Default, Serialize, Deserialize, distributed::Snapshot)]
struct Registered {
    entity: Entity,
    value: String,
}

impl Registered {
    #[distributed::digest("renamed", version = 2)]
    fn rename(&mut self, value: String) {
        self.value = value;
    }

    #[distributed::digest("cleared")]
    fn clear(&mut self) {
        self.value.clear();
    }
}

distributed::aggregate!(Registered, entity {
    "renamed"(value), version = 2 => rename,
    "cleared"() => clear(),
    "ignored"(payload), version = 2 => clear(),
});

#[test]
fn sourced_rejects_same_shaped_older_and_future_events_before_mutation() {
    for version in [0, 1, 3, u64::MAX] {
        for (name, payload) in [
            (
                "renamed",
                bitcode::serialize(&("changed".to_owned(),)).unwrap(),
            ),
            ("cleared", vec![]),
        ] {
            let mut aggregate = Versioned::default();
            aggregate.rename("unchanged".into()).unwrap();
            let before = serde_json::to_value(&aggregate.entity).unwrap();
            let event = EventRecord::new_versioned(name, payload, 2, version);
            assert!(
                aggregate.replay_event(&event).is_err(),
                "accepted {name} v{version}"
            );
            assert_eq!(aggregate.value, "unchanged");
            assert_eq!(aggregate.calls, 1);
            assert_eq!(serde_json::to_value(&aggregate.entity).unwrap(), before);
        }
    }
}

#[test]
fn aggregate_macro_rejects_same_shaped_versions_before_mutation() {
    for (name, expected) in [("renamed", 2), ("cleared", 1), ("ignored", 2)] {
        for version in [expected - 1, expected + 1, u64::MAX] {
            let mut aggregate = Registered::default();
            aggregate.rename("unchanged".into()).unwrap();
            let before = serde_json::to_value(&aggregate.entity).unwrap();
            let event = EventRecord::new_versioned(
                name,
                bitcode::serialize(&("changed".to_owned(),)).unwrap(),
                2,
                version,
            );
            assert!(aggregate.replay_event(&event).is_err());
            assert_eq!(aggregate.value, "unchanged");
            assert_eq!(serde_json::to_value(&aggregate.entity).unwrap(), before);
        }
    }
}

fn rename_event(version: u64, sequence: u64) -> EventRecord {
    EventRecord::new_versioned(
        "renamed",
        bitcode::serialize(&("changed".to_owned(),)).unwrap(),
        sequence,
        version,
    )
}

#[test]
fn exact_version_replays_with_both_macros() {
    let mut entity = Entity::new();
    entity.load_from_history(vec![rename_event(2, 1)]);
    let sourced = hydrate::<Versioned>(entity.clone()).unwrap();
    let registered = hydrate::<Registered>(entity).unwrap();
    assert_eq!(sourced.value, "changed");
    assert_eq!(sourced.calls, 1);
    assert_eq!(registered.value, "changed");
    assert_eq!(sourced.entity.version(), 1);
    assert!(sourced.entity.new_events().is_empty());
}

fn snapshot<A: Snapshottable>() -> SnapshotRecord {
    let mut aggregate = A::new_empty();
    aggregate.entity_mut().set_id("item");
    SnapshotRecord::new(
        A::aggregate_type(),
        "item",
        10,
        A::SNAPSHOT_VERSION,
        bitcode::serialize(&aggregate.create_snapshot()).unwrap(),
    )
}

fn check_snapshot_tail<A: Snapshottable>() {
    for version in [1, 2, 3] {
        // Production cell and native repositories load only the suffix after a
        // cached snapshot; its prefix still counts toward the next write fence.
        let mut entity = Entity::new();
        entity.set_id("item");
        let event = rename_event(version, 11);
        entity.load_tail_from_history(vec![event.clone()], 10);
        let result = hydrate_from_snapshot::<A>(entity, snapshot::<A>());
        if version == 2 {
            let aggregate = result.unwrap();
            assert_eq!(aggregate.entity().version(), 11);
            assert_eq!(aggregate.entity().events(), &[event]);
            assert!(aggregate.entity().new_events().is_empty());
        } else {
            assert!(matches!(result, Err(RepositoryError::Replay(message))
                if message == format!("Unsupported event version for renamed: expected 2, got {version}")));
        }
    }
}

#[test]
fn snapshot_prefix_tail_checks_versions_for_both_macros() {
    check_snapshot_tail::<Versioned>();
    check_snapshot_tail::<Registered>();
}

#[derive(Default, Serialize, Deserialize, distributed::Snapshot)]
struct Upcasted {
    entity: Entity,
    value: String,
}

#[derive(Default)]
struct RegisteredUpcast {
    entity: Entity,
    value: String,
}

impl RegisteredUpcast {
    #[distributed::digest("renamed", version = 2)]
    fn rename(&mut self, value: String) {
        self.value = value;
    }
}

distributed::aggregate!(RegisteredUpcast, entity {
    "renamed"(value), version = 2 => rename,
} upcasters [
    ("renamed", 1 => 2, (String,) => (String,), convert_semantics),
]);

#[derive(Default)]
struct IncompleteUpcast {
    entity: Entity,
}

#[distributed::sourced(entity, upcasters(
    ("renamed", 1 => 2, (String,) => (String,), convert_semantics),
))]
impl IncompleteUpcast {
    #[event("renamed", version = 3)]
    fn rename(&mut self, _value: String) {
        self.entity.set_id("incompatible-event-applied");
    }
}

#[test]
fn incomplete_same_shaped_upcast_chain_is_rejected() {
    let mut entity = Entity::new();
    entity.load_from_history(vec![rename_event(1, 1)]);
    assert!(
        matches!(hydrate::<IncompleteUpcast>(entity), Err(RepositoryError::Replay(message))
        if message == "Unsupported event version for renamed: expected 3, got 2")
    );
}

fn convert_semantics((value,): (String,)) -> (String,) {
    (format!("v2:{value}"),)
}

#[distributed::sourced(entity, upcasters(
    ("renamed", 1 => 2, (String,) => (String,), convert_semantics),
))]
impl Upcasted {
    #[event("renamed", version = 2)]
    fn rename(&mut self, value: String) {
        self.value = value;
    }
}

#[test]
fn same_shaped_upcast_precedes_guard_and_preserves_stored_history() {
    let event = rename_event(1, 1);
    let mut entity = Entity::new();
    entity.load_from_history(vec![event.clone()]);
    let aggregate = hydrate::<Upcasted>(entity).unwrap();
    assert_eq!(aggregate.value, "v2:changed");
    assert_eq!(aggregate.entity.events(), &[event]);

    let mut entity = Entity::new();
    let event = rename_event(1, 1);
    entity.load_from_history(vec![event.clone()]);
    let aggregate = hydrate::<RegisteredUpcast>(entity).unwrap();
    assert_eq!(aggregate.value, "v2:changed");
    assert_eq!(aggregate.entity.events(), &[event]);

    let tail = rename_event(1, 11);
    let mut entity = Entity::new();
    entity.set_id("item");
    entity.load_tail_from_history(vec![tail.clone()], 10);
    let aggregate = hydrate_from_snapshot::<Upcasted>(entity, snapshot::<Upcasted>()).unwrap();
    assert_eq!(aggregate.value, "v2:changed");
    assert_eq!(aggregate.entity.version(), 11);
    assert_eq!(aggregate.entity.events(), &[tail]);
}

#[tokio::test]
async fn cell_repository_rejects_incompatible_tail_without_changing_storage() {
    use distributed::cell_host::CellStreamStore;

    for version in [1, 2, 3] {
        let store = CellStreamStore::new(Versioned::aggregate_type(), "item").unwrap();
        let identity = StreamIdentity::new(Versioned::aggregate_type(), "item").unwrap();
        let mut entity = Entity::new();
        entity.set_id("item");
        for _ in 0..10 {
            entity
                .digest_v("renamed", 2, &("prefix".to_owned(),))
                .unwrap();
        }
        entity
            .digest_v("renamed", version, &("changed".to_owned(),))
            .unwrap();
        store
            .commit_batch(CommitBatch::new(vec![StreamWrite::new(
                identity.clone(),
                &mut entity,
            )]))
            .await
            .unwrap();
        store
            .save_snapshot(&identity, snapshot::<Versioned>())
            .await
            .unwrap();
        let before = store.durable_state().unwrap();
        let repository = AggregateRepository::<_, Versioned>::new(store.clone()).with_snapshots(10);
        let result = repository.get("item").await;
        if version == 2 {
            let aggregate = result.unwrap().unwrap();
            assert_eq!(aggregate.value, "changed");
            assert_eq!(aggregate.calls, 1, "snapshot prefix must not replay");
            assert_eq!(aggregate.entity.version(), 11);
        } else {
            assert!(matches!(result, Err(RepositoryError::Replay(_))));
        }
        assert_eq!(store.durable_state().unwrap(), before);
    }
}

#[test]
fn full_hydration_rejects_same_shaped_unsupported_versions() {
    for version in [1, 3] {
        let mut entity = Entity::new();
        entity.load_from_history(vec![EventRecord::new_versioned(
            "renamed",
            bitcode::serialize(&("changed".to_owned(),)).unwrap(),
            1,
            version,
        )]);
        assert!(hydrate::<Versioned>(entity).is_err());
    }
}
