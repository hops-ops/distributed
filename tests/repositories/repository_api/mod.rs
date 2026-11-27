use distributed::{
    sourced, Aggregate, AggregateBuilder, Entity, GetStream, InMemoryRepository,
    InMemorySnapshotStore, SnapshotRecord, SnapshotStore, Snapshottable, StreamIdentity,
};

#[derive(Default)]
struct SnapshotCounter {
    entity: Entity,
    value: i32,
}

#[sourced(entity, aggregate_type = "async.snapshot_counter")]
impl SnapshotCounter {
    #[event("incremented")]
    fn increment(&mut self, id: String, by: i32) {
        self.entity.set_id(&id);
        self.value += by;
    }
}

impl Snapshottable for SnapshotCounter {
    type Snapshot = i32;

    fn create_snapshot(&self) -> Self::Snapshot {
        self.value
    }

    fn restore_from_snapshot(&mut self, snapshot: Self::Snapshot) {
        self.value = snapshot;
    }
}

#[tokio::test]
async fn snapshot_store_uses_full_stream_identity() {
    let store = InMemorySnapshotStore::new();
    let alpha = StreamIdentity::new("async.alpha", "same-id").unwrap();
    let beta = StreamIdentity::new("async.beta", "same-id").unwrap();

    store
        .save_snapshot(
            &alpha,
            SnapshotRecord::new("async.alpha", "same-id", 1, 1, vec![1]),
        )
        .await
        .unwrap();
    store
        .save_snapshot(
            &beta,
            SnapshotRecord::new("async.beta", "same-id", 2, 1, vec![2]),
        )
        .await
        .unwrap();

    let loaded_alpha = store.get_snapshot(&alpha).await.unwrap().unwrap();
    let loaded_beta = store.get_snapshot(&beta).await.unwrap().unwrap();

    assert_eq!(loaded_alpha.version, 1);
    assert_eq!(loaded_beta.version, 2);
    assert_eq!(loaded_alpha.aggregate_type, "async.alpha");
    assert_eq!(loaded_beta.aggregate_type, "async.beta");
}

#[tokio::test]
async fn snapshot_repository_writes_cache_without_event_record() {
    let repo = InMemoryRepository::new();
    let snapshot_repo = repo
        .clone()
        .aggregate::<SnapshotCounter>()
        .with_snapshots(2);
    let id = "snapshot-counter-1";

    let mut counter = SnapshotCounter::default();
    counter.increment(id.into(), 2).unwrap();
    snapshot_repo.commit(&mut counter).await.unwrap();
    counter.increment(id.into(), 3).unwrap();
    snapshot_repo.commit(&mut counter).await.unwrap();

    let identity = StreamIdentity::new(SnapshotCounter::aggregate_type(), id).unwrap();
    let stream = repo.get_stream(&identity).await.unwrap().unwrap();
    let snapshot = repo.get_snapshot(&identity).await.unwrap().unwrap();

    assert_eq!(stream.events().len(), 2);
    assert_eq!(stream.events()[0].event_name, "incremented");
    assert_eq!(stream.events()[1].event_name, "incremented");
    assert_eq!(snapshot.version, 2);
    assert_eq!(snapshot.aggregate_type, SnapshotCounter::aggregate_type());
    assert_eq!(snapshot.payload, bitcode::serialize(&5_i32).unwrap());
}

#[tokio::test]
async fn snapshot_repository_ignores_invalid_cache_and_replays_events() {
    let repo = InMemoryRepository::new();
    let aggregate_repo = repo.clone().aggregate::<SnapshotCounter>();
    let snapshot_repo = repo
        .clone()
        .aggregate::<SnapshotCounter>()
        .with_snapshots(10);
    let id = "snapshot-counter-invalid";

    let mut counter = SnapshotCounter::default();
    counter.increment(id.into(), 4).unwrap();
    counter.increment(id.into(), 6).unwrap();
    aggregate_repo.commit(&mut counter).await.unwrap();

    let identity = StreamIdentity::new(SnapshotCounter::aggregate_type(), id).unwrap();
    let mut invalid = SnapshotRecord::new(SnapshotCounter::aggregate_type(), id, 1, 1, vec![0xff]);
    invalid.payload_codec = "json".into();
    repo.save_snapshot(&identity, invalid).await.unwrap();

    let loaded = snapshot_repo.get(id).await.unwrap().unwrap();
    assert_eq!(loaded.value, 10);
    assert_eq!(loaded.entity().snapshot_version(), 0);
}

#[tokio::test]
async fn snapshot_repository_ignores_cache_past_stream_version_and_replays_events() {
    let repo = InMemoryRepository::new();
    let aggregate_repo = repo.clone().aggregate::<SnapshotCounter>();
    let snapshot_repo = repo
        .clone()
        .aggregate::<SnapshotCounter>()
        .with_snapshots(10);
    let id = "snapshot-counter-ahead";

    let mut counter = SnapshotCounter::default();
    counter.increment(id.into(), 4).unwrap();
    aggregate_repo.commit(&mut counter).await.unwrap();

    let identity = StreamIdentity::new(SnapshotCounter::aggregate_type(), id).unwrap();
    let record = SnapshotRecord::new(
        SnapshotCounter::aggregate_type(),
        id,
        2,
        1,
        bitcode::serialize(&999_i32).unwrap(),
    );
    repo.save_snapshot(&identity, record).await.unwrap();

    let loaded = snapshot_repo.get(id).await.unwrap().unwrap();
    assert_eq!(loaded.value, 4);
    assert_eq!(loaded.entity().version(), 1);
    assert_eq!(loaded.entity().snapshot_version(), 0);
}
