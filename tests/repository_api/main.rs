use std::time::Duration;

use distributed::read_model::ReadModelWritePlanBuilder;
use distributed::{
    sourced, Aggregate, AggregateBuilder, ClaimOutboxMessages, CommitBatch, Entity, GetStream,
    InMemoryRepository, InMemorySnapshotStore, OutboxMessage, OutboxStore, ReadModel,
    RelationalReadModel, RelationalReadModelQueryStore, RepositoryError, RowKey, RowValue,
    SnapshotRecord, SnapshotStore, Snapshottable, StreamIdentity, StreamWrite, TransactionalCommit,
    Versioned,
};
use serde::{Deserialize, Serialize};

#[derive(Default)]
struct AlphaAggregate {
    entity: Entity,
}

#[sourced(entity, aggregate_type = "async.alpha")]
impl AlphaAggregate {
    #[event("touched")]
    fn touch(&mut self, id: String) {
        self.entity.set_id(&id);
    }
}

#[derive(Default)]
struct BetaAggregate {
    entity: Entity,
}

#[sourced(entity, aggregate_type = "async.beta")]
impl BetaAggregate {
    #[event("touched")]
    fn touch(&mut self, id: String) {
        self.entity.set_id(&id);
    }
}

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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("repository_test_views")]
struct TestView {
    #[id]
    id: String,
    value: i32,
}

fn test_view_key(id: &str) -> RowKey {
    RowKey::new([("id", RowValue::String(id.into()))])
}

async fn load_test_view(repo: &InMemoryRepository, id: &str) -> Option<Versioned<TestView>> {
    let request = ReadModelWritePlanBuilder::new()
        .load::<TestView>(test_view_key(id))
        .unwrap();
    let graph = repo.load_graph(request).await.unwrap();
    graph.root.map(|root| Versioned {
        data: TestView::from_row(root.data).unwrap(),
        version: root.version,
    })
}

#[tokio::test]
async fn aggregate_repository_separates_streams_by_aggregate_type() {
    let repo = InMemoryRepository::new();
    let alpha_repo = repo.clone().aggregate::<AlphaAggregate>();
    let beta_repo = repo.clone().aggregate::<BetaAggregate>();

    let mut alpha = AlphaAggregate::default();
    alpha.touch("shared-id".into()).unwrap();
    let mut beta = BetaAggregate::default();
    beta.touch("shared-id".into()).unwrap();

    alpha_repo.commit(&mut alpha).await.unwrap();
    beta_repo.commit(&mut beta).await.unwrap();

    let loaded_alpha = alpha_repo.get("shared-id").await.unwrap().unwrap();
    let loaded_beta = beta_repo.get("shared-id").await.unwrap().unwrap();

    assert_eq!(loaded_alpha.entity().events().len(), 1);
    assert_eq!(loaded_beta.entity().events().len(), 1);
}

#[tokio::test]
async fn batch_rejects_duplicate_stream_identity_before_write() {
    let repo = InMemoryRepository::new();
    let identity = StreamIdentity::new("async.alpha", "duplicate").unwrap();
    let mut first = Entity::with_id("duplicate");
    first.digest_empty("first_recorded").unwrap();
    let mut second = Entity::with_id("duplicate");
    second.digest_empty("second_recorded").unwrap();

    let err = repo
        .commit_batch(CommitBatch::new(vec![
            StreamWrite::new(identity.clone(), &mut first),
            StreamWrite::new(identity.clone(), &mut second),
        ]))
        .await
        .unwrap_err();

    assert!(
        matches!(
            &err,
            RepositoryError::DuplicateStreamInBatch { id } if id == "async.alpha:duplicate"
        ),
        "unexpected error: {err}"
    );
    assert!(repo.get_stream(&identity).await.unwrap().is_none());
}

#[tokio::test]
async fn read_model_write_plan_can_commit_against_store() {
    let repo = InMemoryRepository::new();
    let view = TestView {
        id: "view-1".into(),
        value: 42,
    };
    let mut session = ReadModelWritePlanBuilder::new();
    session.upsert(&view).unwrap();

    session.commit(&repo).await.unwrap();
    let loaded = load_test_view(&repo, "view-1").await.unwrap();

    assert_eq!(loaded.data, view);
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

#[tokio::test]
async fn outbox_repository_delegates_worker_operations() {
    let repo = InMemoryRepository::new();
    let outbox = repo.outbox_store();
    let message = OutboxMessage::create("msg-1", "alpha.happened", b"{}".to_vec()).unwrap();
    let mut aggregate = AlphaAggregate::default();
    aggregate.touch("outbox-aggregate-1".into()).unwrap();
    repo.clone()
        .aggregate::<AlphaAggregate>()
        .outbox(message)
        .commit(&mut aggregate)
        .await
        .unwrap();

    let claimed = outbox
        .claim(ClaimOutboxMessages::new(
            "worker-1",
            1,
            Duration::from_secs(60),
        ))
        .await
        .unwrap();

    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].worker_id.as_deref(), Some("worker-1"));
}
