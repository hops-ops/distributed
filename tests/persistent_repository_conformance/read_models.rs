use distributed::read_model::ReadModelWritePlanBuilder;
use distributed::{
    Aggregate, AggregateBuilder, CommitBatch, GetStream, ReadModel, ReadModelWritePlanCommitExt,
    ReadModelWritePlanStore, RelationalReadModel, RelationalReadModelQueryStore, RepositoryError,
    RowKey, RowValue, StreamIdentity, StreamWrite, TableSchemaRegistry, TransactionalCommit,
    Versioned,
};
use serde::{Deserialize, Serialize};

use super::scenario::unique_id;
use super::seat::Seat;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("conformance_seat_views")]
struct SeatView {
    #[id]
    id: String,
    status: String,
}

/// The table schemas backing these scenarios, so SQL backends can bootstrap
/// the `conformance_seat_views` table in their `repository()` factory.
/// (Unused by the in-memory target, which needs no schema.)
#[allow(dead_code)]
pub fn conformance_table_registry() -> TableSchemaRegistry {
    let mut registry = TableSchemaRegistry::new();
    registry
        .register::<SeatView>()
        .expect("SeatView schema should register");
    registry
}

fn seat_view_key(id: &str) -> RowKey {
    RowKey::new([("id", RowValue::String(id.into()))])
}

async fn load_seat_view<R>(repo: &R, id: &str) -> Option<Versioned<SeatView>>
where
    R: RelationalReadModelQueryStore + Send + Sync,
{
    let request = ReadModelWritePlanBuilder::new()
        .load::<SeatView>(seat_view_key(id))
        .expect("load request should build");
    let graph = repo
        .load_graph(request)
        .await
        .expect("read model should load");
    graph.root.map(|root| Versioned {
        data: SeatView::from_row(root.data).expect("row should hydrate"),
        version: root.version,
    })
}

pub async fn standalone_relational_write_plan_persists_row<R>(repo: R)
where
    R: ReadModelWritePlanStore + RelationalReadModelQueryStore + Send + Sync,
{
    let view = SeatView {
        id: unique_id("read-model-view"),
        status: "available".into(),
    };
    let mut read_models = ReadModelWritePlanBuilder::new();
    read_models
        .upsert(&view)
        .expect("row mutation should serialize");

    let outcome = read_models
        .commit(&repo)
        .await
        .expect("read model write plan should commit");
    assert!(outcome.was_applied());

    let loaded = load_seat_view(&repo, &view.id)
        .await
        .expect("read model row should exist");
    assert_eq!(loaded.version, 1);
    assert_eq!(loaded.data, view);
}

pub async fn aggregate_commit_persists_read_model_plan<R>(repo: R)
where
    R: GetStream
        + RelationalReadModelQueryStore
        + TransactionalCommit
        + Clone
        + Send
        + Sync
        + 'static,
{
    let seat_id = unique_id("read-model-seat");
    let mut seat = Seat::default();
    seat.add(seat_id.clone(), "balcony".into())
        .expect("seat should be valid");

    let view = SeatView {
        id: seat_id.clone(),
        status: "available".into(),
    };
    let mut read_models = ReadModelWritePlanBuilder::new();
    read_models
        .upsert(&view)
        .expect("row mutation should serialize");

    repo.read_models(read_models)
        .commit(&mut seat)
        .await
        .expect("aggregate and read model should commit");

    let identity =
        StreamIdentity::new(Seat::aggregate_type(), &seat_id).expect("identity should be valid");
    assert!(repo
        .get_stream(&identity)
        .await
        .expect("stream should reload")
        .is_some());
    let loaded = load_seat_view(&repo, &seat_id)
        .await
        .expect("read model should exist");
    assert_eq!(loaded.data, view);
}

pub async fn aggregate_conflict_rolls_back_read_model_plan<R>(repo: R)
where
    R: GetStream
        + RelationalReadModelQueryStore
        + TransactionalCommit
        + Clone
        + Send
        + Sync
        + 'static,
{
    let seat_id = unique_id("read-model-conflict-seat");
    let seat_repo = repo.clone().aggregate::<Seat>();
    let mut original = Seat::default();
    original
        .add(seat_id.clone(), "floor".into())
        .expect("seat should be valid");
    seat_repo
        .commit(&mut original)
        .await
        .expect("initial seat should commit");

    let mut stale = seat_repo
        .get(&seat_id)
        .await
        .expect("stale seat should load")
        .expect("stale seat should exist");
    let mut winner = seat_repo
        .get(&seat_id)
        .await
        .expect("winner seat should load")
        .expect("winner seat should exist");
    stale
        .reserve(unique_id("stale-checkout"), seat_id.clone(), "floor".into())
        .expect("stale reservation should be locally valid");
    winner
        .reserve(
            unique_id("winner-checkout"),
            seat_id.clone(),
            "floor".into(),
        )
        .expect("winner reservation should be locally valid");
    seat_repo
        .commit(&mut winner)
        .await
        .expect("winner should commit");

    let view_id = unique_id("rollback-read-model");
    let mut read_models = ReadModelWritePlanBuilder::new();
    read_models
        .upsert(&SeatView {
            id: view_id.clone(),
            status: "should-not-commit".into(),
        })
        .expect("row mutation should serialize");

    let identity =
        StreamIdentity::new(Seat::aggregate_type(), &seat_id).expect("identity should be valid");
    let err = repo
        .commit_batch(CommitBatch {
            inbox_receipts: Vec::new(),
            alarms: Vec::new(),
            streams: vec![StreamWrite::new(identity, stale.entity_mut())],
            outbox_messages: Vec::new(),
            read_model_plans: vec![read_models
                .into_write_plan()
                .expect("write plan should validate")],
            snapshots: Vec::new(),
        })
        .await
        .expect_err("stale aggregate should reject batch");

    assert!(matches!(err, RepositoryError::ConcurrentWrite { .. }));
    assert!(load_seat_view(&repo, &view_id).await.is_none());
}
