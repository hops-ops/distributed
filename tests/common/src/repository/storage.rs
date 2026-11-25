//! Stream storage, read-model lowering and outbox scheduling, proved on
//! every backend. Raw-SQL fault injection goes through [`SqlFaults`], and a
//! second process over the same storage through [`Durable`].

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use distributed::read_model::ReadModelWritePlanBuilder;
use distributed::{
    Aggregate, AggregateBuilder, ClaimOutboxMessages, CommitBatch, Entity, GetStream,
    OutboxClaimRef, OutboxMessage, OutboxMessageStatus, OutboxStore, ReadModel,
    ReadModelWritePlanCommitExt, ReadModelWritePlanStore, RelationalReadModel,
    RelationalReadModelQueryStore, RepositoryError, RowKey, RowPatch, RowValue, StreamIdentity,
    StreamWrite, TransactionalCommit, Versioned,
};
use serde::{Deserialize, Serialize};

use super::scenario::unique_id;
use super::seat::Seat;
use super::{Durable, SqlFaults};
use crate::todo::Todo;

/// A read model with a JSON column, bootstrapped by the SQL backends (see
/// [`super::read_models::conformance_table_registry`]).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("conformance_counter_views")]
pub struct CounterView {
    #[id]
    pub id: String,
    pub value: i64,
    #[readmodel(jsonb)]
    pub counts: HashMap<String, i64>,
}

pub const COUNTER_VIEWS: &str = "conformance_counter_views";

fn counter_key(id: &str) -> RowKey {
    RowKey::new([("id", RowValue::String(id.into()))])
}

fn counter(id: &str, value: i64, counts: &[(&str, i64)]) -> CounterView {
    CounterView {
        id: id.into(),
        value,
        counts: counts.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
    }
}

async fn load_counter<R>(repo: &R, id: &str) -> Option<Versioned<CounterView>>
where
    R: RelationalReadModelQueryStore + Send + Sync,
{
    let request = ReadModelWritePlanBuilder::new()
        .load::<CounterView>(counter_key(id))
        .expect("load request should build");
    let graph = repo
        .load_graph(request)
        .await
        .expect("read model should load");
    graph.root.map(|root| Versioned {
        data: CounterView::from_row(root.data).expect("row should hydrate"),
        version: root.version,
    })
}

fn todo(id: &str) -> Todo {
    let mut todo = Todo::new();
    todo.initialize(id.into(), "user-1".into(), "write tests".into())
        .expect("todo should initialize");
    todo
}

fn batch<'a>(
    streams: Vec<StreamWrite<'a>>,
    outbox_messages: Vec<OutboxMessage>,
) -> CommitBatch<'a> {
    let mut batch = CommitBatch::new(streams);
    batch.outbox_messages = outbox_messages;
    batch
}

pub async fn aggregate_stream_round_trips_with_metadata<R>(repo: R)
where
    R: GetStream + TransactionalCommit + Clone + Send + Sync + 'static,
{
    let todos = repo.aggregate::<Todo>();
    let id = unique_id("todo");
    let mut created = Todo::new();
    created.entity.set_correlation_id("corr-1");
    created
        .initialize(id.clone(), "user-1".into(), "write tests".into())
        .unwrap();
    created.complete().expect("todo should complete");
    todos
        .commit(&mut created)
        .await
        .expect("todo should commit");

    let loaded = todos.get(&id).await.unwrap().expect("todo should reload");
    assert!(loaded.completed);
    assert_eq!(loaded.task, "write tests");
    let events = loaded.entity.events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].sequence, 1);
    assert_eq!(events[1].sequence, 2);
    assert_eq!(events[0].correlation_id(), Some("corr-1"));
}

pub async fn stream_identity_separates_same_id_across_types<R>(repo: R)
where
    R: GetStream + TransactionalCommit + Clone + Send + Sync + 'static,
{
    let id = unique_id("shared-id");
    let mut created = todo(&id);
    repo.clone()
        .aggregate::<Todo>()
        .commit(&mut created)
        .await
        .unwrap();
    let mut seat = Seat::default();
    seat.add(id.clone(), "floor".into()).unwrap();
    repo.clone()
        .aggregate::<Seat>()
        .commit(&mut seat)
        .await
        .unwrap();

    let loaded_todo = repo.clone().aggregate::<Todo>().get(&id).await.unwrap();
    let loaded_seat = repo.aggregate::<Seat>().get(&id).await.unwrap();
    assert_eq!(loaded_todo.unwrap().entity.events().len(), 1);
    let loaded_seat = loaded_seat.unwrap();
    assert_eq!(loaded_seat.category, "floor");
    assert_eq!(loaded_seat.entity.events().len(), 1);
}

/// 1000 events in one batch: more bind parameters than SQLite's historical
/// limit of 999, so a SQL backend must chunk the insert.
pub async fn commit_batch_with_1000_events_round_trips<R>(repo: R)
where
    R: GetStream + TransactionalCommit + Send + Sync,
{
    let id = unique_id("big-batch");
    let identity = StreamIdentity::new("conformance.bulk", &id).unwrap();
    let mut entity = Entity::with_id(&id);
    for i in 0..1000 {
        entity
            .digest(format!("bulk_recorded_{i}"), &(i as u64))
            .unwrap();
    }
    repo.commit_batch(CommitBatch::new(vec![StreamWrite::new(
        identity.clone(),
        &mut entity,
    )]))
    .await
    .expect("a 1000-event batch should commit");

    let loaded = repo
        .get_stream(&identity)
        .await
        .unwrap()
        .expect("stream should exist");
    assert_eq!(loaded.committed_version(), 1000);
    let sequences: Vec<u64> = loaded.events().iter().map(|event| event.sequence).collect();
    assert_eq!(sequences, (1..=1000).collect::<Vec<u64>>());
    for index in [0usize, 98, 99, 100, 998, 999] {
        let event = &loaded.events()[index];
        assert_eq!(event.event_name, format!("bulk_recorded_{index}"));
        let value: u64 = bitcode::deserialize(&event.payload).expect("payload decodes");
        assert_eq!(value, index as u64);
    }
}

/// A stale stream fails the batch; the batch's other stream and its read-model
/// plan roll back with it.
pub async fn conflict_rolls_back_other_stream_and_read_model_plan<R>(repo: R)
where
    R: GetStream
        + RelationalReadModelQueryStore
        + TransactionalCommit
        + Clone
        + Send
        + Sync
        + 'static,
{
    let seat_id = unique_id("conflict-seat");
    let seats = repo.clone().aggregate::<Seat>();
    let mut original = Seat::default();
    original.add(seat_id.clone(), "floor".into()).unwrap();
    seats.commit(&mut original).await.unwrap();

    let mut stale = seats.get(&seat_id).await.unwrap().unwrap();
    let mut winner = seats.get(&seat_id).await.unwrap().unwrap();
    stale
        .reserve(unique_id("stale"), seat_id.clone(), "floor".into())
        .unwrap();
    winner
        .reserve(unique_id("winner"), seat_id.clone(), "floor".into())
        .unwrap();
    seats.commit(&mut winner).await.unwrap();

    let other_id = unique_id("should-not-commit");
    let mut other = todo(&other_id);
    let mut read_models = ReadModelWritePlanBuilder::new();
    read_models.upsert(&counter(&other_id, 99, &[])).unwrap();

    let stale_identity = StreamIdentity::new(Seat::aggregate_type(), &seat_id).unwrap();
    let other_identity = StreamIdentity::new(Todo::aggregate_type(), &other_id).unwrap();
    let mut conflicting = batch(
        vec![
            StreamWrite::new(stale_identity, stale.entity_mut()),
            StreamWrite::new(other_identity.clone(), other.entity_mut()),
        ],
        Vec::new(),
    );
    conflicting.read_model_plans = vec![read_models.into_write_plan().unwrap()];
    let err = repo.commit_batch(conflicting).await.unwrap_err();

    assert!(
        matches!(err, RepositoryError::ConcurrentWrite { .. }),
        "{err:?}"
    );
    assert!(repo.get_stream(&other_identity).await.unwrap().is_none());
    assert!(load_counter(&repo, &other_id).await.is_none());
    assert_eq!(stale.entity().committed_version(), 1);
    assert_eq!(stale.entity().new_events().len(), 1);
}

/// A plan committed with an aggregate lowers into the registered table,
/// JSON column included.
pub async fn commit_lowers_relational_read_model_plan_into_registered_table<R>(repo: R)
where
    R: GetStream
        + RelationalReadModelQueryStore
        + TransactionalCommit
        + Clone
        + Send
        + Sync
        + 'static,
{
    let id = unique_id("relational-batch");
    let view = counter(&id, 7, &[("wins", 2)]);
    let mut read_models = ReadModelWritePlanBuilder::new();
    read_models.upsert(&view).unwrap();
    let mut created = todo(&id);

    repo.read_models(read_models)
        .commit(&mut created)
        .await
        .unwrap();

    let identity = StreamIdentity::new(Todo::aggregate_type(), &id).unwrap();
    assert!(repo.get_stream(&identity).await.unwrap().is_some());
    let loaded = load_counter(&repo, &id).await.expect("row should exist");
    assert_eq!(loaded.data, view);
    assert_eq!(loaded.version, 1);
}

pub async fn read_model_plan_patches_and_deletes_relational_rows<R>(repo: R)
where
    R: ReadModelWritePlanStore + RelationalReadModelQueryStore + Send + Sync,
{
    let id = unique_id("relational-session");
    let mut setup = ReadModelWritePlanBuilder::new();
    setup.upsert(&counter(&id, 7, &[("wins", 2)])).unwrap();
    setup.commit(&repo).await.unwrap();

    let patched_counts: HashMap<String, i64> =
        [("wins".to_string(), 3), ("losses".to_string(), 1)].into();
    let patch = RowPatch::new()
        .set("value", RowValue::I64(11))
        .set_serde("counts", &patched_counts)
        .unwrap();
    let mut patching = ReadModelWritePlanBuilder::new();
    patching
        .patch::<CounterView>(counter_key(&id), patch)
        .unwrap();
    patching.commit(&repo).await.unwrap();

    let loaded = load_counter(&repo, &id).await.expect("row should exist");
    assert_eq!(loaded.data.value, 11);
    assert_eq!(loaded.data.counts, patched_counts);
    assert_eq!(loaded.version, 2);

    let mut deleting = ReadModelWritePlanBuilder::new();
    deleting.delete::<CounterView>(counter_key(&id)).unwrap();
    deleting.commit(&repo).await.unwrap();
    assert!(load_counter(&repo, &id).await.is_none());
}

/// One commit carries an event, an outbox row and a two-row plan whose second
/// row violates a database constraint the read model does not declare. The
/// whole transaction rolls back, the already-applied first row included, and
/// the failure is permanent.
pub async fn read_model_failure_mid_plan_rolls_back_events_and_outbox<B: SqlFaults>(backend: &B)
where
    B::Repository: GetStream + RelationalReadModelQueryStore + TransactionalCommit + Send + Sync,
{
    backend.reject_negative(COUNTER_VIEWS, "value").await;
    let repo = backend.repository();
    let good_id = unique_id("midplan-good");
    let bad_id = unique_id("midplan-bad");
    let mut read_models = ReadModelWritePlanBuilder::new();
    read_models.upsert(&counter(&good_id, 1, &[])).unwrap();
    read_models.upsert(&counter(&bad_id, -1, &[])).unwrap();

    let aggregate_id = unique_id("midplan-aggregate");
    let mut created = todo(&aggregate_id);
    let identity = StreamIdentity::new(Todo::aggregate_type(), &aggregate_id).unwrap();
    let outbox_id = unique_id("midplan-outbox");
    let message = OutboxMessage::create(&outbox_id, "todo.initialized", b"{}".to_vec()).unwrap();

    let mut failing = batch(
        vec![StreamWrite::new(identity.clone(), created.entity_mut())],
        vec![message],
    );
    failing.read_model_plans = vec![read_models.into_write_plan().unwrap()];
    let err = repo
        .commit_batch(failing)
        .await
        .expect_err("a mid-plan constraint violation must fail the commit");
    assert!(
        matches!(
            &err,
            RepositoryError::Storage {
                retryable: false,
                ..
            }
        ),
        "expected a permanent Storage error, got {err:?}"
    );
    assert!(!err.is_retryable());

    assert!(repo.get_stream(&identity).await.unwrap().is_none());
    assert!(backend.outbox_message(&outbox_id).await.is_none());
    assert!(load_counter(&repo, &good_id).await.is_none());
    assert!(load_counter(&repo, &bad_id).await.is_none());
}

/// A stored event whose payload codec the backend does not support fails on
/// read with a permanent error.
pub async fn unsupported_codec_rows_fail_on_read<B: SqlFaults>(backend: &B)
where
    B::Repository: GetStream,
{
    let id = unique_id("bad-codec");
    backend
        .insert_event_with_unsupported_codec("conformance.codec", &id)
        .await;
    let identity = StreamIdentity::new("conformance.codec", &id).unwrap();
    let err = backend
        .repository()
        .get_stream(&identity)
        .await
        .unwrap_err();
    assert!(
        matches!(
            &err,
            RepositoryError::Storage {
                retryable: false,
                ..
            }
        ),
        "unexpected error: {err}"
    );
    assert!(
        err.to_string().contains("unsupported payload codec"),
        "unexpected error: {err}"
    );
}

/// An outbox row written by another writer with correlation and causation
/// columns surfaces them as message metadata.
pub async fn outbox_trace_columns_round_trip_into_message_metadata<B: SqlFaults>(backend: &B) {
    let id = unique_id("outbox-column-metadata");
    backend.insert_outbox_row_with_trace_columns(&id).await;
    let stored = backend
        .outbox_message(&id)
        .await
        .expect("the row should be listed");
    assert_eq!(stored.status, OutboxMessageStatus::Pending);
    assert_eq!(stored.correlation_id(), Some("corr-column"));
    assert_eq!(stored.causation_id(), Some("cause-column"));
}

/// Backlog stats count the pending rows and report the oldest by time, not
/// by the order rows were written (SQLite stores timestamps as text).
pub async fn outbox_backlog_stats_report_the_oldest_pending_message<R, S>(repo: R, outbox: S)
where
    R: TransactionalCommit,
    S: OutboxStore,
{
    let mut newer =
        OutboxMessage::create(unique_id("backlog-newer"), "todo.touched", b"{}".to_vec()).unwrap();
    newer.created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
    let mut older =
        OutboxMessage::create(unique_id("backlog-older"), "todo.touched", b"{}".to_vec()).unwrap();
    older.created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(2);
    repo.commit_batch(batch(Vec::new(), vec![newer, older]))
        .await
        .expect("outbox-only batch should commit");

    let stats = outbox.backlog_stats().await.expect("backlog stats");
    assert_eq!(stats.pending, 2);
    assert_eq!(
        stats.oldest_created_at,
        Some(SystemTime::UNIX_EPOCH + Duration::from_secs(2))
    );
}

const LEASE: Duration = Duration::from_secs(60);

/// Due rows are claimable, by poll and by explicit id; a row scheduled for
/// later is listed as pending with its schedule but never claimed.
pub async fn outbox_honours_deliver_at<R, S>(repo: R, outbox: S)
where
    R: TransactionalCommit,
    S: OutboxStore,
{
    let now = SystemTime::now();
    let messages = vec![
        OutboxMessage::create_to("immediate", "Ping", "q", vec![1]).unwrap(),
        OutboxMessage::create_to("past", "Ping", "q", vec![2])
            .unwrap()
            .deliver_at(now - Duration::from_secs(5)),
        OutboxMessage::create_to("future", "Ping", "q", vec![3])
            .unwrap()
            .deliver_after(now, Duration::from_secs(3600)),
    ];
    repo.commit_batch(batch(Vec::new(), messages))
        .await
        .unwrap();

    let pending = outbox.pending(10).await.unwrap();
    assert_eq!(pending.len(), 3);
    let scheduled = |id: &str| pending.iter().find(|m| m.id() == id).unwrap().deliver_at;
    assert!(scheduled("future").is_some());
    assert!(scheduled("immediate").is_none());

    let by_id = outbox
        .claim(ClaimOutboxMessages::for_ids(
            "w",
            vec!["future".into()],
            LEASE,
        ))
        .await
        .unwrap();
    assert!(
        by_id.is_empty(),
        "an explicit-id claim must not bypass the schedule"
    );

    let mut claimed: Vec<String> = outbox
        .claim(ClaimOutboxMessages::new("w", 10, LEASE).to_destination("q"))
        .await
        .unwrap()
        .iter()
        .map(|m| m.id().to_string())
        .collect();
    claimed.sort();
    assert_eq!(claimed, ["immediate", "past"]);
}

/// `at` truncated to a whole millisecond, which every backend round-trips.
fn whole_millis(at: SystemTime) -> SystemTime {
    let since = at.duration_since(SystemTime::UNIX_EPOCH).unwrap();
    SystemTime::UNIX_EPOCH + Duration::from_millis(since.as_millis() as u64)
}

/// A scheduled row outlives the process that wrote it and fires once due; a
/// row released with no delay is claimable again at once.
pub async fn scheduled_message_survives_restart_and_fires_when_due<B: Durable>(backend: &B)
where
    B::Repository: TransactionalCommit,
{
    // Long enough that the reopen below is done before the row is due on a
    // loaded machine; an early claim is still told apart from a late one.
    let delay = Duration::from_secs(2);
    let timer_id = unique_id("timer");
    let timer = OutboxMessage::create_to(&timer_id, "Timeout", "saga", b"t".to_vec())
        .unwrap()
        .deliver_after(whole_millis(SystemTime::now()), delay);
    let due_at = timer.deliver_at.unwrap();
    backend
        .repository()
        .commit_batch(batch(Vec::new(), vec![timer]))
        .await
        .unwrap();

    let reopened = backend.reopen().await;
    let outbox = B::outbox_store(&reopened);
    let pending = outbox.pending(10).await.unwrap();
    assert_eq!(pending.len(), 1);
    // Exact: every backend stores at least microsecond resolution, and the
    // schedule above is a whole millisecond.
    assert_eq!(pending[0].deliver_at, Some(due_at), "schedule persisted");
    let early = outbox
        .claim(ClaimOutboxMessages::new("w", 10, LEASE))
        .await
        .unwrap();
    let fired = if early.is_empty() {
        // The one unavoidable wait: the claim compares against wall time.
        let remaining = due_at.duration_since(SystemTime::now()).unwrap_or_default();
        tokio::time::sleep(remaining + Duration::from_millis(50)).await;
        outbox
            .claim(ClaimOutboxMessages::new("w", 10, LEASE))
            .await
            .unwrap()
    } else {
        assert!(
            SystemTime::now() >= due_at,
            "claimed before it was due: {early:?}"
        );
        early
    };
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].payload, b"t");
    outbox
        .complete(&OutboxClaimRef::from_message(&fired[0]).unwrap())
        .await
        .unwrap();

    let retry = OutboxMessage::create_to(unique_id("timer"), "Timeout", "saga", vec![])
        .unwrap()
        .deliver_at(SystemTime::now() - Duration::from_millis(1));
    reopened
        .commit_batch(batch(Vec::new(), vec![retry]))
        .await
        .unwrap();
    let claimed = outbox
        .claim(ClaimOutboxMessages::new("w", 10, LEASE))
        .await
        .unwrap();
    let claim = OutboxClaimRef::from_message(&claimed[0]).unwrap();
    outbox
        .release(&claim, "transient", Duration::ZERO)
        .await
        .unwrap();
    let again = outbox
        .claim(ClaimOutboxMessages::new("w", 10, LEASE))
        .await
        .unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].attempts, 2);
}
