//! Stream storage, read-model lowering and outbox scheduling, proved on
//! every backend. Raw-SQL fault injection goes through [`SqlFaults`], and a
//! second process over the same storage through [`Durable`].

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use distributed::outbox::{OutboxMessage, OutboxMessageStatus};
use distributed::outbox_worker::{ClaimOutboxMessages, OutboxClaimRef, OutboxStore};
use distributed::read_model::ReadModelWritePlanBuilder;
use distributed::read_model::{ReadModel, RelationalReadModel, Versioned};
use distributed::table::{RowKey, RowPatch, RowValue};
use distributed::{
    AggregateBuilder, CommitBatch, Entity, GetStream, ReadModelWritePlanStore,
    RelationalReadModelQueryStore, RepositoryError, StreamIdentity, StreamWrite,
    TransactionalCommit,
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

// --- Raw stream semantics: `commit_batch` and `get_stream` on one `Entity` ---

const STREAM_TYPE: &str = "conformance.stream";

/// A fresh stream id and its identity under [`STREAM_TYPE`].
fn stream(prefix: &str) -> (String, StreamIdentity) {
    let id = unique_id(prefix);
    let identity = StreamIdentity::new(STREAM_TYPE, &id).expect("identity should be valid");
    (id, identity)
}

async fn commit_stream<R>(
    repo: &R,
    identity: &StreamIdentity,
    entity: &mut Entity,
) -> Result<(), RepositoryError>
where
    R: TransactionalCommit,
{
    repo.commit_batch(CommitBatch::new(vec![StreamWrite::new(
        identity.clone(),
        entity,
    )]))
    .await
}

async fn load_stream<R: GetStream>(repo: &R, identity: &StreamIdentity) -> Entity {
    repo.get_stream(identity)
        .await
        .expect("stream should load")
        .expect("stream should exist")
}

/// Each load-modify-commit cycle appends only its new event.
pub async fn load_modify_commit_cycles_append_to_the_stream<R>(repo: R)
where
    R: GetStream + TransactionalCommit + Send + Sync,
{
    let (id, identity) = stream("cycles");
    let mut entity = Entity::with_id(&id);
    entity.digest("initialized", &"v1").unwrap();
    commit_stream(&repo, &identity, &mut entity)
        .await
        .expect("first commit");

    let mut entity = load_stream(&repo, &identity).await;
    assert_eq!(entity.events().len(), 1);
    entity.digest("updated", &"v2").unwrap();
    commit_stream(&repo, &identity, &mut entity)
        .await
        .expect("second commit");

    let mut entity = load_stream(&repo, &identity).await;
    assert_eq!(entity.events().len(), 2);
    entity.digest("updated", &"v3").unwrap();
    commit_stream(&repo, &identity, &mut entity)
        .await
        .expect("third commit");

    let entity = load_stream(&repo, &identity).await;
    let names: Vec<&str> = entity
        .events()
        .iter()
        .map(|event| event.event_name.as_str())
        .collect();
    assert_eq!(names, ["initialized", "updated", "updated"]);
    assert_eq!(entity.version(), 3);
}

/// Committing a loaded stream with no new events succeeds and writes nothing.
pub async fn an_empty_commit_leaves_the_stream_unchanged<R>(repo: R)
where
    R: GetStream + TransactionalCommit + Send + Sync,
{
    let (id, identity) = stream("empty-commit");
    let mut entity = Entity::with_id(&id);
    entity.digest("initialized", &"v1").unwrap();
    commit_stream(&repo, &identity, &mut entity)
        .await
        .expect("first commit");

    let mut entity = load_stream(&repo, &identity).await;
    assert!(entity.new_events().is_empty());
    commit_stream(&repo, &identity, &mut entity)
        .await
        .expect("an empty commit should succeed");

    assert_eq!(load_stream(&repo, &identity).await.events().len(), 1);
}

/// A commit marks the entity's events committed, and a load restores the
/// committed version; neither touches the snapshot version.
pub async fn commit_and_load_track_the_committed_version<R>(repo: R)
where
    R: GetStream + TransactionalCommit + Send + Sync,
{
    let (id, identity) = stream("versions");
    let mut entity = Entity::with_id(&id);
    entity.digest("first_recorded", &"a").unwrap();
    entity.digest("second_recorded", &"b").unwrap();
    assert_eq!(entity.committed_version(), 0);
    assert_eq!(entity.new_events().len(), 2);

    commit_stream(&repo, &identity, &mut entity)
        .await
        .expect("first commit");
    assert_eq!(entity.committed_version(), 2);
    assert_eq!(entity.snapshot_version(), 0);
    assert!(entity.new_events().is_empty());

    entity.digest("third_recorded", &"c").unwrap();
    assert_eq!(entity.new_events().len(), 1);
    assert_eq!(entity.new_events()[0].event_name, "third_recorded");

    commit_stream(&repo, &identity, &mut entity)
        .await
        .expect("second commit");
    assert_eq!(entity.committed_version(), 3);
    assert_eq!(entity.snapshot_version(), 0);
    assert!(entity.new_events().is_empty());

    let loaded = load_stream(&repo, &identity).await;
    assert_eq!(loaded.committed_version(), 3);
    assert_eq!(loaded.snapshot_version(), 0);
    assert_eq!(loaded.version(), 3);
    assert!(loaded.new_events().is_empty());
}

/// Two loads of one version; the second commit is refused and names the
/// stream, the version it loaded and the version storage now holds. (Two
/// loads of one stream would wait on a queued repository's lock.)
pub async fn a_stale_commit_reports_the_stream_and_both_versions<R>(repo: R)
where
    R: GetStream + TransactionalCommit + Send + Sync,
{
    let (id, identity) = stream("stale");
    let mut entity = Entity::with_id(&id);
    entity.digest("initialized", &"v1").unwrap();
    commit_stream(&repo, &identity, &mut entity)
        .await
        .expect("first commit");

    let mut first = load_stream(&repo, &identity).await;
    let mut second = load_stream(&repo, &identity).await;
    assert_eq!(first.committed_version(), 1);
    assert_eq!(second.committed_version(), 1);
    first.digest("updated_by_first", &"1").unwrap();
    second.digest("updated_by_second", &"2").unwrap();

    commit_stream(&repo, &identity, &mut first)
        .await
        .expect("the first writer commits");
    let err = commit_stream(&repo, &identity, &mut second)
        .await
        .expect_err("the stale writer conflicts");
    match err {
        RepositoryError::ConcurrentWrite {
            id: conflict,
            expected,
            actual,
        } => {
            assert_eq!(conflict, format!("{STREAM_TYPE}:{id}"));
            assert_eq!(expected, 1);
            assert_eq!(actual, 2);
        }
        other => panic!("expected ConcurrentWrite, got {other:?}"),
    }
}

/// A batch whose second stream is stale fails as a whole: the first stream's
/// append is not written either.
pub async fn a_conflict_in_one_stream_rolls_back_the_whole_batch<R>(repo: R)
where
    R: GetStream + TransactionalCommit + Send + Sync,
{
    let (first_id, first) = stream("batch-first");
    let (second_id, second) = stream("batch-second");
    let mut e1 = Entity::with_id(&first_id);
    e1.digest("initialized", &"v1").unwrap();
    let mut e2 = Entity::with_id(&second_id);
    e2.digest("initialized", &"v1").unwrap();
    repo.commit_batch(CommitBatch::new(vec![
        StreamWrite::new(first.clone(), &mut e1),
        StreamWrite::new(second.clone(), &mut e2),
    ]))
    .await
    .expect("both streams commit");

    let mut e1_a = load_stream(&repo, &first).await;
    let mut e2_a = load_stream(&repo, &second).await;
    let mut e2_b = load_stream(&repo, &second).await;
    e2_b.digest("conflicted", &"b").unwrap();
    commit_stream(&repo, &second, &mut e2_b)
        .await
        .expect("the other writer commits");

    e1_a.digest("updated", &"a").unwrap();
    e2_a.digest("updated", &"a").unwrap();
    let err = repo
        .commit_batch(CommitBatch::new(vec![
            StreamWrite::new(first.clone(), &mut e1_a),
            StreamWrite::new(second.clone(), &mut e2_a),
        ]))
        .await
        .expect_err("the stale batch conflicts");
    match err {
        RepositoryError::ConcurrentWrite { id, .. } => {
            assert_eq!(id, format!("{STREAM_TYPE}:{second_id}"));
        }
        other => panic!("expected ConcurrentWrite, got {other:?}"),
    }

    assert_eq!(load_stream(&repo, &first).await.events().len(), 1);
    assert_eq!(load_stream(&repo, &second).await.events().len(), 2);
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

/// A write plan lowers into the registered table, JSON column included.
pub async fn write_plan_lowers_relational_read_model_into_registered_table<R>(repo: R)
where
    R: ReadModelWritePlanStore + RelationalReadModelQueryStore + Send + Sync,
{
    let id = unique_id("relational-batch");
    let view = counter(&id, 7, &[("wins", 2)]);
    let mut read_models = ReadModelWritePlanBuilder::new();
    read_models.upsert(&view).unwrap();
    read_models.commit(&repo).await.unwrap();

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

/// A two-row plan whose second row violates a database constraint the read
/// model does not declare. The whole transaction rolls back, the
/// already-applied first row included, and the failure is permanent.
pub async fn read_model_failure_mid_plan_rolls_back_the_whole_plan<B: SqlFaults>(backend: &B)
where
    B::Repository: ReadModelWritePlanStore + RelationalReadModelQueryStore + Send + Sync,
{
    backend.reject_negative(COUNTER_VIEWS, "value").await;
    let repo = backend.repository();
    let good_id = unique_id("midplan-good");
    let bad_id = unique_id("midplan-bad");
    let mut read_models = ReadModelWritePlanBuilder::new();
    read_models.upsert(&counter(&good_id, 1, &[])).unwrap();
    read_models.upsert(&counter(&bad_id, -1, &[])).unwrap();

    let err = repo
        .commit_write_plan(read_models.into_write_plan().unwrap())
        .await
        .expect_err("a mid-plan constraint violation must fail the commit");
    let err = RepositoryError::from(err);
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

/// A released row keeps its backoff across a restart: no claim takes it
/// before the delay ends, and the next claim after it does.
pub async fn released_row_keeps_its_backoff_across_restart<B: Durable>(backend: &B)
where
    B::Repository: TransactionalCommit,
{
    let repo = backend.repository();
    let id = unique_id("retry");
    repo.commit_batch(batch(
        Vec::new(),
        vec![OutboxMessage::create_to(&id, "Retry", "q", b"r".to_vec()).unwrap()],
    ))
    .await
    .unwrap();
    let outbox = B::outbox_store(&repo);
    let claimed = outbox
        .claim(ClaimOutboxMessages::new("w", 10, LEASE))
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    // Long enough that the reopen below is done before the row is due on a
    // loaded machine; an early claim is still told apart from a late one.
    let delay = Duration::from_secs(2);
    let before = SystemTime::now();
    outbox
        .release(
            &OutboxClaimRef::from_message(&claimed[0]).unwrap(),
            "transient",
            delay,
        )
        .await
        .unwrap();
    let after = SystemTime::now();

    let reopened = backend.reopen().await;
    let outbox = B::outbox_store(&reopened);
    let early = outbox
        .claim(ClaimOutboxMessages::new("w", 10, LEASE))
        .await
        .unwrap();
    let retried = if early.is_empty() {
        // The one unavoidable wait: the claim compares against wall time.
        let remaining = (after + delay)
            .duration_since(SystemTime::now())
            .unwrap_or_default();
        tokio::time::sleep(remaining + Duration::from_millis(50)).await;
        outbox
            .claim(ClaimOutboxMessages::new("w", 10, LEASE))
            .await
            .unwrap()
    } else {
        assert!(
            SystemTime::now() >= before + delay,
            "claimed before its backoff ended: {early:?}"
        );
        early
    };
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].id(), id);
    assert_eq!(retried[0].attempts, 2);
}
