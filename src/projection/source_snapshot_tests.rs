//! Same modeled execution proof across memory, SQLite and PostgreSQL.
use std::sync::Arc;

use crate::domain_event::DomainEventContract;
use crate::projection::executor::prepare_portable_projection;
use crate::projection::lower::{EventualOnly, ProjectionDescriptor};
use crate::projection_protocol::*;
use crate::{
    DomainEventDescriptor, DomainEventEnvelope, DomainEventOccurrence, RelationalReadModel, RowKey,
    RowValue,
};

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, crate::DomainState, crate::ReadModel,
)]
#[domain_state(version = 1)]
#[readmodel(table = "source_snapshot_rows", primary_key = ["id"])]
struct SnapshotRow {
    id: String,
    title: String,
}
type SourceSnapshotRows = SnapshotRow;

struct Changed;
impl DomainEventContract for Changed {
    const EVENT_NAME: &'static str = "snapshot.changed";
    const EVENT_VERSION: u64 = 1;
    fn descriptor() -> DomainEventDescriptor {
        DomainEventDescriptor::state::<SnapshotRow>("snapshot.changed", 1)
    }
}
struct Removed;
impl DomainEventContract for Removed {
    const EVENT_NAME: &'static str = "snapshot.removed";
    const EVENT_VERSION: u64 = 1;
    fn descriptor() -> DomainEventDescriptor {
        DomainEventDescriptor::state::<SnapshotRow>("snapshot.removed", 1)
    }
}
#[allow(non_snake_case)]
fn SaveSnapshot() -> crate::Mutation<()> {
    crate::mutation_file!("tests/fixtures/source_snapshot_save.graphql")
}
#[allow(non_snake_case)]
fn DeleteSnapshot() -> crate::Mutation<()> {
    crate::mutation_file!("tests/fixtures/source_snapshot_delete.graphql")
}
crate::projection! {
    const SNAPSHOTS: ProjectionDescriptor<EventualOnly> = {
        name: "source-snapshot-test",
        version: 1,
        epoch: "snapshot-v1",
        model: SnapshotRow,
        source: aggregate_snapshot,
        on { events: [Changed], mutation: SaveSnapshot, input: { row: body }, },
        on { events: [Removed], mutation: DeleteSnapshot, input: { id: aggregate_id }, },
    };
}

fn event(id: &str, sequence: u64, title: &str, delete: bool) -> DomainEventOccurrence {
    occurrence(id, sequence, 0, id, title, delete)
}

fn occurrence(
    id: &str,
    sequence: u64,
    ordinal: u32,
    row_id: &str,
    title: &str,
    delete: bool,
) -> DomainEventOccurrence {
    let mut occurrence = DomainEventOccurrence::capture(
        if delete {
            Removed::descriptor()
        } else {
            Changed::descriptor()
        },
        DomainEventEnvelope {
            aggregate_type: "snapshot-item".into(),
            aggregate_id: id.into(),
            aggregate_sequence: sequence,
            publication_ordinal: ordinal,
            occurred_at: std::time::UNIX_EPOCH,
            metadata: Default::default(),
        },
        &SnapshotRow {
            id: row_id.into(),
            title: title.into(),
        },
    )
    .unwrap();
    occurrence.overwrite_causation_id(&format!("cause-{id}-{sequence}"));
    occurrence
}

struct Harness {
    codec: Arc<ProjectionScopeCodec>,
}
impl Harness {
    async fn new(store: &impl ProjectionProtocolStore) -> Self {
        let topology = ProjectorTopologyId::new(1, "source-snapshot-test", [0x8e; 32]).unwrap();
        let codec = Arc::new(
            ProjectionScopeCodec::with_models(
                topology.clone(),
                [("SnapshotRow", SnapshotRow::schema())],
            )
            .unwrap(),
        );
        store
            .register_projection_models(
                &topology,
                &[ProjectionModelOwnership::new("SnapshotRow", "source_snapshot_rows").unwrap()],
            )
            .await
            .unwrap();
        Self { codec }
    }

    async fn prepare(
        &self,
        store: &impl ProjectionProtocolStore,
        occurrence: &DomainEventOccurrence,
        delivery: u64,
    ) -> Result<ProjectionCommitBatch, ProjectionProtocolError> {
        let partition = self.codec.encode_partition(None).unwrap();
        let input = TrustedProjectionInput::mint(
            ProjectionInputCursor::new(
                self.codec.topology().clone(),
                partition,
                ProjectionSource::new("test-broker", b"stream".to_vec()).unwrap(),
                ProjectionEpoch::new("broker-v1").unwrap(),
                delivery,
            )
            .unwrap(),
            ProjectionInputFingerprint::from_canonical_bytes(
                &occurrence.canonical_bytes().unwrap(),
            ),
            occurrence.id(),
            occurrence.causation_id().unwrap(),
            ProjectionGeneration::initial(),
            false,
        )
        .unwrap();
        let mut workspace = ProjectionWorkspace::new(
            self.codec.clone(),
            None,
            input,
            ProjectionEpoch::new("snapshot-v1").unwrap(),
        )
        .unwrap();
        let lowered = SNAPSHOTS
            .server_executor()
            .unwrap()
            .plan(occurrence)
            .unwrap();
        let prepared = prepare_portable_projection(&workspace, lowered)?;
        let snapshots = store
            .projection_execution_snapshot_batch(prepared.snapshot_request())
            .await?;
        prepared.stage(&mut workspace, snapshots)?;
        workspace.into_batch()
    }

    async fn apply(
        &self,
        store: &impl ProjectionProtocolStore,
        occurrence: &DomainEventOccurrence,
        delivery: u64,
    ) -> Result<ProjectionCommitResult, ProjectionProtocolError> {
        store
            .commit_projection(self.prepare(store, occurrence, delivery).await?)
            .await
    }

    async fn read(
        &self,
        store: &impl ProjectionProtocolStore,
        id: &str,
    ) -> ProjectionQuerySnapshot {
        store
            .projection_query_snapshot(
                &ProjectionQuerySnapshotRequest::new(
                    &self.codec,
                    None,
                    "SnapshotRow",
                    RowKey::new([("id", RowValue::String(id.into()))]),
                    Vec::new(),
                )
                .unwrap(),
            )
            .await
            .unwrap()
    }
}

async fn matrix(store: &impl ProjectionProtocolStore) {
    let h = Harness::new(store).await;
    let first = h
        .apply(store, &event("a", 3, "new", false), 1)
        .await
        .unwrap();
    assert_eq!(first.records.len(), 1);
    for (delivery, sequence) in [(2, 2), (3, 1)] {
        let stale = h
            .apply(store, &event("a", sequence, "old", false), delivery)
            .await
            .unwrap();
        assert!(
            stale.records.is_empty(),
            "stale source must not allocate a row revision"
        );
        let observations: Vec<_> = stale
            .changes
            .iter()
            .filter(|change| change.kind == ProjectionChangeKind::Observation)
            .collect();
        assert_eq!(
            observations.len(),
            1,
            "stale causation must observe current state"
        );
        assert_eq!(
            observations[0].revision,
            Some(first.records[0].revision.clone())
        );
    }
    let snapshot = h.read(store, "a").await;
    assert_eq!(
        snapshot.row.unwrap().get_serde::<String>("title").unwrap(),
        "new"
    );
    assert_eq!(snapshot.record.unwrap().revision, first.records[0].revision);

    // Deletion before every earlier write must still establish a durable fence.
    h.apply(store, &event("b", 5, "", true), 4).await.unwrap();
    h.apply(store, &event("b", 1, "late create", false), 5)
        .await
        .unwrap();
    let deleted = h.read(store, "b").await;
    assert!(deleted.row.is_none());
    assert!(deleted.record.unwrap().tombstone);
    // A newer snapshot explicitly recreates the row; stale deletion cannot undo it.
    h.apply(store, &event("b", 6, "recreated", false), 6)
        .await
        .unwrap();
    h.apply(store, &event("b", 4, "", true), 7).await.unwrap();
    let recreated = h.read(store, "b").await;
    assert_eq!(
        recreated.row.unwrap().get_serde::<String>("title").unwrap(),
        "recreated"
    );
    assert_eq!(recreated.record.unwrap().revision.incarnation(), 2);
    // Repeated newer deletions advance the fence even while the row is absent.
    h.apply(store, &event("b", 7, "", true), 8).await.unwrap();
    h.apply(store, &event("b", 9, "", true), 9).await.unwrap();
    h.apply(store, &event("b", 8, "delayed recreate", false), 10)
        .await
        .unwrap();
    assert!(h.read(store, "b").await.row.is_none());

    // Occurrence IDs alone do not authenticate the body at an equal version.
    assert!(h
        .prepare(store, &event("a", 3, "conflict", false), 11)
        .await
        .is_err());
    // A different stream cannot overwrite another aggregate's row.
    assert!(h
        .prepare(
            store,
            &occurrence("intruder", 99, 0, "a", "takeover", false),
            11
        )
        .await
        .is_err());
    // Different aggregates with equal sequence have independent clocks.
    h.apply(store, &event("c", 1, "independent", false), 11)
        .await
        .unwrap();
    assert!(h.read(store, "c").await.row.is_some());

    // Exact occurrence replay is a transport duplicate, not a second mutation.
    let e = event("a", 4, "latest", false);
    h.apply(store, &e, 12).await.unwrap();
    let replay = h.apply(store, &e, 12).await.unwrap();
    assert_eq!(replay.outcome, ProjectionCommitOutcome::Duplicate);
    assert!(replay.records.is_empty());

    // A stale-read observer cannot falsely confirm a row changed by a concurrent commit.
    h.apply(store, &event("race", 3, "initial", false), 13)
        .await
        .unwrap();
    let stale = h
        .prepare(store, &event("race", 2, "old", false), 15)
        .await
        .unwrap();
    h.apply(store, &event("race", 5, "racing", false), 14)
        .await
        .unwrap();
    assert!(matches!(
        store.commit_projection(stale).await,
        Err(ProjectionProtocolError::RecordRevisionConflict { .. })
    ));

    // Multiple publications in one aggregate commit share a causation but have
    // an ordered publication ordinal. Late confirmation is still idempotent.
    h.apply(
        store,
        &occurrence("ordinal", 1, 1, "ordinal", "second", false),
        16,
    )
    .await
    .unwrap();
    h.apply(
        store,
        &occurrence("ordinal", 1, 0, "ordinal", "first", false),
        17,
    )
    .await
    .unwrap();
    assert_eq!(
        h.read(store, "ordinal")
            .await
            .row
            .unwrap()
            .get_serde::<String>("title")
            .unwrap(),
        "second"
    );

    // Even a lower-level writer cannot clear an existing source fence.
    let mut unfenced = h
        .prepare(store, &event("a", 6, "unsafe", false), 19)
        .await
        .unwrap();
    unfenced.mutations[0].source_snapshot = None;
    assert!(matches!(
        store.commit_projection(unfenced).await,
        Err(ProjectionProtocolError::InvalidBatch(_))
    ));
    assert_eq!(
        h.read(store, "a")
            .await
            .row
            .unwrap()
            .get_serde::<String>("title")
            .unwrap(),
        "latest"
    );

    // Migration does not invent a source version for old read-model rows.
    let mut unversioned = h
        .prepare(store, &event("unversioned", 1, "old schema", false), 19)
        .await
        .unwrap();
    unversioned.mutations[0].source_snapshot = None;
    store.commit_projection(unversioned).await.unwrap();
    assert!(h
        .prepare(store, &event("unversioned", 2, "new schema", false), 20)
        .await
        .is_err());
}

#[tokio::test]
async fn source_snapshots_memory_reordering_and_atomic_confirmation() {
    matrix(&crate::InMemoryRepository::new()).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn source_snapshots_sqlite_reordering_and_restart() {
    use crate::table::TableSchemaRegistry;
    let path = std::env::temp_dir().join(format!(
        "distributed-source-snapshots-{}.sqlite",
        uuid::Uuid::now_v7()
    ));
    let url = format!("sqlite:{}?mode=rwc", path.display());
    let store = crate::SqliteRepository::connect_and_migrate(&url)
        .await
        .unwrap();
    let mut registry = TableSchemaRegistry::new();
    registry
        .register_schema(SnapshotRow::schema().clone())
        .unwrap();
    store
        .bootstrap_table_schema_for_dev(&registry)
        .await
        .unwrap();
    matrix(&store).await;
    store.pool().close().await;
    let restarted = crate::SqliteRepository::connect_and_migrate(&url)
        .await
        .unwrap();
    let h = Harness::new(&restarted).await;
    h.apply(&restarted, &event("b", 2, "after restart", false), 21)
        .await
        .unwrap();
    assert!(h.read(&restarted, "b").await.row.is_none());
    restarted.pool().close().await;
    std::fs::remove_file(path).unwrap();
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn source_snapshots_postgres_reordering_and_restart() {
    let Ok(url) = std::env::var("DISTRIBUTED_SNAPSHOT_TEST_POSTGRES_URL") else {
        return;
    };
    let store = crate::PostgresRepository::connect_and_migrate(&url)
        .await
        .unwrap();
    let mut registry = crate::table::TableSchemaRegistry::new();
    registry
        .register_schema(SnapshotRow::schema().clone())
        .unwrap();
    store
        .bootstrap_table_schema_for_dev(&registry)
        .await
        .unwrap();
    matrix(&store).await;
    store.pool().close().await;
    let restarted = crate::PostgresRepository::connect_and_migrate(&url)
        .await
        .unwrap();
    let h = Harness::new(&restarted).await;
    h.apply(&restarted, &event("b", 2, "after restart", false), 21)
        .await
        .unwrap();
    assert!(h.read(&restarted, "b").await.row.is_none());
}

#[test]
fn source_snapshots_policy_is_explicit_hashed_and_rejects_delta_programs() {
    use crate::projection::{
        ProjectionArm, ProjectionMutationKind as Kind, ProjectionOperation, ProjectionPartition,
        ProjectionProgram,
    };
    let snapshots = SNAPSHOTS.program().unwrap();
    let ordered = ProjectionProgram::try_new(
        snapshots.name(),
        snapshots.version(),
        ProjectionPartition::Unit,
        snapshots.arms().to_vec(),
    )
    .unwrap();
    assert!(snapshots.source_snapshots());
    assert!(!ordered.source_snapshots());
    assert_ne!(snapshots.id().unwrap(), ordered.id().unwrap());
    let arm = &snapshots.arms()[0];
    let op = &arm.operations()[0];
    for kind in [Kind::Insert, Kind::Patch, Kind::UpsertPatch, Kind::Recreate] {
        let operation = ProjectionOperation::try_new(
            op.operation_id(),
            0,
            kind,
            op.target().clone(),
            op.key().to_vec(),
            op.fields().to_vec(),
            vec![],
            vec![],
        )
        .unwrap();
        let arm =
            ProjectionArm::try_new(arm.arm_id(), arm.selector().clone(), vec![operation]).unwrap();
        let program =
            ProjectionProgram::try_new("delta", 1, ProjectionPartition::Unit, vec![arm]).unwrap();
        assert!(
            program.with_source_snapshots().is_err(),
            "{kind:?} cannot silently become a snapshot"
        );
    }
    let partitioned = ProjectionProgram::try_new(
        "partitioned",
        1,
        ProjectionPartition::Expression(op.key()[0].expression().clone()),
        snapshots.arms().to_vec(),
    )
    .unwrap();
    assert!(partitioned.with_source_snapshots().is_err());
}

#[test]
fn source_snapshots_reject_direct_materialization() {
    use crate::projection::placement::*;
    let result = ProjectionBinding::materialize_direct(
        DirectProjectionPlacement::new(&SNAPSHOTS),
        ProjectionSourceBinding::try_new("snapshot-domain", "ordered-domain-events", 1).unwrap(),
        ProjectionOwner::try_new("snapshot-direct").unwrap(),
        "distributed-projection-partition",
        1,
        vec![ProjectionOutput::try_new(
            "SnapshotRow",
            "source_snapshot_rows",
            SnapshotRow::schema().clone(),
        )
        .unwrap()],
        vec![],
        None,
    );
    assert!(matches!(
        result,
        Err(ProjectionTopologyError::DirectIneligible { .. })
    ));
}

fn rebuild_projector() -> crate::graphql::SurfaceProjector {
    let mounts = crate::LocalProjectionMountsBuilder::new("snapshot-test", "events")
        .unwrap()
        .eventual_model::<SnapshotRow, _>("rebuild-test", SNAPSHOTS, "snapshot-v1")
        .unwrap()
        .build()
        .unwrap();
    mounts.projector("rebuild-test").unwrap()
}

async fn rebuild_matrix(store: &impl ProjectionProtocolStore) {
    use crate::projection::rebuild::SnapshotProjectionRebuild;
    let projector = rebuild_projector();
    let (_, binding) = projector.modeled[0].raw().unwrap();
    let physical = binding.physical_topology().unwrap();
    let compiled = CompiledProjectionTopology::from_modeled_binding(
        ProjectorTopologyId::new(physical.version(), physical.name(), physical.digest()).unwrap(),
        binding
            .outputs()
            .iter()
            .map(|o| (o.model(), o.storage(), o.schema())),
    )
    .unwrap();
    store
        .register_projection_models(compiled.topology(), compiled.ownership())
        .await
        .unwrap();
    let h = Harness {
        codec: compiled.codec(),
    };
    let first = event("a", 1, "old", false);
    let second = event("a", 2, "new", false);
    let removed = event("a", 3, "deleted", true);
    let mut old = h.prepare(store, &first, 1).await.unwrap();
    old.mutations[0].source_snapshot = None;
    let applied = store.commit_projection(old).await.unwrap();
    let checkpoint = applied.checkpoint.unwrap();
    let original = h.read(store, "a").await;

    // Neither a missing record nor a suffix masquerading as full history passes.
    for history in [
        vec![],
        vec![second.clone()],
        vec![first.clone(), removed.clone()],
    ] {
        assert!(SnapshotProjectionRebuild::begin(store, &projector)
            .await
            .unwrap()
            .from_complete_history(&history)
            .is_err());
    }
    let mut conflicting = second.clone();
    conflicting.overwrite_causation_id("another-cause");
    assert!(SnapshotProjectionRebuild::begin(store, &projector)
        .await
        .unwrap()
        .from_complete_history(&[first.clone(), second.clone(), conflicting])
        .is_err());
    assert_eq!(h.read(store, "a").await.record, original.record);

    let rebuild = SnapshotProjectionRebuild::begin(store, &projector)
        .await
        .unwrap();
    // Reverse delivery and exact duplicate occurrences are deterministic.
    let plan = rebuild
        .from_complete_history(&[second.clone(), first.clone(), second.clone()])
        .unwrap();
    assert_eq!(plan.record_count(), 1);
    assert_eq!(plan.apply(store).await.unwrap(), 1);
    let row = h.read(store, "a").await;
    assert_eq!(
        row.row.unwrap().get("title"),
        Some(&RowValue::String("new".into()))
    );
    assert!(row.record.unwrap().source_snapshot.is_some());
    assert_eq!(
        store
            .projection_checkpoint(checkpoint.input(), ProjectionGeneration::initial())
            .await
            .unwrap(),
        Some(checkpoint.clone())
    );
    // Inbox/checkpoint identity remains intact; no domain event was republished.
    assert_eq!(
        h.apply(store, &first, 1).await.unwrap().outcome,
        ProjectionCommitOutcome::Duplicate
    );
    h.apply(store, &first, 2).await.unwrap_err(); // same message, different position is still rejected
    h.apply(store, &removed, 3).await.unwrap();

    // Exact inventory CAS rejects modifications made after begin, including inserts.
    let pending = SnapshotProjectionRebuild::begin(store, &projector)
        .await
        .unwrap()
        .from_complete_history(&[first.clone(), second.clone(), removed.clone()])
        .unwrap();
    let other = event("b", 1, "other", false);
    h.apply(store, &other, 4).await.unwrap();
    assert!(pending.apply(store).await.is_err());
    assert!(h.read(store, "a").await.row.is_none());
    // Missing the stored tombstone occurrence cannot resurrect it.
    assert!(SnapshotProjectionRebuild::begin(store, &projector)
        .await
        .unwrap()
        .from_complete_history(&[first.clone(), second.clone(), other.clone()])
        .is_err());
    let recreated = event("a", 4, "recreated", false);
    let history = [first, second, removed, other, recreated.clone()];
    SnapshotProjectionRebuild::begin(store, &projector)
        .await
        .unwrap()
        .from_complete_history(&history)
        .unwrap()
        .apply(store)
        .await
        .unwrap();
    let restored = h.read(store, "a").await;
    assert_eq!(restored.record.unwrap().revision.incarnation(), 2);
    assert_eq!(
        restored.row.unwrap().get("title"),
        Some(&RowValue::String("recreated".into()))
    );
}

async fn rebuild_rollback(store: &impl ProjectionProtocolStore) {
    use crate::projection::rebuild::SnapshotProjectionRebuild;
    let projector = rebuild_projector();
    let before = SnapshotProjectionRebuild::begin(store, &projector)
        .await
        .unwrap();
    let h = Harness {
        codec: before.context.compiled.codec(),
    };
    let original_a = h.read(store, "a").await;
    let original_b = h.read(store, "b").await;
    let history = [
        event("a", 1, "old", false),
        event("a", 2, "new", false),
        event("a", 3, "deleted", true),
        event("a", 4, "recreated", false),
        event("a", 5, "valid first write", false),
        event("b", 1, "other", false),
        event("b", 2, "reject me", false),
    ];
    let error = before
        .from_complete_history(&history)
        .unwrap()
        .apply(store)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("rebuild_test_reject"), "{error}");
    let after_a = h.read(store, "a").await;
    let after_b = h.read(store, "b").await;
    assert_eq!(after_a.row, original_a.row);
    assert_eq!(after_b.row, original_b.row);
    assert_eq!(after_a.record, original_a.record);
    assert_eq!(after_b.record, original_b.record);
}

#[tokio::test]
async fn snapshot_rebuild_memory() {
    rebuild_matrix(&crate::InMemoryRepository::new()).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn snapshot_rebuild_sqlite() {
    let store = crate::SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .unwrap();
    let mut registry = crate::table::TableSchemaRegistry::new();
    registry
        .register_schema(SnapshotRow::schema().clone())
        .unwrap();
    store
        .bootstrap_table_schema_for_dev(&registry)
        .await
        .unwrap();
    rebuild_matrix(&store).await;
    sqlx::query("CREATE TRIGGER rebuild_test_reject BEFORE UPDATE ON source_snapshot_rows WHEN NEW.title = 'reject me' BEGIN SELECT RAISE(ABORT, 'rebuild_test_reject'); END")
        .execute(store.pool()).await.unwrap();
    rebuild_rollback(&store).await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn snapshot_rebuild_postgres() {
    let Ok(url) = std::env::var("DISTRIBUTED_REBUILD_TEST_POSTGRES_URL") else {
        return;
    };
    let store = crate::PostgresRepository::connect_and_migrate(&url)
        .await
        .unwrap();
    let mut registry = crate::table::TableSchemaRegistry::new();
    registry
        .register_schema(SnapshotRow::schema().clone())
        .unwrap();
    store
        .bootstrap_table_schema_for_dev(&registry)
        .await
        .unwrap();
    rebuild_matrix(&store).await;
    sqlx::query("ALTER TABLE source_snapshot_rows ADD CONSTRAINT rebuild_test_reject CHECK (title <> 'reject me')")
        .execute(store.pool()).await.unwrap();
    rebuild_rollback(&store).await;
}
