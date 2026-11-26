//! The projection store conformance suite. Every scenario here runs on the
//! in-memory, SQLite and PostgreSQL stores through
//! [`projection_protocol_scenario_tests`]; a backend's own test file keeps
//! only what has no counterpart on the others.
//!
//! A scenario builds one store. PostgreSQL keeps its data between scenarios,
//! so a scenario that needs several independent histories uses several row
//! keys on that one store, and failure IDs (unique store-wide) go through
//! [`ProjectionProtocolScenario::failure_id`].

use std::future::Future;
use std::sync::Arc;

use super::*;
use crate::repository::{InboxStore, ReadModelWritePlanStore, TransactionalCommit};
use crate::table::{
    ColumnType, DeleteTableRowMutation, ExpectedVersion, PatchMode, PatchTableRowMutation,
    PrimaryKey, RowPatch, RowValue, RowWriteMode, TableColumn, TableKind, TableRowMutation,
};

const TENANT: &str = "scenario-tenant";
const RECORD_ID: &str = "todo-1";

/// A two-column table (`id`, nullable `value`) for a scenario fixture.
pub(crate) fn scenario_schema(model_name: &str, table_name: String) -> &'static TableSchema {
    Box::leak(Box::new(TableSchema {
        model_name: model_name.into(),
        table_name,
        columns: vec![
            TableColumn {
                primary_key: true,
                ..TableColumn::new("id", "id", ColumnType::Text)
            },
            TableColumn {
                nullable: true,
                ..TableColumn::new("value", "value", ColumnType::Text)
            },
        ],
        primary_key: PrimaryKey::new(["id"]),
        version_column: Some(crate::table::DEFAULT_TABLE_VERSION_COLUMN.into()),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    }))
}

pub(crate) trait ProjectionProtocolScenario: Sized + Send + Sync {
    type Store: ProjectionProtocolStore
        + InboxStore
        + ReadModelWritePlanStore
        + TransactionalCommit
        + Clone
        + Send
        + Sync
        + 'static;

    /// The scenario's store, holding a table for each of `schemas` and with
    /// their models registered to [`Self::topology`].
    fn store(&self, schemas: &[&'static TableSchema]) -> impl Future<Output = Self::Store> + Send;

    fn with_retention(
        &self,
        store: Self::Store,
        retention: ProjectionChangeRetention,
    ) -> Self::Store;

    fn topology(&self) -> ProjectorTopologyId;

    fn other_topology(&self) -> ProjectorTopologyId;

    /// The scenario's own table, shaped like [`scenario_schema`].
    fn schema(&self) -> &'static TableSchema;

    /// Failure IDs are unique store-wide; a store that outlives the scenario
    /// makes them unique per scenario.
    fn failure_id(&self, name: &str) -> String {
        name.to_string()
    }

    // Fault hooks: each acts on the scenario's own table or change log
    // behind the store's back.

    /// The `(value, version)` physically stored for row `id`.
    fn physical_row<'a>(
        &'a self,
        store: &'a Self::Store,
        id: &'a str,
    ) -> impl Future<Output = Option<(Option<String>, u64)>> + Send + 'a;

    fn insert_physical_row<'a>(
        &'a self,
        store: &'a Self::Store,
        id: &'a str,
    ) -> impl Future<Output = ()> + Send + 'a;

    fn remove_physical_row<'a>(
        &'a self,
        store: &'a Self::Store,
        id: &'a str,
    ) -> impl Future<Output = ()> + Send + 'a;

    /// While `fail`, any update of the existing row `id` is refused by the
    /// table; `false` lifts it again.
    fn fail_writes_to_row<'a>(
        &'a self,
        store: &'a Self::Store,
        id: &'a str,
        fail: bool,
    ) -> impl Future<Output = ()> + Send + 'a;

    /// Remove one stored change of [`Self::partition`], leaving a hole.
    fn remove_stored_change<'a>(
        &'a self,
        store: &'a Self::Store,
        position: u64,
    ) -> impl Future<Output = ()> + Send + 'a;

    /// The change epoch stored for [`Self::partition`]; `None` until a write
    /// creates the partition.
    fn stored_change_epoch<'a>(
        &'a self,
        store: &'a Self::Store,
    ) -> impl Future<Output = Option<ProjectionEpoch>> + Send + 'a;

    /// Create [`Self::partition`] the way an earlier release did: with an
    /// epoch its application declared.
    fn insert_partition<'a>(
        &'a self,
        store: &'a Self::Store,
        epoch: &'a ProjectionEpoch,
    ) -> impl Future<Output = ()> + Send + 'a;

    /// The positions physically held in [`Self::partition`]'s change log.
    fn stored_change_positions<'a>(
        &'a self,
        store: &'a Self::Store,
    ) -> impl Future<Output = Vec<u64>> + Send + 'a;

    fn repository(&self) -> impl Future<Output = Self::Store> + Send {
        async move { self.store(&[self.schema()]).await }
    }

    fn partition(&self) -> PartitionId {
        ProjectionScopeCodec::new(self.topology())
            .encode_partition(Some(&serde_json::json!(TENANT)))
            .unwrap()
    }

    fn source(&self, name: &str, key: &[u8]) -> ProjectionSource {
        ProjectionSource::new(name, key.to_vec()).unwrap()
    }

    fn input_cursor(&self, position: u64) -> ProjectionInputCursor {
        ProjectionInputCursor::new(
            self.topology(),
            self.partition(),
            self.source("todo_stream", b"todo-1"),
            ProjectionEpoch::new("source-v1").unwrap(),
            position,
        )
        .unwrap()
    }

    fn input(
        &self,
        position: u64,
        fingerprint: &[u8],
        message_id: &str,
        causation_id: &str,
        generation: ProjectionGeneration,
    ) -> TrustedProjectionInput {
        mint(
            self.input_cursor(position),
            fingerprint,
            message_id,
            causation_id,
            generation,
            true,
        )
    }

    fn input_for_partition(
        &self,
        partition: PartitionId,
        position: u64,
        fingerprint: &[u8],
        message_id: &str,
        causation_id: &str,
    ) -> TrustedProjectionInput {
        mint(
            ProjectionInputCursor::new(
                self.topology(),
                partition,
                self.source("todo_stream", b"todo-1"),
                ProjectionEpoch::new("source-v1").unwrap(),
                position,
            )
            .unwrap(),
            fingerprint,
            message_id,
            causation_id,
            ProjectionGeneration::initial(),
            true,
        )
    }

    /// A cursor in the change log of [`Self::partition`], which a write has
    /// already created.
    fn change_cursor<'a>(
        &'a self,
        store: &'a Self::Store,
        position: u64,
    ) -> impl Future<Output = ProjectionChangeCursor> + Send + 'a {
        async move {
            let epoch = self
                .stored_change_epoch(store)
                .await
                .expect("a write created the partition");
            ProjectionChangeCursor::new(self.topology(), self.partition(), epoch, position).unwrap()
        }
    }

    fn ownership(&self) -> ProjectionModelOwnership {
        ownership(self.schema())
    }

    /// A transition of the scenario's one record, `todo-1`.
    fn mutation(
        &self,
        expectation: ProjectionRecordExpectation,
        kind: RecordTransition,
    ) -> ProjectionRecordMutation {
        row_mutation(self, RECORD_ID, None, expectation, kind)
    }

    fn row_exists<'a>(
        &'a self,
        repository: &'a Self::Store,
    ) -> impl Future<Output = bool> + Send + 'a {
        async move { self.physical_row(repository, RECORD_ID).await.is_some() }
    }

    fn batch(
        &self,
        input: TrustedProjectionInput,
        mutations: Vec<ProjectionRecordMutation>,
        observations: Vec<ProjectionObservationRequest>,
    ) -> ProjectionCommitBatch {
        ProjectionCommitBatch {
            input,
            ownership: vec![self.ownership()],
            mutations,
            observations,
        }
    }

    fn failure(&self, input: TrustedProjectionInput, failure_id: &str) -> ProjectionFailureBatch {
        ProjectionFailureBatch::new(input, failure_id, "decode_error", b"bad payload".to_vec())
            .unwrap()
    }
}

fn mint(
    cursor: ProjectionInputCursor,
    fingerprint: &[u8],
    message_id: &str,
    causation_id: &str,
    generation: ProjectionGeneration,
    gap_free: bool,
) -> TrustedProjectionInput {
    TrustedProjectionInput::mint(
        cursor,
        ProjectionInputFingerprint::from_canonical_bytes(fingerprint),
        message_id,
        causation_id,
        generation,
        gap_free,
    )
    .unwrap()
}

fn ownership(schema: &TableSchema) -> ProjectionModelOwnership {
    ProjectionModelOwnership::new(&schema.model_name, &schema.table_name).unwrap()
}

fn codec<S: ProjectionProtocolScenario>(
    scenario: &S,
    schemas: &[&'static TableSchema],
) -> ProjectionScopeCodec {
    ProjectionScopeCodec::with_models(
        scenario.topology(),
        schemas
            .iter()
            .map(|schema| (schema.model_name.as_str(), *schema)),
    )
    .unwrap()
}

fn key(id: &str) -> RowKey {
    RowKey::new([("id", RowValue::String(id.into()))])
}

fn scope_in<S: ProjectionProtocolScenario>(
    scenario: &S,
    schema: &'static TableSchema,
    id: &str,
) -> ProjectionRecordScope {
    codec(scenario, &[schema])
        .encode_row_scope_in_partition(&schema.model_name, scenario.partition(), &key(id))
        .unwrap()
}

fn scope<S: ProjectionProtocolScenario>(scenario: &S, id: &str) -> ProjectionRecordScope {
    scope_in(scenario, scenario.schema(), id)
}

/// An upsert of `columns` (the first is the `id`) into `schema`.
fn upsert_in<S: ProjectionProtocolScenario>(
    scenario: &S,
    schema: &'static TableSchema,
    columns: &[(&str, &str)],
    expectation: ProjectionRecordExpectation,
    kind: RecordTransition,
) -> ProjectionRecordMutation {
    let id = columns[0].1;
    let mut values = RowValues::new();
    for (column, value) in columns {
        values.insert(*column, RowValue::String((*value).into()));
    }
    ProjectionRecordMutation::new(
        scope_in(scenario, schema, id),
        TableMutation::UpsertRow(TableRowMutation {
            schema,
            key: key(id),
            values,
            expected_version: ExpectedVersion::Any,
            mode: RowWriteMode::Upsert,
        }),
        expectation,
        kind,
    )
    .unwrap()
}

/// A transition of row `id` of the scenario's own table.
fn row_mutation<S: ProjectionProtocolScenario>(
    scenario: &S,
    id: &str,
    value: Option<&str>,
    expectation: ProjectionRecordExpectation,
    kind: RecordTransition,
) -> ProjectionRecordMutation {
    if kind == RecordTransition::Delete {
        return ProjectionRecordMutation::new(
            scope(scenario, id),
            TableMutation::DeleteRow(DeleteTableRowMutation {
                schema: scenario.schema(),
                key: key(id),
                expected_version: ExpectedVersion::Any,
            }),
            expectation,
            kind,
        )
        .unwrap();
    }
    let mut columns = vec![("id", id)];
    columns.extend(value.map(|value| ("value", value)));
    upsert_in(scenario, scenario.schema(), &columns, expectation, kind)
}

fn create<S: ProjectionProtocolScenario>(scenario: &S, id: &str) -> ProjectionRecordMutation {
    row_mutation(
        scenario,
        id,
        None,
        ProjectionRecordExpectation::Missing,
        RecordTransition::Upsert,
    )
}

fn checkpoint_probe<S: ProjectionProtocolScenario>(scenario: &S) -> ProjectionCheckpointProbe {
    ProjectionCheckpointProbe::new(
        scenario.topology(),
        scenario.partition(),
        scenario.source("todo_stream", b"todo-1"),
        ProjectionEpoch::new("source-v1").unwrap(),
        ProjectionGeneration::initial(),
    )
}

fn snapshot_request<S: ProjectionProtocolScenario>(
    scenario: &S,
    schemas: &[&'static TableSchema],
    schema: &TableSchema,
    id: &str,
    probes: Vec<ProjectionCheckpointProbe>,
) -> ProjectionQuerySnapshotRequest {
    ProjectionQuerySnapshotRequest::new(
        &codec(scenario, schemas),
        Some(&serde_json::json!(TENANT)),
        &schema.model_name,
        key(id),
        probes,
    )
    .unwrap()
}

async fn inbox_has<S: ProjectionProtocolStore + InboxStore>(
    store: &S,
    input: &TrustedProjectionInput,
) -> bool {
    let receipt = input.inbox_receipt();
    store
        .inbox_contains(&receipt.consumer, &receipt.message_id)
        .await
        .unwrap()
}

async fn all_changes<S: ProjectionProtocolScenario>(
    scenario: &S,
    store: &S::Store,
) -> ProjectionChangeRead {
    store
        .projection_changes(&scenario.topology(), &scenario.partition(), None, 100)
        .await
        .unwrap()
}

/// A commit of `input` left nothing behind.
async fn assert_input_left_no_trace<S: ProjectionProtocolStore + InboxStore>(
    store: &S,
    input: &TrustedProjectionInput,
) {
    assert!(!inbox_has(store, input).await);
    assert_eq!(
        store.projection_input_disposition(input).await.unwrap(),
        ProjectionInputDisposition::Pending
    );
    assert_eq!(
        store
            .projection_checkpoint(&input.cursor, input.generation)
            .await
            .unwrap()
            .filter(|checkpoint| checkpoint.input() == &input.cursor),
        None
    );
}

pub(crate) async fn input_disposition_is_read_only_exact_and_repair_fenced(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    let first_input = scenario.input(
        1,
        b"preflight-one",
        "preflight-message-1",
        "preflight-cause-1",
        ProjectionGeneration::initial(),
    );
    assert_eq!(
        repository
            .projection_input_disposition(&first_input)
            .await
            .unwrap(),
        ProjectionInputDisposition::Pending
    );
    assert_eq!(
        repository
            .projection_partition_runtime_state(&scenario.topology(), &scenario.partition())
            .await
            .unwrap(),
        None,
        "a preflight read must not create projection partition state"
    );

    let applied = repository
        .commit_projection(scenario.batch(first_input.clone(), Vec::new(), Vec::new()))
        .await
        .unwrap();
    assert_eq!(
        repository
            .projection_input_disposition(&first_input)
            .await
            .unwrap(),
        ProjectionInputDisposition::Duplicate(applied.checkpoint.unwrap())
    );

    let stale = scenario.input(
        0,
        b"preflight-stale",
        "preflight-message-0",
        "preflight-cause-0",
        ProjectionGeneration::initial(),
    );
    assert!(matches!(
        repository
            .projection_input_disposition(&stale)
            .await
            .unwrap(),
        ProjectionInputDisposition::Stale(checkpoint) if checkpoint.input().position() == 1
    ));

    let corrupted = scenario.input(
        1,
        b"preflight-corrupt",
        "preflight-message-1",
        "preflight-cause-1",
        ProjectionGeneration::initial(),
    );
    assert!(matches!(
        repository.projection_input_disposition(&corrupted).await,
        Err(ProjectionProtocolError::InputCorruption)
    ));

    let reused_message = scenario.input(
        2,
        b"preflight-two",
        "preflight-message-1",
        "preflight-cause-2",
        ProjectionGeneration::initial(),
    );
    assert!(matches!(
        repository.projection_input_disposition(&reused_message).await,
        Err(ProjectionProtocolError::MessageIdReuse { message_id })
            if message_id == "preflight-message-1"
    ));

    let failed_input = scenario.input(
        2,
        b"preflight-two",
        "preflight-message-2",
        "preflight-cause-2",
        ProjectionGeneration::initial(),
    );
    let preflight_failure = scenario.failure_id("preflight-failure-2");
    repository
        .record_projection_failure(scenario.failure(failed_input.clone(), &preflight_failure))
        .await
        .unwrap();
    assert!(matches!(
        repository
            .projection_input_disposition(&failed_input)
            .await,
        Err(ProjectionProtocolError::PartitionStopped { failure_id })
            if failure_id == preflight_failure
    ));

    let generation = repository
        .repair_projection(
            &scenario.topology(),
            &scenario.partition(),
            &preflight_failure,
        )
        .await
        .unwrap();
    let retry = scenario.input(
        2,
        b"preflight-two",
        "preflight-message-2",
        "preflight-cause-2",
        generation,
    );
    assert_eq!(
        repository
            .projection_input_disposition(&retry)
            .await
            .unwrap(),
        ProjectionInputDisposition::Pending
    );
    assert!(matches!(
        repository.projection_input_disposition(&first_input).await,
        Err(ProjectionProtocolError::GenerationFenced {
            expected: 2,
            actual: 1
        })
    ));
    assert!(matches!(
        repository
            .projection_input_disposition(&scenario.input(
                3,
                b"preflight-later",
                "preflight-message-3",
                "preflight-cause-3",
                generation,
            ))
            .await,
        Err(ProjectionProtocolError::IncomparableInput)
    ));

    let repaired = repository
        .commit_projection(scenario.batch(retry.clone(), Vec::new(), Vec::new()))
        .await
        .unwrap();
    assert_eq!(
        repository
            .projection_input_disposition(&retry)
            .await
            .unwrap(),
        ProjectionInputDisposition::Duplicate(repaired.checkpoint.unwrap())
    );
}

pub(crate) async fn identical_redelivery_advances_only_broker_checkpoint(
    scenario: impl ProjectionProtocolScenario,
) {
    let store = scenario.repository().await;
    let first = scenario.input(
        1,
        b"immutable-input",
        "message-repeat",
        "cause-repeat",
        ProjectionGeneration::initial(),
    );
    let mutation = scenario.mutation(
        ProjectionRecordExpectation::Missing,
        RecordTransition::Upsert,
    );
    let scope = mutation.scope.clone();
    store
        .commit_projection(scenario.batch(first.clone(), vec![mutation], Vec::new()))
        .await
        .unwrap();
    let original = store.projection_record(&scope).await.unwrap().unwrap();
    let repeat = scenario.input(
        2,
        b"immutable-input",
        "message-repeat",
        "cause-repeat",
        ProjectionGeneration::initial(),
    );
    assert_eq!(
        store.projection_input_disposition(&repeat).await.unwrap(),
        ProjectionInputDisposition::Redelivery
    );
    // Even a caller that supplies effects again cannot repeat them.
    let repeated = store
        .commit_projection(scenario.batch(
            repeat.clone(),
            vec![scenario.mutation(
                ProjectionRecordExpectation::Missing,
                RecordTransition::Upsert,
            )],
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(repeated.outcome, ProjectionCommitOutcome::Duplicate);
    assert_eq!(repeated.checkpoint.unwrap().input().position(), 2);
    assert_eq!(repeated.changes.len(), 1);
    assert_eq!(repeated.changes[0].kind, ProjectionChangeKind::Checkpoint);
    assert_eq!(
        store.projection_record(&scope).await.unwrap().unwrap(),
        original
    );
    assert!(matches!(
        store.projection_input_disposition(&repeat).await.unwrap(),
        ProjectionInputDisposition::Duplicate(_)
    ));
    let next = scenario.input(
        3,
        b"next-input",
        "message-next",
        "cause-next",
        ProjectionGeneration::initial(),
    );
    store
        .commit_projection(scenario.batch(next, Vec::new(), Vec::new()))
        .await
        .unwrap();
    // Every alias position permanently retains the original bytes and identity.
    let substitution = scenario.input(
        2,
        b"immutable-input",
        "substituted-message",
        "cause-repeat",
        ProjectionGeneration::initial(),
    );
    assert!(matches!(
        store.projection_input_disposition(&substitution).await,
        Err(ProjectionProtocolError::InputCorruption)
    ));
    let altered = scenario.input(
        4,
        b"changed-input",
        "message-repeat",
        "cause-repeat",
        ProjectionGeneration::initial(),
    );
    assert!(store.projection_input_disposition(&altered).await.is_err());
    let other_partition = scenario.input_for_partition(
        PartitionId::new(b"other-partition".to_vec()).unwrap(),
        1,
        b"immutable-input",
        "message-repeat",
        "cause-repeat",
    );
    assert!(matches!(
        store.projection_input_disposition(&other_partition).await,
        Err(ProjectionProtocolError::MessageIdReuse { .. })
    ));

    // A repaired generation owns a fresh execution history. An old-generation
    // receipt must not suppress that generation's first actual delivery.
    let failed = scenario.input(
        4,
        b"repair-input",
        "repair-message",
        "repair-cause",
        ProjectionGeneration::initial(),
    );
    let repair_redelivery = scenario.failure_id("repair-redelivery");
    store
        .record_projection_failure(scenario.failure(failed, &repair_redelivery))
        .await
        .unwrap();
    let generation = store
        .repair_projection(
            &scenario.topology(),
            &scenario.partition(),
            &repair_redelivery,
        )
        .await
        .unwrap();
    store
        .commit_projection(scenario.batch(
            scenario.input(
                4,
                b"repair-input",
                "repair-message",
                "repair-cause",
                generation,
            ),
            Vec::new(),
            Vec::new(),
        ))
        .await
        .unwrap();
    let replay = scenario.input(
        5,
        b"immutable-input",
        "message-repeat",
        "cause-repeat",
        generation,
    );
    assert_eq!(
        store.projection_input_disposition(&replay).await.unwrap(),
        ProjectionInputDisposition::Pending
    );
    let applied = store
        .commit_projection(scenario.batch(
            replay,
            vec![scenario.mutation(
                ProjectionRecordExpectation::Exact(original.revision),
                RecordTransition::Upsert,
            )],
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(applied.outcome, ProjectionCommitOutcome::Applied);
    let repeated = scenario.input(
        6,
        b"immutable-input",
        "message-repeat",
        "cause-repeat",
        generation,
    );
    assert_eq!(
        store.projection_input_disposition(&repeated).await.unwrap(),
        ProjectionInputDisposition::Redelivery
    );
}

pub(crate) async fn message_identity_is_topology_wide_across_projection_partitions(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    repository
        .commit_projection(scenario.batch(
            scenario.input(
                1,
                b"topology-wide-message",
                "topology-wide-message",
                "topology-wide-cause",
                ProjectionGeneration::initial(),
            ),
            Vec::new(),
            Vec::new(),
        ))
        .await
        .unwrap();

    let other_partition = ProjectionScopeCodec::new(scenario.topology())
        .encode_partition(Some(&serde_json::json!("tenant-b")))
        .unwrap();
    let remapped = scenario.input_for_partition(
        other_partition,
        1,
        b"topology-wide-message",
        "topology-wide-message",
        "topology-wide-cause",
    );

    assert!(matches!(
        repository
            .commit_projection(scenario.batch(remapped, Vec::new(), Vec::new()))
            .await,
        Err(ProjectionProtocolError::MessageIdReuse { message_id })
            if message_id == "topology-wide-message"
    ));
}

pub(crate) async fn failure_recording_is_idempotent_for_exact_batch(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    let failure_id = scenario.failure_id("idempotent-failure");
    let batch = scenario.failure(
        scenario.input(
            1,
            b"idempotent-failure",
            "idempotent-failure-message",
            "idempotent-failure-cause",
            ProjectionGeneration::initial(),
        ),
        &failure_id,
    );

    let failure = repository
        .record_projection_failure(batch.clone())
        .await
        .unwrap();
    assert_eq!(
        repository.record_projection_failure(batch).await.unwrap(),
        failure
    );
    assert_eq!(
        repository
            .projection_failure(&scenario.topology(), &scenario.partition(), &failure_id,)
            .await
            .unwrap(),
        Some(failure)
    );
}

pub(crate) async fn registered_table_ownership_rejects_other_topology(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    assert!(matches!(
        repository
            .register_projection_models(&scenario.other_topology(), &[scenario.ownership()])
            .await,
        Err(ProjectionProtocolError::InvalidBatch(message))
            if message.contains("authorit")
    ));
}

pub(crate) async fn tombstone_requires_explicit_exact_recreation(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    let created = repository
        .commit_projection(scenario.batch(
            scenario.input(
                1,
                b"create",
                "message-1",
                "cause-1",
                ProjectionGeneration::initial(),
            ),
            vec![scenario.mutation(
                ProjectionRecordExpectation::Missing,
                RecordTransition::Upsert,
            )],
            Vec::new(),
        ))
        .await
        .unwrap()
        .records
        .pop()
        .unwrap();
    let deleted = repository
        .commit_projection(scenario.batch(
            scenario.input(
                2,
                b"delete",
                "message-2",
                "cause-2",
                ProjectionGeneration::initial(),
            ),
            vec![scenario.mutation(
                ProjectionRecordExpectation::Exact(created.revision),
                RecordTransition::Delete,
            )],
            Vec::new(),
        ))
        .await
        .unwrap()
        .records
        .pop()
        .unwrap();
    assert!(deleted.tombstone);
    assert!(!scenario.row_exists(&repository).await);

    assert!(matches!(
        repository
            .commit_projection(scenario.batch(
                scenario.input(
                    3,
                    b"plain-upsert",
                    "message-3",
                    "cause-3",
                    ProjectionGeneration::initial(),
                ),
                vec![scenario.mutation(
                    ProjectionRecordExpectation::Exact(deleted.revision.clone()),
                    RecordTransition::Upsert,
                )],
                Vec::new(),
            ))
            .await,
        Err(ProjectionProtocolError::RecordTombstoned { .. })
    ));

    let recreated = repository
        .commit_projection(scenario.batch(
            scenario.input(
                3,
                b"recreate",
                "message-3b",
                "cause-3",
                ProjectionGeneration::initial(),
            ),
            vec![scenario.mutation(
                ProjectionRecordExpectation::Exact(deleted.revision),
                RecordTransition::Recreate,
            )],
            Vec::new(),
        ))
        .await
        .unwrap()
        .records
        .pop()
        .unwrap();
    assert_eq!(recreated.revision.incarnation(), 2);
    assert_eq!(recreated.revision.revision(), 1);
    assert!(!recreated.tombstone);
    assert!(scenario.row_exists(&repository).await);
}

fn source_snapshot(sequence: u64) -> crate::projection_protocol::SourceSnapshotVersion {
    serde_json::from_value(serde_json::json!({
        "aggregate_type": "todo",
        "aggregate_id": "todo-1",
        "sequence": sequence,
        "publication_ordinal": 0,
        "occurrence_id": format!("occurrence-{sequence}"),
        "occurrence_fingerprint": vec![sequence; 32],
    }))
    .unwrap()
}

/// Create then delete the scenario record, optionally source-fenced, and
/// return the tombstone.
async fn tombstone<S: ProjectionProtocolScenario>(
    scenario: &S,
    repository: &S::Store,
    source_fenced: bool,
) -> ProjectionRecordMetadata {
    let mut revision = None;
    for (position, kind) in [(1, RecordTransition::Upsert), (2, RecordTransition::Delete)] {
        let mut mutation = scenario.mutation(
            revision
                .take()
                .map_or(ProjectionRecordExpectation::Missing, |revision| {
                    ProjectionRecordExpectation::Exact(revision)
                }),
            kind,
        );
        mutation.source_snapshot = source_fenced.then(|| source_snapshot(position));
        let record = repository
            .commit_projection(scenario.batch(
                scenario.input(
                    position,
                    format!("tombstone-{position}").as_bytes(),
                    &format!("tombstone-message-{position}"),
                    &format!("tombstone-cause-{position}"),
                    ProjectionGeneration::initial(),
                ),
                vec![mutation],
                Vec::new(),
            ))
            .await
            .unwrap()
            .records
            .pop()
            .unwrap();
        revision = Some(record.revision.clone());
        if kind == RecordTransition::Delete {
            assert!(record.tombstone);
            return record;
        }
    }
    unreachable!()
}

fn confirm_existing<S: ProjectionProtocolScenario>(
    scenario: &S,
    revision: RecordRevision,
) -> ProjectionCommitBatch {
    scenario.batch(
        scenario.input(
            3,
            b"late-confirmation",
            "late-confirmation-message",
            "late-confirmation-cause",
            ProjectionGeneration::initial(),
        ),
        Vec::new(),
        vec![ProjectionObservationRequest {
            kind: ProjectionObservationKind::Record,
            target: ProjectionObservationTarget::ExistingRecord(revision),
        }],
    )
}

/// A deleted row that carries no source fence proves nothing about a later
/// input, so that input cannot confirm it.
pub(crate) async fn existing_record_observation_rejects_an_unfenced_tombstone(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    let deleted = tombstone(&scenario, &repository, false).await;
    assert!(matches!(
        repository
            .commit_projection(confirm_existing(&scenario, deleted.revision))
            .await,
        Err(ProjectionProtocolError::RecordTombstoned { .. })
    ));
}

/// A source-fenced tombstone is the newest state of its stream: a stale
/// input confirms it rather than failing.
pub(crate) async fn existing_record_observation_confirms_a_source_fenced_tombstone(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    let deleted = tombstone(&scenario, &repository, true).await;
    let confirmed = repository
        .commit_projection(confirm_existing(&scenario, deleted.revision.clone()))
        .await
        .unwrap();
    assert_eq!(confirmed.outcome, ProjectionCommitOutcome::Applied);
    let evidence = repository
        .projection_causation_evidence(
            &ProjectionCausationEvidenceRequest::new(
                "late-confirmation-cause",
                vec![scenario.topology()],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(evidence.observations.len(), 1);
    assert_eq!(evidence.observations[0].revision, Some(deleted.revision));
}

/// Causation evidence is ordered by the canonical bytes of each scope, on
/// every backend, never by a storage digest or database collation.
pub(crate) async fn causation_evidence_is_ordered_by_canonical_scope_bytes(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    let model = scenario.ownership().model;
    let partitions = [
        scenario.partition(),
        ProjectionScopeCodec::new(scenario.topology())
            .encode_partition(Some(&serde_json::json!("evidence-order-tenant")))
            .unwrap(),
    ];
    let mut expected = Vec::new();
    for (index, partition) in partitions.iter().enumerate() {
        let scopes = [b"e".as_slice(), b"B", b"d", b"a", b"C", b"aa"].map(|key| {
            ProjectionRecordScope::new(scenario.topology(), partition.clone(), &model, key).unwrap()
        });
        expected.extend(scopes.iter().cloned());
        repository
            .commit_projection(
                scenario.batch(
                    scenario.input_for_partition(
                        partition.clone(),
                        1,
                        format!("evidence-order-{index}").as_bytes(),
                        &format!("evidence-order-message-{index}"),
                        "evidence-order-cause",
                    ),
                    Vec::new(),
                    scopes
                        .into_iter()
                        .map(|scope| ProjectionObservationRequest {
                            kind: ProjectionObservationKind::Dependency,
                            target: ProjectionObservationTarget::Dependency(scope),
                        })
                        .collect(),
                ),
            )
            .await
            .unwrap();
    }
    expected.sort_by_key(|scope| {
        (
            scope.projection_partition().canonical_bytes().to_vec(),
            scope.canonical_key_bytes().to_vec(),
        )
    });

    let evidence = repository
        .projection_causation_evidence(
            &ProjectionCausationEvidenceRequest::new(
                "evidence-order-cause",
                vec![scenario.topology()],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        evidence
            .observations
            .into_iter()
            .map(|observation| observation.scope)
            .collect::<Vec<_>>(),
        expected
    );
}

/// One physical row has one owning projection partition. Another partition
/// creating the same key is told the record exists; it is not a malformed
/// batch.
pub(crate) async fn another_partition_cannot_create_an_owned_row(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    let create = || {
        scenario.mutation(
            ProjectionRecordExpectation::Missing,
            RecordTransition::Upsert,
        )
    };
    repository
        .commit_projection(scenario.batch(
            scenario.input(
                1,
                b"owner-create",
                "owner-create-message",
                "owner-create-cause",
                ProjectionGeneration::initial(),
            ),
            vec![create()],
            Vec::new(),
        ))
        .await
        .unwrap();

    let other_partition = ProjectionScopeCodec::new(scenario.topology())
        .encode_partition(Some(&serde_json::json!("other-owner-tenant")))
        .unwrap();
    let mut intruding = create();
    intruding.scope = ProjectionRecordScope::new(
        scenario.topology(),
        other_partition.clone(),
        intruding.scope.model(),
        intruding.scope.canonical_key_bytes(),
    )
    .unwrap();
    let scope = intruding.scope.clone();
    assert!(matches!(
        repository
            .commit_projection(scenario.batch(
                scenario.input_for_partition(
                    other_partition,
                    1,
                    b"intruder-create",
                    "intruder-create-message",
                    "intruder-create-cause",
                ),
                vec![intruding],
                Vec::new(),
            ))
            .await,
        Err(ProjectionProtocolError::RecordAlreadyExists { .. })
    ));
    assert_eq!(repository.projection_record(&scope).await.unwrap(), None);
    assert!(scenario.row_exists(&repository).await);
}

/// An input that contradicts its source's registered capability is corrupt,
/// whatever else is wrong with it: corruption outranks message-ID reuse.
pub(crate) async fn source_capability_mismatch_outranks_message_id_reuse(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    repository
        .commit_projection(scenario.batch(
            scenario.input(
                1,
                b"capability-one",
                "capability-message",
                "capability-cause",
                ProjectionGeneration::initial(),
            ),
            Vec::new(),
            Vec::new(),
        ))
        .await
        .unwrap();
    let contradicting = TrustedProjectionInput::mint(
        scenario.input_cursor(2),
        ProjectionInputFingerprint::from_canonical_bytes(b"capability-two"),
        "capability-message",
        "capability-cause",
        ProjectionGeneration::initial(),
        false,
    )
    .unwrap();

    assert!(matches!(
        repository
            .projection_input_disposition(&contradicting)
            .await,
        Err(ProjectionProtocolError::InputCorruption)
    ));
    assert!(matches!(
        repository
            .commit_projection(scenario.batch(contradicting, Vec::new(), Vec::new()))
            .await,
        Err(ProjectionProtocolError::InputCorruption)
    ));
}

/// Nobody declares a change epoch. The store mints one when a write creates
/// the partition, and every later write of that partition continues it.
pub(crate) async fn a_new_partition_mints_its_change_epoch_once(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    assert_eq!(scenario.stored_change_epoch(&repository).await, None);

    let first = repository
        .commit_projection(noop(&scenario, 1, ProjectionGeneration::initial()))
        .await
        .unwrap();
    let minted = scenario
        .stored_change_epoch(&repository)
        .await
        .expect("the first write created the partition");
    let change_epoch =
        |checkpoint: Option<&ProjectionCheckpoint>| checkpoint.unwrap().change().epoch().clone();
    assert_eq!(change_epoch(first.checkpoint.as_ref()), minted);

    let second = repository
        .commit_projection(noop(&scenario, 2, ProjectionGeneration::initial()))
        .await
        .unwrap();
    assert_eq!(change_epoch(second.checkpoint.as_ref()), minted);
    let failure = repository
        .record_projection_failure(scenario.failure(
            scenario.input(
                3,
                b"input-3",
                "message-3",
                "cause-3",
                ProjectionGeneration::initial(),
            ),
            &scenario.failure_id("minted-epoch-failure"),
        ))
        .await
        .unwrap();
    assert_eq!(failure.change.epoch(), &minted);
    assert_eq!(
        scenario.stored_change_epoch(&repository).await,
        Some(minted)
    );
}

/// A partition an earlier release created with a declared epoch keeps it:
/// commits and failures continue its change log, and nothing is refused.
pub(crate) async fn an_existing_partition_keeps_the_change_epoch_it_has(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    let declared = ProjectionEpoch::new("todos-v1").unwrap();
    scenario.insert_partition(&repository, &declared).await;

    let committed = repository
        .commit_projection(noop(&scenario, 1, ProjectionGeneration::initial()))
        .await
        .unwrap();
    assert_eq!(committed.outcome, ProjectionCommitOutcome::Applied);
    assert_eq!(committed.checkpoint.unwrap().change().epoch(), &declared);
    let failure = repository
        .record_projection_failure(scenario.failure(
            scenario.input(
                2,
                b"input-2",
                "message-2",
                "cause-2",
                ProjectionGeneration::initial(),
            ),
            &scenario.failure_id("declared-epoch-failure"),
        ))
        .await
        .unwrap();
    assert_eq!(failure.change.epoch(), &declared);
    assert_eq!(
        scenario.stored_change_epoch(&repository).await,
        Some(declared)
    );
}

fn assert_query_snapshot_is_coherent(snapshot: &ProjectionQuerySnapshot) {
    let row = snapshot.row.as_ref().expect("physical row");
    let record = snapshot.record.as_ref().expect("record metadata");
    let checkpoint = snapshot.checkpoints[0]
        .checkpoint
        .as_ref()
        .expect("source checkpoint");
    let value = match row.get("value") {
        Some(RowValue::String(value)) => value.parse::<u64>().unwrap(),
        other => panic!("unexpected query snapshot value {other:?}"),
    };
    assert_eq!(value, record.revision.revision());
    assert_eq!(value, checkpoint.input().position());
    assert_eq!(record.change, *checkpoint.change());
    assert_eq!(
        snapshot.change_head.as_ref(),
        Some(checkpoint.change()),
        "live resume head must come from the same snapshot"
    );
    assert_eq!(snapshot.compacted_through, 0);
}

/// A query snapshot read while a writer advances the row never pairs one
/// commit's row with another commit's revision, checkpoint or change head.
pub(crate) async fn query_snapshot_never_mixes_row_revision_checkpoint_or_resume_head(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    let request = || {
        snapshot_request(
            &scenario,
            &[scenario.schema()],
            scenario.schema(),
            RECORD_ID,
            vec![checkpoint_probe(&scenario)],
        )
    };
    let write = |position: u64, expectation| {
        scenario.batch(
            scenario.input(
                position,
                format!("snapshot-{position}").as_bytes(),
                &format!("snapshot-message-{position}"),
                &format!("snapshot-cause-{position}"),
                ProjectionGeneration::initial(),
            ),
            vec![row_mutation(
                &scenario,
                RECORD_ID,
                Some(&position.to_string()),
                expectation,
                RecordTransition::Upsert,
            )],
            Vec::new(),
        )
    };
    let first = repository
        .commit_projection(write(1, ProjectionRecordExpectation::Missing))
        .await
        .unwrap();
    assert_query_snapshot_is_coherent(
        &repository
            .projection_query_snapshot(&request())
            .await
            .unwrap(),
    );

    let written = std::sync::atomic::AtomicBool::new(false);
    let writer = async {
        let mut expected = first.records[0].revision.clone();
        for position in 2..=64 {
            let committed = repository
                .commit_projection(write(
                    position,
                    ProjectionRecordExpectation::Exact(expected),
                ))
                .await
                .unwrap();
            expected = committed.records[0].revision.clone();
            tokio::task::yield_now().await;
        }
        written.store(true, std::sync::atomic::Ordering::SeqCst);
    };
    let reader = async {
        while !written.load(std::sync::atomic::Ordering::SeqCst) {
            assert_query_snapshot_is_coherent(
                &repository
                    .projection_query_snapshot(&request())
                    .await
                    .unwrap(),
            );
            tokio::task::yield_now().await;
        }
    };
    tokio::join!(writer, reader);
    assert_query_snapshot_is_coherent(
        &repository
            .projection_query_snapshot(&request())
            .await
            .unwrap(),
    );
}

/// An execution snapshot answers every requested scope, in request order,
/// across tables, and a request never names one scope twice.
pub(crate) async fn execution_snapshot_echoes_every_requested_scope_across_tables(
    scenario: impl ProjectionProtocolScenario,
) {
    let base = &scenario.schema().table_name;
    let schemas = [
        scenario_schema("SnapshotParentView", format!("{base}_parents")),
        scenario_schema("SnapshotChildView", format!("{base}_children")),
    ];
    let [parent, child] = schemas;
    let repository = scenario.store(&schemas).await;
    let create = |schema, columns: &[(&str, &str)]| {
        upsert_in(
            &scenario,
            schema,
            columns,
            ProjectionRecordExpectation::Missing,
            RecordTransition::Upsert,
        )
    };
    repository
        .commit_projection(ProjectionCommitBatch {
            input: scenario.input(
                1,
                b"execution-snapshot",
                "execution-snapshot-message-1",
                "execution-snapshot-cause-1",
                ProjectionGeneration::initial(),
            ),
            ownership: schemas.iter().map(|schema| ownership(schema)).collect(),
            mutations: vec![
                create(parent, &[("id", "parent-1")]),
                create(child, &[("id", "child-1")]),
            ],
            observations: Vec::new(),
        })
        .await
        .unwrap();

    let root = |schema, id| snapshot_request(&scenario, &schemas, schema, id, Vec::new());
    let execution_request = ProjectionExecutionSnapshotBatchRequest::new(vec![
        root(parent, "parent-1"),
        root(child, "child-1"),
    ])
    .unwrap();
    let execution = repository
        .projection_execution_snapshot_batch(&execution_request)
        .await
        .unwrap();
    assert_eq!(
        execution
            .snapshots
            .iter()
            .map(|snapshot| snapshot.scope.clone())
            .collect::<Vec<_>>(),
        execution_request
            .requests
            .iter()
            .map(|request| request.scope.clone())
            .collect::<Vec<_>>()
    );
    assert!(execution
        .snapshots
        .iter()
        .all(|snapshot| snapshot.row.is_some() && snapshot.record.is_some()));
    assert!(ProjectionExecutionSnapshotBatchRequest::new(vec![
        execution_request.requests[0].clone(),
        execution_request.requests[0].clone(),
    ])
    .is_err());
}

/// One input writing four tables commits every row and every causal artifact.
pub(crate) async fn one_four_table_occurrence_commits_rows_and_every_causal_artifact(
    scenario: impl ProjectionProtocolScenario,
) {
    let schemas = ["FanoutParent", "FanoutChild", "FanoutSummary", "FanoutJoin"].map(|model| {
        scenario_schema(
            model,
            format!("{}_{}", scenario.schema().table_name, model.to_lowercase()),
        )
    });
    let repository = scenario.store(&schemas).await;
    let id = |index: usize| format!("fanout-{index}");
    let trusted = scenario.input(
        1,
        b"four-table-occurrence",
        "four-table-message-1",
        "four-table-cause-1",
        ProjectionGeneration::initial(),
    );
    let result = repository
        .commit_projection(ProjectionCommitBatch {
            input: trusted.clone(),
            ownership: schemas.iter().map(|schema| ownership(schema)).collect(),
            mutations: schemas
                .iter()
                .enumerate()
                .map(|(index, schema)| {
                    upsert_in(
                        &scenario,
                        schema,
                        &[("id", &id(index))],
                        ProjectionRecordExpectation::Missing,
                        RecordTransition::Upsert,
                    )
                })
                .collect(),
            observations: schemas
                .iter()
                .enumerate()
                .map(|(index, schema)| ProjectionObservationRequest {
                    kind: ProjectionObservationKind::Record,
                    target: ProjectionObservationTarget::StagedRecord(scope_in(
                        &scenario,
                        schema,
                        &id(index),
                    )),
                })
                .collect(),
        })
        .await
        .unwrap();

    assert_eq!(result.outcome, ProjectionCommitOutcome::Applied);
    assert_eq!(result.records.len(), 4);
    assert!(result.records.iter().all(|record| !record.tombstone));
    assert_eq!(result.changes.len(), 4);
    assert!(result
        .changes
        .iter()
        .all(|change| change.kind == ProjectionChangeKind::RecordUpsert));
    assert_eq!(
        result
            .checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.input()),
        Some(&trusted.cursor)
    );
    assert!(inbox_has(&repository, &trusted).await);
    assert_eq!(
        repository
            .projection_causation_evidence(
                &ProjectionCausationEvidenceRequest::new(
                    "four-table-cause-1",
                    vec![scenario.topology()],
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .observations
            .len(),
        4
    );

    for (index, schema) in schemas.iter().enumerate() {
        let snapshot = repository
            .projection_query_snapshot(&snapshot_request(
                &scenario,
                &schemas,
                schema,
                &id(index),
                vec![checkpoint_probe(&scenario)],
            ))
            .await
            .unwrap();
        assert!(snapshot.row.is_some());
        assert_eq!(
            snapshot
                .record
                .as_ref()
                .map(|record| record.revision.revision()),
            Some(1)
        );
        assert_eq!(
            snapshot.checkpoints[0]
                .checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.input()),
            Some(&trusted.cursor)
        );
    }
}

/// A four-row commit whose table refuses the write at each position in turn
/// leaves rows, records, changes, checkpoint, receipts and observations
/// exactly as they were, and the same input then applies once.
pub(crate) async fn commit_failure_at_each_position_rolls_back_every_side_effect(
    scenario: impl ProjectionProtocolScenario,
) {
    const POSITIONS: usize = 4;
    let repository = scenario.repository().await;

    for fail_at in 0..POSITIONS {
        let id = |index: usize| format!("matrix-{fail_at}-{index}");
        let write = |index: usize, value: &str, expectation| {
            row_mutation(
                &scenario,
                &id(index),
                Some(value),
                expectation,
                RecordTransition::Upsert,
            )
        };
        let snapshot = |index: usize| {
            snapshot_request(
                &scenario,
                &[scenario.schema()],
                scenario.schema(),
                &id(index),
                Vec::new(),
            )
        };
        let first_position = 2 * fail_at as u64 + 1;
        let initial = repository
            .commit_projection(
                scenario.batch(
                    scenario.input(
                        first_position,
                        format!("matrix-initial-{fail_at}").as_bytes(),
                        &format!("matrix-message-1-{fail_at}"),
                        &format!("matrix-cause-1-{fail_at}"),
                        ProjectionGeneration::initial(),
                    ),
                    (0..POSITIONS)
                        .map(|index| write(index, "before", ProjectionRecordExpectation::Missing))
                        .collect(),
                    Vec::new(),
                ),
            )
            .await
            .unwrap();
        assert_eq!(initial.records.len(), POSITIONS);

        scenario
            .fail_writes_to_row(&repository, &id(fail_at), true)
            .await;
        let mut physical_before = Vec::new();
        let mut snapshots_before = Vec::new();
        for index in 0..POSITIONS {
            physical_before.push(scenario.physical_row(&repository, &id(index)).await);
            snapshots_before.push(
                repository
                    .projection_query_snapshot(&snapshot(index))
                    .await
                    .unwrap(),
            );
        }
        let changes_before = all_changes(&scenario, &repository).await;
        let retry_input = scenario.input(
            first_position + 1,
            format!("matrix-fail-{fail_at}").as_bytes(),
            &format!("matrix-message-2-{fail_at}"),
            &format!("matrix-cause-2-{fail_at}"),
            ProjectionGeneration::initial(),
        );
        let checkpoint_before = repository
            .projection_checkpoint(&retry_input.cursor, ProjectionGeneration::initial())
            .await
            .unwrap();

        let failed_batch = || {
            scenario.batch(
                retry_input.clone(),
                (0..POSITIONS)
                    .map(|index| {
                        write(
                            index,
                            "after",
                            ProjectionRecordExpectation::Exact(
                                initial.records[index].revision.clone(),
                            ),
                        )
                    })
                    .collect(),
                vec![ProjectionObservationRequest {
                    kind: ProjectionObservationKind::Record,
                    target: ProjectionObservationTarget::StagedRecord(scope(&scenario, &id(0))),
                }],
            )
        };
        let error = repository
            .commit_projection(failed_batch())
            .await
            .unwrap_err();
        assert!(
            matches!(error, ProjectionProtocolError::Table(_)),
            "failure position {fail_at}: {error}"
        );

        for index in 0..POSITIONS {
            assert_eq!(
                scenario.physical_row(&repository, &id(index)).await,
                physical_before[index],
                "failure position {fail_at}"
            );
            assert_eq!(
                repository
                    .projection_query_snapshot(&snapshot(index))
                    .await
                    .unwrap(),
                snapshots_before[index],
                "failure position {fail_at}"
            );
        }
        assert_eq!(
            all_changes(&scenario, &repository).await,
            changes_before,
            "failure position {fail_at}"
        );
        assert_eq!(
            repository
                .projection_checkpoint(&retry_input.cursor, ProjectionGeneration::initial())
                .await
                .unwrap(),
            checkpoint_before,
            "failure position {fail_at}"
        );
        assert_input_left_no_trace(&repository, &retry_input).await;
        assert_eq!(
            repository
                .projection_observation(
                    &retry_input.causation_id,
                    &scope(&scenario, &id(0)),
                    ProjectionObservationKind::Record,
                )
                .await
                .unwrap(),
            None,
            "failure position {fail_at}"
        );

        scenario
            .fail_writes_to_row(&repository, &id(fail_at), false)
            .await;
        let applied = repository.commit_projection(failed_batch()).await.unwrap();
        assert_eq!(applied.outcome, ProjectionCommitOutcome::Applied);
        let duplicate = repository.commit_projection(failed_batch()).await.unwrap();
        assert_eq!(duplicate.outcome, ProjectionCommitOutcome::Duplicate);
        assert!(duplicate.records.is_empty());
        assert!(duplicate.changes.is_empty());
    }
}

/// A row write the table itself refuses leaves no row, record, checkpoint,
/// partition, change or receipt, and the same input then applies.
pub(crate) async fn table_failure_rolls_back_rows_protocol_and_inbox(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    let input = scenario.input(
        1,
        b"invalid",
        "message-invalid",
        "cause-invalid",
        ProjectionGeneration::initial(),
    );
    // No column values at all: the table rejects the row.
    let invalid = ProjectionRecordMutation::new(
        scope(&scenario, RECORD_ID),
        TableMutation::UpsertRow(TableRowMutation {
            schema: scenario.schema(),
            key: key(RECORD_ID),
            values: RowValues::new(),
            expected_version: ExpectedVersion::Any,
            mode: RowWriteMode::Upsert,
        }),
        ProjectionRecordExpectation::Missing,
        RecordTransition::Upsert,
    )
    .unwrap();
    assert!(matches!(
        repository
            .commit_projection(scenario.batch(input.clone(), vec![invalid], Vec::new()))
            .await,
        Err(ProjectionProtocolError::Table(_))
    ));
    assert!(!scenario.row_exists(&repository).await);
    assert_eq!(
        repository
            .projection_record(&scope(&scenario, RECORD_ID))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        repository
            .projection_checkpoint(&input.cursor, ProjectionGeneration::initial())
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        repository
            .projection_partition_runtime_state(&scenario.topology(), &scenario.partition())
            .await
            .unwrap(),
        None
    );
    assert_input_left_no_trace(&repository, &input).await;
    assert!(scenario
        .stored_change_positions(&repository)
        .await
        .is_empty());
    assert_eq!(
        all_changes(&scenario, &repository).await,
        ProjectionChangeRead::Changes {
            head: None,
            compacted_through: 0,
            changes: Vec::new(),
        }
    );

    assert_eq!(
        repository
            .commit_projection(scenario.batch(
                input,
                vec![create(&scenario, RECORD_ID)],
                Vec::new()
            ))
            .await
            .unwrap()
            .outcome,
        ProjectionCommitOutcome::Applied
    );
}

/// The physical row and its record metadata must agree. A row that exists
/// without a record, or a record whose row is gone, fails the commit with the
/// typed drift error and changes nothing.
pub(crate) async fn physical_row_drift_is_rejected_without_side_effects(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    let input = |position: u64, name: &str| {
        scenario.input(
            position,
            format!("{name}-{position}").as_bytes(),
            &format!("{name}-message-{position}"),
            &format!("{name}-cause-{position}"),
            ProjectionGeneration::initial(),
        )
    };
    let exact = |id: &str, revision: &RecordRevision, kind| {
        row_mutation(
            &scenario,
            id,
            None,
            ProjectionRecordExpectation::Exact(revision.clone()),
            kind,
        )
    };

    // A row nobody recorded: creating its record is refused before the
    // partition exists at all.
    scenario.insert_physical_row(&repository, "orphan").await;
    let orphan = input(1, "orphan");
    assert!(matches!(
        repository
            .commit_projection(scenario.batch(
                orphan.clone(),
                vec![create(&scenario, "orphan")],
                Vec::new()
            ))
            .await,
        Err(ProjectionProtocolError::RecordAlreadyExists { .. })
    ));
    assert_eq!(
        repository
            .projection_record(&scope(&scenario, "orphan"))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        repository
            .projection_partition_runtime_state(&scenario.topology(), &scenario.partition())
            .await
            .unwrap(),
        None
    );
    assert_input_left_no_trace(&repository, &orphan).await;
    assert!(scenario.physical_row(&repository, "orphan").await.is_some());

    // A record whose row was removed: save, patch and delete are refused.
    let mut position = 1;
    for operation in ["save", "patch", "delete"] {
        let metadata = repository
            .commit_projection(scenario.batch(
                input(position, operation),
                vec![create(&scenario, operation)],
                Vec::new(),
            ))
            .await
            .unwrap()
            .records
            .remove(0);
        position += 1;
        let changes_before = all_changes(&scenario, &repository).await;
        scenario.remove_physical_row(&repository, operation).await;
        let attempted = match operation {
            "save" => exact(operation, &metadata.revision, RecordTransition::Upsert),
            "patch" => ProjectionRecordMutation::new(
                scope(&scenario, operation),
                TableMutation::PatchRow(PatchTableRowMutation {
                    schema: scenario.schema(),
                    key: key(operation),
                    patch: RowPatch::new().set("id", RowValue::String(operation.into())),
                    expected_version: ExpectedVersion::Any,
                    mode: PatchMode::UpdateExisting,
                }),
                ProjectionRecordExpectation::Exact(metadata.revision.clone()),
                RecordTransition::Upsert,
            )
            .unwrap(),
            "delete" => exact(operation, &metadata.revision, RecordTransition::Delete),
            _ => unreachable!(),
        };
        let refused = input(position, operation);
        assert!(
            matches!(
                repository
                    .commit_projection(scenario.batch(refused.clone(), vec![attempted], Vec::new()))
                    .await,
                Err(ProjectionProtocolError::RecordMissing { .. })
            ),
            "{operation}"
        );
        assert!(
            matches!(
                repository
                    .projection_query_snapshot(&snapshot_request(
                        &scenario,
                        &[scenario.schema()],
                        scenario.schema(),
                        operation,
                        Vec::new(),
                    ))
                    .await,
                Err(ProjectionProtocolError::RecordMissing { .. })
            ),
            "{operation}: a query must not serve a record without its row"
        );
        assert_eq!(
            repository
                .projection_record(&scope(&scenario, operation))
                .await
                .unwrap(),
            Some(metadata),
            "{operation}"
        );
        assert_eq!(
            all_changes(&scenario, &repository).await,
            changes_before,
            "{operation}"
        );
        assert_input_left_no_trace(&repository, &refused).await;
    }

    // A tombstone whose row reappeared: recreation is refused.
    let created = repository
        .commit_projection(scenario.batch(
            input(position, "recreate"),
            vec![create(&scenario, "recreate")],
            Vec::new(),
        ))
        .await
        .unwrap()
        .records
        .remove(0);
    let deleted = repository
        .commit_projection(scenario.batch(
            input(position + 1, "recreate"),
            vec![exact(
                "recreate",
                &created.revision,
                RecordTransition::Delete,
            )],
            Vec::new(),
        ))
        .await
        .unwrap()
        .records
        .remove(0);
    scenario.insert_physical_row(&repository, "recreate").await;
    let changes_before = all_changes(&scenario, &repository).await;
    let refused = input(position + 2, "recreate");
    assert!(matches!(
        repository
            .commit_projection(scenario.batch(
                refused.clone(),
                vec![exact(
                    "recreate",
                    &deleted.revision,
                    RecordTransition::Recreate
                )],
                Vec::new(),
            ))
            .await,
        Err(ProjectionProtocolError::RecordAlreadyExists { .. })
    ));
    assert_eq!(
        repository
            .projection_record(&scope(&scenario, "recreate"))
            .await
            .unwrap(),
        Some(deleted)
    );
    assert_eq!(all_changes(&scenario, &repository).await, changes_before);
    assert_input_left_no_trace(&repository, &refused).await;
    assert!(scenario
        .physical_row(&repository, "recreate")
        .await
        .is_some());
}

fn noop<S: ProjectionProtocolScenario>(
    scenario: &S,
    position: u64,
    generation: ProjectionGeneration,
) -> ProjectionCommitBatch {
    scenario.batch(
        scenario.input(
            position,
            format!("input-{position}").as_bytes(),
            &format!("message-{position}"),
            &format!("cause-{position}"),
            generation,
        ),
        Vec::new(),
        Vec::new(),
    )
}

fn positions(changes: &[ProjectionChange]) -> Vec<u64> {
    changes
        .iter()
        .map(|change| change.cursor.position())
        .collect()
}

/// Retention removes exactly the oldest prefix after successes and failures
/// alike, and a longer retention never brings a removed prefix back.
pub(crate) async fn change_retention_compacts_success_and_failure_prefixes(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.with_retention(
        scenario.repository().await,
        ProjectionChangeRetention::new(2).unwrap(),
    );
    let read = |after: Option<u64>| {
        let repository = &repository;
        let scenario = &scenario;
        async move {
            let after = match after {
                Some(position) => Some(scenario.change_cursor(repository, position).await),
                None => None,
            };
            repository
                .projection_changes(
                    &scenario.topology(),
                    &scenario.partition(),
                    after.as_ref(),
                    100,
                )
                .await
                .unwrap()
        }
    };
    for position in 0..4 {
        repository
            .commit_projection(noop(&scenario, position, ProjectionGeneration::initial()))
            .await
            .unwrap();
    }

    assert_eq!(
        read(None).await,
        ProjectionChangeRead::ResetRequired {
            head: Some(scenario.change_cursor(&repository, 4).await),
            compacted_through: 2,
        }
    );
    assert_eq!(scenario.stored_change_positions(&repository).await, [3, 4]);
    let ProjectionChangeRead::Changes {
        head,
        compacted_through,
        changes,
    } = read(Some(2)).await
    else {
        panic!("the exact compacted-through cursor must resume retained changes");
    };
    assert_eq!(head, Some(scenario.change_cursor(&repository, 4).await));
    assert_eq!(compacted_through, 2);
    assert_eq!(positions(&changes), [3, 4]);
    assert!(matches!(
        read(Some(1)).await,
        ProjectionChangeRead::ResetRequired {
            compacted_through: 2,
            ..
        }
    ));

    let failure_id = scenario.failure_id("failure-4");
    let failure = repository
        .record_projection_failure(scenario.failure(
            scenario.input(
                4,
                b"input-4",
                "message-4",
                "cause-4",
                ProjectionGeneration::initial(),
            ),
            &failure_id,
        ))
        .await
        .unwrap();
    let ProjectionChangeRead::Changes {
        head,
        compacted_through,
        changes,
    } = read(Some(3)).await
    else {
        panic!("the new compacted-through cursor must resume retained changes");
    };
    assert_eq!(head, Some(scenario.change_cursor(&repository, 5).await));
    assert_eq!(compacted_through, 3);
    assert_eq!(
        changes
            .iter()
            .map(|change| (change.cursor.position(), change.kind))
            .collect::<Vec<_>>(),
        vec![
            (4, ProjectionChangeKind::Checkpoint),
            (5, ProjectionChangeKind::Failure),
        ]
    );
    assert_eq!(changes[1].cursor, failure.change);
    assert_eq!(scenario.stored_change_positions(&repository).await, [4, 5]);

    // Lengthening retention keeps what is left and restores nothing.
    let repository =
        scenario.with_retention(repository, ProjectionChangeRetention::new(10).unwrap());
    let generation = repository
        .repair_projection(&scenario.topology(), &scenario.partition(), &failure_id)
        .await
        .unwrap();
    for position in 4..6 {
        repository
            .commit_projection(noop(&scenario, position, generation))
            .await
            .unwrap();
    }
    assert_eq!(
        scenario.stored_change_positions(&repository).await,
        [4, 5, 6, 7]
    );
    assert_eq!(
        repository
            .projection_changes(&scenario.topology(), &scenario.partition(), None, 100)
            .await
            .unwrap(),
        ProjectionChangeRead::ResetRequired {
            head: Some(scenario.change_cursor(&repository, 7).await),
            compacted_through: 3,
        }
    );
}

pub(crate) async fn lengthening_retention_never_restores_a_compacted_prefix(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.with_retention(
        scenario.repository().await,
        ProjectionChangeRetention::new(1).unwrap(),
    );
    for position in 0..3 {
        repository
            .commit_projection(noop(&scenario, position, ProjectionGeneration::initial()))
            .await
            .unwrap();
    }
    assert_eq!(
        repository
            .compact_projection_changes(&scenario.change_cursor(&repository, 3).await)
            .await
            .unwrap(),
        3
    );

    let lengthened =
        scenario.with_retention(repository, ProjectionChangeRetention::new(10).unwrap());
    lengthened
        .commit_projection(noop(&scenario, 3, ProjectionGeneration::initial()))
        .await
        .unwrap();
    assert_eq!(
        all_changes(&scenario, &lengthened).await,
        ProjectionChangeRead::ResetRequired {
            head: Some(scenario.change_cursor(&lengthened, 4).await),
            compacted_through: 3,
        }
    );
    assert_eq!(
        lengthened
            .projection_changes(
                &scenario.topology(),
                &scenario.partition(),
                Some(&scenario.change_cursor(&lengthened, 3).await),
                100
            )
            .await
            .unwrap(),
        ProjectionChangeRead::Changes {
            head: Some(scenario.change_cursor(&lengthened, 4).await),
            compacted_through: 3,
            changes: vec![ProjectionChange {
                cursor: scenario.change_cursor(&lengthened, 4).await,
                kind: ProjectionChangeKind::Checkpoint,
                causation_id: "cause-3".into(),
                observation_kind: None,
                scope: None,
                revision: None,
                failure_id: None,
                program_id: None,
            }],
        }
    );
    assert_eq!(scenario.stored_change_positions(&lengthened).await, [4]);
}

/// When the change that retention must remove is not there, the commit that
/// triggered retention fails whole.
pub(crate) async fn retention_failure_rolls_back_rows_protocol_and_inbox(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.with_retention(
        scenario.repository().await,
        ProjectionChangeRetention::new(1).unwrap(),
    );
    let created = repository
        .commit_projection(scenario.batch(
            scenario.input(
                0,
                b"create",
                "message-0",
                "cause-0",
                ProjectionGeneration::initial(),
            ),
            vec![create(&scenario, RECORD_ID)],
            Vec::new(),
        ))
        .await
        .unwrap()
        .records
        .remove(0);
    let row_before = scenario.physical_row(&repository, RECORD_ID).await;
    scenario.remove_stored_change(&repository, 1).await;

    let update = scenario.input(
        1,
        b"update",
        "message-1",
        "cause-1",
        ProjectionGeneration::initial(),
    );
    let error = repository
        .commit_projection(scenario.batch(
            update.clone(),
            vec![scenario.mutation(
                ProjectionRecordExpectation::Exact(created.revision.clone()),
                RecordTransition::Upsert,
            )],
            Vec::new(),
        ))
        .await
        .unwrap_err();
    assert!(
        matches!(error, ProjectionProtocolError::StorageCorruption(_)),
        "{error}"
    );

    assert_eq!(
        scenario.physical_row(&repository, RECORD_ID).await,
        row_before
    );
    assert_eq!(
        repository
            .projection_record(&scope(&scenario, RECORD_ID))
            .await
            .unwrap(),
        Some(created)
    );
    assert_input_left_no_trace(&repository, &update).await;
    assert!(scenario
        .stored_change_positions(&repository)
        .await
        .is_empty());
    // The head is still the one change that was written, nothing compacted.
    assert_eq!(
        repository
            .projection_changes(
                &scenario.topology(),
                &scenario.partition(),
                Some(&scenario.change_cursor(&repository, 1).await),
                100
            )
            .await
            .unwrap(),
        ProjectionChangeRead::Changes {
            head: Some(scenario.change_cursor(&repository, 1).await),
            compacted_through: 0,
            changes: Vec::new(),
        }
    );
}

/// The retained change log has no holes. A page that would skip a position,
/// or stop short of the head, is storage corruption and is never served.
pub(crate) async fn change_page_with_a_hole_is_rejected(scenario: impl ProjectionProtocolScenario) {
    let repository = scenario.repository().await;
    for position in 1..=3 {
        repository
            .commit_projection(noop(&scenario, position, ProjectionGeneration::initial()))
            .await
            .unwrap();
    }
    let read = |after: Option<u64>, limit: usize| {
        let repository = &repository;
        let scenario = &scenario;
        async move {
            let after = match after {
                Some(position) => Some(scenario.change_cursor(repository, position).await),
                None => None,
            };
            repository
                .projection_changes(
                    &scenario.topology(),
                    &scenario.partition(),
                    after.as_ref(),
                    limit,
                )
                .await
        }
    };
    let is_corrupt = |read: Result<ProjectionChangeRead, ProjectionProtocolError>| {
        matches!(read, Err(ProjectionProtocolError::StorageCorruption(_)))
    };
    let held = |read: Result<ProjectionChangeRead, ProjectionProtocolError>| match read.unwrap() {
        ProjectionChangeRead::Changes { changes, .. } => positions(&changes),
        other => panic!("expected a change page, got {other:?}"),
    };
    assert_eq!(held(read(None, 100).await), [1, 2, 3]);

    scenario.remove_stored_change(&repository, 2).await;
    assert!(is_corrupt(read(None, 100).await));
    assert!(is_corrupt(read(Some(1), 100).await));
    assert_eq!(held(read(None, 1).await), [1]);
    assert_eq!(held(read(Some(2), 100).await), [3]);

    scenario.remove_stored_change(&repository, 3).await;
    assert!(is_corrupt(read(Some(2), 100).await));
    assert!(held(read(Some(3), 100).await).is_empty());
}

fn other_source_input<S: ProjectionProtocolScenario>(
    scenario: &S,
    message_id: &str,
    generation: ProjectionGeneration,
) -> TrustedProjectionInput {
    mint(
        ProjectionInputCursor::new(
            scenario.topology(),
            scenario.partition(),
            scenario.source("other_stream", b"other-1"),
            ProjectionEpoch::new("source-v1").unwrap(),
            0,
        )
        .unwrap(),
        b"other-input",
        message_id,
        "other-cause",
        generation,
        true,
    )
}

pub(crate) async fn cursor_fences_are_exact_and_non_mutating(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    let initial = ProjectionGeneration::initial();
    let commit = |input| {
        let repository = &repository;
        let batch = scenario.batch(input, Vec::new(), Vec::new());
        async move { repository.commit_projection(batch).await }
    };
    let applied = repository
        .commit_projection(scenario.batch(
            scenario.input(1, b"input-1", "message-1", "cause-1", initial),
            vec![create(&scenario, RECORD_ID)],
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(applied.outcome, ProjectionCommitOutcome::Applied);
    assert!(scenario.row_exists(&repository).await);

    let duplicate = repository
        .commit_projection(scenario.batch(
            scenario.input(1, b"input-1", "message-1", "cause-1", initial),
            vec![create(&scenario, RECORD_ID)],
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(duplicate.outcome, ProjectionCommitOutcome::Duplicate);
    assert!(duplicate.changes.is_empty());
    assert_eq!(
        duplicate.checkpoint.as_ref().unwrap().change(),
        applied.checkpoint.as_ref().unwrap().change()
    );

    for corrupted in [
        scenario.input(1, b"input-1", "different-message", "cause-1", initial),
        scenario.input(1, b"input-1", "message-1", "different-cause", initial),
        scenario.input(1, b"corrupt", "message-corrupt", "cause-1", initial),
    ] {
        assert!(matches!(
            commit(corrupted).await,
            Err(ProjectionProtocolError::InputCorruption)
        ));
    }

    let stale = commit(scenario.input(0, b"older", "message-0", "cause-0", initial))
        .await
        .unwrap();
    assert_eq!(stale.outcome, ProjectionCommitOutcome::StaleInput);
    assert!(matches!(
        commit(scenario.input(0, b"older", "message-1", "cause-0", initial)).await,
        Err(ProjectionProtocolError::MessageIdReuse { .. })
    ));
    let incomparable = mint(
        ProjectionInputCursor::new(
            scenario.topology(),
            scenario.partition(),
            scenario.source("todo_stream", b"todo-1"),
            ProjectionEpoch::new("source-v2").unwrap(),
            2,
        )
        .unwrap(),
        b"new-epoch",
        "message-new-epoch",
        "cause-new-epoch",
        initial,
        true,
    );
    assert!(matches!(
        commit(incomparable).await,
        Err(ProjectionProtocolError::IncomparableInput)
    ));

    assert_eq!(
        all_changes(&scenario, &repository).await,
        ProjectionChangeRead::Changes {
            head: applied
                .checkpoint
                .map(|checkpoint| checkpoint.change().clone()),
            compacted_through: 0,
            changes: applied.changes,
        }
    );
}

pub(crate) async fn message_receipts_and_gap_capability_are_immutable(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    let initial = ProjectionGeneration::initial();
    let commit = |input| {
        let repository = &repository;
        let batch = scenario.batch(input, Vec::new(), Vec::new());
        async move { repository.commit_projection(batch).await }
    };
    for (position, fingerprint, message, cause) in [
        (1, b"one".as_slice(), "message-1", "cause-1"),
        (2, b"two".as_slice(), "message-2", "cause-2"),
    ] {
        commit(scenario.input(position, fingerprint, message, cause, initial))
            .await
            .unwrap();
    }

    let old_duplicate = commit(scenario.input(1, b"one", "message-1", "cause-1", initial))
        .await
        .unwrap();
    assert_eq!(old_duplicate.outcome, ProjectionCommitOutcome::Duplicate);

    for changed_old_cursor in [
        scenario.input(1, b"one", "new-message-at-old-cursor", "cause-1", initial),
        scenario.input(
            1,
            b"changed-after-advance",
            "new-message-after-advance",
            "cause-1",
            initial,
        ),
        scenario.input(1, b"one", "message-1", "changed-cause", initial),
    ] {
        assert!(matches!(
            commit(changed_old_cursor).await,
            Err(ProjectionProtocolError::InputCorruption)
        ));
    }

    assert!(matches!(
        commit(scenario.input(3, b"three", "message-1", "cause-3", initial)).await,
        Err(ProjectionProtocolError::MessageIdReuse { .. })
    ));
    assert!(matches!(
        commit(scenario.input(4, b"gap", "gap-message", "gap-cause", initial)).await,
        Err(ProjectionProtocolError::IncomparableInput)
    ));

    for (position, fingerprint, message, cause) in [
        (1, b"one".as_slice(), "old-capability", "cause-1"),
        (2, b"two".as_slice(), "equal-capability", "cause-2"),
        (3, b"three".as_slice(), "new-capability", "cause-3"),
    ] {
        let changed_capability = mint(
            scenario.input_cursor(position),
            fingerprint,
            message,
            cause,
            initial,
            false,
        );
        assert!(matches!(
            commit(changed_capability).await,
            Err(ProjectionProtocolError::InputCorruption)
        ));
    }

    // Another source of the same partition starts its own cursor, wherever
    // its first input happens to be.
    let other_source = |position| {
        ProjectionInputCursor::new(
            scenario.topology(),
            scenario.partition(),
            scenario.source("audit_stream", b"audit-1"),
            ProjectionEpoch::new("source-v1").unwrap(),
            position,
        )
        .unwrap()
    };
    commit(mint(
        other_source(41),
        b"audit",
        "audit-message",
        "audit-cause",
        initial,
        true,
    ))
    .await
    .unwrap();
    assert_eq!(
        repository
            .projection_checkpoint(&other_source(0), initial)
            .await
            .unwrap()
            .unwrap()
            .input()
            .position(),
        41
    );
}

/// A first-ever terminal failure also registers the source capability and
/// position; a repair generation cannot redefine either merely because there
/// was no last-good checkpoint to copy.
pub(crate) async fn first_failure_registers_the_source_for_its_repair(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    let failure_id = scenario.failure_id("failed-first-id");
    repository
        .record_projection_failure(scenario.failure(
            scenario.input(
                0,
                b"failed-first",
                "failed-first-message",
                "failed-first-cause",
                ProjectionGeneration::initial(),
            ),
            &failure_id,
        ))
        .await
        .unwrap();
    let repaired_generation = repository
        .repair_projection(&scenario.topology(), &scenario.partition(), &failure_id)
        .await
        .unwrap();
    assert!(matches!(
        repository
            .commit_projection(scenario.batch(
                scenario.input(
                    1,
                    b"later-first",
                    "later-first-message",
                    "later-first-cause",
                    repaired_generation,
                ),
                Vec::new(),
                Vec::new(),
            ))
            .await,
        Err(ProjectionProtocolError::IncomparableInput)
    ));
    let changed_after_repair = mint(
        scenario.input_cursor(0),
        b"retry",
        "retry-message",
        "retry-cause",
        repaired_generation,
        false,
    );
    assert!(matches!(
        repository
            .commit_projection(scenario.batch(changed_after_repair, Vec::new(), Vec::new()))
            .await,
        Err(ProjectionProtocolError::InputCorruption)
    ));
}

pub(crate) async fn repair_generation_retries_only_the_exact_failed_input(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    let initial = ProjectionGeneration::initial();
    let commit = |input| {
        let repository = &repository;
        let batch = scenario.batch(input, Vec::new(), Vec::new());
        async move { repository.commit_projection(batch).await }
    };
    commit(scenario.input(1, b"one", "message-1", "cause-1", initial))
        .await
        .unwrap();
    let failure_id = scenario.failure_id("failure-2");
    let failure = repository
        .record_projection_failure(scenario.failure(
            scenario.input(2, b"two", "message-2", "cause-2", initial),
            &failure_id,
        ))
        .await
        .unwrap();

    assert!(matches!(
        commit(scenario.input(2, b"changed", "message-2", "cause-2", initial)).await,
        Err(ProjectionProtocolError::InputCorruption)
    ));

    let generation = repository
        .repair_projection(&scenario.topology(), &scenario.partition(), &failure_id)
        .await
        .unwrap();
    let pending_retry = || {
        let repository = &repository;
        let scenario = &scenario;
        async move {
            repository
                .projection_partition_runtime_state(&scenario.topology(), &scenario.partition())
                .await
                .unwrap()
                .unwrap()
                .pending_retry
        }
    };
    let retry = pending_retry().await.unwrap();
    assert_eq!(retry.failure_id, failure_id);
    assert_eq!(retry.failed_generation, initial);
    assert_eq!(retry.input, failure.input);

    assert!(matches!(
        commit(scenario.input(1, b"changed-known-cursor", "message-1", "cause-1", initial)).await,
        Err(ProjectionProtocolError::InputCorruption)
    ));
    let changed_source_capability = mint(
        scenario.input_cursor(3),
        b"changed-source-capability",
        "changed-source-capability-message",
        "changed-source-capability-cause",
        initial,
        false,
    );
    assert!(matches!(
        commit(changed_source_capability).await,
        Err(ProjectionProtocolError::InputCorruption)
    ));
    assert!(matches!(
        commit(other_source_input(
            &scenario,
            "unknown-old-generation",
            initial
        ))
        .await,
        Err(ProjectionProtocolError::GenerationFenced {
            expected: 2,
            actual: 1,
        })
    ));
    assert!(matches!(
        commit(other_source_input(&scenario, "message-1", initial)).await,
        Err(ProjectionProtocolError::MessageIdReuse { message_id })
            if message_id == "message-1"
    ));

    let inherited = commit(scenario.input(1, b"one", "message-1", "cause-1", generation))
        .await
        .unwrap();
    assert_eq!(inherited.outcome, ProjectionCommitOutcome::Duplicate);
    let inherited_older = commit(scenario.input(0, b"zero", "message-0", "cause-0", generation))
        .await
        .unwrap();
    assert_eq!(inherited_older.outcome, ProjectionCommitOutcome::StaleInput);

    assert!(matches!(
        commit(other_source_input(&scenario, "other-message", generation)).await,
        Err(ProjectionProtocolError::IncomparableInput)
    ));
    assert!(matches!(
        commit(other_source_input(&scenario, "message-1", generation)).await,
        Err(ProjectionProtocolError::MessageIdReuse { message_id })
            if message_id == "message-1"
    ));
    assert!(matches!(
        commit(scenario.input(2, b"changed", "message-2", "cause-2", generation)).await,
        Err(ProjectionProtocolError::InputCorruption)
    ));

    let repaired = commit(scenario.input(2, b"two", "message-2", "cause-2", generation))
        .await
        .unwrap();
    assert_eq!(repaired.outcome, ProjectionCommitOutcome::Applied);
    assert_eq!(pending_retry().await, None);
    assert_eq!(
        commit(scenario.input(3, b"three", "message-3", "cause-3", generation))
            .await
            .unwrap()
            .outcome,
        ProjectionCommitOutcome::Applied
    );
}

/// A retry that fails again stops the partition on the new failure.
pub(crate) async fn a_retry_that_fails_again_stops_the_partition(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    let failed = |generation| {
        scenario.input(
            0,
            b"first-failure",
            "first-failure-message",
            "first-failure-cause",
            generation,
        )
    };
    let first_failure = scenario.failure_id("first-failure");
    let second_failure = scenario.failure_id("second-failure");
    repository
        .record_projection_failure(
            scenario.failure(failed(ProjectionGeneration::initial()), &first_failure),
        )
        .await
        .unwrap();
    let retry_generation = repository
        .repair_projection(&scenario.topology(), &scenario.partition(), &first_failure)
        .await
        .unwrap();
    let retried_failure = repository
        .record_projection_failure(scenario.failure(failed(retry_generation), &second_failure))
        .await
        .unwrap();
    assert_eq!(retried_failure.generation, retry_generation);
    assert!(matches!(
        repository
            .commit_projection(scenario.batch(
                other_source_input(&scenario, "blocked-after-retry-failure", retry_generation),
                Vec::new(),
                Vec::new(),
            ))
            .await,
        Err(ProjectionProtocolError::PartitionStopped { failure_id })
            if failure_id == second_failure
    ));
}

/// A source that is not gap-free may fail far past its checkpoint. Its repair
/// still admits the failed cursor before anything later.
pub(crate) async fn non_gap_repair_requires_failed_cursor_before_later_input(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    let input = |position: u64, name: &str, generation| {
        mint(
            scenario.input_cursor(position),
            format!("{name}-{position}").as_bytes(),
            &format!("non-gap-message-{position}"),
            &format!("non-gap-cause-{position}"),
            generation,
            false,
        )
    };
    let commit = |input| {
        let repository = &repository;
        let batch = scenario.batch(input, Vec::new(), Vec::new());
        async move { repository.commit_projection(batch).await }
    };
    let initial = ProjectionGeneration::initial();
    commit(input(5, "checkpoint", initial)).await.unwrap();
    let failure_id = scenario.failure_id("non-gap-failure-9");
    let failure = repository
        .record_projection_failure(scenario.failure(input(9, "failure", initial), &failure_id))
        .await
        .unwrap();
    assert!(!failure.gap_free);
    let generation = repository
        .repair_projection(&scenario.topology(), &scenario.partition(), &failure_id)
        .await
        .unwrap();

    assert_eq!(
        commit(input(5, "checkpoint", generation))
            .await
            .unwrap()
            .outcome,
        ProjectionCommitOutcome::Duplicate
    );
    assert!(matches!(
        commit(input(10, "later", generation)).await,
        Err(ProjectionProtocolError::IncomparableInput)
    ));
    assert_eq!(
        commit(input(9, "failure", generation))
            .await
            .unwrap()
            .outcome,
        ProjectionCommitOutcome::Applied
    );
    assert_eq!(
        commit(input(10, "later", generation))
            .await
            .unwrap()
            .outcome,
        ProjectionCommitOutcome::Applied
    );
}

/// The first observation of a causation at a scope is the one kept, a staged
/// record observation rides on the row change, and a later confirmation of an
/// already-observed record adds only a checkpoint.
pub(crate) async fn observations_are_immutable_and_staged_records_reuse_row_changes(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    let scope = scope(&scenario, RECORD_ID);
    let initial = ProjectionGeneration::initial();
    let observe = |target| {
        vec![ProjectionObservationRequest {
            kind: ProjectionObservationKind::Record,
            target,
        }]
    };
    let staged = || observe(ProjectionObservationTarget::StagedRecord(scope.clone()));
    let observation = |causation_id: &'static str, kind| {
        let repository = &repository;
        let scope = &scope;
        async move {
            repository
                .projection_observation(causation_id, scope, kind)
                .await
                .unwrap()
        }
    };
    let record = ProjectionObservationKind::Record;

    let first = repository
        .commit_projection(scenario.batch(
            scenario.input(1, b"first", "message-1", "cause-stable", initial),
            vec![create(&scenario, RECORD_ID)],
            staged(),
        ))
        .await
        .unwrap();
    assert_eq!(first.changes.len(), 1);
    assert_eq!(first.changes[0].kind, ProjectionChangeKind::RecordUpsert);
    let earliest = observation("cause-stable", record).await.unwrap();
    assert_eq!(earliest.change, first.changes[0].cursor);
    assert_eq!(earliest.revision.as_ref(), Some(&first.records[0].revision));

    let second = repository
        .commit_projection(scenario.batch(
            scenario.input(2, b"second", "message-2", "cause-stable", initial),
            vec![scenario.mutation(
                ProjectionRecordExpectation::Exact(first.records[0].revision.clone()),
                RecordTransition::Upsert,
            )],
            staged(),
        ))
        .await
        .unwrap();
    assert_eq!(second.changes.len(), 1);
    assert_eq!(second.changes[0].kind, ProjectionChangeKind::RecordUpsert);
    assert_eq!(
        observation("cause-stable", record).await,
        Some(earliest.clone())
    );

    let existing = || {
        observe(ProjectionObservationTarget::ExistingRecord(
            second.records[0].revision.clone(),
        ))
    };
    let confirmed = repository
        .commit_projection(scenario.batch(
            scenario.input(3, b"existing", "message-3", "cause-existing", initial),
            Vec::new(),
            existing(),
        ))
        .await
        .unwrap();
    assert_eq!(confirmed.changes.len(), 1);
    assert_eq!(confirmed.changes[0].kind, ProjectionChangeKind::Observation);
    let earliest_existing = observation("cause-existing", record).await.unwrap();

    let repeated = repository
        .commit_projection(scenario.batch(
            scenario.input(
                4,
                b"existing-repeat",
                "message-4",
                "cause-existing",
                initial,
            ),
            Vec::new(),
            existing(),
        ))
        .await
        .unwrap();
    assert_eq!(repeated.changes.len(), 1);
    assert_eq!(repeated.changes[0].kind, ProjectionChangeKind::Checkpoint);
    assert_eq!(
        observation("cause-existing", record).await,
        Some(earliest_existing)
    );

    // Deleting the record under the first causation keeps its first
    // observation, and a recreation may observe the scope as a dependency.
    let deleted = repository
        .commit_projection(scenario.batch(
            scenario.input(5, b"delete", "message-5", "cause-stable", initial),
            vec![scenario.mutation(
                ProjectionRecordExpectation::Exact(second.records[0].revision.clone()),
                RecordTransition::Delete,
            )],
            staged(),
        ))
        .await
        .unwrap();
    assert!(deleted.records[0].tombstone);
    assert!(!scenario.row_exists(&repository).await);
    assert_eq!(observation("cause-stable", record).await, Some(earliest));
    let recreated = repository
        .commit_projection(scenario.batch(
            scenario.input(6, b"recreate", "message-6", "cause-6", initial),
            vec![scenario.mutation(
                ProjectionRecordExpectation::Exact(deleted.records[0].revision.clone()),
                RecordTransition::Recreate,
            )],
            vec![ProjectionObservationRequest {
                kind: ProjectionObservationKind::Dependency,
                target: ProjectionObservationTarget::Dependency(scope.clone()),
            }],
        ))
        .await
        .unwrap();
    assert_eq!(recreated.records[0].revision.incarnation(), 2);
    assert_eq!(recreated.records[0].revision.revision(), 1);
    assert_eq!(recreated.changes.len(), 2);
    assert!(
        observation("cause-6", ProjectionObservationKind::Dependency)
            .await
            .unwrap()
            .revision
            .is_none()
    );
}

pub(crate) async fn causation_evidence_is_topology_allowlisted_and_repair_clears_terminal_failure(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    repository
        .commit_projection(scenario.batch(
            scenario.input(
                1,
                b"status-observation",
                "message-status-1",
                "cause-status",
                ProjectionGeneration::initial(),
            ),
            vec![create(&scenario, RECORD_ID)],
            vec![ProjectionObservationRequest {
                kind: ProjectionObservationKind::Record,
                target: ProjectionObservationTarget::StagedRecord(scope(&scenario, RECORD_ID)),
            }],
        ))
        .await
        .unwrap();

    let evidence = |topology| {
        let repository = &repository;
        async move {
            repository
                .projection_causation_evidence(
                    &ProjectionCausationEvidenceRequest::new("cause-status", vec![topology])
                        .unwrap(),
                )
                .await
                .unwrap()
        }
    };
    let observed = evidence(scenario.topology()).await;
    assert_eq!(observed.observations.len(), 1);
    assert!(observed.terminal_failure_topologies.is_empty());
    let filtered = evidence(scenario.other_topology()).await;
    assert!(filtered.observations.is_empty());
    assert!(filtered.terminal_failure_topologies.is_empty());

    let failure_id = scenario.failure_id("failure-status-2");
    repository
        .record_projection_failure(scenario.failure(
            scenario.input(
                2,
                b"status-failure",
                "message-status-2",
                "cause-status",
                ProjectionGeneration::initial(),
            ),
            &failure_id,
        ))
        .await
        .unwrap();
    assert_eq!(
        evidence(scenario.topology())
            .await
            .terminal_failure_topologies,
        vec![scenario.topology()]
    );
    assert!(evidence(scenario.other_topology())
        .await
        .terminal_failure_topologies
        .is_empty());

    repository
        .repair_projection(&scenario.topology(), &scenario.partition(), &failure_id)
        .await
        .unwrap();
    let repaired = evidence(scenario.topology()).await;
    assert_eq!(repaired.observations.len(), 1);
    assert!(repaired.terminal_failure_topologies.is_empty());
}

/// A terminal failure stops its partition for every input until that exact
/// failure is repaired; the repair generation then admits exactly the failed
/// input next, and the change log stays resumable throughout.
pub(crate) async fn failure_stops_the_partition_until_its_exact_repair(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    let scope = scope(&scenario, RECORD_ID);
    let initial = ProjectionGeneration::initial();
    let commit = |input| {
        let repository = &repository;
        let batch = scenario.batch(input, Vec::new(), Vec::new());
        async move { repository.commit_projection(batch).await }
    };
    let runtime_state = || {
        let repository = &repository;
        let scenario = &scenario;
        async move {
            repository
                .projection_partition_runtime_state(&scenario.topology(), &scenario.partition())
                .await
                .unwrap()
        }
    };
    assert_eq!(runtime_state().await, None);

    let first = repository
        .commit_projection(scenario.batch(
            scenario.input(1, b"dependency", "message-1", "cause-1", initial),
            Vec::new(),
            vec![ProjectionObservationRequest {
                kind: ProjectionObservationKind::Dependency,
                target: ProjectionObservationTarget::Dependency(scope.clone()),
            }],
        ))
        .await
        .unwrap();
    let observation = repository
        .projection_observation("cause-1", &scope, ProjectionObservationKind::Dependency)
        .await
        .unwrap()
        .unwrap();
    assert!(observation.revision.is_none());
    assert_eq!(observation.scope, scope);

    let failure_id = scenario.failure_id("failure-2");
    let failure_batch = scenario.failure(
        scenario.input(2, b"failure", "message-2", "cause-2", initial),
        &failure_id,
    );
    let failure = repository
        .record_projection_failure(failure_batch.clone())
        .await
        .unwrap();
    assert_eq!(
        repository
            .record_projection_failure(failure_batch.clone())
            .await
            .unwrap(),
        failure
    );
    assert!(failure.gap_free);
    let changed_capability = scenario.failure(
        mint(
            scenario.input_cursor(2),
            b"failure",
            "message-2",
            "cause-2",
            initial,
            false,
        ),
        &failure_id,
    );
    assert!(matches!(
        repository
            .record_projection_failure(changed_capability)
            .await,
        Err(ProjectionProtocolError::InputCorruption)
    ));
    assert_eq!(
        repository
            .projection_failure(&scenario.topology(), &scenario.partition(), &failure_id)
            .await
            .unwrap(),
        Some(failure.clone())
    );
    let stopped = runtime_state().await.unwrap();
    assert_eq!(stopped.active_generation, initial);
    assert_eq!(stopped.stopped_failure_id.as_ref(), Some(&failure_id));
    assert_eq!(stopped.pending_retry, None);

    // While stopped, identity violations are still reported as what they
    // are; everything else is told the partition is stopped.
    assert!(matches!(
        commit(scenario.input(3, b"blocked", "message-3", "cause-3", initial)).await,
        Err(ProjectionProtocolError::PartitionStopped { .. })
    ));
    assert!(matches!(
        commit(scenario.input(
            2,
            b"changed-while-stopped",
            "changed-message-2",
            "cause-2",
            initial
        ))
        .await,
        Err(ProjectionProtocolError::InputCorruption)
    ));
    assert!(matches!(
        commit(scenario.input(
            3,
            b"reused-message-while-stopped",
            "message-2",
            "cause-3",
            initial
        ))
        .await,
        Err(ProjectionProtocolError::MessageIdReuse { .. })
    ));
    assert!(matches!(
        commit(scenario.input(2, b"failure", "message-2", "cause-2", initial)).await,
        Err(ProjectionProtocolError::PartitionStopped { .. })
    ));

    let generation = repository
        .repair_projection(&scenario.topology(), &scenario.partition(), &failure_id)
        .await
        .unwrap();
    assert_eq!(generation.get(), 2);
    let repaired = runtime_state().await.unwrap();
    assert_eq!(repaired.active_generation, generation);
    assert_eq!(repaired.stopped_failure_id, None);
    let retry = repaired.pending_retry.unwrap();
    assert_eq!(retry.failure_id, failure_id);
    assert_eq!(retry.input, failure.input);
    assert_eq!(retry.input_fingerprint, failure.input_fingerprint);
    assert_eq!(retry.message_id, failure.message_id);
    assert_eq!(retry.causation_id, failure.causation_id);
    assert_eq!(retry.failed_generation, failure.generation);
    assert_eq!(retry.gap_free, failure.gap_free);

    // The old generation is fenced.
    assert!(matches!(
        repository.record_projection_failure(failure_batch).await,
        Err(ProjectionProtocolError::GenerationFenced {
            expected: 2,
            actual: 1
        })
    ));
    assert!(matches!(
        commit(scenario.input(
            2,
            b"changed-old-generation",
            "changed-old-message-2",
            "cause-2",
            initial
        ))
        .await,
        Err(ProjectionProtocolError::InputCorruption)
    ));
    assert!(matches!(
        commit(scenario.input(
            4,
            b"unknown-old-generation",
            "unknown-old-message-4",
            "unknown-old-cause-4",
            initial
        ))
        .await,
        Err(ProjectionProtocolError::GenerationFenced {
            expected: 2,
            actual: 1
        })
    ));
    assert!(matches!(
        commit(mint(
            scenario.input_cursor(4),
            b"changed-old-capability",
            "changed-old-capability-message",
            "changed-old-capability-cause",
            initial,
            false,
        ))
        .await,
        Err(ProjectionProtocolError::InputCorruption)
    ));

    // The new generation inherits the history and admits the retry first.
    assert_eq!(
        commit(scenario.input(1, b"dependency", "message-1", "cause-1", generation))
            .await
            .unwrap()
            .outcome,
        ProjectionCommitOutcome::Duplicate
    );
    assert_eq!(
        commit(scenario.input(0, b"older", "message-0", "cause-0", generation))
            .await
            .unwrap()
            .outcome,
        ProjectionCommitOutcome::StaleInput
    );
    assert!(matches!(
        commit(scenario.input(3, b"later", "message-3", "cause-3", generation)).await,
        Err(ProjectionProtocolError::IncomparableInput)
    ));
    assert!(matches!(
        commit(scenario.input(2, b"retry", "message-2b", "cause-2", generation)).await,
        Err(ProjectionProtocolError::InputCorruption)
    ));
    let repaired = commit(scenario.input(2, b"failure", "message-2", "cause-2", generation))
        .await
        .unwrap();
    assert_eq!(repaired.changes[0].kind, ProjectionChangeKind::Checkpoint);
    let later = commit(scenario.input(3, b"later", "message-3", "cause-3", generation))
        .await
        .unwrap();
    assert_eq!(later.outcome, ProjectionCommitOutcome::Applied);

    let read = |after: ProjectionChangeCursor| {
        let repository = &repository;
        let scenario = &scenario;
        async move {
            repository
                .projection_changes(
                    &scenario.topology(),
                    &scenario.partition(),
                    Some(&after),
                    100,
                )
                .await
                .unwrap()
        }
    };
    let boundary = first.changes[0].cursor.clone();
    let compacted = repository
        .compact_projection_changes(&boundary)
        .await
        .unwrap();
    assert_eq!(compacted, boundary.position());
    assert!(matches!(
        all_changes(&scenario, &repository).await,
        ProjectionChangeRead::ResetRequired {
            compacted_through,
            ..
        } if compacted_through == compacted
    ));
    // The failure, the repaired checkpoint and the later checkpoint.
    assert!(matches!(
        read(boundary.clone()).await,
        ProjectionChangeRead::Changes {
            compacted_through,
            ref changes,
            ..
        } if compacted_through == compacted && changes.len() == 3
    ));

    repository
        .compact_projection_changes(&failure.change)
        .await
        .unwrap();
    assert!(matches!(
        read(boundary).await,
        ProjectionChangeRead::ResetRequired { .. }
    ));
    let future = scenario
        .change_cursor(&repository, later.changes[0].cursor.position() + 10)
        .await;
    assert!(matches!(
        read(future).await,
        ProjectionChangeRead::ResetRequired { .. }
    ));
}

/// A failure batch whose digest does not cover its bytes is refused before
/// anything is written.
pub(crate) async fn tampered_failure_digest_is_rejected_before_any_write(
    scenario: impl ProjectionProtocolScenario,
) {
    let repository = scenario.repository().await;
    let input = scenario.input(
        1,
        b"tampered-failure",
        "tampered-failure-message",
        "tampered-failure-cause",
        ProjectionGeneration::initial(),
    );
    let failure_id = scenario.failure_id("tampered-failure-id");
    for tamper in [
        |digest: &mut [u8; 32]| *digest = [0; 32],
        |digest: &mut [u8; 32]| digest[0] ^= 0xff,
    ] {
        let mut failure = scenario.failure(input.clone(), &failure_id);
        tamper(&mut failure.failure_digest);
        assert!(matches!(
            repository.record_projection_failure(failure).await,
            Err(ProjectionProtocolError::InvalidBatch(message))
                if message == "projection failure digest does not match its exact bytes"
        ));
    }
    assert_eq!(
        repository
            .projection_failure(&scenario.topology(), &scenario.partition(), &failure_id)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        repository
            .projection_partition_runtime_state(&scenario.topology(), &scenario.partition())
            .await
            .unwrap(),
        None
    );
    assert_input_left_no_trace(&repository, &input).await;
    assert!(scenario
        .stored_change_positions(&repository)
        .await
        .is_empty());
}

/// A table a topology owns takes no write that bypasses the protocol, before
/// or after its first record.
pub(crate) async fn owned_table_rejects_every_raw_write(scenario: impl ProjectionProtocolScenario) {
    let repository = scenario.repository().await;
    let table = scenario.schema().table_name.as_str();
    let raw_plan = || TableWritePlan::new(vec![create(&scenario, RECORD_ID).mutation]);
    assert!(matches!(
        repository.commit_write_plan(raw_plan()).await,
        Err(TableStoreError::CausalWriteRequired { table: ref refused }) if refused == table
    ));
    repository
        .commit_projection(scenario.batch(
            scenario.input(
                1,
                b"claim",
                "message-1",
                "cause-1",
                ProjectionGeneration::initial(),
            ),
            vec![create(&scenario, RECORD_ID)],
            Vec::new(),
        ))
        .await
        .unwrap();
    assert!(matches!(
        repository.commit_write_plan(raw_plan()).await,
        Err(TableStoreError::CausalWriteRequired { table: ref refused }) if refused == table
    ));
}

/// Two deliveries of one input racing each other apply it exactly once.
pub(crate) async fn concurrent_same_input_applies_once(scenario: impl ProjectionProtocolScenario) {
    let repository = scenario.repository().await;
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let mut deliveries = Vec::new();
    for _ in 0..2 {
        let repository = repository.clone();
        let barrier = Arc::clone(&barrier);
        let batch = scenario.batch(
            scenario.input(
                1,
                b"same-input",
                "message-1",
                "cause-1",
                ProjectionGeneration::initial(),
            ),
            vec![create(&scenario, RECORD_ID)],
            Vec::new(),
        );
        deliveries.push(tokio::spawn(async move {
            barrier.wait().await;
            repository.commit_projection(batch).await.unwrap().outcome
        }));
    }
    let mut outcomes = Vec::new();
    for delivery in deliveries {
        outcomes.push(delivery.await.unwrap());
    }
    outcomes.sort_by_key(|outcome| *outcome != ProjectionCommitOutcome::Applied);
    assert_eq!(
        outcomes,
        [
            ProjectionCommitOutcome::Applied,
            ProjectionCommitOutcome::Duplicate
        ]
    );
    assert_eq!(
        repository
            .projection_record(&scope(&scenario, RECORD_ID))
            .await
            .unwrap()
            .unwrap()
            .revision
            .revision(),
        1
    );
}

/// Run every scenario of this suite as a test in the invoking module.
/// `$scenario` is an expression (it may `.await`) yielding an
/// `Option<impl ProjectionProtocolScenario>`; `None` skips the test, for a
/// store whose service is not configured.
macro_rules! projection_protocol_scenario_tests {
    ($scenario:expr) => {
        $crate::projection_protocol::scenario_tests::projection_protocol_scenario_tests!(
            $scenario;
            input_disposition_is_read_only_exact_and_repair_fenced,
            identical_redelivery_advances_only_broker_checkpoint,
            message_identity_is_topology_wide_across_projection_partitions,
            failure_recording_is_idempotent_for_exact_batch,
            registered_table_ownership_rejects_other_topology,
            tombstone_requires_explicit_exact_recreation,
            existing_record_observation_rejects_an_unfenced_tombstone,
            existing_record_observation_confirms_a_source_fenced_tombstone,
            causation_evidence_is_ordered_by_canonical_scope_bytes,
            another_partition_cannot_create_an_owned_row,
            source_capability_mismatch_outranks_message_id_reuse,
            a_new_partition_mints_its_change_epoch_once,
            an_existing_partition_keeps_the_change_epoch_it_has,
            query_snapshot_never_mixes_row_revision_checkpoint_or_resume_head,
            execution_snapshot_echoes_every_requested_scope_across_tables,
            one_four_table_occurrence_commits_rows_and_every_causal_artifact,
            commit_failure_at_each_position_rolls_back_every_side_effect,
            table_failure_rolls_back_rows_protocol_and_inbox,
            physical_row_drift_is_rejected_without_side_effects,
            change_retention_compacts_success_and_failure_prefixes,
            lengthening_retention_never_restores_a_compacted_prefix,
            retention_failure_rolls_back_rows_protocol_and_inbox,
            change_page_with_a_hole_is_rejected,
            cursor_fences_are_exact_and_non_mutating,
            message_receipts_and_gap_capability_are_immutable,
            first_failure_registers_the_source_for_its_repair,
            repair_generation_retries_only_the_exact_failed_input,
            a_retry_that_fails_again_stops_the_partition,
            non_gap_repair_requires_failed_cursor_before_later_input,
            observations_are_immutable_and_staged_records_reuse_row_changes,
            causation_evidence_is_topology_allowlisted_and_repair_clears_terminal_failure,
            failure_stops_the_partition_until_its_exact_repair,
            tampered_failure_digest_is_rejected_before_any_write,
            owned_table_rejects_every_raw_write,
            concurrent_same_input_applies_once,
        );
    };
    ($scenario:expr; $($name:ident),* $(,)?) => {
        $(
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $name() {
                let Some(scenario) = $scenario else {
                    return;
                };
                $crate::projection_protocol::scenario_tests::$name(scenario).await;
            }
        )*
    };
}
pub(crate) use projection_protocol_scenario_tests;
