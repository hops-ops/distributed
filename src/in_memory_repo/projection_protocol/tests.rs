use std::sync::LazyLock;
use std::time::Duration;

use super::*;
use crate::command_ledger::{
    CanonicalInputHash, CausalCommitBatch, CausalTransactionalCommit, CommandContractFingerprint,
    CommandId, CommandLedgerError, CommandLedgerKey, CommandLedgerStore, CommandReservation,
    PrincipalPartitionId, ReservationOutcome, TerminalCommandState,
};
use crate::projection_protocol::{
    ProjectionInputFingerprint, ProjectionModelOwnership, ProjectionObservationRequest,
    ProjectionQuerySnapshotRequest, ProjectionRecordMutation, ProjectionScopeCodec,
    TrustedProjectionInput,
};
use crate::repository::{CommitBatch, ReadModelWritePlanStore};
use crate::table::{
    ColumnType, DeleteTableRowMutation, ExpectedVersion, PrimaryKey, RowKey, RowValue, RowValues,
    RowWriteMode, TableColumn, TableKind, TableMutation, TableRowMutation, TableSchema,
    TableStoreError,
};

fn topology() -> ProjectorTopologyId {
    ProjectorTopologyId::new(1, "todo_projector", [7; 32]).unwrap()
}

fn other_topology() -> ProjectorTopologyId {
    ProjectorTopologyId::new(1, "other_projector", [8; 32]).unwrap()
}

fn partition() -> PartitionId {
    ProjectionScopeCodec::new(topology())
        .encode_partition(Some(&serde_json::json!("tenant-a")))
        .unwrap()
}

fn source() -> ProjectionSource {
    ProjectionSource::new("todo_stream", b"todo-1".to_vec()).unwrap()
}

fn input_cursor(position: u64, source_epoch: &str) -> ProjectionInputCursor {
    ProjectionInputCursor::new(
        topology(),
        partition(),
        source(),
        ProjectionEpoch::new(source_epoch).unwrap(),
        position,
    )
    .unwrap()
}

fn input(
    position: u64,
    fingerprint: &[u8],
    message_id: &str,
    causation_id: &str,
    generation: ProjectionGeneration,
) -> TrustedProjectionInput {
    TrustedProjectionInput::mint(
        input_cursor(position, "source-v1"),
        ProjectionInputFingerprint::from_canonical_bytes(fingerprint),
        message_id,
        causation_id,
        generation,
        true,
    )
    .unwrap()
}

fn schema() -> &'static TableSchema {
    static SCHEMA: LazyLock<TableSchema> = LazyLock::new(|| TableSchema {
        model_name: "TodoView".into(),
        table_name: "todo_views".into(),
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
        version_column: None,
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    });
    &SCHEMA
}

fn scope_codec() -> ProjectionScopeCodec {
    ProjectionScopeCodec::with_models(topology(), [("TodoView", schema())]).unwrap()
}

fn record_key() -> RowKey {
    RowKey::new([("id", RowValue::String("todo-1".into()))])
}

fn record_scope() -> ProjectionRecordScope {
    scope_codec()
        .encode_row_scope_in_partition("TodoView", partition(), &record_key())
        .unwrap()
}

fn ownership() -> ProjectionModelOwnership {
    ProjectionModelOwnership::new("TodoView", "todo_views").unwrap()
}

/// The shared conformance suite on a fresh in-memory repository.
struct ProjectionScenario;

impl ProjectionScenario {
    /// Where the read-model store keeps row `id` of the scenario table.
    fn row_key(id: &str) -> String {
        TableMutation::DeleteRow(DeleteTableRowMutation {
            schema: schema(),
            key: RowKey::new([("id", RowValue::String(id.into()))]),
            expected_version: ExpectedVersion::Any,
        })
        .lock_key()
    }

    fn with_changes<T>(
        &self,
        repository: &InMemoryRepository,
        read: impl FnOnce(&mut BTreeMap<u64, ProjectionChange>) -> T,
    ) -> T {
        use crate::projection_protocol::scenario_tests::ProjectionProtocolScenario;
        let mut protocol = repository.projection_protocol.write().unwrap();
        let partition = protocol
            .partitions
            .get_mut(&PartitionKey::new(&topology(), &self.partition()));
        read(partition.map_or(&mut BTreeMap::new(), |partition| &mut partition.changes))
    }
}

impl crate::projection_protocol::scenario_tests::ProjectionProtocolScenario for ProjectionScenario {
    type Store = InMemoryRepository;

    async fn store(&self, schemas: &[&'static TableSchema]) -> Self::Store {
        let repository = InMemoryRepository::new();
        let owners = schemas
            .iter()
            .map(|schema| {
                ProjectionModelOwnership::new(&schema.model_name, &schema.table_name).unwrap()
            })
            .collect::<Vec<_>>();
        repository
            .register_projection_models(&topology(), &owners)
            .await
            .unwrap();
        repository
    }

    fn with_retention(
        &self,
        store: Self::Store,
        retention: ProjectionChangeRetention,
    ) -> Self::Store {
        store.with_projection_change_retention(retention)
    }

    fn topology(&self) -> ProjectorTopologyId {
        topology()
    }

    fn other_topology(&self) -> ProjectorTopologyId {
        other_topology()
    }

    fn schema(&self) -> &'static TableSchema {
        schema()
    }

    async fn physical_row(&self, store: &Self::Store, id: &str) -> Option<(Option<String>, u64)> {
        let rows = store.model_store.relational_rows.read().unwrap();
        rows.get(&Self::row_key(id)).map(|row| {
            let value = match row.values.get("value") {
                Some(RowValue::String(value)) => Some(value.clone()),
                _ => None,
            };
            (value, row.version)
        })
    }

    async fn insert_physical_row(&self, store: &Self::Store, id: &str) {
        let mut values = RowValues::new();
        values.insert("id", RowValue::String(id.into()));
        apply_read_model_write_plan(
            TableWritePlan::new(vec![TableMutation::UpsertRow(TableRowMutation {
                schema: schema(),
                key: RowKey::new([("id", RowValue::String(id.into()))]),
                values,
                expected_version: ExpectedVersion::Any,
                mode: RowWriteMode::Upsert,
            })]),
            &mut store.model_store.relational_rows.write().unwrap(),
        )
        .unwrap();
    }

    async fn remove_physical_row(&self, store: &Self::Store, id: &str) {
        let mut rows = store.model_store.relational_rows.write().unwrap();
        rows.remove(&Self::row_key(id)).unwrap();
    }

    /// The in-memory table has no trigger to arm; a row at the last
    /// representable version refuses every further write. The rows the
    /// suite arms were written exactly once.
    async fn fail_writes_to_row(&self, store: &Self::Store, id: &str, fail: bool) {
        let mut rows = store.model_store.relational_rows.write().unwrap();
        rows.get_mut(&Self::row_key(id)).unwrap().version = if fail {
            u64::MAX
        } else {
            crate::read_model::in_memory::INITIAL_MODEL_VERSION
        };
    }

    async fn remove_stored_change(&self, store: &Self::Store, position: u64) {
        self.with_changes(store, |changes| changes.remove(&position))
            .unwrap();
    }

    async fn stored_change_epoch(&self, store: &Self::Store) -> Option<ProjectionEpoch> {
        let protocol = store.projection_protocol.read().unwrap();
        protocol
            .partitions
            .get(&PartitionKey::new(&topology(), &self.partition()))
            .map(|partition| partition.change_epoch.clone())
    }

    async fn insert_partition(&self, store: &Self::Store, epoch: &ProjectionEpoch) {
        store.insert_projection_partition(&topology(), &self.partition(), epoch);
    }

    async fn stored_change_positions(&self, store: &Self::Store) -> Vec<u64> {
        self.with_changes(store, |changes| changes.keys().copied().collect())
    }
}

mod scenarios {
    crate::projection_protocol::scenario_tests::projection_protocol_scenario_tests!(Some(
        super::ProjectionScenario
    ));
}

async fn repository() -> InMemoryRepository {
    repository_with_retention(ProjectionChangeRetention::default()).await
}

async fn repository_with_retention(retention: ProjectionChangeRetention) -> InMemoryRepository {
    let repository = InMemoryRepository::new().with_projection_change_retention(retention);
    repository
        .register_projection_models(&topology(), &[ownership()])
        .await
        .unwrap();
    repository
}

fn upsert_table_mutation(valid: bool) -> TableMutation {
    let key = record_key();
    let values = if valid {
        let mut values = RowValues::new();
        values.insert("id", RowValue::String("todo-1".into()));
        values
    } else {
        RowValues::new()
    };
    TableMutation::UpsertRow(TableRowMutation {
        schema: schema(),
        key,
        values,
        expected_version: ExpectedVersion::Any,
        mode: RowWriteMode::Upsert,
    })
}

fn mutation(
    expectation: ProjectionRecordExpectation,
    kind: RecordTransition,
) -> ProjectionRecordMutation {
    let table = match kind {
        RecordTransition::Delete => TableMutation::DeleteRow(DeleteTableRowMutation {
            schema: schema(),
            key: RowKey::new([("id", RowValue::String("todo-1".into()))]),
            expected_version: ExpectedVersion::Any,
        }),
        RecordTransition::Upsert | RecordTransition::Recreate => upsert_table_mutation(true),
    };
    ProjectionRecordMutation::new(record_scope(), table, expectation, kind).unwrap()
}

fn batch(
    trusted: TrustedProjectionInput,
    mutations: Vec<ProjectionRecordMutation>,
    observations: Vec<ProjectionObservationRequest>,
) -> ProjectionCommitBatch {
    ProjectionCommitBatch {
        input: trusted,
        ownership: vec![ownership()],
        mutations,
        observations,
    }
}

fn row_exists(repository: &InMemoryRepository) -> bool {
    repository
        .model_store
        .relational_rows
        .read()
        .unwrap()
        .contains_key(&upsert_table_mutation(true).lock_key())
}

fn direct_batch(causation_id: &str) -> SameTransactionProjectionBatch {
    SameTransactionProjectionBatch::single_upsert(
        topology(),
        partition(),
        ownership(),
        record_scope(),
        upsert_table_mutation(true),
        causation_id,
    )
    .unwrap()
}

fn insert_physical_row(repository: &InMemoryRepository) {
    let mut rows = repository.model_store.relational_rows.write().unwrap();
    apply_read_model_write_plan(
        TableWritePlan::new(vec![upsert_table_mutation(true)]),
        &mut rows,
    )
    .unwrap();
}

fn remove_physical_row(repository: &InMemoryRepository) {
    repository
        .model_store
        .relational_rows
        .write()
        .unwrap()
        .remove(&upsert_table_mutation(true).lock_key());
}

#[test]
fn query_snapshot_probe_bounds_are_explicit() {
    let probes = (0..crate::projection_protocol::MAX_PROJECTION_QUERY_CHECKPOINT_PROBES)
        .map(|index| {
            crate::projection_protocol::ProjectionCheckpointProbe::new(
                topology(),
                partition(),
                ProjectionSource::new(
                    format!("source-{index}"),
                    format!("partition-{index}").into_bytes(),
                )
                .unwrap(),
                ProjectionEpoch::new("source-v1").unwrap(),
                ProjectionGeneration::initial(),
            )
        })
        .collect::<Vec<_>>();
    assert!(ProjectionQuerySnapshotRequest::new(
        &scope_codec(),
        Some(&serde_json::json!("tenant-a")),
        "TodoView",
        record_key(),
        probes.clone(),
    )
    .is_ok());
    let mut over_limit = probes;
    over_limit.push(crate::projection_protocol::ProjectionCheckpointProbe::new(
        topology(),
        partition(),
        ProjectionSource::new("source-over-limit", b"over-limit".to_vec()).unwrap(),
        ProjectionEpoch::new("source-v1").unwrap(),
        ProjectionGeneration::initial(),
    ));
    assert!(matches!(
        ProjectionQuerySnapshotRequest::new(
            &scope_codec(),
            Some(&serde_json::json!("tenant-a")),
            "TodoView",
            record_key(),
            over_limit,
        ),
        Err(ProjectionProtocolError::InvalidBatch(message))
            if message.contains("maximum")
    ));
}

#[tokio::test]
async fn direct_projection_is_fenced_while_exact_failure_repair_is_pending() {
    let repository = repository().await;
    assert_eq!(
        repository
            .projection_partition_runtime_state(&topology(), &partition())
            .await
            .unwrap(),
        None
    );
    let failure = repository
        .record_projection_failure(
            ProjectionFailureBatch::new(
                input(
                    0,
                    b"failed",
                    "failed-message",
                    "failed-cause",
                    ProjectionGeneration::initial(),
                ),
                "failure-1",
                "decode_error",
                b"bad payload".to_vec(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(failure.gap_free);
    let stopped = repository
        .projection_partition_runtime_state(&topology(), &partition())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stopped.active_generation, ProjectionGeneration::initial());
    assert_eq!(stopped.stopped_failure_id.as_deref(), Some("failure-1"));
    assert_eq!(stopped.pending_retry, None);
    repository
        .repair_projection(&topology(), &partition(), "failure-1")
        .await
        .unwrap();
    let repaired = repository
        .projection_partition_runtime_state(&topology(), &partition())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(repaired.active_generation.get(), 2);
    assert_eq!(repaired.stopped_failure_id, None);
    let retry = repaired.pending_retry.unwrap();
    assert_eq!(retry.failure_id, "failure-1");
    assert_eq!(retry.input, failure.input);
    assert_eq!(retry.input_fingerprint, failure.input_fingerprint);
    assert_eq!(retry.message_id, failure.message_id);
    assert_eq!(retry.causation_id, failure.causation_id);
    assert_eq!(retry.failed_generation, failure.generation);
    assert_eq!(retry.gap_free, failure.gap_free);

    let mut staged_protocol = repository.projection_protocol.read().unwrap().clone();
    let mut staged_rows = repository
        .model_store
        .relational_rows
        .read()
        .unwrap()
        .clone();
    assert!(matches!(
        stage_same_transaction_projection(
            &mut staged_protocol,
            &mut staged_rows,
            &direct_batch("direct-during-repair"),
            repository.projection_change_retention,
        ),
        Err(ProjectionProtocolError::IncomparableInput)
    ));

    let partition_key = PartitionKey::new(&topology(), &partition());
    let partition = staged_protocol.partitions.get(&partition_key).unwrap();
    assert_eq!(partition.change_head, 1);
    assert_eq!(partition.compacted_through, 0);
    assert_eq!(
        partition.pending_retry_failure_id.as_deref(),
        Some("failure-1")
    );
    assert!(staged_protocol.ownership.is_empty());
    assert!(staged_protocol.records.is_empty());
    assert!(staged_protocol.observations.is_empty());
    assert!(staged_rows.is_empty());
}

#[tokio::test]
async fn direct_projection_reports_typed_physical_metadata_drift() {
    let orphan_row = repository().await;
    insert_physical_row(&orphan_row);
    let mut staged_protocol = orphan_row.projection_protocol.read().unwrap().clone();
    let mut staged_rows = orphan_row
        .model_store
        .relational_rows
        .read()
        .unwrap()
        .clone();
    assert!(matches!(
        stage_same_transaction_projection(
            &mut staged_protocol,
            &mut staged_rows,
            &direct_batch("direct-orphan-row"),
            orphan_row.projection_change_retention,
        ),
        Err(ProjectionProtocolError::RecordAlreadyExists { model })
            if model == "TodoView"
    ));
    assert!(orphan_row
        .projection_record(&record_scope())
        .await
        .unwrap()
        .is_none());
    assert!(row_exists(&orphan_row));

    let missing_row = repository().await;
    let metadata = missing_row
        .commit_projection(batch(
            input(
                0,
                b"create",
                "create-message",
                "create-cause",
                ProjectionGeneration::initial(),
            ),
            vec![mutation(
                ProjectionRecordExpectation::Missing,
                RecordTransition::Upsert,
            )],
            Vec::new(),
        ))
        .await
        .unwrap()
        .records
        .remove(0);
    remove_physical_row(&missing_row);
    let mut staged_protocol = missing_row.projection_protocol.read().unwrap().clone();
    let mut staged_rows = missing_row
        .model_store
        .relational_rows
        .read()
        .unwrap()
        .clone();
    assert!(matches!(
        stage_same_transaction_projection(
            &mut staged_protocol,
            &mut staged_rows,
            &direct_batch("direct-missing-row"),
            missing_row.projection_change_retention,
        ),
        Err(ProjectionProtocolError::RecordMissing { model })
            if model == "TodoView"
    ));
    assert_eq!(
        missing_row
            .projection_record(&record_scope())
            .await
            .unwrap(),
        Some(metadata)
    );
    assert!(!row_exists(&missing_row));
}

#[tokio::test]
async fn stale_direct_attempt_is_fenced_before_projection_drift_is_inspected() {
    let repository = repository().await;
    let retention = Duration::from_secs(3_600);
    let command_key = CommandLedgerKey::new(
        "in-memory-direct-precedence",
        PrincipalPartitionId::new("tenant:direct-precedence").unwrap(),
        CommandId::parse(uuid::Uuid::now_v7().hyphenated().to_string()).unwrap(),
    )
    .unwrap();
    let reservation = CommandReservation::new(
        command_key,
        "project-todo",
        CommandContractFingerprint::new([41; 32]),
        CanonicalInputHash::new([42; 32]),
        Duration::from_secs(30),
        retention,
    )
    .unwrap();
    let attempt = match repository.reserve_command(reservation).await.unwrap() {
        ReservationOutcome::Acquired(attempt) => attempt,
        _ => panic!("a fresh direct command must acquire its first attempt"),
    };
    let direct = direct_batch(attempt.causation_id().as_str());
    let completion = attempt
        .complete(
            TerminalCommandState::Atomic,
            serde_json::json!({"projected": "must-not-run"}),
            retention,
        )
        .unwrap();
    repository
        .mark_retryable_unknown(completion.attempt_fence())
        .await
        .unwrap();

    // This orphan physical row would produce RecordAlreadyExists if the
    // direct projection inspected protocol state before the stale attempt.
    insert_physical_row(&repository);
    assert!(matches!(
        repository
            .commit_causal_batch(CausalCommitBatch::with_direct_projection(
                CommitBatch::empty(),
                completion,
                direct,
            ))
            .await,
        Err(CommandLedgerError::AttemptFenced { .. })
    ));
    assert!(row_exists(&repository));
    assert!(repository
        .projection_record(&record_scope())
        .await
        .unwrap()
        .is_none());
    assert!(repository
        .projection_protocol
        .read()
        .unwrap()
        .partitions
        .is_empty());
}

#[tokio::test]
async fn direct_projection_retention_preserves_durable_evidence() {
    let repository = repository_with_retention(ProjectionChangeRetention::new(1).unwrap()).await;
    let mut staged_protocol = repository.projection_protocol.read().unwrap().clone();
    let mut staged_rows = HashMap::new();

    let first = stage_same_transaction_projection(
        &mut staged_protocol,
        &mut staged_rows,
        &direct_batch("direct-1"),
        repository.projection_change_retention,
    )
    .unwrap();
    let second = stage_same_transaction_projection(
        &mut staged_protocol,
        &mut staged_rows,
        &direct_batch("direct-2"),
        repository.projection_change_retention,
    )
    .unwrap();

    let partition = staged_protocol
        .partitions
        .get(&PartitionKey::new(&topology(), &partition()))
        .unwrap();
    assert_eq!(partition.change_head, 2);
    assert_eq!(partition.compacted_through, 1);
    assert_eq!(
        partition.changes.keys().copied().collect::<Vec<_>>(),
        vec![2]
    );
    let change_cursor = |position| {
        ProjectionChangeCursor::new(
            topology(),
            self::partition(),
            partition.change_epoch.clone(),
            position,
        )
        .unwrap()
    };
    assert_eq!(first.changes[0].cursor, change_cursor(1));
    assert_eq!(second.changes[0].cursor, change_cursor(2));
    assert_eq!(
        staged_protocol
            .records
            .get(&record_scope())
            .unwrap()
            .revision
            .revision(),
        2
    );
    assert!(staged_protocol.observations.contains_key(&ObservationKey {
        causation_id: "direct-1".into(),
        scope: record_scope(),
        kind: ProjectionObservationKind::Record,
    }));
    assert!(staged_protocol.observations.contains_key(&ObservationKey {
        causation_id: "direct-2".into(),
        scope: record_scope(),
        kind: ProjectionObservationKind::Record,
    }));
    assert!(staged_rows.contains_key(&upsert_table_mutation(true).lock_key()));
}

#[tokio::test]
async fn topology_bootstrap_is_required_for_noop_success_and_failure() {
    let repository = InMemoryRepository::new();
    let noop = ProjectionCommitBatch {
        input: input(
            0,
            b"noop",
            "message-noop",
            "cause-noop",
            ProjectionGeneration::initial(),
        ),
        ownership: Vec::new(),
        mutations: Vec::new(),
        observations: Vec::new(),
    };
    assert!(matches!(
        repository.commit_projection(noop).await,
        Err(ProjectionProtocolError::InvalidBatch(message))
            if message.contains("not bootstrapped")
    ));
    let failure = ProjectionFailureBatch::new(
        input(
            0,
            b"failure",
            "message-failure",
            "cause-failure",
            ProjectionGeneration::initial(),
        ),
        "failure-unbootstrapped",
        "decode_error",
        b"bad payload".to_vec(),
    )
    .unwrap();
    assert!(matches!(
        repository.record_projection_failure(failure).await,
        Err(ProjectionProtocolError::InvalidBatch(message))
            if message.contains("not bootstrapped")
    ));
    assert!(repository
        .projection_protocol
        .read()
        .unwrap()
        .partitions
        .is_empty());
    assert!(repository.inbox_store.read().unwrap().is_empty());

    assert!(matches!(
        repository.register_projection_models(&topology(), &[]).await,
        Err(ProjectionProtocolError::InvalidBatch(message))
            if message.contains("at least one model/table owner")
    ));
    repository
        .register_projection_models(&topology(), &[ownership()])
        .await
        .unwrap();
    let applied = repository
        .commit_projection(batch(
            input(
                0,
                b"noop",
                "message-noop",
                "cause-noop",
                ProjectionGeneration::initial(),
            ),
            Vec::new(),
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(applied.outcome, ProjectionCommitOutcome::Applied);
    repository
        .record_projection_failure(
            ProjectionFailureBatch::new(
                input(
                    1,
                    b"failure",
                    "message-failure",
                    "cause-failure",
                    ProjectionGeneration::initial(),
                ),
                "failure-bootstrapped",
                "decode_error",
                b"bad payload".to_vec(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn registration_is_global_and_rejects_unowned_rows_atomically() {
    let repository = InMemoryRepository::new();
    repository
        .model_store()
        .commit_write_plan(TableWritePlan::new(vec![upsert_table_mutation(true)]))
        .await
        .unwrap();
    assert!(matches!(
        repository
            .register_projection_models(&topology(), &[ownership()])
            .await,
        Err(ProjectionProtocolError::InvalidBatch(message))
            if message.contains("contains rows without causal metadata")
    ));
    {
        let protocol = repository.projection_protocol.read().unwrap();
        assert!(!protocol.registered_topologies.contains(&topology()));
        assert!(protocol.registered_models.is_empty());
        assert!(protocol.authoritative_table_owners.is_empty());
    }
    assert!(!repository
        .causal_tables
        .read()
        .unwrap()
        .contains("todo_views"));
    repository
        .model_store()
        .commit_write_plan(TableWritePlan::new(vec![upsert_table_mutation(true)]))
        .await
        .unwrap();

    let clean = InMemoryRepository::new();
    clean
        .register_projection_models(&topology(), &[ownership()])
        .await
        .unwrap();
    assert!(matches!(
        clean
            .register_projection_models(&other_topology(), &[ownership()])
            .await,
        Err(ProjectionProtocolError::InvalidBatch(message))
            if message.contains("authoritative owner")
    ));
    let protocol = clean.projection_protocol.read().unwrap();
    assert!(!protocol.registered_topologies.contains(&other_topology()));
    assert_eq!(
        protocol
            .authoritative_table_owners
            .get("todo_views")
            .map(|owner| (&owner.topology, owner.model.as_str())),
        Some((&topology(), "TodoView"))
    );
}

/// What the shared repair scenario cannot see through the store's API: the
/// stored lineage of the repair generation, and that a duplicate inherited
/// from the failed generation writes no receipt of its own.
#[tokio::test]
async fn repair_stores_generation_lineage_and_an_inherited_duplicate_writes_no_receipt() {
    let repository = repository().await;
    let initial = ProjectionGeneration::initial();
    repository
        .commit_projection(batch(
            input(1, b"one", "message-1", "cause-1", initial),
            Vec::new(),
            Vec::new(),
        ))
        .await
        .unwrap();
    repository
        .record_projection_failure(
            ProjectionFailureBatch::new(
                input(2, b"two", "message-2", "cause-2", initial),
                "failure-2",
                "decode_error",
                b"bad payload".to_vec(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let generation = repository
        .repair_projection(&topology(), &partition(), "failure-2")
        .await
        .unwrap();
    let partition_key = PartitionKey::new(&topology(), &partition());
    {
        let protocol = repository.projection_protocol.read().unwrap();
        assert_eq!(
            protocol.generations.get(&GenerationKey {
                partition: partition_key.clone(),
                generation,
            }),
            Some(&GenerationLineage {
                retry_of_generation: Some(initial),
                retry_of_failure_id: Some("failure-2".into()),
            })
        );
        assert_eq!(
            protocol.partitions[&partition_key]
                .pending_retry_failure_id
                .as_deref(),
            Some("failure-2")
        );
    }

    let inherited = repository
        .commit_projection(batch(
            input(1, b"one", "message-1", "cause-1", generation),
            Vec::new(),
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(inherited.outcome, ProjectionCommitOutcome::Duplicate);
    repository
        .commit_projection(batch(
            input(2, b"two", "message-2", "cause-2", generation),
            Vec::new(),
            Vec::new(),
        ))
        .await
        .unwrap();
    let protocol = repository.projection_protocol.read().unwrap();
    assert!(!protocol
        .applied_receipts
        .contains_key(&CursorReceiptKey::new(
            &input_cursor(1, "source-v1"),
            generation,
        )));
    assert!(protocol.partitions[&partition_key]
        .pending_retry_failure_id
        .is_none());
}

/// The bare read-model handle of a repository is fenced like the repository
/// itself; the shared suite covers the repository's own write paths.
#[tokio::test]
async fn causal_owned_table_rejects_a_write_through_the_bare_model_store() {
    let repository = repository().await;
    for claimed in [false, true] {
        if claimed {
            repository
                .commit_projection(batch(
                    input(
                        1,
                        b"claim",
                        "message-1",
                        "cause-1",
                        ProjectionGeneration::initial(),
                    ),
                    vec![mutation(
                        ProjectionRecordExpectation::Missing,
                        RecordTransition::Upsert,
                    )],
                    Vec::new(),
                ))
                .await
                .unwrap();
        }
        assert!(matches!(
            repository
                .model_store()
                .commit_write_plan(TableWritePlan::new(vec![upsert_table_mutation(true)]))
                .await,
            Err(TableStoreError::CausalWriteRequired { ref table }) if table == "todo_views"
        ));
    }
}
