use std::sync::{Arc, Barrier, LazyLock};
use std::time::Duration;

use super::*;
use crate::command_ledger::{
    CanonicalInputHash, CausalCommitBatch, CausalTransactionalCommit, CommandContractFingerprint,
    CommandId, CommandLedgerError, CommandLedgerKey, CommandLedgerStore, CommandReservation,
    PrincipalPartitionId, ReservationOutcome, TerminalCommandState,
};
use crate::projection_protocol::{
    ProjectionExecutionSnapshotBatchRequest, ProjectionGraphSnapshotRequest,
    ProjectionInputFingerprint, ProjectionModelOwnership, ProjectionObservationRequest,
    ProjectionQuerySnapshotRequest, ProjectionRecordMutation, ProjectionScopeCodec,
    TrustedProjectionInput,
};
use crate::repository::{CommitBatch, ReadModelWritePlanStore, TransactionalCommit};
use crate::table::{
    ColumnType, DeleteTableRowMutation, ExpectedVersion, ForeignKey, PatchMode,
    PatchTableRowMutation, PrimaryKey, RelationshipDef, RelationshipKind, RowKey, RowPatch,
    RowValue, RowValues, RowWriteMode, TableColumn, TableKind, TableRowMutation, TableSchema,
};

fn topology() -> ProjectorTopologyId {
    ProjectorTopologyId::new(1, "todo_projector", [7; 32]).unwrap()
}

fn other_topology() -> ProjectorTopologyId {
    ProjectorTopologyId::new(1, "other_projector", [8; 32]).unwrap()
}

fn partition() -> ProjectionPartition {
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

fn change_epoch() -> ProjectionEpoch {
    ProjectionEpoch::new("changes-v1").unwrap()
}

fn change_cursor(position: u64) -> ProjectionChangeCursor {
    ProjectionChangeCursor::new(topology(), partition(), change_epoch(), position).unwrap()
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
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    });
    &SCHEMA
}

fn graph_parent_schema() -> &'static TableSchema {
    static SCHEMA: LazyLock<TableSchema> = LazyLock::new(|| TableSchema {
        model_name: "GraphParentView".into(),
        table_name: "graph_parent_views".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..TableColumn::new("id", "id", ColumnType::Text)
            },
            TableColumn::new("parent_id", "parent_id", ColumnType::Text),
        ],
        primary_key: PrimaryKey::new(["id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: vec![
            RelationshipDef {
                references: None,
                field_name: "children".into(),
                kind: RelationshipKind::HasMany,
                target_model: "GraphChildView".into(),
                foreign_key: Some("parent_id".into()),
                through: None,
                target_foreign_key: None,
            },
            RelationshipDef {
                references: None,
                field_name: "featured_children".into(),
                kind: RelationshipKind::HasMany,
                target_model: "GraphChildView".into(),
                foreign_key: Some("parent_id".into()),
                through: None,
                target_foreign_key: None,
            },
        ],
        kind: TableKind::ReadModel,
    });
    &SCHEMA
}

fn graph_child_schema() -> &'static TableSchema {
    static SCHEMA: LazyLock<TableSchema> = LazyLock::new(|| TableSchema {
        model_name: "GraphChildView".into(),
        table_name: "graph_child_views".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..TableColumn::new("id", "id", ColumnType::Text)
            },
            TableColumn {
                foreign_key: Some(ForeignKey::new("graph_parent_views", "id")),
                ..TableColumn::new("parent_id", "parent_id", ColumnType::Text)
            },
        ],
        primary_key: PrimaryKey::new(["id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    });
    &SCHEMA
}

fn graph_ownership() -> Vec<ProjectionModelOwnership> {
    vec![
        ProjectionModelOwnership::new("GraphParentView", "graph_parent_views").unwrap(),
        ProjectionModelOwnership::new("GraphChildView", "graph_child_views").unwrap(),
    ]
}

fn graph_codec() -> ProjectionScopeCodec {
    ProjectionScopeCodec::with_models(
        topology(),
        [
            ("GraphParentView", graph_parent_schema()),
            ("GraphChildView", graph_child_schema()),
        ],
    )
    .unwrap()
}

fn graph_key(model: &str) -> RowKey {
    RowKey::new([(
        "id",
        RowValue::String(
            match model {
                "GraphParentView" => "parent-1",
                "GraphChildView" => "child-1",
                other => panic!("unknown graph model {other}"),
            }
            .into(),
        ),
    )])
}

fn graph_scope(model: &str) -> ProjectionRecordScope {
    graph_codec()
        .encode_row_scope_in_partition(model, partition(), &graph_key(model))
        .unwrap()
}

fn graph_mutation(model: &str) -> ProjectionRecordMutation {
    let (schema, mut values) = match model {
        "GraphParentView" => {
            let mut values = RowValues::new();
            values.insert("id", RowValue::String("parent-1".into()));
            // Deliberately collides with the child's FK field name. HasMany
            // membership must follow the target column's FK reference to `id`.
            values.insert("parent_id", RowValue::String("wrong-parent".into()));
            (graph_parent_schema(), values)
        }
        "GraphChildView" => {
            let mut values = RowValues::new();
            values.insert("id", RowValue::String("child-1".into()));
            values.insert("parent_id", RowValue::String("parent-1".into()));
            (graph_child_schema(), values)
        }
        other => panic!("unknown graph model {other}"),
    };
    ProjectionRecordMutation::new(
        graph_scope(model),
        TableMutation::UpsertRow(TableRowMutation {
            schema,
            key: graph_key(model),
            values: std::mem::take(&mut values),
            expected_version: ExpectedVersion::Any,
            mode: RowWriteMode::Upsert,
        }),
        ProjectionRecordExpectation::Missing,
        ProjectionMutationKind::Upsert,
    )
    .unwrap()
}

fn graph_snapshot_request(max_unique: usize) -> ProjectionGraphSnapshotRequest {
    let root = ProjectionQuerySnapshotRequest::new(
        &graph_codec(),
        Some(&serde_json::json!("tenant-a")),
        "GraphParentView",
        graph_key("GraphParentView"),
        Vec::new(),
    )
    .unwrap();
    ProjectionGraphSnapshotRequest::new(
        root,
        [
            ("children".into(), Arc::new(graph_child_schema().clone())),
            (
                "featured_children".into(),
                Arc::new(graph_child_schema().clone()),
            ),
        ],
        max_unique,
    )
    .unwrap()
}

#[tokio::test]
async fn graph_snapshots_follow_composite_candidate_keys_and_keep_pk_scopes() {
    let mut parent = graph_parent_schema().clone();
    parent.relationships.truncate(1);
    parent.relationships[0].foreign_key = Some("tenant,key".into());
    parent.relationships[0].references = Some("tenant,key".into());
    let mut child = graph_child_schema().clone();
    child.relationships.push(RelationshipDef {
        field_name: "parent".into(),
        kind: RelationshipKind::BelongsTo,
        target_model: parent.model_name.clone(),
        foreign_key: Some("tenant,key".into()),
        references: Some("tenant,key".into()),
        through: None,
        target_foreign_key: None,
    });
    for schema in [&mut parent, &mut child] {
        schema
            .columns
            .push(TableColumn::new("tenant", "tenant", ColumnType::Text));
        schema.columns.push(TableColumn {
            nullable: true,
            ..TableColumn::new("key", "key", ColumnType::Text)
        });
    }
    parent.indexes.push(crate::table::TableIndex {
        name: None,
        columns: vec!["tenant".into(), "key".into()],
        unique: true,
    });
    let parent: &'static TableSchema = Box::leak(Box::new(parent));
    let child: &'static TableSchema = Box::leak(Box::new(child));
    let codec = ProjectionScopeCodec::with_models(
        topology(),
        [
            (parent.model_name.as_str(), parent),
            (child.model_name.as_str(), child),
        ],
    )
    .unwrap();
    let key = |id: &str| RowKey::new([("id", RowValue::String(id.into()))]);
    let mutation =
        |schema: &'static TableSchema, id: &str, tenant: &str, candidate: Option<&str>| {
            let row_key = key(id);
            let mut values = RowValues::new();
            values.insert("id", RowValue::String(id.into()));
            values.insert("parent_id", RowValue::String("unused".into()));
            values.insert("tenant", RowValue::String(tenant.into()));
            values.insert(
                "key",
                candidate
                    .map(|v| RowValue::String(v.into()))
                    .unwrap_or(RowValue::Null),
            );
            ProjectionRecordMutation::new(
                codec
                    .encode_row_scope_in_partition(&schema.model_name, partition(), &row_key)
                    .unwrap(),
                TableMutation::UpsertRow(TableRowMutation {
                    schema,
                    key: row_key,
                    values,
                    expected_version: ExpectedVersion::Any,
                    mode: RowWriteMode::Upsert,
                }),
                ProjectionRecordExpectation::Missing,
                ProjectionMutationKind::Upsert,
            )
            .unwrap()
        };
    let repository = InMemoryRepository::new();
    repository
        .register_projection_models(&topology(), &graph_ownership())
        .await
        .unwrap();
    repository
        .commit_projection(ProjectionCommitBatch {
            input: input(
                1,
                b"candidate-graph",
                "candidate-message",
                "candidate-cause",
                ProjectionGeneration::initial(),
            ),
            change_epoch: change_epoch(),
            ownership: graph_ownership(),
            observations: vec![],
            mutations: vec![
                mutation(parent, "opaque-a", "a", Some("same")),
                mutation(parent, "opaque-b", "b", Some("same")),
                mutation(parent, "null-target", "a", None),
                mutation(child, "source", "a", Some("same")),
                mutation(child, "null-source", "a", None),
            ],
        })
        .await
        .unwrap();
    for (schema, id, field, target, expected) in [
        (child, "source", "parent", parent, Some("opaque-a")),
        (parent, "opaque-a", "children", child, Some("source")),
        (parent, "opaque-b", "children", child, None),
        (child, "null-source", "parent", parent, None),
        (parent, "null-target", "children", child, None),
    ] {
        let root = ProjectionQuerySnapshotRequest::new(
            &codec,
            Some(&serde_json::json!("tenant-a")),
            &schema.model_name,
            key(id),
            vec![],
        )
        .unwrap();
        let request = ProjectionGraphSnapshotRequest::new(
            root,
            [(field.into(), Arc::new(target.clone()))],
            2,
        )
        .unwrap();
        let snapshot = repository
            .projection_graph_snapshot(&request)
            .await
            .unwrap();
        let rows = &snapshot.includes[field].rows;
        assert_eq!(rows.len(), usize::from(expected.is_some()), "{id}/{field}");
        if let Some(expected) = expected {
            assert_eq!(
                rows[0].row.as_ref().unwrap().get("id"),
                Some(&RowValue::String(expected.into()))
            );
            assert_eq!(
                rows[0].scope,
                codec
                    .encode_row_scope_in_partition(&target.model_name, partition(), &key(expected))
                    .unwrap()
            );
            assert!(rows[0].record.is_some());
        }
    }
}

fn fanout_schemas() -> &'static [TableSchema] {
    static SCHEMAS: LazyLock<Vec<TableSchema>> = LazyLock::new(|| {
        [
            ("FanoutParent", "fanout_parents"),
            ("FanoutChild", "fanout_children"),
            ("FanoutSummary", "fanout_summaries"),
            ("FanoutJoin", "fanout_joins"),
        ]
        .into_iter()
        .map(|(model_name, table_name)| TableSchema {
            model_name: model_name.into(),
            table_name: table_name.into(),
            columns: vec![TableColumn {
                primary_key: true,
                ..TableColumn::new("id", "id", ColumnType::Text)
            }],
            primary_key: PrimaryKey::new(["id"]),
            version_column: None,
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: TableKind::ReadModel,
        })
        .collect()
    });
    &SCHEMAS
}

fn fanout_codec() -> ProjectionScopeCodec {
    ProjectionScopeCodec::with_models(
        topology(),
        fanout_schemas()
            .iter()
            .map(|schema| (schema.model_name.as_str(), schema)),
    )
    .unwrap()
}

fn fanout_key(index: usize) -> RowKey {
    RowKey::new([("id", RowValue::String(format!("fanout-{index}")))])
}

fn fanout_scope(index: usize) -> ProjectionRecordScope {
    let schema = &fanout_schemas()[index];
    fanout_codec()
        .encode_row_scope_in_partition(&schema.model_name, partition(), &fanout_key(index))
        .unwrap()
}

fn fanout_mutation(index: usize) -> ProjectionRecordMutation {
    let schema = &fanout_schemas()[index];
    let key = fanout_key(index);
    let mut values = RowValues::new();
    values.insert("id", RowValue::String(format!("fanout-{index}")));
    ProjectionRecordMutation::new(
        fanout_scope(index),
        TableMutation::UpsertRow(TableRowMutation {
            schema,
            key,
            values,
            expected_version: ExpectedVersion::Any,
            mode: RowWriteMode::Upsert,
        }),
        ProjectionRecordExpectation::Missing,
        ProjectionMutationKind::Upsert,
    )
    .unwrap()
}

fn fanout_ownership() -> Vec<ProjectionModelOwnership> {
    fanout_schemas()
        .iter()
        .map(|schema| {
            ProjectionModelOwnership::new(&schema.model_name, &schema.table_name).unwrap()
        })
        .collect()
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

fn matrix_key(index: usize) -> RowKey {
    RowKey::new([("id", RowValue::String(format!("matrix-{index}")))])
}

fn matrix_scope(index: usize) -> ProjectionRecordScope {
    scope_codec()
        .encode_row_scope_in_partition("TodoView", partition(), &matrix_key(index))
        .unwrap()
}

fn matrix_mutation(
    index: usize,
    value: &str,
    expectation: ProjectionRecordExpectation,
) -> ProjectionRecordMutation {
    let key = matrix_key(index);
    let mut values = RowValues::new();
    values.insert("id", RowValue::String(format!("matrix-{index}")));
    values.insert("value", RowValue::String(value.into()));
    ProjectionRecordMutation::new(
        matrix_scope(index),
        TableMutation::UpsertRow(TableRowMutation {
            schema: schema(),
            key,
            values,
            expected_version: ExpectedVersion::Any,
            mode: RowWriteMode::Upsert,
        }),
        expectation,
        ProjectionMutationKind::Upsert,
    )
    .unwrap()
}

fn matrix_snapshot_request(index: usize) -> ProjectionQuerySnapshotRequest {
    ProjectionQuerySnapshotRequest::new(
        &scope_codec(),
        Some(&serde_json::json!("tenant-a")),
        "TodoView",
        matrix_key(index),
        Vec::new(),
    )
    .unwrap()
}

fn ownership() -> ProjectionModelOwnership {
    ProjectionModelOwnership::new("TodoView", "todo_views").unwrap()
}

#[derive(Clone, Copy)]
struct ProjectionScenario;

impl crate::projection_protocol::scenario_tests::ProjectionProtocolScenario for ProjectionScenario {
    type Store = InMemoryRepository;

    fn repository(&self) -> impl std::future::Future<Output = Self::Store> + Send {
        repository()
    }

    fn topology(&self) -> ProjectorTopologyId {
        topology()
    }

    fn other_topology(&self) -> ProjectorTopologyId {
        other_topology()
    }

    fn partition(&self) -> ProjectionPartition {
        partition()
    }

    fn change_epoch(&self) -> ProjectionEpoch {
        change_epoch()
    }

    fn ownership(&self) -> ProjectionModelOwnership {
        ownership()
    }

    fn mutation(
        &self,
        expectation: ProjectionRecordExpectation,
        kind: ProjectionMutationKind,
    ) -> ProjectionRecordMutation {
        mutation(expectation, kind)
    }

    fn row_exists<'a>(
        &'a self,
        repository: &'a Self::Store,
    ) -> impl std::future::Future<Output = bool> + Send + 'a {
        async move { row_exists(repository) }
    }
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

fn valued_mutation(
    value: u64,
    expectation: ProjectionRecordExpectation,
) -> ProjectionRecordMutation {
    let mut values = RowValues::new();
    values.insert("id", RowValue::String("todo-1".into()));
    values.insert("value", RowValue::String(value.to_string()));
    ProjectionRecordMutation::new(
        record_scope(),
        TableMutation::UpsertRow(TableRowMutation {
            schema: schema(),
            key: record_key(),
            values,
            expected_version: ExpectedVersion::Any,
            mode: RowWriteMode::Upsert,
        }),
        expectation,
        ProjectionMutationKind::Upsert,
    )
    .unwrap()
}

fn snapshot_request() -> ProjectionQuerySnapshotRequest {
    ProjectionQuerySnapshotRequest::new(
        &scope_codec(),
        Some(&serde_json::json!("tenant-a")),
        "TodoView",
        record_key(),
        vec![crate::projection_protocol::ProjectionCheckpointProbe::new(
            topology(),
            partition(),
            source(),
            ProjectionEpoch::new("source-v1").unwrap(),
            ProjectionGeneration::initial(),
        )],
    )
    .unwrap()
}

fn mutation(
    expectation: ProjectionRecordExpectation,
    kind: ProjectionMutationKind,
) -> ProjectionRecordMutation {
    let table = match kind {
        ProjectionMutationKind::Delete => TableMutation::DeleteRow(DeleteTableRowMutation {
            schema: schema(),
            key: RowKey::new([("id", RowValue::String("todo-1".into()))]),
            expected_version: ExpectedVersion::Any,
        }),
        ProjectionMutationKind::Upsert | ProjectionMutationKind::Recreate => {
            upsert_table_mutation(true)
        }
    };
    ProjectionRecordMutation::new(record_scope(), table, expectation, kind).unwrap()
}

fn patch_mutation(expected: RecordRevision) -> ProjectionRecordMutation {
    ProjectionRecordMutation::new(
        record_scope(),
        TableMutation::PatchRow(PatchTableRowMutation {
            schema: schema(),
            key: RowKey::new([("id", RowValue::String("todo-1".into()))]),
            patch: RowPatch::new().set("id", RowValue::String("todo-1".into())),
            expected_version: ExpectedVersion::Any,
            mode: PatchMode::UpdateExisting,
        }),
        ProjectionRecordExpectation::Exact(expected),
        ProjectionMutationKind::Upsert,
    )
    .unwrap()
}

fn batch(
    trusted: TrustedProjectionInput,
    mutations: Vec<ProjectionRecordMutation>,
    observations: Vec<ProjectionObservationRequest>,
) -> ProjectionCommitBatch {
    ProjectionCommitBatch {
        input: trusted,
        change_epoch: change_epoch(),
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

fn row_version(repository: &InMemoryRepository) -> Option<u64> {
    repository
        .model_store
        .relational_rows
        .read()
        .unwrap()
        .get(&upsert_table_mutation(true).lock_key())
        .map(|row| row.version)
}

fn direct_batch(causation_id: &str) -> SameTransactionProjectionBatch {
    SameTransactionProjectionBatch::single_upsert(
        topology(),
        partition(),
        change_epoch(),
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

#[test]
fn query_snapshot_probe_and_batch_bounds_are_explicit() {
    let request = snapshot_request();
    assert!(ProjectionQuerySnapshotBatchRequest::new(vec![
        request.clone();
        crate::projection_protocol::MAX_PROJECTION_QUERY_BATCH_ROWS
    ])
    .is_ok());
    assert!(matches!(
        ProjectionQuerySnapshotBatchRequest::new(vec![
            request;
            crate::projection_protocol::MAX_PROJECTION_QUERY_BATCH_ROWS + 1
        ]),
        Err(ProjectionProtocolError::InvalidBatch(message))
            if message.contains("maximum")
    ));

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
    let max_probe_request = ProjectionQuerySnapshotRequest::new(
        &scope_codec(),
        Some(&serde_json::json!("tenant-a")),
        "TodoView",
        record_key(),
        probes.clone(),
    )
    .unwrap();
    assert!(matches!(
        ProjectionQuerySnapshotBatchRequest::new(vec![max_probe_request; 33]),
        Err(ProjectionProtocolError::InvalidBatch(message))
            if message.contains("aggregate checkpoint probes")
    ));
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

    let evidence = (0..crate::projection_protocol::MAX_PROJECTION_EVIDENCE_BATCH_ITEMS)
        .map(|index| {
            crate::projection_protocol::ProjectionObligationEvidenceRequest::new(
                format!("cause-{index}"),
                record_scope(),
                ProjectionObservationKind::Record,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    assert!(
        crate::projection_protocol::ProjectionObligationEvidenceBatchRequest::new(evidence.clone())
            .is_ok()
    );
    let mut too_many_evidence = evidence;
    too_many_evidence.push(
        crate::projection_protocol::ProjectionObligationEvidenceRequest::new(
            "cause-over-limit",
            record_scope(),
            ProjectionObservationKind::Record,
        )
        .unwrap(),
    );
    assert!(matches!(
        crate::projection_protocol::ProjectionObligationEvidenceBatchRequest::new(
            too_many_evidence
        ),
        Err(ProjectionProtocolError::InvalidBatch(message))
            if message.contains("maximum")
    ));
    let duplicate = crate::projection_protocol::ProjectionObligationEvidenceRequest::new(
        "duplicate-cause",
        record_scope(),
        ProjectionObservationKind::Record,
    )
    .unwrap();
    assert!(matches!(
        crate::projection_protocol::ProjectionObligationEvidenceBatchRequest::new(vec![
            duplicate.clone(),
            duplicate,
        ]),
        Err(ProjectionProtocolError::InvalidBatch(message))
            if message.contains("repeats")
    ));

    let live = (0..crate::projection_protocol::MAX_PROJECTION_EVIDENCE_BATCH_ITEMS)
        .map(|index| {
            crate::projection_protocol::ProjectionLiveRecordRequest::new(
                &scope_codec(),
                "TodoView",
                RowKey::new([("id", RowValue::String(format!("todo-{index}")))]),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    assert!(
        crate::projection_protocol::ProjectionLiveRecordBatchRequest::new(live.clone()).is_ok()
    );
    let mut too_many_live = live;
    too_many_live.push(
        crate::projection_protocol::ProjectionLiveRecordRequest::new(
            &scope_codec(),
            "TodoView",
            RowKey::new([("id", RowValue::String("todo-over-limit".into()))]),
        )
        .unwrap(),
    );
    assert!(matches!(
        crate::projection_protocol::ProjectionLiveRecordBatchRequest::new(too_many_live),
        Err(ProjectionProtocolError::InvalidBatch(message))
            if message.contains("maximum")
    ));
}

#[tokio::test]
async fn query_snapshot_never_mixes_row_revision_checkpoint_or_resume_head() {
    let repository = repository().await;
    let first = repository
        .commit_projection(batch(
            input(
                1,
                b"snapshot-1",
                "snapshot-message-1",
                "snapshot-cause-1",
                ProjectionGeneration::initial(),
            ),
            vec![valued_mutation(1, ProjectionRecordExpectation::Missing)],
            Vec::new(),
        ))
        .await
        .unwrap();
    let mut expected = first.records[0].revision.clone();
    assert_query_snapshot_is_coherent(
        &repository
            .projection_query_snapshot(&snapshot_request())
            .await
            .unwrap(),
    );

    let writer_repository = repository.clone();
    let writer = tokio::spawn(async move {
        for position in 2..=64 {
            let committed = writer_repository
                .commit_projection(batch(
                    input(
                        position,
                        format!("snapshot-{position}").as_bytes(),
                        &format!("snapshot-message-{position}"),
                        &format!("snapshot-cause-{position}"),
                        ProjectionGeneration::initial(),
                    ),
                    vec![valued_mutation(
                        position,
                        ProjectionRecordExpectation::Exact(expected),
                    )],
                    Vec::new(),
                ))
                .await
                .unwrap();
            expected = committed.records[0].revision.clone();
            tokio::task::yield_now().await;
        }
    });

    while !writer.is_finished() {
        let batch = repository
            .projection_query_snapshot_batch(
                &ProjectionQuerySnapshotBatchRequest::new(vec![
                    snapshot_request(),
                    snapshot_request(),
                ])
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(batch.snapshots[0], batch.snapshots[1]);
        assert_query_snapshot_is_coherent(&batch.snapshots[0]);
        tokio::task::yield_now().await;
    }
    writer.await.unwrap();
    assert_query_snapshot_is_coherent(
        &repository
            .projection_query_snapshot(&snapshot_request())
            .await
            .unwrap(),
    );
}

#[tokio::test]
async fn obligation_evidence_is_exact_bounded_and_failure_wins_after_compaction() {
    let repository = repository().await;
    let scope = record_scope();
    let observed = repository
        .commit_projection(batch(
            input(
                1,
                b"observed",
                "evidence-message-1",
                "evidence-cause",
                ProjectionGeneration::initial(),
            ),
            vec![mutation(
                ProjectionRecordExpectation::Missing,
                ProjectionMutationKind::Upsert,
            )],
            vec![ProjectionObservationRequest {
                kind: ProjectionObservationKind::Record,
                target: ProjectionObservationTarget::StagedRecord(scope.clone()),
            }],
        ))
        .await
        .unwrap();
    let observed_probe = crate::projection_protocol::ProjectionObligationEvidenceRequest::new(
        "evidence-cause",
        scope.clone(),
        ProjectionObservationKind::Record,
    )
    .unwrap();
    let pending_probe = crate::projection_protocol::ProjectionObligationEvidenceRequest::new(
        "pending-cause",
        scope,
        ProjectionObservationKind::Record,
    )
    .unwrap();
    let before_failure = repository
        .projection_obligation_evidence_batch(
            &crate::projection_protocol::ProjectionObligationEvidenceBatchRequest::new(vec![
                observed_probe.clone(),
                pending_probe.clone(),
            ])
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        &before_failure.evidence[0],
        ProjectionObligationEvidence::Observed(observation)
            if observation.change == observed.changes[0].cursor
    ));
    assert_eq!(
        before_failure.evidence[1],
        ProjectionObligationEvidence::Pending
    );

    let failure = repository
        .record_projection_failure(
            ProjectionFailureBatch::new(
                input(
                    2,
                    b"failed",
                    "evidence-message-2",
                    "evidence-cause",
                    ProjectionGeneration::initial(),
                ),
                change_epoch(),
                "evidence-failure",
                "decode_error",
                b"bad evidence payload".to_vec(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    repository
        .compact_projection_changes(&failure.change)
        .await
        .unwrap();
    let after_failure = repository
        .projection_obligation_evidence_batch(
            &crate::projection_protocol::ProjectionObligationEvidenceBatchRequest::new(vec![
                observed_probe,
                pending_probe,
            ])
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        &after_failure.evidence[0],
        ProjectionObligationEvidence::TerminalFailure(stored)
            if stored == &failure
    ));
    assert_eq!(
        after_failure.evidence[1],
        ProjectionObligationEvidence::Pending
    );
}

#[tokio::test]
async fn unpartitioned_live_record_follows_tombstone_partition_move_and_rejects_ambiguity() {
    let repository = repository().await;
    let old_scope = record_scope();
    let created = repository
        .commit_projection(batch(
            input(
                1,
                b"old-partition",
                "move-message-1",
                "move-cause-1",
                ProjectionGeneration::initial(),
            ),
            vec![mutation(
                ProjectionRecordExpectation::Missing,
                ProjectionMutationKind::Upsert,
            )],
            Vec::new(),
        ))
        .await
        .unwrap()
        .records
        .remove(0);
    let live_request = crate::projection_protocol::ProjectionLiveRecordBatchRequest::new(vec![
        crate::projection_protocol::ProjectionLiveRecordRequest::new(
            &scope_codec(),
            "TodoView",
            record_key(),
        )
        .unwrap(),
    ])
    .unwrap();
    let live = repository
        .projection_live_record_batch(&live_request)
        .await
        .unwrap();
    assert_eq!(
        live.records[0].as_ref().unwrap().revision.scope(),
        &old_scope
    );

    repository
        .commit_projection(batch(
            input(
                2,
                b"old-delete",
                "move-message-2",
                "move-cause-2",
                ProjectionGeneration::initial(),
            ),
            vec![mutation(
                ProjectionRecordExpectation::Exact(created.revision),
                ProjectionMutationKind::Delete,
            )],
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(
        repository
            .projection_live_record_batch(&live_request)
            .await
            .unwrap()
            .records,
        vec![None]
    );

    let new_partition = scope_codec()
        .encode_partition(Some(&serde_json::json!("tenant-b")))
        .unwrap();
    let new_scope = scope_codec()
        .encode_row_scope_in_partition("TodoView", new_partition.clone(), &record_key())
        .unwrap();
    let new_input = TrustedProjectionInput::mint(
        ProjectionInputCursor::new(
            topology(),
            new_partition,
            source(),
            ProjectionEpoch::new("source-v1").unwrap(),
            1,
        )
        .unwrap(),
        ProjectionInputFingerprint::from_canonical_bytes(b"new-partition"),
        "move-message-3",
        "move-cause-3",
        ProjectionGeneration::initial(),
        true,
    )
    .unwrap();
    let moved = repository
        .commit_projection(batch(
            new_input,
            vec![ProjectionRecordMutation::new(
                new_scope.clone(),
                upsert_table_mutation(true),
                ProjectionRecordExpectation::Missing,
                ProjectionMutationKind::Upsert,
            )
            .unwrap()],
            Vec::new(),
        ))
        .await
        .unwrap()
        .records
        .remove(0);
    assert_eq!(
        repository
            .projection_live_record_batch(&live_request)
            .await
            .unwrap()
            .records[0]
            .as_ref()
            .unwrap()
            .revision
            .scope(),
        &new_scope
    );
    assert_eq!(moved.revision.scope(), &new_scope);

    // Simulate durable metadata drift that bypassed the normal write
    // fence. The read path must report corruption rather than choosing.
    repository
        .projection_protocol
        .write()
        .unwrap()
        .records
        .get_mut(&old_scope)
        .unwrap()
        .tombstone = false;
    assert!(matches!(
        repository.projection_live_record_batch(&live_request).await,
        Err(ProjectionProtocolError::InvalidBatch(message))
            if message.contains("ambiguous")
    ));
}

fn other_source_input(
    message_id: &str,
    generation: ProjectionGeneration,
) -> TrustedProjectionInput {
    TrustedProjectionInput::mint(
        ProjectionInputCursor::new(
            topology(),
            partition(),
            ProjectionSource::new("other_stream", b"other-1".to_vec()).unwrap(),
            ProjectionEpoch::new("source-v1").unwrap(),
            0,
        )
        .unwrap(),
        ProjectionInputFingerprint::from_canonical_bytes(b"other-input"),
        message_id,
        "other-cause",
        generation,
        true,
    )
    .unwrap()
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
                change_epoch(),
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
                ProjectionMutationKind::Upsert,
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
async fn automatic_change_retention_compacts_success_and_failure_prefixes() {
    let repository = repository_with_retention(ProjectionChangeRetention::new(2).unwrap()).await;
    for position in 0..4 {
        repository
            .commit_projection(batch(
                input(
                    position,
                    format!("input-{position}").as_bytes(),
                    &format!("message-{position}"),
                    &format!("cause-{position}"),
                    ProjectionGeneration::initial(),
                ),
                Vec::new(),
                Vec::new(),
            ))
            .await
            .unwrap();
    }

    assert_eq!(
        repository
            .projection_changes(&topology(), &partition(), None, 100)
            .await
            .unwrap(),
        ProjectionChangeRead::ResetRequired {
            head: Some(change_cursor(4)),
            compacted_through: 2,
        }
    );
    let retained = repository
        .projection_changes(&topology(), &partition(), Some(&change_cursor(2)), 100)
        .await
        .unwrap();
    let ProjectionChangeRead::Changes {
        head,
        compacted_through,
        changes,
    } = retained
    else {
        panic!("the exact compacted-through cursor must resume retained changes");
    };
    assert_eq!(head, Some(change_cursor(4)));
    assert_eq!(compacted_through, 2);
    assert_eq!(
        changes
            .iter()
            .map(|change| change.cursor.position())
            .collect::<Vec<_>>(),
        vec![3, 4]
    );
    assert!(matches!(
        repository
            .projection_changes(&topology(), &partition(), Some(&change_cursor(1)), 100)
            .await
            .unwrap(),
        ProjectionChangeRead::ResetRequired {
            compacted_through: 2,
            ..
        }
    ));

    repository
        .record_projection_failure(
            ProjectionFailureBatch::new(
                input(
                    4,
                    b"failure",
                    "message-4",
                    "cause-4",
                    ProjectionGeneration::initial(),
                ),
                change_epoch(),
                "failure-4",
                "decode_error",
                b"bad payload".to_vec(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let retained = repository
        .projection_changes(&topology(), &partition(), Some(&change_cursor(3)), 100)
        .await
        .unwrap();
    let ProjectionChangeRead::Changes {
        head,
        compacted_through,
        changes,
    } = retained
    else {
        panic!("the new compacted-through cursor must resume retained changes");
    };
    assert_eq!(head, Some(change_cursor(5)));
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
async fn automatic_retention_failure_rolls_back_rows_protocol_and_inbox() {
    let repository = repository_with_retention(ProjectionChangeRetention::new(1).unwrap()).await;
    let created = repository
        .commit_projection(batch(
            input(
                0,
                b"create",
                "message-0",
                "cause-0",
                ProjectionGeneration::initial(),
            ),
            vec![mutation(
                ProjectionRecordExpectation::Missing,
                ProjectionMutationKind::Upsert,
            )],
            Vec::new(),
        ))
        .await
        .unwrap()
        .records
        .remove(0);
    let row_version_before = row_version(&repository);
    let inbox_before = repository.inbox_store.read().unwrap().clone();
    {
        let mut protocol = repository.projection_protocol.write().unwrap();
        protocol
            .partitions
            .get_mut(&PartitionKey::new(&topology(), &partition()))
            .unwrap()
            .changes
            .remove(&1);
    }

    assert!(matches!(
        repository
            .commit_projection(batch(
                input(
                    1,
                    b"update",
                    "message-1",
                    "cause-1",
                    ProjectionGeneration::initial(),
                ),
                vec![mutation(
                    ProjectionRecordExpectation::Exact(created.revision.clone()),
                    ProjectionMutationKind::Upsert,
                )],
                Vec::new(),
            ))
            .await,
        Err(ProjectionProtocolError::InvalidBatch(message))
            if message.contains(
                "projection change compaction expected to remove 1 contiguous entries but found 0"
            )
    ));

    assert_eq!(row_version(&repository), row_version_before);
    assert_eq!(
        repository.projection_record(&record_scope()).await.unwrap(),
        Some(created)
    );
    assert_eq!(*repository.inbox_store.read().unwrap(), inbox_before);
    let protocol = repository.projection_protocol.read().unwrap();
    let partition = protocol
        .partitions
        .get(&PartitionKey::new(&topology(), &partition()))
        .unwrap();
    assert_eq!(partition.change_head, 1);
    assert_eq!(partition.compacted_through, 0);
    assert!(partition.changes.is_empty());
    assert!(!protocol
        .input_identities
        .contains_key(&CursorIdentityKey::new(&input_cursor(1, "source-v1"))));
    assert!(!protocol
        .applied_receipts
        .contains_key(&CursorReceiptKey::new(
            &input_cursor(1, "source-v1"),
            ProjectionGeneration::initial(),
        )));
}

#[tokio::test]
async fn lengthening_retention_never_restores_a_compacted_prefix() {
    let repository = repository_with_retention(ProjectionChangeRetention::new(1).unwrap()).await;
    for position in 0..3 {
        repository
            .commit_projection(batch(
                input(
                    position,
                    format!("input-{position}").as_bytes(),
                    &format!("message-{position}"),
                    &format!("cause-{position}"),
                    ProjectionGeneration::initial(),
                ),
                Vec::new(),
                Vec::new(),
            ))
            .await
            .unwrap();
    }
    assert_eq!(
        repository
            .compact_projection_changes(&change_cursor(3))
            .await
            .unwrap(),
        3
    );

    let lengthened = repository
        .clone()
        .with_projection_change_retention(ProjectionChangeRetention::new(10).unwrap());
    lengthened
        .commit_projection(batch(
            input(
                3,
                b"input-3",
                "message-3",
                "cause-3",
                ProjectionGeneration::initial(),
            ),
            Vec::new(),
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(
        lengthened
            .projection_changes(&topology(), &partition(), None, 100)
            .await
            .unwrap(),
        ProjectionChangeRead::ResetRequired {
            head: Some(change_cursor(4)),
            compacted_through: 3,
        }
    );
    assert_eq!(
        lengthened
            .projection_changes(&topology(), &partition(), Some(&change_cursor(3)), 100)
            .await
            .unwrap(),
        ProjectionChangeRead::Changes {
            head: Some(change_cursor(4)),
            compacted_through: 3,
            changes: vec![ProjectionChange {
                cursor: change_cursor(4),
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
}

#[tokio::test]
async fn malformed_projection_failure_is_rejected_before_memory_writes() {
    let repository = repository().await;
    let mut failure = ProjectionFailureBatch::new(
        input(
            0,
            b"malformed-failure",
            "message-malformed-failure",
            "cause-malformed-failure",
            ProjectionGeneration::initial(),
        ),
        change_epoch(),
        "failure-malformed",
        "decode_error",
        b"bad payload".to_vec(),
    )
    .unwrap();
    failure.failure_digest = [0; 32];

    assert!(matches!(
        repository.record_projection_failure(failure).await,
        Err(ProjectionProtocolError::InvalidBatch(message))
            if message.contains("digest does not match")
    ));
    assert!(repository
        .projection_failure(&topology(), &partition(), "failure-malformed")
        .await
        .unwrap()
        .is_none());
    assert!(repository
        .projection_partition_runtime_state(&topology(), &partition())
        .await
        .unwrap()
        .is_none());
    assert!(repository.inbox_store.read().unwrap().is_empty());
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
        change_epoch: change_epoch(),
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
        change_epoch(),
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
                change_epoch(),
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

#[tokio::test]
async fn shared_registered_table_ownership_rejects_other_topology() {
    crate::projection_protocol::scenario_tests::registered_table_ownership_rejects_other_topology(
        ProjectionScenario,
    )
    .await;
}

#[tokio::test]
async fn cursor_fences_are_exact_and_non_mutating() {
    let repository = repository().await;
    let applied = repository
        .commit_projection(batch(
            input(
                1,
                b"input-1",
                "message-1",
                "cause-1",
                ProjectionGeneration::initial(),
            ),
            vec![mutation(
                ProjectionRecordExpectation::Missing,
                ProjectionMutationKind::Upsert,
            )],
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(applied.outcome, ProjectionCommitOutcome::Applied);
    assert!(row_exists(&repository));

    let duplicate = repository
        .commit_projection(batch(
            input(
                1,
                b"input-1",
                "message-1",
                "cause-1",
                ProjectionGeneration::initial(),
            ),
            vec![mutation(
                ProjectionRecordExpectation::Missing,
                ProjectionMutationKind::Upsert,
            )],
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(duplicate.outcome, ProjectionCommitOutcome::Duplicate);
    assert!(duplicate.changes.is_empty());

    for corrupted in [
        input(
            1,
            b"input-1",
            "different-message",
            "cause-1",
            ProjectionGeneration::initial(),
        ),
        input(
            1,
            b"input-1",
            "message-1",
            "different-cause",
            ProjectionGeneration::initial(),
        ),
    ] {
        assert!(matches!(
            repository
                .commit_projection(batch(corrupted, Vec::new(), Vec::new()))
                .await,
            Err(ProjectionProtocolError::InputCorruption)
        ));
    }

    let stale = repository
        .commit_projection(batch(
            input(
                0,
                b"older",
                "message-0",
                "cause-0",
                ProjectionGeneration::initial(),
            ),
            Vec::new(),
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(stale.outcome, ProjectionCommitOutcome::StaleInput);
    assert!(matches!(
        repository
            .commit_projection(batch(
                input(
                    0,
                    b"older",
                    "message-1",
                    "cause-0",
                    ProjectionGeneration::initial(),
                ),
                Vec::new(),
                Vec::new(),
            ))
            .await,
        Err(ProjectionProtocolError::MessageIdReuse { .. })
    ));
    let incomparable = TrustedProjectionInput::mint(
        input_cursor(2, "source-v2"),
        ProjectionInputFingerprint::from_canonical_bytes(b"new-epoch"),
        "message-new-epoch",
        "cause-new-epoch",
        ProjectionGeneration::initial(),
        true,
    )
    .unwrap();
    assert!(matches!(
        repository
            .commit_projection(batch(incomparable, Vec::new(), Vec::new()))
            .await,
        Err(ProjectionProtocolError::IncomparableInput)
    ));

    assert!(matches!(
        repository
            .commit_projection(batch(
                input(
                    1,
                    b"corrupt",
                    "message-corrupt",
                    "cause-1",
                    ProjectionGeneration::initial(),
                ),
                Vec::new(),
                Vec::new(),
            ))
            .await,
        Err(ProjectionProtocolError::InputCorruption)
    ));
    assert_eq!(
        repository
            .projection_changes(&topology(), &partition(), None, 100)
            .await
            .unwrap(),
        ProjectionChangeRead::Changes {
            head: applied
                .checkpoint
                .map(|checkpoint| checkpoint.change().clone()),
            compacted_through: 0,
            changes: applied.changes,
        }
    );
}

#[tokio::test]
async fn message_receipts_and_gap_capability_are_immutable() {
    let repo = repository().await;
    for (position, fingerprint, message, cause) in [
        (1, b"one".as_slice(), "message-1", "cause-1"),
        (2, b"two".as_slice(), "message-2", "cause-2"),
    ] {
        repo.commit_projection(batch(
            input(
                position,
                fingerprint,
                message,
                cause,
                ProjectionGeneration::initial(),
            ),
            Vec::new(),
            Vec::new(),
        ))
        .await
        .unwrap();
    }

    let old_duplicate = repo
        .commit_projection(batch(
            input(
                1,
                b"one",
                "message-1",
                "cause-1",
                ProjectionGeneration::initial(),
            ),
            Vec::new(),
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(old_duplicate.outcome, ProjectionCommitOutcome::Duplicate);

    let changed_old_cursor = input(
        1,
        b"one",
        "new-message-at-old-cursor",
        "cause-1",
        ProjectionGeneration::initial(),
    );
    assert!(matches!(
        repo.commit_projection(batch(changed_old_cursor, Vec::new(), Vec::new()))
            .await,
        Err(ProjectionProtocolError::InputCorruption)
    ));

    let reused_old_message = input(
        3,
        b"three",
        "message-1",
        "cause-3",
        ProjectionGeneration::initial(),
    );
    assert!(matches!(
        repo.commit_projection(batch(reused_old_message, Vec::new(), Vec::new()))
            .await,
        Err(ProjectionProtocolError::MessageIdReuse { .. })
    ));

    assert!(matches!(
        repo.commit_projection(batch(
            input(
                4,
                b"gap",
                "gap-message",
                "gap-cause",
                ProjectionGeneration::initial(),
            ),
            Vec::new(),
            Vec::new(),
        ))
        .await,
        Err(ProjectionProtocolError::IncomparableInput)
    ));

    let changed_causation = TrustedProjectionInput::mint(
        input_cursor(1, "source-v1"),
        ProjectionInputFingerprint::from_canonical_bytes(b"one"),
        "message-1",
        "changed-cause",
        ProjectionGeneration::initial(),
        true,
    )
    .unwrap();
    assert!(matches!(
        repo.commit_projection(batch(changed_causation, Vec::new(), Vec::new()))
            .await,
        Err(ProjectionProtocolError::InputCorruption)
    ));

    for (position, fingerprint, message, cause) in [
        (1, b"one".as_slice(), "old-capability", "cause-1"),
        (2, b"two".as_slice(), "equal-capability", "cause-2"),
        (3, b"three".as_slice(), "new-capability", "cause-3"),
    ] {
        let changed_capability = TrustedProjectionInput::mint(
            input_cursor(position, "source-v1"),
            ProjectionInputFingerprint::from_canonical_bytes(fingerprint),
            message,
            cause,
            ProjectionGeneration::initial(),
            false,
        )
        .unwrap();
        assert!(matches!(
            repo.commit_projection(batch(changed_capability, Vec::new(), Vec::new()))
                .await,
            Err(ProjectionProtocolError::InputCorruption)
        ));
    }

    // A first-ever terminal failure also registers the source capability;
    // repair generations cannot silently redefine it merely because there
    // was no last-good checkpoint to copy.
    let failed_first = repository().await;
    failed_first
        .record_projection_failure(
            ProjectionFailureBatch::new(
                input(
                    0,
                    b"failed-first",
                    "failed-first-message",
                    "failed-first-cause",
                    ProjectionGeneration::initial(),
                ),
                change_epoch(),
                "failed-first-id",
                "decode_error",
                b"bad first payload".to_vec(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let repaired_generation = failed_first
        .repair_projection(&topology(), &partition(), "failed-first-id")
        .await
        .unwrap();
    let changed_after_repair = TrustedProjectionInput::mint(
        input_cursor(0, "source-v1"),
        ProjectionInputFingerprint::from_canonical_bytes(b"retry"),
        "retry-message",
        "retry-cause",
        repaired_generation,
        false,
    )
    .unwrap();
    assert!(matches!(
        failed_first
            .commit_projection(batch(changed_after_repair, Vec::new(), Vec::new()))
            .await,
        Err(ProjectionProtocolError::InputCorruption)
    ));
}

#[tokio::test]
async fn message_identity_is_topology_wide_across_projection_partitions() {
    crate::projection_protocol::scenario_tests::message_identity_is_topology_wide_across_projection_partitions(
        ProjectionScenario,
    )
    .await;
}

#[tokio::test]
async fn input_disposition_is_read_only_exact_and_repair_fenced() {
    crate::projection_protocol::scenario_tests::input_disposition_is_read_only_exact_and_repair_fenced(
        ProjectionScenario,
    )
    .await;
}

#[tokio::test]
async fn repair_generation_retries_only_the_exact_failed_input() {
    let repository = repository().await;
    repository
        .commit_projection(batch(
            input(
                1,
                b"one",
                "message-1",
                "cause-1",
                ProjectionGeneration::initial(),
            ),
            Vec::new(),
            Vec::new(),
        ))
        .await
        .unwrap();
    let failed_input = input(
        2,
        b"two",
        "message-2",
        "cause-2",
        ProjectionGeneration::initial(),
    );
    repository
        .record_projection_failure(
            ProjectionFailureBatch::new(
                failed_input.clone(),
                change_epoch(),
                "failure-2",
                "decode_error",
                b"bad payload".to_vec(),
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let corrupt_stopped = input(
        2,
        b"changed",
        "message-2",
        "cause-2",
        ProjectionGeneration::initial(),
    );
    assert!(matches!(
        repository
            .commit_projection(batch(corrupt_stopped, Vec::new(), Vec::new()))
            .await,
        Err(ProjectionProtocolError::InputCorruption)
    ));

    let generation = repository
        .repair_projection(&topology(), &partition(), "failure-2")
        .await
        .unwrap();
    {
        let protocol = repository.projection_protocol.read().unwrap();
        assert_eq!(
            protocol.generations.get(&GenerationKey {
                partition: PartitionKey::new(&topology(), &partition()),
                generation,
            }),
            Some(&GenerationLineage {
                retry_of_generation: Some(ProjectionGeneration::initial()),
                retry_of_failure_id: Some("failure-2".into()),
            })
        );
        assert_eq!(
            protocol
                .partitions
                .get(&PartitionKey::new(&topology(), &partition()))
                .and_then(|partition| partition.pending_retry_failure_id.as_deref()),
            Some("failure-2")
        );
    }

    assert!(matches!(
        repository
            .commit_projection(batch(
                input(
                    1,
                    b"changed-known-cursor",
                    "message-1",
                    "cause-1",
                    ProjectionGeneration::initial(),
                ),
                Vec::new(),
                Vec::new(),
            ))
            .await,
        Err(ProjectionProtocolError::InputCorruption)
    ));
    let changed_source_capability = TrustedProjectionInput::mint(
        input_cursor(3, "source-v1"),
        ProjectionInputFingerprint::from_canonical_bytes(b"changed-source-capability"),
        "changed-source-capability-message",
        "changed-source-capability-cause",
        ProjectionGeneration::initial(),
        false,
    )
    .unwrap();
    assert!(matches!(
        repository
            .commit_projection(batch(changed_source_capability, Vec::new(), Vec::new(),))
            .await,
        Err(ProjectionProtocolError::InputCorruption)
    ));
    assert!(matches!(
        repository
            .commit_projection(batch(
                other_source_input("unknown-old-generation", ProjectionGeneration::initial()),
                Vec::new(),
                Vec::new(),
            ))
            .await,
        Err(ProjectionProtocolError::GenerationFenced {
            expected: 2,
            actual: 1,
        })
    ));
    assert!(matches!(
        repository
            .commit_projection(batch(
                other_source_input("message-1", ProjectionGeneration::initial()),
                Vec::new(),
                Vec::new(),
            ))
            .await,
        Err(ProjectionProtocolError::MessageIdReuse { message_id })
            if message_id == "message-1"
    ));

    let inherited = repository
        .commit_projection(batch(
            input(1, b"one", "message-1", "cause-1", generation),
            Vec::new(),
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(inherited.outcome, ProjectionCommitOutcome::Duplicate);
    assert!(!repository
        .projection_protocol
        .read()
        .unwrap()
        .applied_receipts
        .contains_key(&CursorReceiptKey::new(
            &input_cursor(1, "source-v1"),
            generation,
        )));
    let inherited_older = repository
        .commit_projection(batch(
            input(0, b"zero", "message-0", "cause-0", generation),
            Vec::new(),
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(inherited_older.outcome, ProjectionCommitOutcome::StaleInput);

    assert!(matches!(
        repository
            .commit_projection(batch(
                other_source_input("other-message", generation),
                Vec::new(),
                Vec::new(),
            ))
            .await,
        Err(ProjectionProtocolError::IncomparableInput)
    ));
    assert!(matches!(
        repository
            .commit_projection(batch(
                other_source_input("message-1", generation),
                Vec::new(),
                Vec::new(),
            ))
            .await,
        Err(ProjectionProtocolError::MessageIdReuse { message_id })
            if message_id == "message-1"
    ));
    assert!(matches!(
        repository
            .commit_projection(batch(
                input(2, b"changed", "message-2", "cause-2", generation),
                Vec::new(),
                Vec::new(),
            ))
            .await,
        Err(ProjectionProtocolError::InputCorruption)
    ));

    let repaired = repository
        .commit_projection(batch(
            input(2, b"two", "message-2", "cause-2", generation),
            Vec::new(),
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(repaired.outcome, ProjectionCommitOutcome::Applied);
    assert!(repository
        .projection_protocol
        .read()
        .unwrap()
        .partitions
        .get(&PartitionKey::new(&topology(), &partition()))
        .unwrap()
        .pending_retry_failure_id
        .is_none());
    assert_eq!(
        repository
            .commit_projection(batch(
                input(3, b"three", "message-3", "cause-3", generation),
                Vec::new(),
                Vec::new(),
            ))
            .await
            .unwrap()
            .outcome,
        ProjectionCommitOutcome::Applied
    );

    let failed_again = self::repository().await;
    failed_again
        .record_projection_failure(
            ProjectionFailureBatch::new(
                input(
                    0,
                    b"first-failure",
                    "first-failure-message",
                    "first-failure-cause",
                    ProjectionGeneration::initial(),
                ),
                change_epoch(),
                "first-failure",
                "decode_error",
                b"bad payload".to_vec(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let retry_generation = failed_again
        .repair_projection(&topology(), &partition(), "first-failure")
        .await
        .unwrap();
    let retried_failure = failed_again
        .record_projection_failure(
            ProjectionFailureBatch::new(
                input(
                    0,
                    b"first-failure",
                    "first-failure-message",
                    "first-failure-cause",
                    retry_generation,
                ),
                change_epoch(),
                "second-failure",
                "decode_error",
                b"still bad".to_vec(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(retried_failure.generation, retry_generation);
    assert!(matches!(
        failed_again
            .commit_projection(batch(
                other_source_input("blocked-after-retry-failure", retry_generation),
                Vec::new(),
                Vec::new(),
            ))
            .await,
        Err(ProjectionProtocolError::PartitionStopped { failure_id })
            if failure_id == "second-failure"
    ));
}

#[tokio::test]
async fn tombstone_requires_explicit_exact_recreation() {
    crate::projection_protocol::scenario_tests::tombstone_requires_explicit_exact_recreation(
        ProjectionScenario,
    )
    .await;
}

#[tokio::test]
async fn failure_recording_is_idempotent_for_exact_batch() {
    crate::projection_protocol::scenario_tests::failure_recording_is_idempotent_for_exact_batch(
        ProjectionScenario,
    )
    .await;
}

#[tokio::test]
async fn physical_rows_must_match_projection_record_metadata() {
    let create_with_orphan_row = repository().await;
    insert_physical_row(&create_with_orphan_row);
    assert!(matches!(
        create_with_orphan_row
            .commit_projection(batch(
                input(
                    1,
                    b"orphan",
                    "message-orphan",
                    "cause-orphan",
                    ProjectionGeneration::initial(),
                ),
                vec![mutation(
                    ProjectionRecordExpectation::Missing,
                    ProjectionMutationKind::Upsert,
                )],
                Vec::new(),
            ))
            .await,
        Err(ProjectionProtocolError::InvalidBatch(message))
            if message.contains("physical row to be absent")
    ));
    assert!(create_with_orphan_row
        .projection_record(&record_scope())
        .await
        .unwrap()
        .is_none());
    assert!(create_with_orphan_row
        .inbox_store
        .read()
        .unwrap()
        .is_empty());
    {
        let protocol = create_with_orphan_row.projection_protocol.read().unwrap();
        assert!(protocol.partitions.is_empty());
        assert!(protocol.input_identities.is_empty());
        assert!(protocol.applied_receipts.is_empty());
    }

    for operation in ["save", "patch", "delete"] {
        let repository = repository().await;
        let created = repository
            .commit_projection(batch(
                input(
                    1,
                    format!("{operation}-create").as_bytes(),
                    &format!("{operation}-message-1"),
                    &format!("{operation}-cause-1"),
                    ProjectionGeneration::initial(),
                ),
                vec![mutation(
                    ProjectionRecordExpectation::Missing,
                    ProjectionMutationKind::Upsert,
                )],
                Vec::new(),
            ))
            .await
            .unwrap();
        let metadata = created.records[0].clone();
        let changes_before = repository
            .projection_changes(&topology(), &partition(), None, 100)
            .await
            .unwrap();
        let inbox_before = repository.inbox_store.read().unwrap().clone();
        remove_physical_row(&repository);
        let attempted = match operation {
            "save" => mutation(
                ProjectionRecordExpectation::Exact(metadata.revision.clone()),
                ProjectionMutationKind::Upsert,
            ),
            "patch" => patch_mutation(metadata.revision.clone()),
            "delete" => mutation(
                ProjectionRecordExpectation::Exact(metadata.revision.clone()),
                ProjectionMutationKind::Delete,
            ),
            _ => unreachable!(),
        };
        assert!(matches!(
            repository
                .commit_projection(batch(
                    input(
                        2,
                        format!("{operation}-missing").as_bytes(),
                        &format!("{operation}-message-2"),
                        &format!("{operation}-cause-2"),
                        ProjectionGeneration::initial(),
                    ),
                    vec![attempted],
                    Vec::new(),
                ))
                .await,
            Err(ProjectionProtocolError::InvalidBatch(message))
                if message.contains("physical row to be present")
        ));
        assert_eq!(
            repository.projection_record(&record_scope()).await.unwrap(),
            Some(metadata)
        );
        assert_eq!(
            repository
                .projection_changes(&topology(), &partition(), None, 100)
                .await
                .unwrap(),
            changes_before
        );
        assert_eq!(*repository.inbox_store.read().unwrap(), inbox_before);
        let protocol = repository.projection_protocol.read().unwrap();
        assert!(!protocol
            .input_identities
            .contains_key(&CursorIdentityKey::new(&input_cursor(2, "source-v1"))));
        assert!(!protocol
            .applied_receipts
            .contains_key(&CursorReceiptKey::new(
                &input_cursor(2, "source-v1"),
                ProjectionGeneration::initial(),
            )));
    }

    let recreate_with_row = repository().await;
    let created = recreate_with_row
        .commit_projection(batch(
            input(
                1,
                b"create",
                "recreate-message-1",
                "recreate-cause-1",
                ProjectionGeneration::initial(),
            ),
            vec![mutation(
                ProjectionRecordExpectation::Missing,
                ProjectionMutationKind::Upsert,
            )],
            Vec::new(),
        ))
        .await
        .unwrap()
        .records
        .remove(0);
    let deleted = recreate_with_row
        .commit_projection(batch(
            input(
                2,
                b"delete",
                "recreate-message-2",
                "recreate-cause-2",
                ProjectionGeneration::initial(),
            ),
            vec![mutation(
                ProjectionRecordExpectation::Exact(created.revision),
                ProjectionMutationKind::Delete,
            )],
            Vec::new(),
        ))
        .await
        .unwrap()
        .records
        .remove(0);
    insert_physical_row(&recreate_with_row);
    let changes_before = recreate_with_row
        .projection_changes(&topology(), &partition(), None, 100)
        .await
        .unwrap();
    let inbox_before = recreate_with_row.inbox_store.read().unwrap().clone();
    assert!(matches!(
        recreate_with_row
            .commit_projection(batch(
                input(
                    3,
                    b"recreate",
                    "recreate-message-3",
                    "recreate-cause-3",
                    ProjectionGeneration::initial(),
                ),
                vec![mutation(
                    ProjectionRecordExpectation::Exact(deleted.revision.clone()),
                    ProjectionMutationKind::Recreate,
                )],
                Vec::new(),
            ))
            .await,
        Err(ProjectionProtocolError::InvalidBatch(message))
            if message.contains("physical row to be absent")
    ));
    assert_eq!(
        recreate_with_row
            .projection_record(&record_scope())
            .await
            .unwrap(),
        Some(deleted)
    );
    assert_eq!(
        recreate_with_row
            .projection_changes(&topology(), &partition(), None, 100)
            .await
            .unwrap(),
        changes_before
    );
    assert_eq!(*recreate_with_row.inbox_store.read().unwrap(), inbox_before);
    assert!(row_exists(&recreate_with_row));
}

#[tokio::test]
async fn table_failure_rolls_back_rows_protocol_and_inbox() {
    let repository = repository().await;
    let invalid = ProjectionRecordMutation::new(
        record_scope(),
        upsert_table_mutation(false),
        ProjectionRecordExpectation::Missing,
        ProjectionMutationKind::Upsert,
    )
    .unwrap();
    assert!(matches!(
        repository
            .commit_projection(batch(
                input(
                    1,
                    b"invalid",
                    "message-invalid",
                    "cause-invalid",
                    ProjectionGeneration::initial(),
                ),
                vec![invalid],
                Vec::new(),
            ))
            .await,
        Err(ProjectionProtocolError::Table(_))
    ));
    assert!(!row_exists(&repository));
    assert!(repository
        .projection_record(&record_scope())
        .await
        .unwrap()
        .is_none());
    assert!(repository
        .projection_checkpoint(
            &input_cursor(1, "source-v1"),
            ProjectionGeneration::initial()
        )
        .await
        .unwrap()
        .is_none());
    assert!(repository.inbox_store.read().unwrap().is_empty());
    {
        let protocol = repository.projection_protocol.read().unwrap();
        assert!(protocol.partitions.is_empty());
        assert!(protocol.inputs.is_empty());
        assert!(protocol.input_identities.is_empty());
        assert!(protocol.messages.is_empty());
        assert!(protocol.applied_receipts.is_empty());
        assert!(protocol.observations.is_empty());
        assert!(protocol.failures.is_empty());
    }

    let changes = repository
        .projection_changes(&topology(), &partition(), None, 100)
        .await
        .unwrap();
    assert_eq!(
        changes,
        ProjectionChangeRead::Changes {
            head: None,
            compacted_through: 0,
            changes: Vec::new(),
        }
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_same_input_applies_once() {
    let repository = repository().await;
    let barrier = Arc::new(Barrier::new(2));
    let mut joins = Vec::new();
    for _ in 0..2 {
        let repository = repository.clone();
        let barrier = Arc::clone(&barrier);
        joins.push(tokio::spawn(async move {
            barrier.wait();
            repository
                .commit_projection(batch(
                    input(
                        1,
                        b"same-input",
                        "message-1",
                        "cause-1",
                        ProjectionGeneration::initial(),
                    ),
                    vec![mutation(
                        ProjectionRecordExpectation::Missing,
                        ProjectionMutationKind::Upsert,
                    )],
                    Vec::new(),
                ))
                .await
                .unwrap()
                .outcome
        }));
    }
    let mut outcomes = Vec::with_capacity(joins.len());
    for join in joins {
        outcomes.push(join.await.unwrap());
    }
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == ProjectionCommitOutcome::Applied)
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == ProjectionCommitOutcome::Duplicate)
            .count(),
        1
    );
    assert_eq!(
        repository
            .projection_record(&record_scope())
            .await
            .unwrap()
            .unwrap()
            .revision
            .revision(),
        1
    );
}

#[tokio::test]
async fn observations_are_immutable_and_staged_records_reuse_row_changes() {
    let repository = repository().await;
    let scope = record_scope();
    let first = repository
        .commit_projection(batch(
            input(
                1,
                b"first",
                "message-1",
                "cause-stable",
                ProjectionGeneration::initial(),
            ),
            vec![mutation(
                ProjectionRecordExpectation::Missing,
                ProjectionMutationKind::Upsert,
            )],
            vec![ProjectionObservationRequest {
                kind: ProjectionObservationKind::Record,
                target: ProjectionObservationTarget::StagedRecord(scope.clone()),
            }],
        ))
        .await
        .unwrap();
    assert_eq!(first.changes.len(), 1);
    assert_eq!(first.changes[0].kind, ProjectionChangeKind::RecordUpsert);
    let earliest = repository
        .projection_observation("cause-stable", &scope, ProjectionObservationKind::Record)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(earliest.change, first.changes[0].cursor);
    assert_eq!(earliest.revision.as_ref(), Some(&first.records[0].revision));

    let second = repository
        .commit_projection(batch(
            input(
                2,
                b"second",
                "message-2",
                "cause-stable",
                ProjectionGeneration::initial(),
            ),
            vec![mutation(
                ProjectionRecordExpectation::Exact(first.records[0].revision.clone()),
                ProjectionMutationKind::Upsert,
            )],
            vec![ProjectionObservationRequest {
                kind: ProjectionObservationKind::Record,
                target: ProjectionObservationTarget::StagedRecord(scope.clone()),
            }],
        ))
        .await
        .unwrap();
    assert_eq!(second.changes.len(), 1);
    assert_eq!(second.changes[0].kind, ProjectionChangeKind::RecordUpsert);
    assert_eq!(
        repository
            .projection_observation("cause-stable", &scope, ProjectionObservationKind::Record)
            .await
            .unwrap(),
        Some(earliest)
    );

    let existing = repository
        .commit_projection(batch(
            input(
                3,
                b"existing",
                "message-3",
                "cause-existing",
                ProjectionGeneration::initial(),
            ),
            Vec::new(),
            vec![ProjectionObservationRequest {
                kind: ProjectionObservationKind::Record,
                target: ProjectionObservationTarget::ExistingRecord(
                    second.records[0].revision.clone(),
                ),
            }],
        ))
        .await
        .unwrap();
    assert_eq!(existing.changes.len(), 1);
    assert_eq!(existing.changes[0].kind, ProjectionChangeKind::Observation);
    let earliest_existing = repository
        .projection_observation("cause-existing", &scope, ProjectionObservationKind::Record)
        .await
        .unwrap()
        .unwrap();

    let repeated = repository
        .commit_projection(batch(
            input(
                4,
                b"existing-repeat",
                "message-4",
                "cause-existing",
                ProjectionGeneration::initial(),
            ),
            Vec::new(),
            vec![ProjectionObservationRequest {
                kind: ProjectionObservationKind::Record,
                target: ProjectionObservationTarget::ExistingRecord(
                    second.records[0].revision.clone(),
                ),
            }],
        ))
        .await
        .unwrap();
    assert_eq!(repeated.changes.len(), 1);
    assert_eq!(repeated.changes[0].kind, ProjectionChangeKind::Checkpoint);
    assert_eq!(
        repository
            .projection_observation("cause-existing", &scope, ProjectionObservationKind::Record)
            .await
            .unwrap(),
        Some(earliest_existing)
    );
}

#[tokio::test]
async fn causation_evidence_is_topology_allowlisted_and_repair_clears_terminal_failure() {
    let repository = repository().await;
    let scope = record_scope();
    repository
        .commit_projection(batch(
            input(
                1,
                b"status-observation",
                "message-status-1",
                "cause-status",
                ProjectionGeneration::initial(),
            ),
            vec![mutation(
                ProjectionRecordExpectation::Missing,
                ProjectionMutationKind::Upsert,
            )],
            vec![ProjectionObservationRequest {
                kind: ProjectionObservationKind::Record,
                target: ProjectionObservationTarget::StagedRecord(scope),
            }],
        ))
        .await
        .unwrap();

    let selected =
        ProjectionCausationEvidenceRequest::new("cause-status", vec![topology()]).unwrap();
    let observed = repository
        .projection_causation_evidence(&selected)
        .await
        .unwrap();
    assert_eq!(observed.observations.len(), 1);
    assert!(observed.terminal_failure_topologies.is_empty());

    let unrelated =
        ProjectionCausationEvidenceRequest::new("cause-status", vec![other_topology()]).unwrap();
    let filtered = repository
        .projection_causation_evidence(&unrelated)
        .await
        .unwrap();
    assert!(filtered.observations.is_empty());
    assert!(filtered.terminal_failure_topologies.is_empty());

    let failure = ProjectionFailureBatch::new(
        input(
            2,
            b"status-failure",
            "message-status-2",
            "cause-status",
            ProjectionGeneration::initial(),
        ),
        change_epoch(),
        "failure-status-2",
        "decode_error",
        b"redacted".to_vec(),
    )
    .unwrap();
    repository.record_projection_failure(failure).await.unwrap();
    let failed = repository
        .projection_causation_evidence(&selected)
        .await
        .unwrap();
    assert_eq!(failed.terminal_failure_topologies, vec![topology()]);
    let filtered = repository
        .projection_causation_evidence(&unrelated)
        .await
        .unwrap();
    assert!(filtered.terminal_failure_topologies.is_empty());

    repository
        .repair_projection(&topology(), &partition(), "failure-status-2")
        .await
        .unwrap();
    let repaired = repository
        .projection_causation_evidence(&selected)
        .await
        .unwrap();
    assert_eq!(repaired.observations.len(), 1);
    assert!(repaired.terminal_failure_topologies.is_empty());
}

#[tokio::test]
async fn dependency_failure_repair_and_resume_are_durable() {
    let repository = repository().await;
    let scope = record_scope();
    let first = repository
        .commit_projection(batch(
            input(
                1,
                b"dependency",
                "message-1",
                "cause-1",
                ProjectionGeneration::initial(),
            ),
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

    let failure_batch = ProjectionFailureBatch::new(
        input(
            2,
            b"failure",
            "message-2",
            "cause-2",
            ProjectionGeneration::initial(),
        ),
        change_epoch(),
        "failure-2",
        "decode_error",
        b"bad payload".to_vec(),
    )
    .unwrap();
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
    let changed_capability = ProjectionFailureBatch::new(
        TrustedProjectionInput::mint(
            input_cursor(2, "source-v1"),
            ProjectionInputFingerprint::from_canonical_bytes(b"failure"),
            "message-2",
            "cause-2",
            ProjectionGeneration::initial(),
            false,
        )
        .unwrap(),
        change_epoch(),
        "failure-2",
        "decode_error",
        b"bad payload".to_vec(),
    )
    .unwrap();
    assert!(matches!(
        repository
            .record_projection_failure(changed_capability)
            .await,
        Err(ProjectionProtocolError::InputCorruption)
    ));
    assert_eq!(
        repository
            .projection_failure(&topology(), &partition(), "failure-2")
            .await
            .unwrap(),
        Some(failure.clone())
    );
    assert!(matches!(
        repository
            .commit_projection(batch(
                input(
                    3,
                    b"blocked",
                    "message-3",
                    "cause-3",
                    ProjectionGeneration::initial(),
                ),
                Vec::new(),
                Vec::new(),
            ))
            .await,
        Err(ProjectionProtocolError::PartitionStopped { .. })
    ));

    let generation = repository
        .repair_projection(&topology(), &partition(), "failure-2")
        .await
        .unwrap();
    assert_eq!(generation.get(), 2);
    assert!(matches!(
        repository.record_projection_failure(failure_batch).await,
        Err(ProjectionProtocolError::GenerationFenced {
            expected: 2,
            actual: 1
        })
    ));
    let repaired = repository
        .commit_projection(batch(
            input(2, b"failure", "message-2", "cause-2", generation),
            Vec::new(),
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(repaired.changes[0].kind, ProjectionChangeKind::Checkpoint);

    let compacted = repository
        .compact_projection_changes(&first.changes[0].cursor)
        .await
        .unwrap();
    assert_eq!(compacted, first.changes[0].cursor.position());
    assert!(matches!(
        repository
            .projection_changes(&topology(), &partition(), None, 100)
            .await
            .unwrap(),
        ProjectionChangeRead::ResetRequired {
            compacted_through,
            ..
        } if compacted_through == compacted
    ));
    let boundary_resume = repository
        .projection_changes(
            &topology(),
            &partition(),
            Some(&first.changes[0].cursor),
            100,
        )
        .await
        .unwrap();
    assert!(matches!(
        boundary_resume,
        ProjectionChangeRead::Changes {
            compacted_through,
            ref changes,
            ..
        } if compacted_through == compacted && changes.len() == 2
    ));

    repository
        .compact_projection_changes(&failure.change)
        .await
        .unwrap();
    assert!(matches!(
        repository
            .projection_changes(
                &topology(),
                &partition(),
                Some(&first.changes[0].cursor),
                100
            )
            .await
            .unwrap(),
        ProjectionChangeRead::ResetRequired { .. }
    ));

    let future = ProjectionChangeCursor::new(
        topology(),
        partition(),
        change_epoch(),
        repaired.changes[0].cursor.position() + 10,
    )
    .unwrap();
    assert!(matches!(
        repository
            .projection_changes(&topology(), &partition(), Some(&future), 100)
            .await
            .unwrap(),
        ProjectionChangeRead::ResetRequired { .. }
    ));
}

#[tokio::test]
async fn causal_owned_table_rejects_every_legacy_commit_path() {
    let repository = repository().await;
    let before_first_message = repository
        .model_store()
        .commit_write_plan(TableWritePlan::new(vec![upsert_table_mutation(true)]))
        .await
        .unwrap_err();
    assert!(matches!(
        before_first_message,
        TableStoreError::CausalWriteRequired { ref table } if table == "todo_views"
    ));
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
                ProjectionMutationKind::Upsert,
            )],
            Vec::new(),
        ))
        .await
        .unwrap();

    let direct = repository
        .commit_write_plan(TableWritePlan::new(vec![upsert_table_mutation(true)]))
        .await
        .unwrap_err();
    assert!(matches!(
        direct,
        TableStoreError::CausalWriteRequired { ref table } if table == "todo_views"
    ));
    let bare_handle = repository
        .model_store()
        .commit_write_plan(TableWritePlan::new(vec![upsert_table_mutation(true)]))
        .await
        .unwrap_err();
    assert!(matches!(
        bare_handle,
        TableStoreError::CausalWriteRequired { ref table } if table == "todo_views"
    ));

    let mut raw_batch = CommitBatch::empty();
    raw_batch
        .read_model_plans
        .push(TableWritePlan::new(vec![upsert_table_mutation(true)]));
    let transactional = repository.commit_batch(raw_batch).await.unwrap_err();
    assert!(matches!(
        transactional,
        RepositoryError::CausalWriteRequired { ref table } if table == "todo_views"
    ));
}

#[tokio::test]
async fn coherent_execution_and_graph_snapshots_use_fk_references_and_unique_scope_budgets() {
    let repository = InMemoryRepository::new();
    repository
        .register_projection_models(&topology(), &graph_ownership())
        .await
        .unwrap();
    repository
        .commit_projection(ProjectionCommitBatch {
            input: input(
                1,
                b"graph",
                "graph-message-1",
                "graph-cause-1",
                ProjectionGeneration::initial(),
            ),
            change_epoch: change_epoch(),
            ownership: graph_ownership(),
            mutations: vec![
                graph_mutation("GraphParentView"),
                graph_mutation("GraphChildView"),
            ],
            observations: Vec::new(),
        })
        .await
        .unwrap();

    let execution_request = ProjectionExecutionSnapshotBatchRequest::new(vec![
        ProjectionQuerySnapshotRequest::new(
            &graph_codec(),
            Some(&serde_json::json!("tenant-a")),
            "GraphParentView",
            graph_key("GraphParentView"),
            Vec::new(),
        )
        .unwrap(),
        ProjectionQuerySnapshotRequest::new(
            &graph_codec(),
            Some(&serde_json::json!("tenant-a")),
            "GraphChildView",
            graph_key("GraphChildView"),
            Vec::new(),
        )
        .unwrap(),
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

    let graph = repository
        .projection_graph_snapshot(&graph_snapshot_request(2))
        .await
        .unwrap();
    assert_eq!(graph.includes["children"].rows.len(), 1);
    assert_eq!(graph.includes["featured_children"].rows.len(), 1);
    assert_eq!(
        graph.includes["children"].rows[0].scope, graph.includes["featured_children"].rows[0].scope,
        "two includes may return one shared target scope"
    );
    assert_eq!(
        graph.includes["children"].rows[0]
            .row
            .as_ref()
            .unwrap()
            .get("parent_id"),
        Some(&RowValue::String("parent-1".into())),
        "HasMany follows the target FK reference to root `id`, not the colliding root `parent_id`"
    );

    let error = repository
        .projection_graph_snapshot(&graph_snapshot_request(1))
        .await
        .unwrap_err();
    assert!(
        matches!(error, ProjectionProtocolError::InvalidBatch(ref message)
            if message.contains("returned 2 unique record scopes")
                && message.contains("budget is 1")),
        "{error}"
    );
}

#[tokio::test]
async fn one_four_table_occurrence_commits_rows_and_every_causal_artifact() {
    let repository = InMemoryRepository::new();
    repository
        .register_projection_models(&topology(), &fanout_ownership())
        .await
        .unwrap();
    let trusted = input(
        1,
        b"four-table-occurrence",
        "four-table-message-1",
        "four-table-cause-1",
        ProjectionGeneration::initial(),
    );
    let result = repository
        .commit_projection(ProjectionCommitBatch {
            input: trusted.clone(),
            change_epoch: change_epoch(),
            ownership: fanout_ownership(),
            mutations: (0..fanout_schemas().len()).map(fanout_mutation).collect(),
            observations: (0..fanout_schemas().len())
                .map(|index| ProjectionObservationRequest {
                    kind: ProjectionObservationKind::Record,
                    target: ProjectionObservationTarget::StagedRecord(fanout_scope(index)),
                })
                .collect(),
        })
        .await
        .unwrap();

    assert_eq!(result.outcome, ProjectionCommitOutcome::Applied);
    assert_eq!(result.records.len(), 4);
    assert!(result.records.iter().all(|record| !record.tombstone));
    assert_eq!(
        result
            .changes
            .iter()
            .filter(|change| change.kind == ProjectionChangeKind::RecordUpsert)
            .count(),
        4
    );
    assert_eq!(result.changes.len(), 4);
    assert_eq!(
        result
            .checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.input()),
        Some(&trusted.cursor)
    );
    assert_eq!(repository.inbox_store.read().unwrap().len(), 1);
    assert_eq!(
        repository
            .projection_protocol
            .read()
            .unwrap()
            .observations
            .len(),
        4
    );

    for index in 0..fanout_schemas().len() {
        let schema = &fanout_schemas()[index];
        let snapshot = repository
            .projection_query_snapshot(
                &ProjectionQuerySnapshotRequest::new(
                    &fanout_codec(),
                    Some(&serde_json::json!("tenant-a")),
                    &schema.model_name,
                    fanout_key(index),
                    vec![crate::projection_protocol::ProjectionCheckpointProbe::new(
                        topology(),
                        partition(),
                        source(),
                        ProjectionEpoch::new("source-v1").unwrap(),
                        ProjectionGeneration::initial(),
                    )],
                )
                .unwrap(),
            )
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

#[tokio::test]
async fn four_position_in_memory_projection_commit_failure_matrix_rolls_back_every_side_effect() {
    const PHYSICAL_MUTATION_POSITIONS: usize = 4;

    for fail_at in 0..PHYSICAL_MUTATION_POSITIONS {
        let repository = repository().await;
        let initial = repository
            .commit_projection(batch(
                input(
                    1,
                    b"matrix-initial",
                    "matrix-message-1",
                    "matrix-cause-1",
                    ProjectionGeneration::initial(),
                ),
                (0..PHYSICAL_MUTATION_POSITIONS)
                    .map(|index| {
                        matrix_mutation(index, "before", ProjectionRecordExpectation::Missing)
                    })
                    .collect(),
                Vec::new(),
            ))
            .await
            .unwrap();
        assert_eq!(initial.records.len(), PHYSICAL_MUTATION_POSITIONS);

        {
            let target = matrix_mutation(
                fail_at,
                "after",
                ProjectionRecordExpectation::Exact(initial.records[fail_at].revision.clone()),
            )
            .mutation
            .lock_key();
            repository
                .model_store
                .relational_rows
                .write()
                .unwrap()
                .get_mut(&target)
                .unwrap()
                .version = u64::MAX;
        }
        let physical_before = {
            let rows = repository.model_store.relational_rows.read().unwrap();
            (0..PHYSICAL_MUTATION_POSITIONS)
                .map(|index| {
                    let key = matrix_mutation(
                        index,
                        "ignored",
                        ProjectionRecordExpectation::Exact(initial.records[index].revision.clone()),
                    )
                    .mutation
                    .lock_key();
                    let row = rows.get(&key).unwrap();
                    (key, row.values.clone(), row.version)
                })
                .collect::<Vec<_>>()
        };
        let mut snapshots_before = Vec::new();
        for index in 0..PHYSICAL_MUTATION_POSITIONS {
            snapshots_before.push(
                repository
                    .projection_query_snapshot(&matrix_snapshot_request(index))
                    .await
                    .unwrap(),
            );
        }
        let changes_before = repository
            .projection_changes(&topology(), &partition(), None, 100)
            .await
            .unwrap();
        let inbox_before = repository.inbox_store.read().unwrap().len();
        let observations_before = repository
            .projection_protocol
            .read()
            .unwrap()
            .observations
            .len();

        let retry_input = input(
            2,
            format!("matrix-fail-{fail_at}").as_bytes(),
            &format!("matrix-message-2-{fail_at}"),
            &format!("matrix-cause-2-{fail_at}"),
            ProjectionGeneration::initial(),
        );
        let failed_batch = || ProjectionCommitBatch {
            input: retry_input.clone(),
            change_epoch: change_epoch(),
            ownership: vec![ownership()],
            mutations: (0..PHYSICAL_MUTATION_POSITIONS)
                .map(|index| {
                    matrix_mutation(
                        index,
                        "after",
                        ProjectionRecordExpectation::Exact(initial.records[index].revision.clone()),
                    )
                })
                .collect(),
            observations: vec![ProjectionObservationRequest {
                kind: ProjectionObservationKind::Record,
                target: ProjectionObservationTarget::StagedRecord(matrix_scope(0)),
            }],
        };
        let error = repository
            .commit_projection(failed_batch())
            .await
            .unwrap_err();
        assert!(
            matches!(error, ProjectionProtocolError::Table(TableStoreError::Storage(ref message))
                if message.contains("version overflow")),
            "failure position {fail_at}: {error}"
        );

        {
            let rows = repository.model_store.relational_rows.read().unwrap();
            for (key, values, version) in &physical_before {
                let row = rows.get(key).unwrap();
                assert_eq!(&row.values, values, "failure position {fail_at}");
                assert_eq!(row.version, *version, "failure position {fail_at}");
            }
        }
        for (index, expected) in snapshots_before.iter().enumerate() {
            assert_eq!(
                &repository
                    .projection_query_snapshot(&matrix_snapshot_request(index))
                    .await
                    .unwrap(),
                expected,
                "failure position {fail_at}"
            );
        }
        assert_eq!(
            repository
                .projection_changes(&topology(), &partition(), None, 100)
                .await
                .unwrap(),
            changes_before,
            "failure position {fail_at}"
        );
        assert_eq!(
            repository.inbox_store.read().unwrap().len(),
            inbox_before,
            "failure position {fail_at}"
        );
        assert_eq!(
            repository
                .projection_protocol
                .read()
                .unwrap()
                .observations
                .len(),
            observations_before,
            "failure position {fail_at}"
        );
        assert_eq!(
            repository
                .projection_input_disposition(&retry_input)
                .await
                .unwrap(),
            ProjectionInputDisposition::Pending,
            "failure position {fail_at}"
        );

        let target = matrix_mutation(
            fail_at,
            "after",
            ProjectionRecordExpectation::Exact(initial.records[fail_at].revision.clone()),
        )
        .mutation
        .lock_key();
        repository
            .model_store
            .relational_rows
            .write()
            .unwrap()
            .get_mut(&target)
            .unwrap()
            .version = 1;
        let applied = repository.commit_projection(failed_batch()).await.unwrap();
        assert_eq!(applied.outcome, ProjectionCommitOutcome::Applied);
        let duplicate = repository.commit_projection(failed_batch()).await.unwrap();
        assert_eq!(duplicate.outcome, ProjectionCommitOutcome::Duplicate);
        assert!(duplicate.records.is_empty());
        assert!(duplicate.changes.is_empty());
    }
}
