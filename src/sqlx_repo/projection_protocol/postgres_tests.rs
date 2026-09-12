#[cfg(all(test, feature = "postgres"))]
mod postgres_tests {
    use std::sync::{Arc, LazyLock};
    use std::time::Duration;

    use super::*;
    use crate::command_ledger::{
        CanonicalInputHash, CausalCommitBatch, CausalTransactionalCommit,
        CommandContractFingerprint, CommandId, CommandLedgerKey, CommandLedgerStore,
        CommandReservation, PrincipalPartitionId, ReservationOutcome, TerminalCommandState,
    };
    use crate::projection_protocol::{
        ProjectionCheckpointProbe, ProjectionExecutionSnapshotBatchRequest,
        ProjectionGraphSnapshotRequest, ProjectionQuerySnapshotRequest,
        ProjectionObservationRequest, ProjectionRecordMutation, ProjectionScopeCodec,
    };
    use crate::repository::{CommitBatch, ReadModelWritePlanStore};
    use crate::table::{
        ColumnType, DeleteTableRowMutation, ExpectedVersion, ForeignKey, PrimaryKey,
        RelationshipDef, RelationshipKind, RowKey, RowValue, RowValues, RowWriteMode,
        TableColumn, TableKind, TableRowMutation, TableSchema, TableSchemaRegistry,
        TableStoreError, TableWritePlan,
    };

    static POSTGRES_PROJECTION_TEST_LOCK: tokio::sync::Mutex<()> =
        tokio::sync::Mutex::const_new(());

    fn topology() -> ProjectorTopologyId {
        ProjectorTopologyId::new(1, "postgres_projection_runtime", [71; 32]).unwrap()
    }

    fn partition() -> ProjectionPartition {
        ProjectionScopeCodec::new(topology())
            .encode_partition(Some(&serde_json::json!("postgres-runtime-tenant")))
            .unwrap()
    }

    fn change_epoch() -> ProjectionEpoch {
        ProjectionEpoch::new("postgres-changes-v1").unwrap()
    }

    fn schema() -> &'static TableSchema {
        static SCHEMA: LazyLock<TableSchema> = LazyLock::new(|| TableSchema {
            model_name: "PostgresProjectionView".into(),
            table_name: "postgres_projection_views".into(),
            columns: vec![
                TableColumn {
                    primary_key: true,
                    ..TableColumn::new("id", "id", ColumnType::Text)
                },
                TableColumn::new("value", "value", ColumnType::Text),
            ],
            primary_key: PrimaryKey::new(["id"]),
            version_column: Some(crate::table::DEFAULT_TABLE_VERSION_COLUMN.into()),
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: TableKind::ReadModel,
        });
        &SCHEMA
    }

    fn graph_topology() -> ProjectorTopologyId {
        ProjectorTopologyId::new(1, "postgres_graph_runtime", [72; 32]).unwrap()
    }

    fn graph_partition() -> ProjectionPartition {
        ProjectionScopeCodec::new(graph_topology())
            .encode_partition(Some(&serde_json::json!("postgres-graph-tenant")))
            .unwrap()
    }

    fn graph_parent_schema() -> &'static TableSchema {
        static SCHEMA: LazyLock<TableSchema> = LazyLock::new(|| TableSchema {
            model_name: "PostgresGraphParentView".into(),
            table_name: "postgres_graph_parent_views".into(),
            columns: vec![
                TableColumn {
                    primary_key: true,
                    ..TableColumn::new("id", "id", ColumnType::Text)
                },
                TableColumn::new("parent_id", "parent_id", ColumnType::Text),
            ],
            primary_key: PrimaryKey::new(["id"]),
            version_column: Some(crate::table::DEFAULT_TABLE_VERSION_COLUMN.into()),
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            relationships: vec![
                RelationshipDef {
                    references: None,
                    field_name: "children".into(),
                    kind: RelationshipKind::HasMany,
                    target_model: "PostgresGraphChildView".into(),
                    foreign_key: Some("parent_id".into()),
                    through: None,
                    target_foreign_key: None,
                },
                RelationshipDef {
                    references: None,
                    field_name: "featured_children".into(),
                    kind: RelationshipKind::HasMany,
                    target_model: "PostgresGraphChildView".into(),
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
            model_name: "PostgresGraphChildView".into(),
            table_name: "postgres_graph_child_views".into(),
            columns: vec![
                TableColumn {
                    primary_key: true,
                    ..TableColumn::new("id", "id", ColumnType::Text)
                },
                TableColumn {
                    foreign_key: Some(ForeignKey::new("postgres_graph_parent_views", "id")),
                    ..TableColumn::new("parent_id", "parent_id", ColumnType::Text)
                },
            ],
            primary_key: PrimaryKey::new(["id"]),
            version_column: Some(crate::table::DEFAULT_TABLE_VERSION_COLUMN.into()),
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: TableKind::ReadModel,
        });
        &SCHEMA
    }

    fn graph_ownership() -> Vec<ProjectionModelOwnership> {
        vec![
            ProjectionModelOwnership::new(
                "PostgresGraphParentView",
                "postgres_graph_parent_views",
            )
            .unwrap(),
            ProjectionModelOwnership::new(
                "PostgresGraphChildView",
                "postgres_graph_child_views",
            )
            .unwrap(),
        ]
    }

    fn graph_codec() -> ProjectionScopeCodec {
        ProjectionScopeCodec::with_models(
            graph_topology(),
            [
                ("PostgresGraphParentView", graph_parent_schema()),
                ("PostgresGraphChildView", graph_child_schema()),
            ],
        )
        .unwrap()
    }

    fn graph_key(model: &str) -> RowKey {
        RowKey::new([(
            "id",
            RowValue::String(match model {
                "PostgresGraphParentView" => "parent-1",
                "PostgresGraphChildView" => "child-1",
                other => panic!("unknown PostgreSQL graph model {other}"),
            }
            .into()),
        )])
    }

    fn graph_scope(model: &str) -> ProjectionRecordScope {
        graph_codec()
            .encode_row_scope_in_partition(model, graph_partition(), &graph_key(model))
            .unwrap()
    }

    fn graph_mutation(model: &str) -> ProjectionRecordMutation {
        let (schema, values) = match model {
            "PostgresGraphParentView" => {
                let mut values = RowValues::new();
                values.insert("id", RowValue::String("parent-1".into()));
                values.insert("parent_id", RowValue::String("wrong-parent".into()));
                (graph_parent_schema(), values)
            }
            "PostgresGraphChildView" => {
                let mut values = RowValues::new();
                values.insert("id", RowValue::String("child-1".into()));
                values.insert("parent_id", RowValue::String("parent-1".into()));
                (graph_child_schema(), values)
            }
            other => panic!("unknown PostgreSQL graph model {other}"),
        };
        ProjectionRecordMutation::new(
            graph_scope(model),
            TableMutation::UpsertRow(TableRowMutation {
                schema,
                key: graph_key(model),
                values,
                expected_version: ExpectedVersion::Any,
                mode: RowWriteMode::Upsert,
            }),
            ProjectionRecordExpectation::Missing,
            ProjectionMutationKind::Upsert,
        )
        .unwrap()
    }

    fn graph_input() -> TrustedProjectionInput {
        TrustedProjectionInput::mint(
            ProjectionInputCursor::new(
                graph_topology(),
                graph_partition(),
                ProjectionSource::new("postgres_graph_source", b"parent-1".to_vec()).unwrap(),
                ProjectionEpoch::new("postgres-graph-source-v1").unwrap(),
                1,
            )
            .unwrap(),
            ProjectionInputFingerprint::from_canonical_bytes(b"postgres-graph-input"),
            "postgres-graph-message-1",
            "postgres-graph-cause-1",
            ProjectionGeneration::initial(),
            true,
        )
        .unwrap()
    }

    fn graph_snapshot_request(max_unique: usize) -> ProjectionGraphSnapshotRequest {
        let root = ProjectionQuerySnapshotRequest::new(
            &graph_codec(),
            Some(&serde_json::json!("postgres-graph-tenant")),
            "PostgresGraphParentView",
            graph_key("PostgresGraphParentView"),
            Vec::new(),
        )
        .unwrap();
        ProjectionGraphSnapshotRequest::new(
            root,
            [
                (
                    "children".into(),
                    Arc::new(graph_child_schema().clone()),
                ),
                (
                    "featured_children".into(),
                    Arc::new(graph_child_schema().clone()),
                ),
            ],
            max_unique,
        )
        .unwrap()
    }

    fn ownership() -> ProjectionModelOwnership {
        ProjectionModelOwnership::new("PostgresProjectionView", "postgres_projection_views")
            .unwrap()
    }

    fn scope_codec() -> ProjectionScopeCodec {
        ProjectionScopeCodec::with_models(topology(), [("PostgresProjectionView", schema())])
            .unwrap()
    }

    fn record_key() -> RowKey {
        RowKey::new([("id", RowValue::String("runtime-row".into()))])
    }

    fn matrix_partition(scenario: usize) -> ProjectionPartition {
        ProjectionScopeCodec::new(matrix_topology())
            .encode_partition(Some(&serde_json::json!(format!(
                "postgres-matrix-{scenario}"
            ))))
            .unwrap()
    }

    fn matrix_topology() -> ProjectorTopologyId {
        ProjectorTopologyId::new(1, "postgres_projection_matrix", [73; 32]).unwrap()
    }

    fn matrix_schema() -> &'static TableSchema {
        static SCHEMA: LazyLock<TableSchema> = LazyLock::new(|| TableSchema {
            model_name: "PostgresProjectionMatrixView".into(),
            table_name: "postgres_projection_matrix_views".into(),
            columns: vec![
                TableColumn {
                    primary_key: true,
                    ..TableColumn::new("id", "id", ColumnType::Text)
                },
                TableColumn::new("value", "value", ColumnType::Text),
            ],
            primary_key: PrimaryKey::new(["id"]),
            version_column: Some(crate::table::DEFAULT_TABLE_VERSION_COLUMN.into()),
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: TableKind::ReadModel,
        });
        &SCHEMA
    }

    fn matrix_ownership() -> ProjectionModelOwnership {
        ProjectionModelOwnership::new(
            "PostgresProjectionMatrixView",
            "postgres_projection_matrix_views",
        )
        .unwrap()
    }

    fn matrix_codec() -> ProjectionScopeCodec {
        ProjectionScopeCodec::with_models(
            matrix_topology(),
            [("PostgresProjectionMatrixView", matrix_schema())],
        )
        .unwrap()
    }

    fn matrix_key(scenario: usize, index: usize) -> RowKey {
        RowKey::new([(
            "id",
            RowValue::String(format!("matrix-{scenario}-{index}")),
        )])
    }

    fn matrix_scope(scenario: usize, index: usize) -> ProjectionRecordScope {
        matrix_codec()
            .encode_row_scope_in_partition(
                "PostgresProjectionMatrixView",
                matrix_partition(scenario),
                &matrix_key(scenario, index),
            )
            .unwrap()
    }

    fn matrix_mutation(
        scenario: usize,
        index: usize,
        value: &str,
        expectation: ProjectionRecordExpectation,
    ) -> ProjectionRecordMutation {
        let mut values = RowValues::new();
        values.insert(
            "id",
            RowValue::String(format!("matrix-{scenario}-{index}")),
        );
        values.insert("value", RowValue::String(value.into()));
        ProjectionRecordMutation::new(
            matrix_scope(scenario, index),
            TableMutation::UpsertRow(TableRowMutation {
                schema: matrix_schema(),
                key: matrix_key(scenario, index),
                values,
                expected_version: ExpectedVersion::Any,
                mode: RowWriteMode::Upsert,
            }),
            expectation,
            ProjectionMutationKind::Upsert,
        )
        .unwrap()
    }

    fn matrix_input(scenario: usize, position: u64) -> TrustedProjectionInput {
        TrustedProjectionInput::mint(
            ProjectionInputCursor::new(
                matrix_topology(),
                matrix_partition(scenario),
                ProjectionSource::new(
                    "postgres_matrix_source",
                    format!("matrix-{scenario}").into_bytes(),
                )
                .unwrap(),
                ProjectionEpoch::new("postgres-matrix-source-v1").unwrap(),
                position,
            )
            .unwrap(),
            ProjectionInputFingerprint::from_canonical_bytes(
                format!("postgres-matrix-{scenario}-{position}").as_bytes(),
            ),
            format!("postgres-matrix-message-{scenario}-{position}"),
            format!("postgres-matrix-cause-{scenario}-{position}"),
            ProjectionGeneration::initial(),
            true,
        )
        .unwrap()
    }

    fn matrix_snapshot_request(
        scenario: usize,
        index: usize,
    ) -> ProjectionQuerySnapshotRequest {
        ProjectionQuerySnapshotRequest::new(
            &matrix_codec(),
            Some(&serde_json::json!(format!(
                "postgres-matrix-{scenario}"
            ))),
            "PostgresProjectionMatrixView",
            matrix_key(scenario, index),
            Vec::new(),
        )
        .unwrap()
    }

    fn scope() -> ProjectionRecordScope {
        scope_codec()
            .encode_row_scope(
                "postgres_projection_runtime",
                "PostgresProjectionView",
                Some(&serde_json::json!("postgres-runtime-tenant")),
                &record_key(),
            )
            .unwrap()
    }

    fn source() -> ProjectionSource {
        ProjectionSource::new("postgres_runtime_source", b"runtime-row".to_vec()).unwrap()
    }

    fn snapshot_request(generation: ProjectionGeneration) -> ProjectionQuerySnapshotRequest {
        ProjectionQuerySnapshotRequest::new(
            &scope_codec(),
            Some(&serde_json::json!("postgres-runtime-tenant")),
            "PostgresProjectionView",
            record_key(),
            vec![ProjectionCheckpointProbe::new(
                topology(),
                partition(),
                source(),
                ProjectionEpoch::new("postgres-source-v1").unwrap(),
                generation,
            )],
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
            ProjectionInputCursor::new(
                topology(),
                partition(),
                source(),
                ProjectionEpoch::new("postgres-source-v1").unwrap(),
                position,
            )
            .unwrap(),
            ProjectionInputFingerprint::from_canonical_bytes(fingerprint),
            message_id,
            causation_id,
            generation,
            false,
        )
        .unwrap()
    }

    fn upsert_table_mutation(value: &str) -> TableMutation {
        let mut values = RowValues::new();
        values.insert("id", RowValue::String("runtime-row".into()));
        values.insert("value", RowValue::String(value.into()));
        TableMutation::UpsertRow(TableRowMutation {
            schema: schema(),
            key: record_key(),
            values,
            expected_version: ExpectedVersion::Any,
            mode: RowWriteMode::Upsert,
        })
    }

    fn upsert_mutation(
        value: &str,
        expectation: ProjectionRecordExpectation,
        kind: ProjectionMutationKind,
    ) -> ProjectionRecordMutation {
        ProjectionRecordMutation::new(scope(), upsert_table_mutation(value), expectation, kind)
            .unwrap()
    }

    fn delete_mutation(expected: RecordRevision) -> ProjectionRecordMutation {
        ProjectionRecordMutation::new(
            scope(),
            TableMutation::DeleteRow(DeleteTableRowMutation {
                schema: schema(),
                key: record_key(),
                expected_version: ExpectedVersion::Any,
            }),
            ProjectionRecordExpectation::Exact(expected),
            ProjectionMutationKind::Delete,
        )
        .unwrap()
    }

    fn batch(
        input: TrustedProjectionInput,
        mutations: Vec<ProjectionRecordMutation>,
    ) -> ProjectionCommitBatch {
        ProjectionCommitBatch {
            input,
            change_epoch: change_epoch(),
            ownership: vec![ownership()],
            mutations,
            observations: Vec::new(),
        }
    }

    fn assert_live_snapshot(
        snapshot: &ProjectionQuerySnapshot,
        value: &str,
        incarnation: u64,
        revision: u64,
        source_position: u64,
    ) {
        assert_eq!(
            snapshot.row.as_ref().and_then(|row| row.get("value")),
            Some(&RowValue::String(value.into()))
        );
        let record = snapshot.record.as_ref().expect("live record metadata");
        assert!(!record.tombstone);
        assert_eq!(record.revision.incarnation(), incarnation);
        assert_eq!(record.revision.revision(), revision);
        let checkpoint = snapshot.checkpoints[0]
            .checkpoint
            .as_ref()
            .expect("explicit source checkpoint");
        assert_eq!(checkpoint.input().position(), source_position);
        assert_eq!(checkpoint.change(), &record.change);
        assert_eq!(snapshot.change_head.as_ref(), Some(&record.change));
    }

    #[tokio::test]
    async fn postgres_coherent_execution_and_graph_snapshots_use_fk_references_and_unique_budgets()
    {
        let Ok(database_url) = std::env::var("DISTRIBUTED_TEST_POSTGRES_URL") else {
            return;
        };
        let _test_guard = POSTGRES_PROJECTION_TEST_LOCK.lock().await;
        let repository = SqlxRepository::<sqlx::Postgres>::connect_and_migrate(&database_url)
            .await
            .unwrap();
        let mut registry = TableSchemaRegistry::new();
        registry
            .register_schema(graph_parent_schema().clone())
            .unwrap();
        registry
            .register_schema(graph_child_schema().clone())
            .unwrap();
        repository
            .bootstrap_table_schema_for_dev(&registry)
            .await
            .unwrap();
        repository
            .register_projection_models(&graph_topology(), &graph_ownership())
            .await
            .unwrap();
        repository
            .commit_projection(ProjectionCommitBatch {
                input: graph_input(),
                change_epoch: ProjectionEpoch::new("postgres-graph-changes-v1").unwrap(),
                ownership: graph_ownership(),
                mutations: vec![
                    graph_mutation("PostgresGraphParentView"),
                    graph_mutation("PostgresGraphChildView"),
                ],
                observations: Vec::new(),
            })
            .await
            .unwrap();

        let execution_request = ProjectionExecutionSnapshotBatchRequest::new(vec![
            ProjectionQuerySnapshotRequest::new(
                &graph_codec(),
                Some(&serde_json::json!("postgres-graph-tenant")),
                "PostgresGraphParentView",
                graph_key("PostgresGraphParentView"),
                Vec::new(),
            )
            .unwrap(),
            ProjectionQuerySnapshotRequest::new(
                &graph_codec(),
                Some(&serde_json::json!("postgres-graph-tenant")),
                "PostgresGraphChildView",
                graph_key("PostgresGraphChildView"),
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

        let graph = repository
            .projection_graph_snapshot(&graph_snapshot_request(2))
            .await
            .unwrap();
        assert_eq!(graph.includes["children"].rows.len(), 1);
        assert_eq!(graph.includes["featured_children"].rows.len(), 1);
        assert_eq!(
            graph.includes["children"].rows[0].scope,
            graph.includes["featured_children"].rows[0].scope
        );
        assert_eq!(
            graph.includes["children"].rows[0]
                .row
                .as_ref()
                .unwrap()
                .get("parent_id"),
            Some(&RowValue::String("parent-1".into()))
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
    async fn four_position_postgres_projection_commit_failure_matrix_rolls_back_every_side_effect()
    {
        const PHYSICAL_MUTATION_POSITIONS: usize = 4;
        let Ok(database_url) = std::env::var("DISTRIBUTED_TEST_POSTGRES_URL") else {
            return;
        };
        let _test_guard = POSTGRES_PROJECTION_TEST_LOCK.lock().await;

        for fail_at in 0..PHYSICAL_MUTATION_POSITIONS {
            let repository = SqlxRepository::<sqlx::Postgres>::connect_and_migrate(&database_url)
                .await
                .unwrap();
            let mut registry = TableSchemaRegistry::new();
            registry.register_schema(matrix_schema().clone()).unwrap();
            repository
                .bootstrap_table_schema_for_dev(&registry)
                .await
                .unwrap();
            repository
                .register_projection_models(&matrix_topology(), &[matrix_ownership()])
                .await
                .unwrap();

            let initial_input = matrix_input(fail_at, 1);
            let initial = repository
                .commit_projection(ProjectionCommitBatch {
                    input: initial_input,
                    change_epoch: ProjectionEpoch::new("postgres-matrix-changes-v1").unwrap(),
                    ownership: vec![matrix_ownership()],
                    mutations: (0..PHYSICAL_MUTATION_POSITIONS)
                        .map(|index| {
                            matrix_mutation(
                                fail_at,
                                index,
                                "before",
                                ProjectionRecordExpectation::Missing,
                            )
                        })
                        .collect(),
                    observations: Vec::new(),
                })
                .await
                .unwrap();
            assert_eq!(initial.records.len(), PHYSICAL_MUTATION_POSITIONS);

            sqlx::query(
                "CREATE OR REPLACE FUNCTION distributed_test_fail_projection_matrix() \
                 RETURNS trigger AS $function$ \
                 BEGIN \
                   IF NEW.value = 'after' AND NEW.id IN \
                     ('matrix-0-0', 'matrix-1-1', 'matrix-2-2', 'matrix-3-3') \
                   THEN RAISE EXCEPTION 'forced projection matrix failure'; \
                   END IF; \
                   RETURN NEW; \
                 END; \
                 $function$ LANGUAGE plpgsql",
            )
            .execute(repository.pool())
            .await
            .unwrap();
            sqlx::query(
                "DROP TRIGGER IF EXISTS fail_projection_matrix_position \
                 ON postgres_projection_matrix_views",
            )
            .execute(repository.pool())
            .await
            .unwrap();
            sqlx::query(
                "CREATE TRIGGER fail_projection_matrix_position \
                 BEFORE UPDATE ON postgres_projection_matrix_views FOR EACH ROW \
                 EXECUTE FUNCTION distributed_test_fail_projection_matrix()",
            )
            .execute(repository.pool())
            .await
            .unwrap();

            let pattern = format!("matrix-{fail_at}-%");
            let physical_before = sqlx::query(
                "SELECT id, value, _sourced_version FROM postgres_projection_matrix_views \
                 WHERE id LIKE $1 ORDER BY id",
            )
            .bind(&pattern)
            .fetch_all(repository.pool())
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                (
                    row.try_get::<String, _>("id").unwrap(),
                    row.try_get::<String, _>("value").unwrap(),
                    row.try_get::<i64, _>("_sourced_version").unwrap(),
                )
            })
            .collect::<Vec<_>>();
            let mut snapshots_before = Vec::new();
            for index in 0..PHYSICAL_MUTATION_POSITIONS {
                snapshots_before.push(
                    repository
                        .projection_query_snapshot(&matrix_snapshot_request(fail_at, index))
                        .await
                        .unwrap(),
                );
            }
            let changes_before = repository
                .projection_changes(&matrix_topology(), &matrix_partition(fail_at), None, 100)
                .await
                .unwrap();
            let retry_input = matrix_input(fail_at, 2);
            let checkpoint_before = repository
                .projection_checkpoint(&retry_input.cursor, ProjectionGeneration::initial())
                .await
                .unwrap();
            let mut local_changes = repository.read_model_changes();
            let mut pg_changes = sqlx::postgres::PgListener::connect(&database_url)
                .await
                .unwrap();
            pg_changes
                .listen("distributed_read_model_changes")
                .await
                .unwrap();

            let failed_batch = || ProjectionCommitBatch {
                input: retry_input.clone(),
                change_epoch: ProjectionEpoch::new("postgres-matrix-changes-v1").unwrap(),
                ownership: vec![matrix_ownership()],
                mutations: (0..PHYSICAL_MUTATION_POSITIONS)
                    .map(|index| {
                        matrix_mutation(
                            fail_at,
                            index,
                            "after",
                            ProjectionRecordExpectation::Exact(
                                initial.records[index].revision.clone(),
                            ),
                        )
                    })
                    .collect(),
                observations: vec![ProjectionObservationRequest {
                    kind: ProjectionObservationKind::Record,
                    target: ProjectionObservationTarget::StagedRecord(matrix_scope(fail_at, 0)),
                }],
            };
            assert!(
                matches!(
                    repository.commit_projection(failed_batch()).await,
                    Err(ProjectionProtocolError::Table(TableStoreError::BackendStorage {
                        ..
                    }))
                ),
                "failure position {fail_at}"
            );
            assert!(matches!(
                local_changes.try_recv(),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty)
            ));
            assert!(
                tokio::time::timeout(Duration::from_millis(150), pg_changes.recv())
                    .await
                    .is_err(),
                "failure position {fail_at} emitted a rolled-back pg_notify"
            );

            let physical_after = sqlx::query(
                "SELECT id, value, _sourced_version FROM postgres_projection_matrix_views \
                 WHERE id LIKE $1 ORDER BY id",
            )
            .bind(&pattern)
            .fetch_all(repository.pool())
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                (
                    row.try_get::<String, _>("id").unwrap(),
                    row.try_get::<String, _>("value").unwrap(),
                    row.try_get::<i64, _>("_sourced_version").unwrap(),
                )
            })
            .collect::<Vec<_>>();
            assert_eq!(physical_after, physical_before, "failure position {fail_at}");
            for (index, expected) in snapshots_before.iter().enumerate() {
                assert_eq!(
                    &repository
                        .projection_query_snapshot(&matrix_snapshot_request(fail_at, index))
                        .await
                        .unwrap(),
                    expected,
                    "failure position {fail_at}"
                );
            }
            assert_eq!(
                repository
                    .projection_changes(&matrix_topology(), &matrix_partition(fail_at), None, 100)
                    .await
                    .unwrap(),
                changes_before,
                "failure position {fail_at}"
            );
            assert_eq!(
                repository
                    .projection_checkpoint(&retry_input.cursor, ProjectionGeneration::initial())
                    .await
                    .unwrap(),
                checkpoint_before
            );
            assert_eq!(
                repository
                    .projection_input_disposition(&retry_input)
                    .await
                    .unwrap(),
                ProjectionInputDisposition::Pending
            );
            assert_eq!(
                repository
                    .projection_observation(
                        &retry_input.causation_id,
                        &matrix_scope(fail_at, 0),
                        ProjectionObservationKind::Record,
                    )
                    .await
                    .unwrap(),
                None
            );
            assert_eq!(
                sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM projection_input_receipts WHERE message_id = $1",
                )
                .bind(&retry_input.message_id)
                .fetch_one(repository.pool())
                .await
                .unwrap(),
                0
            );
            assert_eq!(
                sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM consumer_inbox WHERE message_id = $1",
                )
                .bind(&retry_input.message_id)
                .fetch_one(repository.pool())
                .await
                .unwrap(),
                0
            );

            sqlx::query(
                "DROP TRIGGER fail_projection_matrix_position ON postgres_projection_matrix_views",
            )
            .execute(repository.pool())
            .await
            .unwrap();
            let applied = repository.commit_projection(failed_batch()).await.unwrap();
            assert_eq!(applied.outcome, ProjectionCommitOutcome::Applied);
            let duplicate = repository.commit_projection(failed_batch()).await.unwrap();
            assert_eq!(duplicate.outcome, ProjectionCommitOutcome::Duplicate);
            assert!(duplicate.records.is_empty());
            assert!(duplicate.changes.is_empty());
        }
    }

    #[tokio::test]
    async fn postgres_projection_protocol_runtime_conforms() {
        let Ok(database_url) = std::env::var("DISTRIBUTED_TEST_POSTGRES_URL") else {
            return;
        };
        let _test_guard = POSTGRES_PROJECTION_TEST_LOCK.lock().await;
        let repository = SqlxRepository::<sqlx::Postgres>::connect_and_migrate(&database_url)
            .await
            .unwrap()
            .with_projection_change_retention(ProjectionChangeRetention::new(3).unwrap());
        let mut registry = TableSchemaRegistry::new();
        registry.register_schema(schema().clone()).unwrap();
        repository
            .bootstrap_table_schema_for_dev(&registry)
            .await
            .unwrap();

        let mut raw_first_tx = repository.pool().begin().await.unwrap();
        let ownership_tables = BTreeSet::from(["postgres_projection_views".to_string()]);
        lock_projection_table_ownership_fences_in_tx(&mut raw_first_tx, &ownership_tables)
            .await
            .unwrap();
        let registration_repository = repository.clone();
        let racing_registration = tokio::spawn(async move {
            registration_repository
                .register_projection_models(&topology(), &[ownership()])
                .await
        });
        tokio::task::yield_now().await;
        assert!(!racing_registration.is_finished());
        apply_read_model_write_plan_in_tx(
            &mut raw_first_tx,
            TableWritePlan::new(vec![upsert_table_mutation("raw-first")]),
        )
        .await
        .unwrap();
        raw_first_tx.commit().await.unwrap();
        assert!(matches!(
            racing_registration.await.unwrap(),
            Err(ProjectionProtocolError::InvalidBatch(message))
                if message.contains("unverified legacy rows")
        ));
        sqlx::query("DELETE FROM postgres_projection_views WHERE id = $1")
            .bind("runtime-row")
            .execute(repository.pool())
            .await
            .unwrap();
        repository
            .register_projection_models(&topology(), &[ownership()])
            .await
            .unwrap();

        assert_eq!(
            repository
                .projection_partition_runtime_state(&topology(), &partition())
                .await
                .unwrap(),
            None
        );
        assert!(matches!(
            repository
                .commit_write_plan(TableWritePlan::new(vec![upsert_table_mutation("raw")]))
                .await,
            Err(TableStoreError::CausalWriteRequired { ref table })
                if table == "postgres_projection_views"
        ));

        let mut local_changes = repository.read_model_changes();
        let mut pg_changes = sqlx::postgres::PgListener::connect(&database_url)
            .await
            .unwrap();
        pg_changes
            .listen("distributed_read_model_changes")
            .await
            .unwrap();
        let initial_input = input(
            5,
            b"postgres-checkpoint-5",
            "postgres-message-5",
            "postgres-cause-5",
            ProjectionGeneration::initial(),
        );
        assert_eq!(
            repository
                .projection_input_disposition(&initial_input)
                .await
                .unwrap(),
            ProjectionInputDisposition::Pending
        );
        let mut initial = batch(
            initial_input.clone(),
            vec![upsert_mutation(
                "5",
                ProjectionRecordExpectation::Missing,
                ProjectionMutationKind::Upsert,
            )],
        );
        initial.observations = vec![crate::projection_protocol::ProjectionObservationRequest {
            kind: ProjectionObservationKind::Record,
            target: ProjectionObservationTarget::StagedRecord(scope()),
        }];
        assert_eq!(
            repository.commit_projection(initial).await.unwrap().outcome,
            ProjectionCommitOutcome::Applied
        );
        assert!(matches!(
            repository
                .projection_input_disposition(&initial_input)
                .await
                .unwrap(),
            ProjectionInputDisposition::Duplicate(checkpoint)
                if checkpoint.input().position() == 5
        ));
        let local_change = tokio::time::timeout(Duration::from_secs(2), local_changes.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(local_change.tables.contains(PROJECTION_CHANGE_NOTIFY_TABLE));
        tokio::time::timeout(Duration::from_secs(2), pg_changes.recv())
            .await
            .expect("PostgreSQL notifies only after the projection transaction commits")
            .unwrap();
        assert_live_snapshot(
            &repository
                .projection_query_snapshot(&snapshot_request(ProjectionGeneration::initial()))
                .await
                .unwrap(),
            "5",
            1,
            1,
            5,
        );
        let causal_probe = crate::projection_protocol::ProjectionObligationEvidenceRequest::new(
            "postgres-cause-5",
            scope(),
            ProjectionObservationKind::Record,
        )
        .unwrap();
        assert!(matches!(
            repository
                .projection_obligation_evidence_batch(
                    &ProjectionObligationEvidenceBatchRequest::new(vec![causal_probe.clone()])
                        .unwrap(),
                )
                .await
                .unwrap()
                .evidence
                .as_slice(),
            [ProjectionObligationEvidence::Observed(_)]
        ));

        assert_eq!(
            repository
                .commit_projection(batch(
                    input(
                        5,
                        b"postgres-checkpoint-5",
                        "postgres-message-5",
                        "postgres-cause-5",
                        ProjectionGeneration::initial(),
                    ),
                    vec![upsert_mutation(
                        "ignored-duplicate",
                        ProjectionRecordExpectation::Missing,
                        ProjectionMutationKind::Upsert,
                    )],
                ))
                .await
                .unwrap()
                .outcome,
            ProjectionCommitOutcome::Duplicate
        );
        assert!(matches!(
            repository
                .commit_projection(batch(
                    input(
                        5,
                        b"postgres-corrupt-5",
                        "postgres-message-5",
                        "postgres-cause-5",
                        ProjectionGeneration::initial(),
                    ),
                    Vec::new(),
                ))
                .await,
            Err(ProjectionProtocolError::InputCorruption)
        ));
        let remapped_partition = ProjectionScopeCodec::new(topology())
            .encode_partition(Some(&serde_json::json!("postgres-other-tenant")))
            .unwrap();
        let remapped_message = TrustedProjectionInput::mint(
            ProjectionInputCursor::new(
                topology(),
                remapped_partition,
                source(),
                ProjectionEpoch::new("postgres-source-v1").unwrap(),
                5,
            )
            .unwrap(),
            ProjectionInputFingerprint::from_canonical_bytes(b"postgres-checkpoint-5"),
            "postgres-message-5",
            "postgres-cause-5",
            ProjectionGeneration::initial(),
            false,
        )
        .unwrap();
        assert!(matches!(
            repository
                .commit_projection(batch(remapped_message, Vec::new()))
                .await,
            Err(ProjectionProtocolError::MessageIdReuse { message_id })
                if message_id == "postgres-message-5"
        ));
        let stale_input = input(
            4,
            b"postgres-stale-4",
            "postgres-message-4",
            "postgres-cause-4",
            ProjectionGeneration::initial(),
        );
        assert_eq!(
            repository
                .commit_projection(batch(stale_input.clone(), Vec::new()))
                .await
                .unwrap()
                .outcome,
            ProjectionCommitOutcome::StaleInput
        );
        assert!(matches!(
            repository
                .projection_input_disposition(&stale_input)
                .await
                .unwrap(),
            ProjectionInputDisposition::Stale(checkpoint)
                if checkpoint.input().position() == 5
        ));

        let created = repository
            .projection_record(&scope())
            .await
            .unwrap()
            .unwrap();
        let deleted = repository
            .commit_projection(batch(
                input(
                    6,
                    b"postgres-delete-6",
                    "postgres-message-6",
                    "postgres-cause-6",
                    ProjectionGeneration::initial(),
                ),
                vec![delete_mutation(created.revision)],
            ))
            .await
            .unwrap();
        assert!(deleted.records[0].tombstone);
        assert!(repository
            .projection_query_snapshot(&snapshot_request(ProjectionGeneration::initial()))
            .await
            .unwrap()
            .row
            .is_none());
        let recreated = repository
            .commit_projection(batch(
                input(
                    7,
                    b"postgres-recreate-7",
                    "postgres-message-7",
                    "postgres-cause-7",
                    ProjectionGeneration::initial(),
                ),
                vec![upsert_mutation(
                    "7",
                    ProjectionRecordExpectation::Exact(deleted.records[0].revision.clone()),
                    ProjectionMutationKind::Recreate,
                )],
            ))
            .await
            .unwrap();
        assert_eq!(recreated.records[0].revision.incarnation(), 2);
        assert_eq!(recreated.records[0].revision.revision(), 1);

        let failed_input = input(
            9,
            b"postgres-failure-9",
            "postgres-message-9",
            "postgres-cause-5",
            ProjectionGeneration::initial(),
        );
        let failure = repository
            .record_projection_failure(
                ProjectionFailureBatch::new(
                    failed_input.clone(),
                    change_epoch(),
                    "postgres-failure-9",
                    "decode_error",
                    b"bad postgres payload".to_vec(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            repository
                .projection_input_disposition(&failed_input)
                .await,
            Err(ProjectionProtocolError::PartitionStopped { failure_id })
                if failure_id == "postgres-failure-9"
        ));
        assert!(!failure.gap_free);
        let stopped = repository
            .projection_partition_runtime_state(&topology(), &partition())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stopped.active_generation, ProjectionGeneration::initial());
        assert_eq!(
            stopped.stopped_failure_id.as_deref(),
            Some("postgres-failure-9")
        );
        assert_eq!(stopped.pending_retry, None);
        let generation = repository
            .repair_projection(&topology(), &partition(), "postgres-failure-9")
            .await
            .unwrap();
        let retry_input = input(
            9,
            b"postgres-failure-9",
            "postgres-message-9",
            "postgres-cause-5",
            generation,
        );
        assert_eq!(
            repository
                .projection_input_disposition(&retry_input)
                .await
                .unwrap(),
            ProjectionInputDisposition::Pending
        );
        let repaired = repository
            .projection_partition_runtime_state(&topology(), &partition())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(repaired.active_generation, generation);
        assert_eq!(repaired.stopped_failure_id, None);
        let pending_retry = repaired.pending_retry.unwrap();
        assert_eq!(pending_retry.failure_id, failure.failure_id);
        assert_eq!(pending_retry.input, failure.input);
        assert_eq!(pending_retry.input_fingerprint, failure.input_fingerprint);
        assert_eq!(pending_retry.message_id, failure.message_id);
        assert_eq!(pending_retry.causation_id, failure.causation_id);
        assert_eq!(pending_retry.failed_generation, failure.generation);
        assert_eq!(pending_retry.gap_free, failure.gap_free);
        assert!(matches!(
            repository
                .commit_projection(batch(
                    input(
                        10,
                        b"postgres-later-10",
                        "postgres-message-10",
                        "postgres-cause-10",
                        generation,
                    ),
                    vec![upsert_mutation(
                        "must-not-run-before-retry",
                        ProjectionRecordExpectation::Exact(recreated.records[0].revision.clone(),),
                        ProjectionMutationKind::Upsert,
                    )],
                ))
                .await,
            Err(ProjectionProtocolError::IncomparableInput)
        ));
        assert_eq!(
            repository
                .commit_projection(batch(retry_input, Vec::new()))
                .await
                .unwrap()
                .outcome,
            ProjectionCommitOutcome::Applied
        );
        assert_eq!(
            repository
                .commit_projection(batch(
                    input(
                        10,
                        b"postgres-later-10",
                        "postgres-message-10",
                        "postgres-cause-10",
                        generation,
                    ),
                    vec![upsert_mutation(
                        "10",
                        ProjectionRecordExpectation::Exact(recreated.records[0].revision.clone()),
                        ProjectionMutationKind::Upsert,
                    )],
                ))
                .await
                .unwrap()
                .outcome,
            ProjectionCommitOutcome::Applied
        );

        let before_rollback = repository
            .projection_query_snapshot(&snapshot_request(generation))
            .await
            .unwrap();
        assert_live_snapshot(&before_rollback, "10", 2, 2, 10);
        let before_rollback_inbox: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM consumer_inbox")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        sqlx::query(
            "CREATE OR REPLACE FUNCTION fail_projection_record_write() RETURNS trigger \
             LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'forced projection record failure'; \
             END; $$",
        )
        .execute(repository.pool())
        .await
        .unwrap();
        sqlx::query(
            "CREATE TRIGGER fail_projection_record_write \
             BEFORE INSERT OR UPDATE ON projection_records FOR EACH ROW \
             EXECUTE FUNCTION fail_projection_record_write()",
        )
        .execute(repository.pool())
        .await
        .unwrap();
        let mut rollback_local_changes = repository.read_model_changes();
        let mut rollback_pg_changes = sqlx::postgres::PgListener::connect(&database_url)
            .await
            .unwrap();
        rollback_pg_changes
            .listen("distributed_read_model_changes")
            .await
            .unwrap();
        assert!(repository
            .commit_projection(batch(
                input(
                    11,
                    b"postgres-rollback-11",
                    "postgres-message-11",
                    "postgres-cause-11",
                    generation,
                ),
                vec![upsert_mutation(
                    "must-roll-back",
                    ProjectionRecordExpectation::Exact(
                        before_rollback.record.as_ref().unwrap().revision.clone(),
                    ),
                    ProjectionMutationKind::Upsert,
                )],
            ))
            .await
            .is_err());
        assert_eq!(
            rollback_local_changes.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(150), rollback_pg_changes.recv())
                .await
                .is_err(),
            "a rolled-back pg_notify must never be delivered"
        );
        assert_eq!(
            repository
                .projection_query_snapshot(&snapshot_request(generation))
                .await
                .unwrap(),
            before_rollback
        );
        let after_rollback_inbox: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM consumer_inbox")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        assert_eq!(after_rollback_inbox, before_rollback_inbox);
        let rolled_back_receipt: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM projection_input_receipts WHERE message_id = $1",
        )
        .bind("postgres-message-11")
        .fetch_one(repository.pool())
        .await
        .unwrap();
        assert_eq!(rolled_back_receipt, 0);
        sqlx::query("DROP TRIGGER fail_projection_record_write ON projection_records")
            .execute(repository.pool())
            .await
            .unwrap();
        sqlx::query("DROP FUNCTION fail_projection_record_write()")
            .execute(repository.pool())
            .await
            .unwrap();

        let successful_11 = repository
            .commit_projection(batch(
                input(
                    11,
                    b"postgres-rollback-11",
                    "postgres-message-11",
                    "postgres-cause-11",
                    generation,
                ),
                vec![upsert_mutation(
                    "11",
                    ProjectionRecordExpectation::Exact(
                        before_rollback.record.as_ref().unwrap().revision.clone(),
                    ),
                    ProjectionMutationKind::Upsert,
                )],
            ))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), rollback_local_changes.recv())
            .await
            .expect("local change is published after commit")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), rollback_pg_changes.recv())
            .await
            .expect("PostgreSQL notification is delivered after commit")
            .unwrap();
        let committed_11 = repository
            .projection_query_snapshot(&snapshot_request(generation))
            .await
            .unwrap();
        assert_live_snapshot(&committed_11, "11", 2, 3, 11);

        let repeatable_request = snapshot_request(generation);
        let repeatable_live_request = ProjectionLiveRecordBatchRequest::new(vec![
            crate::projection_protocol::ProjectionLiveRecordRequest::new(
                &scope_codec(),
                "PostgresProjectionView",
                record_key(),
            )
            .unwrap(),
        ])
        .unwrap();
        let repeatable_expectation = successful_11.records[0].revision.clone();
        let writer_repository = repository.clone();
        let (inside_before, inside_live_before, inside_after, inside_live_after) =
            with_projection_read_snapshot(repository.pool(), move |connection| {
                Box::pin(async move {
                    let before = read_projection_query_snapshot_in_executor::<sqlx::Postgres, _>(
                        &mut *connection,
                        &repeatable_request,
                    )
                    .await?;
                    let live_before =
                        read_projection_live_record_batch_in_executor::<sqlx::Postgres>(
                            &mut *connection,
                            &repeatable_live_request,
                        )
                        .await?;
                    writer_repository
                        .commit_projection(batch(
                            input(
                                12,
                                b"postgres-repeatable-12",
                                "postgres-message-12",
                                "postgres-cause-12",
                                generation,
                            ),
                            vec![upsert_mutation(
                                "12",
                                ProjectionRecordExpectation::Exact(repeatable_expectation),
                                ProjectionMutationKind::Upsert,
                            )],
                        ))
                        .await?;
                    let after = read_projection_query_snapshot_in_executor::<sqlx::Postgres, _>(
                        &mut *connection,
                        &repeatable_request,
                    )
                    .await?;
                    let live_after =
                        read_projection_live_record_batch_in_executor::<sqlx::Postgres>(
                            &mut *connection,
                            &repeatable_live_request,
                        )
                        .await?;
                    Ok((before, live_before, after, live_after))
                })
            })
            .await
            .unwrap();
        assert_eq!(inside_after, inside_before);
        assert_eq!(inside_live_after, inside_live_before);
        assert_eq!(
            inside_live_before.records[0], inside_before.record,
            "physical query and magic live-record evidence share one repeatable snapshot"
        );
        assert_live_snapshot(&inside_before, "11", 2, 3, 11);
        let committed_12 = repository
            .projection_query_snapshot(&snapshot_request(generation))
            .await
            .unwrap();
        assert_live_snapshot(&committed_12, "12", 2, 4, 12);
        let committed_live = repository
            .projection_live_record_batch(
                &ProjectionLiveRecordBatchRequest::new(vec![
                    crate::projection_protocol::ProjectionLiveRecordRequest::new(
                        &scope_codec(),
                        "PostgresProjectionView",
                        record_key(),
                    )
                    .unwrap(),
                ])
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(committed_live.records[0], committed_12.record);

        let retained_failure_change: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM projection_changes WHERE failure_id = $1")
                .bind(failure.failure_id.as_str())
                .fetch_one(repository.pool())
                .await
                .unwrap();
        assert_eq!(
            retained_failure_change, 0,
            "bounded retention must compact the failure change before evidence lookup"
        );
        assert!(matches!(
            repository
                .projection_obligation_evidence_batch(
                    &ProjectionObligationEvidenceBatchRequest::new(vec![causal_probe]).unwrap(),
                )
                .await
                .unwrap()
                .evidence
                .as_slice(),
            [ProjectionObligationEvidence::TerminalFailure(stored)]
                if stored == &failure
        ));

        let physical_version: i64 = sqlx::query_scalar(
            "SELECT _sourced_version FROM postgres_projection_views WHERE id = $1",
        )
        .bind("runtime-row")
        .fetch_one(repository.pool())
        .await
        .unwrap();
        sqlx::query("DELETE FROM postgres_projection_views WHERE id = $1")
            .bind("runtime-row")
            .execute(repository.pool())
            .await
            .unwrap();
        assert!(matches!(
            repository
                .projection_query_snapshot(&snapshot_request(generation))
                .await,
            Err(ProjectionProtocolError::RecordMissing { .. })
        ));
        sqlx::query(
            "INSERT INTO postgres_projection_views (id, value, _sourced_version) \
             VALUES ($1, $2, $3)",
        )
        .bind("runtime-row")
        .bind("12")
        .bind(physical_version)
        .execute(repository.pool())
        .await
        .unwrap();
        assert_eq!(
            repository
                .projection_query_snapshot(&snapshot_request(generation))
                .await
                .unwrap(),
            committed_12
        );

        let retention = Duration::from_secs(3600);
        let reservation = CommandReservation::new(
            CommandLedgerKey::new(
                "postgres-projection-runtime",
                PrincipalPartitionId::new("tenant:postgres-direct").unwrap(),
                CommandId::parse(uuid::Uuid::now_v7().hyphenated().to_string()).unwrap(),
            )
            .unwrap(),
            "postgres-project-view",
            CommandContractFingerprint::new([81; 32]),
            CanonicalInputHash::new([82; 32]),
            Duration::from_secs(30),
            retention,
        )
        .unwrap();
        let attempt = match repository.reserve_command(reservation).await.unwrap() {
            ReservationOutcome::Acquired(attempt) => attempt,
            _ => panic!("fresh PostgreSQL command must acquire its first attempt"),
        };
        let causation_id = attempt.causation_id().as_str().to_string();
        let completion = attempt
            .complete(
                TerminalCommandState::Atomic,
                serde_json::json!({"postgres_projected": true}),
                retention,
            )
            .unwrap();
        let direct = SameTransactionProjectionBatch::single_upsert(
            topology(),
            partition(),
            change_epoch(),
            ownership(),
            scope(),
            upsert_table_mutation("direct"),
            causation_id,
        )
        .unwrap();
        repository
            .commit_causal_batch(CausalCommitBatch::with_direct_projection(
                CommitBatch::empty(),
                completion,
                direct,
            ))
            .await
            .unwrap();
        let after_direct = repository
            .projection_record(&scope())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after_direct.revision.incarnation(), 2);
        assert_eq!(after_direct.revision.revision(), 5);

        let (head, compacted_through) = match repository
            .projection_changes(&topology(), &partition(), None, 100)
            .await
            .unwrap()
        {
            ProjectionChangeRead::ResetRequired {
                head,
                compacted_through,
            } => (head.unwrap(), compacted_through),
            other => panic!("retention must require reset from origin: {other:?}"),
        };
        assert!(compacted_through > 0);
        assert_eq!(head.position(), compacted_through + 3);
        let boundary =
            ProjectionChangeCursor::new(topology(), partition(), change_epoch(), compacted_through)
                .unwrap();
        match repository
            .projection_changes(&topology(), &partition(), Some(&boundary), 100)
            .await
            .unwrap()
        {
            ProjectionChangeRead::Changes {
                head: retained_head,
                compacted_through: retained_watermark,
                changes,
            } => {
                assert_eq!(retained_head, Some(head));
                assert_eq!(retained_watermark, compacted_through);
                assert_eq!(changes.len(), 3);
                assert_eq!(changes[0].cursor.position(), compacted_through + 1);
            }
            other => panic!("exact compacted boundary must resume retained suffix: {other:?}"),
        }

        let resume_after = ProjectionChangeCursor::new(
            topology(),
            partition(),
            change_epoch(),
            compacted_through + 1,
        )
        .unwrap();
        let compact_through = ProjectionChangeCursor::new(
            topology(),
            partition(),
            change_epoch(),
            compacted_through + 2,
        )
        .unwrap();
        let reader_pool = repository.pool().clone();
        let raced_resume_after = resume_after.clone();
        let (state_observed_tx, state_observed_rx) = tokio::sync::oneshot::channel();
        let (compaction_committed_tx, compaction_committed_rx) = tokio::sync::oneshot::channel();
        let reader = tokio::spawn(async move {
            read_projection_changes_in_snapshot(
                &reader_pool,
                topology(),
                partition(),
                Some(raced_resume_after),
                100,
                async move {
                    state_observed_tx
                        .send(())
                        .expect("PostgreSQL resume reader reports its established snapshot");
                    compaction_committed_rx
                        .await
                        .expect("PostgreSQL compaction completion reaches resume reader");
                },
            )
            .await
        });

        state_observed_rx
            .await
            .expect("PostgreSQL resume reader establishes its snapshot");
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(5),
                repository.compact_projection_changes(&compact_through),
            )
            .await
            .expect("PostgreSQL compaction commits while repeatable reader remains open")
            .unwrap(),
            compacted_through + 2
        );
        compaction_committed_tx
            .send(())
            .expect("PostgreSQL resume reader remains active after compaction");

        match reader.await.unwrap().unwrap() {
            ProjectionChangeRead::Changes {
                head,
                compacted_through: raced_watermark,
                changes,
            } => {
                assert_eq!(
                    head.as_ref().map(ProjectionChangeCursor::position),
                    Some(compacted_through + 3)
                );
                assert_eq!(raced_watermark, compacted_through);
                assert_eq!(
                    changes
                        .iter()
                        .map(|change| change.cursor.position())
                        .collect::<Vec<_>>(),
                    vec![compacted_through + 2, compacted_through + 3],
                    "repeatable snapshot returns the complete pre-compaction suffix"
                );
            }
            other => {
                panic!("PostgreSQL repeatable resume must return its complete page: {other:?}")
            }
        }
        assert!(matches!(
            repository
                .projection_changes(&topology(), &partition(), Some(&resume_after), 100)
                .await
                .unwrap(),
            ProjectionChangeRead::ResetRequired {
                compacted_through: fresh_watermark,
                ..
            } if fresh_watermark == compacted_through + 2
        ));
        assert!(matches!(
            repository
                .projection_changes(&topology(), &partition(), Some(&compact_through), 100)
                .await
                .unwrap(),
            ProjectionChangeRead::Changes {
                compacted_through: fresh_watermark,
                ref changes,
                ..
            } if fresh_watermark == compacted_through + 2
                && changes.len() == 1
                && changes[0].cursor.position() == compacted_through + 3
        ));
    }
}
