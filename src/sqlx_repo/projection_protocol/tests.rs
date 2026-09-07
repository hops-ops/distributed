#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, LazyLock};
    use std::time::Duration;

    use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
    use sqlx::Row;

    use super::*;
    use crate::command_ledger::{
        CanonicalInputHash, CausalCommitBatch, CausalTransactionalCommit,
        CommandContractFingerprint, CommandId, CommandLedgerError, CommandLedgerKey,
        CommandLedgerStore, CommandLookup, CommandLookupScope, CommandReservation,
        PrincipalPartitionId, ReservationOutcome, TerminalCommandState,
    };
    use crate::projection_protocol::{
        ProjectionCheckpointProbe, ProjectionExecutionSnapshotBatchRequest,
        ProjectionGraphSnapshotRequest, ProjectionObservationRequest,
        ProjectionQuerySnapshotRequest, ProjectionRecordMutation, ProjectionScopeCodec,
    };
    use crate::repository::{CommitBatch, ReadModelWritePlanStore, TransactionalCommit};
    use crate::table::{
        ColumnType, DeleteTableRowMutation, ExpectedVersion, ForeignKey, PrimaryKey,
        RelationshipDef, RelationshipKind, RowKey, RowValue, RowValues, RowWriteMode,
        TableColumn, TableKind, TableRowMutation, TableSchema, TableSchemaRegistry,
    };

    fn topology() -> ProjectorTopologyId {
        ProjectorTopologyId::new(1, "sql_todo_projector", [17; 32]).unwrap()
    }

    fn other_topology() -> ProjectorTopologyId {
        ProjectorTopologyId::new(1, "other_sql_todo_projector", [99; 32]).unwrap()
    }

    fn partition() -> ProjectionPartition {
        ProjectionScopeCodec::new(topology())
            .encode_partition(Some(&serde_json::json!("tenant-sql")))
            .unwrap()
    }

    fn source(name: &str, key: &[u8]) -> ProjectionSource {
        ProjectionSource::new(name, key.to_vec()).unwrap()
    }

    fn input_cursor_for(
        source: ProjectionSource,
        position: u64,
        source_epoch: &str,
    ) -> ProjectionInputCursor {
        ProjectionInputCursor::new(
            topology(),
            partition(),
            source,
            ProjectionEpoch::new(source_epoch).unwrap(),
            position,
        )
        .unwrap()
    }

    fn input_cursor(position: u64) -> ProjectionInputCursor {
        input_cursor_for(source("todo_stream", b"todo-1"), position, "source-v1")
    }

    fn input(
        position: u64,
        fingerprint: &[u8],
        message_id: &str,
        causation_id: &str,
        generation: ProjectionGeneration,
    ) -> TrustedProjectionInput {
        TrustedProjectionInput::mint(
            input_cursor(position),
            ProjectionInputFingerprint::from_canonical_bytes(fingerprint),
            message_id,
            causation_id,
            generation,
            true,
        )
        .unwrap()
    }

    fn non_gap_input(
        position: u64,
        fingerprint: &[u8],
        message_id: &str,
        causation_id: &str,
        generation: ProjectionGeneration,
    ) -> TrustedProjectionInput {
        TrustedProjectionInput::mint(
            input_cursor(position),
            ProjectionInputFingerprint::from_canonical_bytes(fingerprint),
            message_id,
            causation_id,
            generation,
            false,
        )
        .unwrap()
    }

    fn input_for_source(
        source: ProjectionSource,
        position: u64,
        fingerprint: &[u8],
        message_id: &str,
        causation_id: &str,
    ) -> TrustedProjectionInput {
        TrustedProjectionInput::mint(
            input_cursor_for(source, position, "source-v1"),
            ProjectionInputFingerprint::from_canonical_bytes(fingerprint),
            message_id,
            causation_id,
            ProjectionGeneration::initial(),
            true,
        )
        .unwrap()
    }

    fn change_epoch() -> ProjectionEpoch {
        ProjectionEpoch::new("changes-v1").unwrap()
    }

    fn schema() -> &'static TableSchema {
        static SCHEMA: LazyLock<TableSchema> = LazyLock::new(|| TableSchema {
            model_name: "SqlTodoView".into(),
            table_name: "sql_todo_views".into(),
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
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: TableKind::ReadModel,
        });
        &SCHEMA
    }

    fn graph_parent_schema() -> &'static TableSchema {
        static SCHEMA: LazyLock<TableSchema> = LazyLock::new(|| TableSchema {
            model_name: "SqlGraphParentView".into(),
            table_name: "sql_graph_parent_views".into(),
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
                    field_name: "children".into(),
                    kind: RelationshipKind::HasMany,
                    target_model: "SqlGraphChildView".into(),
                    foreign_key: Some("parent_id".into()),
                    through: None,
                    target_foreign_key: None,
                },
                RelationshipDef {
                    field_name: "featured_children".into(),
                    kind: RelationshipKind::HasMany,
                    target_model: "SqlGraphChildView".into(),
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
            model_name: "SqlGraphChildView".into(),
            table_name: "sql_graph_child_views".into(),
            columns: vec![
                TableColumn {
                    primary_key: true,
                    ..TableColumn::new("id", "id", ColumnType::Text)
                },
                TableColumn {
                    foreign_key: Some(ForeignKey::new("sql_graph_parent_views", "id")),
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
            ProjectionModelOwnership::new("SqlGraphParentView", "sql_graph_parent_views").unwrap(),
            ProjectionModelOwnership::new("SqlGraphChildView", "sql_graph_child_views").unwrap(),
        ]
    }

    fn graph_codec() -> ProjectionScopeCodec {
        ProjectionScopeCodec::with_models(
            topology(),
            [
                ("SqlGraphParentView", graph_parent_schema()),
                ("SqlGraphChildView", graph_child_schema()),
            ],
        )
        .unwrap()
    }

    fn graph_key(model: &str) -> RowKey {
        RowKey::new([(
            "id",
            RowValue::String(match model {
                "SqlGraphParentView" => "parent-1",
                "SqlGraphChildView" => "child-1",
                other => panic!("unknown SQL graph model {other}"),
            }
            .into()),
        )])
    }

    fn graph_scope(model: &str) -> ProjectionRecordScope {
        graph_codec()
            .encode_row_scope_in_partition(model, partition(), &graph_key(model))
            .unwrap()
    }

    fn graph_mutation(model: &str) -> ProjectionRecordMutation {
        let (schema, values) = match model {
            "SqlGraphParentView" => {
                let mut values = RowValues::new();
                values.insert("id", RowValue::String("parent-1".into()));
                values.insert("parent_id", RowValue::String("wrong-parent".into()));
                (graph_parent_schema(), values)
            }
            "SqlGraphChildView" => {
                let mut values = RowValues::new();
                values.insert("id", RowValue::String("child-1".into()));
                values.insert("parent_id", RowValue::String("parent-1".into()));
                (graph_child_schema(), values)
            }
            other => panic!("unknown SQL graph model {other}"),
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

    fn graph_snapshot_request(max_unique: usize) -> ProjectionGraphSnapshotRequest {
        let root = ProjectionQuerySnapshotRequest::new(
            &graph_codec(),
            Some(&serde_json::json!("tenant-sql")),
            "SqlGraphParentView",
            graph_key("SqlGraphParentView"),
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

    fn scope_codec() -> ProjectionScopeCodec {
        ProjectionScopeCodec::with_models(topology(), [("SqlTodoView", schema())]).unwrap()
    }

    fn record_key() -> RowKey {
        RowKey::new([("id", RowValue::String("todo-1".into()))])
    }

    fn record_scope() -> ProjectionRecordScope {
        scope_codec()
            .encode_row_scope_in_partition("SqlTodoView", partition(), &record_key())
            .unwrap()
    }

    fn matrix_key(index: usize) -> RowKey {
        RowKey::new([("id", RowValue::String(format!("matrix-{index}")))])
    }

    fn matrix_scope(index: usize) -> ProjectionRecordScope {
        scope_codec()
            .encode_row_scope_in_partition("SqlTodoView", partition(), &matrix_key(index))
            .unwrap()
    }

    fn matrix_mutation(
        index: usize,
        value: &str,
        expectation: ProjectionRecordExpectation,
    ) -> ProjectionRecordMutation {
        let mut values = RowValues::new();
        values.insert("id", RowValue::String(format!("matrix-{index}")));
        values.insert("value", RowValue::String(value.into()));
        ProjectionRecordMutation::new(
            matrix_scope(index),
            TableMutation::UpsertRow(TableRowMutation {
                schema: schema(),
                key: matrix_key(index),
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
            Some(&serde_json::json!("tenant-sql")),
            "SqlTodoView",
            matrix_key(index),
            Vec::new(),
        )
        .unwrap()
    }

    fn ownership() -> ProjectionModelOwnership {
        ProjectionModelOwnership::new("SqlTodoView", "sql_todo_views").unwrap()
    }

    #[derive(Clone, Copy)]
    struct ProjectionScenario;

    impl crate::projection_protocol::scenario_tests::ProjectionProtocolScenario for ProjectionScenario {
        type Store = SqlxRepository<sqlx::Sqlite>;

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
            async move { self::row_exists(repository).await }
        }
    }

    async fn repository() -> SqlxRepository<sqlx::Sqlite> {
        let repository = unregistered_repository().await;
        repository
            .register_projection_models(&topology(), &[ownership()])
            .await
            .unwrap();
        repository
    }

    async fn repository_with_retention(max_retained_changes: u64) -> SqlxRepository<sqlx::Sqlite> {
        let repository = unregistered_repository()
            .await
            .with_projection_change_retention(
                ProjectionChangeRetention::new(max_retained_changes).unwrap(),
            );
        repository
            .register_projection_models(&topology(), &[ownership()])
            .await
            .unwrap();
        repository
    }

    async fn wal_repository_with_retention(
        max_retained_changes: u64,
    ) -> (SqlxRepository<sqlx::Sqlite>, PathBuf) {
        let database_path = std::env::temp_dir().join(format!(
            "distributed-projection-resume-{}.sqlite",
            uuid::Uuid::now_v7()
        ));
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&database_path)
                    .create_if_missing(true)
                    .journal_mode(SqliteJournalMode::Wal),
            )
            .await
            .unwrap();
        let repository = SqlxRepository::<sqlx::Sqlite>::new(pool)
            .with_projection_change_retention(
                ProjectionChangeRetention::new(max_retained_changes).unwrap(),
            );
        repository.migrate().await.unwrap();
        let mut registry = TableSchemaRegistry::new();
        registry.register_schema(schema().clone()).unwrap();
        repository
            .bootstrap_table_schema_for_dev(&registry)
            .await
            .unwrap();
        repository
            .register_projection_models(&topology(), &[ownership()])
            .await
            .unwrap();
        (repository, database_path)
    }

    async fn remove_wal_database(repository: SqlxRepository<sqlx::Sqlite>, path: &Path) {
        repository.pool().close().await;
        for candidate in [
            path.to_path_buf(),
            path.with_extension("sqlite-wal"),
            path.with_extension("sqlite-shm"),
        ] {
            match std::fs::remove_file(&candidate) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => panic!(
                    "remove SQLite projection resume test file {}: {error}",
                    candidate.display()
                ),
            }
        }
    }

    async fn unregistered_repository() -> SqlxRepository<sqlx::Sqlite> {
        let repository = SqlxRepository::<sqlx::Sqlite>::connect_and_migrate("sqlite::memory:")
            .await
            .unwrap();
        let mut registry = TableSchemaRegistry::new();
        registry.register_schema(schema().clone()).unwrap();
        repository
            .bootstrap_table_schema_for_dev(&registry)
            .await
            .unwrap();
        repository
    }

    async fn graph_repository() -> SqlxRepository<sqlx::Sqlite> {
        let repository = SqlxRepository::<sqlx::Sqlite>::connect_and_migrate("sqlite::memory:")
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
            .register_projection_models(&topology(), &graph_ownership())
            .await
            .unwrap();
        repository
    }

    fn upsert_table_mutation(id: &str) -> TableMutation {
        let key = RowKey::new([("id", RowValue::String(id.into()))]);
        let mut values = RowValues::new();
        values.insert("id", RowValue::String(id.into()));
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
            Some(&serde_json::json!("tenant-sql")),
            "SqlTodoView",
            record_key(),
            vec![ProjectionCheckpointProbe::new(
                topology(),
                partition(),
                source("todo_stream", b"todo-1"),
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
                upsert_table_mutation("todo-1")
            }
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
            change_epoch: change_epoch(),
            ownership: vec![ownership()],
            mutations,
            observations,
        }
    }

    async fn row_exists(repository: &SqlxRepository<sqlx::Sqlite>) -> bool {
        let row = sqlx::query("SELECT 1 AS present FROM sql_todo_views WHERE id = ? LIMIT 1")
            .bind("todo-1")
            .fetch_optional(repository.pool())
            .await
            .unwrap();
        row.and_then(|row| row.try_get::<i64, _>("present").ok())
            .is_some()
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
            "live resume head must come from the same SQL statement snapshot"
        );
        assert_eq!(snapshot.compacted_through, 0);
    }

    #[tokio::test]
    async fn sqlite_query_snapshot_never_mixes_row_revision_checkpoint_or_resume_head() {
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
            for position in 2..=32 {
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
    async fn sqlite_input_disposition_is_read_only_exact_and_repair_fenced() {
        crate::projection_protocol::scenario_tests::input_disposition_is_read_only_exact_and_repair_fenced(
            ProjectionScenario,
        )
        .await;
    }

    #[tokio::test]
    async fn sqlite_obligation_and_unpartitioned_live_evidence_are_exact_and_durable() {
        let evidence_repository = repository().await;
        let scope = record_scope();
        let live_request = ProjectionLiveRecordBatchRequest::new(vec![
            crate::projection_protocol::ProjectionLiveRecordRequest::new(
                &scope_codec(),
                "SqlTodoView",
                record_key(),
            )
            .unwrap(),
        ])
        .unwrap();
        assert_eq!(
            evidence_repository
                .projection_live_record_batch(&live_request)
                .await
                .unwrap()
                .records,
            vec![None]
        );

        let created = evidence_repository
            .commit_projection(batch(
                input(
                    1,
                    b"evidence-created",
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
        assert_eq!(
            evidence_repository
                .projection_live_record_batch(&live_request)
                .await
                .unwrap()
                .records[0]
                .as_ref()
                .unwrap()
                .revision
                .scope(),
            &scope
        );
        let observed = crate::projection_protocol::ProjectionObligationEvidenceRequest::new(
            "evidence-cause",
            scope.clone(),
            ProjectionObservationKind::Record,
        )
        .unwrap();
        let pending = crate::projection_protocol::ProjectionObligationEvidenceRequest::new(
            "pending-cause",
            scope.clone(),
            ProjectionObservationKind::Record,
        )
        .unwrap();
        let before_failure = evidence_repository
            .projection_obligation_evidence_batch(
                &ProjectionObligationEvidenceBatchRequest::new(vec![
                    observed.clone(),
                    pending.clone(),
                ])
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            &before_failure.evidence[0],
            ProjectionObligationEvidence::Observed(observation)
                if observation.change == created.changes[0].cursor
        ));
        assert_eq!(
            before_failure.evidence[1],
            ProjectionObligationEvidence::Pending
        );

        let failure = evidence_repository
            .record_projection_failure(
                ProjectionFailureBatch::new(
                    input(
                        2,
                        b"evidence-failed",
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
        evidence_repository
            .compact_projection_changes(&failure.change)
            .await
            .unwrap();
        let after_failure = evidence_repository
            .projection_obligation_evidence_batch(
                &ProjectionObligationEvidenceBatchRequest::new(vec![observed, pending]).unwrap(),
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

        let moved_repository = repository().await;
        let old_scope = record_scope();
        let old = moved_repository
            .commit_projection(batch(
                input(
                    1,
                    b"move-old",
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
        moved_repository
            .commit_projection(batch(
                input(
                    2,
                    b"move-delete",
                    "move-message-2",
                    "move-cause-2",
                    ProjectionGeneration::initial(),
                ),
                vec![mutation(
                    ProjectionRecordExpectation::Exact(old.revision),
                    ProjectionMutationKind::Delete,
                )],
                Vec::new(),
            ))
            .await
            .unwrap();
        assert_eq!(
            moved_repository
                .projection_live_record_batch(&live_request)
                .await
                .unwrap()
                .records,
            vec![None]
        );

        let new_partition = scope_codec()
            .encode_partition(Some(&serde_json::json!("tenant-moved")))
            .unwrap();
        let new_scope = scope_codec()
            .encode_row_scope_in_partition("SqlTodoView", new_partition.clone(), &record_key())
            .unwrap();
        let new_input = TrustedProjectionInput::mint(
            ProjectionInputCursor::new(
                topology(),
                new_partition,
                source("todo_stream", b"todo-1"),
                ProjectionEpoch::new("source-v1").unwrap(),
                1,
            )
            .unwrap(),
            ProjectionInputFingerprint::from_canonical_bytes(b"move-new"),
            "move-message-3",
            "move-cause-3",
            ProjectionGeneration::initial(),
            true,
        )
        .unwrap();
        moved_repository
            .commit_projection(ProjectionCommitBatch {
                input: new_input,
                change_epoch: change_epoch(),
                ownership: vec![ownership()],
                mutations: vec![ProjectionRecordMutation::new(
                    new_scope.clone(),
                    upsert_table_mutation("todo-1"),
                    ProjectionRecordExpectation::Missing,
                    ProjectionMutationKind::Upsert,
                )
                .unwrap()],
                observations: Vec::new(),
            })
            .await
            .unwrap();
        assert_eq!(
            moved_repository
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

        let old_partition_hash = partition().digest();
        assert!(
            sqlx::query(
                "UPDATE projection_records SET tombstone = 0 \
                 WHERE topology_hash = ? AND partition_hash = ? AND model_name = ?",
            )
            .bind(topology().digest().as_slice())
            .bind(old_partition_hash.as_slice())
            .bind("SqlTodoView")
            .execute(moved_repository.pool())
            .await
            .is_err(),
            "the partial unique index must reject a second live partition"
        );
        sqlx::query("DROP INDEX projection_records_unique_live_identity")
            .execute(moved_repository.pool())
            .await
            .unwrap();
        sqlx::query(
            "UPDATE projection_records SET tombstone = 0 \
             WHERE topology_hash = ? AND partition_hash = ? AND model_name = ?",
        )
        .bind(topology().digest().as_slice())
        .bind(old_partition_hash.as_slice())
        .bind("SqlTodoView")
        .execute(moved_repository.pool())
        .await
        .unwrap();
        assert!(matches!(
            moved_repository
                .projection_live_record_batch(&live_request)
                .await,
            Err(ProjectionProtocolError::InvalidBatch(message))
                if message.contains("ambiguous")
        ));

        let drift_repository = repository().await;
        drift_repository
            .commit_projection(batch(
                input(
                    1,
                    b"drift",
                    "drift-message-1",
                    "drift-cause-1",
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
        sqlx::query(
            "UPDATE projection_records SET canonical_key_bytes = ? \
             WHERE topology_hash = ? AND model_name = ?",
        )
        .bind(b"corrupt-key".as_slice())
        .bind(topology().digest().as_slice())
        .bind("SqlTodoView")
        .execute(drift_repository.pool())
        .await
        .unwrap();
        assert!(matches!(
            drift_repository
                .projection_live_record_batch(&live_request)
                .await,
            Err(ProjectionProtocolError::InvalidBatch(message))
                if message.contains("canonical key")
        ));

        // Keep the compiler from treating the old exact scope as an incidental
        // local: it is the durable tombstone retained across the move.
        assert_ne!(old_scope, new_scope);
    }

    #[tokio::test]
    async fn sqlite_receipts_source_fences_and_raw_write_fence_are_exact() {
        let repository = repository().await;
        let raw_error = repository
            .commit_write_plan(TableWritePlan::new(vec![upsert_table_mutation("todo-1")]))
            .await
            .unwrap_err();
        assert!(matches!(
            raw_error,
            TableStoreError::CausalWriteRequired { ref table } if table == "sql_todo_views"
        ));

        let applied = repository
            .commit_projection(batch(
                input(
                    1,
                    b"one",
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
        assert!(row_exists(&repository).await);

        let duplicate = repository
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
        assert_eq!(duplicate.outcome, ProjectionCommitOutcome::Duplicate);
        assert_eq!(
            duplicate.checkpoint.as_ref().unwrap().change(),
            applied.checkpoint.as_ref().unwrap().change()
        );

        assert!(matches!(
            repository
                .commit_projection(batch(
                    input(
                        1,
                        b"changed",
                        "new-message",
                        "cause-1",
                        ProjectionGeneration::initial(),
                    ),
                    Vec::new(),
                    Vec::new(),
                ))
                .await,
            Err(ProjectionProtocolError::InputCorruption)
        ));

        repository
            .commit_projection(batch(
                input(
                    2,
                    b"two",
                    "message-2",
                    "cause-2",
                    ProjectionGeneration::initial(),
                ),
                Vec::new(),
                Vec::new(),
            ))
            .await
            .unwrap();
        let old_after_advance = repository
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
        assert_eq!(
            old_after_advance.outcome,
            ProjectionCommitOutcome::Duplicate
        );
        assert!(matches!(
            repository
                .commit_projection(batch(
                    input(
                        1,
                        b"changed-after-advance",
                        "new-message-after-advance",
                        "cause-1",
                        ProjectionGeneration::initial(),
                    ),
                    Vec::new(),
                    Vec::new(),
                ))
                .await,
            Err(ProjectionProtocolError::InputCorruption)
        ));
        assert!(matches!(
            repository
                .commit_projection(batch(
                    input(
                        3,
                        b"three",
                        "message-1",
                        "cause-3",
                        ProjectionGeneration::initial(),
                    ),
                    Vec::new(),
                    Vec::new(),
                ))
                .await,
            Err(ProjectionProtocolError::MessageIdReuse { .. })
        ));
        let stale = repository
            .commit_projection(batch(
                input(
                    0,
                    b"stale",
                    "stale-message",
                    "stale-cause",
                    ProjectionGeneration::initial(),
                ),
                Vec::new(),
                Vec::new(),
            ))
            .await
            .unwrap();
        assert_eq!(stale.outcome, ProjectionCommitOutcome::StaleInput);
        let changed_capability = TrustedProjectionInput::mint(
            input_cursor(3),
            ProjectionInputFingerprint::from_canonical_bytes(b"changed-capability"),
            "capability-message",
            "capability-cause",
            ProjectionGeneration::initial(),
            false,
        )
        .unwrap();
        assert!(matches!(
            repository
                .commit_projection(batch(changed_capability, Vec::new(), Vec::new()))
                .await,
            Err(ProjectionProtocolError::InputCorruption)
        ));
        assert!(matches!(
            repository
                .commit_projection(batch(
                    input(
                        4,
                        b"gap",
                        "message-4",
                        "cause-4",
                        ProjectionGeneration::initial(),
                    ),
                    Vec::new(),
                    Vec::new(),
                ))
                .await,
            Err(ProjectionProtocolError::IncomparableInput)
        ));

        let other_source = source("audit_stream", b"audit-1");
        let other = input_for_source(
            other_source.clone(),
            41,
            b"audit",
            "audit-message",
            "audit-cause",
        );
        repository
            .commit_projection(batch(other, Vec::new(), Vec::new()))
            .await
            .unwrap();
        assert_eq!(
            repository
                .projection_checkpoint(
                    &input_cursor_for(other_source, 0, "source-v1"),
                    ProjectionGeneration::initial(),
                )
                .await
                .unwrap()
                .unwrap()
                .input()
                .position(),
            41
        );

        let mut transactional = CommitBatch::empty();
        transactional
            .read_model_plans
            .push(TableWritePlan::new(vec![upsert_table_mutation("todo-1")]));
        assert!(matches!(
            repository.commit_batch(transactional).await,
            Err(crate::RepositoryError::CausalWriteRequired { .. })
        ));
    }

    #[tokio::test]
    async fn sqlite_message_identity_is_topology_wide_across_projection_partitions() {
        crate::projection_protocol::scenario_tests::message_identity_is_topology_wide_across_projection_partitions(
            ProjectionScenario,
        )
        .await;
    }

    #[tokio::test]
    async fn sqlite_failure_recording_is_idempotent_for_exact_batch() {
        crate::projection_protocol::scenario_tests::failure_recording_is_idempotent_for_exact_batch(
            ProjectionScenario,
        )
        .await;
    }

    #[tokio::test]
    async fn sqlite_row_failure_rolls_back_protocol_receipt_inbox_and_domain_row() {
        let repository = repository().await;
        sqlx::query(
            "CREATE TRIGGER fail_sql_todo_insert BEFORE INSERT ON sql_todo_views \
             BEGIN SELECT RAISE(ABORT, 'forced projection row failure'); END",
        )
        .execute(repository.pool())
        .await
        .unwrap();
        assert!(matches!(
            repository
                .commit_projection(batch(
                    input(
                        1,
                        b"rollback",
                        "rollback-message",
                        "rollback-cause",
                        ProjectionGeneration::initial(),
                    ),
                    vec![mutation(
                        ProjectionRecordExpectation::Missing,
                        ProjectionMutationKind::Upsert,
                    )],
                    Vec::new(),
                ))
                .await,
            Err(ProjectionProtocolError::Table(_))
        ));
        assert!(!row_exists(&repository).await);
        assert!(repository
            .projection_record(&record_scope())
            .await
            .unwrap()
            .is_none());
        assert!(repository
            .projection_checkpoint(&input_cursor(1), ProjectionGeneration::initial(),)
            .await
            .unwrap()
            .is_none());
        sqlx::query("DROP TRIGGER fail_sql_todo_insert")
            .execute(repository.pool())
            .await
            .unwrap();
        assert_eq!(
            repository
                .commit_projection(batch(
                    input(
                        1,
                        b"rollback",
                        "rollback-message",
                        "rollback-cause",
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
                .outcome,
            ProjectionCommitOutcome::Applied
        );
    }

    #[tokio::test]
    async fn sqlite_rejects_tampered_failure_digest_before_any_protocol_write() {
        let repository = repository().await;
        let mut failure = ProjectionFailureBatch::new(
            input(
                1,
                b"tampered-failure",
                "tampered-failure-message",
                "tampered-failure-cause",
                ProjectionGeneration::initial(),
            ),
            change_epoch(),
            "tampered-failure-id",
            "decode_error",
            b"shape-valid failure details".to_vec(),
        )
        .unwrap();
        failure.failure_digest[0] ^= 0xff;

        assert!(matches!(
            repository.record_projection_failure(failure).await,
            Err(ProjectionProtocolError::InvalidBatch(message))
                if message == "projection failure digest does not match its exact bytes"
        ));
        for (table, count_sql) in [
            (
                "projection_partitions",
                "SELECT COUNT(*) FROM projection_partitions",
            ),
            (
                "projection_changes",
                "SELECT COUNT(*) FROM projection_changes",
            ),
            (
                "projection_failures",
                "SELECT COUNT(*) FROM projection_failures",
            ),
            (
                "projection_input_identities",
                "SELECT COUNT(*) FROM projection_input_identities",
            ),
            (
                "projection_input_receipts",
                "SELECT COUNT(*) FROM projection_input_receipts",
            ),
            ("consumer_inbox", "SELECT COUNT(*) FROM consumer_inbox"),
        ] {
            let count: i64 = sqlx::query_scalar(count_sql)
                .fetch_one(repository.pool())
                .await
                .unwrap();
            assert_eq!(
                count, 0,
                "tampered failure validation must precede writes to {table}"
            );
        }
    }

    #[tokio::test]
    async fn sqlite_tombstones_observations_failure_repair_and_compaction_conform() {
        let repository = repository().await;
        let scope = record_scope();
        assert_eq!(
            repository
                .projection_partition_runtime_state(&topology(), &partition())
                .await
                .unwrap(),
            None
        );
        let created = repository
            .commit_projection(batch(
                input(
                    1,
                    b"create",
                    "message-1",
                    "stable-cause",
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
        assert_eq!(created.changes.len(), 1);
        let earliest = repository
            .projection_observation("stable-cause", &scope, ProjectionObservationKind::Record)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(earliest.change, created.changes[0].cursor);

        let deleted = repository
            .commit_projection(batch(
                input(
                    2,
                    b"delete",
                    "message-2",
                    "stable-cause",
                    ProjectionGeneration::initial(),
                ),
                vec![mutation(
                    ProjectionRecordExpectation::Exact(created.records[0].revision.clone()),
                    ProjectionMutationKind::Delete,
                )],
                vec![ProjectionObservationRequest {
                    kind: ProjectionObservationKind::Record,
                    target: ProjectionObservationTarget::StagedRecord(scope.clone()),
                }],
            ))
            .await
            .unwrap();
        assert!(deleted.records[0].tombstone);
        assert!(!row_exists(&repository).await);
        assert_eq!(
            repository
                .projection_observation("stable-cause", &scope, ProjectionObservationKind::Record,)
                .await
                .unwrap()
                .unwrap(),
            earliest
        );
        assert!(matches!(
            repository
                .commit_projection(batch(
                    input(
                        3,
                        b"plain-upsert",
                        "message-3",
                        "cause-3",
                        ProjectionGeneration::initial(),
                    ),
                    vec![mutation(
                        ProjectionRecordExpectation::Exact(deleted.records[0].revision.clone(),),
                        ProjectionMutationKind::Upsert,
                    )],
                    Vec::new(),
                ))
                .await,
            Err(ProjectionProtocolError::RecordTombstoned { .. })
        ));
        let recreated = repository
            .commit_projection(batch(
                input(
                    3,
                    b"recreate",
                    "message-3b",
                    "cause-3",
                    ProjectionGeneration::initial(),
                ),
                vec![mutation(
                    ProjectionRecordExpectation::Exact(deleted.records[0].revision.clone()),
                    ProjectionMutationKind::Recreate,
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
        assert!(repository
            .projection_observation("cause-3", &scope, ProjectionObservationKind::Dependency,)
            .await
            .unwrap()
            .unwrap()
            .revision
            .is_none());

        let failure_batch = ProjectionFailureBatch::new(
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
        assert_eq!(
            repository
                .projection_failure(&topology(), &partition(), "failure-4")
                .await
                .unwrap(),
            Some(failure.clone())
        );
        assert!(failure.gap_free);
        let stopped = repository
            .projection_partition_runtime_state(&topology(), &partition())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stopped.active_generation, ProjectionGeneration::initial());
        assert_eq!(stopped.stopped_failure_id.as_deref(), Some("failure-4"));
        assert_eq!(stopped.pending_retry, None);
        assert!(matches!(
            repository
                .commit_projection(batch(
                    input(
                        5,
                        b"blocked",
                        "message-5",
                        "cause-5",
                        ProjectionGeneration::initial(),
                    ),
                    Vec::new(),
                    Vec::new(),
                ))
                .await,
            Err(ProjectionProtocolError::PartitionStopped { .. })
        ));
        assert!(matches!(
            repository
                .commit_projection(batch(
                    input(
                        4,
                        b"changed-while-stopped",
                        "changed-message-4",
                        "cause-4",
                        ProjectionGeneration::initial(),
                    ),
                    Vec::new(),
                    Vec::new(),
                ))
                .await,
            Err(ProjectionProtocolError::InputCorruption)
        ));
        assert!(matches!(
            repository
                .commit_projection(batch(
                    input(
                        5,
                        b"reused-message-while-stopped",
                        "message-4",
                        "cause-5",
                        ProjectionGeneration::initial(),
                    ),
                    Vec::new(),
                    Vec::new(),
                ))
                .await,
            Err(ProjectionProtocolError::MessageIdReuse { .. })
        ));
        assert!(matches!(
            repository
                .commit_projection(batch(
                    input(
                        4,
                        b"failure",
                        "message-4",
                        "cause-4",
                        ProjectionGeneration::initial(),
                    ),
                    Vec::new(),
                    Vec::new(),
                ))
                .await,
            Err(ProjectionProtocolError::PartitionStopped { .. })
        ));
        let generation = repository
            .repair_projection(&topology(), &partition(), "failure-4")
            .await
            .unwrap();
        assert_eq!(generation.get(), 2);
        let repaired = repository
            .projection_partition_runtime_state(&topology(), &partition())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(repaired.active_generation, generation);
        assert_eq!(repaired.stopped_failure_id, None);
        let retry = repaired.pending_retry.unwrap();
        assert_eq!(retry.failure_id, "failure-4");
        assert_eq!(retry.input, failure.input);
        assert_eq!(retry.input_fingerprint, failure.input_fingerprint);
        assert_eq!(retry.message_id, failure.message_id);
        assert_eq!(retry.causation_id, failure.causation_id);
        assert_eq!(retry.failed_generation, failure.generation);
        assert_eq!(retry.gap_free, failure.gap_free);
        assert!(matches!(
            repository.record_projection_failure(failure_batch).await,
            Err(ProjectionProtocolError::GenerationFenced {
                expected: 2,
                actual: 1
            })
        ));
        assert!(matches!(
            repository
                .commit_projection(batch(
                    input(
                        4,
                        b"changed-old-generation",
                        "changed-old-message-4",
                        "cause-4",
                        ProjectionGeneration::initial(),
                    ),
                    Vec::new(),
                    Vec::new(),
                ))
                .await,
            Err(ProjectionProtocolError::InputCorruption)
        ));
        assert!(matches!(
            repository
                .commit_projection(batch(
                    input(
                        6,
                        b"unknown-old-generation",
                        "unknown-old-message-6",
                        "unknown-old-cause-6",
                        ProjectionGeneration::initial(),
                    ),
                    Vec::new(),
                    Vec::new(),
                ))
                .await,
            Err(ProjectionProtocolError::GenerationFenced {
                expected: 2,
                actual: 1
            })
        ));
        let changed_old_capability = TrustedProjectionInput::mint(
            input_cursor(6),
            ProjectionInputFingerprint::from_canonical_bytes(b"changed-old-capability"),
            "changed-old-capability-message",
            "changed-old-capability-cause",
            ProjectionGeneration::initial(),
            false,
        )
        .unwrap();
        assert!(matches!(
            repository
                .commit_projection(batch(changed_old_capability, Vec::new(), Vec::new()))
                .await,
            Err(ProjectionProtocolError::InputCorruption)
        ));
        assert_eq!(
            repository
                .commit_projection(batch(
                    input(3, b"recreate", "message-3b", "cause-3", generation),
                    Vec::new(),
                    Vec::new(),
                ))
                .await
                .unwrap()
                .outcome,
            ProjectionCommitOutcome::Duplicate
        );
        assert_eq!(
            repository
                .commit_projection(batch(
                    input(2, b"delete", "message-2", "stable-cause", generation),
                    Vec::new(),
                    Vec::new(),
                ))
                .await
                .unwrap()
                .outcome,
            ProjectionCommitOutcome::StaleInput
        );
        assert!(matches!(
            repository
                .commit_projection(batch(
                    input(5, b"later", "message-5", "cause-5", generation),
                    Vec::new(),
                    Vec::new(),
                ))
                .await,
            Err(ProjectionProtocolError::IncomparableInput)
        ));
        assert!(matches!(
            repository
                .commit_projection(batch(
                    input(4, b"retry", "message-4b", "cause-4", generation),
                    Vec::new(),
                    Vec::new(),
                ))
                .await,
            Err(ProjectionProtocolError::InputCorruption)
        ));
        let repaired = repository
            .commit_projection(batch(
                input(4, b"failure", "message-4", "cause-4", generation),
                Vec::new(),
                Vec::new(),
            ))
            .await
            .unwrap();
        assert_eq!(repaired.changes[0].kind, ProjectionChangeKind::Checkpoint);
        assert_eq!(
            repository
                .commit_projection(batch(
                    input(5, b"later", "message-5", "cause-5", generation),
                    Vec::new(),
                    Vec::new(),
                ))
                .await
                .unwrap()
                .outcome,
            ProjectionCommitOutcome::Applied
        );

        let compacted = repository
            .compact_projection_changes(&created.changes[0].cursor)
            .await
            .unwrap();
        assert_eq!(compacted, created.changes[0].cursor.position());
        assert!(matches!(
            repository
                .projection_changes(
                    &topology(),
                    &partition(),
                    Some(&created.changes[0].cursor),
                    100,
                )
                .await
                .unwrap(),
            ProjectionChangeRead::Changes {
                compacted_through,
                ref changes,
                ..
            } if compacted_through == compacted && !changes.is_empty()
        ));
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
        repository
            .compact_projection_changes(&failure.change)
            .await
            .unwrap();
        assert!(matches!(
            repository
                .projection_changes(
                    &topology(),
                    &partition(),
                    Some(&created.changes[0].cursor),
                    100,
                )
                .await
                .unwrap(),
            ProjectionChangeRead::ResetRequired { .. }
        ));

        let failed_first = self::repository().await;
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
        assert!(matches!(
            failed_first
                .commit_projection(batch(
                    input(
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
        let changed_after_repair = TrustedProjectionInput::mint(
            input_cursor(0),
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
    async fn sqlite_registration_rejects_legacy_rows_and_cross_topology_table_owners() {
        let with_legacy_row = unregistered_repository().await;
        assert!(matches!(
            with_legacy_row
                .register_projection_models(&topology(), &[])
                .await,
            Err(ProjectionProtocolError::InvalidBatch(message))
                if message.contains("at least one owned model")
        ));
        with_legacy_row
            .commit_write_plan(TableWritePlan::new(vec![upsert_table_mutation("legacy")]))
            .await
            .unwrap();
        assert!(matches!(
            with_legacy_row
                .register_projection_models(&topology(), &[ownership()])
                .await,
            Err(ProjectionProtocolError::InvalidBatch(message))
                if message.contains("unverified legacy rows")
        ));

        // Deterministically hold the raw-writer fence while bootstrap starts.
        // Raw-first must commit its row; bootstrap then wakes, observes that
        // legacy row under the same fence, and rejects causal ownership.
        let racing = unregistered_repository().await;
        let mut raw_tx = racing.pool().begin().await.unwrap();
        let racing_tables = BTreeSet::from(["sql_todo_views".to_string()]);
        lock_projection_table_ownership_fences_in_tx(&mut raw_tx, &racing_tables)
            .await
            .unwrap();
        let registration_repository = racing.clone();
        let registration = tokio::spawn(async move {
            registration_repository
                .register_projection_models(&topology(), &[ownership()])
                .await
        });
        tokio::task::yield_now().await;
        assert!(!registration.is_finished());
        apply_read_model_write_plan_in_tx(
            &mut raw_tx,
            TableWritePlan::new(vec![upsert_table_mutation("racing-legacy")]),
        )
        .await
        .unwrap();
        raw_tx.commit().await.unwrap();
        assert!(matches!(
            registration.await.unwrap(),
            Err(ProjectionProtocolError::InvalidBatch(message))
                if message.contains("unverified legacy rows")
        ));

        let registered = repository().await;
        assert!(matches!(
            registered
                .register_projection_models(&other_topology(), &[ownership()])
                .await,
            Err(ProjectionProtocolError::InvalidBatch(message))
                if message.contains("authoritatively owned")
        ));
    }

    #[tokio::test]
    async fn sqlite_shared_registered_table_ownership_rejects_other_topology() {
        crate::projection_protocol::scenario_tests::registered_table_ownership_rejects_other_topology(
            ProjectionScenario,
        )
        .await;
    }

    #[tokio::test]
    async fn sqlite_coherent_execution_and_graph_snapshots_use_fk_references_and_unique_budgets() {
        let repository = graph_repository().await;
        repository
            .commit_projection(ProjectionCommitBatch {
                input: input(
                    1,
                    b"sqlite-graph",
                    "sqlite-graph-message-1",
                    "sqlite-graph-cause-1",
                    ProjectionGeneration::initial(),
                ),
                change_epoch: change_epoch(),
                ownership: graph_ownership(),
                mutations: vec![
                    graph_mutation("SqlGraphParentView"),
                    graph_mutation("SqlGraphChildView"),
                ],
                observations: Vec::new(),
            })
            .await
            .unwrap();

        let execution_request = ProjectionExecutionSnapshotBatchRequest::new(vec![
            ProjectionQuerySnapshotRequest::new(
                &graph_codec(),
                Some(&serde_json::json!("tenant-sql")),
                "SqlGraphParentView",
                graph_key("SqlGraphParentView"),
                Vec::new(),
            )
            .unwrap(),
            ProjectionQuerySnapshotRequest::new(
                &graph_codec(),
                Some(&serde_json::json!("tenant-sql")),
                "SqlGraphChildView",
                graph_key("SqlGraphChildView"),
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
    async fn four_position_sqlite_projection_commit_failure_matrix_rolls_back_every_side_effect() {
        const PHYSICAL_MUTATION_POSITIONS: usize = 4;

        for fail_at in 0..PHYSICAL_MUTATION_POSITIONS {
            let repository = repository().await;
            let initial = repository
                .commit_projection(batch(
                    input(
                        1,
                        b"sqlite-matrix-initial",
                        "sqlite-matrix-message-1",
                        "sqlite-matrix-cause-1",
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

            let trigger = match fail_at {
                0 => "CREATE TRIGGER fail_projection_matrix_position BEFORE UPDATE ON sql_todo_views WHEN NEW.id = 'matrix-0' AND NEW.value = 'after' BEGIN SELECT RAISE(ABORT, 'forced projection matrix failure'); END",
                1 => "CREATE TRIGGER fail_projection_matrix_position BEFORE UPDATE ON sql_todo_views WHEN NEW.id = 'matrix-1' AND NEW.value = 'after' BEGIN SELECT RAISE(ABORT, 'forced projection matrix failure'); END",
                2 => "CREATE TRIGGER fail_projection_matrix_position BEFORE UPDATE ON sql_todo_views WHEN NEW.id = 'matrix-2' AND NEW.value = 'after' BEGIN SELECT RAISE(ABORT, 'forced projection matrix failure'); END",
                3 => "CREATE TRIGGER fail_projection_matrix_position BEFORE UPDATE ON sql_todo_views WHEN NEW.id = 'matrix-3' AND NEW.value = 'after' BEGIN SELECT RAISE(ABORT, 'forced projection matrix failure'); END",
                _ => unreachable!("the failure matrix has four positions"),
            };
            sqlx::query(trigger)
                .execute(repository.pool())
                .await
                .unwrap();

            let physical_before = sqlx::query(
                "SELECT id, value, _sourced_version FROM sql_todo_views \
                 WHERE id LIKE 'matrix-%' ORDER BY id",
            )
            .fetch_all(repository.pool())
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                (
                    row.try_get::<String, _>("id").unwrap(),
                    row.try_get::<Option<String>, _>("value").unwrap(),
                    row.try_get::<i64, _>("_sourced_version").unwrap(),
                )
            })
            .collect::<Vec<_>>();
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
            let inbox_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM consumer_inbox")
                .fetch_one(repository.pool())
                .await
                .unwrap();
            let observations_before: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM projection_observations")
                    .fetch_one(repository.pool())
                    .await
                    .unwrap();
            let checkpoints_before: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM projection_input_cursors")
                    .fetch_one(repository.pool())
                    .await
                    .unwrap();
            let mut live_changes = repository.read_model_changes();

            let retry_input = input(
                2,
                format!("sqlite-matrix-fail-{fail_at}").as_bytes(),
                &format!("sqlite-matrix-message-2-{fail_at}"),
                &format!("sqlite-matrix-cause-2-{fail_at}"),
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
                            ProjectionRecordExpectation::Exact(
                                initial.records[index].revision.clone(),
                            ),
                        )
                    })
                    .collect(),
                observations: vec![ProjectionObservationRequest {
                    kind: ProjectionObservationKind::Record,
                    target: ProjectionObservationTarget::StagedRecord(matrix_scope(0)),
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
                live_changes.try_recv(),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty)
            ));

            let physical_after = sqlx::query(
                "SELECT id, value, _sourced_version FROM sql_todo_views \
                 WHERE id LIKE 'matrix-%' ORDER BY id",
            )
            .fetch_all(repository.pool())
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                (
                    row.try_get::<String, _>("id").unwrap(),
                    row.try_get::<Option<String>, _>("value").unwrap(),
                    row.try_get::<i64, _>("_sourced_version").unwrap(),
                )
            })
            .collect::<Vec<_>>();
            assert_eq!(physical_after, physical_before, "failure position {fail_at}");
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
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM consumer_inbox")
                    .fetch_one(repository.pool())
                    .await
                    .unwrap(),
                inbox_before
            );
            assert_eq!(
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM projection_observations")
                    .fetch_one(repository.pool())
                    .await
                    .unwrap(),
                observations_before
            );
            assert_eq!(
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM projection_input_cursors")
                    .fetch_one(repository.pool())
                    .await
                    .unwrap(),
                checkpoints_before
            );
            assert_eq!(
                repository
                    .projection_input_disposition(&retry_input)
                    .await
                    .unwrap(),
                ProjectionInputDisposition::Pending
            );

            sqlx::query("DROP TRIGGER fail_projection_matrix_position")
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
    async fn sqlite_tombstone_requires_explicit_exact_recreation() {
        crate::projection_protocol::scenario_tests::tombstone_requires_explicit_exact_recreation(
            ProjectionScenario,
        )
        .await;
    }

    #[tokio::test]
    async fn sqlite_non_gap_repair_requires_failed_cursor_before_later_input() {
        let repository = repository().await;
        repository
            .commit_projection(batch(
                non_gap_input(
                    5,
                    b"checkpoint-5",
                    "non-gap-message-5",
                    "non-gap-cause-5",
                    ProjectionGeneration::initial(),
                ),
                Vec::new(),
                Vec::new(),
            ))
            .await
            .unwrap();
        repository
            .record_projection_failure(
                ProjectionFailureBatch::new(
                    non_gap_input(
                        9,
                        b"failure-9",
                        "non-gap-message-9",
                        "non-gap-cause-9",
                        ProjectionGeneration::initial(),
                    ),
                    change_epoch(),
                    "non-gap-failure-9",
                    "decode_error",
                    b"bad non-gap payload".to_vec(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let generation = repository
            .repair_projection(&topology(), &partition(), "non-gap-failure-9")
            .await
            .unwrap();

        assert_eq!(
            repository
                .commit_projection(batch(
                    non_gap_input(
                        5,
                        b"checkpoint-5",
                        "non-gap-message-5",
                        "non-gap-cause-5",
                        generation,
                    ),
                    Vec::new(),
                    Vec::new(),
                ))
                .await
                .unwrap()
                .outcome,
            ProjectionCommitOutcome::Duplicate
        );
        assert!(matches!(
            repository
                .commit_projection(batch(
                    non_gap_input(
                        10,
                        b"later-10",
                        "non-gap-message-10",
                        "non-gap-cause-10",
                        generation,
                    ),
                    Vec::new(),
                    Vec::new(),
                ))
                .await,
            Err(ProjectionProtocolError::IncomparableInput)
        ));
        assert_eq!(
            repository
                .commit_projection(batch(
                    non_gap_input(
                        9,
                        b"failure-9",
                        "non-gap-message-9",
                        "non-gap-cause-9",
                        generation,
                    ),
                    Vec::new(),
                    Vec::new(),
                ))
                .await
                .unwrap()
                .outcome,
            ProjectionCommitOutcome::Applied
        );
        assert_eq!(
            repository
                .commit_projection(batch(
                    non_gap_input(
                        10,
                        b"later-10",
                        "non-gap-message-10",
                        "non-gap-cause-10",
                        generation,
                    ),
                    Vec::new(),
                    Vec::new(),
                ))
                .await
                .unwrap()
                .outcome,
            ProjectionCommitOutcome::Applied
        );
    }

    #[tokio::test]
    async fn sqlite_record_metadata_is_fenced_against_physical_row_drift() {
        let missing_physical = repository().await;
        let created = missing_physical
            .commit_projection(batch(
                input(
                    1,
                    b"create-for-drift",
                    "drift-message-1",
                    "drift-cause-1",
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
        sqlx::query("DELETE FROM sql_todo_views WHERE id = ?")
            .bind("todo-1")
            .execute(missing_physical.pool())
            .await
            .unwrap();
        assert!(matches!(
            missing_physical
                .commit_projection(batch(
                    input(
                        2,
                        b"update-after-drift",
                        "drift-message-2",
                        "drift-cause-2",
                        ProjectionGeneration::initial(),
                    ),
                    vec![mutation(
                        ProjectionRecordExpectation::Exact(created.records[0].revision.clone()),
                        ProjectionMutationKind::Upsert,
                    )],
                    Vec::new(),
                ))
                .await,
            Err(ProjectionProtocolError::RecordMissing { .. })
        ));
        let direct_missing = SameTransactionProjectionBatch::single_upsert(
            topology(),
            partition(),
            change_epoch(),
            ownership(),
            record_scope(),
            upsert_table_mutation("todo-1"),
            "direct-missing-physical",
        )
        .unwrap();
        let mut tx = missing_physical.pool().begin().await.unwrap();
        assert!(matches!(
            apply_same_transaction_projection_in_tx(
                &mut tx,
                &direct_missing,
                missing_physical.projection_change_retention(),
            )
            .await,
            Err(ProjectionProtocolError::RecordMissing { .. })
        ));
        drop(tx);

        let untracked_physical = repository().await;
        sqlx::query("INSERT INTO sql_todo_views (id, _sourced_version) VALUES (?, ?)")
            .bind("todo-1")
            .bind(1_i64)
            .execute(untracked_physical.pool())
            .await
            .unwrap();
        assert!(matches!(
            untracked_physical
                .commit_projection(batch(
                    input(
                        1,
                        b"claim-untracked",
                        "untracked-message-1",
                        "untracked-cause-1",
                        ProjectionGeneration::initial(),
                    ),
                    vec![mutation(
                        ProjectionRecordExpectation::Missing,
                        ProjectionMutationKind::Upsert,
                    )],
                    Vec::new(),
                ))
                .await,
            Err(ProjectionProtocolError::RecordAlreadyExists { .. })
        ));
        let direct_untracked = SameTransactionProjectionBatch::single_upsert(
            topology(),
            partition(),
            change_epoch(),
            ownership(),
            record_scope(),
            upsert_table_mutation("todo-1"),
            "direct-untracked-physical",
        )
        .unwrap();
        let mut tx = untracked_physical.pool().begin().await.unwrap();
        assert!(matches!(
            apply_same_transaction_projection_in_tx(
                &mut tx,
                &direct_untracked,
                untracked_physical.projection_change_retention(),
            )
            .await,
            Err(ProjectionProtocolError::RecordAlreadyExists { .. })
        ));
    }

    #[tokio::test]
    async fn sqlite_retention_prunes_exact_prefix_and_never_restores_it() {
        let repository = repository_with_retention(2).await;
        let mut cursors = Vec::new();
        for position in 1..=3 {
            let result = repository
                .commit_projection(batch(
                    input(
                        position,
                        format!("retained-{position}").as_bytes(),
                        &format!("retained-message-{position}"),
                        &format!("retained-cause-{position}"),
                        ProjectionGeneration::initial(),
                    ),
                    Vec::new(),
                    Vec::new(),
                ))
                .await
                .unwrap();
            cursors.push(result.changes[0].cursor.clone());
        }
        let failure = repository
            .record_projection_failure(
                ProjectionFailureBatch::new(
                    input(
                        4,
                        b"retained-failure-4",
                        "retained-message-4",
                        "retained-cause-4",
                        ProjectionGeneration::initial(),
                    ),
                    change_epoch(),
                    "retained-failure-4",
                    "decode_error",
                    b"retention failure".to_vec(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let retained: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM projection_changes")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        assert_eq!(retained, 2);
        assert!(matches!(
            repository
                .projection_changes(&topology(), &partition(), Some(&cursors[0]), 100)
                .await
                .unwrap(),
            ProjectionChangeRead::ResetRequired {
                compacted_through: 2,
                ..
            }
        ));
        assert!(matches!(
            repository
                .projection_changes(&topology(), &partition(), Some(&cursors[1]), 100)
                .await
                .unwrap(),
            ProjectionChangeRead::Changes {
                compacted_through: 2,
                ref changes,
                ..
            } if changes.len() == 2
                && changes[0].cursor.position() == 3
                && changes[1].cursor == failure.change
        ));

        let repository = repository
            .with_projection_change_retention(ProjectionChangeRetention::new(10).unwrap());
        let generation = repository
            .repair_projection(&topology(), &partition(), "retained-failure-4")
            .await
            .unwrap();
        repository
            .commit_projection(batch(
                input(
                    4,
                    b"retained-failure-4",
                    "retained-message-4",
                    "retained-cause-4",
                    generation,
                ),
                Vec::new(),
                Vec::new(),
            ))
            .await
            .unwrap();
        repository
            .commit_projection(batch(
                input(
                    5,
                    b"retained-5",
                    "retained-message-5",
                    "retained-cause-5",
                    generation,
                ),
                Vec::new(),
                Vec::new(),
            ))
            .await
            .unwrap();
        let retained: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM projection_changes")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        assert_eq!(retained, 4);
        assert!(matches!(
            repository
                .projection_changes(&topology(), &partition(), None, 100)
                .await
                .unwrap(),
            ProjectionChangeRead::ResetRequired {
                compacted_through: 2,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn sqlite_projection_change_executor_read_uses_existing_snapshot() {
        let repository = repository().await;
        let mut cursors = Vec::new();
        for position in 1..=3 {
            let result = repository
                .commit_projection(batch(
                    input(
                        position,
                        format!("executor-read-{position}").as_bytes(),
                        &format!("executor-read-message-{position}"),
                        &format!("executor-read-cause-{position}"),
                        ProjectionGeneration::initial(),
                    ),
                    Vec::new(),
                    Vec::new(),
                ))
                .await
                .unwrap();
            cursors.push(result.changes[0].cursor.clone());
        }

        let read_topology = topology();
        let read_partition = partition();
        let resume_after = cursors[0].clone();
        let read = with_projection_read_snapshot(repository.pool(), move |connection| {
            Box::pin(async move {
                read_projection_changes_in_executor::<sqlx::Sqlite>(
                    connection,
                    &read_topology,
                    &read_partition,
                    Some(&resume_after),
                    100,
                )
                .await
            })
        })
        .await
        .unwrap();

        match read {
            ProjectionChangeRead::Changes {
                head,
                compacted_through,
                changes,
            } => {
                assert_eq!(head.as_ref().map(ProjectionChangeCursor::position), Some(3));
                assert_eq!(compacted_through, 0);
                assert_eq!(
                    changes
                        .iter()
                        .map(|change| change.cursor.position())
                        .collect::<Vec<_>>(),
                    vec![2, 3]
                );
            }
            other => panic!("executor read must return the retained suffix: {other:?}"),
        }
    }

    #[tokio::test]
    async fn sqlite_resume_and_concurrent_compaction_share_one_snapshot() {
        let (repository, database_path) = wal_repository_with_retention(16).await;
        let mut cursors = Vec::new();
        for position in 1..=3 {
            let result = repository
                .commit_projection(batch(
                    input(
                        position,
                        format!("resume-{position}").as_bytes(),
                        &format!("resume-message-{position}"),
                        &format!("resume-cause-{position}"),
                        ProjectionGeneration::initial(),
                    ),
                    Vec::new(),
                    Vec::new(),
                ))
                .await
                .unwrap();
            cursors.push(result.changes[0].cursor.clone());
        }

        let reader_pool = repository.pool().clone();
        let resume_after = cursors[0].clone();
        let compact_through = cursors[1].clone();
        let (state_observed_tx, state_observed_rx) = tokio::sync::oneshot::channel();
        let (compaction_committed_tx, compaction_committed_rx) = tokio::sync::oneshot::channel();
        let reader = tokio::spawn(async move {
            read_projection_changes_in_snapshot(
                &reader_pool,
                topology(),
                partition(),
                Some(resume_after),
                100,
                async move {
                    state_observed_tx
                        .send(())
                        .expect("resume reader reports its established state snapshot");
                    compaction_committed_rx
                        .await
                        .expect("compaction completion is reported to resume reader");
                },
            )
            .await
        });

        state_observed_rx
            .await
            .expect("resume reader establishes its snapshot");
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(5),
                repository.compact_projection_changes(&compact_through),
            )
            .await
            .expect("WAL compaction commits while the reader snapshot remains open")
            .unwrap(),
            2
        );
        compaction_committed_tx
            .send(())
            .expect("resume reader remains active after concurrent compaction");

        match reader.await.unwrap().unwrap() {
            ProjectionChangeRead::Changes {
                head,
                compacted_through,
                changes,
            } => {
                assert_eq!(head.as_ref().map(ProjectionChangeCursor::position), Some(3));
                assert_eq!(compacted_through, 0);
                assert_eq!(
                    changes
                        .iter()
                        .map(|change| change.cursor.position())
                        .collect::<Vec<_>>(),
                    vec![2, 3],
                    "the established snapshot returns the complete pre-compaction suffix"
                );
            }
            other => panic!("established resume snapshot must return its complete page: {other:?}"),
        }

        assert!(matches!(
            repository
                .projection_changes(&topology(), &partition(), Some(&cursors[0]), 100)
                .await
                .unwrap(),
            ProjectionChangeRead::ResetRequired {
                compacted_through: 2,
                ..
            }
        ));
        assert!(matches!(
            repository
                .projection_changes(&topology(), &partition(), Some(&cursors[1]), 100)
                .await
                .unwrap(),
            ProjectionChangeRead::Changes {
                compacted_through: 2,
                ref changes,
                ..
            } if changes.len() == 1 && changes[0].cursor.position() == 3
        ));

        remove_wal_database(repository, &database_path).await;
    }

    #[tokio::test]
    async fn sqlite_same_transaction_projection_allocates_adapter_evidence() {
        let repository = repository_with_retention(1).await;
        let direct = |causation_id: &str| {
            SameTransactionProjectionBatch::single_upsert(
                topology(),
                partition(),
                change_epoch(),
                ownership(),
                record_scope(),
                upsert_table_mutation("todo-1"),
                causation_id,
            )
            .unwrap()
        };

        let mut tx = repository.pool().begin().await.unwrap();
        let created = apply_same_transaction_projection_in_tx(
            &mut tx,
            &direct("direct-cause-1"),
            repository.projection_change_retention(),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(created.records[0].revision.incarnation(), 1);
        assert_eq!(created.records[0].revision.revision(), 1);
        assert_eq!(created.changes[0].kind, ProjectionChangeKind::RecordUpsert);
        assert_eq!(
            created.observations[0].revision,
            Some(created.records[0].revision.clone())
        );
        assert!(row_exists(&repository).await);

        let mut tx = repository.pool().begin().await.unwrap();
        let updated = apply_same_transaction_projection_in_tx(
            &mut tx,
            &direct("direct-cause-2"),
            repository.projection_change_retention(),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(updated.records[0].revision.incarnation(), 1);
        assert_eq!(updated.records[0].revision.revision(), 2);
        assert_eq!(
            updated.changes[0].cursor.position(),
            created.changes[0].cursor.position() + 1
        );
        assert_eq!(
            repository
                .projection_observation(
                    "direct-cause-2",
                    &record_scope(),
                    ProjectionObservationKind::Record,
                )
                .await
                .unwrap()
                .unwrap(),
            updated.observations[0]
        );
        let retained: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM projection_changes")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        assert_eq!(retained, 1);
        assert!(matches!(
            repository
                .projection_changes(&topology(), &partition(), None, 100)
                .await
                .unwrap(),
            ProjectionChangeRead::ResetRequired {
                compacted_through: 1,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn sqlite_direct_projection_and_ledger_replay_commit_atomically() {
        let repository = repository().await;
        #[cfg(all(feature = "graphql", feature = "gateway-delivery"))]
        let (cache_store, cache_pool, cache_tables, cache_before) = {
            let pool = crate::graphql::GraphqlPool::Sqlite(repository.pool().clone());
            let tables = vec!["sql_todo_views".to_owned()];
            let store = crate::graphql::delivery::GatewayVersionStore::install(&pool, "atomic-cache-test", tables.clone()).await.unwrap();
            let before = serde_json::to_value(store.current(&pool, &tables).await.unwrap()).unwrap();
            (store, pool, tables, before)
        };
        let command_id = uuid::Uuid::now_v7().hyphenated().to_string();
        let key = CommandLedgerKey::new(
            "projection-runtime-test",
            PrincipalPartitionId::new("tenant:direct").unwrap(),
            CommandId::parse(command_id).unwrap(),
        )
        .unwrap();
        let retention = Duration::from_secs(3600);
        let reservation = CommandReservation::new(
            key.clone(),
            "project-todo",
            CommandContractFingerprint::new([51; 32]),
            CanonicalInputHash::new([52; 32]),
            Duration::from_secs(30),
            retention,
        )
        .unwrap();
        let attempt = match repository.reserve_command(reservation).await.unwrap() {
            ReservationOutcome::Acquired(attempt) => attempt,
            _ => panic!("fresh command reservation must acquire its first attempt"),
        };
        let causation_id = attempt.causation_id().as_str().to_string();
        let completion = attempt
            .complete(
                TerminalCommandState::Atomic,
                serde_json::json!({"projected": true}),
                retention,
            )
            .unwrap();
        let direct = SameTransactionProjectionBatch::single_upsert(
            topology(),
            partition(),
            change_epoch(),
            ownership(),
            record_scope(),
            upsert_table_mutation("todo-1"),
            causation_id.as_str(),
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

        #[cfg(all(feature = "graphql", feature = "gateway-delivery"))]
        let cache_committed = {
            let committed = serde_json::to_value(cache_store.current(&cache_pool, &cache_tables).await.unwrap()).unwrap();
            assert_ne!(committed, cache_before, "Atomic result and cache dependency versions commit together");
            committed
        };
        let metadata = repository
            .projection_record(&record_scope())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(metadata.revision.revision(), 1);
        assert!(row_exists(&repository).await);
        match repository
            .lookup_command(&key, CommandLookupScope::CommandName("project-todo"))
            .await
            .unwrap()
        {
            CommandLookup::Replay(replay) => {
                assert_eq!(replay.outcome, serde_json::json!({"projected": true}));
                assert!(replay.direct_projection.is_some());
            }
            _ => panic!("completed direct projection must replay its exact evidence"),
        }

        let failed_key = CommandLedgerKey::new(
            "projection-runtime-test",
            PrincipalPartitionId::new("tenant:direct").unwrap(),
            CommandId::parse(uuid::Uuid::now_v7().hyphenated().to_string()).unwrap(),
        )
        .unwrap();
        let failed_reservation = CommandReservation::new(
            failed_key.clone(),
            "project-todo",
            CommandContractFingerprint::new([61; 32]),
            CanonicalInputHash::new([62; 32]),
            Duration::from_secs(30),
            retention,
        )
        .unwrap();
        let failed_attempt = match repository
            .reserve_command(failed_reservation)
            .await
            .unwrap()
        {
            ReservationOutcome::Acquired(attempt) => attempt,
            _ => panic!("fresh rollback reservation must acquire its first attempt"),
        };
        let failed_causation = failed_attempt.causation_id().as_str().to_string();
        let failed_completion = failed_attempt
            .complete(
                TerminalCommandState::Atomic,
                serde_json::json!({"projected": "must-roll-back"}),
                retention,
            )
            .unwrap();
        let failed_direct = SameTransactionProjectionBatch::single_upsert(
            topology(),
            partition(),
            change_epoch(),
            ownership(),
            record_scope(),
            upsert_table_mutation("todo-1"),
            failed_causation,
        )
        .unwrap();
        sqlx::query(
            "CREATE TRIGGER fail_direct_ledger_completion \
             BEFORE UPDATE OF state ON command_ledger \
             WHEN NEW.state = 'atomic' \
             BEGIN SELECT RAISE(ABORT, 'forced direct ledger failure'); END",
        )
        .execute(repository.pool())
        .await
        .unwrap();
        assert!(repository
            .commit_causal_batch(CausalCommitBatch::with_direct_projection(
                CommitBatch::empty(),
                failed_completion,
                failed_direct,
            ))
            .await
            .is_err());
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
        assert!(matches!(
            repository
                .lookup_command(&failed_key, CommandLookupScope::CommandName("project-todo"),)
                .await
                .unwrap(),
            CommandLookup::InProgress { .. }
        ));
        #[cfg(all(feature = "graphql", feature = "gateway-delivery"))]
        assert_eq!(serde_json::to_value(cache_store.current(&cache_pool, &cache_tables).await.unwrap()).unwrap(), cache_committed,
            "failed Atomic ledger completion must roll back cache versions too");
        sqlx::query("DROP TRIGGER fail_direct_ledger_completion")
            .execute(repository.pool())
            .await
            .unwrap();

        let fenced_key = CommandLedgerKey::new(
            "projection-runtime-test",
            PrincipalPartitionId::new("tenant:direct").unwrap(),
            CommandId::parse(uuid::Uuid::now_v7().hyphenated().to_string()).unwrap(),
        )
        .unwrap();
        let fenced_reservation = CommandReservation::new(
            fenced_key,
            "project-todo",
            CommandContractFingerprint::new([71; 32]),
            CanonicalInputHash::new([72; 32]),
            Duration::from_secs(30),
            retention,
        )
        .unwrap();
        let fenced_attempt = match repository
            .reserve_command(fenced_reservation)
            .await
            .unwrap()
        {
            ReservationOutcome::Acquired(attempt) => attempt,
            _ => panic!("fresh fenced reservation must acquire its first attempt"),
        };
        let fenced_causation = fenced_attempt.causation_id().as_str().to_string();
        let fenced_completion = fenced_attempt
            .complete(
                TerminalCommandState::Atomic,
                serde_json::json!({"projected": "must-not-run"}),
                retention,
            )
            .unwrap();
        repository
            .mark_retryable_unknown(fenced_completion.attempt_fence())
            .await
            .unwrap();
        let fenced_direct = SameTransactionProjectionBatch::single_upsert(
            topology(),
            partition(),
            change_epoch(),
            ownership(),
            record_scope(),
            upsert_table_mutation("todo-1"),
            fenced_causation,
        )
        .unwrap();
        sqlx::query(
            "CREATE TRIGGER fail_if_fenced_projection_runs \
             BEFORE UPDATE ON sql_todo_views \
             BEGIN SELECT RAISE(ABORT, 'fenced direct projection executed'); END",
        )
        .execute(repository.pool())
        .await
        .unwrap();
        assert!(matches!(
            repository
                .commit_causal_batch(CausalCommitBatch::with_direct_projection(
                    CommitBatch::empty(),
                    fenced_completion,
                    fenced_direct,
                ))
                .await,
            Err(CommandLedgerError::AttemptFenced { .. })
        ));
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
        sqlx::query("DROP TRIGGER fail_if_fenced_projection_runs")
            .execute(repository.pool())
            .await
            .unwrap();
    }
}
