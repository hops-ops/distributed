#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::LazyLock;
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
        ProjectionLiveRecordRequest, ProjectionObservationRequest, ProjectionRecordMutation,
        ProjectionScopeCodec,
    };
    use crate::repository::{CommitBatch, ReadModelWritePlanStore};
    use crate::table::{
        ColumnType, DeleteTableRowMutation, ExpectedVersion, PrimaryKey, RowKey, RowValue,
        RowValues, RowWriteMode, TableColumn, TableKind, TableRowMutation, TableSchema,
        TableSchemaRegistry,
    };

    fn topology() -> ProjectorTopologyId {
        ProjectorTopologyId::new(1, "sql_todo_projector", [17; 32]).unwrap()
    }

    fn other_topology() -> ProjectorTopologyId {
        ProjectorTopologyId::new(1, "other_sql_todo_projector", [99; 32]).unwrap()
    }

    fn partition() -> PartitionId {
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

    fn ownership() -> ProjectionModelOwnership {
        ProjectionModelOwnership::new("SqlTodoView", "sql_todo_views").unwrap()
    }

    fn semantic_program_id(fill: char) -> crate::ProjectionProgramId {
        let hex = std::iter::repeat_n(fill, 64).collect::<String>();
        crate::ProjectionProgramId::parse(&format!("pp1:sha256:{hex}"))
            .expect("test semantic program ID is canonical")
    }

    /// The shared conformance suite on a fresh in-memory SQLite database.
    struct ProjectionScenario;

    impl ProjectionScenario {
        fn partition_hashes(&self) -> (Vec<u8>, Vec<u8>) {
            use crate::projection_protocol::scenario_tests::ProjectionProtocolScenario;
            (
                topology().digest().to_vec(),
                self.partition().digest().to_vec(),
            )
        }
    }

    impl crate::projection_protocol::scenario_tests::ProjectionProtocolScenario for ProjectionScenario {
        type Store = SqlxRepository<sqlx::Sqlite>;

        async fn store(&self, schemas: &[&'static TableSchema]) -> Self::Store {
            let repository = SqlxRepository::<sqlx::Sqlite>::connect_and_migrate("sqlite::memory:")
                .await
                .unwrap();
            let mut registry = TableSchemaRegistry::new();
            let mut owners = Vec::new();
            for schema in schemas {
                registry.register_schema((*schema).clone()).unwrap();
                owners.push(
                    ProjectionModelOwnership::new(&schema.model_name, &schema.table_name).unwrap(),
                );
            }
            repository
                .bootstrap_table_schema_for_dev(&registry)
                .await
                .unwrap();
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

        async fn physical_row(
            &self,
            store: &Self::Store,
            id: &str,
        ) -> Option<(Option<String>, u64)> {
            sqlx::query("SELECT value, _sourced_version FROM sql_todo_views WHERE id = ?")
                .bind(id)
                .fetch_optional(store.pool())
                .await
                .unwrap()
                .map(|row| {
                    (
                        row.get("value"),
                        row.get::<i64, _>("_sourced_version") as u64,
                    )
                })
        }

        async fn insert_physical_row(&self, store: &Self::Store, id: &str) {
            sqlx::query("INSERT INTO sql_todo_views (id, _sourced_version) VALUES (?, 1)")
                .bind(id)
                .execute(store.pool())
                .await
                .unwrap();
        }

        async fn remove_physical_row(&self, store: &Self::Store, id: &str) {
            let removed = sqlx::query("DELETE FROM sql_todo_views WHERE id = ?")
                .bind(id)
                .execute(store.pool())
                .await
                .unwrap();
            assert_eq!(removed.rows_affected(), 1);
        }

        async fn fail_writes_to_row(&self, store: &Self::Store, id: &str, fail: bool) {
            let statement = if fail {
                format!(
                    "CREATE TRIGGER fail_projection_row_write BEFORE UPDATE ON sql_todo_views \
                     WHEN NEW.id = '{id}' \
                     BEGIN SELECT RAISE(ABORT, 'forced projection row failure'); END"
                )
            } else {
                "DROP TRIGGER fail_projection_row_write".to_string()
            };
            sqlx::query(sqlx::AssertSqlSafe(statement))
                .execute(store.pool())
                .await
                .unwrap();
        }

        async fn remove_stored_change(&self, store: &Self::Store, position: u64) {
            let (topology_hash, partition_hash) = self.partition_hashes();
            let removed = sqlx::query(
                "DELETE FROM projection_changes \
                 WHERE topology_hash = ? AND partition_hash = ? AND change_position = ?",
            )
            .bind(topology_hash)
            .bind(partition_hash)
            .bind(position as i64)
            .execute(store.pool())
            .await
            .unwrap();
            assert_eq!(removed.rows_affected(), 1);
        }

        async fn stored_change_positions(&self, store: &Self::Store) -> Vec<u64> {
            let (topology_hash, partition_hash) = self.partition_hashes();
            sqlx::query_scalar::<_, i64>(
                "SELECT change_position FROM projection_changes \
                 WHERE topology_hash = ? AND partition_hash = ? ORDER BY change_position",
            )
            .bind(topology_hash)
            .bind(partition_hash)
            .fetch_all(store.pool())
            .await
            .unwrap()
            .into_iter()
            .map(|position| position as u64)
            .collect()
        }
    }

    mod scenarios {
        crate::projection_protocol::scenario_tests::projection_protocol_scenario_tests!(Some(
            super::ProjectionScenario
        ));
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

    async fn reopen_wal_repository(path: &Path) -> SqlxRepository<sqlx::Sqlite> {
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(path)
                    .create_if_missing(false)
                    .journal_mode(SqliteJournalMode::Wal),
            )
            .await
            .unwrap();
        let repository = SqlxRepository::<sqlx::Sqlite>::new(pool)
            .with_projection_change_retention(ProjectionChangeRetention::new(16).unwrap());
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
        repository
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
            RecordTransition::Upsert | RecordTransition::Recreate => {
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

        evidence_repository
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
                    RecordTransition::Upsert,
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
                    RecordTransition::Upsert,
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
                    RecordTransition::Delete,
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
                    RecordTransition::Upsert,
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
                    RecordTransition::Upsert,
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
    async fn sqlite_modeled_projection_identity_is_durable_across_restart_and_null_history_stays_readable(
    ) {
        let (repository, database_path) = wal_repository_with_retention(16).await;
        let program_a = semantic_program_id('a');
        let scope = record_scope();
        let result = repository
            .commit_projection(ProjectionCommitBatch {
                input: input(
                    1,
                    b"semantic-identity-a",
                    "semantic-identity-message-a",
                    "semantic-identity-cause-a",
                    ProjectionGeneration::initial(),
                ),
                change_epoch: change_epoch(),
                ownership: vec![ownership().with_program_id(program_a)],
                mutations: vec![mutation(
                    ProjectionRecordExpectation::Missing,
                    RecordTransition::Upsert,
                )],
                observations: vec![ProjectionObservationRequest {
                    kind: ProjectionObservationKind::Record,
                    target: ProjectionObservationTarget::StagedRecord(scope.clone()),
                }],
            })
            .await
            .unwrap();
        assert!(result
            .changes
            .iter()
            .all(|change| change.program_id == Some(program_a)));

        let selected =
            ProjectionCausationEvidenceRequest::new("semantic-identity-cause-a", vec![topology()])
                .unwrap();
        let evidence = repository
            .projection_causation_evidence(&selected)
            .await
            .unwrap();
        assert_eq!(evidence.observations.len(), 1);
        assert_eq!(evidence.observations[0].program_id, Some(program_a));

        repository.pool().close().await;
        let reopened = reopen_wal_repository(&database_path).await;
        let changes = match reopened
            .projection_changes(&topology(), &partition(), None, 100)
            .await
            .unwrap()
        {
            ProjectionChangeRead::Changes { changes, .. } => changes,
            other => panic!("restarted repository must retain projection changes: {other:?}"),
        };
        assert_eq!(
            changes.len(),
            1,
            "the staged observation shares the record change"
        );
        assert!(changes
            .iter()
            .all(|change| change.program_id == Some(program_a)));
        let evidence = reopened
            .projection_causation_evidence(&selected)
            .await
            .unwrap();
        assert_eq!(evidence.observations.len(), 1);
        assert_eq!(evidence.observations[0].program_id, Some(program_a));

        // Rows written before semantic identities existed remain useful after
        // migration, but their null identity cannot mint modeled proof.
        sqlx::query("UPDATE projection_changes SET program_id = NULL WHERE causation_id = ?")
            .bind("semantic-identity-cause-a")
            .execute(reopened.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE projection_observations SET program_id = NULL WHERE causation_id = ?")
            .bind("semantic-identity-cause-a")
            .execute(reopened.pool())
            .await
            .unwrap();
        let null_history = reopened
            .projection_causation_evidence(&selected)
            .await
            .unwrap();
        assert_eq!(null_history.observations.len(), 1);
        assert_eq!(null_history.observations[0].program_id, None);
        let readable = reopened
            .projection_live_record_batch(
                &ProjectionLiveRecordBatchRequest::new(vec![ProjectionLiveRecordRequest::new(
                    &scope_codec(),
                    "SqlTodoView",
                    record_key(),
                )
                .unwrap()])
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(
            readable.records[0].is_some(),
            "unversioned rows remain readable"
        );
        assert_eq!(
            readable.records[0].as_ref().unwrap().revision.scope(),
            &scope
        );

        remove_wal_database(reopened, &database_path).await;
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

    /// A commit that rolls back publishes no read-model change to local
    /// subscribers. The rollback itself is the shared suite's business.
    #[tokio::test]
    async fn sqlite_rolled_back_projection_commit_publishes_no_read_model_change() {
        let repository = repository().await;
        let created = repository
            .commit_projection(batch(
                input(
                    1,
                    b"create",
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
        sqlx::query(
            "CREATE TRIGGER fail_sql_todo_update BEFORE UPDATE ON sql_todo_views \
             BEGIN SELECT RAISE(ABORT, 'forced projection row failure'); END",
        )
        .execute(repository.pool())
        .await
        .unwrap();
        let mut live_changes = repository.read_model_changes();
        assert!(matches!(
            repository
                .commit_projection(batch(
                    input(
                        2,
                        b"update",
                        "message-2",
                        "cause-2",
                        ProjectionGeneration::initial(),
                    ),
                    vec![mutation(
                        ProjectionRecordExpectation::Exact(created.records[0].revision.clone()),
                        RecordTransition::Upsert,
                    )],
                    Vec::new(),
                ))
                .await,
            Err(ProjectionProtocolError::Table(
                TableStoreError::BackendStorage { .. }
            ))
        ));
        assert!(matches!(
            live_changes.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
    }

    /// A same-transaction projection reports physical drift with the same
    /// typed errors as a commit; the commit side is the shared suite's.
    #[tokio::test]
    async fn sqlite_direct_projection_reports_typed_physical_metadata_drift() {
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

        let missing_physical = repository().await;
        missing_physical
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
                    RecordTransition::Upsert,
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
        let mut tx = missing_physical.pool().begin().await.unwrap();
        assert!(matches!(
            apply_same_transaction_projection_in_tx(
                &mut tx,
                &direct("direct-missing-physical"),
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
        let mut tx = untracked_physical.pool().begin().await.unwrap();
        assert!(matches!(
            apply_same_transaction_projection_in_tx(
                &mut tx,
                &direct("direct-untracked-physical"),
                untracked_physical.projection_change_retention(),
            )
            .await,
            Err(ProjectionProtocolError::RecordAlreadyExists { .. })
        ));
    }

    /// Commit and failure read the registered topology before they take
    /// the partition lock, so they must hold the write lock from the start.
    /// While another connection holds it they wait; a deferred transaction
    /// would be refused as busy the moment it tried to write.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_projection_commit_and_failure_wait_for_a_concurrent_writer() {
        let database_path = std::env::temp_dir().join(format!(
            "distributed-projection-write-wait-{}.sqlite",
            uuid::Uuid::now_v7()
        ));
        let pool = SqlitePoolOptions::new()
            .max_connections(3)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&database_path)
                    .create_if_missing(true),
            )
            .await
            .unwrap();
        let repository = SqlxRepository::<sqlx::Sqlite>::new(pool);
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

        let commit = {
            let repository = repository.clone();
            async move {
                repository
                    .commit_projection(batch(
                        input(
                            1,
                            b"wait-one",
                            "wait-message-1",
                            "wait-cause-1",
                            ProjectionGeneration::initial(),
                        ),
                        Vec::new(),
                        Vec::new(),
                    ))
                    .await
                    .map(|_| ())
                    .map_err(|error| error.to_string())
            }
        };
        while_another_connection_writes(repository.pool(), commit).await;
        let failure = {
            let repository = repository.clone();
            async move {
                repository
                    .record_projection_failure(
                        ProjectionFailureBatch::new(
                            input(
                                2,
                                b"wait-two",
                                "wait-message-2",
                                "wait-cause-2",
                                ProjectionGeneration::initial(),
                            ),
                            change_epoch(),
                            "wait-failure",
                            "decode_error",
                            b"bad payload".to_vec(),
                        )
                        .unwrap(),
                    )
                    .await
                    .map(|_| ())
                    .map_err(|error| error.to_string())
            }
        };
        while_another_connection_writes(repository.pool(), failure).await;
        repository.pool().close().await;
        let _ = std::fs::remove_file(database_path);
    }

    /// Run `operation` while another connection holds the write lock: it must
    /// still be waiting after a pause, and succeed once the lock is released.
    async fn while_another_connection_writes(
        pool: &sqlx::SqlitePool,
        operation: impl std::future::Future<Output = Result<(), String>> + Send + 'static,
    ) {
        let writer = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
        let mut operation = tokio::spawn(operation);
        let waited =
            tokio::time::timeout(std::time::Duration::from_millis(200), &mut operation).await;
        assert!(waited.is_err(), "did not wait for the writer: {waited:?}");
        writer.commit().await.unwrap();
        operation.await.unwrap().unwrap();
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
            let store = crate::graphql::delivery::GatewayVersionStore::install(
                &pool,
                "atomic-cache-test",
                tables.clone(),
            )
            .await
            .unwrap();
            let before =
                serde_json::to_value(store.current(&pool, &tables).await.unwrap()).unwrap();
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
            let committed = serde_json::to_value(
                cache_store
                    .current(&cache_pool, &cache_tables)
                    .await
                    .unwrap(),
            )
            .unwrap();
            assert_ne!(
                committed, cache_before,
                "Atomic result and cache dependency versions commit together"
            );
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
        assert_eq!(
            serde_json::to_value(
                cache_store
                    .current(&cache_pool, &cache_tables)
                    .await
                    .unwrap()
            )
            .unwrap(),
            cache_committed,
            "failed Atomic ledger completion must roll back cache versions too"
        );
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
