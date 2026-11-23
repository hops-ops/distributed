#[cfg(all(test, feature = "postgres"))]
mod postgres_tests {
    use std::sync::LazyLock;
    use std::time::Duration;

    use super::*;
    use crate::command_ledger::{
        CanonicalInputHash, CausalCommitBatch, CausalTransactionalCommit,
        CommandContractFingerprint, CommandId, CommandLedgerKey, CommandLedgerStore,
        CommandReservation, PrincipalPartitionId, ReservationOutcome, TerminalCommandState,
    };
    use crate::projection_protocol::{
        ProjectionCheckpointProbe, ProjectionQuerySnapshotRequest, ProjectionRecordMutation,
        ProjectionScopeCodec,
    };
    use crate::repository::{CommitBatch, ReadModelWritePlanStore};
    use crate::table::{
        ColumnType, DeleteTableRowMutation, ExpectedVersion, PrimaryKey, RowKey, RowValue,
        RowValues, RowWriteMode, TableColumn, TableKind, TableRowMutation, TableSchema,
        TableSchemaRegistry, TableStoreError, TableWritePlan,
    };

    static POSTGRES_PROJECTION_TEST_LOCK: tokio::sync::Mutex<()> =
        tokio::sync::Mutex::const_new(());

    /// A request dropped while `begin` waits for the server must not hand
    /// the next read snapshot a connection that is already in a transaction.
    #[tokio::test]
    async fn postgres_cancelled_begin_does_not_poison_the_next_read_snapshot() {
        let Some(url) = crate::test_env::require_service(
            "DISTRIBUTED_TEST_POSTGRES_URL",
            "Postgres projection protocol test",
        ) else {
            return;
        };
        let pool = <sqlx::Postgres as crate::sqlx_repo::repo::SqlxRepoBackend>::pool_options(
            sqlx::postgres::PgPoolOptions::new().max_connections(1),
        )
        .connect(&url)
        .await
        .unwrap();
        for micros in (0..3000).step_by(10) {
            let cancelled = std::time::Duration::from_micros(micros);
            let _ = tokio::time::timeout(cancelled, pool.begin()).await;
            sqlx::query("SELECT 1").execute(&pool).await.unwrap();
            let mut snapshot = pool.begin().await.unwrap();
            sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                .execute(&mut *snapshot)
                .await
                .unwrap_or_else(|error| panic!("begin cancelled at {micros}us: {error}"));
            snapshot.rollback().await.unwrap();
        }
    }

    #[tokio::test]
    async fn postgres_redelivery_identity_is_atomic_across_partition_and_cursor_races() {
        let Some(url) = crate::test_env::require_service(
            "DISTRIBUTED_TEST_POSTGRES_URL",
            "Postgres projection protocol test",
        ) else {
            return;
        };
        let _test_guard = POSTGRES_PROJECTION_TEST_LOCK.lock().await;
        let repository = SqlxRepository::<sqlx::Postgres>::connect_and_migrate(&url)
            .await
            .unwrap();
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
        let unique = uuid::Uuid::now_v7().to_string();
        let topology = topology();
        let input = |partition: &str, position: u64, id: &str, bytes: &[u8]| {
            TrustedProjectionInput::mint(
                ProjectionInputCursor::new(
                    topology.clone(),
                    PartitionId::new(format!("{unique}-{partition}").into_bytes()).unwrap(),
                    ProjectionSource::new("broker", b"retained-stream".to_vec()).unwrap(),
                    ProjectionEpoch::new("broker-v1").unwrap(),
                    position,
                )
                .unwrap(),
                ProjectionInputFingerprint::from_canonical_bytes(bytes),
                format!("{unique}-{id}"),
                "cause",
                ProjectionGeneration::initial(),
                false,
            )
            .unwrap()
        };
        let batch = |input| ProjectionCommitBatch {
            input,
            change_epoch: ProjectionEpoch::new("changes-v1").unwrap(),
            ownership: vec![ownership()],
            mutations: Vec::new(),
            observations: Vec::new(),
        };
        // The canonical unique topology/message binding arbitrates different
        // partition locks: only one scope/content may ever claim this identity.
        let (a, b) = tokio::join!(
            repository.commit_projection(batch(input("a", 1, "shared", b"one"))),
            repository.commit_projection(batch(input("b", 1, "shared", b"two"))),
        );
        assert_eq!(
            usize::from(a.is_ok()) + usize::from(b.is_ok()),
            1,
            "a={a:?}, b={b:?}"
        );
        let (partition, bytes) = if a.is_ok() {
            ("a", b"one".as_slice())
        } else {
            ("b", b"two".as_slice())
        };
        let duplicate = input(partition, 2, "shared", bytes);
        assert_eq!(
            repository
                .projection_input_disposition(&duplicate)
                .await
                .unwrap(),
            ProjectionInputDisposition::Redelivery
        );
        // Alias and new canonical input race for exactly the same broker cursor.
        // Partition serialization must forbid a cross-table double binding.
        let (alias, canonical) = tokio::join!(
            repository.commit_projection(batch(duplicate.clone())),
            repository.commit_projection(batch(input(partition, 2, "different", b"different"))),
        );
        assert_eq!(
            usize::from(alias.is_ok()) + usize::from(canonical.is_ok()),
            1
        );
        let loser = if alias.is_ok() {
            input(partition, 2, "different", b"different")
        } else {
            duplicate
        };
        assert!(matches!(
            repository.projection_input_disposition(&loser).await,
            Err(ProjectionProtocolError::InputCorruption)
        ));
        let repeated = repository
            .commit_projection(batch(input(partition, 3, "shared", bytes)))
            .await
            .unwrap();
        assert_eq!(repeated.outcome, ProjectionCommitOutcome::Duplicate);
        assert_eq!(repeated.checkpoint.unwrap().input().position(), 3);
        assert_eq!(repeated.changes.len(), 1);
        assert_eq!(repeated.changes[0].kind, ProjectionChangeKind::Checkpoint);
    }

    fn topology() -> ProjectorTopologyId {
        ProjectorTopologyId::new(1, "postgres_projection_runtime", [71; 32]).unwrap()
    }

    fn partition() -> PartitionId {
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
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: TableKind::ReadModel,
        });
        &SCHEMA
    }

    fn ownership() -> ProjectionModelOwnership {
        ProjectionModelOwnership::new("PostgresProjectionView", "postgres_projection_views")
            .unwrap()
    }

    /// The live-record read as the GraphQL query path runs it: a validated
    /// request inside one read snapshot.
    async fn live_records(
        repository: &SqlxRepository<sqlx::Postgres>,
        request: &ProjectionLiveRecordBatchRequest,
    ) -> Result<ProjectionLiveRecordBatch, ProjectionProtocolError> {
        request.validate()?;
        let request = request.clone();
        with_projection_read_snapshot(repository.pool(), move |connection| {
            Box::pin(async move {
                read_projection_live_record_batch_in_executor::<sqlx::Postgres>(
                    connection, &request,
                )
                .await
            })
        })
        .await
    }

    fn scope_codec() -> ProjectionScopeCodec {
        ProjectionScopeCodec::with_models(topology(), [("PostgresProjectionView", schema())])
            .unwrap()
    }

    fn record_key() -> RowKey {
        RowKey::new([("id", RowValue::String("runtime-row".into()))])
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
        kind: RecordTransition,
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
            RecordTransition::Delete,
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

    /// Raw writers of one table hold its ownership fence together; causal
    /// registration of that table excludes them, in both orders.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn postgres_raw_writers_share_the_table_fence_registration_excludes() {
        let Some(url) = crate::test_env::require_service(
            "DISTRIBUTED_TEST_POSTGRES_URL",
            "Postgres projection protocol test",
        ) else {
            return;
        };
        let _test_guard = POSTGRES_PROJECTION_TEST_LOCK.lock().await;
        let repository = SqlxRepository::<sqlx::Postgres>::connect_and_migrate(&url)
            .await
            .unwrap();
        let id = uuid::Uuid::now_v7();
        let schema = crate::projection_protocol::scenario_tests::scenario_schema(
            "FenceView",
            format!("fence_{}", id.simple()),
        );
        let mut registry = TableSchemaRegistry::new();
        registry.register_schema(schema.clone()).unwrap();
        repository
            .bootstrap_table_schema_for_dev(&registry)
            .await
            .unwrap();
        let tables = BTreeSet::from([schema.table_name.clone()]);
        let row = |id: &str| {
            let mut values = RowValues::new();
            values.insert("id", RowValue::String(id.into()));
            TableWritePlan::new(vec![TableMutation::UpsertRow(TableRowMutation {
                schema,
                key: RowKey::new([("id", RowValue::String(id.into()))]),
                values,
                expected_version: ExpectedVersion::Any,
                mode: RowWriteMode::Upsert,
            })])
        };
        let mut digest = [7; 32];
        digest[..16].copy_from_slice(id.as_bytes());
        let topology = ProjectorTopologyId::new(1, "fence_projector", digest).unwrap();
        let owner = ProjectionModelOwnership::new(&schema.model_name, &schema.table_name).unwrap();
        let register = || {
            let repository = repository.clone();
            let (topology, owner) = (topology.clone(), owner.clone());
            tokio::spawn(async move {
                repository
                    .register_projection_models(&topology, &[owner])
                    .await
            })
        };
        let blocked = Duration::from_millis(300);

        // The table's first writer creates its fence, and until that commits
        // a second writer waits to learn whether the fence exists.
        repository.commit_write_plan(row("first")).await.unwrap();

        // From then on an open writer does not make a writer of another row
        // wait.
        let mut open_writer = repository.pool().begin().await.unwrap();
        reject_causal_table_writes_in_tx(&mut open_writer, &tables)
            .await
            .unwrap();
        apply_read_model_write_plan_in_tx(&mut open_writer, row("open"))
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(10),
            repository.commit_write_plan(row("concurrent")),
        )
        .await
        .expect("a raw writer waited for another raw writer of the same table")
        .unwrap();

        // Registration waits for the open writer, then finds its row.
        let registration = register();
        tokio::time::sleep(blocked).await;
        assert!(!registration.is_finished());
        open_writer.commit().await.unwrap();
        assert!(matches!(
            registration.await.unwrap(),
            Err(ProjectionProtocolError::InvalidBatch(message))
                if message.contains("unverified legacy rows")
        ));

        // A writer waits for an open registration, then finds its marker.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM {}",
            quote_identifier(&schema.table_name)
        )))
        .execute(repository.pool())
        .await
        .unwrap();
        let mut open_registration = repository.pool().begin().await.unwrap();
        lock_projection_table_ownership_fences_in_tx(
            &mut open_registration,
            &tables,
            <sqlx::Postgres as SqlxRepoBackend>::OWNERSHIP_FENCE_REGISTRATION_LOCK,
        )
        .await
        .unwrap();
        let writer = {
            let repository = repository.clone();
            let plan = row("late");
            tokio::spawn(async move { repository.commit_write_plan(plan).await })
        };
        tokio::time::sleep(blocked).await;
        assert!(!writer.is_finished());
        sqlx::query(
            "INSERT INTO projection_causal_tables (table_name, model_name) VALUES ($1, $2)",
        )
        .bind(&schema.table_name)
        .bind(&schema.model_name)
        .execute(&mut *open_registration)
        .await
        .unwrap();
        open_registration.commit().await.unwrap();
        assert!(matches!(
            writer.await.unwrap(),
            Err(TableStoreError::CausalWriteRequired { table }) if table == schema.table_name
        ));
        register().await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn postgres_projection_protocol_runtime_conforms() {
        let Some(database_url) = crate::test_env::require_service(
            "DISTRIBUTED_TEST_POSTGRES_URL",
            "Postgres projection protocol test",
        ) else {
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
        reject_causal_table_writes_in_tx(&mut raw_first_tx, &ownership_tables)
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
                RecordTransition::Upsert,
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
                        RecordTransition::Upsert,
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
                    RecordTransition::Recreate,
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
                        RecordTransition::Upsert,
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
                        RecordTransition::Upsert,
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
                    RecordTransition::Upsert,
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
                    RecordTransition::Upsert,
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
                                RecordTransition::Upsert,
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
        let committed_live = live_records(
            &repository,
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

    /// The shared conformance suite against a server that keeps its data.
    /// Every scenario gets its own topology, tables and failure IDs, so
    /// scenarios never meet, in one run or across runs against one database.
    /// It holds the suite lock for its lifetime because other tests here
    /// assert on database-wide notifications.
    struct PostgresScenario {
        url: String,
        /// Unique per scenario; a topology is identified by its digest.
        id: uuid::Uuid,
        schema: &'static TableSchema,
        _suite: tokio::sync::MutexGuard<'static, ()>,
    }

    impl PostgresScenario {
        async fn new() -> Option<Self> {
            let url = crate::test_env::require_service(
                "DISTRIBUTED_TEST_POSTGRES_URL",
                "Postgres projection protocol conformance test",
            )?;
            let id = uuid::Uuid::now_v7();
            Some(Self {
                url,
                id,
                schema: crate::projection_protocol::scenario_tests::scenario_schema(
                    "ScenarioView",
                    format!("scenario_{}", id.simple()),
                ),
                _suite: POSTGRES_PROJECTION_TEST_LOCK.lock().await,
            })
        }

        fn topology_digest(&self, role: u8) -> [u8; 32] {
            let mut digest = [role; 32];
            digest[..16].copy_from_slice(self.id.as_bytes());
            digest
        }

        /// `statement` with `{table}` naming the scenario's own table.
        fn on_table(&self, statement: &str) -> sqlx::AssertSqlSafe<String> {
            sqlx::AssertSqlSafe(statement.replace("{table}", &self.schema.table_name))
        }

        fn partition_hashes(&self) -> (Vec<u8>, Vec<u8>) {
            use crate::projection_protocol::scenario_tests::ProjectionProtocolScenario;
            (
                self.topology().digest().to_vec(),
                self.partition().digest().to_vec(),
            )
        }
    }

    impl crate::projection_protocol::scenario_tests::ProjectionProtocolScenario for PostgresScenario {
        type Store = SqlxRepository<sqlx::Postgres>;

        async fn store(&self, schemas: &[&'static TableSchema]) -> Self::Store {
            let repository = SqlxRepository::<sqlx::Postgres>::connect_and_migrate(&self.url)
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
                .register_projection_models(&self.topology(), &owners)
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
            ProjectorTopologyId::new(1, &self.schema.table_name, self.topology_digest(1)).unwrap()
        }

        fn other_topology(&self) -> ProjectorTopologyId {
            ProjectorTopologyId::new(
                1,
                format!("other_{}", self.schema.table_name),
                self.topology_digest(2),
            )
            .unwrap()
        }

        fn schema(&self) -> &'static TableSchema {
            self.schema
        }

        fn failure_id(&self, name: &str) -> String {
            format!("{name}-{}", self.id.simple())
        }

        async fn physical_row(
            &self,
            store: &Self::Store,
            id: &str,
        ) -> Option<(Option<String>, u64)> {
            sqlx::query(self.on_table("SELECT value, _sourced_version FROM {table} WHERE id = $1"))
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
            sqlx::query(self.on_table("INSERT INTO {table} (id, _sourced_version) VALUES ($1, 1)"))
                .bind(id)
                .execute(store.pool())
                .await
                .unwrap();
        }

        async fn remove_physical_row(&self, store: &Self::Store, id: &str) {
            let removed = sqlx::query(self.on_table("DELETE FROM {table} WHERE id = $1"))
                .bind(id)
                .execute(store.pool())
                .await
                .unwrap();
            assert_eq!(removed.rows_affected(), 1);
        }

        async fn fail_writes_to_row(&self, store: &Self::Store, id: &str, fail: bool) {
            let statements = if fail {
                vec![
                    "CREATE OR REPLACE FUNCTION distributed_test_fail_projection_row_write() \
                     RETURNS trigger LANGUAGE plpgsql AS $$ \
                     BEGIN RAISE EXCEPTION 'forced projection row failure'; END; $$"
                        .to_string(),
                    format!(
                        "CREATE TRIGGER fail_projection_row_write BEFORE UPDATE ON {{table}} \
                         FOR EACH ROW WHEN (NEW.id = '{id}') \
                         EXECUTE FUNCTION distributed_test_fail_projection_row_write()"
                    ),
                ]
            } else {
                vec!["DROP TRIGGER fail_projection_row_write ON {table}".to_string()]
            };
            for statement in statements {
                sqlx::query(self.on_table(&statement))
                    .execute(store.pool())
                    .await
                    .unwrap();
            }
        }

        async fn remove_stored_change(&self, store: &Self::Store, position: u64) {
            let (topology_hash, partition_hash) = self.partition_hashes();
            let removed = sqlx::query(
                "DELETE FROM projection_changes \
                 WHERE topology_hash = $1 AND partition_hash = $2 AND change_position = $3",
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
                 WHERE topology_hash = $1 AND partition_hash = $2 ORDER BY change_position",
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
        crate::projection_protocol::scenario_tests::projection_protocol_scenario_tests!(
            super::PostgresScenario::new().await
        );
    }
}
