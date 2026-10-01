use super::*;

async fn preflight_command_completion_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    completion: &CommandCompletion,
) -> Result<(), CommandLedgerError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    crate::repository::sql::ledger::preflight(
        &mut executor::ConnectionExecutor::<DB>(&mut **tx),
        completion,
    )
    .await
}

async fn commit_sqlx_batch<'a, DB>(
    repository: &'a SqlxRepository<DB>,
    batch: CommitBatch<'a>,
    mut completion: Option<CommandCompletion>,
    direct_projection: Option<SameTransactionProjectionBatch>,
) -> Result<(), CommandLedgerError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    for<'c> &'c Pool<DB>: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> f64: Encode<'q, DB> + Type<DB>,
    for<'q> String: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'q> Option<i64>: Encode<'q, DB> + Type<DB>,
    for<'q> Option<&'q str>: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let prepared = validate_commit_batch(&batch)?;
    for plan in &batch.read_model_plans {
        validate_sql_write_plan(plan).map_err(RepositoryError::from)?;
    }
    if let Some(direct_projection) = &direct_projection {
        direct_projection.validate().map_err(|error| {
            CommandLedgerError::Storage(RepositoryError::Model(error.to_string()))
        })?;
        let completion = completion.as_ref().ok_or_else(|| {
            CommandLedgerError::Invalid(
                "same-transaction direct projection requires a command completion".into(),
            )
        })?;
        if direct_projection.causation_id != completion.attempt().causation_id().as_str() {
            return Err(CommandLedgerError::Invalid(
                "direct projection causation differs from its command attempt".into(),
            ));
        }
    }

    let mut tx = repository
        .pool
        .begin()
        .await
        .map_err(|err| repository_storage_error::<DB>("begin commit transaction", err))?;
    if let Some(completion) = completion.as_ref() {
        preflight_command_completion_in_tx(&mut tx, completion).await?;
    }

    let requested_tables = batch
        .read_model_plans
        .iter()
        .flat_map(|plan| plan.mutations.iter())
        .map(|mutation| mutation.table_name().to_string())
        .collect::<std::collections::BTreeSet<_>>();
    reject_causal_table_writes_in_tx(&mut tx, &requested_tables)
        .await
        .map_err(RepositoryError::from)?;

    let versions = stream_versions_in_tx(&mut tx, &prepared).await?;
    for append in &prepared {
        let actual = versions
            .get(&append.identity.storage_key())
            .copied()
            .unwrap_or(0);
        if actual != append.expected_version {
            return Err(RepositoryError::ConcurrentWrite {
                id: append.identity.to_string(),
                expected: append.expected_version,
                actual,
            }
            .into());
        }
    }

    insert_events_in_tx(&repository.pool, &mut tx, &prepared).await?;
    insert_outbox_messages_in_tx(&mut tx, &batch.outbox_messages).await?;

    let mut changed_tables = std::collections::BTreeSet::new();
    for plan in batch.read_model_plans {
        for mutation in &plan.mutations {
            changed_tables.insert(mutation.table_name().to_string());
        }
        apply_read_model_write_plan_in_tx(&mut tx, plan)
            .await
            .map_err(RepositoryError::from)?;
    }

    if let Some(direct_projection) = &direct_projection {
        let evidence = apply_same_transaction_projection_in_tx(
            &mut tx,
            direct_projection,
            repository.projection_change_retention,
        )
        .await
        .map_err(|error| CommandLedgerError::Storage(RepositoryError::Model(error.to_string())))?;
        let completion = completion
            .as_mut()
            .expect("direct projection completion was validated before opening its transaction");
        completion.attach_direct_projection(&evidence)?;
        for mutation in &direct_projection.mutations {
            changed_tables.insert(mutation.mutation.table_name().to_string());
        }
        changed_tables.insert(PROJECTION_CHANGE_NOTIFY_TABLE.to_string());
    }

    for write in batch.snapshots {
        match write {
            SnapshotWrite::Save { identity, record } => {
                save_snapshot_in_tx(&mut tx, &identity, record).await?;
            }
        }
    }
    for receipt in &batch.inbox_receipts {
        insert_inbox_receipt_in_tx(&mut tx, receipt).await?;
    }

    if repository.notify_enabled && !changed_tables.is_empty() {
        DB::push_change_notify(&mut *tx, &changed_tables)
            .await
            .map_err(RepositoryError::from)?;
    }

    // This fenced terminal update is intentionally the final SQL statement
    // before COMMIT. A stale/expired generation affects zero rows and rolls
    // back every domain write above with the surrounding transaction.
    if let Some(completion) = completion.as_ref() {
        complete_command_in_tx(&mut tx, completion).await?;
    }

    tx.commit()
        .await
        .map_err(|err| repository_storage_error::<DB>("commit transaction", err))?;

    if !changed_tables.is_empty() {
        repository.publish_read_model_change(crate::ReadModelChange {
            tables: changed_tables,
        });
    }
    for stream in batch.streams {
        stream.entity.mark_committed();
    }
    Ok(())
}

impl<DB> TransactionalCommit for SqlxRepository<DB>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    for<'c> &'c Pool<DB>: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> f64: Encode<'q, DB> + Type<DB>,
    for<'q> String: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'q> Option<i64>: Encode<'q, DB> + Type<DB>,
    for<'q> Option<&'q str>: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    fn commit_batch<'a>(
        &'a self,
        batch: CommitBatch<'a>,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
        async move {
            match commit_sqlx_batch(self, batch, None, None).await {
                Ok(()) => Ok(()),
                Err(CommandLedgerError::Storage(error)) => Err(error),
                Err(error) => Err(RepositoryError::Model(format!(
                    "unexpected command ledger error in ordinary commit: {error}"
                ))),
            }
        }
    }
}

impl<DB> CausalTransactionalCommit for SqlxRepository<DB>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    for<'c> &'c Pool<DB>: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> f64: Encode<'q, DB> + Type<DB>,
    for<'q> String: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'q> Option<i64>: Encode<'q, DB> + Type<DB>,
    for<'q> Option<&'q str>: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    fn commit_causal_batch<'a>(
        &'a self,
        batch: CausalCommitBatch<'a>,
    ) -> impl Future<Output = Result<(), CommandLedgerError>> + Send + 'a {
        commit_sqlx_batch(
            self,
            batch.domain,
            Some(batch.completion),
            batch.direct_projection,
        )
    }
}

async fn complete_command_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    completion: &CommandCompletion,
) -> Result<(), CommandLedgerError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    crate::repository::sql::ledger::complete(
        &mut executor::ConnectionExecutor::<DB>(&mut **tx),
        completion,
    )
    .await
}

impl<DB> CommandLedgerStore for SqlxRepository<DB>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    async fn reserve_command(
        &self,
        reservation: CommandReservation,
    ) -> Result<ReservationOutcome, CommandLedgerError> {
        let mut tx =
            self.pool.begin().await.map_err(|error| {
                repository_storage_error::<DB>("begin command reservation", error)
            })?;
        let outcome = crate::repository::sql::ledger::reserve(
            &mut executor::ConnectionExecutor::<DB>(&mut *tx),
            &reservation,
        )
        .await?;
        tx.commit()
            .await
            .map_err(|error| repository_storage_error::<DB>("commit command reservation", error))?;
        Ok(outcome)
    }

    async fn lookup_command<'a>(
        &'a self,
        key: &'a CommandLedgerKey,
        scope: CommandLookupScope<'a>,
    ) -> Result<CommandLookup, CommandLedgerError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| repository_storage_error::<DB>("begin command lookup", error))?;
        let outcome = crate::repository::sql::ledger::lookup(
            &mut executor::ConnectionExecutor::<DB>(&mut *tx),
            key,
            scope,
        )
        .await?;
        tx.commit()
            .await
            .map_err(|error| repository_storage_error::<DB>("commit command lookup", error))?;
        Ok(outcome)
    }

    async fn mark_retryable_unknown(
        &self,
        attempt: AttemptFence,
    ) -> Result<(), CommandLedgerError> {
        let mut connection = self.pool.acquire().await.map_err(|error| {
            repository_storage_error::<DB>("acquire command ledger connection", error)
        })?;
        crate::repository::sql::ledger::mark_retryable(
            &mut executor::ConnectionExecutor::<DB>(&mut *connection),
            &attempt,
        )
        .await
    }

    async fn complete_external_command(
        &self,
        completion: ExternalCommandCompletion,
    ) -> Result<(), CommandLedgerError> {
        let mut tx = self.pool.begin().await.map_err(|error| {
            repository_storage_error::<DB>("begin external command completion", error)
        })?;
        crate::repository::sql::ledger::complete_external(
            &mut executor::ConnectionExecutor::<DB>(&mut *tx),
            &completion,
        )
        .await?;
        tx.commit().await.map_err(|error| {
            repository_storage_error::<DB>("commit external command completion", error)
        })?;
        Ok(())
    }

    async fn compact_expired_commands(&self, limit: usize) -> Result<u64, CommandLedgerError> {
        if limit == 0 {
            return Ok(0);
        }
        let mut tx =
            self.pool.begin().await.map_err(|error| {
                repository_storage_error::<DB>("begin command compaction", error)
            })?;
        let count = crate::repository::sql::ledger::compact(
            &mut executor::ConnectionExecutor::<DB>(&mut *tx),
            limit,
        )
        .await?;
        tx.commit()
            .await
            .map_err(|error| repository_storage_error::<DB>("commit command compaction", error))?;
        Ok(count)
    }
}
/// Record a consumer inbox receipt in the commit transaction. The
/// `(consumer, message_id)` primary key is the dedupe gate: a unique violation
/// means the message was already processed, so the whole batch rolls back and
/// the effects are not double-applied. `processed_at` defaults server-side.
async fn insert_inbox_receipt_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    receipt: &InboxReceipt,
) -> Result<(), RepositoryError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
{
    receipt.validate()?;
    let mut builder =
        QueryBuilder::<DB>::new("INSERT INTO consumer_inbox (consumer, message_id) VALUES (");
    builder.push_bind(receipt.consumer.as_str());
    builder.push(", ");
    builder.push_bind(receipt.message_id.as_str());
    builder.push(")");
    let result = builder.build().execute(&mut **tx).await;
    match result {
        Ok(_) => Ok(()),
        Err(err) if DB::is_unique_violation(&err) => Err(RepositoryError::DuplicateInboxReceipt {
            consumer: receipt.consumer.clone(),
            message_id: receipt.message_id.clone(),
        }),
        Err(err) => Err(repository_storage_error::<DB>(
            "insert consumer inbox receipt",
            err,
        )),
    }
}

/// Execute the shared event insert plan in the command's existing transaction.
async fn insert_events_in_tx<DB>(
    pool: &Pool<DB>,
    tx: &mut Transaction<'_, DB>,
    prepared: &[PreparedEventAppend<'_>],
) -> Result<(), RepositoryError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    for<'c> &'c Pool<DB>: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    use crate::repository::sql::SqlExecutor;
    for insert in crate::repository::sql::event_inserts(prepared, DB::MAX_BIND_PARAMS)? {
        let result = super::executor::ConnectionExecutor::<DB>(&mut **tx)
            .execute(insert.statement)
            .await;
        if let Err(error) = result {
            let unique = match &error {
                RepositoryError::Storage {
                    source: Some(source),
                    ..
                } => source
                    .downcast_ref::<sqlx::Error>()
                    .is_some_and(DB::is_unique_violation),
                _ => false,
            };
            if !unique {
                return Err(error);
            }
            return Err(if DB::CONFLICT_REREAD_IN_TX {
                let candidates: Vec<_> = insert
                    .candidates
                    .iter()
                    .map(|(identity, version)| (identity, *version))
                    .collect();
                concurrent_write_from_conflict(&mut **tx, &candidates).await
            } else {
                let candidates: Vec<_> = prepared
                    .iter()
                    .map(|append| (&append.identity, append.expected_version))
                    .collect();
                match pool.acquire().await {
                    Ok(mut conn) => concurrent_write_from_conflict(&mut conn, &candidates).await,
                    Err(error) => {
                        repository_storage_error::<DB>("acquire conflict re-read connection", error)
                    }
                }
            });
        }
    }
    Ok(())
}

/// After an event-insert unique violation, find the candidate stream whose
/// actual version no longer matches its expected version and report it as
/// `ConcurrentWrite`. Falls back to the first candidate if a concurrent
/// writer's effect cannot be pinned down (the violation still indicates a
/// conflicting write). Candidates must be non-empty and deduplicated.
async fn concurrent_write_from_conflict<DB>(
    conn: &mut DB::Connection,
    candidates: &[(&StreamIdentity, u64)],
) -> RepositoryError
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    for &(identity, expected) in candidates {
        match stream_version(&mut *conn, identity).await {
            Ok(actual) if actual != expected => {
                return RepositoryError::ConcurrentWrite {
                    id: identity.to_string(),
                    expected,
                    actual,
                };
            }
            Ok(_) => {}
            Err(err) => return err,
        }
    }

    let (identity, expected) = candidates[0];
    match stream_version(&mut *conn, identity).await {
        Ok(actual) => RepositoryError::ConcurrentWrite {
            id: identity.to_string(),
            expected,
            actual,
        },
        Err(err) => err,
    }
}

/// Current committed version (`MAX(sequence)`, 0 for a missing stream) through
/// any executor (pool or transaction).
pub(super) async fn stream_version<'e, DB, E>(
    executor: E,
    identity: &StreamIdentity,
) -> Result<u64, RepositoryError>
where
    DB: SqlxRepoBackend,
    E: Executor<'e, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let mut builder = QueryBuilder::<DB>::new(
        "SELECT MAX(sequence) AS version FROM aggregate_events WHERE aggregate_type = ",
    );
    builder.push_bind(identity.aggregate_type());
    builder.push(" AND aggregate_id = ");
    builder.push_bind(identity.aggregate_id());
    let row = builder
        .build()
        .fetch_one(executor)
        .await
        .map_err(|err| repository_storage_error::<DB>("load stream version", err))?;

    let version: Option<i64> = row
        .try_get("version")
        .map_err(|err| repository_storage_error::<DB>("decode stream version row", err))?;
    version
        .map(|value| repository_u64_from_i64(DB::BACKEND, value, "sequence"))
        .unwrap_or(Ok(0))
}

/// Current committed versions for every stream in the batch, in one grouped
/// query (`MAX(sequence)` per stream; missing streams simply have no row and
/// default to 0 at the call site). Chunked so a very large batch stays under
/// the backend's bound-parameter limit (two binds per stream).
async fn stream_versions_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    prepared: &[PreparedEventAppend<'_>],
) -> Result<HashMap<String, u64>, RepositoryError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let mut versions = HashMap::with_capacity(prepared.len());
    if prepared.is_empty() {
        return Ok(versions);
    }

    for chunk in prepared.chunks(DB::MAX_BIND_PARAMS / 2) {
        let mut builder = QueryBuilder::<DB>::new(
            "SELECT aggregate_type, aggregate_id, MAX(sequence) AS version \
             FROM aggregate_events WHERE ",
        );
        let mut first = true;
        for append in chunk {
            if !first {
                builder.push(" OR ");
            }
            first = false;
            builder.push("(aggregate_type = ");
            builder.push_bind(append.identity.aggregate_type());
            builder.push(" AND aggregate_id = ");
            builder.push_bind(append.identity.aggregate_id());
            builder.push(")");
        }
        builder.push(" GROUP BY aggregate_type, aggregate_id");

        let rows = builder
            .build()
            .fetch_all(&mut **tx)
            .await
            .map_err(|err| repository_storage_error::<DB>("load stream versions", err))?;

        for row in rows {
            let aggregate_type: String = row.try_get("aggregate_type").map_err(|err| {
                repository_storage_error::<DB>("decode stream version aggregate type row", err)
            })?;
            let aggregate_id: String = row.try_get("aggregate_id").map_err(|err| {
                repository_storage_error::<DB>("decode stream version aggregate id row", err)
            })?;
            let version: i64 = row
                .try_get("version")
                .map_err(|err| repository_storage_error::<DB>("decode stream version row", err))?;
            versions.insert(
                StreamIdentity::new(&aggregate_type, &aggregate_id)?.storage_key(),
                repository_u64_from_i64(DB::BACKEND, version, "sequence")?,
            );
        }
    }

    Ok(versions)
}
