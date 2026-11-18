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
    for<'q> String: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let fence = completion.attempt_fence();

    // SQLite needs a write statement to reserve the database writer before
    // the read; PostgreSQL's subsequent SELECT also carries FOR UPDATE. This
    // establishes one portable lock order before any domain participant is
    // mutated.
    let mut lock = QueryBuilder::<DB>::new(
        "UPDATE command_ledger SET updated_at = updated_at WHERE service_id = ",
    );
    lock.push_bind(fence.key().service_id());
    lock.push(" AND principal_partition = ");
    lock.push_bind(fence.key().principal_partition());
    lock.push(" AND command_id = ");
    lock.push_bind(fence.key().command_id());
    let result =
        lock.build().execute(&mut **tx).await.map_err(|error| {
            repository_storage_error::<DB>("lock command attempt preflight", error)
        })?;
    if DB::rows_affected(&result) != 1 {
        return Err(CommandLedgerError::AttemptFenced {
            command_id: fence.key().command_id().to_string(),
        });
    }

    let record = select_command_ledger_record_in_tx(tx, fence.key(), None)
        .await?
        .ok_or_else(|| CommandLedgerError::AttemptFenced {
            command_id: fence.key().command_id().to_string(),
        })?;
    let now = command_ledger_now_in_tx(tx).await?;
    record.validate_live_attempt(&fence, now)
}

async fn commit_sqlx_batch<'a, DB>(
    repository: &'a SqlxRepository<DB>,
    batch: &mut CommitBatch<'a>,
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
    for plan in &batch.read_model_plans {
        for mutation in &plan.mutations {
            changed_tables.insert(mutation.table_name().to_string());
        }
        apply_read_model_write_plan_in_tx(&mut tx, plan.clone())
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

    for write in &batch.snapshots {
        match write {
            SnapshotWrite::Save { identity, record } => {
                save_snapshot_in_tx(&mut tx, identity, record.clone()).await?;
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
    for stream in &mut batch.streams {
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
        mut batch: CommitBatch<'a>,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
        async move {
            match commit_sqlx_batch(self, &mut batch, None, None).await {
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
        mut batch: CausalCommitBatch<'a>,
    ) -> impl Future<Output = Result<(), CommandLedgerError>> + Send + 'a {
        async move {
            commit_sqlx_batch(
                self,
                &mut batch.domain,
                Some(batch.completion),
                batch.direct_projection,
            )
            .await
        }
    }
}

fn corrupt_ledger_value(error: CommandLedgerError) -> CommandLedgerError {
    CommandLedgerError::Corrupt(error.to_string())
}

#[allow(dead_code)]
fn command_ledger_key_from_row<DB>(row: &DB::Row) -> Result<CommandLedgerKey, CommandLedgerError>
where
    DB: SqlxRepoBackend,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let service_id: String = row.try_get("service_id").map_err(|error| {
        repository_storage_error::<DB>("decode command ledger service ID", error)
    })?;
    let principal: String = row.try_get("principal_partition").map_err(|error| {
        repository_storage_error::<DB>("decode command ledger principal partition", error)
    })?;
    let command_id: String = row.try_get("command_id").map_err(|error| {
        repository_storage_error::<DB>("decode command ledger command ID", error)
    })?;
    CommandLedgerKey::new(
        service_id,
        PrincipalPartitionId::new(principal).map_err(corrupt_ledger_value)?,
        CommandId::parse(command_id).map_err(corrupt_ledger_value)?,
    )
    .map_err(corrupt_ledger_value)
}

fn command_ledger_record_from_row<DB>(
    row: &DB::Row,
    key: CommandLedgerKey,
) -> Result<CommandLedgerRecord, CommandLedgerError>
where
    DB: SqlxRepoBackend,
    for<'q> i64: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let decode = |operation: &'static str, error| repository_storage_error::<DB>(operation, error);
    let command_name: String = row
        .try_get("command_name")
        .map_err(|error| decode("decode command ledger name", error))?;
    let contract: Vec<u8> = row
        .try_get("command_contract_hash")
        .map_err(|error| decode("decode command contract hash", error))?;
    let input: Vec<u8> = row
        .try_get("input_hash")
        .map_err(|error| decode("decode canonical command input hash", error))?;
    let state: String = row
        .try_get("state")
        .map_err(|error| decode("decode command ledger state", error))?;
    let causation_id: String = row
        .try_get("causation_id")
        .map_err(|error| decode("decode command ledger causation ID", error))?;
    let attempt_token: Option<String> = row
        .try_get("attempt_token")
        .map_err(|error| decode("decode command ledger attempt token", error))?;
    let attempt_number: i64 = row
        .try_get("attempt_number")
        .map_err(|error| decode("decode command ledger attempt number", error))?;
    let outcome_json: Option<String> = row
        .try_get("outcome")
        .map_err(|error| decode("decode command ledger outcome", error))?;

    let record = CommandLedgerRecord {
        key,
        command_name,
        contract_fingerprint: CommandContractFingerprint::try_from_slice(&contract)
            .map_err(corrupt_ledger_value)?,
        input_hash: CanonicalInputHash::try_from_slice(&input).map_err(corrupt_ledger_value)?,
        state: CommandLedgerState::parse(&state)?,
        causation_id: CausationId::parse_stored(causation_id)?,
        attempt_token: attempt_token.map(AttemptToken::parse_stored).transpose()?,
        attempt_number: repository_u64_from_i64(
            DB::BACKEND,
            attempt_number,
            "command ledger attempt number",
        )?,
        lease_expires_at: DB::decode_optional_timestamp(row, "lease_expires_at")?,
        outcome_json,
        created_at: DB::decode_timestamp(row, "created_at")?,
        updated_at: DB::decode_timestamp(row, "updated_at")?,
        completed_at: DB::decode_optional_timestamp(row, "completed_at")?,
        retention_expires_at: DB::decode_timestamp(row, "retention_expires_at")?,
        compacted_at: DB::decode_optional_timestamp(row, "compacted_at")?,
    };
    record.validate_stored_shape()?;
    Ok(record)
}

async fn command_ledger_now_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
) -> Result<SystemTime, CommandLedgerError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let mut builder = QueryBuilder::<DB>::new("SELECT ");
    DB::push_command_ledger_now_epoch(&mut builder);
    builder.push(" AS ledger_now");
    let row = builder
        .build()
        .fetch_one(&mut **tx)
        .await
        .map_err(|error| repository_storage_error::<DB>("read command ledger clock", error))?;
    Ok(DB::decode_timestamp(&row, "ledger_now")?)
}

async fn select_command_ledger_record_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    key: &CommandLedgerKey,
    expected_command_name: Option<&str>,
) -> Result<Option<CommandLedgerRecord>, CommandLedgerError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let mut builder = QueryBuilder::<DB>::new("SELECT ");
    builder.push(DB::COMMAND_LEDGER_SELECT);
    builder.push(" FROM command_ledger WHERE service_id = ");
    builder.push_bind(key.service_id());
    builder.push(" AND principal_partition = ");
    builder.push_bind(key.principal_partition());
    builder.push(" AND command_id = ");
    builder.push_bind(key.command_id());
    if let Some(expected_command_name) = expected_command_name {
        builder.push(" AND command_name = ");
        builder.push_bind(expected_command_name);
    }
    builder.push(DB::COMMAND_LEDGER_LOCK_SUFFIX);
    let row = builder
        .build()
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| repository_storage_error::<DB>("select command ledger row", error))?;
    row.map(|row| command_ledger_record_from_row::<DB>(&row, key.clone()))
        .transpose()
}

async fn insert_command_reservation_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    reservation: &CommandReservation,
) -> Result<bool, CommandLedgerError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB>,
    for<'q> f64: Encode<'q, DB> + Type<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
{
    let mut builder = QueryBuilder::<DB>::new(
        "INSERT INTO command_ledger (service_id, principal_partition, command_id, \
         command_name, command_contract_hash, input_hash, state, causation_id, attempt_token, \
         attempt_number, lease_expires_at, outcome, created_at, updated_at, completed_at, \
         retention_expires_at, compacted_at) VALUES (",
    );
    builder.push_bind(reservation.key().service_id());
    builder.push(", ");
    builder.push_bind(reservation.key().principal_partition());
    builder.push(", ");
    builder.push_bind(reservation.key().command_id());
    builder.push(", ");
    builder.push_bind(reservation.command_name());
    builder.push(", ");
    builder.push_bind(reservation.contract_fingerprint_bytes().as_slice());
    builder.push(", ");
    builder.push_bind(reservation.input_hash_bytes().as_slice());
    builder.push(", ");
    builder.push_bind(CommandLedgerState::InProgress.as_str());
    builder.push(", ");
    builder.push_bind(reservation.candidate_causation().as_str());
    builder.push(", ");
    builder.push_bind(reservation.candidate_attempt().as_str());
    builder.push(", ");
    builder.push_bind(1_i64);
    builder.push(", ");
    DB::push_command_ledger_deadline(&mut builder, reservation.lease());
    builder.push(", NULL, ");
    DB::push_command_ledger_now(&mut builder);
    builder.push(", ");
    DB::push_command_ledger_now(&mut builder);
    builder.push(", NULL, ");
    DB::push_command_ledger_deadline(&mut builder, reservation.retention());
    builder.push(", NULL");
    builder.push(") ON CONFLICT (service_id, principal_partition, command_id) DO NOTHING");
    let result = builder
        .build()
        .execute(&mut **tx)
        .await
        .map_err(|error| repository_storage_error::<DB>("insert command reservation", error))?;
    Ok(DB::rows_affected(&result) == 1)
}

async fn expire_command_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    key: &CommandLedgerKey,
    require_retention_due: bool,
) -> Result<u64, CommandLedgerError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> String: Encode<'q, DB> + Type<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
{
    let mut builder = QueryBuilder::<DB>::new(
        "UPDATE command_ledger SET state = 'expired', attempt_token = NULL, \
         lease_expires_at = NULL, outcome = NULL, updated_at = ",
    );
    DB::push_command_ledger_now(&mut builder);
    builder.push(", compacted_at = ");
    DB::push_command_ledger_now(&mut builder);
    builder.push(" WHERE service_id = ");
    builder.push_bind(key.service_id());
    builder.push(" AND principal_partition = ");
    builder.push_bind(key.principal_partition());
    builder.push(" AND command_id = ");
    builder.push_bind(key.command_id());
    builder.push(" AND state <> 'expired'");
    if require_retention_due {
        builder.push(" AND retention_expires_at <= ");
        DB::push_command_ledger_now(&mut builder);
    }
    let result = builder
        .build()
        .execute(&mut **tx)
        .await
        .map_err(|error| repository_storage_error::<DB>("expire command ledger row", error))?;
    Ok(DB::rows_affected(&result))
}

async fn reclaim_command_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    record: &mut CommandLedgerRecord,
    reservation: &CommandReservation,
    now: SystemTime,
) -> Result<(), CommandLedgerError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB>,
    for<'q> f64: Encode<'q, DB> + Type<DB>,
    for<'q> String: Encode<'q, DB> + Type<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
{
    record.reclaim(reservation, now)?;
    let attempt_number = repository_i64_from_u64(
        DB::BACKEND,
        record.attempt_number,
        "command ledger attempt number",
        DB::INTEGER_STORAGE,
    )?;
    let mut builder = QueryBuilder::<DB>::new(
        "UPDATE command_ledger SET state = 'in_progress', attempt_token = ",
    );
    builder.push_bind(reservation.candidate_attempt().as_str());
    builder.push(", attempt_number = ");
    builder.push_bind(attempt_number);
    builder.push(", lease_expires_at = ");
    DB::push_command_ledger_deadline(&mut builder, reservation.lease());
    builder.push(", outcome = NULL, updated_at = ");
    DB::push_command_ledger_now(&mut builder);
    builder.push(", completed_at = NULL, retention_expires_at = ");
    DB::push_command_ledger_deadline(&mut builder, reservation.retention());
    builder.push(", compacted_at = NULL WHERE service_id = ");
    builder.push_bind(record.key.service_id());
    builder.push(" AND principal_partition = ");
    builder.push_bind(record.key.principal_partition());
    builder.push(" AND command_id = ");
    builder.push_bind(record.key.command_id());
    let result = builder
        .build()
        .execute(&mut **tx)
        .await
        .map_err(|error| repository_storage_error::<DB>("reclaim command attempt", error))?;
    if DB::rows_affected(&result) != 1 {
        return Err(CommandLedgerError::AttemptFenced {
            command_id: record.key.command_id().to_string(),
        });
    }
    Ok(())
}

async fn complete_command_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    completion: &CommandCompletion,
) -> Result<(), CommandLedgerError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB>,
    for<'q> f64: Encode<'q, DB> + Type<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
{
    let fence = completion.attempt_fence();
    let attempt_number = repository_i64_from_u64(
        DB::BACKEND,
        fence.attempt_number(),
        "command ledger attempt number",
        DB::INTEGER_STORAGE,
    )?;
    let terminal_state = CommandLedgerState::from(completion.state()).as_str();
    let retention_expires_at = completion
        .retention_expires_at()
        .map(DB::timestamp_value)
        .transpose()
        .map_err(CommandLedgerError::Storage)?;
    let mut builder = QueryBuilder::<DB>::new("UPDATE command_ledger SET state = ");
    builder.push_bind(terminal_state);
    builder.push(", attempt_token = NULL, lease_expires_at = NULL, outcome = ");
    DB::push_command_ledger_json(&mut builder, completion.replay_json());
    builder.push(", updated_at = ");
    DB::push_command_ledger_now(&mut builder);
    builder.push(", completed_at = ");
    DB::push_command_ledger_now(&mut builder);
    builder.push(", retention_expires_at = ");
    match retention_expires_at.as_ref() {
        Some(deadline) => DB::push_timestamp_assign(&mut builder, deadline),
        None => DB::push_command_ledger_deadline(&mut builder, completion.retention()),
    }
    builder.push(", compacted_at = NULL WHERE service_id = ");
    builder.push_bind(fence.key().service_id());
    builder.push(" AND principal_partition = ");
    builder.push_bind(fence.key().principal_partition());
    builder.push(" AND command_id = ");
    builder.push_bind(fence.key().command_id());
    builder.push(" AND command_contract_hash = ");
    builder.push_bind(fence.contract_fingerprint_bytes().as_slice());
    builder.push(" AND input_hash = ");
    builder.push_bind(fence.input_hash_bytes().as_slice());
    builder.push(" AND state = 'in_progress' AND causation_id = ");
    builder.push_bind(fence.causation_id().as_str());
    builder.push(" AND attempt_token = ");
    builder.push_bind(fence.attempt_token().as_str());
    builder.push(" AND attempt_number = ");
    builder.push_bind(attempt_number);
    builder.push(" AND lease_expires_at > ");
    DB::push_command_ledger_now(&mut builder);
    if let Some(deadline) = retention_expires_at.as_ref() {
        builder.push(" AND ");
        DB::push_command_ledger_deadline_is_live(&mut builder, deadline);
    }
    let result =
        builder.build().execute(&mut **tx).await.map_err(|error| {
            repository_storage_error::<DB>("complete command ledger row", error)
        })?;
    if DB::rows_affected(&result) != 1 {
        return Err(CommandLedgerError::AttemptFenced {
            command_id: fence.key().command_id().to_string(),
        });
    }
    Ok(())
}

impl<DB> CommandLedgerStore for SqlxRepository<DB>
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
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    fn reserve_command(
        &self,
        reservation: CommandReservation,
    ) -> impl Future<Output = Result<ReservationOutcome, CommandLedgerError>> + Send + '_ {
        async move {
            let mut tx = self.pool.begin().await.map_err(|error| {
                repository_storage_error::<DB>("begin command reservation", error)
            })?;
            if insert_command_reservation_in_tx(&mut tx, &reservation).await? {
                tx.commit().await.map_err(|error| {
                    repository_storage_error::<DB>("commit command reservation", error)
                })?;
                return Ok(ReservationOutcome::Acquired(
                    reservation.acquired_candidate_attempt(),
                ));
            }

            let mut record = select_command_ledger_record_in_tx(&mut tx, reservation.key(), None)
                .await?
                .ok_or_else(|| {
                    CommandLedgerError::Corrupt(format!(
                        "conflicting command `{}` disappeared during reservation",
                        reservation.key().command_id()
                    ))
                })?;
            let now = command_ledger_now_in_tx(&mut tx).await?;
            let decision = record.classify_reservation(&reservation, now)?;
            let outcome = match decision {
                ReservationDecision::Expire => {
                    expire_command_in_tx(&mut tx, reservation.key(), false).await?;
                    ReservationOutcome::Expired
                }
                ReservationDecision::Reclaim => {
                    reclaim_command_in_tx(&mut tx, &mut record, &reservation, now).await?;
                    ReservationOutcome::Acquired(record.acquired_attempt()?)
                }
                other => record.reservation_outcome(other)?,
            };
            tx.commit().await.map_err(|error| {
                repository_storage_error::<DB>("commit command reservation decision", error)
            })?;
            Ok(outcome)
        }
    }

    fn lookup_command<'a>(
        &'a self,
        key: &'a CommandLedgerKey,
        scope: CommandLookupScope<'a>,
    ) -> impl Future<Output = Result<CommandLookup, CommandLedgerError>> + Send + 'a {
        async move {
            let mut tx = self.pool.begin().await.map_err(|error| {
                repository_storage_error::<DB>("begin command ledger lookup", error)
            })?;

            // Establish SQLite's single-writer reservation before selecting;
            // PostgreSQL additionally takes the row lock through its suffix.
            let mut lock = QueryBuilder::<DB>::new(
                "UPDATE command_ledger SET updated_at = updated_at WHERE service_id = ",
            );
            lock.push_bind(key.service_id());
            lock.push(" AND principal_partition = ");
            lock.push_bind(key.principal_partition());
            lock.push(" AND command_id = ");
            lock.push_bind(key.command_id());
            match scope {
                CommandLookupScope::CommandName(expected_command_name)
                | CommandLookupScope::CommandContract {
                    command_name: expected_command_name,
                    ..
                } => {
                    lock.push(" AND command_name = ");
                    lock.push_bind(expected_command_name);
                }
                CommandLookupScope::Attempt(_) => {}
            }
            lock.build().execute(&mut *tx).await.map_err(|error| {
                repository_storage_error::<DB>("lock command ledger lookup", error)
            })?;

            let expected_command_name = match scope {
                CommandLookupScope::CommandName(expected) => Some(expected),
                CommandLookupScope::CommandContract {
                    command_name: expected,
                    ..
                } => Some(expected),
                CommandLookupScope::Attempt(_) => None,
            };
            let Some(mut record) =
                select_command_ledger_record_in_tx(&mut tx, key, expected_command_name).await?
            else {
                tx.commit().await.map_err(|error| {
                    repository_storage_error::<DB>("commit empty command ledger lookup", error)
                })?;
                return Ok(CommandLookup::Unknown);
            };
            if !record.matches_lookup_scope(scope) {
                tx.commit().await.map_err(|error| {
                    repository_storage_error::<DB>("commit mismatched command ledger lookup", error)
                })?;
                return Ok(CommandLookup::Unknown);
            }
            let now = command_ledger_now_in_tx(&mut tx).await?;
            if record.state != CommandLedgerState::Expired && record.retention_expires_at <= now {
                expire_command_in_tx(&mut tx, key, true).await?;
                record.expire(now);
            }
            let lookup = record.lookup()?;
            tx.commit().await.map_err(|error| {
                repository_storage_error::<DB>("commit command ledger lookup", error)
            })?;
            Ok(lookup)
        }
    }

    fn mark_retryable_unknown(
        &self,
        attempt: AttemptFence,
    ) -> impl Future<Output = Result<(), CommandLedgerError>> + Send + '_ {
        async move {
            let attempt_number = repository_i64_from_u64(
                DB::BACKEND,
                attempt.attempt_number(),
                "command ledger attempt number",
                DB::INTEGER_STORAGE,
            )?;
            let mut builder = QueryBuilder::<DB>::new(
                "UPDATE command_ledger SET state = 'retryable_unknown', attempt_token = NULL, \
                 lease_expires_at = NULL, updated_at = ",
            );
            DB::push_command_ledger_now(&mut builder);
            builder.push(" WHERE service_id = ");
            builder.push_bind(attempt.key().service_id());
            builder.push(" AND principal_partition = ");
            builder.push_bind(attempt.key().principal_partition());
            builder.push(" AND command_id = ");
            builder.push_bind(attempt.key().command_id());
            builder.push(" AND command_contract_hash = ");
            builder.push_bind(attempt.contract_fingerprint_bytes().as_slice());
            builder.push(" AND input_hash = ");
            builder.push_bind(attempt.input_hash_bytes().as_slice());
            builder.push(" AND state = 'in_progress' AND causation_id = ");
            builder.push_bind(attempt.causation_id().as_str());
            builder.push(" AND attempt_token = ");
            builder.push_bind(attempt.attempt_token().as_str());
            builder.push(" AND attempt_number = ");
            builder.push_bind(attempt_number);
            let result = builder.build().execute(&self.pool).await.map_err(|error| {
                repository_storage_error::<DB>("mark command retryable unknown", error)
            })?;
            if DB::rows_affected(&result) != 1 {
                return Err(CommandLedgerError::AttemptFenced {
                    command_id: attempt.key().command_id().to_string(),
                });
            }
            Ok(())
        }
    }

    fn compact_expired_commands(
        &self,
        limit: usize,
    ) -> impl Future<Output = Result<u64, CommandLedgerError>> + Send + '_ {
        async move {
            if limit == 0 {
                return Ok(0);
            }
            let limit = i64::try_from(limit).map_err(|_| {
                CommandLedgerError::Invalid("command compaction limit exceeds i64".into())
            })?;
            let mut tx = self.pool.begin().await.map_err(|error| {
                repository_storage_error::<DB>("begin command ledger compaction", error)
            })?;

            // A no-op write obtains SQLite's transaction-wide writer lock.
            // PostgreSQL relies on the per-row SKIP LOCKED suffix below.
            QueryBuilder::<DB>::new(
                "UPDATE command_ledger SET updated_at = updated_at WHERE 1 = 0",
            )
            .build()
            .execute(&mut *tx)
            .await
            .map_err(|error| {
                repository_storage_error::<DB>("lock command ledger compaction", error)
            })?;

            let mut select = QueryBuilder::<DB>::new(
                "SELECT service_id, principal_partition, command_id FROM command_ledger \
                 WHERE state <> 'expired' AND retention_expires_at <= ",
            );
            DB::push_command_ledger_now(&mut select);
            select.push(" ORDER BY retention_expires_at, service_id, principal_partition, command_id LIMIT ");
            select.push_bind(limit);
            select.push(DB::COMMAND_LEDGER_COMPACTION_LOCK_SUFFIX);
            let rows = select.build().fetch_all(&mut *tx).await.map_err(|error| {
                repository_storage_error::<DB>("select command ledger compaction rows", error)
            })?;
            let mut compacted = 0;
            for row in rows {
                let key = command_ledger_key_from_row::<DB>(&row)?;
                compacted += expire_command_in_tx(&mut tx, &key, true).await?;
            }
            tx.commit().await.map_err(|error| {
                repository_storage_error::<DB>("commit command ledger compaction", error)
            })?;
            Ok(compacted)
        }
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

/// One `aggregate_events` row with pre-validated bind values, built before the
/// query so any conversion error surfaces before we touch the database. The
/// stream identity and expected version ride along for conflict recovery.
struct EventRow<'a, DB: SqlxRepoBackend> {
    identity: &'a StreamIdentity,
    expected_version: u64,
    sequence: i64,
    event_name: &'a str,
    event_version: i64,
    payload: &'a [u8],
    payload_codec: &'a str,
    payload_codec_version: i64,
    metadata: String,
    recorded_at: DB::TimestampValue,
}

/// Insert every event across all prepared appends with multi-row INSERTs,
/// chunked to respect the backend's bound-parameter limit (Postgres is
/// effectively unlimited, so its chunking collapses to one statement).
///
/// Conflict detection is unchanged from the per-row path: the `(aggregate_type,
/// aggregate_id, sequence)` primary key is the contiguity gate, and a unique
/// violation still surfaces as `ConcurrentWrite`. Recovery re-reads stream
/// versions in-tx or over the pool depending on
/// [`SqlxRepoBackend::CONFLICT_REREAD_IN_TX`].
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
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let mut rows = Vec::new();
    for append in prepared {
        for event in append.events {
            rows.push(EventRow::<DB> {
                identity: &append.identity,
                expected_version: append.expected_version,
                sequence: repository_i64_from_u64(
                    DB::BACKEND,
                    event.sequence,
                    "sequence",
                    DB::INTEGER_STORAGE,
                )?,
                event_name: &event.event_name,
                event_version: repository_i64_from_u64(
                    DB::BACKEND,
                    event.event_version,
                    "event_version",
                    DB::INTEGER_STORAGE,
                )?,
                payload: &event.payload,
                payload_codec: &event.payload_codec,
                payload_codec_version: i64::from(event.payload_codec_version),
                metadata: serialize_event_metadata(&event.metadata)?,
                recorded_at: DB::timestamp_value(event.timestamp)?,
            });
        }
    }

    for chunk in rows.chunks(DB::MAX_BIND_PARAMS / EVENT_BIND_COLUMNS) {
        let mut builder = QueryBuilder::<DB>::new(
            "INSERT INTO aggregate_events (\
             aggregate_type, aggregate_id, sequence, event_name, event_version, \
             payload, payload_codec, payload_codec_version, metadata, recorded_at) ",
        );
        builder.push_values(chunk, |mut row, event| {
            row.push_bind(event.identity.aggregate_type())
                .push_bind(event.identity.aggregate_id())
                .push_bind(event.sequence)
                .push_bind(event.event_name)
                .push_bind(event.event_version)
                .push_bind(event.payload)
                .push_bind(event.payload_codec)
                .push_bind(event.payload_codec_version);
            DB::push_metadata(&mut row, event.metadata.as_str());
            DB::push_timestamp(&mut row, &event.recorded_at);
        });

        let result = builder.build().execute(&mut **tx).await;
        match result {
            Ok(_) => {}
            Err(err) if DB::is_unique_violation(&err) => {
                return Err(if DB::CONFLICT_REREAD_IN_TX {
                    // The transaction survives the constraint error: re-read in
                    // the same tx, scoped to this chunk (earlier chunks were
                    // already inserted in this tx and would skew the versions
                    // of their streams).
                    let mut seen = std::collections::HashSet::new();
                    let candidates: Vec<_> = chunk
                        .iter()
                        .filter(|event| seen.insert(event.identity.storage_key()))
                        .map(|event| (event.identity, event.expected_version))
                        .collect();
                    concurrent_write_from_conflict(&mut **tx, &candidates).await
                } else {
                    // The failed statement aborted the transaction: re-read the
                    // conflicting streams' actual versions on a separate
                    // connection, across the whole batch.
                    let candidates: Vec<_> = prepared
                        .iter()
                        .map(|append| (&append.identity, append.expected_version))
                        .collect();
                    match pool.acquire().await {
                        Ok(mut conn) => {
                            concurrent_write_from_conflict(&mut conn, &candidates).await
                        }
                        Err(err) => repository_storage_error::<DB>(
                            "acquire conflict re-read connection",
                            err,
                        ),
                    }
                });
            }
            Err(err) => return Err(repository_storage_error::<DB>("insert events", err)),
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
