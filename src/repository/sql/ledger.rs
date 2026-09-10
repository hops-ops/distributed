//! Shared fenced command-ledger operations. The caller owns the transaction.
//! Both SQLx and cell SQL use these statements and the same record state machine.
use super::{signed, unsigned, SqlBind, SqlExecutor, SqlPart, SqlRow, Statement};
use crate::command_ledger::{
    AttemptFence, AttemptToken, CanonicalInputHash, CausationId, CommandCompletion,
    CommandContractFingerprint, CommandId, CommandLedgerError, CommandLedgerKey,
    CommandLedgerRecord, CommandLedgerState, CommandLookup, CommandLookupScope, CommandReservation,
    ExternalCommandCompletion, ExternalDispatchBinding, PrincipalPartitionId, ReservationDecision,
    ReservationOutcome,
};
use std::time::SystemTime;

fn corrupt(error: CommandLedgerError) -> CommandLedgerError {
    CommandLedgerError::Corrupt(error.to_string())
}

pub(crate) fn key_from_row(row: &impl SqlRow) -> Result<CommandLedgerKey, CommandLedgerError> {
    CommandLedgerKey::new(
        row.text("service_id")?,
        PrincipalPartitionId::new(row.text("principal_partition")?).map_err(corrupt)?,
        CommandId::parse(row.text("command_id")?).map_err(corrupt)?,
    )
    .map_err(corrupt)
}

fn record_from_row(
    row: &impl SqlRow,
    key: CommandLedgerKey,
) -> Result<CommandLedgerRecord, CommandLedgerError> {
    let record = CommandLedgerRecord {
        key,
        command_name: row.text("command_name")?,
        external_binding: row
            .optional_text("external_binding")?
            .as_deref()
            .map(ExternalDispatchBinding::from_storage)
            .transpose()?,
        contract_fingerprint: CommandContractFingerprint::try_from_slice(
            &row.bytes("command_contract_hash")?,
        )
        .map_err(corrupt)?,
        input_hash: CanonicalInputHash::try_from_slice(&row.bytes("input_hash")?)
            .map_err(corrupt)?,
        state: CommandLedgerState::parse(&row.text("state")?)?,
        causation_id: CausationId::parse_stored(row.text("causation_id")?)?,
        attempt_token: row
            .optional_text("attempt_token")?
            .map(AttemptToken::parse_stored)
            .transpose()?,
        attempt_number: unsigned(
            row.integer("attempt_number")?,
            "command ledger attempt number",
        )?,
        lease_expires_at: row.optional_timestamp("lease_expires_at")?,
        outcome_json: row.optional_text("outcome")?,
        created_at: row.timestamp("created_at")?,
        updated_at: row.timestamp("updated_at")?,
        completed_at: row.optional_timestamp("completed_at")?,
        retention_expires_at: row.timestamp("retention_expires_at")?,
        compacted_at: row.optional_timestamp("compacted_at")?,
    };
    record.validate_stored_shape()?;
    Ok(record)
}

pub(crate) async fn now(executor: &mut impl SqlExecutor) -> Result<SystemTime, CommandLedgerError> {
    let mut statement = Statement::new("SELECT ");
    statement.part(SqlPart::LedgerNowEpoch);
    statement.push(" AS ledger_now");
    let rows = executor.query(statement).await?;
    let row = rows
        .first()
        .ok_or_else(|| CommandLedgerError::Corrupt("database clock returned no row".into()))?;
    Ok(row.timestamp("ledger_now")?)
}

fn key_filter<'a>(mut statement: Statement<'a>, key: &CommandLedgerKey) -> Statement<'a> {
    statement.push_bind(key.service_id());
    statement.push(" AND principal_partition = ");
    statement.push_bind(key.principal_partition());
    statement.push(" AND command_id = ");
    statement.push_bind(key.command_id());
    statement
}

async fn select<E: SqlExecutor>(
    executor: &mut E,
    key: &CommandLedgerKey,
    expected_command_name: Option<&str>,
) -> Result<Option<CommandLedgerRecord>, CommandLedgerError> {
    let mut statement = Statement::new("SELECT ");
    statement.push(E::COMMAND_LEDGER_SELECT);
    statement.push(" FROM command_ledger WHERE service_id = ");
    statement = key_filter(statement, key);
    if let Some(name) = expected_command_name {
        statement.push(" AND command_name = ");
        statement.push_bind(name);
    }
    statement.push(E::COMMAND_LEDGER_LOCK_SUFFIX);
    let rows = executor.query(statement).await?;
    rows.first()
        .map(|row| record_from_row(row, key.clone()))
        .transpose()
}

pub(crate) async fn preflight<E: SqlExecutor>(
    executor: &mut E,
    completion: &CommandCompletion,
) -> Result<(), CommandLedgerError> {
    let fence = completion.attempt_fence();

    // SQLite needs a write statement to reserve the database writer before
    // the read; PostgreSQL's subsequent SELECT also carries FOR UPDATE. This
    // establishes one portable lock order before any domain participant is
    // mutated.
    let mut lock =
        Statement::new("UPDATE command_ledger SET updated_at = updated_at WHERE service_id = ");
    lock.push_bind(fence.key().service_id());
    lock.push(" AND principal_partition = ");
    lock.push_bind(fence.key().principal_partition());
    lock.push(" AND command_id = ");
    lock.push_bind(fence.key().command_id());
    let result = executor.execute(lock).await?;
    if result != 1 {
        return Err(CommandLedgerError::AttemptFenced {
            command_id: fence.key().command_id().to_string(),
        });
    }

    let record = select(executor, fence.key(), None).await?.ok_or_else(|| {
        CommandLedgerError::AttemptFenced {
            command_id: fence.key().command_id().to_string(),
        }
    })?;
    if completion.attempt().external_binding().is_some() {
        return Err(CommandLedgerError::Invalid(
            "local causal completion cannot complete an externally bound reservation".into(),
        ));
    }
    let now = now(executor).await?;
    record.validate_live_attempt(&fence, now)
}

pub(crate) async fn insert_reservation<E: SqlExecutor>(
    executor: &mut E,
    reservation: &CommandReservation,
) -> Result<bool, CommandLedgerError> {
    let mut builder = Statement::new(
        "INSERT INTO command_ledger (service_id, principal_partition, command_id, \
         command_name, command_contract_hash, input_hash, state, causation_id, attempt_token, \
         attempt_number, external_binding, lease_expires_at, outcome, created_at, updated_at, completed_at, \
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
    let external_binding = reservation
        .external_binding()
        .map(ExternalDispatchBinding::to_storage)
        .transpose()?;
    match external_binding.as_deref() {
        Some(binding) => builder.push_bind(binding),
        None => builder.push("NULL"),
    }
    builder.push(", ");
    builder.part(SqlPart::LedgerDeadline(reservation.lease()));
    builder.push(", NULL, ");
    builder.part(SqlPart::LedgerNow);
    builder.push(", ");
    builder.part(SqlPart::LedgerNow);
    builder.push(", NULL, ");
    builder.part(SqlPart::LedgerDeadline(reservation.retention()));
    builder.push(", NULL");
    builder.push(") ON CONFLICT (service_id, principal_partition, command_id) DO NOTHING");
    let result = executor.execute(builder).await?;
    Ok(result == 1)
}

pub(crate) async fn expire<E: SqlExecutor>(
    executor: &mut E,
    key: &CommandLedgerKey,
    require_retention_due: bool,
) -> Result<u64, CommandLedgerError> {
    let mut builder = Statement::new(
        "UPDATE command_ledger SET state = 'expired', attempt_token = NULL, \
         lease_expires_at = NULL, outcome = NULL, updated_at = ",
    );
    builder.part(SqlPart::LedgerNow);
    builder.push(", compacted_at = ");
    builder.part(SqlPart::LedgerNow);
    builder.push(" WHERE service_id = ");
    builder.push_bind(key.service_id());
    builder.push(" AND principal_partition = ");
    builder.push_bind(key.principal_partition());
    builder.push(" AND command_id = ");
    builder.push_bind(key.command_id());
    builder.push(" AND state <> 'expired'");
    if require_retention_due {
        builder.push(" AND retention_expires_at <= ");
        builder.part(SqlPart::LedgerNow);
    }
    let result = executor.execute(builder).await?;
    Ok(result)
}

pub(crate) async fn reclaim<E: SqlExecutor>(
    executor: &mut E,
    record: &mut CommandLedgerRecord,
    reservation: &CommandReservation,
    now: SystemTime,
) -> Result<(), CommandLedgerError> {
    record.reclaim(reservation, now)?;
    let attempt_number = signed(record.attempt_number, "command ledger attempt number")?;
    let mut builder =
        Statement::new("UPDATE command_ledger SET state = 'in_progress', attempt_token = ");
    builder.push_bind(reservation.candidate_attempt().as_str());
    builder.push(", attempt_number = ");
    builder.push_bind(attempt_number);
    builder.push(", lease_expires_at = ");
    builder.part(SqlPart::LedgerDeadline(reservation.lease()));
    builder.push(", outcome = NULL, updated_at = ");
    builder.part(SqlPart::LedgerNow);
    builder.push(", completed_at = NULL, retention_expires_at = ");
    builder.part(SqlPart::LedgerDeadline(reservation.retention()));
    builder.push(", compacted_at = NULL WHERE service_id = ");
    builder.push_bind(record.key.service_id());
    builder.push(" AND principal_partition = ");
    builder.push_bind(record.key.principal_partition());
    builder.push(" AND command_id = ");
    builder.push_bind(record.key.command_id());
    let result = executor.execute(builder).await?;
    if result != 1 {
        return Err(CommandLedgerError::AttemptFenced {
            command_id: record.key.command_id().to_string(),
        });
    }
    Ok(())
}

pub(crate) async fn complete<E: SqlExecutor>(
    executor: &mut E,
    completion: &CommandCompletion,
) -> Result<(), CommandLedgerError> {
    let fence = completion.attempt_fence();
    let attempt_number = signed(fence.attempt_number(), "command ledger attempt number")?;
    let terminal_state = CommandLedgerState::from(completion.state()).as_str();
    let retention_expires_at = completion.retention_expires_at();
    let mut builder = Statement::new("UPDATE command_ledger SET state = ");
    builder.push_bind(terminal_state);
    builder.push(", attempt_token = NULL, lease_expires_at = NULL, outcome = ");
    builder.part(SqlPart::LedgerJson(completion.replay_json().into()));
    builder.push(", updated_at = ");
    builder.part(SqlPart::LedgerNow);
    builder.push(", completed_at = ");
    builder.part(SqlPart::LedgerNow);
    builder.push(", retention_expires_at = ");
    match retention_expires_at.as_ref() {
        Some(deadline) => builder.push_bind(SqlBind::Timestamp(*deadline)),
        None => builder.part(SqlPart::LedgerDeadline(completion.retention())),
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
    builder.push(" AND external_binding IS NULL");
    builder.push(" AND state = 'in_progress' AND causation_id = ");
    builder.push_bind(fence.causation_id().as_str());
    builder.push(" AND attempt_token = ");
    builder.push_bind(fence.attempt_token().as_str());
    builder.push(" AND attempt_number = ");
    builder.push_bind(attempt_number);
    builder.push(" AND lease_expires_at > ");
    builder.part(SqlPart::LedgerNow);
    if let Some(deadline) = retention_expires_at.as_ref() {
        builder.push(" AND ");
        builder.part(SqlPart::LedgerDeadlineIsLive(*deadline));
    }
    let result = executor.execute(builder).await?;
    if result != 1 {
        return Err(CommandLedgerError::AttemptFenced {
            command_id: fence.key().command_id().to_string(),
        });
    }
    Ok(())
}

pub(crate) async fn complete_external<E: SqlExecutor>(
    executor: &mut E,
    completion: &ExternalCommandCompletion,
) -> Result<(), CommandLedgerError> {
    let fence = completion.attempt_fence();
    let attempt_number = signed(fence.attempt_number(), "command ledger attempt number")?;
    let terminal_state = CommandLedgerState::from(completion.state()).as_str();
    let retention_expires_at = completion.retention_expires_at();
    let binding = completion.binding().to_storage()?;
    let mut builder = Statement::new("UPDATE command_ledger SET state = ");
    builder.push_bind(terminal_state);
    builder.push(", attempt_token = NULL, lease_expires_at = NULL, outcome = ");
    builder.part(SqlPart::LedgerJson(completion.replay_json().into()));
    builder.push(", updated_at = ");
    builder.part(SqlPart::LedgerNow);
    builder.push(", completed_at = ");
    builder.part(SqlPart::LedgerNow);
    builder.push(", retention_expires_at = ");
    match retention_expires_at.as_ref() {
        Some(deadline) => builder.push_bind(SqlBind::Timestamp(*deadline)),
        None => builder.part(SqlPart::LedgerDeadline(completion.retention())),
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
    builder.push(" AND external_binding = ");
    builder.push_bind(binding.as_str());
    builder.push(" AND state = 'in_progress' AND causation_id = ");
    builder.push_bind(fence.causation_id().as_str());
    builder.push(" AND attempt_token = ");
    builder.push_bind(fence.attempt_token().as_str());
    builder.push(" AND attempt_number = ");
    builder.push_bind(attempt_number);
    builder.push(" AND lease_expires_at > ");
    builder.part(SqlPart::LedgerNow);
    if let Some(deadline) = retention_expires_at.as_ref() {
        builder.push(" AND ");
        builder.part(SqlPart::LedgerDeadlineIsLive(*deadline));
    }
    if executor.execute(builder).await? != 1 {
        return Err(CommandLedgerError::AttemptFenced {
            command_id: fence.key().command_id().to_string(),
        });
    }
    Ok(())
}

pub(crate) async fn reserve(
    executor: &mut impl SqlExecutor,
    reservation: &CommandReservation,
) -> Result<ReservationOutcome, CommandLedgerError> {
    if insert_reservation(executor, reservation).await? {
        return Ok(ReservationOutcome::Acquired(
            reservation.acquired_candidate_attempt(),
        ));
    }
    let mut record = select(executor, reservation.key(), None)
        .await?
        .ok_or_else(|| {
            CommandLedgerError::Corrupt("conflicting command disappeared during reservation".into())
        })?;
    let now = now(executor).await?;
    match record.classify_reservation(reservation, now)? {
        ReservationDecision::Expire => {
            expire(executor, reservation.key(), false).await?;
            Ok(ReservationOutcome::Expired)
        }
        ReservationDecision::Reclaim => {
            reclaim(executor, &mut record, reservation, now).await?;
            Ok(ReservationOutcome::Acquired(record.acquired_attempt()?))
        }
        other => record.reservation_outcome(other),
    }
}

pub(crate) async fn lookup(
    executor: &mut impl SqlExecutor,
    key: &CommandLedgerKey,
    scope: CommandLookupScope<'_>,
) -> Result<CommandLookup, CommandLedgerError> {
    let expected_name = match scope {
        CommandLookupScope::CommandName(name)
        | CommandLookupScope::CommandContract {
            command_name: name, ..
        } => Some(name),
        CommandLookupScope::Attempt(_) => None,
    };
    // Reserve SQLite's writer before reading; PostgreSQL also locks the selected row.
    let mut lock = key_filter(
        Statement::new("UPDATE command_ledger SET updated_at = updated_at WHERE service_id = "),
        key,
    );
    if let Some(name) = expected_name {
        lock.push(" AND command_name = ");
        lock.push_bind(name);
    }
    executor.execute(lock).await?;
    let Some(mut record) = select(executor, key, expected_name).await? else {
        return Ok(CommandLookup::Unknown);
    };
    if !record.matches_lookup_scope(scope) {
        return Ok(CommandLookup::Unknown);
    }
    let now = now(executor).await?;
    if record.state != CommandLedgerState::Expired && record.retention_expires_at <= now {
        expire(executor, key, true).await?;
        record.expire(now);
    }
    record.lookup()
}

pub(crate) async fn mark_retryable(
    executor: &mut impl SqlExecutor,
    attempt: &AttemptFence,
) -> Result<(), CommandLedgerError> {
    let mut builder = Statement::new("UPDATE command_ledger SET state = 'retryable_unknown', attempt_token = NULL, lease_expires_at = NULL, updated_at = ");
    builder.part(SqlPart::LedgerNow);
    builder.push(" WHERE service_id = ");
    builder = key_filter(builder, attempt.key());
    builder.push(" AND command_contract_hash = ");
    builder.push_bind(attempt.contract_fingerprint_bytes().as_slice());
    builder.push(" AND input_hash = ");
    builder.push_bind(attempt.input_hash_bytes().as_slice());
    builder.push(" AND state = 'in_progress' AND causation_id = ");
    builder.push_bind(attempt.causation_id().as_str());
    builder.push(" AND attempt_token = ");
    builder.push_bind(attempt.attempt_token().as_str());
    builder.push(" AND attempt_number = ");
    builder.push_bind(signed(
        attempt.attempt_number(),
        "command ledger attempt number",
    )?);
    if executor.execute(builder).await? != 1 {
        return Err(CommandLedgerError::AttemptFenced {
            command_id: attempt.key().command_id().to_string(),
        });
    }
    Ok(())
}

pub(crate) async fn compact<E: SqlExecutor>(
    executor: &mut E,
    limit: usize,
) -> Result<u64, CommandLedgerError> {
    if limit == 0 {
        return Ok(0);
    }
    let limit = i64::try_from(limit)
        .map_err(|_| CommandLedgerError::Invalid("command compaction limit exceeds i64".into()))?;
    executor
        .execute(Statement::new(
            "UPDATE command_ledger SET updated_at = updated_at WHERE 1 = 0",
        ))
        .await?;
    let mut select = Statement::new("SELECT service_id, principal_partition, command_id FROM command_ledger WHERE state <> 'expired' AND retention_expires_at <= ");
    select.part(SqlPart::LedgerNow);
    select
        .push(" ORDER BY retention_expires_at, service_id, principal_partition, command_id LIMIT ");
    select.push_bind(limit);
    select.push(E::COMMAND_LEDGER_COMPACTION_LOCK_SUFFIX);
    let rows = executor.query(select).await?;
    let mut count = 0;
    for row in rows {
        count += expire(executor, &key_from_row(&row)?, true).await?;
    }
    Ok(count)
}
