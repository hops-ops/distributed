//! Delivery rows shared by native SQL and cell-local SQL. Transactions belong
//! to the caller, so these inserts join the command's event and receipt writes.

use super::{json_error, signed, unsigned, SqlBind, SqlExecutor, SqlPart, SqlRow, Statement};
use crate::outbox::{OutboxMessage, OutboxMessageStatus};
use crate::outbox_worker::{ensure_active_claim, ClaimOutboxMessages, OutboxClaimRef};
use crate::repository::{sqlite_codec, RepositoryError};
use std::borrow::Cow;
use std::time::SystemTime;

pub(crate) struct OutboxInsert<'a> {
    pub statement: Statement<'a>,
    pub first_id: &'a str,
}

fn optional(value: Option<SqlBind<'_>>) -> SqlPart<'_> {
    value
        .map(SqlPart::Bind)
        .unwrap_or_else(|| SqlPart::Sql("NULL".into()))
}

/// Borrow payloads and emit at most nineteen bindings per row. Optional NULLs
/// are SQL literals, never stringified values or untyped driver bindings.
pub(crate) fn inserts(
    messages: &[OutboxMessage],
    max_bind_params: usize,
) -> Result<Vec<OutboxInsert<'_>>, RepositoryError> {
    const COLUMNS: usize = 19;
    if max_bind_params < COLUMNS {
        return Err(RepositoryError::Model(
            "SQL executor cannot bind one outbox row".into(),
        ));
    }
    let mut result = Vec::new();
    for chunk in messages.chunks(max_bind_params / COLUMNS) {
        let mut statement = Statement::new("INSERT INTO outbox_messages (message_id, event_type, payload, payload_codec, payload_codec_version, destination, metadata, status, created_at, next_available_at, claimed_by, claimed_until, attempts, last_error, source_aggregate_type, source_aggregate_id, source_sequence, correlation_id, causation_id) VALUES ");
        for (index, message) in chunk.iter().enumerate() {
            if index != 0 {
                statement.push(", ");
            }
            statement.push("(");
            let values = [
                Some(SqlBind::from(message.id())),
                Some(SqlBind::from(message.event_type.as_str())),
                Some(SqlBind::Bytes(Cow::Borrowed(&message.payload))),
                Some(SqlBind::from(message.payload_codec.as_str())),
                Some(SqlBind::Integer(i64::from(message.payload_codec_version))),
                message.destination.as_deref().map(SqlBind::from),
                Some(SqlBind::Metadata(
                    serde_json::to_string(&message.metadata).map_err(json_error)?,
                )),
                Some(SqlBind::from(message.status.as_str())),
                Some(SqlBind::Timestamp(message.created_at)),
                Some(SqlBind::Timestamp(message.created_at)),
                message.worker_id.as_deref().map(SqlBind::from),
                message.leased_until.map(SqlBind::Timestamp),
                Some(SqlBind::Integer(i64::from(message.attempts))),
                message.last_error.as_deref().map(SqlBind::from),
                message.source_aggregate_type.as_deref().map(SqlBind::from),
                message.source_aggregate_id.as_deref().map(SqlBind::from),
                message
                    .source_sequence
                    .map(|value| signed(value, "outbox source sequence").map(SqlBind::Integer))
                    .transpose()?,
                message.correlation_id().map(SqlBind::from),
                message.causation_id().map(SqlBind::from),
            ];
            for (column, value) in values.into_iter().enumerate() {
                if column != 0 {
                    statement.push(", ");
                }
                statement.part(optional(value));
            }
            statement.push(")");
        }
        result.push(OutboxInsert {
            statement,
            first_id: chunk[0].id(),
        });
    }
    Ok(result)
}

pub(crate) fn from_row(row: impl SqlRow) -> Result<OutboxMessage, RepositoryError> {
    let status_text = row.text("status")?;
    let status = status_text
        .parse::<OutboxMessageStatus>()
        .map_err(|_| RepositoryError::Model(format!("outbox status `{status_text}` is invalid")))?;
    let mut metadata: std::collections::HashMap<String, String> =
        serde_json::from_str(&row.text("metadata")?).map_err(json_error)?;
    for column in ["correlation_id", "causation_id"] {
        if let Some(value) = row.optional_text(column)? {
            metadata.insert(column.into(), value);
        }
    }
    Ok(OutboxMessage {
        id: row.text("message_id")?,
        event_type: row.text("event_type")?,
        payload: row.bytes("payload")?,
        payload_codec: row.text("payload_codec")?,
        payload_codec_version: u16::try_from(row.integer("payload_codec_version")?)
            .map_err(|_| RepositoryError::Model("invalid outbox payload codec version".into()))?,
        status,
        metadata,
        created_at: row.timestamp("created_at")?,
        worker_id: row.optional_text("claimed_by")?,
        leased_until: row.optional_timestamp("claimed_until")?,
        attempts: u32::try_from(row.integer("attempts")?)
            .map_err(|_| RepositoryError::Model("invalid outbox attempts".into()))?,
        last_error: row.optional_text("last_error")?,
        destination: row.optional_text("destination")?,
        source_aggregate_type: row.optional_text("source_aggregate_type")?,
        source_aggregate_id: row.optional_text("source_aggregate_id")?,
        source_sequence: row
            .optional_integer("source_sequence")?
            .map(|value| unsigned(value, "outbox source sequence"))
            .transpose()?,
    })
}

fn sqlite_claimable(statement: &mut Statement<'_>, now: SystemTime) {
    statement.push("((status = 'pending' AND CAST(next_available_at AS REAL) <= CAST(");
    statement.push_bind(SqlBind::Timestamp(now));
    statement.push(" AS REAL)) OR (status = 'in_flight' AND (claimed_until IS NULL OR CAST(claimed_until AS REAL) <= CAST(");
    statement.push_bind(SqlBind::Timestamp(now));
    statement.push(" AS REAL))))");
}

fn destination(statement: &mut Statement<'_>, request: &ClaimOutboxMessages) {
    if let Some(destination) = request.destination.as_deref() {
        statement.push(" AND destination = ");
        statement.push_bind(destination);
    }
}

/// SQLite has no row-lock/SKIP LOCKED claim. Both SQLx SQLite and cells use
/// this candidate scan plus conditional update in a caller-owned transaction.
pub(crate) async fn claim_sqlite(
    executor: &mut impl SqlExecutor,
    request: ClaimOutboxMessages,
    now: SystemTime,
) -> Result<Vec<OutboxMessage>, RepositoryError> {
    if request.batch_size == 0 {
        return Ok(Vec::new());
    }
    let deadline = now
        .checked_add(request.lease)
        .ok_or_else(|| RepositoryError::Model("failed to compute outbox lease deadline".into()))?;
    let ids = if let Some(ids) = request.message_ids.clone() {
        ids
    } else {
        let mut query = Statement::new("SELECT message_id FROM outbox_messages WHERE ");
        sqlite_claimable(&mut query, now);
        destination(&mut query, &request);
        query.push(" ORDER BY CAST(created_at AS REAL) ASC, message_id ASC LIMIT ");
        query.push_bind(signed(request.batch_size as u64, "outbox claim limit")?);
        executor
            .query(query)
            .await?
            .into_iter()
            .map(|row| row.text("message_id"))
            .collect::<Result<Vec<_>, _>>()?
    };
    let mut claimed = Vec::new();
    for id in ids {
        if claimed.len() >= request.batch_size {
            break;
        }
        let mut update =
            Statement::new("UPDATE outbox_messages SET status = 'in_flight', claimed_by = ")
                .bind(request.worker_id.as_str().into())
                .sql(", claimed_until = ")
                .bind(SqlBind::Timestamp(deadline))
                .sql(
                    ", attempts = attempts + 1, updated_at = CURRENT_TIMESTAMP WHERE message_id = ",
                )
                .bind(id.as_str().into())
                .sql(" AND ");
        sqlite_claimable(&mut update, now);
        destination(&mut update, &request);
        if executor.execute(update).await? == 0 {
            continue;
        }
        let rows = executor
            .query(
                Statement::new("SELECT ")
                    .sql(sqlite_codec::OUTBOX_SELECT)
                    .sql(" FROM outbox_messages WHERE message_id = ")
                    .bind(id.as_str().into()),
            )
            .await?;
        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| RepositoryError::NotFound { id })?;
        claimed.push(from_row(row)?);
    }
    Ok(claimed)
}

pub(crate) enum Transition<'a> {
    Complete,
    Release(&'a str),
    Fail(&'a str),
}

/// Acknowledgement removes delivery work. Event history and command-replay
/// evidence are separate records and are never reconstructed from this table.
pub(crate) async fn transition<E: SqlExecutor>(
    executor: &mut E,
    claim: &OutboxClaimRef,
    transition: Transition<'_>,
    now: SystemTime,
) -> Result<(), RepositoryError> {
    let mut statement = match transition {
        Transition::Complete => Statement::new("DELETE FROM outbox_messages"),
        Transition::Release(error) | Transition::Fail(error) => {
            let released = matches!(transition, Transition::Release(_));
            let mut statement = Statement::new("UPDATE outbox_messages SET status = ")
                .bind(if released { "pending" } else { "failed" }.into())
                .sql(", claimed_by = NULL, claimed_until = NULL, last_error = ");
            statement.part(optional((!error.is_empty()).then(|| error.into())));
            statement.push(if released {
                ", next_available_at = "
            } else {
                ", failed_at = "
            });
            statement.push_bind(SqlBind::Timestamp(now));
            statement.push(", updated_at = ");
            statement.push(E::NOW);
            statement
        }
    };
    statement.push(" WHERE message_id = ");
    statement.push_bind(claim.message_id.as_str());
    statement.push(" AND status = 'in_flight' AND claimed_by = ");
    statement.push_bind(claim.worker_id.as_str());
    statement.push(" AND attempts = ");
    statement.push_bind(i64::from(claim.attempt));
    statement.push(" AND claimed_until IS NOT NULL AND ");
    statement.part(SqlPart::TimestampCompare {
        column: "claimed_until",
        operator: ">",
        value: now,
    });
    if executor.execute(statement).await? > 0 {
        return Ok(());
    }
    let row = executor
        .query(
            Statement::new("SELECT ")
                .sql(E::OUTBOX_SELECT)
                .sql(" FROM outbox_messages WHERE message_id = ")
                .bind(claim.message_id.as_str().into()),
        )
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| RepositoryError::NotFound {
            id: claim.message_id.clone(),
        })?;
    let message = from_row(row)?;
    ensure_active_claim(&message, Some(claim), now)?;
    Err(RepositoryError::InvalidState {
        id: claim.message_id.clone(),
        expected: "settled outbox claim",
        actual: "conditional settlement changed no row".into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_plans_bound_rows_and_borrow_payloads() {
        let messages = (0..3)
            .map(|n| {
                OutboxMessage::create(format!("message-{n}"), "Changed", vec![7; 4096]).unwrap()
            })
            .collect::<Vec<_>>();
        let plans = inserts(&messages, 38).unwrap();
        assert_eq!(plans.len(), 2);
        let payloads = plans
            .iter()
            .flat_map(|plan| &plan.statement.0)
            .filter_map(|part| match part {
                SqlPart::Bind(SqlBind::Bytes(Cow::Borrowed(bytes))) => Some(*bytes),
                SqlPart::Bind(SqlBind::Bytes(Cow::Owned(_))) => panic!("payload was copied"),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(payloads.len(), messages.len());
        for (payload, message) in payloads.iter().zip(&messages) {
            assert_eq!(payload.as_ptr(), message.payload.as_ptr());
        }
        for plan in &plans {
            assert!(
                plan.statement
                    .0
                    .iter()
                    .filter(|part| matches!(part, SqlPart::Bind(_)))
                    .count()
                    <= 38
            );
        }
        assert!(inserts(&messages, 18).is_err());
        let mut overflow = messages;
        overflow[0].source_sequence = Some(u64::MAX);
        assert!(inserts(&overflow, 38).is_err());
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn stale_ack_cannot_delete_reclaimed_work_and_delivery_keeps_event_history() {
        use crate::sqlx_repo::repo::ConnectionExecutor;
        use crate::{
            CommitBatch, Entity, GetStream, SqliteRepository, StreamIdentity, StreamWrite,
            TransactionalCommit,
        };
        use std::time::{Duration, UNIX_EPOCH};

        let repo = SqliteRepository::connect_and_migrate("sqlite::memory:")
            .await
            .unwrap();
        let identity = StreamIdentity::new("Item", "one").unwrap();
        let mut entity = Entity::with_id("one");
        entity.digest_empty("created").unwrap();
        let mut message = OutboxMessage::create("event-1", "Created", b"payload".to_vec()).unwrap();
        let now = UNIX_EPOCH + Duration::from_secs(100);
        message.created_at = now;
        let mut batch = CommitBatch::new(vec![StreamWrite::new(identity.clone(), &mut entity)]);
        batch.outbox_messages.push(message);
        repo.commit_batch(batch).await.unwrap();

        let mut tx = repo.pool().begin().await.unwrap();
        let first = claim_sqlite(
            &mut ConnectionExecutor::<sqlx::Sqlite>(&mut tx),
            ClaimOutboxMessages::new("same-worker", 1, Duration::from_secs(1)),
            now,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let first = OutboxClaimRef::from_message(&first[0]).unwrap();
        let expired = now + Duration::from_secs(1);
        let mut tx = repo.pool().begin().await.unwrap();
        let second = claim_sqlite(
            &mut ConnectionExecutor::<sqlx::Sqlite>(&mut tx),
            ClaimOutboxMessages::new("same-worker", 1, Duration::from_secs(60)),
            expired,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(second[0].id(), first.message_id);
        assert_eq!(second[0].attempts, first.attempt + 1);
        assert_eq!(second[0].payload, b"payload");
        let second = OutboxClaimRef::from_message(&second[0]).unwrap();

        let mut connection = repo.pool().acquire().await.unwrap();
        let mut executor = ConnectionExecutor::<sqlx::Sqlite>(&mut connection);
        for action in [
            Transition::Complete,
            Transition::Release("late"),
            Transition::Fail("late"),
        ] {
            assert!(matches!(
                transition(&mut executor, &first, action, expired).await,
                Err(RepositoryError::InvalidState { .. })
            ));
        }
        assert_eq!(
            executor
                .query(Statement::new("SELECT message_id FROM outbox_messages"))
                .await
                .unwrap()
                .len(),
            1
        );
        transition(&mut executor, &second, Transition::Complete, expired)
            .await
            .unwrap();
        assert!(executor
            .query(Statement::new("SELECT message_id FROM outbox_messages"))
            .await
            .unwrap()
            .is_empty());
        assert!(matches!(
            transition(&mut executor, &second, Transition::Complete, expired).await,
            Err(RepositoryError::NotFound { .. })
        ));
        drop(connection);
        let restored = repo.get_stream(&identity).await.unwrap().unwrap();
        assert_eq!(restored.version(), 1);
        assert_eq!(restored.committed_version(), 1);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn delivery_settlement_rolls_back_with_its_transaction() {
        use crate::sqlx_repo::repo::ConnectionExecutor;
        use crate::{CommitBatch, OutboxStore, SqliteRepository, TransactionalCommit};
        use std::time::Duration;
        let repo = SqliteRepository::connect_and_migrate("sqlite::memory:")
            .await
            .unwrap();
        let mut batch = CommitBatch::empty();
        batch
            .outbox_messages
            .push(OutboxMessage::create("event-1", "Changed", vec![]).unwrap());
        repo.commit_batch(batch).await.unwrap();
        let store = repo.outbox_store();
        let claimed = store
            .claim(ClaimOutboxMessages::new(
                "worker",
                1,
                Duration::from_secs(60),
            ))
            .await
            .unwrap();
        let claim = OutboxClaimRef::from_message(&claimed[0]).unwrap();
        let mut tx = repo.pool().begin().await.unwrap();
        transition(
            &mut ConnectionExecutor::<sqlx::Sqlite>(&mut tx),
            &claim,
            Transition::Complete,
            SystemTime::now(),
        )
        .await
        .unwrap();
        tx.rollback().await.unwrap();
        let remaining = store
            .messages_by_status(OutboxMessageStatus::InFlight, 1)
            .await
            .unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(OutboxClaimRef::from_message(&remaining[0]).unwrap(), claim);
        store.complete(&claim).await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox_messages")
            .fetch_one(repo.pool())
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
}
