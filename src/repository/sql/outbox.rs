//! Delivery rows shared by native SQL and cell-local SQL. Transactions belong
//! to the caller, so these inserts join the command's event and receipt writes.

#[cfg(any(
    feature = "sqlite",
    all(feature = "cloudflare-workers", target_arch = "wasm32")
))]
use super::timestamp_micros;
use super::{json_error, signed, unsigned, SqlBind, SqlExecutor, SqlPart, SqlRow, Statement};
use crate::outbox::{OutboxMessage, OutboxMessageStatus};
use crate::outbox_worker::store::ensure_active_claim;
#[cfg(any(
    feature = "sqlite",
    all(feature = "cloudflare-workers", target_arch = "wasm32")
))]
use crate::outbox_worker::ClaimOutboxMessages;
use crate::outbox_worker::{OutboxBacklogStats, OutboxClaimRef};
use crate::repository::RepositoryError;
use std::borrow::Cow;
use std::time::{Duration, SystemTime};

pub(crate) struct OutboxInsert<'a> {
    pub statement: Statement<'a>,
    // Read by the SQLx adapters only; cell executors do not report it.
    #[cfg_attr(not(any(feature = "sqlite", feature = "postgres")), allow(dead_code))]
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
                Some(SqlBind::timestamp(message.created_at)?),
                // A new row is claimable at once; only a release delays it.
                Some(SqlBind::timestamp(message.created_at)?),
                message.worker_id.as_deref().map(SqlBind::from),
                message.leased_until.map(SqlBind::timestamp).transpose()?,
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
    let created_at = row.timestamp("created_at")?;
    // Only a release with a backoff moves `next_available_at` past creation.
    let available_at = Some(row.timestamp("next_available_at")?).filter(|at| *at > created_at);
    Ok(OutboxMessage {
        id: row.text("message_id")?,
        event_type: row.text("event_type")?,
        payload: row.bytes("payload")?,
        payload_codec: row.text("payload_codec")?,
        payload_codec_version: u16::try_from(row.integer("payload_codec_version")?)
            .map_err(|_| RepositoryError::Model("invalid outbox payload codec version".into()))?,
        status,
        metadata,
        created_at,
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
        available_at,
    })
}

/// Rows a claim may take at `now`: pending and due, or in flight with an
/// expired lease. The leading term is the claim index's own WHERE clause,
/// which SQLite needs to see verbatim before it uses that index.
#[cfg(any(
    feature = "sqlite",
    all(feature = "cloudflare-workers", target_arch = "wasm32")
))]
fn claimable(statement: &mut Statement<'_>, now: i64) {
    statement.push(
        "status IN ('pending', 'in_flight') AND ((status = 'pending' AND next_available_at <= ",
    );
    statement.push_bind(now);
    statement.push(") OR (status = 'in_flight' AND (claimed_until IS NULL OR claimed_until <= ");
    statement.push_bind(now);
    statement.push(")))");
}

#[cfg(any(
    feature = "sqlite",
    all(feature = "cloudflare-workers", target_arch = "wasm32")
))]
fn destination(statement: &mut Statement<'_>, request: &ClaimOutboxMessages) {
    if let Some(destination) = request.destination.as_deref() {
        statement.push(" AND destination = ");
        statement.push_bind(destination);
    }
}

/// SQLite has no row-lock/SKIP LOCKED claim. Both SQLx SQLite and cells use
/// this candidate scan plus conditional update in a caller-owned transaction.
#[cfg(any(
    feature = "sqlite",
    all(feature = "cloudflare-workers", target_arch = "wasm32")
))]
pub(crate) async fn claim_sqlite<E: SqlExecutor>(
    executor: &mut E,
    request: ClaimOutboxMessages,
    now: SystemTime,
) -> Result<Vec<OutboxMessage>, RepositoryError> {
    if request.batch_size == 0 {
        return Ok(Vec::new());
    }
    let deadline = now
        .checked_add(request.lease)
        .ok_or_else(|| RepositoryError::Model("failed to compute outbox lease deadline".into()))?;
    let deadline = timestamp_micros(deadline)?;
    let now = timestamp_micros(now)?;
    let ids = if let Some(ids) = request.message_ids.clone() {
        ids
    } else {
        let mut query = Statement::new("SELECT message_id FROM outbox_messages WHERE ");
        claimable(&mut query, now);
        destination(&mut query, &request);
        query.push(" ORDER BY created_at ASC, message_id ASC LIMIT ");
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
                .bind(deadline.into())
                .sql(", attempts = attempts + 1, updated_at = ")
                .sql(E::NOW)
                .sql(" WHERE message_id = ")
                .bind(id.as_str().into())
                .sql(" AND ");
        claimable(&mut update, now);
        destination(&mut update, &request);
        if executor.execute(update).await? == 0 {
            continue;
        }
        let rows = executor
            .query(
                Statement::new("SELECT ")
                    .sql(E::OUTBOX_SELECT)
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

/// Rows of one status, oldest first.
pub(crate) async fn by_status<E: SqlExecutor>(
    executor: &mut E,
    status: OutboxMessageStatus,
    limit: i64,
) -> Result<Vec<OutboxMessage>, RepositoryError> {
    executor
        .query(
            Statement::new("SELECT ")
                .sql(E::OUTBOX_SELECT)
                .sql(" FROM outbox_messages WHERE status = ")
                .bind(status.as_str().into())
                .sql(" ORDER BY created_at ASC, message_id ASC LIMIT ")
                .bind(limit.into()),
        )
        .await?
        .into_iter()
        .map(from_row)
        .collect()
}

/// How many rows are pending, and since when.
pub(crate) async fn backlog<E: SqlExecutor>(
    executor: &mut E,
) -> Result<OutboxBacklogStats, RepositoryError> {
    let rows = executor
        .query(Statement::new(
            "SELECT COUNT(*) AS pending, MIN(created_at) AS oldest FROM outbox_messages \
             WHERE status = 'pending'",
        ))
        .await?;
    let row = rows
        .first()
        .ok_or_else(|| RepositoryError::Model("outbox count returned no row".into()))?;
    Ok(OutboxBacklogStats {
        pending: usize::try_from(row.integer("pending")?)
            .map_err(|_| RepositoryError::Model("outbox count is not representable".into()))?,
        oldest_created_at: row.optional_timestamp("oldest")?,
    })
}

pub(crate) enum Transition<'a> {
    Complete,
    /// Back to pending, claimable again after the delay.
    Release(&'a str, Duration),
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
        Transition::Release(error, _) | Transition::Fail(error) => {
            let released = matches!(transition, Transition::Release(..));
            let mut statement = Statement::new("UPDATE outbox_messages SET status = ")
                .bind(if released { "pending" } else { "failed" }.into())
                .sql(", claimed_by = NULL, claimed_until = NULL, last_error = ");
            statement.part(optional((!error.is_empty()).then(|| error.into())));
            statement.push(if released {
                ", next_available_at = "
            } else {
                ", failed_at = "
            });
            statement.push_bind(SqlBind::timestamp(match transition {
                Transition::Release(_, retry_after) => now + retry_after,
                _ => now,
            })?);
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
    statement.push(" AND claimed_until > ");
    statement.push_bind(SqlBind::timestamp(now)?);
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

/// Put a failed row back in the queue with a fresh attempt count, claimable
/// at once.
pub(crate) async fn requeue_failed<E: SqlExecutor>(
    executor: &mut E,
    message_id: &str,
    now: SystemTime,
) -> Result<(), RepositoryError> {
    let mut statement = Statement::new(
        "UPDATE outbox_messages SET status = 'pending', attempts = 0, failed_at = NULL, \
         next_available_at = ",
    );
    statement.push_bind(SqlBind::timestamp(now)?);
    statement.push(", updated_at = ");
    statement.push(E::NOW);
    statement.push(" WHERE status = 'failed' AND message_id = ");
    statement.push_bind(message_id);
    if executor.execute(statement).await? > 0 {
        return Ok(());
    }
    let row = executor
        .query(
            Statement::new("SELECT ")
                .sql(E::OUTBOX_SELECT)
                .sql(" FROM outbox_messages WHERE message_id = ")
                .bind(message_id.into()),
        )
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| RepositoryError::NotFound {
            id: message_id.to_string(),
        })?;
    let message = from_row(row)?;
    Err(RepositoryError::InvalidState {
        id: message_id.to_string(),
        expected: "failed",
        actual: format!("{:?}", message.status),
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
            Transition::Release("late", Duration::ZERO),
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

    /// A stream append reads the stream version before it inserts, and an
    /// outbox claim scans before it updates, so both must hold the write
    /// lock from the start. While another connection holds it they wait; a
    /// deferred transaction would be refused as busy the moment it wrote.
    #[cfg(feature = "sqlite")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_stream_append_and_outbox_claim_wait_for_a_concurrent_writer() {
        use crate::outbox_worker::OutboxStore;
        use crate::{
            CommitBatch, Entity, SqliteRepository, StreamIdentity, StreamWrite, TransactionalCommit,
        };
        use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

        let directory =
            std::env::temp_dir().join(format!("sqlite-write-wait-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&directory).unwrap();
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(directory.clone());
        let pool = SqlitePoolOptions::new()
            .max_connections(3)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(directory.join("app.sqlite"))
                    .create_if_missing(true),
            )
            .await
            .unwrap();
        let repo = SqliteRepository::new(pool.clone());
        repo.migrate().await.unwrap();

        let append = {
            let repo = repo.clone();
            async move {
                let mut entity = Entity::with_id("one");
                entity.digest_empty("created").unwrap();
                let mut batch = CommitBatch::new(vec![StreamWrite::new(
                    StreamIdentity::new("Item", "one").unwrap(),
                    &mut entity,
                )]);
                batch.outbox_messages.push(
                    OutboxMessage::create("event-1", "Created", b"payload".to_vec()).unwrap(),
                );
                repo.commit_batch(batch)
                    .await
                    .map_err(|error| error.to_string())
            }
        };
        while_another_connection_writes(&pool, append).await;
        let claim = {
            let outbox = repo.outbox_store();
            async move {
                let claimed = outbox
                    .claim(ClaimOutboxMessages::new(
                        "worker",
                        1,
                        std::time::Duration::from_secs(60),
                    ))
                    .await
                    .map_err(|error| error.to_string())?;
                assert_eq!(claimed.len(), 1);
                Ok(())
            }
        };
        while_another_connection_writes(&pool, claim).await;
    }

    /// Run `operation` while another connection holds the write lock: it must
    /// still be waiting after a pause, and succeed once the lock is released.
    #[cfg(feature = "sqlite")]
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
}
