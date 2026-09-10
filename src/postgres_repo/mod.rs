//! Postgres backend for the shared SQLx repository.
//!
//! The event-store/snapshot/outbox/inbox logic lives once in
//! [`crate::sqlx_repo::repo`]; this module carries only what is genuinely
//! Postgres-specific: the schema SQL, the epoch-`f64`/`to_timestamp()`
//! timestamp codec, the unique-violation predicate, and the CTE +
//! `FOR UPDATE SKIP LOCKED` outbox claim. It is feature-gated behind
//! `postgres` and async-only.

use std::sync::LazyLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sqlx::migrate::Migrator;
use sqlx::query_builder::Separated;
use sqlx::{PgPool, Postgres, QueryBuilder, Row};

use crate::outbox::{OutboxMessage, OutboxMessageStatus};
use crate::outbox_worker::ClaimOutboxMessages;
use crate::repository::RepositoryError;
use crate::sqlx_repo::read_model::quote_identifier;
use crate::sqlx_repo::repo::{
    embedded_migrator, outbox_message_from_row, system_time_epoch_secs, SqlxOutboxStore,
    SqlxRepository, POSTGRES_MIGRATIONS,
};
use crate::sqlx_repo::{
    self, is_postgres_unique_violation, read_model_i64_from_u64 as sqlx_read_model_i64_from_u64,
    read_model_u64_from_i64 as sqlx_read_model_u64_from_i64,
    repository_i64_from_u64 as sqlx_repository_i64_from_u64,
};
use crate::table::TableSqlDialect;
use crate::table::{
    ColumnType, RowValue, TableColumn as ColumnDef, TableStoreError as ReadModelError,
};

static POSTGRES_MIGRATOR: LazyLock<Migrator> =
    LazyLock::new(|| embedded_migrator(POSTGRES_MIGRATIONS));
const POSTGRES_BACKEND: &str = "postgres";
const BIGINT_STORAGE: &str = "bigint storage";

/// Postgres-backed async repository.
pub type PostgresRepository = SqlxRepository<Postgres>;

/// Postgres-backed outbox table store.
pub type PostgresOutboxStore = SqlxOutboxStore<Postgres>;

impl crate::sqlx_repo::repo::SqlxRepoBackend for Postgres {
    fn migrator() -> &'static Migrator {
        &POSTGRES_MIGRATOR
    }
    // The Postgres extended-query protocol caps bind parameters at 65535 per
    // statement. The previous unchunked insert would fail outright on a
    // commit batch above ~6500 events; the shared chunking makes such batches
    // just work.
    const MAX_BIND_PARAMS: usize = 65000;
    // A failed statement aborts the Postgres transaction, so conflict recovery
    // must re-read stream versions over the pool (a separate connection).
    const CONFLICT_REREAD_IN_TX: bool = false;
    const NOW: &'static str = "now()";
    const COMMAND_LEDGER_SELECT: &'static str = "command_name, command_contract_hash, \
         input_hash, state, causation_id, attempt_token, attempt_number, external_binding, \
         EXTRACT(EPOCH FROM lease_expires_at)::double precision AS lease_expires_at, \
         outcome::text AS outcome, \
         EXTRACT(EPOCH FROM created_at)::double precision AS created_at, \
         EXTRACT(EPOCH FROM updated_at)::double precision AS updated_at, \
         EXTRACT(EPOCH FROM completed_at)::double precision AS completed_at, \
         EXTRACT(EPOCH FROM retention_expires_at)::double precision AS retention_expires_at, \
         EXTRACT(EPOCH FROM compacted_at)::double precision AS compacted_at";
    const COMMAND_LEDGER_LOCK_SUFFIX: &'static str = " FOR UPDATE";
    const COMMAND_LEDGER_COMPACTION_LOCK_SUFFIX: &'static str = " FOR UPDATE SKIP LOCKED";
    const EVENT_SELECT: &'static str = "event_name, \
         event_version, \
         payload, \
         payload_codec, \
         payload_codec_version, \
         metadata::text AS metadata, \
         sequence, \
         EXTRACT(EPOCH FROM recorded_at)::double precision AS recorded_at";
    const SNAPSHOT_SELECT: &'static str = "aggregate_type, \
         aggregate_id, \
         version, \
         snapshot_version, \
         payload, \
         payload_codec, \
         payload_codec_version, \
         metadata::text AS metadata, \
         EXTRACT(EPOCH FROM recorded_at)::double precision AS recorded_at";
    const OUTBOX_SELECT: &'static str = "message_id, \
         event_type, \
         payload, \
         payload_codec, \
         payload_codec_version, \
         metadata::text AS metadata, \
         status, \
         EXTRACT(EPOCH FROM created_at)::double precision AS created_at, \
         claimed_by, \
         EXTRACT(EPOCH FROM claimed_until)::double precision AS claimed_until, \
         attempts, \
         last_error, \
         destination, \
         source_aggregate_type, \
         source_aggregate_id, \
         source_sequence, \
         correlation_id, \
         causation_id";
    const ORDER_BY_CREATED_AT: &'static str = "created_at";
    const OUTBOX_OLDEST_CREATED_AT_SELECT: &'static str =
        "EXTRACT(EPOCH FROM MIN(created_at))::double precision AS oldest_created_at";
    const TABLE_DIALECT: TableSqlDialect = TableSqlDialect::Postgres;

    /// Epoch seconds; stored via `to_timestamp(...)` into `timestamptz`.
    type TimestampValue = f64;

    fn is_unique_violation(err: &sqlx::Error) -> bool {
        is_postgres_unique_violation(err)
    }

    fn timestamp_value(timestamp: SystemTime) -> Result<f64, RepositoryError> {
        system_time_epoch_secs::<Postgres>(timestamp)
    }

    fn push_timestamp(sep: &mut Separated<'_, Postgres, &'static str>, value: &f64) {
        sep.push("to_timestamp(")
            .push_bind_unseparated(*value)
            .push_unseparated(")");
    }

    fn push_optional_timestamp(
        sep: &mut Separated<'_, Postgres, &'static str>,
        value: Option<&f64>,
    ) {
        // NULL stays NULL through to_timestamp; the cast keeps `$n` typed.
        sep.push("to_timestamp(")
            .push_bind_unseparated(value.copied())
            .push_unseparated("::double precision)");
    }

    fn push_timestamp_assign(builder: &mut QueryBuilder<Postgres>, value: &f64) {
        builder.push("to_timestamp(");
        builder.push_bind(*value);
        builder.push(")");
    }

    fn push_timestamp_cmp(
        builder: &mut QueryBuilder<Postgres>,
        column: &'static str,
        op: &'static str,
        epoch_secs: f64,
    ) {
        builder.push(column);
        builder.push(" ");
        builder.push(op);
        builder.push(" to_timestamp(");
        builder.push_bind(epoch_secs);
        builder.push(")");
    }

    fn push_command_ledger_now(builder: &mut QueryBuilder<Postgres>) {
        builder.push("clock_timestamp()");
    }

    fn push_command_ledger_now_epoch(builder: &mut QueryBuilder<Postgres>) {
        builder.push("EXTRACT(EPOCH FROM clock_timestamp())::double precision");
    }

    fn push_command_ledger_deadline(builder: &mut QueryBuilder<Postgres>, duration: Duration) {
        builder.push("(clock_timestamp() + make_interval(secs => ");
        builder.push_bind(duration.as_secs_f64());
        builder.push("))");
    }

    fn push_command_ledger_deadline_is_live(builder: &mut QueryBuilder<Postgres>, deadline: &f64) {
        builder.push("to_timestamp(");
        builder.push_bind(*deadline);
        builder.push(") > clock_timestamp()");
    }

    fn push_command_ledger_json(builder: &mut QueryBuilder<Postgres>, json: &str) {
        builder.push_bind(json);
        builder.push("::jsonb");
    }

    fn decode_timestamp(
        row: &sqlx::postgres::PgRow,
        column: &'static str,
    ) -> Result<SystemTime, RepositoryError> {
        system_time_from_epoch_secs(row.try_get(column).map_err(|err| {
            repository_storage_error(&format!("decode {column} timestamp row"), err)
        })?)
    }

    fn decode_optional_timestamp(
        row: &sqlx::postgres::PgRow,
        column: &'static str,
    ) -> Result<Option<SystemTime>, RepositoryError> {
        row.try_get::<Option<f64>, _>(column)
            .map_err(|err| {
                repository_storage_error(&format!("decode {column} timestamp row"), err)
            })?
            .map(system_time_from_epoch_secs)
            .transpose()
    }

    fn push_metadata(sep: &mut Separated<'_, Postgres, &'static str>, json: &str) {
        sep.push_bind(json).push_unseparated("::jsonb");
    }

    fn push_id_filter(builder: &mut QueryBuilder<Postgres>, ids: &[&str]) {
        builder.push("aggregate_id = ANY(");
        builder.push_bind(ids.to_vec());
        builder.push(")");
    }

    fn inbox_purge_query(age: Duration) -> QueryBuilder<Postgres> {
        // `make_interval` takes the cutoff age in whole seconds.
        let mut builder = QueryBuilder::new(
            "DELETE FROM consumer_inbox WHERE processed_at < now() - make_interval(secs => ",
        );
        builder.push_bind(age.as_secs() as f64);
        builder.push(")");
        builder
    }

    async fn claim_outbox(
        pool: &PgPool,
        request: ClaimOutboxMessages,
    ) -> Result<Vec<OutboxMessage>, RepositoryError> {
        {
            if request.batch_size == 0 {
                return Ok(Vec::new());
            }

            let now = SystemTime::now();
            let now_epoch = system_time_epoch_secs::<Postgres>(now)?;
            let claimed_until = now.checked_add(request.lease).ok_or_else(|| {
                RepositoryError::Model("failed to compute outbox lease deadline".into())
            })?;
            let claimed_until_epoch = system_time_epoch_secs::<Postgres>(claimed_until)?;
            let limit = sqlx_repository_i64_from_u64(
                POSTGRES_BACKEND,
                request.batch_size as u64,
                "outbox claim limit",
                BIGINT_STORAGE,
            )?;

            let mut tx = pool
                .begin()
                .await
                .map_err(|err| repository_storage_error("begin outbox claim transaction", err))?;

            let rows = sqlx::query(
                r#"
                WITH candidates AS (
                  SELECT message_id
                  FROM outbox_messages
                  WHERE (
                    (status = $1 AND next_available_at <= to_timestamp($2))
                    OR (status = $3 AND (claimed_until IS NULL OR claimed_until <= to_timestamp($2)))
                  )
                    AND ($4::text IS NULL OR destination = $4)
                    AND ($9::text[] IS NULL OR message_id = ANY($9::text[]))
                  ORDER BY created_at ASC, message_id ASC
                  LIMIT $5
                  FOR UPDATE SKIP LOCKED
                )
                UPDATE outbox_messages AS message
                SET status = $6,
                    claimed_by = $7,
                    claimed_until = to_timestamp($8),
                    attempts = attempts + 1,
                    updated_at = now()
                FROM candidates
                WHERE message.message_id = candidates.message_id
                RETURNING message.message_id,
                          message.event_type,
                          message.payload,
                          message.payload_codec,
                          message.payload_codec_version,
                          message.metadata::text AS metadata,
                          message.status,
                          EXTRACT(EPOCH FROM message.created_at)::double precision AS created_at,
                          message.claimed_by,
                          EXTRACT(EPOCH FROM message.claimed_until)::double precision AS claimed_until,
                          message.attempts,
                          message.last_error,
                          message.destination,
                          message.source_aggregate_type,
                          message.source_aggregate_id,
                          message.source_sequence,
                          message.correlation_id,
                          message.causation_id
                "#,
            )
            .bind(OutboxMessageStatus::Pending.as_str())
            .bind(now_epoch)
            .bind(OutboxMessageStatus::InFlight.as_str())
            .bind(request.destination.as_deref())
            .bind(limit)
            .bind(OutboxMessageStatus::InFlight.as_str())
            .bind(&request.worker_id)
            .bind(claimed_until_epoch)
            .bind(request.message_ids.as_deref())
            .fetch_all(&mut *tx)
            .await
            .map_err(|err| repository_storage_error("claim outbox messages", err))?;

            tx.commit()
                .await
                .map_err(|err| repository_storage_error("commit outbox claim transaction", err))?;

            rows.into_iter()
                .map(outbox_message_from_row::<Postgres>)
                .collect()
        }
    }
}

impl crate::sqlx_repo::read_model::SqlxReadModelBackend for Postgres {
    const BACKEND: &'static str = POSTGRES_BACKEND;
    const INTEGER_STORAGE: &'static str = BIGINT_STORAGE;

    fn push_row_value_bind(
        builder: &mut QueryBuilder<Postgres>,
        value: RowValue,
        column: &ColumnDef,
    ) -> Result<(), ReadModelError> {
        match value {
            RowValue::Null => Self::push_null_bind(builder, column)?,
            RowValue::Bool(value) => {
                builder.push_bind(value);
            }
            RowValue::I64(value) => {
                builder.push_bind(value);
            }
            RowValue::U64(value) => {
                builder.push_bind(sqlx_read_model_i64_from_u64(
                    POSTGRES_BACKEND,
                    value,
                    &column.column_name,
                    BIGINT_STORAGE,
                )?);
            }
            RowValue::F64(value) => {
                builder.push_bind(value);
            }
            RowValue::String(value) => {
                builder.push_bind(value);
            }
            RowValue::Bytes(value) => {
                builder.push_bind(value);
            }
            RowValue::Json(value) => {
                let payload = serde_json::to_string(&value)
                    .map_err(|err| ReadModelError::Serde(err.to_string()))?;
                builder.push_bind(payload);
            }
        }
        push_postgres_type_cast(builder, column);
        Ok(())
    }

    fn push_null_bind(
        builder: &mut QueryBuilder<Postgres>,
        column: &ColumnDef,
    ) -> Result<(), ReadModelError> {
        match &column.column_type {
            ColumnType::Text | ColumnType::Json | ColumnType::Timestamp => {
                builder.push_bind(Option::<String>::None);
            }
            ColumnType::Boolean => {
                builder.push_bind(Option::<bool>::None);
            }
            ColumnType::Integer | ColumnType::UnsignedInteger => {
                builder.push_bind(Option::<i64>::None);
            }
            ColumnType::Float => {
                builder.push_bind(Option::<f64>::None);
            }
            ColumnType::Bytes => {
                builder.push_bind(Option::<Vec<u8>>::None);
            }
            ColumnType::Unsupported(type_name) => {
                return Err(ReadModelError::Metadata(format!(
                    "read model `{}` column `{}` has unsupported type `{}`",
                    column.field_name, column.column_name, type_name
                )));
            }
        }
        Ok(())
    }

    fn rows_affected(result: &sqlx::postgres::PgQueryResult) -> u64 {
        result.rows_affected()
    }

    fn push_select_column(builder: &mut QueryBuilder<Postgres>, column: &ColumnDef) {
        builder.push(quote_identifier(&column.column_name));
        if matches!(column.column_type, ColumnType::Json | ColumnType::Timestamp) {
            builder.push("::text");
        }
        builder.push(" AS ");
        builder.push(quote_identifier(&column.column_name));
    }

    fn row_value(
        row: &sqlx::postgres::PgRow,
        column: &ColumnDef,
    ) -> Result<RowValue, ReadModelError> {
        Ok(match column.column_type {
            ColumnType::Text | ColumnType::Timestamp => row
                .try_get::<Option<String>, _>(column.column_name.as_str())
                .map_err(|err| read_model_storage_error("decode relational text column", err))?
                .map(RowValue::String)
                .unwrap_or(RowValue::Null),
            ColumnType::Boolean => row
                .try_get::<Option<bool>, _>(column.column_name.as_str())
                .map_err(|err| read_model_storage_error("decode relational boolean column", err))?
                .map(RowValue::Bool)
                .unwrap_or(RowValue::Null),
            ColumnType::Integer => row
                .try_get::<Option<i64>, _>(column.column_name.as_str())
                .map_err(|err| read_model_storage_error("decode relational integer column", err))?
                .map(RowValue::I64)
                .unwrap_or(RowValue::Null),
            ColumnType::UnsignedInteger => row
                .try_get::<Option<i64>, _>(column.column_name.as_str())
                .map_err(|err| {
                    read_model_storage_error("decode relational unsigned integer column", err)
                })?
                .map(|value| {
                    sqlx_read_model_u64_from_i64(
                        POSTGRES_BACKEND,
                        value,
                        column.column_name.as_str(),
                    )
                    .map(RowValue::U64)
                })
                .transpose()?
                .unwrap_or(RowValue::Null),
            ColumnType::Float => row
                .try_get::<Option<f64>, _>(column.column_name.as_str())
                .map_err(|err| read_model_storage_error("decode relational float column", err))?
                .map(RowValue::F64)
                .unwrap_or(RowValue::Null),
            ColumnType::Bytes => row
                .try_get::<Option<Vec<u8>>, _>(column.column_name.as_str())
                .map_err(|err| read_model_storage_error("decode relational bytes column", err))?
                .map(RowValue::Bytes)
                .unwrap_or(RowValue::Null),
            ColumnType::Json => row
                .try_get::<Option<String>, _>(column.column_name.as_str())
                .map_err(|err| read_model_storage_error("decode relational json column", err))?
                .map(|payload| {
                    serde_json::from_str(&payload)
                        .map(RowValue::Json)
                        .map_err(|err| ReadModelError::Serde(err.to_string()))
                })
                .transpose()?
                .unwrap_or(RowValue::Null),
            ColumnType::Unsupported(ref type_name) => {
                return Err(ReadModelError::Metadata(format!(
                    "read model `{}` column `{}` has unsupported type `{}`",
                    column.field_name, column.column_name, type_name
                )));
            }
        })
    }

    #[allow(clippy::manual_async_fn)]
    fn push_change_notify<'e, E>(
        executor: E,
        tables: &std::collections::BTreeSet<String>,
    ) -> impl std::future::Future<Output = Result<(), ReadModelError>> + Send
    where
        E: sqlx::Executor<'e, Database = Postgres> + Send,
    {
        async move {
            if tables.is_empty() {
                return Ok(());
            }
            let payload = serde_json::to_string(&tables.iter().collect::<Vec<_>>())
                .map_err(|err| ReadModelError::Serde(err.to_string()))?;
            sqlx::query("SELECT pg_notify('distributed_read_model_changes', $1)")
                .bind(payload)
                .execute(executor)
                .await
                .map_err(|err| {
                    crate::sqlx_repo::read_model_storage_error(
                        "postgres",
                        "pg_notify read model changes",
                        err,
                    )
                })?;
            Ok(())
        }
    }
}

fn push_postgres_type_cast(builder: &mut QueryBuilder<Postgres>, column: &ColumnDef) {
    match column.column_type {
        ColumnType::Json => {
            builder.push("::jsonb");
        }
        ColumnType::Timestamp => {
            builder.push("::timestamptz");
        }
        _ => {}
    }
}

fn system_time_from_epoch_secs(value: f64) -> Result<SystemTime, RepositoryError> {
    if !value.is_finite() || value < 0.0 {
        return Err(RepositoryError::Model(format!(
            "postgres recorded_at epoch value {value} is invalid"
        )));
    }
    Ok(UNIX_EPOCH + Duration::from_secs_f64(value))
}

fn repository_storage_error(operation: &str, err: sqlx::Error) -> RepositoryError {
    sqlx_repo::repository_storage_error(POSTGRES_BACKEND, operation, err)
}

fn read_model_storage_error(operation: &str, err: sqlx::Error) -> ReadModelError {
    sqlx_repo::read_model_storage_error(POSTGRES_BACKEND, operation, err)
}
