//! SQLite backend for the shared SQLx repository.
//!
//! The event-store/snapshot/outbox/inbox logic lives once in
//! [`crate::sqlx_repo::repo`]; this module carries only what is genuinely
//! SQLite-specific: the schema SQL, the database clock, bind-parameter
//! chunking, the unique-constraint predicate, and the candidate-scan outbox
//! claim (SQLite has no row locks). It is feature-gated behind `sqlite` and
//! async-only.

use std::sync::LazyLock;
use std::time::SystemTime;

use sqlx::migrate::Migrator;
use sqlx::query_builder::Separated;
use sqlx::sqlite::SqliteRow;
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool};

use crate::outbox::OutboxMessage;
use crate::outbox_worker::ClaimOutboxMessages;
use crate::repository::RepositoryError;
use crate::sqlx_repo::read_model::quote_identifier;
use crate::sqlx_repo::repo::{
    embedded_migrator, SqlxOutboxStore, SqlxRepository, SQLITE_MIGRATIONS,
};
use crate::sqlx_repo::{
    self, is_sqlite_unique_constraint, read_model_i64_from_u64 as sqlx_read_model_i64_from_u64,
    read_model_u64_from_i64 as sqlx_read_model_u64_from_i64,
};
use crate::table::TableSqlDialect;
use crate::table::{ColumnType, RowValue, TableColumn, TableStoreError};

static SQLITE_MIGRATOR: LazyLock<Migrator> = LazyLock::new(|| embedded_migrator(SQLITE_MIGRATIONS));
const SQLITE_BACKEND: &str = "sqlite";
const SIGNED_INTEGER_STORAGE: &str = "signed integer storage";

/// SQLite-backed async repository.
pub type SqliteRepository = SqlxRepository<Sqlite>;

/// SQLite-backed outbox table store.
pub type SqliteOutboxStore = SqlxOutboxStore<Sqlite>;

impl crate::sqlx_repo::repo::SqlxRepoBackend for Sqlite {
    fn migrator() -> &'static Migrator {
        &SQLITE_MIGRATOR
    }
    // SQLite's historical bound-parameter limit is 999; staying under it keeps
    // the batched inserts portable across SQLite builds.
    const MAX_BIND_PARAMS: usize = 900;
    // SQLite does not abort the transaction on a constraint error, so conflict
    // recovery re-reads stream versions in the same transaction.
    const CONFLICT_REREAD_IN_TX: bool = true;
    const NOW: &'static str = crate::repository::sql::sqlite_now!();
    const BEGIN_READ_THEN_WRITE: &'static str = "BEGIN IMMEDIATE";
    const BEGIN_READ_SNAPSHOT: &'static str = "BEGIN";
    const OWNERSHIP_FENCE_WRITER_LOCK: &'static str = "";
    const OWNERSHIP_FENCE_REGISTRATION_LOCK: &'static str = "";
    const COMMAND_LEDGER_SELECT: &'static str =
        crate::repository::sqlite_select::COMMAND_LEDGER_SELECT;
    const COMMAND_LEDGER_LOCK_SUFFIX: &'static str = "";
    const SCHEDULE_CLAIM_LOCK_SUFFIX: &'static str = "";
    const COMMAND_LEDGER_COMPACTION_LOCK_SUFFIX: &'static str = "";
    const EVENT_SELECT: &'static str = crate::repository::sqlite_select::EVENT_SELECT;
    const SNAPSHOT_SELECT: &'static str = crate::repository::sqlite_select::SNAPSHOT_SELECT;
    const OUTBOX_SELECT: &'static str = crate::repository::sqlite_select::OUTBOX_SELECT;
    const TABLE_DIALECT: TableSqlDialect = TableSqlDialect::Sqlite;

    fn default_pool_size(database_url: &str) -> u32 {
        if database_url.contains(":memory:") {
            1
        } else {
            5
        }
    }

    fn is_unique_violation(err: &sqlx::Error) -> bool {
        is_sqlite_unique_constraint(err)
    }

    fn push_command_ledger_json(builder: &mut QueryBuilder<Sqlite>, json: &str) {
        builder.push_bind(json);
    }

    fn push_metadata(sep: &mut Separated<'_, Sqlite, &'static str>, json: &str) {
        sep.push_bind(json);
    }

    fn push_id_filter(builder: &mut QueryBuilder<Sqlite>, ids: &[&str]) {
        // SQLite has no array type, so the id list is built as bound
        // placeholders.
        builder.push("aggregate_id IN (");
        {
            let mut separated = builder.separated(", ");
            for id in ids {
                separated.push_bind(*id);
            }
        }
        builder.push(")");
    }

    async fn claim_outbox(
        pool: &SqlitePool,
        request: ClaimOutboxMessages,
    ) -> Result<Vec<OutboxMessage>, RepositoryError> {
        if request.batch_size == 0 {
            return Ok(Vec::new());
        }
        // The claim scans candidates before it updates them.
        let mut tx = pool
            .begin_with(Self::BEGIN_READ_THEN_WRITE)
            .await
            .map_err(|error| repository_storage_error("begin outbox claim transaction", error))?;
        let claimed = crate::repository::sql::outbox::claim_sqlite(
            &mut crate::sqlx_repo::repo::ConnectionExecutor::<Sqlite>(&mut tx),
            request,
            SystemTime::now(),
        )
        .await?;
        tx.commit()
            .await
            .map_err(|error| repository_storage_error("commit outbox claim transaction", error))?;
        Ok(claimed)
    }
}

impl crate::sqlx_repo::read_model::SqlxReadModelBackend for Sqlite {
    const BACKEND: &'static str = SQLITE_BACKEND;
    const INTEGER_STORAGE: &'static str = SIGNED_INTEGER_STORAGE;

    fn push_row_value_bind(
        builder: &mut QueryBuilder<Sqlite>,
        value: RowValue,
        column: &TableColumn,
    ) -> Result<(), TableStoreError> {
        match value {
            RowValue::Null => Self::push_null_bind(builder, column)?,
            RowValue::Bool(value) => {
                builder.push_bind(i64::from(value));
            }
            RowValue::I64(value) => {
                builder.push_bind(value);
            }
            RowValue::U64(value) => {
                builder.push_bind(sqlx_read_model_i64_from_u64(
                    SQLITE_BACKEND,
                    value,
                    &column.column_name,
                    SIGNED_INTEGER_STORAGE,
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
                    .map_err(|err| TableStoreError::Serde(err.to_string()))?;
                builder.push_bind(payload);
            }
        }
        Ok(())
    }

    fn push_null_bind(
        builder: &mut QueryBuilder<Sqlite>,
        column: &TableColumn,
    ) -> Result<(), TableStoreError> {
        match &column.column_type {
            ColumnType::Text | ColumnType::Json | ColumnType::Timestamp => {
                builder.push_bind(Option::<String>::None);
            }
            ColumnType::Boolean | ColumnType::Integer | ColumnType::UnsignedInteger => {
                builder.push_bind(Option::<i64>::None);
            }
            ColumnType::Float => {
                builder.push_bind(Option::<f64>::None);
            }
            ColumnType::Bytes => {
                builder.push_bind(Option::<Vec<u8>>::None);
            }
            ColumnType::Unsupported(type_name) => {
                return Err(TableStoreError::Metadata(format!(
                    "read model `{}` column `{}` has unsupported type `{}`",
                    column.field_name, column.column_name, type_name
                )));
            }
        }
        Ok(())
    }

    fn rows_affected(result: &sqlx::sqlite::SqliteQueryResult) -> u64 {
        result.rows_affected()
    }

    fn push_select_column(builder: &mut QueryBuilder<Sqlite>, column: &TableColumn) {
        builder.push(quote_identifier(&column.column_name));
    }

    fn row_value(row: &SqliteRow, column: &TableColumn) -> Result<RowValue, TableStoreError> {
        Ok(match column.column_type {
            ColumnType::Text | ColumnType::Timestamp => row
                .try_get::<Option<String>, _>(column.column_name.as_str())
                .map_err(|err| read_model_storage_error("decode relational text column", err))?
                .map(RowValue::String)
                .unwrap_or(RowValue::Null),
            ColumnType::Boolean => row
                .try_get::<Option<i64>, _>(column.column_name.as_str())
                .map_err(|err| read_model_storage_error("decode relational boolean column", err))?
                .map(|value| RowValue::Bool(value != 0))
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
                    sqlx_read_model_u64_from_i64(SQLITE_BACKEND, value, column.column_name.as_str())
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
                        .map_err(|err| TableStoreError::Serde(err.to_string()))
                })
                .transpose()?
                .unwrap_or(RowValue::Null),
            ColumnType::Unsupported(ref type_name) => {
                return Err(TableStoreError::Metadata(format!(
                    "read model `{}` column `{}` has unsupported type `{}`",
                    column.field_name, column.column_name, type_name
                )));
            }
        })
    }
}

fn repository_storage_error(operation: &str, err: sqlx::Error) -> RepositoryError {
    sqlx_repo::repository_storage_error(SQLITE_BACKEND, operation, err)
}

fn read_model_storage_error(operation: &str, err: sqlx::Error) -> TableStoreError {
    sqlx_repo::read_model_storage_error(SQLITE_BACKEND, operation, err)
}
