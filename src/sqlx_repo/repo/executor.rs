//! SQLx execution adapter for the runtime-independent SQL event store.

use super::*;
use crate::repository::sql::{SqlBind, SqlExecutor, SqlPart, SqlRow, Statement};

pub(crate) struct ConnectionExecutor<'a, DB: SqlxRepoBackend>(pub &'a mut DB::Connection);
pub(crate) struct EventRow<DB: SqlxRepoBackend>(pub DB::Row);

impl<DB> SqlRow for EventRow<DB>
where
    DB: SqlxRepoBackend,
    for<'q> i64: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    fn text(&self, column: &'static str) -> Result<String, RepositoryError> {
        self.0
            .try_get(column)
            .map_err(|error| repository_storage_error::<DB>(column, error))
    }
    fn optional_text(&self, column: &'static str) -> Result<Option<String>, RepositoryError> {
        self.0
            .try_get(column)
            .map_err(|error| repository_storage_error::<DB>(column, error))
    }
    fn integer(&self, column: &'static str) -> Result<i64, RepositoryError> {
        self.0
            .try_get(column)
            .map_err(|error| repository_storage_error::<DB>(column, error))
    }
    fn optional_integer(&self, column: &'static str) -> Result<Option<i64>, RepositoryError> {
        self.0
            .try_get(column)
            .map_err(|error| repository_storage_error::<DB>(column, error))
    }
    fn bytes(&self, column: &'static str) -> Result<Vec<u8>, RepositoryError> {
        self.0
            .try_get(column)
            .map_err(|error| repository_storage_error::<DB>(column, error))
    }
    fn timestamp(&self, column: &'static str) -> Result<SystemTime, RepositoryError> {
        DB::decode_timestamp(&self.0, column)
    }
    fn optional_timestamp(
        &self,
        column: &'static str,
    ) -> Result<Option<SystemTime>, RepositoryError> {
        DB::decode_optional_timestamp(&self.0, column)
    }
}

fn build<DB>(statement: &Statement<'_>) -> Result<QueryBuilder<DB>, RepositoryError>
where
    DB: SqlxRepoBackend,
    for<'q> i64: Encode<'q, DB> + Type<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
{
    let mut builder = QueryBuilder::<DB>::new("");
    for part in &statement.0 {
        match part {
            SqlPart::TimestampCompare {
                column,
                operator,
                value,
            } => {
                DB::push_timestamp_cmp(
                    &mut builder,
                    column,
                    operator,
                    system_time_epoch_secs::<DB>(*value)?,
                );
            }
            SqlPart::LedgerNow => DB::push_command_ledger_now(&mut builder),
            SqlPart::LedgerNowEpoch => DB::push_command_ledger_now_epoch(&mut builder),
            SqlPart::LedgerDeadline(value) => {
                DB::push_command_ledger_deadline(&mut builder, *value)
            }
            SqlPart::LedgerDeadlineIsLive(value) => DB::push_command_ledger_deadline_is_live(
                &mut builder,
                &DB::timestamp_value(*value)?,
            ),
            SqlPart::LedgerJson(value) => DB::push_command_ledger_json(&mut builder, value),
            SqlPart::Sql(sql) => {
                builder.push(sql);
            }
            SqlPart::Bind(SqlBind::Text(value)) => {
                builder.push_bind(value.as_str());
            }
            SqlPart::Bind(SqlBind::Integer(value)) => {
                builder.push_bind(*value);
            }
            SqlPart::Bind(SqlBind::Bytes(value)) => {
                builder.push_bind(value.as_ref());
            }
            SqlPart::Bind(SqlBind::Metadata(value)) => {
                DB::push_metadata(&mut builder.separated(""), value);
            }
            SqlPart::Bind(SqlBind::Timestamp(value)) => {
                DB::push_timestamp(&mut builder.separated(""), &DB::timestamp_value(*value)?);
            }
        }
    }
    Ok(builder)
}

impl<DB> SqlExecutor for ConnectionExecutor<'_, DB>
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
    type Row = EventRow<DB>;
    const EVENT_SELECT: &'static str = DB::EVENT_SELECT;
    const SNAPSHOT_SELECT: &'static str = DB::SNAPSHOT_SELECT;
    const OUTBOX_SELECT: &'static str = DB::OUTBOX_SELECT;
    const NOW: &'static str = DB::NOW;
    const COMMAND_LEDGER_SELECT: &'static str = DB::COMMAND_LEDGER_SELECT;
    const COMMAND_LEDGER_LOCK_SUFFIX: &'static str = DB::COMMAND_LEDGER_LOCK_SUFFIX;
    const COMMAND_LEDGER_COMPACTION_LOCK_SUFFIX: &'static str =
        DB::COMMAND_LEDGER_COMPACTION_LOCK_SUFFIX;

    async fn query(&mut self, statement: Statement<'_>) -> Result<Vec<Self::Row>, RepositoryError> {
        build::<DB>(&statement)?
            .build()
            .fetch_all(&mut *self.0)
            .await
            .map(|rows| rows.into_iter().map(EventRow).collect())
            .map_err(|error| repository_storage_error::<DB>("query event store", error))
    }

    async fn execute(&mut self, statement: Statement<'_>) -> Result<u64, RepositoryError> {
        build::<DB>(&statement)?
            .build()
            .execute(&mut *self.0)
            .await
            .map(|result| DB::rows_affected(&result))
            .map_err(|error| repository_storage_error::<DB>("write event store", error))
    }
}
