use super::*;

pub(crate) fn event_from_row<DB>(row: DB::Row) -> Result<EventRecord, RepositoryError>
where
    DB: SqlxRepoBackend,
    for<'q> i64: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    crate::repository::sql::event_from_row(&super::executor::EventRow::<DB>(row))
}
