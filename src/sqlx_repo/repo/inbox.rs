use super::*;

impl<DB> InboxStore for SqlxRepository<DB>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    for<'c> &'c Pool<DB>: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    fn inbox_contains<'a>(
        &'a self,
        consumer: &'a str,
        message_id: &'a str,
    ) -> impl Future<Output = Result<bool, RepositoryError>> + Send + 'a {
        async move {
            let mut builder =
                QueryBuilder::<DB>::new("SELECT 1 FROM consumer_inbox WHERE consumer = ");
            builder.push_bind(consumer);
            builder.push(" AND message_id = ");
            builder.push_bind(message_id);
            builder.push(" LIMIT 1");
            let row = builder
                .build()
                .fetch_optional(&self.pool)
                .await
                .map_err(|err| repository_storage_error::<DB>("query consumer inbox", err))?;
            Ok(row.is_some())
        }
    }

    fn purge_inbox_older_than(
        &self,
        age: std::time::Duration,
    ) -> impl Future<Output = Result<u64, RepositoryError>> + Send {
        async move {
            // Compare against the database clock to avoid client/server skew.
            // An age past any stored time deletes nothing; clamping it keeps
            // the subtraction inside the integer range.
            let age = crate::repository::sql::duration_micros(age).unwrap_or(i64::MAX / 2);
            let mut builder =
                QueryBuilder::<DB>::new("DELETE FROM consumer_inbox WHERE processed_at < ");
            builder.push(DB::NOW);
            builder.push(" - ");
            builder.push_bind(age);
            let result = builder
                .build()
                .execute(&self.pool)
                .await
                .map_err(|err| repository_storage_error::<DB>("purge consumer inbox", err))?;
            Ok(DB::rows_affected(&result))
        }
    }
}
