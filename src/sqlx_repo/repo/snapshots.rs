use super::*;

impl<DB> SnapshotStore for SqlxRepository<DB>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    for<'c> &'c Pool<DB>: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    fn get_snapshot<'a>(
        &'a self,
        identity: &'a StreamIdentity,
    ) -> impl Future<Output = Result<Option<SnapshotRecord>, RepositoryError>> + Send + 'a {
        async move {
            let mut connection = self.pool.acquire().await.map_err(|error| {
                repository_storage_error::<DB>("acquire snapshot connection", error)
            })?;
            crate::repository::sql::load_snapshot(
                &mut super::executor::ConnectionExecutor::<DB>(&mut connection),
                identity,
            )
            .await
        }
    }

    fn get_snapshots<'a>(
        &'a self,
        identities: &'a [StreamIdentity],
    ) -> impl Future<Output = Result<Vec<SnapshotRecord>, RepositoryError>> + Send + 'a {
        async move {
            if identities.is_empty() {
                return Ok(Vec::new());
            }

            let mut records = Vec::with_capacity(identities.len());
            for (aggregate_type, aggregate_ids) in ids_by_type(identities) {
                let mut builder = QueryBuilder::<DB>::new("SELECT ");
                builder.push(DB::SNAPSHOT_SELECT);
                builder.push(" FROM aggregate_snapshots WHERE aggregate_type = ");
                builder.push_bind(aggregate_type);
                builder.push(" AND ");
                DB::push_id_filter(&mut builder, &aggregate_ids);
                let rows = builder
                    .build()
                    .fetch_all(&self.pool)
                    .await
                    .map_err(|err| repository_storage_error::<DB>("load snapshots", err))?;
                for row in rows {
                    records.push(snapshot_from_row::<DB>(row)?);
                }
            }
            Ok(records)
        }
    }

    fn save_snapshot<'a>(
        &'a self,
        identity: &'a StreamIdentity,
        record: SnapshotRecord,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
        async move {
            let mut tx =
                self.pool.begin().await.map_err(|err| {
                    repository_storage_error::<DB>("begin snapshot transaction", err)
                })?;
            save_snapshot_in_tx(&mut tx, identity, record).await?;
            tx.commit().await.map_err(|err| {
                repository_storage_error::<DB>("commit snapshot transaction", err)
            })?;
            Ok(())
        }
    }

    fn delete_snapshot<'a>(
        &'a self,
        identity: &'a StreamIdentity,
    ) -> impl Future<Output = Result<bool, RepositoryError>> + Send + 'a {
        async move {
            let mut connection = self.pool.acquire().await.map_err(|error| {
                repository_storage_error::<DB>("acquire snapshot connection", error)
            })?;
            crate::repository::sql::delete_snapshot(
                &mut super::executor::ConnectionExecutor::<DB>(&mut connection),
                identity,
            )
            .await
        }
    }
}
pub(super) async fn save_snapshot_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    identity: &StreamIdentity,
    record: SnapshotRecord,
) -> Result<(), RepositoryError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
{
    crate::repository::sql::save_snapshot(
        &mut super::executor::ConnectionExecutor::<DB>(&mut **tx),
        identity,
        &record,
    )
    .await
}
pub(super) fn snapshot_from_row<DB>(row: DB::Row) -> Result<SnapshotRecord, RepositoryError>
where
    DB: SqlxRepoBackend,
    for<'q> i64: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    crate::repository::sql::snapshot_from_row(&super::executor::EventRow::<DB>(row))
}
