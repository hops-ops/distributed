use super::*;

use crate::repository::sql::outbox::Transition as OutboxTransition;

impl<DB> OutboxStore for SqlxOutboxStore<DB>
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
    fn messages_by_status(
        &self,
        status: OutboxMessageStatus,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<OutboxMessage>, RepositoryError>> + Send + '_ {
        async move {
            let mut builder = QueryBuilder::<DB>::new("SELECT ");
            builder.push(DB::OUTBOX_SELECT);
            builder.push(" FROM outbox_messages WHERE status = ");
            builder.push_bind(status.as_str());
            builder.push(" ORDER BY ");
            builder.push(DB::ORDER_BY_CREATED_AT);
            builder.push(" ASC, message_id ASC LIMIT ");
            // usize::MAX means "no practical bound"; clamp to what the column
            // type can carry.
            builder.push_bind(i64::try_from(limit).unwrap_or(i64::MAX));
            let rows = builder.build().fetch_all(&self.pool).await.map_err(|err| {
                repository_storage_error::<DB>("load outbox messages by status", err)
            })?;

            rows.into_iter()
                .map(outbox_message_from_row::<DB>)
                .collect()
        }
    }

    fn backlog_stats(
        &self,
    ) -> impl Future<Output = Result<OutboxBacklogStats, RepositoryError>> + Send + '_ {
        async move {
            let mut builder = QueryBuilder::<DB>::new("SELECT COUNT(*) AS pending_count, ");
            builder.push(DB::OUTBOX_OLDEST_CREATED_AT_SELECT);
            builder.push(" FROM outbox_messages WHERE status = ");
            builder.push_bind(OutboxMessageStatus::Pending.as_str());
            let row =
                builder.build().fetch_one(&self.pool).await.map_err(|err| {
                    repository_storage_error::<DB>("load outbox backlog stats", err)
                })?;

            let pending_count: i64 = row.try_get("pending_count").map_err(|err| {
                repository_storage_error::<DB>("decode outbox backlog count row", err)
            })?;
            let pending =
                repository_u64_from_i64(DB::BACKEND, pending_count, "outbox backlog count")
                    .and_then(|value| {
                        usize::try_from(value).map_err(|_| {
                            RepositoryError::Model(format!(
                                "{} outbox backlog count value {value} is invalid",
                                DB::BACKEND
                            ))
                        })
                    })?;
            let oldest_created_at = DB::decode_optional_timestamp(&row, "oldest_created_at")?;

            Ok(OutboxBacklogStats {
                pending,
                oldest_created_at,
            })
        }
    }

    fn claim<'a>(
        &'a self,
        request: ClaimOutboxMessages,
    ) -> impl Future<Output = Result<Vec<OutboxMessage>, RepositoryError>> + Send + 'a {
        DB::claim_outbox(&self.pool, request)
    }

    fn complete<'a>(
        &'a self,
        claim: &'a OutboxClaimRef,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
        transition_claimed_outbox_message(&self.pool, claim, OutboxTransition::Complete)
    }

    fn release<'a>(
        &'a self,
        claim: &'a OutboxClaimRef,
        error: &'a str,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
        transition_claimed_outbox_message(&self.pool, claim, OutboxTransition::Release(error))
    }

    fn fail<'a>(
        &'a self,
        claim: &'a OutboxClaimRef,
        error: &'a str,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
        transition_claimed_outbox_message(&self.pool, claim, OutboxTransition::Fail(error))
    }
}

/// Execute the shared, lease-fenced delivery transition on one connection.
async fn transition_claimed_outbox_message<'a, DB>(
    pool: &'a Pool<DB>,
    claim: &'a OutboxClaimRef,
    transition: OutboxTransition<'a>,
) -> Result<(), RepositoryError>
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
    let mut connection = pool
        .acquire()
        .await
        .map_err(|error| repository_storage_error::<DB>("acquire outbox connection", error))?;
    crate::repository::sql::outbox::transition(
        &mut executor::ConnectionExecutor::<DB>(&mut connection),
        claim,
        transition,
        SystemTime::now(),
    )
    .await
}

/// Insert shared delivery-row plans in the command's existing transaction.
pub(super) async fn insert_outbox_messages_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    messages: &[OutboxMessage],
) -> Result<(), RepositoryError>
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
    use crate::repository::sql::SqlExecutor;
    use std::error::Error;
    for insert in crate::repository::sql::outbox::inserts(messages, DB::MAX_BIND_PARAMS)? {
        let result = executor::ConnectionExecutor::<DB>(&mut **tx)
            .execute(insert.statement)
            .await;
        if let Err(error) = result {
            if error
                .source()
                .and_then(|source| source.downcast_ref::<sqlx::Error>())
                .is_some_and(DB::is_unique_violation)
            {
                return Err(RepositoryError::DuplicateOutboxMessageInBatch {
                    id: insert.first_id.to_string(),
                });
            }
            return Err(error);
        }
    }
    Ok(())
}

pub(crate) fn outbox_message_from_row<DB>(row: DB::Row) -> Result<OutboxMessage, RepositoryError>
where
    DB: SqlxRepoBackend,
    for<'q> i64: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    crate::repository::sql::outbox::from_row(executor::EventRow::<DB>(row))
}
