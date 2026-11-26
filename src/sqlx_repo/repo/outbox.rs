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
            let mut connection = self.pool.acquire().await.map_err(|error| {
                repository_storage_error::<DB>("acquire outbox connection", error)
            })?;
            // usize::MAX means "no practical bound"; clamp to what the column
            // type can carry.
            crate::repository::sql::outbox::by_status(
                &mut executor::ConnectionExecutor::<DB>(&mut connection),
                status,
                i64::try_from(limit).unwrap_or(i64::MAX),
            )
            .await
        }
    }

    fn backlog_stats(
        &self,
    ) -> impl Future<Output = Result<OutboxBacklogStats, RepositoryError>> + Send + '_ {
        async move {
            let mut connection = self.pool.acquire().await.map_err(|error| {
                repository_storage_error::<DB>("acquire outbox connection", error)
            })?;
            crate::repository::sql::outbox::backlog(&mut executor::ConnectionExecutor::<DB>(
                &mut connection,
            ))
            .await
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
        retry_after: Duration,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
        transition_claimed_outbox_message(
            &self.pool,
            claim,
            OutboxTransition::Release(error, retry_after),
        )
    }

    fn requeue_failed<'a>(
        &'a self,
        message_id: &'a str,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
        requeue_failed_outbox_message(&self.pool, message_id)
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

/// Requeue one failed delivery row.
async fn requeue_failed_outbox_message<'a, DB>(
    pool: &'a Pool<DB>,
    message_id: &'a str,
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
    crate::repository::sql::outbox::requeue_failed(
        &mut executor::ConnectionExecutor::<DB>(&mut connection),
        message_id,
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
                return Err(RepositoryError::DuplicateOutboxMessage {
                    id: insert.first_id.to_string(),
                });
            }
            return Err(error);
        }
    }
    Ok(())
}

#[cfg(feature = "postgres")]
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
