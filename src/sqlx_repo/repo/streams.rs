use super::*;

impl<DB> GetStream for SqlxRepository<DB>
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
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    fn get_stream<'a>(
        &'a self,
        identity: &'a StreamIdentity,
    ) -> impl Future<Output = Result<Option<Entity>, RepositoryError>> + Send + 'a {
        async move {
            let mut connection = self.pool.acquire().await.map_err(|error| {
                repository_storage_error::<DB>("acquire stream connection", error)
            })?;
            crate::repository::sql::load_stream(
                &mut super::executor::ConnectionExecutor::<DB>(&mut connection),
                identity,
                None,
            )
            .await
        }
    }

    fn get_streams<'a>(
        &'a self,
        identities: &'a [StreamIdentity],
    ) -> impl Future<Output = Result<Vec<Entity>, RepositoryError>> + Send + 'a {
        async move {
            if identities.is_empty() {
                return Ok(Vec::new());
            }

            let mut entities = Vec::with_capacity(identities.len());
            for (aggregate_type, aggregate_ids) in ids_by_type(identities) {
                // Ordering by aggregate_id then sequence lets us slice the flat
                // result into per-aggregate entities in one pass. Callers of
                // `get_all` accept storage-order results.
                let mut builder = QueryBuilder::<DB>::new("SELECT aggregate_id, ");
                builder.push(DB::EVENT_SELECT);
                builder.push(" FROM aggregate_events WHERE aggregate_type = ");
                builder.push_bind(aggregate_type);
                builder.push(" AND ");
                DB::push_id_filter(&mut builder, &aggregate_ids);
                builder.push(" ORDER BY aggregate_id ASC, sequence ASC");

                let rows = builder
                    .build()
                    .fetch_all(&self.pool)
                    .await
                    .map_err(|err| repository_storage_error::<DB>("load streams", err))?;

                let mut current_id: Option<String> = None;
                let mut current_events: Vec<EventRecord> = Vec::new();
                for row in rows {
                    let row_id: String = row.try_get("aggregate_id").map_err(|err| {
                        repository_storage_error::<DB>("decode aggregate id row", err)
                    })?;
                    let event = event_from_row::<DB>(row)?;
                    match &current_id {
                        Some(id) if id == &row_id => current_events.push(event),
                        _ => {
                            if let Some(id) = current_id.take() {
                                entities.push(entity_from_events(
                                    id,
                                    std::mem::take(&mut current_events),
                                ));
                            }
                            current_id = Some(row_id);
                            current_events.push(event);
                        }
                    }
                }
                if let Some(id) = current_id.take() {
                    entities.push(entity_from_events(id, current_events));
                }
            }

            Ok(entities)
        }
    }

    fn get_stream_tail<'a>(
        &'a self,
        identity: &'a StreamIdentity,
        after_version: u64,
    ) -> impl Future<Output = Result<Option<Entity>, RepositoryError>> + Send + 'a {
        async move {
            let mut connection = self.pool.acquire().await.map_err(|error| {
                repository_storage_error::<DB>("acquire stream tail connection", error)
            })?;
            crate::repository::sql::load_stream(
                &mut super::executor::ConnectionExecutor::<DB>(&mut connection),
                identity,
                Some(after_version),
            )
            .await
        }
    }
}

impl<DB> CausalGetStream for SqlxRepository<DB>
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
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    fn get_causal_stream_tail<'a>(
        &'a self,
        identity: &'a StreamIdentity,
        after_version: u64,
    ) -> impl Future<Output = Result<Option<Entity>, RepositoryError>> + Send + 'a {
        GetStream::get_stream_tail(self, identity, after_version)
    }

    fn get_causal_stream<'a>(
        &'a self,
        identity: &'a StreamIdentity,
    ) -> impl Future<Output = Result<Option<Entity>, RepositoryError>> + Send + 'a {
        GetStream::get_stream(self, identity)
    }
}

impl<DB> CausalRepositoryIdentity for SqlxRepository<DB>
where
    DB: SqlxRepoBackend,
{
    fn causal_storage_identity(&self) -> CausalStorageIdentity {
        self.causal_storage_identity
    }
}
pub(super) fn entity_from_events(aggregate_id: String, events: Vec<EventRecord>) -> Entity {
    let mut entity = Entity::new();
    entity.set_id(aggregate_id);
    entity.load_from_history(events);
    entity
}
