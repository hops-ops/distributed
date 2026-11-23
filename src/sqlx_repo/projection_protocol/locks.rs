use super::*;

pub(crate) const PROJECTION_CHANGE_NOTIFY_TABLE: &str = "projection_changes";

/// Reject ordinary/raw table writes once a model-wide causal owner exists.
///
/// The writer holds each table's ownership fence until it commits, and
/// `register_projection_models` holds the same fences exclusively. Whichever
/// transaction gets a fence first decides: a writer that waited for a
/// registration reads the marker it committed, and a registration that waited
/// for writers finds the rows they committed. Writers share the fence, so
/// they never wait for each other.
pub(crate) async fn reject_causal_table_writes_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    tables: &BTreeSet<String>,
) -> Result<(), TableStoreError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
{
    lock_projection_table_ownership_fences_in_tx(tx, tables, DB::OWNERSHIP_FENCE_WRITER_LOCK)
        .await?;
    // The marker is read by its own statement, after the fences are held: a
    // PostgreSQL statement that waited for a lock still reads every other
    // table as of its start.
    let names = tables.iter().map(String::as_str).collect::<Vec<_>>();
    for chunk in names.chunks(DB::MAX_BIND_PARAMS) {
        if !any_causal_table_in_tx(tx, chunk).await? {
            continue;
        }
        // Naming the table costs a statement each, on the refusal path only.
        for table in chunk {
            if any_causal_table_in_tx(tx, &[table]).await? {
                return Err(TableStoreError::CausalWriteRequired {
                    table: (*table).to_string(),
                });
            }
        }
    }
    Ok(())
}

async fn any_causal_table_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    tables: &[&str],
) -> Result<bool, TableStoreError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
{
    let mut builder =
        QueryBuilder::<DB>::new("SELECT 1 FROM projection_causal_tables WHERE table_name IN (");
    let mut names = builder.separated(", ");
    for table in tables {
        names.push_bind(*table);
    }
    builder.push(") LIMIT 1");
    let row = builder
        .build()
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| {
            crate::sqlx_repo::read_model_storage_error(
                DB::BACKEND,
                "check causal projection ownership",
                error,
            )
        })?;
    Ok(row.is_some())
}

/// Hold the ownership fence of every table in `tables` with the row lock
/// `lock` until the transaction ends, creating the fences that do not exist.
pub(super) async fn lock_projection_table_ownership_fences_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    tables: &BTreeSet<String>,
    lock: &'static str,
) -> Result<(), TableStoreError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
{
    // Every statement names the tables in BTreeSet order, so multi-table
    // writers and registration batches never take fences in opposite orders.
    let tables = tables.iter().map(String::as_str).collect::<Vec<_>>();
    for chunk in tables.chunks(DB::MAX_BIND_PARAMS) {
        if locked_fences_in_tx(tx, chunk, lock).await? == chunk.len() {
            continue;
        }
        let mut insert =
            QueryBuilder::<DB>::new("INSERT INTO projection_table_ownership_fences (table_name) ");
        insert.push_values(chunk, |mut row, table| {
            row.push_bind(*table);
        });
        insert.push(" ON CONFLICT (table_name) DO NOTHING");
        insert.build().execute(&mut **tx).await.map_err(|error| {
            crate::sqlx_repo::read_model_storage_error(
                DB::BACKEND,
                "create causal projection ownership fence",
                error,
            )
        })?;
        if locked_fences_in_tx(tx, chunk, lock).await? != chunk.len() {
            return Err(TableStoreError::Storage(format!(
                "{} causal projection ownership fence disappeared",
                DB::BACKEND
            )));
        }
    }
    Ok(())
}

/// Lock the existing fences of `tables` in name order; the number locked.
async fn locked_fences_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    tables: &[&str],
    lock: &'static str,
) -> Result<usize, TableStoreError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
{
    let mut builder = QueryBuilder::<DB>::new(
        "SELECT table_name FROM projection_table_ownership_fences WHERE table_name IN (",
    );
    let mut names = builder.separated(", ");
    for table in tables {
        names.push_bind(*table);
    }
    builder.push(") ORDER BY table_name");
    builder.push(lock);
    let rows = builder
        .build()
        .fetch_all(&mut **tx)
        .await
        .map_err(|error| {
            crate::sqlx_repo::read_model_storage_error(
                DB::BACKEND,
                "lock causal projection ownership fence",
                error,
            )
        })?;
    Ok(rows.len())
}
