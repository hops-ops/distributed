use sqlx::{Database, Encode, Executor, IntoArguments, QueryBuilder, Row, Transaction, Type};

use super::{
    initial_row_version, patch_values_preserving_key, push_key_predicates, quote_identifier,
    row_concurrency_conflict, row_values_from_key_and_patch, row_write_values,
    validate_row_expected_version, validate_sql_write_plan, validate_values_match_key,
    version_column, SqlxReadModelBackend,
};
use crate::sqlx_repo::{
    read_model_i64_from_u64, read_model_storage_error, read_model_u64_from_i64,
};
use crate::table::{
    key_fingerprint, validate_key, validate_row_values, DeleteTableRowMutation, ExpectedVersion,
    PatchMode, PatchTableRowMutation, RowKey, RowValue, RowValues, RowWriteMode, TableColumn,
    TableCommitOutcome, TableMutation, TableRowMutation, TableSchema, TableStoreError,
    TableWritePlan,
};

pub(crate) async fn commit_read_model_tx<DB: SqlxReadModelBackend>(
    tx: Transaction<'_, DB>,
) -> Result<(), TableStoreError> {
    tx.commit()
        .await
        .map_err(|err| read_model_storage_error(DB::BACKEND, "commit transaction", err))
}

pub(crate) async fn apply_read_model_write_plan_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    plan: TableWritePlan,
) -> Result<TableCommitOutcome, TableStoreError>
where
    DB: SqlxReadModelBackend,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<<DB as Database>::Row>,
{
    validate_sql_write_plan(&plan)?;

    for mutation in plan.mutations {
        match mutation {
            TableMutation::UpsertRow(mutation) => {
                upsert_relational_row_in_tx(tx, mutation).await?;
            }
            TableMutation::PatchRow(mutation) => {
                patch_relational_row_in_tx(tx, mutation).await?;
            }
            TableMutation::DeleteRow(mutation) => {
                delete_relational_row_in_tx(tx, mutation).await?;
            }
        }
    }

    Ok(TableCommitOutcome::applied())
}

pub(crate) async fn upsert_relational_row_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    mutation: TableRowMutation,
) -> Result<(), TableStoreError>
where
    DB: SqlxReadModelBackend,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<<DB as Database>::Row>,
{
    validate_key(mutation.schema, &mutation.key)?;
    validate_row_values(mutation.schema, &mutation.values, true)?;
    validate_values_match_key(mutation.schema, &mutation.key, &mutation.values)?;

    // The common case — upsert without an optimistic-version check — is a single
    // `INSERT ... ON CONFLICT (pk) DO UPDATE` round trip. Only version-checked
    // writes need to observe the current row first.
    if matches!(mutation.mode, RowWriteMode::Upsert)
        && matches!(mutation.expected_version, ExpectedVersion::Any)
    {
        return upsert_relational_row_on_conflict_in_tx(tx, mutation.schema, &mutation.values)
            .await;
    }

    let current_version = row_version_in_tx(tx, mutation.schema, &mutation.key).await?;
    validate_row_expected_version(
        mutation.schema,
        &mutation.key,
        &mutation.expected_version,
        current_version,
    )?;
    if matches!(mutation.mode, RowWriteMode::Insert) && current_version.is_some() {
        return Err(row_concurrency_conflict(
            mutation.schema,
            &mutation.key,
            0,
            current_version.unwrap_or_default(),
        ));
    }

    match current_version {
        Some(expected_version) => {
            let rows_affected = update_relational_row_values_in_tx(
                tx,
                mutation.schema,
                &mutation.key,
                &mutation.values,
                Some(expected_version),
            )
            .await?;
            if rows_affected == 0 {
                let actual = row_version_in_tx(tx, mutation.schema, &mutation.key)
                    .await?
                    .unwrap_or(expected_version);
                return Err(row_concurrency_conflict(
                    mutation.schema,
                    &mutation.key,
                    expected_version,
                    actual,
                ));
            }
        }
        None => {
            insert_relational_row_in_tx(
                tx,
                mutation.schema,
                &mutation.values,
                initial_row_version(),
            )
            .await?;
        }
    }

    Ok(())
}

pub(crate) async fn patch_relational_row_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    mutation: PatchTableRowMutation,
) -> Result<(), TableStoreError>
where
    DB: SqlxReadModelBackend,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<<DB as Database>::Row>,
{
    validate_key(mutation.schema, &mutation.key)?;

    // `NotExists` is the only shape that has to observe the row before writing;
    // `Any`/`Exact` run the UPDATE directly and only re-read on a miss to tell
    // "not found" apart from a version conflict.
    if matches!(mutation.expected_version, ExpectedVersion::NotExists) {
        let current_version = row_version_in_tx(tx, mutation.schema, &mutation.key).await?;
        validate_row_expected_version(
            mutation.schema,
            &mutation.key,
            &mutation.expected_version,
            current_version,
        )?;
        if !matches!(mutation.mode, PatchMode::InsertMissing) {
            return Err(TableStoreError::NotFound {
                collection: mutation.schema.table_name.clone(),
                id: key_fingerprint(&mutation.key),
            });
        }
        let values = row_values_from_key_and_patch(mutation.schema, &mutation.key, mutation.patch)?;
        return insert_relational_row_in_tx(tx, mutation.schema, &values, initial_row_version())
            .await;
    }

    let expected_version = match mutation.expected_version {
        ExpectedVersion::Exact(expected) => Some(expected),
        _ => None,
    };
    let patch_values =
        patch_values_preserving_key(mutation.schema, &mutation.key, &mutation.patch)?;
    let rows_affected = update_relational_columns_in_tx(
        tx,
        mutation.schema,
        &mutation.key,
        patch_values,
        expected_version,
    )
    .await?;
    if rows_affected == 0 {
        if let Some(expected_version) = expected_version {
            return match row_version_in_tx(tx, mutation.schema, &mutation.key).await? {
                Some(actual) => Err(row_concurrency_conflict(
                    mutation.schema,
                    &mutation.key,
                    expected_version,
                    actual,
                )),
                None => Err(TableStoreError::NotFound {
                    collection: mutation.schema.table_name.clone(),
                    id: key_fingerprint(&mutation.key),
                }),
            };
        }
        if matches!(mutation.mode, PatchMode::InsertMissing) {
            let values =
                row_values_from_key_and_patch(mutation.schema, &mutation.key, mutation.patch)?;
            insert_relational_row_in_tx(tx, mutation.schema, &values, initial_row_version())
                .await?;
        } else {
            return Err(TableStoreError::NotFound {
                collection: mutation.schema.table_name.clone(),
                id: key_fingerprint(&mutation.key),
            });
        }
    }

    Ok(())
}

pub(crate) async fn delete_relational_row_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    mutation: DeleteTableRowMutation,
) -> Result<(), TableStoreError>
where
    DB: SqlxReadModelBackend,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<<DB as Database>::Row>,
{
    validate_key(mutation.schema, &mutation.key)?;

    match mutation.expected_version {
        // The row must not exist: nothing to delete, but surface a conflict if it does.
        ExpectedVersion::NotExists => {
            let current_version = row_version_in_tx(tx, mutation.schema, &mutation.key).await?;
            validate_row_expected_version(
                mutation.schema,
                &mutation.key,
                &mutation.expected_version,
                current_version,
            )?;
            Ok(())
        }
        // No version check: one DELETE; deleting a missing row is a no-op.
        ExpectedVersion::Any => {
            delete_relational_row_where_version_in_tx(tx, mutation.schema, &mutation.key, None)
                .await?;
            Ok(())
        }
        // Version-checked delete: only re-read on a miss to tell "not found"
        // apart from a version conflict.
        ExpectedVersion::Exact(expected_version) => {
            let rows_affected = delete_relational_row_where_version_in_tx(
                tx,
                mutation.schema,
                &mutation.key,
                Some(expected_version),
            )
            .await?;
            if rows_affected == 0 {
                return match row_version_in_tx(tx, mutation.schema, &mutation.key).await? {
                    Some(actual) => Err(row_concurrency_conflict(
                        mutation.schema,
                        &mutation.key,
                        expected_version,
                        actual,
                    )),
                    None => Err(TableStoreError::NotFound {
                        collection: mutation.schema.table_name.clone(),
                        id: key_fingerprint(&mutation.key),
                    }),
                };
            }
            Ok(())
        }
    }
}

pub(crate) async fn row_version_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    schema: &TableSchema,
    key: &RowKey,
) -> Result<Option<u64>, TableStoreError>
where
    DB: SqlxReadModelBackend,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<<DB as Database>::Row>,
{
    let version_column = version_column(schema)?;
    let mut builder = QueryBuilder::<DB>::new("SELECT ");
    builder.push(quote_identifier(version_column));
    builder.push(" FROM ");
    builder.push(quote_identifier(&schema.table_name));
    push_key_predicates(&mut builder, schema, key)?;

    let row = builder
        .build()
        .fetch_optional(&mut **tx)
        .await
        .map_err(|err| read_model_storage_error(DB::BACKEND, "load relational row version", err))?;

    row.map(|row| {
        read_model_u64_from_i64(
            DB::BACKEND,
            row.try_get::<i64, _>(version_column).map_err(|err| {
                read_model_storage_error(DB::BACKEND, "decode relational row version", err)
            })?,
            version_column,
        )
    })
    .transpose()
}

pub(crate) async fn insert_relational_row_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    schema: &TableSchema,
    values: &RowValues,
    version: u64,
) -> Result<(), TableStoreError>
where
    DB: SqlxReadModelBackend,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<<DB as Database>::Row>,
{
    let version_column = version_column(schema)?;
    let write_values = row_write_values(schema, values)?;
    let has_write_values = !write_values.is_empty();
    let mut builder = QueryBuilder::<DB>::new("INSERT INTO ");
    builder.push(quote_identifier(&schema.table_name));
    builder.push(" (");
    for (index, (column, _)) in write_values.iter().enumerate() {
        if index > 0 {
            builder.push(", ");
        }
        builder.push(quote_identifier(&column.column_name));
    }
    if has_write_values {
        builder.push(", ");
    }
    builder.push(quote_identifier(version_column));
    builder.push(") VALUES (");
    for (index, (column, value)) in write_values.into_iter().enumerate() {
        if index > 0 {
            builder.push(", ");
        }
        DB::push_row_value_bind(&mut builder, value, column)?;
    }
    if has_write_values {
        builder.push(", ");
    }
    builder.push_bind(read_model_i64_from_u64(
        DB::BACKEND,
        version,
        version_column,
        DB::INTEGER_STORAGE,
    )?);
    builder.push(")");

    builder
        .build()
        .execute(&mut **tx)
        .await
        .map_err(|err| read_model_storage_error(DB::BACKEND, "insert relational row", err))?;

    Ok(())
}

/// Upsert one row in a single statement: `INSERT ... ON CONFLICT (pk) DO UPDATE
/// SET <non-pk columns> = excluded.<column>, <version> = <version> + 1`.
///
/// Both Postgres and SQLite (≥ 3.35) support `ON CONFLICT` with an explicit
/// column-list target and the `excluded` pseudo-table. New rows start at
/// version 1; conflicting rows bump their version in-database (an increment
/// past `i64::MAX` fails as a storage error).
pub(crate) async fn upsert_relational_row_on_conflict_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    schema: &TableSchema,
    values: &RowValues,
) -> Result<(), TableStoreError>
where
    DB: SqlxReadModelBackend,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<<DB as Database>::Row>,
{
    let version_column = version_column(schema)?;
    let write_values = row_write_values(schema, values)?;
    let mut builder = QueryBuilder::<DB>::new("INSERT INTO ");
    builder.push(quote_identifier(&schema.table_name));
    builder.push(" (");
    for (column, _) in &write_values {
        builder.push(quote_identifier(&column.column_name));
        builder.push(", ");
    }
    builder.push(quote_identifier(version_column));
    builder.push(") VALUES (");
    for (column, value) in write_values.iter().cloned() {
        DB::push_row_value_bind(&mut builder, value, column)?;
        builder.push(", ");
    }
    builder.push_bind(read_model_i64_from_u64(
        DB::BACKEND,
        initial_row_version(),
        version_column,
        DB::INTEGER_STORAGE,
    )?);
    builder.push(") ON CONFLICT (");
    for (index, column_name) in schema.primary_key.columns.iter().enumerate() {
        if index > 0 {
            builder.push(", ");
        }
        builder.push(quote_identifier(column_name));
    }
    builder.push(") DO UPDATE SET ");
    for (column, _) in write_values
        .iter()
        .filter(|(column, _)| !column.primary_key)
    {
        builder.push(quote_identifier(&column.column_name));
        builder.push(" = excluded.");
        builder.push(quote_identifier(&column.column_name));
        builder.push(", ");
    }
    // Qualify the existing-row reference with the table name: inside `DO UPDATE`
    // an unqualified column is ambiguous with `excluded` on Postgres.
    builder.push(quote_identifier(version_column));
    builder.push(" = ");
    builder.push(quote_identifier(&schema.table_name));
    builder.push(".");
    builder.push(quote_identifier(version_column));
    builder.push(" + 1");

    builder
        .build()
        .execute(&mut **tx)
        .await
        .map_err(|err| read_model_storage_error(DB::BACKEND, "upsert relational row", err))?;

    Ok(())
}

pub(crate) async fn update_relational_row_values_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    schema: &TableSchema,
    key: &RowKey,
    values: &RowValues,
    expected_version: Option<u64>,
) -> Result<u64, TableStoreError>
where
    DB: SqlxReadModelBackend,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<<DB as Database>::Row>,
{
    let mut write_values = row_write_values(schema, values)?;
    write_values.retain(|(column, _)| !column.primary_key);
    update_relational_columns_in_tx(tx, schema, key, write_values, expected_version).await
}

/// `UPDATE <table> SET <columns>, <version> = <version> + 1 WHERE <pk> [AND
/// <version> = <expected>]`, returning the affected-row count. The version bump
/// happens in-database; an increment past `i64::MAX` fails as a storage error.
pub(crate) async fn update_relational_columns_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    schema: &TableSchema,
    key: &RowKey,
    write_values: Vec<(&TableColumn, RowValue)>,
    expected_version: Option<u64>,
) -> Result<u64, TableStoreError>
where
    DB: SqlxReadModelBackend,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<<DB as Database>::Row>,
{
    let version_column = version_column(schema)?;
    let mut builder = QueryBuilder::<DB>::new("UPDATE ");
    builder.push(quote_identifier(&schema.table_name));
    builder.push(" SET ");
    for (column, value) in write_values {
        builder.push(quote_identifier(&column.column_name));
        builder.push(" = ");
        DB::push_row_value_bind(&mut builder, value, column)?;
        builder.push(", ");
    }
    builder.push(quote_identifier(version_column));
    builder.push(" = ");
    builder.push(quote_identifier(version_column));
    builder.push(" + 1");
    push_key_predicates(&mut builder, schema, key)?;
    if let Some(expected_version) = expected_version {
        builder.push(" AND ");
        builder.push(quote_identifier(version_column));
        builder.push(" = ");
        builder.push_bind(read_model_i64_from_u64(
            DB::BACKEND,
            expected_version,
            "expected version",
            DB::INTEGER_STORAGE,
        )?);
    }

    let result = builder
        .build()
        .execute(&mut **tx)
        .await
        .map_err(|err| read_model_storage_error(DB::BACKEND, "update relational row", err))?;
    Ok(DB::rows_affected(&result))
}

pub(crate) async fn delete_relational_row_where_version_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    schema: &TableSchema,
    key: &RowKey,
    expected_version: Option<u64>,
) -> Result<u64, TableStoreError>
where
    DB: SqlxReadModelBackend,
    for<'c> &'c mut <DB as Database>::Connection: Executor<'c, Database = DB>,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<<DB as Database>::Row>,
{
    let mut builder = QueryBuilder::<DB>::new("DELETE FROM ");
    builder.push(quote_identifier(&schema.table_name));
    push_key_predicates(&mut builder, schema, key)?;
    if let Some(version) = expected_version {
        let version_column = version_column(schema)?;
        builder.push(" AND ");
        builder.push(quote_identifier(version_column));
        builder.push(" = ");
        builder.push_bind(read_model_i64_from_u64(
            DB::BACKEND,
            version,
            "expected version",
            DB::INTEGER_STORAGE,
        )?);
    }

    let result = builder
        .build()
        .execute(&mut **tx)
        .await
        .map_err(|err| read_model_storage_error(DB::BACKEND, "delete relational row", err))?;
    Ok(DB::rows_affected(&result))
}
