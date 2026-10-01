use std::collections::BTreeMap;
use std::sync::RwLock;

use sqlx::{Database, Encode, Executor, IntoArguments, Type};

use super::{
    column_by_name, push_key_predicates, push_order_by_primary_key, quote_identifier,
    relational_row_select, resolve_registered_read_model_schemas, row_to_versioned_values,
    IncludeSpec, SqlxReadModelBackend,
};
use crate::read_model::{
    ReadModelIncludeRows, ReadModelLoadGraph, ReadModelLoadRequest, ReadModelQueryCapabilities,
    Versioned,
};
use crate::sqlx_repo::read_model_storage_error;
use crate::table::{
    has_many_join_columns, validate_key, RelationshipKind, RowKey, RowValue, RowValues,
    TableSchema, TableSchemaRegistry, TableStoreError,
};

pub(crate) async fn load_relational_row_by_key<DB>(
    pool: &sqlx::Pool<DB>,
    schema: &TableSchema,
    key: &RowKey,
) -> Result<Option<Versioned<RowValues>>, TableStoreError>
where
    DB: SqlxReadModelBackend,
    for<'c> &'c sqlx::Pool<DB>: Executor<'c, Database = DB>,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<<DB as Database>::Row>,
{
    validate_key(schema, key)?;
    let mut builder = relational_row_select::<DB>(schema)?;
    push_key_predicates(&mut builder, schema, key)?;
    let row = builder
        .build()
        .fetch_optional(pool)
        .await
        .map_err(|err| read_model_storage_error(DB::BACKEND, "load relational row", err))?;
    row.map(|row| row_to_versioned_values::<DB>(schema, &row))
        .transpose()
}

pub(crate) async fn load_relationship_rows<DB>(
    pool: &sqlx::Pool<DB>,
    root_schema: &TableSchema,
    root_row: &RowValues,
    spec: &IncludeSpec,
) -> Result<Vec<Versioned<RowValues>>, TableStoreError>
where
    DB: SqlxReadModelBackend,
    for<'c> &'c sqlx::Pool<DB>: Executor<'c, Database = DB>,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<<DB as Database>::Row>,
{
    match spec.relationship.kind {
        RelationshipKind::HasMany => load_has_many_rows(pool, root_schema, root_row, spec).await,
        RelationshipKind::BelongsTo => {
            load_belongs_to_rows(pool, root_schema, root_row, spec).await
        }
        RelationshipKind::ManyToMany => Err(TableStoreError::Metadata(format!(
            "many-to-many relationship `{}` includes are not supported yet",
            spec.relationship.field_name
        ))),
    }
}

pub(crate) async fn load_read_model_graph<DB>(
    pool: &sqlx::Pool<DB>,
    schemas: &RwLock<TableSchemaRegistry>,
    request: ReadModelLoadRequest,
    capabilities: ReadModelQueryCapabilities,
) -> Result<ReadModelLoadGraph, TableStoreError>
where
    DB: SqlxReadModelBackend,
    for<'c> &'c sqlx::Pool<DB>: Executor<'c, Database = DB>,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<<DB as Database>::Row>,
{
    request.validate_for_query_capabilities(&capabilities)?;

    let (root_schema, include_specs) = resolve_registered_read_model_schemas(schemas, &request)?;
    validate_key(&root_schema, &request.key)?;

    let Some(root) = load_relational_row_by_key(pool, &root_schema, &request.key).await? else {
        return Ok(ReadModelLoadGraph::default());
    };

    let mut includes = BTreeMap::new();
    for spec in include_specs {
        let rows = load_relationship_rows(pool, &root_schema, &root.data, &spec).await?;
        includes.insert(
            spec.name,
            ReadModelIncludeRows {
                relationship: spec.relationship,
                target_schema: spec.target_schema,
                rows,
            },
        );
    }

    Ok(ReadModelLoadGraph {
        root: Some(root),
        includes,
    })
}

async fn load_has_many_rows<DB>(
    pool: &sqlx::Pool<DB>,
    root_schema: &TableSchema,
    root_row: &RowValues,
    spec: &IncludeSpec,
) -> Result<Vec<Versioned<RowValues>>, TableStoreError>
where
    DB: SqlxReadModelBackend,
    for<'c> &'c sqlx::Pool<DB>: Executor<'c, Database = DB>,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<<DB as Database>::Row>,
{
    let (target_column, root_column) =
        has_many_join_columns(root_schema, &spec.relationship, &spec.target_schema)?;
    let root_value = root_row.get(&root_column).ok_or_else(|| {
        TableStoreError::Metadata(format!(
            "read model `{}` root row is missing relationship key `{}`",
            root_schema.model_name, root_column
        ))
    })?;

    load_rows_matching_column(pool, &spec.target_schema, &target_column, root_value).await
}

async fn load_belongs_to_rows<DB>(
    pool: &sqlx::Pool<DB>,
    root_schema: &TableSchema,
    root_row: &RowValues,
    spec: &IncludeSpec,
) -> Result<Vec<Versioned<RowValues>>, TableStoreError>
where
    DB: SqlxReadModelBackend,
    for<'c> &'c sqlx::Pool<DB>: Executor<'c, Database = DB>,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<<DB as Database>::Row>,
{
    let (source_column, target_column) = crate::table::belongs_to_join_columns(
        root_schema,
        &spec.relationship,
        &spec.target_schema,
    )?;
    let source_value = root_row.get(&source_column).ok_or_else(|| {
        TableStoreError::Metadata(format!(
            "read model `{}` root row is missing relationship key `{}`",
            root_schema.model_name, source_column
        ))
    })?;

    load_rows_matching_column(pool, &spec.target_schema, &target_column, source_value).await
}

async fn load_rows_matching_column<DB>(
    pool: &sqlx::Pool<DB>,
    schema: &TableSchema,
    column_name: &str,
    value: &RowValue,
) -> Result<Vec<Versioned<RowValues>>, TableStoreError>
where
    DB: SqlxReadModelBackend,
    for<'c> &'c sqlx::Pool<DB>: Executor<'c, Database = DB>,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<<DB as Database>::Row>,
{
    let column = column_by_name(schema, column_name)?;
    let mut builder = relational_row_select::<DB>(schema)?;
    builder.push(" WHERE ");
    builder.push(quote_identifier(column_name));
    builder.push(" = ");
    DB::push_row_value_bind(&mut builder, value.clone(), column)?;
    push_order_by_primary_key(&mut builder, schema);

    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .map_err(|err| read_model_storage_error(DB::BACKEND, "load relationship rows", err))?;

    rows.iter()
        .map(|row| row_to_versioned_values::<DB>(schema, row))
        .collect()
}
