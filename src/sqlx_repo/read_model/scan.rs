//! The bounded scan lowers declared columns and typed binds through the shared
//! relational adapter. Applications never construct query text.
use super::{
    column_by_name, quote_identifier, relational_row_select, row_to_versioned_values,
    SqlxReadModelBackend,
};
use crate::{
    read_model::{ReadModelScanPage, ReadModelScanRequest},
    table::{RowValue, TableStoreError},
};
use sqlx::{Database, Encode, Executor, IntoArguments, Type};

pub(crate) async fn scan_read_model<DB>(
    pool: &sqlx::Pool<DB>,
    request: ReadModelScanRequest,
) -> Result<ReadModelScanPage, TableStoreError>
where
    DB: SqlxReadModelBackend,
    for<'c> &'c sqlx::Pool<DB>: Executor<'c, Database = DB>,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<<DB as Database>::Row>,
{
    request.validate()?;
    let schema = &request.schema;
    let collation = if DB::BACKEND == "postgres" {
        " COLLATE \"C\""
    } else {
        " COLLATE BINARY"
    };
    let mut builder = relational_row_select::<DB>(schema)?;
    builder.push(" WHERE 1=1");
    for (name, value) in request.equals.iter() {
        builder.push(" AND ");
        builder.push(quote_identifier(name));
        if column_by_name(schema, name)?.column_type == crate::ColumnType::Text {
            builder.push(collation);
        }
        if matches!(value, RowValue::Null) {
            builder.push(" IS NULL");
        } else {
            builder.push(" = ");
            DB::push_row_value_bind(&mut builder, value.clone(), column_by_name(schema, name)?)?;
        }
    }
    // Bytewise text order matches Rust and remains stable across database locales.
    if let Some(after) = request.after() {
        builder.push(" AND ");
        builder.push(quote_identifier(request.key_column()));
        builder.push(collation);
        builder.push(" > ");
        DB::push_row_value_bind(
            &mut builder,
            RowValue::String(after.into()),
            column_by_name(schema, request.key_column())?,
        )?;
    }
    builder.push(" ORDER BY ");
    builder.push(quote_identifier(request.key_column()));
    builder.push(collation);
    builder.push(" ASC LIMIT ");
    builder.push_bind(i64::from(request.limit) + 1);
    let rows = builder.build().fetch_all(pool).await.map_err(|error| {
        crate::sqlx_repo::read_model_storage_error(DB::BACKEND, "scan read-model page", error)
    })?;
    request.finish(
        rows.iter()
            .map(|row| row_to_versioned_values::<DB>(schema, row))
            .collect::<Result<_, _>>()?,
    )
}
