use crate::table::{
    key_fingerprint, validate_row_values, ExpectedVersion, RowKey, RowValue, RowValues,
    TableAdapterCapabilities, TableColumn, TableSchema, TableStoreError, TableWritePlan,
};

pub(crate) fn sql_read_model_capabilities() -> TableAdapterCapabilities {
    TableAdapterCapabilities {
        relational_rows: true,
        sparse_patches: true,
        deletes: true,
    }
}

pub(crate) fn validate_sql_write_plan(plan: &TableWritePlan) -> Result<(), TableStoreError> {
    plan.validate_for(&sql_read_model_capabilities())
}

pub(crate) fn initial_row_version() -> u64 {
    1
}

pub(crate) fn validate_row_expected_version(
    schema: &TableSchema,
    key: &RowKey,
    expected_version: &ExpectedVersion,
    current_version: Option<u64>,
) -> Result<(), TableStoreError> {
    match (expected_version, current_version) {
        (ExpectedVersion::Any, _) => Ok(()),
        (ExpectedVersion::Exact(expected), Some(actual)) if expected == &actual => Ok(()),
        (ExpectedVersion::Exact(expected), Some(actual)) => {
            Err(row_concurrency_conflict(schema, key, *expected, actual))
        }
        (ExpectedVersion::Exact(_), None) => Err(TableStoreError::NotFound {
            collection: schema.table_name.clone(),
            id: key_fingerprint(key),
        }),
        (ExpectedVersion::NotExists, None) => Ok(()),
        (ExpectedVersion::NotExists, Some(actual)) => {
            Err(row_concurrency_conflict(schema, key, 0, actual))
        }
    }
}

pub(crate) fn row_concurrency_conflict(
    schema: &TableSchema,
    key: &RowKey,
    expected: u64,
    actual: u64,
) -> TableStoreError {
    TableStoreError::ConcurrencyConflict {
        collection: schema.table_name.clone(),
        id: key_fingerprint(key),
        expected,
        actual,
    }
}

pub(crate) fn row_values_from_key_and_patch(
    schema: &TableSchema,
    key: &RowKey,
    patch: crate::table::RowPatch,
) -> Result<RowValues, TableStoreError> {
    let mut values = RowValues::new();
    for (column, value) in key.iter() {
        values.insert(column.to_string(), value.clone());
    }
    for (column, value) in patch.into_values() {
        if schema
            .primary_key
            .columns
            .iter()
            .any(|primary_key| primary_key == &column)
        {
            let key_value = key.get(&column).ok_or_else(|| {
                TableStoreError::Metadata(format!(
                    "read model `{}` row key is missing primary-key column `{}`",
                    schema.model_name, column
                ))
            })?;
            if key_value != &value {
                return Err(TableStoreError::Metadata(format!(
                    "read model `{}` patch cannot change primary-key column `{}`",
                    schema.model_name, column
                )));
            }
        }
        values.insert(column, value);
    }
    validate_row_values(schema, &values, true)?;
    validate_values_match_key(schema, key, &values)?;
    Ok(values)
}

pub(crate) fn patch_values_preserving_key<'schema>(
    schema: &'schema TableSchema,
    key: &RowKey,
    patch: &crate::table::RowPatch,
) -> Result<Vec<(&'schema TableColumn, RowValue)>, TableStoreError> {
    let mut values = Vec::new();
    for (column_name, value) in patch.iter() {
        let column = column_by_name(schema, column_name)?;
        if column.primary_key {
            let key_value = key.get(column_name).ok_or_else(|| {
                TableStoreError::Metadata(format!(
                    "read model `{}` row key is missing primary-key column `{}`",
                    schema.model_name, column_name
                ))
            })?;
            if key_value != value {
                return Err(TableStoreError::Metadata(format!(
                    "read model `{}` patch cannot change primary-key column `{}`",
                    schema.model_name, column_name
                )));
            }
            continue;
        }
        values.push((column, value.clone()));
    }
    Ok(values)
}

pub(crate) fn validate_values_match_key(
    schema: &TableSchema,
    key: &RowKey,
    values: &RowValues,
) -> Result<(), TableStoreError> {
    for column in &schema.primary_key.columns {
        let key_value = key.get(column).ok_or_else(|| {
            TableStoreError::Metadata(format!(
                "read model `{}` row key is missing primary-key column `{}`",
                schema.model_name, column
            ))
        })?;
        let row_value = values.get(column).ok_or_else(|| {
            TableStoreError::Metadata(format!(
                "read model `{}` row is missing primary-key column `{}`",
                schema.model_name, column
            ))
        })?;
        if row_value != key_value {
            return Err(TableStoreError::Metadata(format!(
                "read model `{}` row values cannot change primary-key column `{}`",
                schema.model_name, column
            )));
        }
    }
    Ok(())
}

pub(crate) fn row_write_values<'schema>(
    schema: &'schema TableSchema,
    values: &RowValues,
) -> Result<Vec<(&'schema TableColumn, RowValue)>, TableStoreError> {
    values
        .iter()
        .map(|(column_name, value)| Ok((column_by_name(schema, column_name)?, value.clone())))
        .collect()
}

pub(crate) fn column_by_name<'schema>(
    schema: &'schema TableSchema,
    column_name: &str,
) -> Result<&'schema TableColumn, TableStoreError> {
    schema
        .columns
        .iter()
        .find(|column| column.column_name == column_name)
        .ok_or_else(|| {
            TableStoreError::Metadata(format!(
                "read model `{}` write references missing column `{}`",
                schema.model_name, column_name
            ))
        })
}

pub(crate) fn version_column(schema: &TableSchema) -> Result<&str, TableStoreError> {
    schema.version_column.as_deref().ok_or_else(|| {
        TableStoreError::Metadata(format!(
            "read model `{}` requires a version column for SQL write-plan persistence",
            schema.model_name
        ))
    })
}

pub(crate) fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
