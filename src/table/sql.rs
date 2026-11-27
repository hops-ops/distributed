use std::collections::BTreeMap;

use crate::table::{
    ColumnType, TableMigrationArtifact, TableSchema, TableSchemaBootstrap, TableSchemaRegistry,
    TableStoreError,
};

/// SQL dialect used to render table-schema migration artifacts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableSqlDialect {
    Sqlite,
    Postgres,
}

pub fn generate_table_migration_artifacts(
    registry: &TableSchemaRegistry,
    dialect: TableSqlDialect,
) -> Result<Vec<TableMigrationArtifact>, TableStoreError> {
    Ok(vec![TableMigrationArtifact::new(
        artifact_name(dialect),
        table_schema_statements(registry, dialect)?,
    )])
}

pub fn table_schema_statements(
    registry: &TableSchemaRegistry,
    dialect: TableSqlDialect,
) -> Result<Vec<String>, TableStoreError> {
    registry.validate()?;

    let schemas = table_schemas_in_dependency_order(registry)?;
    let mut statements = Vec::new();
    for schema in &schemas {
        statements.push(create_table_statement(schema, dialect)?);
    }
    for schema in schemas {
        statements.extend(index_statements(schema));
    }
    Ok(statements)
}

/// Statements that bring existing tables' text columns to the collation
/// [`table_schema_statements`] declares, for a bootstrap that finds tables an
/// earlier release created (`CREATE TABLE IF NOT EXISTS` leaves them as they
/// are). Postgres text columns used to take the database's default collation;
/// each statement alters, in place, only the declared text columns that are
/// not yet `COLLATE "C"`, so it is a catalog read once they are. Changing a
/// text column's collation keeps its values and needs no table rewrite; the
/// column's indexes are rebuilt. SQLite text is `BINARY` already.
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) fn text_collation_upgrades(
    registry: &TableSchemaRegistry,
    dialect: TableSqlDialect,
) -> Result<Vec<String>, TableStoreError> {
    const TAG: &str = "$distributed_collation$";
    if dialect == TableSqlDialect::Sqlite {
        return Ok(Vec::new());
    }
    let quote_literal = |value: &str| format!("'{}'", value.replace('\'', "''"));
    let mut statements = Vec::new();
    for schema in registry.schemas() {
        let text_columns = schema
            .columns
            .iter()
            .filter(|column| column.column_type == ColumnType::Text)
            .map(|column| quote_literal(&column.column_name))
            .collect::<Vec<_>>();
        if text_columns.is_empty() {
            continue;
        }
        let statement = format!(
            "DO {TAG}\n\
             DECLARE\n  target_table regclass := to_regclass({table});\n  target record;\n\
             BEGIN\n\
             FOR target IN\n\
             SELECT a.attname FROM pg_attribute a\n\
             WHERE a.attrelid = target_table AND a.attnum > 0 AND NOT a.attisdropped\n\
             AND a.attname = ANY (ARRAY[{columns}]::text[])\n\
             AND a.atttypid = 'text'::regtype\n\
             AND a.attcollation <> (SELECT oid FROM pg_collation\n\
             WHERE collname = 'C' AND collnamespace = 'pg_catalog'::regnamespace)\n\
             LOOP\n\
             EXECUTE format('ALTER TABLE %s ALTER COLUMN %I TYPE text COLLATE \"C\"', target_table, target.attname);\n\
             END LOOP;\n\
             END\n{TAG}",
            table = quote_literal(&quote_identifier(&schema.table_name)),
            columns = text_columns.join(", "),
        );
        // A name containing the dollar-quote tag would end the body early.
        if statement.matches(TAG).count() != 2 {
            return Err(TableStoreError::Metadata(format!(
                "table `{}` has a name that cannot be quoted in a collation upgrade",
                schema.table_name
            )));
        }
        statements.push(statement);
    }
    Ok(statements)
}

fn table_schemas_in_dependency_order(
    registry: &TableSchemaRegistry,
) -> Result<Vec<&TableSchema>, TableStoreError> {
    let mut remaining = registry
        .schemas()
        .map(|schema| (schema.table_name.as_str(), schema))
        .collect::<BTreeMap<_, _>>();
    let mut ordered = Vec::with_capacity(remaining.len());

    while !remaining.is_empty() {
        let ready = remaining
            .iter()
            .filter(|(table_name, schema)| {
                schema_dependency_tables(schema).all(|dependency| {
                    dependency == **table_name || !remaining.contains_key(dependency)
                })
            })
            .map(|(table_name, schema)| (*table_name, *schema))
            .collect::<Vec<_>>();

        if ready.is_empty() {
            let cycle = remaining.into_keys().collect::<Vec<_>>().join(", ");
            return Err(TableStoreError::Metadata(format!(
                "table schema foreign-key cycle cannot be bootstrapped inline: {cycle}"
            )));
        }

        for (table_name, schema) in ready {
            remaining.remove(table_name);
            ordered.push(schema);
        }
    }

    Ok(ordered)
}

fn schema_dependency_tables(schema: &TableSchema) -> impl Iterator<Item = &str> {
    schema
        .columns
        .iter()
        .filter_map(|column| column.foreign_key.as_ref())
        .map(|foreign_key| foreign_key.table.as_str())
}

fn create_table_statement(
    schema: &TableSchema,
    dialect: TableSqlDialect,
) -> Result<String, TableStoreError> {
    let mut definitions = schema
        .columns
        .iter()
        .map(|column| {
            let mut definition = format!(
                "{} {}",
                quote_identifier(&column.column_name),
                sql_type(&column.column_type, dialect)?
            );
            if !column.nullable || column.primary_key {
                definition.push_str(" NOT NULL");
            }
            if column.has_default {
                if let Some(default) = column.default.as_deref() {
                    // The single spot in this generator where a value is emitted
                    // unquoted: a DEFAULT clause must be SQL, not a quoted literal.
                    // Restrict it to a small allowlist of safe forms so a stray or
                    // hostile default can't inject arbitrary DDL into a generated
                    // CREATE TABLE.
                    validate_column_default(&column.column_name, default)?;
                    definition.push_str(" DEFAULT ");
                    definition.push_str(default);
                }
            }
            Ok(definition)
        })
        .collect::<Result<Vec<_>, TableStoreError>>()?;

    if let Some(version_column) = schema.version_column.as_deref() {
        definitions.push(format!(
            "{} {} NOT NULL DEFAULT 1",
            quote_identifier(version_column),
            sql_type(&ColumnType::UnsignedInteger, dialect)?
        ));
    }

    definitions.push(format!(
        "PRIMARY KEY ({})",
        schema
            .primary_key
            .columns
            .iter()
            .map(|column| quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", ")
    ));

    for column in &schema.columns {
        if let Some(foreign_key) = &column.foreign_key {
            definitions.push(format!(
                "FOREIGN KEY ({}) REFERENCES {} ({})",
                quote_identifier(&column.column_name),
                quote_identifier(&foreign_key.table),
                quote_identifier(&foreign_key.column)
            ));
        }
    }

    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {} (\n  {}\n);",
        quote_identifier(&schema.table_name),
        definitions.join(",\n  ")
    ))
}

fn index_statements(schema: &TableSchema) -> impl Iterator<Item = String> + '_ {
    schema.indexes.iter().map(|index| {
        let name = index
            .name
            .clone()
            .unwrap_or_else(|| format!("{}_{}_idx", schema.table_name, index.columns.join("_")));
        let unique = if index.unique { "UNIQUE " } else { "" };
        format!(
            "CREATE {unique}INDEX IF NOT EXISTS {} ON {} ({});",
            quote_identifier(&name),
            quote_identifier(&schema.table_name),
            index
                .columns
                .iter()
                .map(|column| quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        )
    })
}

fn sql_type(
    column_type: &ColumnType,
    dialect: TableSqlDialect,
) -> Result<&'static str, TableStoreError> {
    let type_name = match (dialect, column_type) {
        (TableSqlDialect::Sqlite, ColumnType::Text) => "TEXT",
        (TableSqlDialect::Sqlite, ColumnType::Boolean) => "INTEGER",
        (TableSqlDialect::Sqlite, ColumnType::Integer | ColumnType::UnsignedInteger) => "INTEGER",
        (TableSqlDialect::Sqlite, ColumnType::Float) => "REAL",
        (TableSqlDialect::Sqlite, ColumnType::Bytes) => "BLOB",
        (TableSqlDialect::Sqlite, ColumnType::Json) => "TEXT",
        (TableSqlDialect::Sqlite, ColumnType::Timestamp) => "TEXT",
        // Bytewise order and equality on every database locale, as on SQLite
        // (whose default collation is BINARY) and in Rust.
        (TableSqlDialect::Postgres, ColumnType::Text) => "text COLLATE \"C\"",
        (TableSqlDialect::Postgres, ColumnType::Boolean) => "boolean",
        (TableSqlDialect::Postgres, ColumnType::Integer | ColumnType::UnsignedInteger) => "bigint",
        (TableSqlDialect::Postgres, ColumnType::Float) => "double precision",
        (TableSqlDialect::Postgres, ColumnType::Bytes) => "bytea",
        (TableSqlDialect::Postgres, ColumnType::Json) => "jsonb",
        (TableSqlDialect::Postgres, ColumnType::Timestamp) => "timestamptz",
        (_, ColumnType::Unsupported(type_name)) => {
            return Err(TableStoreError::Metadata(format!(
                "unsupported table column type `{type_name}`"
            )));
        }
    };
    Ok(type_name)
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

/// Validate a column `DEFAULT` expression against a conservative allowlist.
///
/// A `DEFAULT` clause is emitted as raw SQL (it must be — quoting it would change
/// its meaning), so this is the one place the otherwise fully-quoted generator
/// trusts a caller-provided string. To keep that hole closed, only a small set of
/// unambiguous, dialect-portable forms is allowed:
///
/// - numeric literals (`0`, `-1`, `3.14`);
/// - boolean / null keywords (`TRUE`, `FALSE`, `NULL`, case-insensitive);
/// - the timestamp keyword `CURRENT_TIMESTAMP`;
/// - single-quoted string literals with embedded quotes properly doubled
///   (`'active'`, `'O''Brien'`).
///
/// Anything else — function calls, multiple tokens, comments, an unbalanced or
/// improperly-escaped quote — is rejected. These defaults come from internal
/// schema definitions today; the allowlist makes that boundary explicit and
/// fails loudly rather than splicing unknown SQL into a `CREATE TABLE`.
fn validate_column_default(column: &str, default: &str) -> Result<(), TableStoreError> {
    if is_allowed_column_default(default) {
        Ok(())
    } else {
        Err(TableStoreError::Metadata(format!(
            "column `{column}` has an unsupported DEFAULT `{default}`; \
             allowed: numeric/boolean/NULL/CURRENT_TIMESTAMP keywords or a \
             single-quoted string literal"
        )))
    }
}

fn is_allowed_column_default(default: &str) -> bool {
    // No surrounding whitespace, no empty value: a default is a single token or a
    // single quoted literal, never a whitespace-joined expression.
    if default.is_empty() || default.trim().len() != default.len() {
        return false;
    }

    let upper = default.to_ascii_uppercase();
    if matches!(
        upper.as_str(),
        "NULL" | "TRUE" | "FALSE" | "CURRENT_TIMESTAMP"
    ) {
        return true;
    }

    // A numeric literal: optional sign, digits, optional single decimal point.
    if is_numeric_literal(default) {
        return true;
    }

    // A single-quoted string literal: starts and ends with `'`, and every interior
    // `'` is doubled (`''`). Walk the bytes pairing escaped quotes so an odd,
    // unescaped quote (which would let the literal "escape" the clause) is rejected.
    is_single_quoted_literal(default)
}

fn is_numeric_literal(value: &str) -> bool {
    let digits = value.strip_prefix(['+', '-']).unwrap_or(value);
    if digits.is_empty() {
        return false;
    }
    let mut seen_dot = false;
    let mut seen_digit = false;
    for ch in digits.chars() {
        match ch {
            '0'..='9' => seen_digit = true,
            '.' if !seen_dot => seen_dot = true,
            _ => return false,
        }
    }
    seen_digit
}

fn is_single_quoted_literal(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() < 2 || bytes[0] != b'\'' || bytes[bytes.len() - 1] != b'\'' {
        return false;
    }
    // Scan the interior (between the outer quotes), requiring every `'` to be part
    // of a doubled `''` escape.
    let inner = &bytes[1..bytes.len() - 1];
    let mut i = 0;
    while i < inner.len() {
        if inner[i] == b'\'' {
            // Must be doubled: the next byte (still inside the interior) is `'`.
            if i + 1 < inner.len() && inner[i + 1] == b'\'' {
                i += 2;
                continue;
            }
            return false;
        }
        i += 1;
    }
    true
}

fn artifact_name(dialect: TableSqlDialect) -> &'static str {
    match dialect {
        TableSqlDialect::Sqlite => "sqlite-tables",
        TableSqlDialect::Postgres => "postgres-tables",
    }
}

pub fn bootstrap_result(registry: &TableSchemaRegistry) -> TableSchemaBootstrap {
    TableSchemaBootstrap::new(registry.table_names().map(str::to_string))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{ForeignKey, PrimaryKey, TableColumn, TableSchema};

    /// A table with text, nullable timestamp and defaulted timestamp columns.
    fn deliveries_schema() -> TableSchema {
        let column = |name: &str, column_type, nullable| TableColumn {
            nullable,
            ..TableColumn::new(name, name, column_type)
        };
        TableSchema {
            model_name: "Delivery".into(),
            table_name: "deliveries".into(),
            columns: vec![
                column("message_id", ColumnType::Text, false),
                column("created_at", ColumnType::Timestamp, false),
                column("claimed_until", ColumnType::Timestamp, true),
                TableColumn {
                    has_default: true,
                    default: Some("CURRENT_TIMESTAMP".into()),
                    ..column("updated_at", ColumnType::Timestamp, false)
                },
            ],
            primary_key: PrimaryKey::new(["message_id"]),
            version_column: None,
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: crate::table::TableKind::ReadModel,
        }
    }

    #[test]
    fn renders_table_schema_for_sqlite() {
        let mut registry = TableSchemaRegistry::new();
        registry
            .register_schema(deliveries_schema())
            .expect("schema should register");

        let artifact = generate_table_migration_artifacts(&registry, TableSqlDialect::Sqlite)
            .expect("artifact should render")
            .pop()
            .expect("artifact should exist");

        assert_eq!(artifact.name, "sqlite-tables");
        assert!(artifact
            .statements
            .iter()
            .any(|statement| statement.contains("CREATE TABLE IF NOT EXISTS \"deliveries\"")));
        assert!(artifact
            .statements
            .iter()
            .any(|statement| statement.contains("\"message_id\" TEXT NOT NULL")));
        assert!(artifact
            .statements
            .iter()
            .any(|statement| statement.contains("\"created_at\" TEXT NOT NULL")));
        assert!(artifact.statements.iter().any(|statement| statement
            .contains("\"updated_at\" TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP")));
    }

    #[test]
    fn renders_table_schema_for_postgres_with_timestamp_columns() {
        let mut registry = TableSchemaRegistry::new();
        registry
            .register_schema(deliveries_schema())
            .expect("schema should register");

        let artifact = generate_table_migration_artifacts(&registry, TableSqlDialect::Postgres)
            .expect("artifact should render")
            .pop()
            .expect("artifact should exist");

        assert_eq!(artifact.name, "postgres-tables");
        // Text compares bytewise without a per-query COLLATE.
        assert!(artifact
            .statements
            .iter()
            .any(|statement| statement.contains("\"message_id\" text COLLATE \"C\" NOT NULL")));
        assert!(artifact
            .statements
            .iter()
            .any(|statement| statement.contains("\"created_at\" timestamptz NOT NULL")));
        assert!(artifact
            .statements
            .iter()
            .any(|statement| statement.contains("\"claimed_until\" timestamptz")));
        assert!(artifact.statements.iter().any(|statement| statement
            .contains("\"updated_at\" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP")));
    }

    #[test]
    fn orders_parent_tables_before_foreign_key_dependents() {
        let parent = TableSchema {
            model_name: "Parent".into(),
            table_name: "parents".into(),
            columns: vec![TableColumn::new("parent_id", "parent_id", ColumnType::Text)],
            primary_key: PrimaryKey::new(["parent_id"]),
            version_column: None,
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: crate::table::TableKind::ReadModel,
        };
        let child = TableSchema {
            model_name: "Child".into(),
            table_name: "children".into(),
            columns: vec![
                TableColumn::new("child_id", "child_id", ColumnType::Text),
                TableColumn {
                    foreign_key: Some(ForeignKey::new("parents", "parent_id")),
                    ..TableColumn::new("parent_id", "parent_id", ColumnType::Text)
                },
            ],
            primary_key: PrimaryKey::new(["child_id"]),
            version_column: None,
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: crate::table::TableKind::ReadModel,
        };
        let mut registry = TableSchemaRegistry::new();
        registry.register_schema(child).expect("child registers");
        registry.register_schema(parent).expect("parent registers");

        let statements = table_schema_statements(&registry, TableSqlDialect::Postgres)
            .expect("statements should render");
        let parent_position = statements
            .iter()
            .position(|statement| statement.contains("CREATE TABLE IF NOT EXISTS \"parents\""))
            .expect("parent statement should exist");
        let child_position = statements
            .iter()
            .position(|statement| statement.contains("CREATE TABLE IF NOT EXISTS \"children\""))
            .expect("child statement should exist");

        assert!(parent_position < child_position);
    }

    #[test]
    fn allows_safe_column_defaults() {
        for default in [
            "0",
            "-1",
            "42",
            "3.14",
            "-0.5",
            "NULL",
            "null",
            "TRUE",
            "false",
            "CURRENT_TIMESTAMP",
            "current_timestamp",
            "'active'",
            "''",
            "'O''Brien'",
        ] {
            assert!(
                is_allowed_column_default(default),
                "expected `{default}` to be allowed"
            );
        }
    }

    #[test]
    fn rejects_unsafe_column_defaults() {
        for default in [
            "",
            " ",
            "1; DROP TABLE users",
            "now()",
            "CURRENT_TIMESTAMP, x",
            "'unterminated",
            "trailing'",
            "'bad'quote'",
            "1 OR 1=1",
            "0x10",
            "-- comment",
            "  0",
        ] {
            assert!(
                !is_allowed_column_default(default),
                "expected `{default}` to be rejected"
            );
        }
    }

    #[test]
    fn create_table_rejects_injected_default() {
        let mut column = TableColumn::new("note", "note", ColumnType::Text);
        column.nullable = true;
        column.has_default = true;
        column.default = Some("'x'); DROP TABLE secrets;--".into());
        let schema = TableSchema {
            model_name: "Note".into(),
            table_name: "notes".into(),
            columns: vec![column],
            primary_key: PrimaryKey::new(["note"]),
            version_column: None,
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: crate::table::TableKind::ReadModel,
        };

        let err = create_table_statement(&schema, TableSqlDialect::Sqlite)
            .expect_err("an injected default must be rejected");
        assert!(
            matches!(err, TableStoreError::Metadata(msg) if msg.contains("unsupported DEFAULT")),
            "unexpected error variant"
        );
    }

    #[test]
    fn bootstrap_result_lists_registered_tables() {
        let mut registry = TableSchemaRegistry::new();
        registry
            .register_schema(deliveries_schema())
            .expect("schema should register");

        let result = bootstrap_result(&registry);

        assert_eq!(result.bootstrapped_tables, vec!["deliveries".to_string()]);
    }
}
