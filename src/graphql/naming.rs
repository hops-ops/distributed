//! GraphQL name-derivation rules shared by the dep-free SDL renderer and the
//! dynamic schema builder. Every generated name lives here and only here.

use crate::table::{ColumnType, TableSchema};

/// GraphQL name grammar: `[_A-Za-z][_0-9A-Za-z]*` with no leading `__`.
pub fn is_valid_graphql_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some('_') => {
            if name.starts_with("__") {
                return false;
            }
            chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        }
        Some(c) if c.is_ascii_alphabetic() => chars.all(|c| c.is_ascii_alphanumeric() || c == '_'),
        _ => false,
    }
}

pub fn object_type_name(schema: &TableSchema) -> &str {
    &schema.model_name
}

pub fn root_list_field(schema: &TableSchema) -> &str {
    &schema.table_name
}

pub fn by_pk_field(schema: &TableSchema) -> String {
    format!("{}_by_pk", schema.table_name)
}

/// Syntax-only mutation IR field. Same snake_case table name as [`root_list_field`].
pub fn mutation_upsert_field(schema: &TableSchema) -> String {
    format!("upsert_{}", schema.table_name)
}

/// Syntax-only mutation IR delete. Same table name as [`root_list_field`].
pub fn mutation_delete_by_pk_field(schema: &TableSchema) -> String {
    format!("delete_{}_by_pk", schema.table_name)
}

/// Syntax-only mutation IR insert-one. Same table name as [`root_list_field`].
pub fn mutation_insert_one_field(schema: &TableSchema) -> String {
    format!("insert_{}_one", schema.table_name)
}

/// Syntax-only mutation IR update. Same table name as [`root_list_field`].
pub fn mutation_update_by_pk_field(schema: &TableSchema) -> String {
    format!("update_{}_by_pk", schema.table_name)
}

pub fn aggregate_field(schema: &TableSchema) -> String {
    format!("{}_aggregate", schema.table_name)
}

pub fn bool_exp_name(schema: &TableSchema) -> String {
    format!("{}_bool_exp", schema.table_name)
}

pub fn order_by_name(schema: &TableSchema) -> String {
    format!("{}_order_by", schema.table_name)
}

pub fn aggregate_type_name(schema: &TableSchema) -> String {
    format!("{}_aggregate", schema.table_name)
}

pub fn aggregate_fields_type_name(schema: &TableSchema) -> String {
    format!("{}_aggregate_fields", schema.table_name)
}

pub fn sum_fields_type_name(schema: &TableSchema) -> String {
    format!("{}_sum_fields", schema.table_name)
}

pub fn avg_fields_type_name(schema: &TableSchema) -> String {
    format!("{}_avg_fields", schema.table_name)
}

pub fn min_fields_type_name(schema: &TableSchema) -> String {
    format!("{}_min_fields", schema.table_name)
}

pub fn max_fields_type_name(schema: &TableSchema) -> String {
    format!("{}_max_fields", schema.table_name)
}

/// Map a column type to its GraphQL scalar type name (without nullability).
pub fn scalar_type_name(column_type: &ColumnType) -> Option<&'static str> {
    crate::command::scalar_type_name(column_type)
}

pub fn comparison_exp_name(scalar: &str) -> String {
    format!("{scalar}_comparison_exp")
}

/// Portable comparison operators on every scalar's `*_comparison_exp`.
///
/// These compile on both SQLite and Postgres (with dialect-specific cast/ILIKE
/// mapping handled at compile time).
pub const PORTABLE_COMPARISON_OPS: &[&str] = &[
    "_eq", "_neq", "_gt", "_gte", "_lt", "_lte", "_in", "_nin", "_is_null",
];

/// String-only comparison operators (portable; SQLite maps `_ilike` → `LIKE`).
pub const STRING_COMPARISON_OPS: &[&str] = &["_like", "_ilike"];

/// Postgres `jsonb` operators — only on `JSON_comparison_exp` when the engine
/// dialect is Postgres. **Must not** appear on SQLite schema or SDL.
pub const POSTGRES_JSON_COMPARISON_OPS: &[&str] = &["_contains", "_contained_in", "_has_key"];

/// Whether the GraphQL surface should advertise PG JSON comparison ops.
///
/// Pass `true` only for Postgres-backed engines (or SDL rendered for PG).
pub fn include_postgres_json_comparison_ops(dialect_is_postgres: bool) -> bool {
    dialect_is_postgres
}

/// Comparison-exp field names for a GraphQL scalar given dialect JSON-op policy.
pub fn comparison_op_fields(scalar: &str, postgres_json_ops: bool) -> Vec<&'static str> {
    let mut ops: Vec<&'static str> = PORTABLE_COMPARISON_OPS.to_vec();
    if scalar == "String" {
        ops.extend_from_slice(STRING_COMPARISON_OPS);
    }
    if scalar == "JSON" && postgres_json_ops {
        ops.extend_from_slice(POSTGRES_JSON_COMPARISON_OPS);
    }
    ops
}

/// Custom scalars emitted once, alphabetically.
pub const CUSTOM_SCALARS: &[&str] = &["BigInt", "Bytea", "JSON", "Timestamptz"];

/// Framework-owned causal command-status query field.
pub const COMMAND_STATUS_ROOT_FIELD: &str = "commandStatus";

/// Framework-owned causal command-status object and state enum.
pub const DISTRIBUTED_COMMAND_STATUS_TYPE: &str = "DistributedCommandStatus";
pub const DISTRIBUTED_COMMAND_STATE_TYPE: &str = "DistributedCommandState";

/// Stable public command-state vocabulary. Lowercase values are intentional.
pub const DISTRIBUTED_COMMAND_STATE_VALUES: &[&str] = &[
    "in_progress",
    "succeeded",
    "succeeded_pending_projection",
    "atomic",
    "rejected",
    "projection_failed",
    "expired",
    "unknown",
];

/// Built-in + custom scalars that are reserved and must not collide with
/// generated type names.
pub fn reserved_type_names() -> impl Iterator<Item = &'static str> {
    [
        "String",
        "Boolean",
        "Int",
        "Float",
        "ID",
        "Query",
        "Mutation",
        "Subscription",
        "order_by",
        "BigInt",
        "Bytea",
        "JSON",
        "Timestamptz",
    ]
    .into_iter()
}

/// Additional names reserved only on a selected Surface with causal commands.
///
/// Legacy/query-only surfaces retain their existing namespace. The SDL and
/// runtime builders opt into this set after role/application selection proves
/// at least one visible command with a consistency contract.
pub fn causal_protocol_type_names() -> impl Iterator<Item = &'static str> {
    [
        DISTRIBUTED_COMMAND_STATE_TYPE,
        DISTRIBUTED_COMMAND_STATUS_TYPE,
    ]
    .into_iter()
}

pub fn order_by_enum_values() -> &'static [&'static str] {
    &[
        "asc",
        "asc_nulls_first",
        "asc_nulls_last",
        "desc",
        "desc_nulls_first",
        "desc_nulls_last",
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutation_ir_fields_use_snake_case_table_names() {
        let schema = TableSchema {
            model_name: "ChatMessages".into(),
            table_name: "chat_messages".into(),
            columns: Vec::new(),
            primary_key: crate::table::PrimaryKey::new(["message_id"]),
            version_column: None,
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: crate::table::TableKind::ReadModel,
        };
        assert_eq!(root_list_field(&schema), "chat_messages");
        assert_eq!(mutation_upsert_field(&schema), "upsert_chat_messages");
        assert_eq!(
            mutation_delete_by_pk_field(&schema),
            "delete_chat_messages_by_pk"
        );
        assert_eq!(
            mutation_insert_one_field(&schema),
            "insert_chat_messages_one"
        );
        assert_eq!(
            mutation_update_by_pk_field(&schema),
            "update_chat_messages_by_pk"
        );
    }

    #[test]
    fn validates_graphql_names() {
        assert!(is_valid_graphql_name("players"));
        assert!(is_valid_graphql_name("_private"));
        assert!(is_valid_graphql_name("PlayerView"));
        assert!(!is_valid_graphql_name("__typename"));
        assert!(!is_valid_graphql_name("1players"));
        assert!(!is_valid_graphql_name("play-ers"));
        assert!(!is_valid_graphql_name(""));
    }

    #[test]
    fn comparison_op_matrix_sqlite_omits_pg_json() {
        assert!(!include_postgres_json_comparison_ops(false));
        let json_ops = comparison_op_fields("JSON", false);
        for op in POSTGRES_JSON_COMPARISON_OPS {
            assert!(
                !json_ops.contains(op),
                "SQLite JSON comparison must not include {op}"
            );
        }
        assert!(json_ops.contains(&"_eq"));
        let string_ops = comparison_op_fields("String", false);
        assert!(string_ops.contains(&"_like"));
        assert!(string_ops.contains(&"_ilike"));
        assert!(!string_ops.contains(&"_contains"));
    }

    #[test]
    fn comparison_op_matrix_postgres_includes_json_ops() {
        assert!(include_postgres_json_comparison_ops(true));
        let json_ops = comparison_op_fields("JSON", true);
        for op in POSTGRES_JSON_COMPARISON_OPS {
            assert!(json_ops.contains(op), "PG JSON comparison missing {op}");
        }
    }

    #[test]
    fn causal_protocol_names_and_lowercase_states_are_frozen_separately() {
        assert_eq!(
            causal_protocol_type_names().collect::<Vec<_>>(),
            [
                DISTRIBUTED_COMMAND_STATE_TYPE,
                DISTRIBUTED_COMMAND_STATUS_TYPE,
            ]
        );
        assert!(!reserved_type_names().any(|name| {
            name == DISTRIBUTED_COMMAND_STATE_TYPE || name == DISTRIBUTED_COMMAND_STATUS_TYPE
        }));
        assert_eq!(
            DISTRIBUTED_COMMAND_STATE_VALUES,
            &[
                "in_progress",
                "succeeded",
                "succeeded_pending_projection",
                "atomic",
                "rejected",
                "projection_failed",
                "expired",
                "unknown",
            ]
        );
        assert!(DISTRIBUTED_COMMAND_STATE_VALUES
            .iter()
            .all(|value| is_valid_graphql_name(value)));
    }
}
