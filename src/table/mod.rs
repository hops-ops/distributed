//! Neutral table/row primitives shared by read models and operational tables.
//!
//! The read-model ORM introduced these structures first, but they are not
//! inherently read-model concepts: outbox storage, inbox/checkpoint tables, and
//! future operational tables use the same schema, mutation, and row-write
//! vocabulary. This module owns the canonical types; `read_model` builds its
//! typed staging/load surface on top of them.

mod catalog;
mod error;
mod metadata;
mod mutation;
mod plan;
mod registry;
mod sql;

pub use catalog::ReadModelCatalog;
pub use error::TableStoreError;
pub use metadata::{
    ColumnType, ForeignKey, PrimaryKey, RelationshipDef, RelationshipKind, RowKey, RowValue,
    RowValues, TableColumn, TableIndex, TableKind, TableSchema, DEFAULT_TABLE_VERSION_COLUMN,
};
pub(crate) use mutation::{
    belongs_to_join_columns, column_name_for, has_many_join_columns, key_fingerprint, key_from_row,
    validate_delete_mutation, validate_expected_version, validate_key, validate_patch_mutation,
    validate_row_mutation, validate_row_values,
};
pub use mutation::{
    DeleteTableRowMutation, ExpectedVersion, PatchMode, PatchTableRowMutation, RowPatch,
    RowWriteMode, TableMutation, TableRowMutation,
};
pub use plan::TableWritePlan;
pub use registry::{
    resolve_direct_join_keys, resolve_m2m_join_keys, DirectJoinPair, JoinColumnPair, M2mJoinKeys,
    TableMigrationArtifact, TableSchemaBootstrap, TableSchemaRegistry,
};
pub use sql::{
    bootstrap_result as table_schema_bootstrap_result, generate_table_migration_artifacts,
    table_schema_statements, TableSqlDialect,
};
