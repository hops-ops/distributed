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
    column_name_for, has_many_join_columns, key_fingerprint, key_from_row,
    validate_delete_mutation, validate_expected_version, validate_key, validate_patch_mutation,
    validate_row_mutation, validate_row_values,
};
pub use mutation::{
    DeleteTableRowMutation, ExpectedVersion, PatchMode, PatchTableRowMutation, RowPatch,
    RowWriteMode, TableMutation, TableRowMutation,
};
pub use plan::{TableAdapterCapabilities, TableCommitOutcome, TableWritePlan};
pub use registry::{
    resolve_direct_join_keys, resolve_m2m_join_keys, DirectJoinPair, JoinColumnPair, M2mJoinKeys,
    TableMigrationArtifact, TableSchemaAdapter, TableSchemaAdapterCapabilities,
    TableSchemaBootstrap, TableSchemaIssue, TableSchemaIssueKind, TableSchemaRegistry,
    TableSchemaVerification,
};
pub use sql::{
    bootstrap_result as table_schema_bootstrap_result, generate_table_migration_artifacts,
    table_schema_statements, TableSqlDialect, TableSqlSchemaAdapter,
};

/// Opt-in trait for non-read-model types that map to a relational table row.
pub trait TableModel: Clone + Send + Sync + Sized {
    fn table_schema() -> &'static TableSchema;
    fn table_key(&self) -> Result<RowKey, TableStoreError>;
    fn to_table_row(&self) -> Result<RowValues, TableStoreError>;
}

/// Extension methods for registering neutral table models.
pub trait TableSchemaRegistryExt {
    fn register_table<M>(&mut self) -> Result<&mut Self, TableStoreError>
    where
        M: TableModel;
}

impl TableSchemaRegistryExt for TableSchemaRegistry {
    fn register_table<M>(&mut self) -> Result<&mut Self, TableStoreError>
    where
        M: TableModel,
    {
        self.register_schema(M::table_schema().clone())
    }
}
