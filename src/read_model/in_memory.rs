//! InMemoryReadModelStore - HashMap-backed read model store for testing and development.
#![expect(
    clippy::manual_async_fn,
    reason = "async trait impls return impl Future + Send to preserve public Send bounds"
)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, RwLock};

use super::{
    ReadModelIncludeRows, ReadModelLoadGraph, ReadModelLoadRequest, ReadModelQueryCapabilities,
    RelationalReadModel, Versioned,
};
use crate::repository::{ReadModelWritePlanStore, RelationalReadModelQueryStore};
use crate::table::{has_many_join_columns, key_fingerprint, validate_key, validate_row_values};
use crate::table::{
    ExpectedVersion, PatchMode, RelationshipDef, RelationshipKind, RowKey, RowValue, RowValues,
    RowWriteMode, TableAdapterCapabilities, TableCommitOutcome, TableMutation, TableSchema,
    TableSchemaRegistry, TableStoreError, TableWritePlan,
};

#[derive(Clone)]
pub(crate) struct StoredRow {
    pub(crate) values: RowValues,
    pub(crate) version: u64,
}

pub(crate) const INITIAL_MODEL_VERSION: u64 = 1;

/// Return the next optimistic version for a read model row.
///
/// Missing rows start at version 1; existing rows must increment without
/// overflowing so release builds cannot wrap back to 0.
pub(crate) fn next_model_version(
    key: &str,
    current_version: Option<u64>,
) -> Result<u64, TableStoreError> {
    match current_version {
        Some(version) => version.checked_add(1).ok_or_else(|| {
            TableStoreError::Storage(format!("read model version overflow for {key}"))
        }),
        None => Ok(INITIAL_MODEL_VERSION),
    }
}

fn relational_capabilities() -> TableAdapterCapabilities {
    TableAdapterCapabilities::default()
}

pub(crate) fn apply_read_model_write_plan(
    plan: TableWritePlan,
    staged_rows: &mut HashMap<String, StoredRow>,
) -> Result<TableCommitOutcome, TableStoreError> {
    plan.validate_for(&relational_capabilities())?;

    for mutation in plan.mutations {
        match mutation {
            TableMutation::UpsertRow(mutation) => {
                let key = relational_storage_key(&mutation.schema.table_name, &mutation.key);
                let current_version = staged_rows.get(&key).map(|row| row.version);
                validate_row_expected_version(
                    mutation.schema,
                    &mutation.key,
                    &mutation.expected_version,
                    current_version,
                )?;
                if matches!(mutation.mode, RowWriteMode::Insert) && current_version.is_some() {
                    return Err(concurrency_conflict(
                        mutation.schema,
                        &mutation.key,
                        0,
                        current_version.unwrap_or_default(),
                    ));
                }

                let new_version = next_model_version(&key, current_version)?;
                staged_rows.insert(
                    key,
                    StoredRow {
                        values: mutation.values,
                        version: new_version,
                    },
                );
            }
            TableMutation::PatchRow(mutation) => {
                let key = relational_storage_key(&mutation.schema.table_name, &mutation.key);
                let current_version = staged_rows.get(&key).map(|row| row.version);
                validate_row_expected_version(
                    mutation.schema,
                    &mutation.key,
                    &mutation.expected_version,
                    current_version,
                )?;

                match staged_rows.get_mut(&key) {
                    Some(row) => {
                        apply_patch_values_preserving_key(
                            mutation.schema,
                            &mutation.key,
                            &mut row.values,
                            mutation.patch.into_values(),
                        )?;
                        row.version = next_model_version(&key, current_version)?;
                    }
                    None if matches!(mutation.mode, PatchMode::InsertMissing) => {
                        let values = row_values_from_key_and_patch(
                            mutation.schema,
                            &mutation.key,
                            mutation.patch.into_values(),
                        )?;
                        staged_rows.insert(
                            key.clone(),
                            StoredRow {
                                values,
                                version: INITIAL_MODEL_VERSION,
                            },
                        );
                    }
                    None => {
                        return Err(TableStoreError::NotFound {
                            collection: mutation.schema.table_name.clone(),
                            id: key_fingerprint(&mutation.key),
                        });
                    }
                }
            }
            TableMutation::DeleteRow(mutation) => {
                let key = relational_storage_key(&mutation.schema.table_name, &mutation.key);
                let current_version = staged_rows.get(&key).map(|row| row.version);
                validate_row_expected_version(
                    mutation.schema,
                    &mutation.key,
                    &mutation.expected_version,
                    current_version,
                )?;
                staged_rows.remove(&key);
            }
        }
    }

    Ok(TableCommitOutcome::applied())
}

pub(crate) fn relational_storage_key(table_name: &str, key: &RowKey) -> String {
    format!("{}:{}", table_name, key_fingerprint(key))
}

fn row_values_from_key_and_patch(
    schema: &TableSchema,
    key: &RowKey,
    patch_values: RowValues,
) -> Result<RowValues, TableStoreError> {
    let mut values = RowValues::new();
    for (column, value) in key.iter() {
        values.insert(column.to_string(), value.clone());
    }
    apply_patch_values_preserving_key(schema, key, &mut values, patch_values)?;
    validate_row_values(schema, &values, true)?;
    Ok(values)
}

fn apply_patch_values_preserving_key(
    schema: &TableSchema,
    key: &RowKey,
    values: &mut RowValues,
    patch_values: RowValues,
) -> Result<(), TableStoreError> {
    for (column, value) in patch_values {
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
    Ok(())
}

fn validate_row_expected_version(
    schema: &TableSchema,
    key: &RowKey,
    expected_version: &ExpectedVersion,
    current_version: Option<u64>,
) -> Result<(), TableStoreError> {
    match (expected_version, current_version) {
        (ExpectedVersion::Any, _) => Ok(()),
        (ExpectedVersion::Exact(expected), Some(actual)) if expected == &actual => Ok(()),
        (ExpectedVersion::Exact(expected), Some(actual)) => {
            Err(concurrency_conflict(schema, key, *expected, actual))
        }
        (ExpectedVersion::Exact(_), None) => Err(TableStoreError::NotFound {
            collection: schema.table_name.clone(),
            id: key_fingerprint(key),
        }),
        (ExpectedVersion::NotExists, None) => Ok(()),
        (ExpectedVersion::NotExists, Some(actual)) => {
            Err(concurrency_conflict(schema, key, 0, actual))
        }
    }
}

fn concurrency_conflict(
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

/// In-memory read model store backed by a HashMap.
///
/// Clone-friendly via Arc.
#[derive(Clone)]
pub struct InMemoryReadModelStore {
    pub(crate) relational_rows: Arc<RwLock<HashMap<String, StoredRow>>>,
    schema_registry: Arc<RwLock<TableSchemaRegistry>>,
    causal_tables: Arc<RwLock<HashSet<String>>>,
}

impl Default for InMemoryReadModelStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryReadModelStore {
    /// Create a new empty read model store.
    pub fn new() -> Self {
        Self::with_causal_table_marker(Arc::new(RwLock::new(HashSet::new())))
    }

    pub(crate) fn with_causal_table_marker(causal_tables: Arc<RwLock<HashSet<String>>>) -> Self {
        Self {
            relational_rows: Arc::new(RwLock::new(HashMap::new())),
            schema_registry: Arc::new(RwLock::new(TableSchemaRegistry::new())),
            causal_tables,
        }
    }

    /// Register a relational read-model schema for explicit include execution.
    pub fn register_schema<M>(&self) -> Result<(), TableStoreError>
    where
        M: RelationalReadModel,
    {
        let mut registry = self
            .schema_registry
            .write()
            .map_err(|_| TableStoreError::Storage("schema registry lock poisoned".into()))?;
        registry.register::<M>()?;
        Ok(())
    }

    /// Register an already-built relational read-model schema.
    pub fn register_read_model_schema(&self, schema: TableSchema) -> Result<(), TableStoreError> {
        let mut registry = self
            .schema_registry
            .write()
            .map_err(|_| TableStoreError::Storage("schema registry lock poisoned".into()))?;
        registry.register_schema(schema)?;
        Ok(())
    }
}

impl ReadModelWritePlanStore for InMemoryReadModelStore {
    fn read_model_capabilities(&self) -> TableAdapterCapabilities {
        relational_capabilities()
    }

    fn commit_write_plan(
        &self,
        plan: TableWritePlan,
    ) -> impl Future<Output = Result<TableCommitOutcome, TableStoreError>> + Send + '_ {
        async move {
            let mut relational_rows = self
                .relational_rows
                .write()
                .map_err(|_| TableStoreError::Storage("lock poisoned".into()))?;
            let causal_tables = self.causal_tables.read().map_err(|_| {
                TableStoreError::Storage("causal table marker lock poisoned".into())
            })?;
            if let Some(table) = plan
                .mutations
                .iter()
                .map(TableMutation::table_name)
                .find(|table| causal_tables.contains(*table))
            {
                return Err(TableStoreError::CausalWriteRequired {
                    table: table.to_string(),
                });
            }

            let mut staged_rows = relational_rows.clone();
            let outcome = apply_read_model_write_plan(plan, &mut staged_rows)?;

            if outcome.was_applied() {
                *relational_rows = staged_rows;
            }

            Ok(outcome)
        }
    }
}

#[derive(Clone)]
struct IncludeSpec {
    name: String,
    relationship: RelationshipDef,
    target_schema: TableSchema,
}

impl RelationalReadModelQueryStore for InMemoryReadModelStore {
    fn read_model_query_capabilities(&self) -> ReadModelQueryCapabilities {
        ReadModelQueryCapabilities::relationship_includes()
    }

    fn load_graph(
        &self,
        request: ReadModelLoadRequest,
    ) -> impl Future<Output = Result<ReadModelLoadGraph, TableStoreError>> + Send + '_ {
        async move {
            request.validate_for_query_capabilities(&self.read_model_query_capabilities())?;

            let (root_schema, include_specs) = {
                let registry = self.schema_registry.read().map_err(|_| {
                    TableStoreError::Storage("schema registry lock poisoned".into())
                })?;
                resolve_request_schemas(&registry, &request)?
            };
            validate_key(&root_schema, &request.key)?;

            let rows = self
                .relational_rows
                .read()
                .map_err(|_| TableStoreError::Storage("lock poisoned".into()))?;
            let root_storage_key = relational_storage_key(&root_schema.table_name, &request.key);
            let Some(root_row) = rows.get(&root_storage_key) else {
                return Ok(ReadModelLoadGraph::default());
            };
            let root = Versioned {
                data: root_row.values.clone(),
                version: root_row.version,
            };

            let mut includes = BTreeMap::new();
            for spec in include_specs {
                let loaded_rows = load_relationship_rows(&rows, &root_schema, &root.data, &spec)?;
                includes.insert(
                    spec.name,
                    ReadModelIncludeRows {
                        relationship: spec.relationship,
                        target_schema: spec.target_schema,
                        rows: loaded_rows,
                    },
                );
            }

            Ok(ReadModelLoadGraph {
                root: Some(root),
                includes,
            })
        }
    }
}

fn resolve_request_schemas(
    registry: &TableSchemaRegistry,
    request: &ReadModelLoadRequest,
) -> Result<(TableSchema, Vec<IncludeSpec>), TableStoreError> {
    let root_schema = registry
        .schema_for_model(&request.schema.model_name)
        .cloned()
        .or_else(|| {
            if request.includes.is_empty() {
                Some(request.schema.clone())
            } else {
                None
            }
        })
        .ok_or_else(|| {
            TableStoreError::Metadata(format!(
                "read model `{}` is not registered for relationship includes",
                request.schema.model_name
            ))
        })?;
    if root_schema != request.schema {
        return Err(TableStoreError::Metadata(format!(
            "read model `{}` load request does not match registered schema",
            request.schema.model_name
        )));
    }

    let mut include_specs = Vec::with_capacity(request.includes.len());
    for include_name in &request.includes {
        let relationship = root_schema
            .relationships
            .iter()
            .find(|relationship| relationship.field_name == *include_name)
            .ok_or_else(|| {
                TableStoreError::Metadata(format!(
                    "read model `{}` has no relationship `{}`",
                    root_schema.model_name, include_name
                ))
            })?;
        if matches!(relationship.kind, RelationshipKind::ManyToMany) {
            return Err(TableStoreError::Metadata(format!(
                "many-to-many relationship `{}` includes are not supported until join metadata declares source and target keys",
                relationship.field_name
            )));
        }
        let target_schema = registry
            .schema_for_model(&relationship.target_model)
            .ok_or_else(|| {
                TableStoreError::Metadata(format!(
                    "read model `{}` relationship `{}` targets unregistered model `{}`",
                    root_schema.model_name, relationship.field_name, relationship.target_model
                ))
            })?;

        include_specs.push(IncludeSpec {
            name: include_name.clone(),
            relationship: relationship.clone(),
            target_schema: target_schema.clone(),
        });
    }

    Ok((root_schema, include_specs))
}

fn load_relationship_rows(
    rows: &HashMap<String, StoredRow>,
    root_schema: &TableSchema,
    root_row: &RowValues,
    spec: &IncludeSpec,
) -> Result<Vec<Versioned<RowValues>>, TableStoreError> {
    match spec.relationship.kind {
        RelationshipKind::HasMany => load_has_many_rows(rows, root_schema, root_row, spec),
        RelationshipKind::BelongsTo => load_belongs_to_rows(rows, root_schema, root_row, spec),
        RelationshipKind::ManyToMany => Err(TableStoreError::Metadata(format!(
            "many-to-many relationship `{}` includes are not supported yet",
            spec.relationship.field_name
        ))),
    }
}

fn load_has_many_rows(
    rows: &HashMap<String, StoredRow>,
    root_schema: &TableSchema,
    root_row: &RowValues,
    spec: &IncludeSpec,
) -> Result<Vec<Versioned<RowValues>>, TableStoreError> {
    let (target_column, root_column) =
        has_many_join_columns(root_schema, &spec.relationship, &spec.target_schema)?;
    let root_value = root_row.get(&root_column).ok_or_else(|| {
        TableStoreError::Metadata(format!(
            "read model `{}` root row is missing relationship key `{}`",
            root_schema.model_name, root_column
        ))
    })?;
    Ok(rows_matching_column(
        rows,
        &spec.target_schema.table_name,
        &target_column,
        root_value,
    ))
}

fn load_belongs_to_rows(
    rows: &HashMap<String, StoredRow>,
    root_schema: &TableSchema,
    root_row: &RowValues,
    spec: &IncludeSpec,
) -> Result<Vec<Versioned<RowValues>>, TableStoreError> {
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
    Ok(rows_matching_column(
        rows,
        &spec.target_schema.table_name,
        &target_column,
        source_value,
    ))
}

fn rows_matching_column(
    rows: &HashMap<String, StoredRow>,
    table_name: &str,
    column: &str,
    value: &RowValue,
) -> Vec<Versioned<RowValues>> {
    if matches!(value, RowValue::Null) {
        return Vec::new();
    }
    let prefix = format!("{table_name}:");
    let mut matches = rows
        .iter()
        .filter(|(key, row)| key.starts_with(&prefix) && row.values.get(column) == Some(value))
        .map(|(key, row)| {
            (
                key.clone(),
                Versioned {
                    data: row.values.clone(),
                    version: row.version,
                },
            )
        })
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| left.0.cmp(&right.0));
    matches.into_iter().map(|(_, row)| row).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ColumnType, DeleteTableRowMutation, PatchTableRowMutation, PrimaryKey, RowPatch,
        TableColumn, TableRowMutation,
    };

    fn test_row_schema() -> &'static TableSchema {
        static SCHEMA: std::sync::LazyLock<TableSchema> =
            std::sync::LazyLock::new(|| TableSchema {
                model_name: "TestRow".into(),
                table_name: "test_rows".into(),
                columns: vec![TableColumn::new("id", "id", ColumnType::Text)],
                primary_key: PrimaryKey::new(["id"]),
                version_column: None,
                foreign_keys: Vec::new(),
                indexes: Vec::new(),
                relationships: Vec::new(),
                kind: crate::TableKind::ReadModel,
            });
        &SCHEMA
    }

    #[tokio::test]
    async fn relational_write_plan_upserts_rows() {
        let store = InMemoryReadModelStore::new();
        let schema = test_row_schema();
        let key = RowKey::new([("id", RowValue::String("row-1".into()))]);
        let mut values = RowValues::new();
        values.insert("id", RowValue::String("row-1".into()));

        let outcome = store
            .commit_write_plan(TableWritePlan::new(vec![TableMutation::UpsertRow(
                TableRowMutation {
                    schema,
                    key: key.clone(),
                    values,
                    expected_version: ExpectedVersion::Any,
                    mode: RowWriteMode::Upsert,
                },
            )]))
            .await
            .unwrap();
        let row = store
            .relational_rows
            .read()
            .unwrap()
            .get(&relational_storage_key(&schema.table_name, &key))
            .cloned()
            .unwrap();

        assert!(outcome.was_applied());
        assert_eq!(row.version, 1);
        assert_eq!(
            row.values.get("id"),
            Some(&RowValue::String("row-1".into()))
        );
    }

    #[test]
    fn belongs_to_unique_key_matches_values_not_primary_key_and_never_nulls() {
        let mut target = test_row_schema().clone();
        target.columns.push(TableColumn {
            nullable: true,
            ..TableColumn::new("slug", "slug", ColumnType::Text)
        });
        target.indexes.push(crate::TableIndex {
            name: None,
            columns: vec!["slug".into()],
            unique: true,
        });
        let mut source = test_row_schema().clone();
        source.columns.push(TableColumn {
            nullable: true,
            ..TableColumn::new("target_slug", "target_slug", ColumnType::Text)
        });
        let spec = IncludeSpec {
            name: "target".into(),
            target_schema: target.clone(),
            relationship: RelationshipDef {
                field_name: "target".into(),
                kind: RelationshipKind::BelongsTo,
                target_model: target.model_name.clone(),
                foreign_key: Some("target_slug".into()),
                references: Some("slug".into()),
                through: None,
                target_foreign_key: None,
            },
        };
        let mut rows = HashMap::new();
        for (id, slug) in [
            ("opaque", RowValue::String("chosen".into())),
            ("chosen", RowValue::String("other".into())),
            ("null-target", RowValue::Null),
        ] {
            let key = RowKey::new([("id", RowValue::String(id.into()))]);
            let mut values = RowValues::new();
            values.insert("id", RowValue::String(id.into()));
            values.insert("slug", slug);
            rows.insert(
                relational_storage_key(&target.table_name, &key),
                StoredRow { values, version: 1 },
            );
        }
        let mut root = RowValues::new();
        root.insert("target_slug", RowValue::String("chosen".into()));
        let found = load_belongs_to_rows(&rows, &source, &root, &spec).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(
            found[0].data.get("id"),
            Some(&RowValue::String("opaque".into()))
        );
        root.insert("target_slug", RowValue::Null);
        assert!(load_belongs_to_rows(&rows, &source, &root, &spec)
            .unwrap()
            .is_empty());
        root.insert("target_slug", RowValue::String("missing".into()));
        assert!(load_belongs_to_rows(&rows, &source, &root, &spec)
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn relational_write_plan_patches_and_deletes_rows() {
        let store = InMemoryReadModelStore::new();
        let schema = test_row_schema();
        let key = RowKey::new([("id", RowValue::String("row-1".into()))]);
        let mut values = RowValues::new();
        values.insert("id", RowValue::String("row-1".into()));

        store
            .commit_write_plan(TableWritePlan::new(vec![TableMutation::UpsertRow(
                TableRowMutation {
                    schema,
                    key: key.clone(),
                    values,
                    expected_version: ExpectedVersion::Any,
                    mode: RowWriteMode::Upsert,
                },
            )]))
            .await
            .unwrap();
        store
            .commit_write_plan(TableWritePlan::new(vec![TableMutation::PatchRow(
                PatchTableRowMutation {
                    schema,
                    key: key.clone(),
                    patch: RowPatch::new().set("id", RowValue::String("row-1".into())),
                    expected_version: ExpectedVersion::Exact(1),
                    mode: PatchMode::UpdateExisting,
                },
            )]))
            .await
            .unwrap();
        let version = store
            .relational_rows
            .read()
            .unwrap()
            .get(&relational_storage_key(&schema.table_name, &key))
            .unwrap()
            .version;
        assert_eq!(version, 2);

        store
            .commit_write_plan(TableWritePlan::new(vec![TableMutation::DeleteRow(
                DeleteTableRowMutation {
                    schema,
                    key: key.clone(),
                    expected_version: ExpectedVersion::Exact(2),
                },
            )]))
            .await
            .unwrap();
        assert!(!store
            .relational_rows
            .read()
            .unwrap()
            .contains_key(&relational_storage_key(&schema.table_name, &key)));
    }
}
