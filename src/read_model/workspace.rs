//! Store-bound read-model workspace for load, mutate, sync, commit workflows.

use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;

use crate::repository::{ReadModelWritePlanStore, RelationalReadModelQueryStore};

use super::plan::{populate_delegated_relationship_values, validated_schema, RowIdentity};
use super::{
    ReadModelIncludeRows, ReadModelLoadBuilder, ReadModelWritePlanBuilder, RelationalReadModel,
    RelationalReadModelIncludes, Versioned,
};
use crate::table::{
    key_fingerprint, key_from_row, validate_delete_mutation, validate_key, validate_patch_mutation,
    validate_row_mutation,
};
use crate::table::{
    DeleteTableRowMutation, ExpectedVersion, PatchMode, PatchTableRowMutation, RelationshipDef,
    RelationshipKind, RowKey, RowPatch, RowValues, RowWriteMode, TableMutation, TableRowMutation,
    TableSchema, TableStoreError, TableWritePlan,
};

#[derive(Clone, Debug)]
struct TrackedRowBaseline {
    key: RowKey,
    row: RowValues,
    version: u64,
}

#[derive(Clone, Debug)]
struct TrackedIncludeBaseline {
    relationship: RelationshipDef,
    target_schema: &'static TableSchema,
    rows: BTreeMap<String, TrackedRowBaseline>,
}

#[derive(Clone, Debug)]
struct TrackedModelBaseline {
    root_schema: &'static TableSchema,
    root_key: RowKey,
    root_row: RowValues,
    root_version: u64,
    includes: BTreeMap<String, TrackedIncludeBaseline>,
}

const INITIAL_TRACKED_ROW_VERSION: u64 = 1;

/// Store-bound read-model workspace for load, mutate, sync, commit workflows.
///
/// The mutation/sync/diff surface is store-independent; `load`/`commit`
/// are provided by the async-store impl block below.
pub struct ReadModelWorkspace<'a, S> {
    pub(super) store: &'a S,
    pub(super) writes: ReadModelWritePlanBuilder,
    baselines: Vec<TrackedModelBaseline>,
}

impl<'a, S> ReadModelWorkspace<'a, S> {
    pub fn new(store: &'a S) -> Self {
        Self {
            store,
            writes: ReadModelWritePlanBuilder::new(),
            baselines: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.writes.is_empty()
    }

    pub fn sync<M>(&mut self, model: M) -> Result<&mut Self, TableStoreError>
    where
        M: RelationalReadModel + RelationalReadModelIncludes,
    {
        let schema = validated_schema::<M>()?;
        let key = model.primary_key()?;
        validate_key(schema, &key)?;
        let identity = RowIdentity {
            table_name: schema.table_name.clone(),
            key: key_fingerprint(&key),
        };
        let baseline_index = self
            .baselines
            .iter()
            .position(|baseline| {
                baseline.root_schema.table_name == identity.table_name
                    && key_fingerprint(&baseline.root_key) == identity.key
            })
            .ok_or_else(|| {
                TableStoreError::Metadata(format!(
                    "read model `{}` has no tracked baseline for sync",
                    schema.model_name
                ))
            })?;
        let baseline = self.baselines[baseline_index].clone();
        let current_row = model.to_row()?;

        let root_version = self
            .stage_row_diff(
                schema,
                key.clone(),
                &baseline.root_row,
                &current_row,
                baseline.root_version,
            )?
            .unwrap_or(baseline.root_version);

        let mut refreshed_includes = BTreeMap::new();
        for (include_name, include) in &baseline.includes {
            let current_rows = model.include_rows(include_name)?;
            let refreshed_include =
                self.stage_include_changes(schema, &current_row, include, current_rows)?;
            refreshed_includes.insert(include_name.clone(), refreshed_include);
        }

        self.writes.expected_versions.insert(identity, root_version);
        self.baselines[baseline_index] = TrackedModelBaseline {
            root_schema: schema,
            root_key: key,
            root_row: current_row,
            root_version,
            includes: refreshed_includes,
        };

        Ok(self)
    }

    pub fn upsert<M>(&mut self, model: &M) -> Result<&mut Self, TableStoreError>
    where
        M: RelationalReadModel,
    {
        self.writes.upsert(model)?;
        Ok(self)
    }

    pub fn insert<M>(&mut self, model: &M) -> Result<&mut Self, TableStoreError>
    where
        M: RelationalReadModel,
    {
        self.writes.insert(model)?;
        Ok(self)
    }

    pub fn upsert_related<P, C>(
        &mut self,
        parent: &P,
        relationship_field: &str,
        child: &C,
    ) -> Result<&mut Self, TableStoreError>
    where
        P: RelationalReadModel,
        C: RelationalReadModel,
    {
        self.writes
            .upsert_related(parent, relationship_field, child)?;
        Ok(self)
    }

    pub fn insert_related<P, C>(
        &mut self,
        parent: &P,
        relationship_field: &str,
        child: &C,
    ) -> Result<&mut Self, TableStoreError>
    where
        P: RelationalReadModel,
        C: RelationalReadModel,
    {
        self.writes
            .insert_related(parent, relationship_field, child)?;
        Ok(self)
    }

    pub fn patch<M>(&mut self, key: RowKey, patch: RowPatch) -> Result<&mut Self, TableStoreError>
    where
        M: RelationalReadModel,
    {
        self.writes.patch::<M>(key, patch)?;
        Ok(self)
    }

    pub fn upsert_patch<M>(
        &mut self,
        key: RowKey,
        patch: RowPatch,
    ) -> Result<&mut Self, TableStoreError>
    where
        M: RelationalReadModel,
    {
        self.writes.upsert_patch::<M>(key, patch)?;
        Ok(self)
    }

    pub fn delete<M>(&mut self, key: RowKey) -> Result<&mut Self, TableStoreError>
    where
        M: RelationalReadModel,
    {
        self.writes.delete::<M>(key)?;
        Ok(self)
    }

    pub fn delete_model<M>(&mut self, model: &M) -> Result<&mut Self, TableStoreError>
    where
        M: RelationalReadModel,
    {
        self.writes.delete_model(model)?;
        Ok(self)
    }

    pub fn into_write_plan(self) -> Result<TableWritePlan, TableStoreError> {
        self.writes.into_write_plan()
    }

    /// Seed the ordinary graph-diff engine from a framework-coherent snapshot.
    ///
    /// This is crate-private so causal projector wrappers can reuse the exact
    /// sync semantics without exposing the store-bound commit terminal.
    pub(crate) fn track_graph<M>(
        &mut self,
        root: Versioned<RowValues>,
        includes: BTreeMap<String, ReadModelIncludeRows>,
    ) -> Result<(), TableStoreError>
    where
        M: RelationalReadModel + RelationalReadModelIncludes,
    {
        let schema = M::schema();
        let root_key = key_from_row(schema, &root.data)?;
        let root_identity = RowIdentity {
            table_name: schema.table_name.clone(),
            key: key_fingerprint(&root_key),
        };
        self.writes
            .expected_versions
            .insert(root_identity, root.version);

        let mut tracked_includes = BTreeMap::new();
        for (include_name, include_rows) in includes {
            let target_schema = M::include_target_schema(&include_name)?;
            let mut rows = BTreeMap::new();
            for row in include_rows.rows {
                let key = key_from_row(target_schema, &row.data)?;
                rows.insert(
                    key_fingerprint(&key),
                    TrackedRowBaseline {
                        key,
                        row: row.data,
                        version: row.version,
                    },
                );
            }
            tracked_includes.insert(
                include_name,
                TrackedIncludeBaseline {
                    relationship: include_rows.relationship,
                    target_schema,
                    rows,
                },
            );
        }

        let fingerprint = key_fingerprint(&root_key);
        self.baselines.retain(|baseline| {
            baseline.root_schema.table_name != schema.table_name
                || key_fingerprint(&baseline.root_key) != fingerprint
        });
        self.baselines.push(TrackedModelBaseline {
            root_schema: schema,
            root_key,
            root_row: root.data,
            root_version: root.version,
            includes: tracked_includes,
        });
        Ok(())
    }

    fn stage_include_changes(
        &mut self,
        root_schema: &TableSchema,
        root_row: &RowValues,
        baseline: &TrackedIncludeBaseline,
        current_rows: Vec<RowValues>,
    ) -> Result<TrackedIncludeBaseline, TableStoreError> {
        if matches!(baseline.relationship.kind, RelationshipKind::BelongsTo)
            && current_rows.len() > 1
        {
            return Err(TableStoreError::Metadata(format!(
                "belongs_to relationship `{}` can sync at most one related row",
                baseline.relationship.field_name
            )));
        }

        let mut current_fingerprints = BTreeSet::new();
        let mut refreshed_rows = BTreeMap::new();
        for mut current_row in current_rows {
            match baseline.relationship.kind {
                RelationshipKind::HasMany => populate_delegated_relationship_values(
                    root_schema,
                    root_row,
                    &baseline.relationship,
                    baseline.target_schema,
                    &mut current_row,
                )?,
                RelationshipKind::BelongsTo => {}
                RelationshipKind::ManyToMany => {
                    return Err(TableStoreError::Metadata(format!(
                        "many-to-many relationship `{}` includes are not supported yet",
                        baseline.relationship.field_name
                    )));
                }
            }

            let key = key_from_row(baseline.target_schema, &current_row)?;
            let fingerprint = key_fingerprint(&key);
            current_fingerprints.insert(fingerprint.clone());
            if let Some(loaded) = baseline.rows.get(&fingerprint) {
                let version = self
                    .stage_row_diff(
                        baseline.target_schema,
                        loaded.key.clone(),
                        &loaded.row,
                        &current_row,
                        loaded.version,
                    )?
                    .unwrap_or(loaded.version);
                refreshed_rows.insert(
                    fingerprint,
                    TrackedRowBaseline {
                        key,
                        row: current_row,
                        version,
                    },
                );
            } else {
                self.stage_upsert_row(baseline.target_schema, key.clone(), current_row.clone())?;
                refreshed_rows.insert(
                    fingerprint,
                    TrackedRowBaseline {
                        key,
                        row: current_row,
                        version: INITIAL_TRACKED_ROW_VERSION,
                    },
                );
            }
        }

        // `sync` makes storage match the struct: an owned `has_many` child
        // dropped from the loaded collection is deleted. `belongs_to` clears never
        // delete the target, which is the owner that other rows may reference.
        if matches!(baseline.relationship.kind, RelationshipKind::HasMany) {
            for (fingerprint, loaded) in &baseline.rows {
                if !current_fingerprints.contains(fingerprint) {
                    self.stage_delete_row(
                        baseline.target_schema,
                        loaded.key.clone(),
                        loaded.version,
                    )?;
                }
            }
        } else {
            for (fingerprint, loaded) in &baseline.rows {
                if !current_fingerprints.contains(fingerprint) {
                    refreshed_rows.insert(fingerprint.clone(), loaded.clone());
                }
            }
        }

        Ok(TrackedIncludeBaseline {
            relationship: baseline.relationship.clone(),
            target_schema: baseline.target_schema,
            rows: refreshed_rows,
        })
    }

    fn stage_row_diff(
        &mut self,
        schema: &'static TableSchema,
        key: RowKey,
        before: &RowValues,
        after: &RowValues,
        expected_version: u64,
    ) -> Result<Option<u64>, TableStoreError> {
        let patch = diff_rows(before, after);
        if patch.is_empty() {
            return Ok(None);
        }
        let next_version = next_tracked_version(schema, &key, expected_version)?;

        let mutation = PatchTableRowMutation {
            schema,
            key,
            patch,
            expected_version: ExpectedVersion::Exact(expected_version),
            mode: PatchMode::UpdateExisting,
        };
        validate_patch_mutation(&mutation)?;
        self.writes.push(TableMutation::PatchRow(mutation));
        Ok(Some(next_version))
    }

    fn stage_upsert_row(
        &mut self,
        schema: &'static TableSchema,
        key: RowKey,
        values: RowValues,
    ) -> Result<(), TableStoreError> {
        let mutation = TableRowMutation {
            schema,
            key,
            values,
            expected_version: ExpectedVersion::Any,
            mode: RowWriteMode::Upsert,
        };
        validate_row_mutation(&mutation)?;
        self.writes.push(TableMutation::UpsertRow(mutation));
        Ok(())
    }

    fn stage_delete_row(
        &mut self,
        schema: &'static TableSchema,
        key: RowKey,
        expected_version: u64,
    ) -> Result<(), TableStoreError> {
        let mutation = DeleteTableRowMutation {
            schema,
            key,
            expected_version: ExpectedVersion::Exact(expected_version),
        };
        validate_delete_mutation(&mutation)?;
        self.writes.push(TableMutation::DeleteRow(mutation));
        Ok(())
    }
}

impl<'a, S> ReadModelWorkspace<'a, S>
where
    S: ReadModelWritePlanStore + RelationalReadModelQueryStore,
{
    /// Begin a tracked load against the asynchronous store traits.
    pub fn load<M>(&mut self, key: RowKey) -> ReadModelLoadBuilder<'_, 'a, S, M>
    where
        M: RelationalReadModel + RelationalReadModelIncludes,
    {
        ReadModelLoadBuilder {
            unit: self,
            key,
            includes: Vec::new(),
            _marker: PhantomData,
        }
    }

    /// Commit the staged write plan through the asynchronous store.
    pub async fn commit(self) -> Result<(), TableStoreError> {
        self.writes.commit(self.store).await
    }
}

/// Extension trait that starts a tracked read-model workspace from an async store.
pub trait ReadModelWorkspaceExt:
    ReadModelWritePlanStore + RelationalReadModelQueryStore + Sized
{
    fn workspace(&self) -> ReadModelWorkspace<'_, Self> {
        ReadModelWorkspace::new(self)
    }
}

impl<S> ReadModelWorkspaceExt for S where S: ReadModelWritePlanStore + RelationalReadModelQueryStore {}

fn diff_rows(before: &RowValues, after: &RowValues) -> RowPatch {
    let mut patch = RowPatch::new();
    for (column, value) in after.iter() {
        if before.get(column) != Some(value) {
            patch = patch.set(column.to_string(), value.clone());
        }
    }
    patch
}

fn next_tracked_version(
    schema: &TableSchema,
    key: &RowKey,
    current_version: u64,
) -> Result<u64, TableStoreError> {
    current_version.checked_add(1).ok_or_else(|| {
        TableStoreError::Storage(format!(
            "read model version overflow for {}:{}",
            schema.table_name,
            key_fingerprint(key)
        ))
    })
}
