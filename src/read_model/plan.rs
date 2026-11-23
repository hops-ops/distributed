//! Detached builder that stages read-model mutations into a write plan.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::repository::ReadModelWritePlanStore;
use crate::table::{
    column_name_for, has_many_join_columns, key_fingerprint, key_from_row,
    validate_expected_version, validate_key, DeleteTableRowMutation, ExpectedVersion, PatchMode,
    PatchTableRowMutation, RelationshipDef, RelationshipKind, RowKey, RowPatch, RowValues,
    RowWriteMode, TableMutation, TableRowMutation, TableSchema, TableStoreError, TableWritePlan,
};

use super::{ReadModelLoadRequest, RelationalReadModel, Versioned};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct RowIdentity {
    pub(super) table_name: String,
    pub(super) key: String,
}

#[derive(Clone, Debug)]
struct StagedMutation {
    sequence: u64,
    mutation: TableMutation,
}

/// Detached builder for read-model write plans that are applied at commit.
#[derive(Clone, Debug, Default)]
pub struct ReadModelWritePlanBuilder {
    mutations: Vec<StagedMutation>,
    pub(super) expected_versions: BTreeMap<RowIdentity, u64>,
    next_sequence: u64,
}

impl ReadModelWritePlanBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Framework-only access to the same validated schema path used by every
    /// hand-authored read-model mutation.
    pub(crate) fn projection_validated_schema<M>() -> Result<&'static TableSchema, TableStoreError>
    where
        M: RelationalReadModel,
    {
        validated_schema::<M>()
    }

    /// Framework-only access to the authoritative relationship lookup.
    pub(crate) fn projection_relationship_for<'a>(
        parent_schema: &'a TableSchema,
        relationship_field: &str,
        child_schema: &TableSchema,
    ) -> Result<&'a RelationshipDef, TableStoreError> {
        relationship_for(parent_schema, relationship_field, child_schema)
    }

    /// Framework-only access to delegated foreign-key resolution.
    pub(crate) fn projection_delegated_relationship_columns(
        parent_schema: &TableSchema,
        relationship: &RelationshipDef,
        child_schema: &TableSchema,
    ) -> Result<Vec<(String, String)>, TableStoreError> {
        delegated_relationship_columns(parent_schema, relationship, child_schema)
    }

    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }

    pub fn load<M>(&self, key: RowKey) -> Result<ReadModelLoadRequest, TableStoreError>
    where
        M: RelationalReadModel,
    {
        self.load_with::<M, Vec<String>, String>(key, Vec::new())
    }

    pub fn load_with<M, I, S>(
        &self,
        key: RowKey,
        includes: I,
    ) -> Result<ReadModelLoadRequest, TableStoreError>
    where
        M: RelationalReadModel,
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let schema = validated_schema::<M>()?;
        validate_key(schema, &key)?;
        let includes: Vec<String> = includes.into_iter().map(Into::into).collect();
        for include in &includes {
            if !schema
                .relationships
                .iter()
                .any(|relationship| relationship.field_name == *include)
            {
                return Err(TableStoreError::Metadata(format!(
                    "read model `{}` has no relationship `{}`",
                    schema.model_name, include
                )));
            }
        }

        Ok(ReadModelLoadRequest {
            schema: schema.clone(),
            key,
            includes,
        })
    }

    pub fn track_loaded<M>(
        &mut self,
        versioned: &Versioned<M>,
    ) -> Result<&mut Self, TableStoreError>
    where
        M: RelationalReadModel,
    {
        self.expect_version::<M>(versioned.data.primary_key()?, versioned.version)
    }

    pub fn expect_version<M>(
        &mut self,
        key: RowKey,
        expected_version: u64,
    ) -> Result<&mut Self, TableStoreError>
    where
        M: RelationalReadModel,
    {
        let schema = validated_schema::<M>()?;
        validate_key(schema, &key)?;
        validate_expected_version(&ExpectedVersion::Exact(expected_version), schema)?;
        self.expected_versions.insert(
            RowIdentity {
                table_name: schema.table_name.clone(),
                key: key_fingerprint(&key),
            },
            expected_version,
        );
        Ok(self)
    }

    pub fn insert<M>(&mut self, model: &M) -> Result<&mut Self, TableStoreError>
    where
        M: RelationalReadModel,
    {
        self.stage_full_row(
            model,
            RowWriteMode::Insert,
            Some(ExpectedVersion::NotExists),
        )
    }

    pub fn upsert<M>(&mut self, model: &M) -> Result<&mut Self, TableStoreError>
    where
        M: RelationalReadModel,
    {
        self.stage_full_row(model, RowWriteMode::Upsert, None)
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
        self.stage_related_row(parent, relationship_field, child, RowWriteMode::Insert)
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
        self.stage_related_row(parent, relationship_field, child, RowWriteMode::Upsert)
    }

    pub fn patch<M>(&mut self, key: RowKey, patch: RowPatch) -> Result<&mut Self, TableStoreError>
    where
        M: RelationalReadModel,
    {
        self.stage_patch::<M>(key, patch, PatchMode::UpdateExisting)
    }

    pub fn upsert_patch<M>(
        &mut self,
        key: RowKey,
        patch: RowPatch,
    ) -> Result<&mut Self, TableStoreError>
    where
        M: RelationalReadModel,
    {
        self.stage_patch::<M>(key, patch, PatchMode::InsertMissing)
    }

    pub fn delete<M>(&mut self, key: RowKey) -> Result<&mut Self, TableStoreError>
    where
        M: RelationalReadModel,
    {
        let schema = validated_schema::<M>()?;
        validate_key(schema, &key)?;
        let expected_version = self.expected_for(schema, &key);
        let mutation = DeleteTableRowMutation {
            schema,
            key,
            expected_version,
        };
        self.push(TableMutation::DeleteRow(mutation));
        Ok(self)
    }

    pub fn delete_model<M>(&mut self, model: &M) -> Result<&mut Self, TableStoreError>
    where
        M: RelationalReadModel,
    {
        self.delete::<M>(model.primary_key()?)
    }

    pub fn into_write_plan(self) -> Result<TableWritePlan, TableStoreError> {
        // Precompute each mutation's sort key once; building the formatted key
        // inside the comparator would allocate two Strings per comparison.
        let mut mutations = self
            .mutations
            .into_iter()
            .map(|staged| (staged.mutation.sort_key(), staged))
            .collect::<Vec<_>>();
        mutations.sort_by(|(left_key, left), (right_key, right)| {
            left.mutation
                .operation_rank()
                .cmp(&right.mutation.operation_rank())
                .then_with(|| {
                    left.mutation
                        .dependency_order(&right.mutation)
                        .unwrap_or(Ordering::Equal)
                })
                .then_with(|| left_key.cmp(right_key))
                .then(left.sequence.cmp(&right.sequence))
        });
        let mutations = mutations
            .into_iter()
            .map(|(_, staged)| staged.mutation)
            .collect::<Vec<_>>();
        let plan = TableWritePlan::new(mutations);
        plan.validate()?;
        Ok(plan)
    }

    pub async fn commit<S>(self, store: &S) -> Result<(), TableStoreError>
    where
        S: ReadModelWritePlanStore + ?Sized,
    {
        store.commit_write_plan(self.into_write_plan()?).await
    }

    fn stage_full_row<M>(
        &mut self,
        model: &M,
        mode: RowWriteMode,
        expected_version: Option<ExpectedVersion>,
    ) -> Result<&mut Self, TableStoreError>
    where
        M: RelationalReadModel,
    {
        let schema = validated_schema::<M>()?;
        let key = model.primary_key()?;
        let values = model.to_row()?;
        validate_key(schema, &key)?;
        let expected_version = expected_version.unwrap_or_else(|| self.expected_for(schema, &key));
        let mutation = TableRowMutation {
            schema,
            key,
            values,
            expected_version,
            mode,
        };
        self.push(TableMutation::UpsertRow(mutation));
        Ok(self)
    }

    pub(crate) fn stage_projection_full_row<M>(
        &mut self,
        key: RowKey,
        values: RowValues,
        mode: RowWriteMode,
        expected_version: ExpectedVersion,
    ) -> Result<&mut Self, TableStoreError>
    where
        M: RelationalReadModel,
    {
        let schema = validated_schema::<M>()?;
        validate_key(schema, &key)?;
        let mutation = TableRowMutation {
            schema,
            key,
            values,
            expected_version,
            mode,
        };
        self.push(TableMutation::UpsertRow(mutation));
        Ok(self)
    }

    pub(crate) fn stage_projection_patch<M>(
        &mut self,
        key: RowKey,
        patch: RowPatch,
        mode: PatchMode,
    ) -> Result<&mut Self, TableStoreError>
    where
        M: RelationalReadModel,
    {
        let schema = validated_schema::<M>()?;
        validate_key(schema, &key)?;
        let mutation = PatchTableRowMutation {
            schema,
            key,
            patch,
            expected_version: ExpectedVersion::Any,
            mode,
        };
        self.push(TableMutation::PatchRow(mutation));
        Ok(self)
    }

    pub(crate) fn stage_projection_delete<M>(
        &mut self,
        key: RowKey,
    ) -> Result<&mut Self, TableStoreError>
    where
        M: RelationalReadModel,
    {
        let schema = validated_schema::<M>()?;
        validate_key(schema, &key)?;
        self.push(TableMutation::DeleteRow(DeleteTableRowMutation {
            schema,
            key,
            expected_version: ExpectedVersion::Any,
        }));
        Ok(self)
    }

    pub(crate) fn stage_projection_related_row<P, C>(
        &mut self,
        relationship_field: &str,
        parent_row: &RowValues,
        mut child_row: RowValues,
        mode: RowWriteMode,
        expected_version: ExpectedVersion,
    ) -> Result<&mut Self, TableStoreError>
    where
        P: RelationalReadModel,
        C: RelationalReadModel,
    {
        let parent_schema = validated_schema::<P>()?;
        let child_schema = validated_schema::<C>()?;
        let relationship = relationship_for(parent_schema, relationship_field, child_schema)?;
        populate_delegated_relationship_values(
            parent_schema,
            parent_row,
            relationship,
            child_schema,
            &mut child_row,
        )?;
        let key = key_from_row(child_schema, &child_row)?;
        self.stage_projection_full_row::<C>(key, child_row, mode, expected_version)
    }

    fn stage_related_row<P, C>(
        &mut self,
        parent: &P,
        relationship_field: &str,
        child: &C,
        mode: RowWriteMode,
    ) -> Result<&mut Self, TableStoreError>
    where
        P: RelationalReadModel,
        C: RelationalReadModel,
    {
        let parent_schema = validated_schema::<P>()?;
        let child_schema = validated_schema::<C>()?;
        let relationship = relationship_for(parent_schema, relationship_field, child_schema)?;

        let parent_row = parent.to_row()?;
        let mut child_row = child.to_row()?;
        populate_delegated_relationship_values(
            parent_schema,
            &parent_row,
            relationship,
            child_schema,
            &mut child_row,
        )?;
        let key = key_from_row(child_schema, &child_row)?;
        let expected_version = match mode {
            RowWriteMode::Insert => ExpectedVersion::NotExists,
            RowWriteMode::Upsert => self.expected_for(child_schema, &key),
        };
        let mutation = TableRowMutation {
            schema: child_schema,
            key,
            values: child_row,
            expected_version,
            mode,
        };
        self.push(TableMutation::UpsertRow(mutation));
        Ok(self)
    }

    fn stage_patch<M>(
        &mut self,
        key: RowKey,
        patch: RowPatch,
        mode: PatchMode,
    ) -> Result<&mut Self, TableStoreError>
    where
        M: RelationalReadModel,
    {
        let schema = validated_schema::<M>()?;
        validate_key(schema, &key)?;
        let expected_version = self.expected_for(schema, &key);
        let mutation = PatchTableRowMutation {
            schema,
            key,
            patch,
            expected_version,
            mode,
        };
        self.push(TableMutation::PatchRow(mutation));
        Ok(self)
    }

    pub(crate) fn push(&mut self, mutation: TableMutation) {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.mutations.push(StagedMutation { sequence, mutation });
    }

    fn expected_for(&self, schema: &TableSchema, key: &RowKey) -> ExpectedVersion {
        self.expected_versions
            .get(&RowIdentity {
                table_name: schema.table_name.clone(),
                key: key_fingerprint(key),
            })
            .copied()
            .map(ExpectedVersion::Exact)
            .unwrap_or(ExpectedVersion::Any)
    }
}

pub(crate) fn validated_schema<M>() -> Result<&'static TableSchema, TableStoreError>
where
    M: RelationalReadModel,
{
    let schema = M::schema();
    schema.validate()?;
    Ok(schema)
}

pub(crate) fn relationship_for<'a>(
    parent_schema: &'a TableSchema,
    relationship_field: &str,
    child_schema: &TableSchema,
) -> Result<&'a RelationshipDef, TableStoreError> {
    let relationship = parent_schema
        .relationships
        .iter()
        .find(|relationship| relationship.field_name == relationship_field)
        .ok_or_else(|| {
            TableStoreError::Metadata(format!(
                "read model `{}` has no relationship `{}`",
                parent_schema.model_name, relationship_field
            ))
        })?;
    if relationship.target_model != child_schema.model_name {
        return Err(TableStoreError::Metadata(format!(
            "relationship `{}` targets `{}`, not `{}`",
            relationship.field_name, relationship.target_model, child_schema.model_name
        )));
    }
    Ok(relationship)
}

pub(crate) fn delegated_relationship_columns(
    parent_schema: &TableSchema,
    relationship: &RelationshipDef,
    child_schema: &TableSchema,
) -> Result<Vec<(String, String)>, TableStoreError> {
    if !matches!(relationship.kind, RelationshipKind::HasMany) {
        return Err(TableStoreError::Metadata(format!(
            "relationship `{}` must be has_many to delegate target columns",
            relationship.field_name
        )));
    }
    let mut mappings = Vec::new();
    for column in child_schema
        .columns
        .iter()
        .filter(|column| column.delegated_from.is_some())
    {
        let delegated_from = column.delegated_from.as_deref().unwrap_or_default();
        let Some((model_name, source_name)) = delegated_from.split_once('.') else {
            return Err(TableStoreError::Metadata(format!(
                "read model `{}` delegated column `{}` has invalid source `{}`",
                child_schema.model_name, column.column_name, delegated_from
            )));
        };
        if model_name != parent_schema.model_name {
            continue;
        }
        let source_column = column_name_for(parent_schema, source_name).ok_or_else(|| {
            TableStoreError::Metadata(format!(
                "read model `{}` delegated source `{}` is not a parent column",
                child_schema.model_name, delegated_from
            ))
        })?;
        mappings.push((column.column_name.clone(), source_column));
    }
    if mappings.is_empty() {
        let (child_column, parent_column) =
            has_many_join_columns(parent_schema, relationship, child_schema)?;
        mappings.push((child_column, parent_column));
    }
    Ok(mappings)
}

pub(crate) fn populate_delegated_relationship_values(
    parent_schema: &TableSchema,
    parent_row: &RowValues,
    relationship: &RelationshipDef,
    child_schema: &TableSchema,
    child_row: &mut RowValues,
) -> Result<(), TableStoreError> {
    for (child_column, parent_column) in
        delegated_relationship_columns(parent_schema, relationship, child_schema)?
    {
        let value = parent_row.get(&parent_column).cloned().ok_or_else(|| {
            TableStoreError::Metadata(format!(
                "read model `{}` parent row is missing delegated source column `{}`",
                parent_schema.model_name, parent_column
            ))
        })?;
        child_row.insert(child_column, value);
    }

    Ok(())
}
