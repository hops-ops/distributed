//! Staged relational row mutations and the row/key validation helpers shared
//! by the table write-plan builder, the workspace, and storage adapters.

use std::cmp::Ordering;

use serde::Serialize;

use super::{
    RelationshipDef, RelationshipKind, RowKey, RowValue, RowValues, TableSchema, TableStoreError,
};

/// Expected optimistic version carried by a staged table write.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ExpectedVersion {
    /// No optimistic version check is requested.
    #[default]
    Any,
    /// The target row must currently have this version.
    Exact(u64),
    /// The target row must not exist yet.
    NotExists,
}

/// Full-row write behavior for a relational row mutation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RowWriteMode {
    Insert,
    Upsert,
}

/// Sparse patch behavior for a relational row mutation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PatchMode {
    UpdateExisting,
    InsertMissing,
}

/// Sparse column updates for a relational row.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RowPatch {
    values: RowValues,
}

impl RowPatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(mut self, column: impl Into<String>, value: RowValue) -> Self {
        self.values.insert(column, value);
        self
    }

    pub fn set_serde<T: Serialize + ?Sized>(
        mut self,
        column: impl Into<String>,
        value: &T,
    ) -> Result<Self, TableStoreError> {
        self.values.insert_serde(column, value)?;
        Ok(self)
    }

    pub fn get(&self, column: &str) -> Option<&RowValue> {
        self.values.get(column)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &RowValue)> {
        self.values.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn into_values(self) -> RowValues {
        self.values
    }
}

/// Full relational row insert/upsert mutation.
#[derive(Clone, Debug, PartialEq)]
pub struct TableRowMutation {
    pub schema: &'static TableSchema,
    pub key: RowKey,
    pub values: RowValues,
    pub expected_version: ExpectedVersion,
    pub mode: RowWriteMode,
}

/// Sparse relational row patch mutation.
#[derive(Clone, Debug, PartialEq)]
pub struct PatchTableRowMutation {
    pub schema: &'static TableSchema,
    pub key: RowKey,
    pub patch: RowPatch,
    pub expected_version: ExpectedVersion,
    pub mode: PatchMode,
}

/// Relational row delete mutation.
#[derive(Clone, Debug, PartialEq)]
pub struct DeleteTableRowMutation {
    pub schema: &'static TableSchema,
    pub key: RowKey,
    pub expected_version: ExpectedVersion,
}

/// First-pass table write-plan mutation surface.
#[derive(Clone, Debug, PartialEq)]
pub enum TableMutation {
    UpsertRow(TableRowMutation),
    PatchRow(PatchTableRowMutation),
    DeleteRow(DeleteTableRowMutation),
}

impl TableMutation {
    pub fn table_name(&self) -> &str {
        self.schema().table_name.as_str()
    }

    pub fn lock_key(&self) -> String {
        format!("{}:{}", self.table_name(), key_fingerprint(self.key()))
    }

    fn key(&self) -> &RowKey {
        match self {
            TableMutation::UpsertRow(mutation) => &mutation.key,
            TableMutation::PatchRow(mutation) => &mutation.key,
            TableMutation::DeleteRow(mutation) => &mutation.key,
        }
    }

    fn schema(&self) -> &'static TableSchema {
        match self {
            TableMutation::UpsertRow(mutation) => mutation.schema,
            TableMutation::PatchRow(mutation) => mutation.schema,
            TableMutation::DeleteRow(mutation) => mutation.schema,
        }
    }

    /// This mutation's place in the canonical apply order.
    pub(crate) fn write_position(&self, sequence: u64) -> WritePosition {
        match self {
            TableMutation::UpsertRow(row) => {
                WritePosition::full_row(row.schema, &row.key, sequence)
            }
            TableMutation::PatchRow(patch) => {
                WritePosition::patch(patch.schema, &patch.key, sequence)
            }
            TableMutation::DeleteRow(delete) => {
                WritePosition::delete(delete.schema, &delete.key, sequence)
            }
        }
    }

    /// Validate the row, key and version fence against the schema.
    pub(crate) fn validate(&self) -> Result<(), TableStoreError> {
        match self {
            TableMutation::UpsertRow(mutation) => validate_row_mutation(mutation),
            TableMutation::PatchRow(mutation) => validate_patch_mutation(mutation),
            TableMutation::DeleteRow(mutation) => validate_delete_mutation(mutation),
        }
    }
}

/// One staged write's place in the canonical apply order: full rows, then
/// patches, then deletes; a parent row before the child that references it
/// (the child first when both are deletes); then table and key; then the
/// order the writes were staged in.
pub(crate) struct WritePosition {
    schema: &'static TableSchema,
    rank: u8,
    sort_key: String,
    sequence: u64,
}

impl WritePosition {
    const FULL_ROW: u8 = 1;
    const PATCH: u8 = 2;
    const DELETE: u8 = 3;

    pub(crate) fn full_row(schema: &'static TableSchema, key: &RowKey, sequence: u64) -> Self {
        Self::new(schema, Self::FULL_ROW, key, sequence)
    }

    pub(crate) fn patch(schema: &'static TableSchema, key: &RowKey, sequence: u64) -> Self {
        Self::new(schema, Self::PATCH, key, sequence)
    }

    pub(crate) fn delete(schema: &'static TableSchema, key: &RowKey, sequence: u64) -> Self {
        Self::new(schema, Self::DELETE, key, sequence)
    }

    fn new(schema: &'static TableSchema, rank: u8, key: &RowKey, sequence: u64) -> Self {
        // Built once per write; formatting inside the comparator would
        // allocate two Strings per comparison.
        let sort_key = format!("{rank}|{}|{}", schema.table_name, key_fingerprint(key));
        Self {
            schema,
            rank,
            sort_key,
            sequence,
        }
    }

    fn depends_on(&self, other: &Self) -> bool {
        self.schema.columns.iter().any(|column| {
            column
                .foreign_key
                .as_ref()
                .is_some_and(|foreign_key| foreign_key.table == other.schema.table_name)
        })
    }

    fn dependency_order(&self, other: &Self) -> Option<Ordering> {
        let both_delete = self.rank == Self::DELETE && other.rank == Self::DELETE;
        match (self.depends_on(other), other.depends_on(self)) {
            (true, false) if both_delete => Some(Ordering::Less),
            (true, false) => Some(Ordering::Greater),
            (false, true) if both_delete => Some(Ordering::Greater),
            (false, true) => Some(Ordering::Less),
            _ => None,
        }
    }

    fn cmp(&self, other: &Self) -> Ordering {
        self.rank
            .cmp(&other.rank)
            .then_with(|| self.dependency_order(other).unwrap_or(Ordering::Equal))
            .then_with(|| self.sort_key.cmp(&other.sort_key))
            .then(self.sequence.cmp(&other.sequence))
    }
}

/// Sort staged writes into the canonical apply order (see [`WritePosition`]).
pub(crate) fn sort_writes<T>(writes: Vec<T>, position: impl Fn(&T) -> WritePosition) -> Vec<T> {
    let mut positioned = writes
        .into_iter()
        .map(|write| (position(&write), write))
        .collect::<Vec<_>>();
    positioned.sort_by(|(left, _), (right, _)| left.cmp(right));
    positioned.into_iter().map(|(_, write)| write).collect()
}

pub(crate) fn validate_row_mutation(mutation: &TableRowMutation) -> Result<(), TableStoreError> {
    mutation.schema.validate()?;
    validate_key(mutation.schema, &mutation.key)?;
    validate_expected_version(&mutation.expected_version, mutation.schema)?;
    validate_row_values(mutation.schema, &mutation.values, true)
}

pub(crate) fn validate_patch_mutation(
    mutation: &PatchTableRowMutation,
) -> Result<(), TableStoreError> {
    mutation.schema.validate()?;
    validate_key(mutation.schema, &mutation.key)?;
    validate_expected_version(&mutation.expected_version, mutation.schema)?;
    if mutation.patch.is_empty() {
        return Err(TableStoreError::Metadata(format!(
            "model `{}` patch must set at least one column",
            mutation.schema.model_name
        )));
    }
    validate_row_values(mutation.schema, &mutation.patch.values, false)
}

pub(crate) fn validate_delete_mutation(
    mutation: &DeleteTableRowMutation,
) -> Result<(), TableStoreError> {
    mutation.schema.validate()?;
    validate_key(mutation.schema, &mutation.key)?;
    validate_expected_version(&mutation.expected_version, mutation.schema)
}

pub(crate) fn validate_expected_version(
    expected_version: &ExpectedVersion,
    schema: &TableSchema,
) -> Result<(), TableStoreError> {
    if matches!(expected_version, ExpectedVersion::Exact(0)) {
        return Err(TableStoreError::Metadata(format!(
            "model `{}` expected version must be greater than zero",
            schema.model_name
        )));
    }
    Ok(())
}

pub(crate) fn validate_key(schema: &TableSchema, key: &RowKey) -> Result<(), TableStoreError> {
    if key.is_empty() {
        return Err(TableStoreError::Metadata(format!(
            "model `{}` row key cannot be empty",
            schema.model_name
        )));
    }

    for column in &schema.primary_key.columns {
        match key.get(column) {
            Some(RowValue::Null) => {
                return Err(TableStoreError::Metadata(format!(
                    "model `{}` primary-key column `{}` cannot be null",
                    schema.model_name, column
                )));
            }
            Some(_) => {}
            None => {
                return Err(TableStoreError::Metadata(format!(
                    "model `{}` row key is missing primary-key column `{}`",
                    schema.model_name, column
                )));
            }
        }
    }

    for (column, _) in key.iter() {
        if !schema.primary_key.columns.iter().any(|key| key == column) {
            return Err(TableStoreError::Metadata(format!(
                "model `{}` row key includes non-primary-key column `{}`",
                schema.model_name, column
            )));
        }
    }

    Ok(())
}

pub(crate) fn validate_row_values(
    schema: &TableSchema,
    values: &RowValues,
    full_row: bool,
) -> Result<(), TableStoreError> {
    for (column_name, value) in values.iter() {
        let column = schema
            .columns
            .iter()
            .find(|column| column.column_name == column_name)
            .ok_or_else(|| {
                TableStoreError::Metadata(format!(
                    "model `{}` write references missing column `{}`",
                    schema.model_name, column_name
                ))
            })?;

        if matches!(value, RowValue::Null) {
            if column.primary_key {
                return Err(TableStoreError::Metadata(format!(
                    "model `{}` primary-key column `{}` cannot be null",
                    schema.model_name, column.column_name
                )));
            }
            if !column.nullable && !column.has_default {
                return Err(TableStoreError::Metadata(format!(
                    "model `{}` column `{}` is not nullable",
                    schema.model_name, column.column_name
                )));
            }
        }
    }

    if full_row {
        for column in &schema.columns {
            if column.skipped || column.nullable || column.has_default {
                continue;
            }
            if !values.contains_key(&column.column_name) {
                return Err(TableStoreError::Metadata(format!(
                    "model `{}` row is missing required column `{}`",
                    schema.model_name, column.column_name
                )));
            }
        }

        for column in schema
            .columns
            .iter()
            .filter(|column| column.delegated_from.is_some())
        {
            match values.get(&column.column_name) {
                Some(RowValue::Null) | None => {
                    return Err(TableStoreError::Metadata(format!(
                        "model `{}` delegated column `{}` must be populated before write",
                        schema.model_name, column.column_name
                    )));
                }
                Some(_) => {}
            }
        }
    }

    Ok(())
}

pub(crate) fn key_from_row(
    schema: &TableSchema,
    row: &RowValues,
) -> Result<RowKey, TableStoreError> {
    let mut key = RowKey::default();
    for column in &schema.primary_key.columns {
        let value = row.get(column).cloned().ok_or_else(|| {
            TableStoreError::Metadata(format!(
                "model `{}` row is missing primary-key column `{}`",
                schema.model_name, column
            ))
        })?;
        key.insert(column.clone(), value);
    }
    validate_key(schema, &key)?;
    Ok(key)
}

pub(crate) fn column_name_for(schema: &TableSchema, field_or_column: &str) -> Option<String> {
    schema
        .columns
        .iter()
        .find(|column| {
            column.field_name == field_or_column || column.column_name == field_or_column
        })
        .map(|column| column.column_name.clone())
}

/// Single-column `has_many` join used by ORM includes and projection graphs.
///
/// Composite direct joins resolve through `resolve_direct_join_keys`; this
/// helper stays one pair so existing loaders do not AND N predicates.
pub(crate) fn has_many_join_columns(
    root_schema: &TableSchema,
    relationship: &RelationshipDef,
    target_schema: &TableSchema,
) -> Result<(String, String), TableStoreError> {
    if !matches!(relationship.kind, RelationshipKind::HasMany) {
        return Err(TableStoreError::Metadata(format!(
            "relationship `{}` must be has_many to delegate a target foreign key",
            relationship.field_name
        )));
    }
    let pairs =
        super::registry::resolve_direct_join_keys(root_schema, relationship, target_schema)?;
    match pairs.as_slice() {
        [pair] => Ok((
            pair.foreign_key_column.clone(),
            pair.primary_key_column.clone(),
        )),
        _ => Err(TableStoreError::Metadata(format!(
            "relationship `{}` has a composite direct join; single-column has_many helpers cannot load it",
            relationship.field_name
        ))),
    }
}

pub(crate) fn belongs_to_join_columns(
    source: &TableSchema,
    relationship: &RelationshipDef,
    target: &TableSchema,
) -> Result<(String, String), TableStoreError> {
    if !matches!(relationship.kind, RelationshipKind::BelongsTo) {
        return Err(TableStoreError::Metadata(
            "expected a belongs_to relationship".into(),
        ));
    }
    if relationship.references.is_none() && target.primary_key.columns.len() != 1 {
        return Err(TableStoreError::Metadata(format!(
            "belongs_to include `{}` targeting `{}` through `{}` requires a single-column primary key",
            relationship.field_name, target.model_name, relationship.foreign_key.as_deref().unwrap_or("unspecified foreign key"),
        )));
    }
    let pairs = super::registry::resolve_direct_join_keys(source, relationship, target)?;
    match pairs.as_slice() {
        [pair] => Ok((pair.foreign_key_column.clone(), pair.primary_key_column.clone())),
        _ => Err(TableStoreError::Metadata(format!(
            "relationship `{}` has a composite direct join; single-column belongs_to helpers cannot load it",
            relationship.field_name,
        ))),
    }
}

pub(crate) fn key_fingerprint(key: &RowKey) -> String {
    let mut fingerprint = String::new();
    for (column, value) in key.iter() {
        push_fingerprint_part(&mut fingerprint, column);
        push_fingerprint_part(&mut fingerprint, &value_fingerprint(value));
    }
    fingerprint
}

fn push_fingerprint_part(fingerprint: &mut String, part: &str) {
    fingerprint.push_str(&part.len().to_string());
    fingerprint.push(':');
    fingerprint.push_str(part);
    fingerprint.push(';');
}

fn value_fingerprint(value: &RowValue) -> String {
    match value {
        RowValue::Null => "null".into(),
        RowValue::Bool(value) => format!("bool:{value}"),
        RowValue::I64(value) => format!("i64:{value}"),
        RowValue::U64(value) => format!("u64:{value}"),
        RowValue::F64(value) => format!("f64:{value:?}"),
        RowValue::String(value) => format!("string:{value}"),
        RowValue::Bytes(value) => format!("bytes:{value:?}"),
        RowValue::Json(value) => format!(
            "json:{}",
            serde_json::to_string(value).unwrap_or_else(|_| value.to_string())
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{
        ColumnType, ForeignKey, PrimaryKey, RelationshipKind, TableColumn, TableKind,
    };

    #[test]
    fn key_fingerprint_distinguishes_delimiter_collisions() {
        let left = RowKey::new([
            ("a", RowValue::String("x,b=y".into())),
            ("b", RowValue::String("z".into())),
        ]);
        let right = RowKey::new([
            ("a", RowValue::String("x".into())),
            ("b", RowValue::String("y,b=z".into())),
        ]);

        assert_ne!(key_fingerprint(&left), key_fingerprint(&right));
    }

    #[test]
    fn key_fingerprint_distinguishes_row_value_types() {
        let integer = RowKey::new([("id", RowValue::I64(1))]);
        let string = RowKey::new([("id", RowValue::String("1".into()))]);

        assert_ne!(key_fingerprint(&integer), key_fingerprint(&string));
    }

    fn colliding_has_many_schemas() -> (TableSchema, TableSchema, RelationshipDef) {
        let root = TableSchema {
            model_name: "Parent".into(),
            table_name: "parents".into(),
            columns: vec![
                TableColumn {
                    primary_key: true,
                    ..TableColumn::new("id", "id", ColumnType::Text)
                },
                TableColumn::new("parent_id", "parent_id", ColumnType::Text),
            ],
            primary_key: PrimaryKey::new(["id"]),
            version_column: None,
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: TableKind::ReadModel,
        };
        let target = TableSchema {
            model_name: "Child".into(),
            table_name: "children".into(),
            columns: vec![
                TableColumn {
                    primary_key: true,
                    ..TableColumn::new("id", "id", ColumnType::Text)
                },
                TableColumn {
                    foreign_key: Some(ForeignKey::new("parents", "id")),
                    ..TableColumn::new("parent_id", "parent_id", ColumnType::Text)
                },
            ],
            primary_key: PrimaryKey::new(["id"]),
            version_column: None,
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: TableKind::ReadModel,
        };
        let relationship = RelationshipDef {
            references: None,
            field_name: "children".into(),
            kind: RelationshipKind::HasMany,
            target_model: "Child".into(),
            foreign_key: Some("parent_id".into()),
            through: None,
            target_foreign_key: None,
        };
        (root, target, relationship)
    }

    #[test]
    fn has_many_join_uses_the_target_foreign_key_reference_over_a_name_collision() {
        let (root, target, relationship) = colliding_has_many_schemas();
        assert_eq!(
            has_many_join_columns(&root, &relationship, &target).unwrap(),
            ("parent_id".into(), "id".into())
        );
    }

    #[test]
    fn has_many_join_rejects_other_relationship_kinds() {
        let (root, target, mut relationship) = colliding_has_many_schemas();
        relationship.kind = RelationshipKind::BelongsTo;

        let error = has_many_join_columns(&root, &relationship, &target).unwrap_err();

        assert!(
            matches!(error, TableStoreError::Metadata(message) if message.contains("must be has_many"))
        );
    }

    #[test]
    fn has_many_join_rejects_ambiguous_composite_root_fallback() {
        let (mut root, mut target, relationship) = colliding_has_many_schemas();
        root.primary_key = PrimaryKey::new(["id", "parent_id"]);
        target
            .columns
            .iter_mut()
            .find(|column| column.column_name == "parent_id")
            .expect("target foreign-key column")
            .foreign_key = None;

        let error = has_many_join_columns(&root, &relationship, &target).unwrap_err();

        assert!(
            matches!(error, TableStoreError::Metadata(message) if message.contains("lists 1 column") && message.contains("primary key has 2"))
        );
    }
}
