//! Registry and schema-management adapter surface for table schemas.

use std::collections::{BTreeMap, BTreeSet};

use crate::read_model::RelationalReadModel;

use super::{RelationshipDef, RelationshipKind, TableSchema, TableStoreError};

/// Registry of table schemas an adapter should manage.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableSchemaRegistry {
    schemas_by_table: BTreeMap<String, TableSchema>,
    tables_by_model: BTreeMap<String, String>,
}

impl TableSchemaRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<M>(&mut self) -> Result<&mut Self, TableStoreError>
    where
        M: RelationalReadModel,
    {
        self.register_schema(M::schema().clone())
    }

    pub fn register_schema(&mut self, schema: TableSchema) -> Result<&mut Self, TableStoreError> {
        schema.validate()?;

        if self.schemas_by_table.contains_key(&schema.table_name) {
            return Err(TableStoreError::Metadata(format!(
                "table schema registry already contains table `{}`",
                schema.table_name
            )));
        }
        if self.tables_by_model.contains_key(&schema.model_name) {
            return Err(TableStoreError::Metadata(format!(
                "table schema registry already contains model `{}`",
                schema.model_name
            )));
        }

        self.tables_by_model
            .insert(schema.model_name.clone(), schema.table_name.clone());
        self.schemas_by_table
            .insert(schema.table_name.clone(), schema);
        Ok(self)
    }

    pub fn len(&self) -> usize {
        self.schemas_by_table.len()
    }

    pub fn is_empty(&self) -> bool {
        self.schemas_by_table.is_empty()
    }

    pub fn schemas(&self) -> impl Iterator<Item = &TableSchema> {
        self.schemas_by_table.values()
    }

    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.schemas_by_table.keys().map(String::as_str)
    }

    pub fn schema_for_table(&self, table_name: &str) -> Option<&TableSchema> {
        self.schemas_by_table.get(table_name)
    }

    pub fn schema_for_model(&self, model_name: &str) -> Option<&TableSchema> {
        self.tables_by_model
            .get(model_name)
            .and_then(|table_name| self.schema_for_table(table_name))
    }

    pub fn validate(&self) -> Result<(), TableStoreError> {
        let table_names = self
            .schemas_by_table
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        for schema in self.schemas() {
            schema.validate()?;
            self.validate_column_foreign_keys(schema, &table_names)?;
            self.validate_schema_foreign_keys(schema, &table_names)?;
            self.validate_relationships(schema, &table_names)?;
        }

        Ok(())
    }

    fn validate_column_foreign_keys(
        &self,
        schema: &TableSchema,
        table_names: &BTreeSet<String>,
    ) -> Result<(), TableStoreError> {
        for column in &schema.columns {
            let Some(foreign_key) = &column.foreign_key else {
                continue;
            };
            self.validate_foreign_key_target(
                &schema.model_name,
                &schema.table_name,
                &column.column_name,
                &foreign_key.table,
                &foreign_key.column,
                table_names,
            )?;
        }
        Ok(())
    }

    fn validate_schema_foreign_keys(
        &self,
        schema: &TableSchema,
        table_names: &BTreeSet<String>,
    ) -> Result<(), TableStoreError> {
        for foreign_key in &schema.foreign_keys {
            self.validate_foreign_key_target(
                &schema.model_name,
                &schema.table_name,
                "",
                &foreign_key.table,
                &foreign_key.column,
                table_names,
            )?;
        }
        Ok(())
    }

    fn validate_relationships(
        &self,
        schema: &TableSchema,
        table_names: &BTreeSet<String>,
    ) -> Result<(), TableStoreError> {
        for relationship in &schema.relationships {
            let target_schema = self
                .schema_for_model(&relationship.target_model)
                .ok_or_else(|| {
                    TableStoreError::Metadata(format!(
                        "model `{}` relationship `{}` targets unregistered model `{}`",
                        schema.model_name, relationship.field_name, relationship.target_model
                    ))
                })?;

            if let Some(through) = relationship.through.as_deref() {
                if !table_names.contains(through) {
                    return Err(TableStoreError::Metadata(format!(
                        "model `{}` relationship `{}` references unregistered join table `{}`",
                        schema.model_name, relationship.field_name, through
                    )));
                }
            }

            match relationship.kind {
                RelationshipKind::HasMany | RelationshipKind::BelongsTo => {
                    let _ = resolve_direct_join_keys(schema, relationship, target_schema)?;
                }
                RelationshipKind::ManyToMany => {
                    if relationship.references.is_some() {
                        return Err(TableStoreError::Metadata(format!(
                            "model `{}` relationship `{}` references requires a direct relationship",
                            schema.model_name, relationship.field_name,
                        )));
                    }
                    let through = relationship.through.as_deref().ok_or_else(|| {
                        TableStoreError::Metadata(format!(
                            "model `{}` relationship `{}` many-to-many must declare `through`",
                            schema.model_name, relationship.field_name
                        ))
                    })?;
                    let through_schema = self.schema_for_table(through).ok_or_else(|| {
                        TableStoreError::Metadata(format!(
                            "model `{}` relationship `{}` references unavailable join table `{}`",
                            schema.model_name, relationship.field_name, through
                        ))
                    })?;
                    let _ =
                        resolve_m2m_join_keys(schema, relationship, through_schema, target_schema)?;
                }
            }
        }
        Ok(())
    }

    fn validate_foreign_key_target(
        &self,
        model_name: &str,
        table_name: &str,
        column_name: &str,
        target_table: &str,
        target_column: &str,
        table_names: &BTreeSet<String>,
    ) -> Result<(), TableStoreError> {
        if !table_names.contains(target_table) {
            return Err(TableStoreError::Metadata(format!(
                "model `{model_name}` table `{table_name}` references unregistered foreign-key table `{target_table}`"
            )));
        }

        let target_schema = self.schemas_by_table.get(target_table).ok_or_else(|| {
            TableStoreError::Metadata(format!(
                "model `{model_name}` references unavailable foreign-key table `{target_table}`"
            ))
        })?;
        if !target_schema
            .columns
            .iter()
            .any(|column| column.column_name == target_column)
        {
            let local_column = if column_name.is_empty() {
                "schema".to_string()
            } else {
                format!("column `{column_name}`")
            };
            return Err(TableStoreError::Metadata(format!(
                "model `{model_name}` {local_column} references missing foreign-key column `{target_table}.{target_column}`"
            )));
        }

        Ok(())
    }
}

/// One through-table column paired with one end-table primary-key column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinColumnPair {
    pub through_column: String,
    pub end_column: String,
}

impl JoinColumnPair {
    pub fn new(through_column: impl Into<String>, end_column: impl Into<String>) -> Self {
        Self {
            through_column: through_column.into(),
            end_column: end_column.into(),
        }
    }
}

/// Through-table join keys for both ends of a many-to-many relationship.
///
/// `parent` pairs through columns with the source model's primary key.
/// `target` pairs through columns with the target model's primary key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct M2mJoinKeys {
    pub parent: Vec<JoinColumnPair>,
    pub target: Vec<JoinColumnPair>,
}

/// One foreign-key column paired with a referenced key column for a direct join.
/// The referenced key defaults to the primary key, or is selected by `references`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectJoinPair {
    pub foreign_key_column: String,
    pub primary_key_column: String,
}

impl DirectJoinPair {
    pub fn new(
        foreign_key_column: impl Into<String>,
        primary_key_column: impl Into<String>,
    ) -> Self {
        Self {
            foreign_key_column: foreign_key_column.into(),
            primary_key_column: primary_key_column.into(),
        }
    }
}

/// Resolve `has_many` / `belongs_to` join equalities.
///
/// `foreign_key` lists the FK-holding table's columns in the order of `references`,
/// or the other end's primary key when references are omitted. Arity must match.
///
/// - **HasMany**: FK columns live on the target; referenced key is on the source.
/// - **BelongsTo**: FK columns live on the source; referenced key is on the target.
pub fn resolve_direct_join_keys(
    source: &TableSchema,
    relationship: &RelationshipDef,
    target: &TableSchema,
) -> Result<Vec<DirectJoinPair>, TableStoreError> {
    let (fk_schema, pk_schema) = match relationship.kind {
        RelationshipKind::HasMany => (target, source),
        RelationshipKind::BelongsTo => (source, target),
        RelationshipKind::ManyToMany => {
            return Err(TableStoreError::Metadata(format!(
                "model `{}` relationship `{}` must be has_many or belongs_to to resolve a direct join",
                source.model_name, relationship.field_name
            )));
        }
    };
    let explicit = parse_explicit_through_columns(
        source,
        relationship,
        "references",
        relationship.references.as_deref(),
    )?;
    let referenced_columns = if let Some(names) = explicit {
        let columns = names
            .iter()
            .map(|name| {
                column_name_on(pk_schema, name)
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        TableStoreError::Metadata(format!(
                "model `{}` relationship `{}` references unknown column `{name}` on `{}`",
                source.model_name, relationship.field_name, pk_schema.model_name,
            ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if columns.iter().collect::<BTreeSet<_>>().len() != columns.len() {
            return Err(TableStoreError::Metadata(format!(
                "model `{}` relationship `{}` references repeats a physical column",
                source.model_name, relationship.field_name,
            )));
        }
        let unique = (columns.len() == pk_schema.primary_key.columns.len()
            && pk_schema
                .primary_key
                .columns
                .iter()
                .all(|column| columns.contains(column)))
            || pk_schema.indexes.iter().any(|index| {
                index.unique
                    && index.columns.len() == columns.len()
                    && index.columns.iter().all(|column| columns.contains(column))
            });
        if !unique {
            return Err(TableStoreError::Metadata(format!(
                "model `{}` relationship `{}` references must name a declared unique key on `{}`",
                source.model_name, relationship.field_name, pk_schema.model_name,
            )));
        }
        columns
    } else {
        pk_schema.primary_key.columns.clone()
    };
    let pk_columns = &referenced_columns;
    if pk_columns.is_empty() {
        return Err(TableStoreError::Metadata(format!(
            "model `{}` relationship `{}` cannot join because `{}` has an empty primary key",
            source.model_name, relationship.field_name, pk_schema.model_name
        )));
    }
    let Some(fk_names) = parse_explicit_through_columns(
        source,
        relationship,
        "foreign_key",
        relationship.foreign_key.as_deref(),
    )?
    else {
        return Err(TableStoreError::Metadata(format!(
            "model `{}` relationship `{}` must declare a foreign key",
            source.model_name, relationship.field_name
        )));
    };
    if fk_names.len() != pk_columns.len() {
        return Err(TableStoreError::Metadata(format!(
            "model `{}` relationship `{}` foreign_key lists {} column(s) but `{}` {} has {}",
            source.model_name,
            relationship.field_name,
            fk_names.len(),
            pk_schema.model_name,
            if relationship.references.is_some() {
                "referenced key"
            } else {
                "primary key"
            },
            pk_columns.len()
        )));
    }
    let mut pairs = Vec::with_capacity(pk_columns.len());
    let mut seen_foreign_columns = BTreeSet::new();
    for (fk_name, pk_column) in fk_names.iter().zip(pk_columns) {
        let foreign_key_column = column_name_on(fk_schema, fk_name).ok_or_else(|| {
            let side = match relationship.kind {
                RelationshipKind::HasMany => "target",
                _ => "source",
            };
            TableStoreError::Metadata(format!(
                "model `{}` relationship `{}` foreign key `{fk_name}` is not a column on {side} model `{}`",
                source.model_name, relationship.field_name, fk_schema.model_name
            ))
        })?;
        if !seen_foreign_columns.insert(foreign_key_column) {
            return Err(TableStoreError::Metadata(format!(
                "model `{}` relationship `{}` foreign_key repeats a physical column",
                source.model_name, relationship.field_name,
            )));
        }
        let foreign = fk_schema
            .columns
            .iter()
            .find(|column| column.column_name == foreign_key_column)
            .unwrap();
        let referenced = pk_schema
            .columns
            .iter()
            .find(|column| column.column_name == *pk_column)
            .ok_or_else(|| {
                TableStoreError::Metadata(format!("referenced key column `{pk_column}` is missing"))
            })?;
        if foreign.column_type != referenced.column_type || foreign.jsonb != referenced.jsonb {
            return Err(TableStoreError::Metadata(format!(
                "model `{}` relationship `{}` joins incompatible column types for `{foreign_key_column}` and `{pk_column}`",
                source.model_name, relationship.field_name,
            )));
        }
        pairs.push(DirectJoinPair::new(foreign_key_column, pk_column.clone()));
    }
    Ok(pairs)
}

/// Resolve every through-column ↔ end-PK pair for a many-to-many relationship.
///
/// Through columns are either:
/// - the end's primary-key column names (present on the join table), or
/// - `foreign_key` / `target_foreign_key` listing through columns in PK order,
///   same arity as that end's primary key (comma-separated when more than one).
///
/// When those are absent, a unique through-column foreign-key reference to each
/// PK column is accepted. Compile SQL must not invent a second pairing.
pub fn resolve_m2m_join_keys(
    source: &TableSchema,
    relationship: &RelationshipDef,
    through_schema: &TableSchema,
    target_schema: &TableSchema,
) -> Result<M2mJoinKeys, TableStoreError> {
    if relationship.references.is_some() {
        return Err(TableStoreError::Metadata(format!(
            "model `{}` relationship `{}` references requires a direct relationship",
            source.model_name, relationship.field_name,
        )));
    }
    if !matches!(relationship.kind, RelationshipKind::ManyToMany) {
        return Err(TableStoreError::Metadata(format!(
            "model `{}` relationship `{}` must be many-to-many to resolve join keys",
            source.model_name, relationship.field_name
        )));
    }
    Ok(M2mJoinKeys {
        parent: m2m_key_pairs(
            source,
            relationship,
            through_schema,
            source,
            "foreign_key",
            relationship.foreign_key.as_deref(),
        )?,
        target: m2m_key_pairs(
            source,
            relationship,
            through_schema,
            target_schema,
            "target_foreign_key",
            relationship.target_foreign_key.as_deref(),
        )?,
    })
}

fn m2m_key_pairs(
    source: &TableSchema,
    relationship: &RelationshipDef,
    through: &TableSchema,
    end: &TableSchema,
    field: &str,
    explicit: Option<&str>,
) -> Result<Vec<JoinColumnPair>, TableStoreError> {
    let pk_columns = &end.primary_key.columns;
    if pk_columns.is_empty() {
        return Err(TableStoreError::Metadata(format!(
            "model `{}` relationship `{}` cannot join through `{}` because `{}` has an empty primary key",
            source.model_name, relationship.field_name, through.table_name, end.model_name
        )));
    }
    if let Some(through_names) =
        parse_explicit_through_columns(source, relationship, field, explicit)?
    {
        if through_names.len() != pk_columns.len() {
            return Err(TableStoreError::Metadata(format!(
                "model `{}` relationship `{}` {field} lists {} through column(s) but `{}` primary key has {}",
                source.model_name,
                relationship.field_name,
                through_names.len(),
                end.model_name,
                pk_columns.len()
            )));
        }
        let mut pairs = Vec::with_capacity(pk_columns.len());
        for (through_name, end_column) in through_names.iter().zip(pk_columns) {
            let through_column = column_name_on(through, through_name).ok_or_else(|| {
                TableStoreError::Metadata(format!(
                    "model `{}` relationship `{}` {field} `{through_name}` is not a column on join table `{}`",
                    source.model_name, relationship.field_name, through.table_name
                ))
            })?;
            pairs.push(JoinColumnPair::new(through_column, end_column.clone()));
        }
        return Ok(pairs);
    }

    let mut pairs = Vec::with_capacity(pk_columns.len());
    let mut missing = Vec::new();
    for end_column in pk_columns {
        match column_name_on(through, end_column) {
            Some(through_column) => {
                pairs.push(JoinColumnPair::new(through_column, end_column.clone()));
            }
            None => missing.push(end_column.as_str()),
        }
    }
    if missing.is_empty() {
        return Ok(pairs);
    }
    if let Some(inferred) = infer_m2m_pairs_from_foreign_keys(through, end) {
        return Ok(inferred);
    }
    Err(TableStoreError::Metadata(format!(
        "model `{}` relationship `{}` cannot resolve {field} on join table `{}` for `{}` primary key [{}] \
         (missing same-named through columns: {}); declare `{field}` as a PK-order through-column list",
        source.model_name,
        relationship.field_name,
        through.table_name,
        end.model_name,
        pk_columns.join(", "),
        missing.join(", ")
    )))
}

fn parse_explicit_through_columns(
    source: &TableSchema,
    relationship: &RelationshipDef,
    field: &str,
    value: Option<&str>,
) -> Result<Option<Vec<String>>, TableStoreError> {
    let Some(raw) = value else {
        return Ok(None);
    };
    if raw.trim().is_empty() {
        return Err(TableStoreError::Metadata(format!(
            "model `{}` relationship `{}` {field} must not be empty",
            source.model_name, relationship.field_name
        )));
    }
    let mut columns = Vec::new();
    for part in raw.split(',') {
        let name = part.trim();
        if name.is_empty() {
            return Err(TableStoreError::Metadata(format!(
                "model `{}` relationship `{}` {field} lists an empty through column",
                source.model_name, relationship.field_name
            )));
        }
        columns.push(name.to_string());
    }
    Ok(Some(columns))
}

fn infer_m2m_pairs_from_foreign_keys(
    through: &TableSchema,
    end: &TableSchema,
) -> Option<Vec<JoinColumnPair>> {
    let mut pairs = Vec::with_capacity(end.primary_key.columns.len());
    for end_column in &end.primary_key.columns {
        let matches: Vec<&str> = through
            .columns
            .iter()
            .filter(|column| {
                column
                    .foreign_key
                    .as_ref()
                    .is_some_and(|fk| fk.table == end.table_name && fk.column == *end_column)
            })
            .map(|column| column.column_name.as_str())
            .collect();
        let [only] = matches.as_slice() else {
            return None;
        };
        pairs.push(JoinColumnPair::new(*only, end_column.clone()));
    }
    Some(pairs)
}

fn column_name_on<'a>(schema: &'a TableSchema, name: &str) -> Option<&'a str> {
    schema.columns.iter().find_map(|column| {
        if column.column_name == name || column.field_name == name {
            Some(column.column_name.as_str())
        } else {
            None
        }
    })
}

/// Schema lifecycle operations an adapter can support.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableSchemaAdapterCapabilities {
    pub migration_artifacts: bool,
    pub schema_verification: bool,
    pub dev_bootstrap: bool,
}

impl TableSchemaAdapterCapabilities {
    pub fn all() -> Self {
        Self {
            migration_artifacts: true,
            schema_verification: true,
            dev_bootstrap: true,
        }
    }
}

/// Generated or user-consumable migration artifact for registered schemas.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableMigrationArtifact {
    pub name: String,
    pub statements: Vec<String>,
}

impl TableMigrationArtifact {
    pub fn new(name: impl Into<String>, statements: impl IntoIterator<Item = String>) -> Self {
        Self {
            name: name.into(),
            statements: statements.into_iter().collect(),
        }
    }
}

/// Result of verifying registered metadata against an adapter-owned schema.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableSchemaVerification {
    pub issues: Vec<TableSchemaIssue>,
}

impl TableSchemaVerification {
    pub fn verified() -> Self {
        Self::default()
    }

    pub fn is_verified(&self) -> bool {
        self.issues.is_empty()
    }
}

/// Adapter-facing schema verification issue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableSchemaIssue {
    pub table_name: String,
    pub column_name: Option<String>,
    pub kind: TableSchemaIssueKind,
    pub message: String,
}

impl TableSchemaIssue {
    pub fn new(
        table_name: impl Into<String>,
        column_name: Option<impl Into<String>>,
        kind: TableSchemaIssueKind,
        message: impl Into<String>,
    ) -> Self {
        Self {
            table_name: table_name.into(),
            column_name: column_name.map(Into::into),
            kind,
            message: message.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableSchemaIssueKind {
    MissingTable,
    MissingColumn,
    TypeMismatch,
    PrimaryKeyMismatch,
    ForeignKeyMismatch,
    IndexMismatch,
    NullabilityMismatch,
    DefaultMismatch,
    VersionColumnMismatch,
    Unsupported(String),
}

/// Result of an explicit dev/test schema bootstrap operation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableSchemaBootstrap {
    pub bootstrapped_tables: Vec<String>,
}

impl TableSchemaBootstrap {
    pub fn new(bootstrapped_tables: impl IntoIterator<Item = String>) -> Self {
        Self {
            bootstrapped_tables: bootstrapped_tables.into_iter().collect(),
        }
    }
}

/// Adapter contract for schema generation, verification, and dev/test bootstrap.
pub trait TableSchemaAdapter {
    fn schema_capabilities(&self) -> TableSchemaAdapterCapabilities;

    fn generate_migration_artifacts(
        &self,
        _registry: &TableSchemaRegistry,
    ) -> Result<Vec<TableMigrationArtifact>, TableStoreError> {
        Err(TableStoreError::Metadata(
            "read-model schema adapter does not support migration artifact generation".into(),
        ))
    }

    fn verify_schema(
        &self,
        _registry: &TableSchemaRegistry,
    ) -> Result<TableSchemaVerification, TableStoreError> {
        Err(TableStoreError::Metadata(
            "read-model schema adapter does not support startup schema verification".into(),
        ))
    }

    fn bootstrap_schema_for_dev(
        &self,
        _registry: &TableSchemaRegistry,
    ) -> Result<TableSchemaBootstrap, TableStoreError> {
        Err(TableStoreError::Metadata(
            "read-model schema adapter does not support explicit dev/test bootstrap".into(),
        ))
    }
}

#[cfg(test)]
mod m2m_join_key_tests {
    use super::*;
    use crate::table::{
        ColumnType, ForeignKey, PrimaryKey, RelationshipDef, RelationshipKind, TableColumn,
        TableKind, TableSchema,
    };

    fn pk_column(name: &str) -> TableColumn {
        TableColumn {
            primary_key: true,
            ..TableColumn::new(name, name, ColumnType::Text)
        }
    }

    fn column(name: &str) -> TableColumn {
        TableColumn::new(name, name, ColumnType::Text)
    }

    fn schema(
        model: &str,
        table: &str,
        columns: Vec<TableColumn>,
        pk: &[&str],
        relationships: Vec<RelationshipDef>,
    ) -> TableSchema {
        TableSchema {
            model_name: model.into(),
            table_name: table.into(),
            columns,
            primary_key: PrimaryKey::new(pk.iter().copied()),
            version_column: None,
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            relationships,
            kind: TableKind::ReadModel,
        }
    }

    fn labels() -> TableSchema {
        schema(
            "LabelView",
            "labels",
            vec![pk_column("label_id"), column("name")],
            &["label_id"],
            Vec::new(),
        )
    }

    fn projects(rel: RelationshipDef) -> TableSchema {
        schema(
            "ProjectView",
            "projects",
            vec![pk_column("workspace_id"), pk_column("path"), column("kind")],
            &["workspace_id", "path"],
            vec![rel],
        )
    }

    fn project_labels() -> TableSchema {
        schema(
            "ProjectLabel",
            "project_labels",
            vec![
                pk_column("workspace_id"),
                pk_column("path"),
                pk_column("label_id"),
            ],
            &["workspace_id", "path", "label_id"],
            Vec::new(),
        )
    }

    fn labels_rel(foreign_key: Option<&str>, target_foreign_key: Option<&str>) -> RelationshipDef {
        RelationshipDef {
            references: None,
            field_name: "labels".into(),
            kind: RelationshipKind::ManyToMany,
            target_model: "LabelView".into(),
            foreign_key: foreign_key.map(str::to_string),
            through: Some("project_labels".into()),
            target_foreign_key: target_foreign_key.map(str::to_string),
        }
    }

    #[test]
    fn same_named_through_columns_pair_composite_and_single_keys() {
        let source = projects(labels_rel(None, None));
        let keys = resolve_m2m_join_keys(
            &source,
            &source.relationships[0],
            &project_labels(),
            &labels(),
        )
        .unwrap();
        assert_eq!(
            keys.parent,
            vec![
                JoinColumnPair::new("workspace_id", "workspace_id"),
                JoinColumnPair::new("path", "path"),
            ]
        );
        assert_eq!(
            keys.target,
            vec![JoinColumnPair::new("label_id", "label_id")]
        );
    }

    #[test]
    fn explicit_pk_order_list_renames_composite_through_columns() {
        let mut through = project_labels();
        through.columns = vec![
            pk_column("project_workspace_id"),
            pk_column("project_path"),
            pk_column("tag_id"),
        ];
        through.primary_key = PrimaryKey::new(["project_workspace_id", "project_path", "tag_id"]);
        let source = projects(labels_rel(
            Some("project_workspace_id,project_path"),
            Some("tag_id"),
        ));
        let keys =
            resolve_m2m_join_keys(&source, &source.relationships[0], &through, &labels()).unwrap();
        assert_eq!(
            keys.parent,
            vec![
                JoinColumnPair::new("project_workspace_id", "workspace_id"),
                JoinColumnPair::new("project_path", "path"),
            ]
        );
        assert_eq!(keys.target, vec![JoinColumnPair::new("tag_id", "label_id")]);
    }

    #[test]
    fn partial_explicit_foreign_key_does_not_silently_ignore_composite_pk() {
        let source = projects(labels_rel(Some("workspace_id"), Some("label_id")));
        let error = resolve_m2m_join_keys(
            &source,
            &source.relationships[0],
            &project_labels(),
            &labels(),
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("lists 1 through column") && error.contains("primary key has 2"),
            "{error}"
        );
    }

    #[test]
    fn empty_string_foreign_key_is_not_a_missing_sentinel() {
        let source = projects(labels_rel(Some(""), None));
        let error = resolve_m2m_join_keys(
            &source,
            &source.relationships[0],
            &project_labels(),
            &labels(),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("foreign_key must not be empty"), "{error}");
    }

    #[test]
    fn infers_renamed_through_columns_from_column_foreign_keys() {
        let mut through = project_labels();
        through.columns = vec![
            TableColumn {
                primary_key: true,
                foreign_key: Some(ForeignKey::new("projects", "workspace_id")),
                ..TableColumn::new(
                    "project_workspace_id",
                    "project_workspace_id",
                    ColumnType::Text,
                )
            },
            TableColumn {
                primary_key: true,
                foreign_key: Some(ForeignKey::new("projects", "path")),
                ..TableColumn::new("project_path", "project_path", ColumnType::Text)
            },
            TableColumn {
                primary_key: true,
                foreign_key: Some(ForeignKey::new("labels", "label_id")),
                ..TableColumn::new("tag_id", "tag_id", ColumnType::Text)
            },
        ];
        through.primary_key = PrimaryKey::new(["project_workspace_id", "project_path", "tag_id"]);
        let source = projects(labels_rel(None, None));
        let keys =
            resolve_m2m_join_keys(&source, &source.relationships[0], &through, &labels()).unwrap();
        assert_eq!(
            keys.parent,
            vec![
                JoinColumnPair::new("project_workspace_id", "workspace_id"),
                JoinColumnPair::new("project_path", "path"),
            ]
        );
        assert_eq!(keys.target, vec![JoinColumnPair::new("tag_id", "label_id")]);
    }

    fn files_rel(foreign_key: &str) -> RelationshipDef {
        RelationshipDef {
            references: None,
            field_name: "files".into(),
            kind: RelationshipKind::HasMany,
            target_model: "ProjectFileView".into(),
            foreign_key: Some(foreign_key.into()),
            through: None,
            target_foreign_key: None,
        }
    }

    fn project_files() -> TableSchema {
        schema(
            "ProjectFileView",
            "project_files",
            vec![
                pk_column("workspace_id"),
                pk_column("path"),
                pk_column("file_id"),
            ],
            &["workspace_id", "path", "file_id"],
            Vec::new(),
        )
    }

    #[test]
    fn has_many_pairs_composite_parent_key_in_pk_order() {
        let source = projects(files_rel("workspace_id,path"));
        let pairs =
            resolve_direct_join_keys(&source, &source.relationships[0], &project_files()).unwrap();
        assert_eq!(
            pairs,
            vec![
                DirectJoinPair::new("workspace_id", "workspace_id"),
                DirectJoinPair::new("path", "path"),
            ]
        );
    }

    #[test]
    fn belongs_to_pairs_composite_target_key_in_pk_order() {
        let target = projects(files_rel("workspace_id,path"));
        let source = schema(
            "ProjectFileView",
            "project_files",
            vec![
                pk_column("workspace_id"),
                pk_column("path"),
                pk_column("file_id"),
            ],
            &["workspace_id", "path", "file_id"],
            vec![RelationshipDef {
                references: None,
                field_name: "project".into(),
                kind: RelationshipKind::BelongsTo,
                target_model: "ProjectView".into(),
                foreign_key: Some("workspace_id,path".into()),
                through: None,
                target_foreign_key: None,
            }],
        );
        let pairs = resolve_direct_join_keys(&source, &source.relationships[0], &target).unwrap();
        assert_eq!(
            pairs,
            vec![
                DirectJoinPair::new("workspace_id", "workspace_id"),
                DirectJoinPair::new("path", "path"),
            ]
        );
    }

    #[test]
    fn partial_direct_foreign_key_does_not_silently_take_the_first_pk_column() {
        let source = projects(files_rel("workspace_id"));
        let error = resolve_direct_join_keys(&source, &source.relationships[0], &project_files())
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("lists 1 column") && error.contains("primary key has 2"),
            "{error}"
        );
    }

    #[test]
    fn renamed_direct_foreign_key_columns_pair_in_pk_order() {
        let mut files = project_files();
        files.columns = vec![
            pk_column("project_workspace_id"),
            pk_column("project_path"),
            pk_column("file_id"),
        ];
        files.primary_key = PrimaryKey::new(["project_workspace_id", "project_path", "file_id"]);
        let source = projects(files_rel("project_workspace_id,project_path"));
        let pairs = resolve_direct_join_keys(&source, &source.relationships[0], &files).unwrap();
        assert_eq!(
            pairs,
            vec![
                DirectJoinPair::new("project_workspace_id", "workspace_id"),
                DirectJoinPair::new("project_path", "path"),
            ]
        );
    }

    #[test]
    fn direct_join_can_reference_a_composite_candidate_key() {
        let mut target = schema(
            "Object",
            "objects",
            vec![pk_column("id"), column("namespace"), column("oid")],
            &["id"],
            vec![],
        );
        target.indexes.push(crate::table::TableIndex {
            name: None,
            columns: vec!["namespace".into(), "oid".into()],
            unique: true,
        });
        let relation = RelationshipDef {
            references: Some("namespace,oid".into()),
            field_name: "object".into(),
            kind: RelationshipKind::BelongsTo,
            target_model: "Object".into(),
            foreign_key: Some("scope,object_oid".into()),
            through: None,
            target_foreign_key: None,
        };
        let source = schema(
            "Ref",
            "refs",
            vec![pk_column("ref_id"), column("scope"), column("object_oid")],
            &["ref_id"],
            vec![relation.clone()],
        );
        assert_eq!(
            resolve_direct_join_keys(&source, &relation, &target).unwrap(),
            vec![
                DirectJoinPair::new("scope", "namespace"),
                DirectJoinPair::new("object_oid", "oid"),
            ]
        );
        assert_eq!(target.primary_key.columns, vec!["id"]);
        let reverse = RelationshipDef {
            field_name: "refs".into(),
            kind: RelationshipKind::HasMany,
            target_model: "Ref".into(),
            ..relation
        };
        assert_eq!(
            resolve_direct_join_keys(&target, &reverse, &source).unwrap(),
            vec![
                DirectJoinPair::new("scope", "namespace"),
                DirectJoinPair::new("object_oid", "oid"),
            ]
        );
    }

    #[test]
    fn direct_join_rejects_non_unique_candidate_key() {
        let source = projects(files_rel("workspace_id,path"));
        let target = project_files();
        let mut relation = source.relationships[0].clone();
        relation.references = Some("kind".into());
        let error = resolve_direct_join_keys(&source, &relation, &target)
            .unwrap_err()
            .to_string();
        assert!(error.contains("declared unique key"), "{error}");
        relation.references = Some("missing".into());
        assert!(resolve_direct_join_keys(&source, &relation, &target)
            .unwrap_err()
            .to_string()
            .contains("unknown column"));
    }

    #[test]
    fn direct_join_rejects_alias_duplicates_and_incompatible_types() {
        let mut source = projects(files_rel("workspace_id,path"));
        let mut target = project_files();
        let mut relation = source.relationships[0].clone();
        source.columns[0].field_name = "workspace_alias".into();
        relation.references = Some("workspace_alias,workspace_id".into());
        assert!(resolve_direct_join_keys(&source, &relation, &target)
            .unwrap_err()
            .to_string()
            .contains("repeats a physical column"));
        relation.references = Some("workspace_id,path".into());
        target.columns[0].column_type = ColumnType::Integer;
        assert!(resolve_direct_join_keys(&source, &relation, &target)
            .unwrap_err()
            .to_string()
            .contains("incompatible column types"));
    }

    #[test]
    fn programmatic_many_to_many_cannot_silently_ignore_references() {
        let source = projects(labels_rel(None, None));
        let mut relation = source.relationships[0].clone();
        relation.references = Some("path".into());
        let error = resolve_m2m_join_keys(&source, &relation, &source, &labels())
            .unwrap_err()
            .to_string();
        assert!(error.contains("requires a direct relationship"), "{error}");
    }
}
