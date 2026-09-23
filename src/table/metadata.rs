use std::collections::{BTreeMap, BTreeSet};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use super::TableStoreError;

pub const DEFAULT_TABLE_VERSION_COLUMN: &str = "_sourced_version";

/// Logical storage type for a relational table column.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnType {
    Text,
    Boolean,
    Integer,
    UnsignedInteger,
    Float,
    Bytes,
    Json,
    Timestamp,
    Unsupported(String),
}

/// A foreign-key declaration from one read-model column to another table column.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignKey {
    pub table: String,
    pub column: String,
}

impl ForeignKey {
    pub fn new(table: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            column: column.into(),
        }
    }
}

/// Primary-key metadata for a relational table.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrimaryKey {
    pub columns: Vec<String>,
}

impl PrimaryKey {
    pub fn new(columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            columns: columns.into_iter().map(Into::into).collect(),
        }
    }
}

/// Runtime primary-key values for one table row.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RowKey {
    pub values: BTreeMap<String, RowValue>,
}

impl RowKey {
    pub fn new(values: impl IntoIterator<Item = (impl Into<String>, RowValue)>) -> Self {
        Self {
            values: values
                .into_iter()
                .map(|(column, value)| (column.into(), value))
                .collect(),
        }
    }

    pub fn insert(&mut self, column: impl Into<String>, value: RowValue) -> Option<RowValue> {
        self.values.insert(column.into(), value)
    }

    pub fn get(&self, column: &str) -> Option<&RowValue> {
        self.values.get(column)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &RowValue)> {
        self.values
            .iter()
            .map(|(column, value)| (column.as_str(), value))
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// Column metadata for a relational table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableColumn {
    pub field_name: String,
    pub column_name: String,
    pub column_type: ColumnType,
    pub nullable: bool,
    pub has_default: bool,
    pub default: Option<String>,
    pub primary_key: bool,
    pub foreign_key: Option<ForeignKey>,
    pub delegated_from: Option<String>,
    pub jsonb: bool,
    pub skipped: bool,
}

impl TableColumn {
    pub fn new(
        field_name: impl Into<String>,
        column_name: impl Into<String>,
        column_type: ColumnType,
    ) -> Self {
        Self {
            field_name: field_name.into(),
            column_name: column_name.into(),
            column_type,
            nullable: false,
            has_default: false,
            default: None,
            primary_key: false,
            foreign_key: None,
            delegated_from: None,
            jsonb: false,
            skipped: false,
        }
    }
}

/// Index or unique-constraint metadata for a relational table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableIndex {
    pub name: Option<String>,
    pub columns: Vec<String>,
    pub unique: bool,
}

impl TableIndex {
    pub fn new(columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            name: None,
            columns: columns.into_iter().map(Into::into).collect(),
            unique: false,
        }
    }
}

/// Relationship category for later workspace/write-plan lowering.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelationshipKind {
    HasMany,
    BelongsTo,
    ManyToMany,
}

/// Relationship metadata for a relational table model.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationshipDef {
    /// Ordered columns of the referenced primary or declared unique key.
    /// Omission selects the primary key; row identity is never changed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub references: Option<String>,
    pub field_name: String,
    pub kind: RelationshipKind,
    pub target_model: String,
    pub foreign_key: Option<String>,
    pub through: Option<String>,
    /// Join-table column referencing the TARGET row (many-to-many).
    ///
    /// When `None`, resolvers infer the unique join column whose column-level FK
    /// targets the target model's table, excluding the source-side `foreign_key`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_foreign_key: Option<String>,
}

/// Discriminator for tables owned by the framework vs read-model projections.
///
/// Operational tables (outbox, inbox, …) are never exposed on the GraphQL query
/// surface; schema-catalog adapters consume only `ReadModel` entries.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum TableKind {
    #[default]
    ReadModel,
    Operational,
}

impl TableKind {
    pub fn is_read_model(&self) -> bool {
        matches!(self, Self::ReadModel)
    }
}

/// Schema metadata for one relational table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub model_name: String,
    pub table_name: String,
    pub columns: Vec<TableColumn>,
    pub primary_key: PrimaryKey,
    pub version_column: Option<String>,
    pub foreign_keys: Vec<ForeignKey>,
    pub indexes: Vec<TableIndex>,
    pub relationships: Vec<RelationshipDef>,
    /// Defaults to [`TableKind::ReadModel`]; skipped when serializing the default
    /// so existing describe-JSON artifacts stay byte-identical on upgrade.
    #[serde(default, skip_serializing_if = "TableKind::is_read_model")]
    pub kind: TableKind,
}

impl TableSchema {
    /// Compare the physical row contract while ignoring query-only relationships.
    ///
    /// Relationship metadata can be composed at a deployment boundary without
    /// changing the model/table identity that projection bindings pin.
    pub fn has_same_storage_contract(&self, other: &Self) -> bool {
        let mut left = self.clone();
        let mut right = other.clone();
        left.relationships.clear();
        right.relationships.clear();
        left == right
    }

    pub fn validate(&self) -> Result<(), TableStoreError> {
        if self.table_name.is_empty() {
            return Err(TableStoreError::Metadata(
                "table schema must declare a table name".into(),
            ));
        }

        let mut columns = BTreeSet::new();
        for column in &self.columns {
            if column.column_name.is_empty() {
                return Err(TableStoreError::Metadata(format!(
                    "model `{}` has a column with an empty name",
                    self.model_name
                )));
            }
            if !columns.insert(column.column_name.as_str()) {
                return Err(TableStoreError::Metadata(format!(
                    "model `{}` declares duplicate column `{}`",
                    self.model_name, column.column_name
                )));
            }
            if let ColumnType::Unsupported(type_name) = &column.column_type {
                return Err(TableStoreError::Metadata(format!(
                    "model `{}` field `{}` has unsupported field shape `{}`",
                    self.model_name, column.field_name, type_name
                )));
            }
            if column.primary_key && column.nullable {
                return Err(TableStoreError::Metadata(format!(
                    "model `{}` primary-key column `{}` must be non-null",
                    self.model_name, column.column_name
                )));
            }
            if let Some(foreign_key) = &column.foreign_key {
                validate_foreign_key(&self.model_name, foreign_key)?;
            }
        }

        if let Some(version_column) = &self.version_column {
            if version_column.is_empty() {
                return Err(TableStoreError::Metadata(format!(
                    "model `{}` declares an empty version column",
                    self.model_name
                )));
            }
            if columns.contains(version_column.as_str()) {
                return Err(TableStoreError::Metadata(format!(
                    "model `{}` version column `{}` conflicts with a mapped column",
                    self.model_name, version_column
                )));
            }
        }

        if self.primary_key.columns.is_empty() {
            return Err(TableStoreError::Metadata(format!(
                "model `{}` must declare at least one primary-key column",
                self.model_name
            )));
        }

        for column in &self.primary_key.columns {
            if !columns.contains(column.as_str()) {
                return Err(TableStoreError::Metadata(format!(
                    "model `{}` primary key references missing column `{}`",
                    self.model_name, column
                )));
            }
            if self
                .columns
                .iter()
                .find(|candidate| candidate.column_name == *column)
                .is_some_and(|candidate| candidate.nullable)
            {
                return Err(TableStoreError::Metadata(format!(
                    "model `{}` primary-key column `{}` must be non-null",
                    self.model_name, column
                )));
            }
        }

        for foreign_key in &self.foreign_keys {
            validate_foreign_key(&self.model_name, foreign_key)?;
        }

        let mut index_names = BTreeSet::new();
        for index in &self.indexes {
            if let Some(name) = index.name.as_deref() {
                if !index_names.insert(name) {
                    return Err(TableStoreError::Metadata(format!(
                        "model `{}` declares duplicate index name `{}`",
                        self.model_name, name
                    )));
                }
            }
            if index.columns.is_empty() {
                return Err(TableStoreError::Metadata(format!(
                    "model `{}` declares an index with no columns",
                    self.model_name
                )));
            }
            for column in &index.columns {
                if !columns.contains(column.as_str()) {
                    return Err(TableStoreError::Metadata(format!(
                        "model `{}` index references missing column `{}`",
                        self.model_name, column
                    )));
                }
            }
        }

        for relationship in &self.relationships {
            if relationship.target_model.is_empty() {
                return Err(TableStoreError::Metadata(format!(
                    "model `{}` relationship `{}` must declare a target model",
                    self.model_name, relationship.field_name
                )));
            }
            let foreign_key = relationship.foreign_key.as_deref();
            match relationship.kind {
                RelationshipKind::HasMany | RelationshipKind::BelongsTo
                    if foreign_key.is_none_or(str::is_empty) =>
                {
                    return Err(TableStoreError::Metadata(format!(
                        "model `{}` relationship `{}` must declare a foreign key",
                        self.model_name, relationship.field_name
                    )));
                }
                RelationshipKind::ManyToMany if foreign_key.is_some_and(str::is_empty) => {
                    return Err(TableStoreError::Metadata(format!(
                        "model `{}` relationship `{}` foreign_key must not be empty",
                        self.model_name, relationship.field_name
                    )));
                }
                RelationshipKind::ManyToMany
                    if relationship
                        .target_foreign_key
                        .as_deref()
                        .is_some_and(str::is_empty) =>
                {
                    return Err(TableStoreError::Metadata(format!(
                        "model `{}` relationship `{}` target_foreign_key must not be empty",
                        self.model_name, relationship.field_name
                    )));
                }
                _ => {}
            }
        }

        Ok(())
    }
}

fn validate_foreign_key(model_name: &str, foreign_key: &ForeignKey) -> Result<(), TableStoreError> {
    if foreign_key.table.is_empty() || foreign_key.column.is_empty() {
        return Err(TableStoreError::Metadata(format!(
            "model `{model_name}` has an invalid foreign-key declaration"
        )));
    }
    Ok(())
}

/// A typed value in a relational table row.
#[derive(Clone, Debug, PartialEq)]
pub enum RowValue {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
}

impl RowValue {
    pub fn from_serde<T: Serialize + ?Sized>(value: &T) -> Result<Self, TableStoreError> {
        let value =
            serde_json::to_value(value).map_err(|err| TableStoreError::Serde(err.to_string()))?;
        Ok(Self::from_json_value(value))
    }

    /// Serialize a `#[readmodel(text)]` value without allowing structured JSON
    /// to enter a SQL text column. This is public only for derive output.
    #[doc(hidden)]
    pub fn from_text_serde<T: Serialize + ?Sized>(
        value: &T,
        nullable: bool,
        column: &str,
    ) -> Result<Self, TableStoreError> {
        match serde_json::to_value(value)
            .map_err(|error| TableStoreError::Serde(error.to_string()))?
        {
            serde_json::Value::String(value) => Ok(Self::String(value)),
            serde_json::Value::Null if nullable => Ok(Self::Null),
            serde_json::Value::Null => Err(TableStoreError::Metadata(format!(
                "#[readmodel(text)] column `{column}` is non-null but its value serialized as null"
            ))),
            value => Err(TableStoreError::Metadata(format!(
                "#[readmodel(text)] column `{column}` must serialize as a JSON string{}; got {}",
                if nullable { " or null" } else { "" },
                match value {
                    serde_json::Value::Bool(_) => "boolean",
                    serde_json::Value::Number(_) => "number",
                    serde_json::Value::Array(_) => "array",
                    serde_json::Value::Object(_) => "object",
                    serde_json::Value::String(_) | serde_json::Value::Null => unreachable!(),
                }
            ))),
        }
    }

    pub fn into_json(self) -> serde_json::Value {
        match self {
            RowValue::Null => serde_json::Value::Null,
            RowValue::Bool(value) => serde_json::Value::Bool(value),
            RowValue::I64(value) => serde_json::Value::Number(value.into()),
            RowValue::U64(value) => serde_json::Value::Number(value.into()),
            RowValue::F64(value) => serde_json::json!(value),
            RowValue::String(value) => serde_json::Value::String(value),
            RowValue::Bytes(value) => serde_json::json!(value),
            RowValue::Json(value) => value,
        }
    }

    fn from_json_value(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => RowValue::Null,
            serde_json::Value::Bool(value) => RowValue::Bool(value),
            serde_json::Value::Number(value) => {
                if let Some(value) = value.as_i64() {
                    RowValue::I64(value)
                } else if let Some(value) = value.as_u64() {
                    RowValue::U64(value)
                } else if let Some(value) = value.as_f64() {
                    RowValue::F64(value)
                } else {
                    RowValue::Json(serde_json::Value::Number(value))
                }
            }
            serde_json::Value::String(value) => RowValue::String(value),
            value @ (serde_json::Value::Array(_) | serde_json::Value::Object(_)) => {
                RowValue::Json(value)
            }
        }
    }
}

/// Column-value map for one relational table row.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RowValues {
    values: BTreeMap<String, RowValue>,
}

impl RowValues {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, column: impl Into<String>, value: RowValue) -> Option<RowValue> {
        self.values.insert(column.into(), value)
    }

    pub fn insert_serde<T: Serialize + ?Sized>(
        &mut self,
        column: impl Into<String>,
        value: &T,
    ) -> Result<Option<RowValue>, TableStoreError> {
        let value = RowValue::from_serde(value)?;
        Ok(self.insert(column, value))
    }

    pub fn get(&self, column: &str) -> Option<&RowValue> {
        self.values.get(column)
    }

    pub fn contains_key(&self, column: &str) -> bool {
        self.values.contains_key(column)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn get_serde<T: DeserializeOwned>(&self, column: &str) -> Result<T, TableStoreError> {
        let value = self.values.get(column).ok_or_else(|| {
            TableStoreError::Metadata(format!("row is missing required column `{column}`"))
        })?;
        serde_json::from_value(value.clone().into_json())
            .map_err(|err| TableStoreError::Serde(err.to_string()))
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &RowValue)> {
        self.values
            .iter()
            .map(|(column, value)| (column.as_str(), value))
    }
}

impl IntoIterator for RowValues {
    type Item = (String, RowValue);
    type IntoIter = std::collections::btree_map::IntoIter<String, RowValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_schema() -> TableSchema {
        TableSchema {
            model_name: "PlayerWeapon".into(),
            table_name: "player_weapons".into(),
            columns: vec![
                TableColumn {
                    primary_key: true,
                    foreign_key: Some(ForeignKey::new("players", "player_id")),
                    delegated_from: Some("Player.player_id".into()),
                    ..TableColumn::new("player_id", "player_id", ColumnType::Text)
                },
                TableColumn {
                    primary_key: true,
                    ..TableColumn::new("weapon_id", "weapon_id", ColumnType::Text)
                },
            ],
            primary_key: PrimaryKey::new(["player_id", "weapon_id"]),
            version_column: Some(DEFAULT_TABLE_VERSION_COLUMN.into()),
            foreign_keys: vec![ForeignKey::new("players", "player_id")],
            indexes: vec![TableIndex::new(["player_id"])],
            relationships: Vec::new(),
            kind: TableKind::ReadModel,
        }
    }

    #[test]
    fn table_kind_deserializes_missing_as_read_model() {
        let json = r#"{
            "model_name": "PlayerWeapon",
            "table_name": "player_weapons",
            "columns": [],
            "primary_key": { "columns": [] },
            "version_column": null,
            "foreign_keys": [],
            "indexes": [],
            "relationships": []
        }"#;
        let schema: TableSchema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.kind, TableKind::ReadModel);
        assert!(schema.target_foreign_key_absent());
    }

    #[test]
    fn table_kind_read_model_skipped_from_json() {
        let schema = valid_schema();
        let value = serde_json::to_value(&schema).unwrap();
        assert!(value.get("kind").is_none());
    }

    #[test]
    fn relationship_target_foreign_key_defaults_absent() {
        let json = r#"{
            "field_name": "tags",
            "kind": "ManyToMany",
            "target_model": "Tag",
            "foreign_key": "post_id",
            "through": "post_tags"
        }"#;
        let rel: RelationshipDef = serde_json::from_str(json).unwrap();
        assert_eq!(rel.target_foreign_key, None);
    }

    impl TableSchema {
        fn target_foreign_key_absent(&self) -> bool {
            self.relationships
                .iter()
                .all(|r| r.target_foreign_key.is_none())
        }
    }

    #[test]
    fn validate_accepts_composite_delegated_key_metadata() {
        let schema = valid_schema();

        schema.validate().unwrap();
    }

    #[test]
    fn storage_contract_comparison_ignores_only_query_relationships() {
        let schema = valid_schema();
        let mut with_relationship = schema.clone();
        with_relationship.relationships.push(RelationshipDef {
            references: None,
            field_name: "player".into(),
            kind: RelationshipKind::BelongsTo,
            target_model: "Player".into(),
            foreign_key: Some("player_id".into()),
            through: None,
            target_foreign_key: None,
        });
        assert!(schema.has_same_storage_contract(&with_relationship));

        let mut different_table = with_relationship.clone();
        different_table.table_name = "other_player_weapons".into();
        assert!(!schema.has_same_storage_contract(&different_table));

        let mut different_type = with_relationship.clone();
        different_type.columns[0].column_type = ColumnType::Integer;
        assert!(!schema.has_same_storage_contract(&different_type));

        let mut different_key = with_relationship.clone();
        different_key.primary_key = PrimaryKey::new(["player_id"]);
        assert!(!schema.has_same_storage_contract(&different_key));

        let mut different_shape = with_relationship;
        different_shape.columns[0].column_name = "renamed_player_id".into();
        assert!(!schema.has_same_storage_contract(&different_shape));
    }

    #[test]
    fn validate_rejects_missing_primary_key() {
        let mut schema = valid_schema();
        schema.primary_key = PrimaryKey::default();

        let err = schema.validate().unwrap_err();

        assert!(
            matches!(err, TableStoreError::Metadata(message) if message.contains("primary-key"))
        );
    }

    #[test]
    fn validate_rejects_unsupported_field_shapes() {
        let mut schema = valid_schema();
        schema.columns.push(TableColumn::new(
            "callback",
            "callback",
            ColumnType::Unsupported("fn()".into()),
        ));

        let err = schema.validate().unwrap_err();

        assert!(
            matches!(err, TableStoreError::Metadata(message) if message.contains("unsupported field shape"))
        );
    }

    #[test]
    fn validate_rejects_relationships_without_foreign_keys() {
        let mut schema = valid_schema();
        schema.relationships.push(RelationshipDef {
            references: None,
            field_name: "weapons".into(),
            kind: RelationshipKind::HasMany,
            target_model: "PlayerWeapon".into(),
            foreign_key: None,
            through: None,
            target_foreign_key: None,
        });

        let err = schema.validate().unwrap_err();

        assert!(
            matches!(err, TableStoreError::Metadata(message) if message.contains("foreign key"))
        );
    }

    #[test]
    fn validate_rejects_duplicate_explicit_index_names() {
        let mut schema = valid_schema();
        schema.indexes = vec![
            TableIndex {
                name: Some("idx_player_weapons_player_id".into()),
                columns: vec!["player_id".into()],
                unique: false,
            },
            TableIndex {
                name: Some("idx_player_weapons_player_id".into()),
                columns: vec!["weapon_id".into()],
                unique: false,
            },
        ];

        let err = schema.validate().unwrap_err();

        assert!(matches!(err, TableStoreError::Metadata(message)
                if message.contains("duplicate index name `idx_player_weapons_player_id`")));
    }

    #[test]
    fn row_values_round_trip_scalar_and_json_values() {
        let mut row = RowValues::new();
        row.insert_serde("name", "Ada").unwrap();
        row.insert_serde("count", &3_i64).unwrap();
        row.insert_serde("payload", &serde_json::json!({"wins": [1, 2]}))
            .unwrap();

        assert_eq!(row.get_serde::<String>("name").unwrap(), "Ada");
        assert_eq!(row.get_serde::<i64>("count").unwrap(), 3);
        assert_eq!(
            row.get_serde::<serde_json::Value>("payload").unwrap(),
            serde_json::json!({"wins": [1, 2]})
        );
    }

    #[test]
    fn text_serde_accepts_only_string_shaped_values_and_nullable_null() {
        #[derive(Serialize)]
        enum UnitStatus {
            Open,
        }
        #[derive(Serialize)]
        enum StructuredStatus {
            Reason { message: String },
        }

        assert_eq!(
            RowValue::from_text_serde(&UnitStatus::Open, false, "status").unwrap(),
            RowValue::String("Open".into())
        );
        assert_eq!(
            RowValue::from_text_serde(&Option::<UnitStatus>::None, true, "status").unwrap(),
            RowValue::Null
        );
        let error = RowValue::from_text_serde(
            &StructuredStatus::Reason {
                message: "why".into(),
            },
            false,
            "status",
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("must serialize as a JSON string; got object"));
    }
}
