//! Transport-neutral command data shapes.
//!
//! Scalar names are stable codec identifiers shared by command validation,
//! fingerprints, and client artifacts. Their historical spellings are retained
//! for wire compatibility; adapters own syntax and representability checks.

use std::any::TypeId;

use crate::read_model::RelationalReadModel;
use crate::table::ColumnType;

/// One field on a command input or output object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandTypeField {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
    pub list: bool,
    /// Whether list elements are nullable. Always `false` for non-list fields.
    pub item_nullable: bool,
    /// Nested object type definition when `type_name` is not a scalar.
    pub nested: Option<Box<CommandTypeDef>>,
}

/// Full type definition for a derive-emitted input or output object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandTypeDef {
    pub name: String,
    pub fields: Vec<CommandTypeField>,
    pub type_id: Option<TypeId>,
}

impl CommandTypeDef {
    pub fn new(name: impl Into<String>, fields: Vec<CommandTypeField>) -> Self {
        Self {
            name: name.into(),
            fields,
            type_id: None,
        }
    }

    pub fn with_type_id(mut self, id: TypeId) -> Self {
        self.type_id = Some(id);
        self
    }

    /// Transitive nested type defs (depth-first, deduped by name).
    pub fn transitive_nested(&self) -> Vec<CommandTypeDef> {
        let mut out = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        self.collect_nested(&mut out, &mut seen);
        out
    }

    fn collect_nested(
        &self,
        out: &mut Vec<CommandTypeDef>,
        seen: &mut std::collections::BTreeSet<String>,
    ) {
        for field in &self.fields {
            if let Some(nested) = &field.nested {
                if seen.insert(nested.name.clone()) {
                    out.push((**nested).clone());
                    nested.collect_nested(out, seen);
                }
            }
        }
    }
}

/// Structural description of the serialized input accepted by a command.
///
/// Prefer deriving [`CommandInput`](crate::CommandInput). Field names follow
/// Serde's deserialization direction. Transport-specific naming restrictions
/// are checked when an adapter exposes the declaration.
pub trait CommandInputType {
    fn command_type() -> CommandTypeDef;
}

/// Structural description of a command result, following Serde serialization.
///
/// Prefer deriving [`CommandOutput`](crate::CommandOutput). Atomic read-model
/// outcomes derive their shape directly from the relational schema instead.
pub trait CommandOutputType {
    fn command_type() -> CommandTypeDef;
}

/// Build a command-output object from the same relational schema that owns
/// the stored read model.
///
/// Atomic command results contain stored columns only. Relationships stay
/// query-time fields and are never invented by a same-transaction row result.
pub(crate) fn read_model_command_type<M>() -> CommandTypeDef
where
    M: RelationalReadModel + 'static,
{
    let schema = M::schema();
    let fields = schema
        .columns
        .iter()
        .filter(|column| !column.skipped)
        .map(|column| {
            let type_name = scalar_type_name(&column.column_type).unwrap_or_else(|| {
                panic!(
                    "read model `{}` column `{}` has no command scalar mapping",
                    schema.model_name, column.column_name
                )
            });
            CommandTypeField {
                name: column.column_name.clone(),
                type_name: type_name.into(),
                nullable: column.nullable,
                list: false,
                item_nullable: false,
                nested: None,
            }
        })
        .collect();
    CommandTypeDef::new(schema.model_name.clone(), fields).with_type_id(TypeId::of::<M>())
}

/// Stable scalar codec name for a relational column in a command result.
pub(crate) fn scalar_type_name(column_type: &ColumnType) -> Option<&'static str> {
    match column_type {
        ColumnType::Text => Some("String"),
        ColumnType::Boolean => Some("Boolean"),
        ColumnType::Integer | ColumnType::UnsignedInteger => Some("BigInt"),
        ColumnType::Float => Some("Float"),
        ColumnType::Json => Some("JSON"),
        ColumnType::Timestamp => Some("Timestamptz"),
        ColumnType::Bytes => Some("Bytea"),
        ColumnType::Unsupported(_) => None,
    }
}
