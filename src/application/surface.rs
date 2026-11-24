//! A selected surface in an application manifest: its id, its contract and
//! the fingerprint of both. The contract is compiled once from the authorized
//! Surface IR and stored once; the module and application inventories of
//! models and projections are read from it, never stored beside it.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::command::{CommandTypeField, CommandTypeSpec};
use super::error::ApplicationResult;
use super::identity::{canonical_json, sha256_fingerprint, LogicalId};
use super::module::{ModelFieldSpec, ModelRelationshipSpec, ModelSpec, ProjectionSpec};
use crate::graphql::surface::{
    RelField, RootField, RootKind, Surface, SurfaceArgument, SurfaceArgumentKind, SurfaceCommand,
    SurfaceCommandShape, SurfaceRelationshipKeys, SurfaceRowPolicy, SurfaceSelection,
    SurfaceTypeDef,
};

/// One selected surface: its contract and the fingerprint of its id and
/// contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SurfaceSpec {
    pub id: String,
    pub contract: SurfaceContract,
    pub fingerprint: String,
}

/// Everything that affects how a selected surface behaves, compiled from the
/// authorized Surface IR: who it is for, its models, roots, commands and
/// projections. Nothing about where it runs (no SQL dialect, no service
/// routes). It is the identity input of SDL and client compilation and is
/// never reconstructed from tables.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SurfaceContract {
    pub selection: SurfaceSelectionSpec,
    pub aggregates: bool,
    pub subscriptions: bool,
    pub default_limit: u64,
    pub max_limit: u64,
    pub comparison_ops: BTreeMap<String, Vec<String>>,
    /// Sorted by id.
    pub models: Vec<ModelSpec>,
    /// Sorted by (operation, name).
    pub roots: Vec<SurfaceRootSpec>,
    /// Sorted by id.
    pub commands: Vec<SurfaceCommandSpec>,
    pub commands_attached: bool,
    /// Sorted by id.
    pub projections: Vec<ProjectionSpec>,
    pub projectors_attached: bool,
    /// Canonical external read source declarations
    /// (`ExternalReadSource::contract`) by section, in attachment order.
    /// Absent when empty.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub external: BTreeMap<String, Vec<serde_json::Value>>,
}

/// Who a surface was selected for.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SurfaceSelectionSpec {
    /// The whole catalog, unfiltered by role.
    Catalog,
    /// One role's view.
    Role { name: String },
    /// A named application: the roles it serves (sorted) and the roles whose
    /// intersection shapes its schema (sorted, a subset).
    Application {
        name: String,
        eligible_roles: Vec<String>,
        schema_roles: Vec<String>,
    },
}

impl SurfaceSelectionSpec {
    /// Roles that may use the surface (none for the catalog).
    pub fn eligible_roles(&self) -> &[String] {
        match self {
            Self::Catalog => &[],
            Self::Role { name } => std::slice::from_ref(name),
            Self::Application { eligible_roles, .. } => eligible_roles,
        }
    }

    /// Roles whose grants shape the schema (none for the catalog).
    pub fn schema_roles(&self) -> &[String] {
        match self {
            Self::Catalog => &[],
            Self::Role { name } => std::slice::from_ref(name),
            Self::Application { schema_roles, .. } => schema_roles,
        }
    }
}

/// A filter, order, pagination or key argument of a root or relationship.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SurfaceArgumentSpec {
    pub name: String,
    pub kind: String,
    pub type_name: String,
    pub nullable: bool,
    pub list: bool,
}

/// The aggregate field of a relationship.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SurfaceAggregateSpec {
    pub name: String,
    pub type_name: String,
    pub arguments: Vec<SurfaceArgumentSpec>,
    pub dependencies: Vec<String>,
}

/// A query or subscription root field.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SurfaceRootSpec {
    pub operation: String,
    pub name: String,
    pub kind: String,
    pub object: String,
    pub model: String,
    pub dependencies: Vec<String>,
    pub arguments: Vec<SurfaceArgumentSpec>,
    pub default_limit: Option<u64>,
    pub max_limit: Option<u64>,
}

/// A command as the surface exposes it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SurfaceCommandSpec {
    pub id: String,
    pub field_name: String,
    pub roles: Vec<String>,
    pub input: Option<CommandTypeSpec>,
    pub output: Option<CommandTypeSpec>,
    pub consistency: crate::command::CommandConsistency,
    pub defaults: serde_json::Value,
    pub projection_contract: serde_json::Value,
    pub direct_projection: Option<serde_json::Value>,
    pub projected_model: Option<String>,
}

impl SurfaceCommandSpec {
    /// The previews the command applies: the `previews` of its projection
    /// contract.
    pub fn applies(&self) -> serde_json::Value {
        self.projection_contract["previews"].clone()
    }
}

impl SurfaceSpec {
    /// Compile the already-authorized Surface IR without constructing a
    /// repository, service, lock manager, or handler mount.
    pub fn from_surface(id: impl Into<String>, surface: &Surface) -> ApplicationResult<Self> {
        let id = LogicalId::try_new("surface", id)?.into_string();
        let mut spec = Self {
            id,
            contract: SurfaceContract::from_surface(surface)?,
            fingerprint: String::new(),
        };
        spec.fingerprint = sha256_fingerprint(&spec.canonical_bytes()?);
        Ok(spec)
    }

    /// Canonical identity material with the derived fingerprint removed.
    pub fn canonical_bytes(&self) -> ApplicationResult<Vec<u8>> {
        let mut value = serde_json::to_value(self)?;
        if let serde_json::Value::Object(fields) = &mut value {
            fields.insert(
                "fingerprint".into(),
                serde_json::Value::String(String::new()),
            );
        }
        serde_json::to_vec(&canonical_json(&value)).map_err(Into::into)
    }
}

impl SurfaceContract {
    fn from_surface(surface: &Surface) -> ApplicationResult<Self> {
        let selection = match &surface.selection {
            SurfaceSelection::Catalog => SurfaceSelectionSpec::Catalog,
            SurfaceSelection::Role { name } => SurfaceSelectionSpec::Role { name: name.clone() },
            SurfaceSelection::Application {
                name,
                eligible_roles,
                schema_roles,
            } => SurfaceSelectionSpec::Application {
                name: name.clone(),
                eligible_roles: sorted(eligible_roles.clone()),
                schema_roles: sorted(schema_roles.clone()),
            },
        };

        let mut models = surface
            .models
            .values()
            .map(|model| {
                let mut spec = ModelSpec {
                    id: LogicalId::try_new("model", model.model_name.clone())?.into_string(),
                    table: model.table_name.clone(),
                    object: model.object_name.clone(),
                    fields: model
                        .columns
                        .iter()
                        .map(|field| ModelFieldSpec {
                            name: field.name.clone(),
                            scalar: field.scalar.clone(),
                            nullable: field.nullable,
                        })
                        .collect(),
                    primary_key: model.primary_key.clone(),
                    relationships: model.relationships.iter().map(relationship).collect(),
                    row_policy: row_policy(&model.row_policy),
                    role_limit: model.role_limit,
                    aggregations: model.aggregations,
                    fingerprint: String::new(),
                };
                spec.fields
                    .sort_by(|left, right| left.name.cmp(&right.name));
                spec.primary_key.sort();
                spec.relationships
                    .sort_by(|left, right| left.name.cmp(&right.name));
                spec.refresh_fingerprint()?;
                Ok(spec)
            })
            .collect::<ApplicationResult<Vec<_>>>()?;
        models.sort_by(|left, right| left.id.cmp(&right.id));

        let mut roots = surface
            .query_fields
            .iter()
            .map(|root| root_spec("query", root))
            .chain(
                surface
                    .subscription_fields
                    .iter()
                    .map(|root| root_spec("subscription", root)),
            )
            .collect::<Vec<_>>();
        roots.sort_by(|left, right| {
            (left.operation.as_str(), left.name.as_str())
                .cmp(&(right.operation.as_str(), right.name.as_str()))
        });

        let mut commands = surface
            .commands
            .iter()
            .map(command_spec)
            .collect::<ApplicationResult<Vec<_>>>()?;
        commands.sort_by(|left, right| left.id.cmp(&right.id));

        let mut projections = surface
            .projectors
            .iter()
            .map(ProjectionSpec::from_owner)
            .collect::<ApplicationResult<Vec<_>>>()?;
        projections.sort_by(|left, right| left.id.cmp(&right.id));

        Ok(Self {
            selection,
            aggregates: surface.aggregates,
            subscriptions: surface.subscriptions,
            default_limit: surface.default_limit,
            max_limit: surface.max_limit,
            comparison_ops: surface.comparison_ops.clone(),
            models,
            roots,
            commands,
            commands_attached: surface.commands_attached,
            projections,
            projectors_attached: surface.projectors_attached,
            external: surface.external.iter().fold(
                BTreeMap::<String, Vec<serde_json::Value>>::new(),
                |mut sections, source| {
                    sections
                        .entry(source.section().to_string())
                        .or_default()
                        .push(source.contract());
                    sections
                },
            ),
        })
    }
}

fn sorted(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values.dedup();
    values
}

fn root_spec(operation: &str, root: &RootField) -> SurfaceRootSpec {
    SurfaceRootSpec {
        operation: operation.into(),
        name: root.name.clone(),
        kind: match root.kind {
            RootKind::List => "list",
            RootKind::ByPk => "by_pk",
            RootKind::Aggregate => "aggregate",
        }
        .into(),
        object: root.object.clone(),
        model: root.model_name.clone(),
        dependencies: root.dependencies.clone(),
        arguments: root.arguments.iter().map(argument).collect(),
        default_limit: root.default_limit,
        max_limit: root.max_limit,
    }
}

fn command_spec(command: &SurfaceCommand) -> ApplicationResult<SurfaceCommandSpec> {
    Ok(SurfaceCommandSpec {
        id: LogicalId::try_new("command", command.command_name.clone())?.into_string(),
        field_name: command.field_name.clone(),
        roles: command.roles.clone(),
        input: shape(&command.input),
        output: shape(&command.output),
        consistency: command.consistency,
        defaults: serde_json::to_value(&command.input_defaults)?,
        projection_contract: serde_json::to_value(&command.projections)?,
        direct_projection: command
            .direct_projection
            .as_ref()
            .map(crate::command::CommandDirectProjectionTarget::canonical_value),
        projected_model: command
            .projected_model
            .as_ref()
            .map(|model| model.model.clone()),
    })
}

fn argument(argument: &SurfaceArgument) -> SurfaceArgumentSpec {
    SurfaceArgumentSpec {
        name: argument.name.clone(),
        kind: match argument.kind {
            SurfaceArgumentKind::Filter => "filter",
            SurfaceArgumentKind::Order => "order",
            SurfaceArgumentKind::Limit => "limit",
            SurfaceArgumentKind::Offset => "offset",
            SurfaceArgumentKind::PrimaryKey => "primary_key",
        }
        .into(),
        type_name: argument.type_name.clone(),
        nullable: argument.nullable,
        list: argument.list,
    }
}

fn relationship(relationship: &RelField) -> ModelRelationshipSpec {
    ModelRelationshipSpec {
        name: relationship.name.clone(),
        target_model: relationship.target_model.clone(),
        target_object: relationship.target_object.clone(),
        kind: relationship.kind.clone(),
        list: relationship.list,
        nullable: relationship.nullable,
        arguments: relationship.arguments.iter().map(argument).collect(),
        keys: relationship_keys(&relationship.keys),
        dependencies: relationship.dependencies.clone(),
        aggregate: relationship
            .aggregate
            .as_ref()
            .map(|aggregate| SurfaceAggregateSpec {
                name: aggregate.name.clone(),
                type_name: aggregate.type_name.clone(),
                arguments: aggregate.arguments.iter().map(argument).collect(),
                dependencies: aggregate.dependencies.clone(),
            }),
    }
}

fn relationship_keys(keys: &SurfaceRelationshipKeys) -> serde_json::Value {
    match keys {
        SurfaceRelationshipKeys::Direct { local, remote } => {
            serde_json::json!({"kind": "direct", "local": local, "remote": remote})
        }
        SurfaceRelationshipKeys::Through {
            local,
            remote,
            table,
            source_foreign_key,
            target_foreign_key,
        } => serde_json::json!({
            "kind": "through",
            "local": local,
            "remote": remote,
            "table": table,
            "source_foreign_key": source_foreign_key,
            "target_foreign_key": target_foreign_key,
        }),
        SurfaceRelationshipKeys::ThroughOpaque {
            local,
            remote,
            dependency,
        } => serde_json::json!({
            "kind": "through_opaque",
            "local": local,
            "remote": remote,
            "dependency": dependency,
        }),
        SurfaceRelationshipKeys::Embedded => serde_json::json!({"kind": "embedded"}),
    }
}

fn row_policy(policy: &SurfaceRowPolicy) -> serde_json::Value {
    match policy {
        SurfaceRowPolicy::Unrestricted => serde_json::json!({"kind": "unrestricted"}),
        SurfaceRowPolicy::Predicate(predicate) => serde_json::json!({
            "kind": "predicate",
            "expression": predicate,
        }),
        SurfaceRowPolicy::ServerOnly(_) => serde_json::json!({"kind": "server_only"}),
    }
}

fn shape(shape: &SurfaceCommandShape) -> Option<CommandTypeSpec> {
    match shape {
        SurfaceCommandShape::None => None,
        SurfaceCommandShape::Typed(definition) => Some(type_spec(definition)),
    }
}

fn type_spec(definition: &SurfaceTypeDef) -> CommandTypeSpec {
    CommandTypeSpec {
        name: definition.name.clone(),
        fields: definition
            .fields
            .iter()
            .map(|field| CommandTypeField {
                unsigned_integer: field.unsigned_integer,
                name: field.name.clone(),
                type_name: field.type_name.clone(),
                nullable: field.nullable,
                list: field.list,
                item_nullable: field.item_nullable,
                nested: field.nested.as_deref().map(type_spec).map(Box::new),
            })
            .collect(),
    }
}
