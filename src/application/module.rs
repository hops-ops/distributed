use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::command::{CommandDefinition, CommandMount, CommandSpec, CommandTypeSpec, EventSpec};
use super::error::{ApplicationError, ApplicationResult};
use super::identity::{canonical_json, sha256_fingerprint, LogicalId};
use crate::command::TypedCommandContract;
use crate::graphql::surface::{
    RootKind, Surface, SurfaceArgument, SurfaceCommand, SurfaceCommandShape,
    SurfaceProjectionOwner, SurfaceRelationshipKeys, SurfaceSelection, SurfaceTypeDef,
};

/// One exposed model field in a portable surface/module artifact.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelFieldSpec {
    pub name: String,
    pub scalar: String,
    pub nullable: bool,
}

/// Stable model identity and authorized field inventory.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelSpec {
    pub id: String,
    pub table: String,
    pub object: String,
    pub fields: Vec<ModelFieldSpec>,
    pub primary_key: Vec<String>,
    pub relationships: Vec<ModelRelationshipSpec>,
    pub row_policy: serde_json::Value,
    pub role_limit: Option<u64>,
    pub aggregations: bool,
    pub fingerprint: String,
}

/// Complete authorized relationship material retained by a model contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRelationshipSpec {
    pub name: String,
    pub target_model: String,
    pub target_object: String,
    pub kind: String,
    pub list: bool,
    pub nullable: bool,
    pub arguments: Vec<SurfaceArgumentSpec>,
    pub keys: serde_json::Value,
    pub dependencies: Vec<String>,
    pub aggregate: Option<SurfaceAggregateSpec>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SurfaceArgumentSpec {
    pub name: String,
    pub kind: String,
    pub type_name: String,
    pub nullable: bool,
    pub list: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SurfaceAggregateSpec {
    pub name: String,
    pub type_name: String,
    pub arguments: Vec<SurfaceArgumentSpec>,
    pub dependencies: Vec<String>,
}

impl ModelSpec {
    /// Construct a minimal portable model contract for composition tests and
    /// plan fixtures. Surfaces may still re-author richer model material.
    pub fn try_new(
        id: impl Into<String>,
        table: impl Into<String>,
        fields: impl IntoIterator<Item = ModelFieldSpec>,
        primary_key: impl IntoIterator<Item = impl Into<String>>,
    ) -> ApplicationResult<Self> {
        let id = LogicalId::try_new("model", id)?.into_string();
        let table = LogicalId::try_new("model table", table)?.into_string();
        let mut fields = fields.into_iter().collect::<Vec<_>>();
        fields.sort_by(|left, right| left.name.cmp(&right.name));
        let mut primary_key = primary_key.into_iter().map(Into::into).collect::<Vec<_>>();
        primary_key.sort();
        primary_key.dedup();
        if fields.is_empty() {
            return Err(ApplicationError::InvalidSpec(format!(
                "model `{id}` must declare at least one field"
            )));
        }
        if primary_key.is_empty() {
            return Err(ApplicationError::InvalidSpec(format!(
                "model `{id}` must declare a primary key"
            )));
        }
        let mut spec = Self {
            id: id.clone(),
            table,
            object: id,
            fields,
            primary_key,
            relationships: Vec::new(),
            row_policy: serde_json::json!({ "kind": "unrestricted" }),
            role_limit: None,
            aggregations: false,
            fingerprint: String::new(),
        };
        spec.refresh_fingerprint()?;
        Ok(spec)
    }

    fn refresh_fingerprint(&mut self) -> ApplicationResult<()> {
        let mut value = serde_json::to_value(&*self)?;
        if let serde_json::Value::Object(fields) = &mut value {
            fields.insert(
                "fingerprint".into(),
                serde_json::Value::String(String::new()),
            );
        }
        self.fingerprint = sha256_fingerprint(&serde_json::to_vec(&canonical_json(&value))?);
        Ok(())
    }
}

/// Portable projection-owner identity aggregated into a module.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionSpec {
    pub id: String,
    pub facts: Vec<String>,
    pub models: Vec<String>,
    pub dependencies: Vec<String>,
    pub direct: bool,
    pub change_epoch: Option<String>,
    pub modeled_programs: Vec<String>,
    /// Canonical behavior-affecting program/binding material selected from the
    /// authoritative Surface IR. Runtime routes and physical service config
    /// are intentionally absent.
    #[serde(default)]
    pub modeled: Vec<serde_json::Value>,
    #[serde(default)]
    pub partition: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_fingerprint: Option<String>,
    pub fingerprint: String,
}

impl ProjectionSpec {
    pub fn try_new(
        id: impl Into<String>,
        facts: impl IntoIterator<Item = impl Into<String>>,
        models: impl IntoIterator<Item = impl Into<String>>,
    ) -> ApplicationResult<Self> {
        let id = LogicalId::try_new("projection", id)?.into_string();
        let mut spec = Self {
            id,
            facts: facts.into_iter().map(Into::into).collect(),
            models: models.into_iter().map(Into::into).collect(),
            dependencies: Vec::new(),
            direct: false,
            change_epoch: None,
            modeled_programs: Vec::new(),
            modeled: Vec::new(),
            partition: serde_json::Value::Null,
            catalog_fingerprint: None,
            fingerprint: String::new(),
        };
        spec.canonicalize();
        spec.refresh_fingerprint()?;
        Ok(spec)
    }

    /// Mark the projection as direct (Atomic seal) and refresh its fingerprint.
    pub fn with_direct(mut self, direct: bool) -> ApplicationResult<Self> {
        self.direct = direct;
        self.canonicalize();
        self.refresh_fingerprint()?;
        Ok(self)
    }

    fn from_owner(owner: &SurfaceProjectionOwner) -> ApplicationResult<Self> {
        let mut spec = Self {
            id: LogicalId::try_new("projection", owner.name.clone())?.into_string(),
            facts: owner.facts.clone(),
            models: owner.models.clone(),
            dependencies: owner.dependencies.clone(),
            direct: owner.is_direct(),
            change_epoch: owner.change_epoch.clone(),
            modeled_programs: owner
                .modeled
                .iter()
                .map(|modeled| modeled.program_id().to_string())
                .collect(),
            modeled: owner
                .modeled
                .iter()
                .map(|modeled| modeled.canonical_contract_value())
                .collect::<Result<Vec<_>, _>>()
                .map_err(ApplicationError::InvalidSpec)?,
            partition: serde_json::to_value(&owner.partition)?,
            catalog_fingerprint: None,
            fingerprint: String::new(),
        };
        spec.canonicalize();
        spec.refresh_fingerprint()?;
        Ok(spec)
    }

    /// Reference an existing canonical projection catalog without copying
    /// executable routes or handler values into the module artifact.
    pub fn from_catalog(
        id: impl Into<String>,
        catalog: &crate::projection::catalog::ProjectionCatalog,
    ) -> ApplicationResult<Self> {
        let bytes = catalog
            .canonical_bytes()
            .map_err(|error| ApplicationError::Canonical(error.to_string()))?;
        let mut spec = Self::try_new(id, Vec::<String>::new(), Vec::<String>::new())?;
        spec.catalog_fingerprint = Some(sha256_fingerprint(&bytes));
        spec.refresh_fingerprint()?;
        Ok(spec)
    }

    fn canonicalize(&mut self) {
        self.facts.sort();
        self.facts.dedup();
        self.models.sort();
        self.models.dedup();
        self.dependencies.sort();
        self.dependencies.dedup();
        self.modeled_programs.sort();
        self.modeled_programs.dedup();
    }

    fn refresh_fingerprint(&mut self) -> ApplicationResult<()> {
        let mut value = serde_json::to_value(&*self)?;
        if let serde_json::Value::Object(fields) = &mut value {
            fields.insert(
                "fingerprint".into(),
                serde_json::Value::String(String::new()),
            );
        }
        self.fingerprint = sha256_fingerprint(&serde_json::to_vec(&canonical_json(&value))?);
        Ok(())
    }

    /// Canonical projection identity material with the derived fingerprint
    /// removed. This is the same byte source used by generated projection
    /// owners and by manifest validation.
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

/// A root field identity retained by a surface contract.
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

/// A command shape as exposed by a selected surface.
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
    pub effects: serde_json::Value,
    pub confirmations: serde_json::Value,
    pub projection_contract: serde_json::Value,
    pub applies: serde_json::Value,
    pub direct_projection: Option<serde_json::Value>,
    pub projected_model: Option<String>,
    pub confirmation_unavailable: bool,
}

/// Authorized, placement-independent surface identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SurfaceSpec {
    pub id: String,
    pub selection: String,
    pub eligible_roles: Vec<String>,
    pub schema_roles: Vec<String>,
    pub models: Vec<ModelSpec>,
    pub roots: Vec<SurfaceRootSpec>,
    pub commands: Vec<SurfaceCommandSpec>,
    pub projections: Vec<ProjectionSpec>,
    pub dialect: String,
    pub aggregates: bool,
    pub subscriptions: bool,
    pub default_limit: u64,
    pub max_limit: u64,
    pub comparison_ops: BTreeMap<String, Vec<String>>,
    pub commands_attached: bool,
    pub projectors_attached: bool,
    /// Canonical lossless Surface IR snapshot. It is the identity authority
    /// for SDL/client compilation and must never be reconstructed from tables.
    pub contract: serde_json::Value,
    pub fingerprint: String,
}

impl SurfaceSpec {
    pub fn try_new(id: impl Into<String>) -> ApplicationResult<Self> {
        let id = LogicalId::try_new("surface", id)?.into_string();
        let mut spec = Self {
            id,
            selection: "catalog".into(),
            eligible_roles: Vec::new(),
            schema_roles: Vec::new(),
            models: Vec::new(),
            roots: Vec::new(),
            commands: Vec::new(),
            projections: Vec::new(),
            dialect: "sqlite".into(),
            aggregates: true,
            subscriptions: true,
            default_limit: 100,
            max_limit: 1000,
            comparison_ops: BTreeMap::new(),
            commands_attached: false,
            projectors_attached: false,
            contract: serde_json::json!({
                "version": 1,
                "selection": {"kind": "catalog"},
                "dialect": "sqlite",
                "aggregates": true,
                "subscriptions": true,
                "default_limit": 100,
                "max_limit": 1000,
                "models": [],
                "roots": [],
                "comparison_ops": {},
                "commands": [],
                "commands_attached": false,
                "projectors": [],
                "projectors_attached": false,
            }),
            fingerprint: String::new(),
        };
        spec.refresh_fingerprint()?;
        Ok(spec)
    }

    /// Compile the already-authorized Surface IR without constructing a
    /// repository, service, lock manager, or handler mount.
    pub fn from_surface(id: impl Into<String>, surface: &Surface) -> ApplicationResult<Self> {
        let id = LogicalId::try_new("surface", id)?.into_string();
        let (selection, mut eligible_roles, mut schema_roles) = match &surface.selection {
            SurfaceSelection::Catalog => ("catalog".to_owned(), Vec::new(), Vec::new()),
            SurfaceSelection::Role { name } => {
                ("role".to_owned(), vec![name.clone()], vec![name.clone()])
            }
            SurfaceSelection::Application {
                name,
                eligible_roles,
                schema_roles,
            } => (
                format!("application:{name}"),
                eligible_roles.clone(),
                schema_roles.clone(),
            ),
        };
        eligible_roles.sort();
        eligible_roles.dedup();
        schema_roles.sort();
        schema_roles.dedup();

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
                    relationships: model
                        .relationships
                        .iter()
                        .map(surface_relationship_spec)
                        .collect::<ApplicationResult<Vec<_>>>()?,
                    row_policy: surface_row_policy(&model.row_policy)?,
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
            .map(|root| ("query", root))
            .chain(
                surface
                    .subscription_fields
                    .iter()
                    .map(|root| ("subscription", root)),
            )
            .map(|(operation, root)| SurfaceRootSpec {
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
                arguments: root.arguments.iter().map(surface_argument_spec).collect(),
                default_limit: root.default_limit,
                max_limit: root.max_limit,
            })
            .collect::<Vec<_>>();
        roots.sort_by(|left, right| {
            (left.operation.as_str(), left.name.as_str())
                .cmp(&(right.operation.as_str(), right.name.as_str()))
        });

        let mut commands = surface
            .commands
            .iter()
            .map(surface_command_spec)
            .collect::<ApplicationResult<Vec<_>>>()?;
        commands.sort_by(|left, right| left.id.cmp(&right.id));

        let mut projections = surface
            .projectors
            .iter()
            .map(ProjectionSpec::from_owner)
            .collect::<ApplicationResult<Vec<_>>>()?;
        projections.sort_by(|left, right| left.id.cmp(&right.id));

        let mut spec = Self {
            id,
            selection,
            eligible_roles,
            schema_roles,
            models,
            roots,
            commands,
            projections,
            dialect: format!("{:?}", surface.dialect).to_ascii_lowercase(),
            aggregates: surface.aggregates,
            subscriptions: surface.subscriptions,
            default_limit: surface.default_limit,
            max_limit: surface.max_limit,
            comparison_ops: surface.comparison_ops.clone(),
            commands_attached: surface.commands_attached,
            projectors_attached: surface.projectors_attached,
            contract: surface
                .canonical_contract_value()
                .map_err(ApplicationError::InvalidSpec)?,
            fingerprint: String::new(),
        };
        spec.refresh_fingerprint()?;
        Ok(spec)
    }

    fn refresh_fingerprint(&mut self) -> ApplicationResult<()> {
        let mut value = serde_json::to_value(&*self)?;
        if let serde_json::Value::Object(fields) = &mut value {
            fields.insert(
                "fingerprint".into(),
                serde_json::Value::String(String::new()),
            );
        }
        self.fingerprint = sha256_fingerprint(&serde_json::to_vec(&canonical_json(&value))?);
        Ok(())
    }

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

fn surface_command_spec(command: &SurfaceCommand) -> ApplicationResult<SurfaceCommandSpec> {
    Ok(SurfaceCommandSpec {
        id: LogicalId::try_new("command", command.command_name.clone())?.into_string(),
        field_name: command.field_name.clone(),
        roles: command.roles.clone(),
        input: surface_shape_spec(&command.input),
        output: surface_shape_spec(&command.output),
        consistency: command.consistency,
        defaults: serde_json::to_value(&command.input_defaults)?,
        effects: serde_json::to_value(&command.effects)?,
        confirmations: serde_json::to_value(&command.confirmations)?,
        projection_contract: serde_json::to_value(&command.projections)?,
        applies: serde_json::to_value(&command.projections.previews)?,
        direct_projection: command
            .direct_projection
            .as_ref()
            .map(crate::command::CommandDirectProjectionTarget::canonical_value),
        projected_model: command
            .projected_model
            .as_ref()
            .map(|model| model.model.clone()),
        confirmation_unavailable: command.confirmation_unavailable,
    })
}

fn surface_argument_spec(argument: &SurfaceArgument) -> SurfaceArgumentSpec {
    SurfaceArgumentSpec {
        name: argument.name.clone(),
        kind: match argument.kind {
            crate::graphql::surface::SurfaceArgumentKind::Filter => "filter",
            crate::graphql::surface::SurfaceArgumentKind::Order => "order",
            crate::graphql::surface::SurfaceArgumentKind::Limit => "limit",
            crate::graphql::surface::SurfaceArgumentKind::Offset => "offset",
            crate::graphql::surface::SurfaceArgumentKind::PrimaryKey => "primary_key",
        }
        .into(),
        type_name: argument.type_name.clone(),
        nullable: argument.nullable,
        list: argument.list,
    }
}

fn surface_relationship_spec(
    relationship: &crate::graphql::surface::RelField,
) -> ApplicationResult<ModelRelationshipSpec> {
    Ok(ModelRelationshipSpec {
        name: relationship.name.clone(),
        target_model: relationship.target_model.clone(),
        target_object: relationship.target_object.clone(),
        kind: format!("{:?}", relationship.kind).to_ascii_lowercase(),
        list: relationship.list,
        nullable: relationship.nullable,
        arguments: relationship
            .arguments
            .iter()
            .map(surface_argument_spec)
            .collect(),
        keys: surface_relationship_keys(&relationship.keys),
        dependencies: relationship.dependencies.clone(),
        aggregate: relationship
            .aggregate
            .as_ref()
            .map(|aggregate| SurfaceAggregateSpec {
                name: aggregate.name.clone(),
                type_name: aggregate.type_name.clone(),
                arguments: aggregate
                    .arguments
                    .iter()
                    .map(surface_argument_spec)
                    .collect(),
                dependencies: aggregate.dependencies.clone(),
            }),
    })
}

fn surface_relationship_keys(keys: &SurfaceRelationshipKeys) -> serde_json::Value {
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

fn surface_row_policy(
    policy: &crate::graphql::surface::SurfaceRowPolicy,
) -> ApplicationResult<serde_json::Value> {
    Ok(match policy {
        crate::graphql::surface::SurfaceRowPolicy::Unrestricted => {
            serde_json::json!({"kind": "unrestricted"})
        }
        crate::graphql::surface::SurfaceRowPolicy::Predicate(predicate) => serde_json::json!({
            "kind": "predicate",
            "expression": predicate,
        }),
        crate::graphql::surface::SurfaceRowPolicy::ServerOnly => {
            serde_json::json!({"kind": "server_only"})
        }
    })
}

fn surface_shape_spec(shape: &SurfaceCommandShape) -> Option<CommandTypeSpec> {
    match shape {
        SurfaceCommandShape::None => None,
        SurfaceCommandShape::Typed(definition) => Some(surface_type_spec(definition)),
    }
}

fn surface_type_spec(definition: &SurfaceTypeDef) -> CommandTypeSpec {
    CommandTypeSpec {
        name: definition.name.clone(),
        fields: definition
            .fields
            .iter()
            .map(|field| super::command::CommandTypeField {
                name: field.name.clone(),
                type_name: field.type_name.clone(),
                nullable: field.nullable,
                list: field.list,
                item_nullable: field.item_nullable,
                nested: field.nested.as_deref().map(surface_type_spec).map(Box::new),
            })
            .collect(),
    }
}

/// The serializable logical portion of a module.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModuleManifest {
    pub id: String,
    pub commands: Vec<CommandSpec>,
    pub events: Vec<EventSpec>,
    pub projections: Vec<ProjectionSpec>,
    pub models: Vec<ModelSpec>,
    pub surfaces: Vec<SurfaceSpec>,
    pub required_capabilities: Vec<String>,
    pub fingerprint: String,
}

impl ModuleManifest {
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

/// One explicit logical composition unit with optional executable mounts.
pub struct Module {
    manifest: ModuleManifest,
    mounts: Vec<CommandMount>,
    definitions: Vec<CommandDefinition>,
}

impl Clone for Module {
    fn clone(&self) -> Self {
        Self {
            manifest: self.manifest.clone(),
            mounts: self.mounts.clone(),
            definitions: self.definitions.clone(),
        }
    }
}

impl std::fmt::Debug for Module {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Module")
            .field("id", &self.manifest.id)
            .field("commands", &self.manifest.commands.len())
            .field("mounts", &self.mounts.len())
            .finish()
    }
}

impl serde::Serialize for Module {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.manifest.serialize(serializer)
    }
}

impl Module {
    pub fn new(id: impl Into<String>) -> ModuleBuilder {
        ModuleBuilder::new(id)
    }

    pub fn builder(id: impl Into<String>) -> ModuleBuilder {
        ModuleBuilder::new(id)
    }

    pub fn manifest(&self) -> &ModuleManifest {
        &self.manifest
    }

    pub fn id(&self) -> &str {
        &self.manifest.id
    }

    pub fn commands(&self) -> &[CommandSpec] {
        &self.manifest.commands
    }

    pub fn surfaces(&self) -> &[SurfaceSpec] {
        &self.manifest.surfaces
    }

    pub fn mounts(&self) -> &[CommandMount] {
        &self.mounts
    }

    pub fn definitions(&self) -> &[CommandDefinition] {
        &self.definitions
    }

    /// Full module: commands and projectors together.
    pub fn module(&self) -> Self {
        self.clone()
    }

    /// Command half of the same module (no projection mounts).
    pub fn commands_only(&self) -> ApplicationResult<Self> {
        Module::new(self.id())
            .command_definitions(self.definitions.clone())
            .models(self.manifest.models.clone())
            .required_capabilities(self.manifest.required_capabilities.clone())
            .build()
    }

    /// Projector half of the same module (no command mounts).
    pub fn projectors_only(&self) -> ApplicationResult<Self> {
        Module::new(self.id())
            .projections(self.manifest.projections.clone())
            .models(self.manifest.models.clone())
            .required_capabilities(self.manifest.required_capabilities.clone())
            .build()
    }

    /// Return the exact typed command inventory retained by generated command
    /// definitions. A manually assembled `CommandSpec` has no typed source
    /// contract and therefore cannot be used to bind commands to a Surface.
    pub(crate) fn typed_command_contracts(&self) -> Result<Vec<TypedCommandContract>, String> {
        let mut contracts = Vec::with_capacity(self.definitions.len());
        for definition in &self.definitions {
            let Some(contract) = definition.typed_contract() else {
                return Err(format!(
                    "module `{}` command `{}` has no retained typed command contract",
                    self.id(),
                    definition.spec().id
                ));
            };
            let expected = CommandSpec::from_contract(contract)
                .map_err(|error| format!("cannot compile typed command contract: {error}"))?;
            if expected != *definition.spec() {
                return Err(format!(
                    "module `{}` command `{}` has a stale portable spec beside its typed command contract",
                    self.id(),
                    definition.spec().id
                ));
            }
            contracts.push(contract.clone());
        }
        Ok(contracts)
    }

    pub fn canonical_bytes(&self) -> ApplicationResult<Vec<u8>> {
        self.manifest.canonical_bytes()
    }
}

/// Fluent explicit module authoring API.
pub struct ModuleBuilder {
    id: String,
    definitions: Vec<CommandDefinition>,
    projections: Vec<ProjectionSpec>,
    models: Vec<ModelSpec>,
    surfaces: Vec<SurfaceSpec>,
    required_capabilities: Vec<String>,
}

impl ModuleBuilder {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            definitions: Vec::new(),
            projections: Vec::new(),
            models: Vec::new(),
            surfaces: Vec::new(),
            required_capabilities: Vec::new(),
        }
    }

    /// Add one declaration whose optional runtime mount is derived from the
    /// same command spec.
    pub fn command_definition(mut self, definition: CommandDefinition) -> Self {
        self.definitions.push(definition);
        self
    }

    pub fn command_definitions(
        mut self,
        definitions: impl IntoIterator<Item = CommandDefinition>,
    ) -> Self {
        self.definitions.extend(definitions);
        self
    }

    pub fn projection(mut self, projection: ProjectionSpec) -> Self {
        self.projections.push(projection);
        self
    }

    pub fn projections(mut self, projections: impl IntoIterator<Item = ProjectionSpec>) -> Self {
        self.projections.extend(projections);
        self
    }

    pub fn model(mut self, model: ModelSpec) -> Self {
        self.models.push(model);
        self
    }

    pub fn models(mut self, models: impl IntoIterator<Item = ModelSpec>) -> Self {
        self.models.extend(models);
        self
    }

    pub fn surface(mut self, surface: SurfaceSpec) -> Self {
        self.surfaces.push(surface);
        self
    }

    pub fn surfaces(mut self, surfaces: impl IntoIterator<Item = SurfaceSpec>) -> Self {
        self.surfaces.extend(surfaces);
        self
    }

    pub fn required_capability(mut self, capability: impl Into<String>) -> Self {
        self.required_capabilities.push(capability.into());
        self
    }

    pub fn required_capabilities(
        mut self,
        capabilities: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.required_capabilities
            .extend(capabilities.into_iter().map(Into::into));
        self
    }

    pub fn build(self) -> ApplicationResult<Module> {
        let id = LogicalId::try_new("module", self.id)?.into_string();
        let mut definitions = self.definitions;
        definitions.sort_by(|left, right| left.spec().id.cmp(&right.spec().id));
        let mut commands = definitions
            .iter()
            .map(|definition| definition.spec().clone())
            .collect::<Vec<_>>();
        commands.sort_by(|left, right| left.id.cmp(&right.id));
        validate_unique("command", commands.iter().map(|command| command.id.clone()))?;
        for command in &commands {
            command.validate()?;
            command.validate_fingerprint()?;
        }

        for definition in &definitions {
            if let Some(contract) = definition.typed_contract() {
                let expected = CommandSpec::from_contract(contract)?;
                if expected != *definition.spec() {
                    return Err(ApplicationError::Collision {
                        kind: "command",
                        identity: definition.spec().id.clone(),
                        reason: "typed command contract and portable spec disagree".into(),
                    });
                }
            }
        }

        let mut mounts = definitions
            .iter()
            .filter_map(|definition| definition.mount().cloned())
            .collect::<Vec<_>>();
        let command_ids = commands
            .iter()
            .map(|command| command.id.as_str())
            .collect::<BTreeSet<_>>();
        for mount in &mounts {
            if !command_ids.contains(mount.spec().id.as_str()) {
                return Err(ApplicationError::Missing {
                    kind: "command",
                    identity: mount.spec().id.clone(),
                });
            }
            let Some(command) = commands
                .iter()
                .find(|command| command.id == mount.spec().id)
            else {
                continue;
            };
            if command.fingerprint != mount.spec().fingerprint {
                return Err(ApplicationError::Collision {
                    kind: "command",
                    identity: command.id.clone(),
                    reason: "executable mount spec differs from module command spec".into(),
                });
            }
        }

        mounts.sort_by(|left, right| left.spec().id.cmp(&right.spec().id));
        validate_unique(
            "command mount",
            mounts.iter().map(|mount| mount.spec().id.clone()),
        )?;

        let mut surfaces = self.surfaces;
        surfaces.sort_by(|left, right| left.id.cmp(&right.id));
        validate_unique("surface", surfaces.iter().map(|surface| surface.id.clone()))?;

        let mut projections = self.projections;
        projections.extend(
            surfaces
                .iter()
                .flat_map(|surface| surface.projections.iter().cloned()),
        );
        projections.sort_by(|left, right| left.id.cmp(&right.id));
        projections = dedup_identical("projection", projections, |projection| {
            projection.id.clone()
        })?;

        let mut events = commands
            .iter()
            .flat_map(|command| command.emits.iter().cloned())
            .collect::<Vec<_>>();
        events.sort_by(|left, right| {
            (
                left.name.as_str(),
                left.version,
                left.body_fingerprint.as_str(),
            )
                .cmp(&(
                    right.name.as_str(),
                    right.version,
                    right.body_fingerprint.as_str(),
                ))
        });
        let mut event_by_name = BTreeMap::new();
        for event in events {
            if let Some(existing) = event_by_name.insert(event.name.clone(), event.clone()) {
                if existing != event {
                    return Err(ApplicationError::Collision {
                        kind: "event",
                        identity: event.name,
                        reason: "event selectors with one name disagree".into(),
                    });
                }
            }
        }
        let events = event_by_name.into_values().collect();

        let mut models = self.models;
        models.extend(
            surfaces
                .iter()
                .flat_map(|surface| surface.models.iter().cloned()),
        );
        models.sort_by(|left, right| left.id.cmp(&right.id));
        models = dedup_identical("model", models, |model| model.id.clone())?;

        let mut required_capabilities = self.required_capabilities;
        required_capabilities.sort();
        required_capabilities.dedup();

        let mut manifest = ModuleManifest {
            id,
            commands,
            events,
            projections,
            models,
            surfaces,
            required_capabilities,
            fingerprint: String::new(),
        };
        manifest.fingerprint = sha256_fingerprint(&manifest.canonical_bytes()?);
        Ok(Module {
            manifest,
            mounts,
            definitions,
        })
    }
}

fn validate_unique(
    kind: &'static str,
    identities: impl IntoIterator<Item = String>,
) -> ApplicationResult<()> {
    let mut seen = BTreeSet::new();
    for identity in identities {
        if !seen.insert(identity.clone()) {
            return Err(ApplicationError::Duplicate { kind, identity });
        }
    }
    Ok(())
}

fn dedup_identical<T: PartialEq>(
    kind: &'static str,
    values: Vec<T>,
    identity: impl Fn(&T) -> String,
) -> ApplicationResult<Vec<T>> {
    let mut out = Vec::new();
    let mut seen = BTreeMap::<String, T>::new();
    for value in values {
        let id = identity(&value);
        if let Some(existing) = seen.get(&id) {
            if existing != &value {
                return Err(ApplicationError::Collision {
                    kind,
                    identity: id,
                    reason: "same identity has incompatible portable definitions".into(),
                });
            }
            continue;
        }
        seen.insert(id, value);
    }
    out.extend(seen.into_values());
    Ok(out)
}
