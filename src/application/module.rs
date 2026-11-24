use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::command::{CommandDefinition, CommandMount, CommandSpec, EventSpec};
use super::error::{ApplicationError, ApplicationResult};
use super::identity::{canonical_json, sha256_fingerprint, LogicalId};
use super::surface::{SurfaceAggregateSpec, SurfaceArgumentSpec, SurfaceSpec};
use crate::command::TypedCommandContract;
use crate::graphql::surface::SurfaceProjectionOwner;
use crate::table::RelationshipKind;

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
    pub kind: RelationshipKind,
    pub list: bool,
    pub nullable: bool,
    pub arguments: Vec<SurfaceArgumentSpec>,
    pub keys: serde_json::Value,
    pub dependencies: Vec<String>,
    pub aggregate: Option<SurfaceAggregateSpec>,
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

    pub(super) fn refresh_fingerprint(&mut self) -> ApplicationResult<()> {
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

    pub(super) fn from_owner(owner: &SurfaceProjectionOwner) -> ApplicationResult<Self> {
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
            .build()
    }

    /// Projector half of the same module (no command mounts).
    pub fn projectors_only(&self) -> ApplicationResult<Self> {
        Module::new(self.id())
            .projections(self.manifest.projections.clone())
            .models(self.manifest.models.clone())
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
}

impl ModuleBuilder {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            definitions: Vec::new(),
            projections: Vec::new(),
            models: Vec::new(),
            surfaces: Vec::new(),
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
                .flat_map(|surface| surface.contract.projections.iter().cloned()),
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
                .flat_map(|surface| surface.contract.models.iter().cloned()),
        );
        models.sort_by(|left, right| left.id.cmp(&right.id));
        models = dedup_identical("model", models, |model| model.id.clone())?;

        let mut manifest = ModuleManifest {
            id,
            commands,
            events,
            projections,
            models,
            surfaces,
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
