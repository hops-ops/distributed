use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::*;

// The compiler treats one manifest version as an exact executable contract.
// Same-version extensions must bump the pre-release version instead of being
// silently ignored by an older client compiler.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestWire {
    manifest_version: u64,
    protocol_version: u64,
    service_id: String,
    surface: ManifestSurface,
    schema_fingerprint: String,
    protocol_fingerprint: String,
    execution: ManifestExecutionLimits,
    capabilities: ManifestCapabilities,
    scalar_codecs: Vec<ManifestScalarCodec>,
    models: Vec<ManifestModel>,
    roots: Vec<ManifestRoot>,
    commands: Vec<ManifestCommand>,
    protocol_operations: ManifestProtocolOperations,
    projectors: Vec<ManifestProjector>,
    projection_programs: Vec<ManifestProjectionProgram>,
    projection_bindings: Vec<ManifestProjectionBinding>,
    /// Absent on manifests without leaderboards (fingerprints unchanged).
    #[serde(default)]
    leaderboards: Vec<ManifestLeaderboard>,
}

#[derive(Serialize)]
struct ManifestSchemaMaterial<'a> {
    manifest_version: u64,
    protocol_version: u64,
    service_id: &'a str,
    surface: &'a ManifestSurface,
    execution: &'a ManifestExecutionLimits,
    capabilities: &'a ManifestCapabilities,
    scalar_codecs: &'a [ManifestScalarCodec],
    models: &'a [ManifestModel],
    roots: &'a [ManifestRoot],
    commands: &'a [ManifestCommand],
    protocol_operations: &'a ManifestProtocolOperations,
    projectors: &'a [ManifestProjector],
    projection_programs: &'a [ManifestProjectionProgram],
    projection_bindings: &'a [ManifestProjectionBinding],
    #[serde(skip_serializing_if = "<[ManifestLeaderboard]>::is_empty")]
    leaderboards: &'a [ManifestLeaderboard],
}

impl ClientManifest {
    pub(crate) fn parse(
        value: JsonValue,
        selector: &ClientSurfaceSelector,
    ) -> Result<Self, ClientCompileError> {
        validate_input_default_generators_in_json(&value)?;
        let mut wire: ManifestWire = serde_json::from_value(value).map_err(|error| {
            ClientCompileError::manifest(
                "client.manifest.invalid",
                format!("invalid Distributed client manifest: {error}"),
            )
        })?;
        let computed_schema_fingerprint = schema_fingerprint(&wire)?;
        if wire.manifest_version != MANIFEST_VERSION {
            return Err(ClientCompileError::manifest(
                "client.manifest.version",
                format!(
                    "client compiler requires manifest_version {MANIFEST_VERSION}, received {}",
                    wire.manifest_version
                ),
            ));
        }
        if wire.protocol_version != PROTOCOL_VERSION {
            return Err(ClientCompileError::manifest(
                "client.manifest.protocol_version",
                format!(
                    "client compiler requires protocol_version {PROTOCOL_VERSION}, received {}",
                    wire.protocol_version
                ),
            ));
        }
        validate_nonempty(&wire.service_id, "manifest.service_id")?;
        validate_hash(&wire.schema_fingerprint, "manifest.schema_fingerprint")?;
        validate_hash(&wire.protocol_fingerprint, "manifest.protocol_fingerprint")?;
        validate_execution_limits(&wire.execution)?;
        if wire.protocol_fingerprint != PROTOCOL_FINGERPRINT {
            return Err(ClientCompileError::manifest(
                "client.manifest.protocol_fingerprint",
                format!(
                    "client compiler protocol contract is `{PROTOCOL_FINGERPRINT}`, received `{}`; regenerate the manifest and use a matching distributed version",
                    wire.protocol_fingerprint
                ),
            ));
        }
        canonicalize_surface(&mut wire.surface)?;
        validate_surface(&wire.surface, selector)?;
        validate_capabilities(&wire.capabilities)?;

        let scalar_codecs = validate_scalar_codecs(wire.scalar_codecs)?;
        let mut models = BTreeMap::new();
        let mut typenames = BTreeSet::new();
        let mut source_tables = BTreeSet::new();
        let mut filter_input_types = BTreeSet::new();
        for mut model in wire.models {
            canonicalize_model(&mut model)?;
            validate_model(&model, &scalar_codecs)?;
            if !typenames.insert(model.typename.clone()) {
                return Err(ClientCompileError::manifest(
                    "client.manifest.duplicate_typename",
                    format!("duplicate manifest model typename `{}`", model.typename),
                ));
            }
            if !source_tables.insert(model.source_table.clone()) {
                return Err(ClientCompileError::manifest(
                    "client.manifest.duplicate_source_table",
                    format!(
                        "multiple manifest models claim source table `{}`",
                        model.source_table
                    ),
                ));
            }
            if !filter_input_types.insert(model.filter_input.type_name.clone()) {
                return Err(ClientCompileError::manifest(
                    "client.manifest.duplicate_filter_input",
                    format!(
                        "multiple manifest models claim filter input type `{}`",
                        model.filter_input.type_name
                    ),
                ));
            }
            let id = model.id.clone();
            if models.insert(id.clone(), model).is_some() {
                return Err(ClientCompileError::manifest(
                    "client.manifest.duplicate_model",
                    format!("duplicate manifest model id `{id}`"),
                ));
            }
        }
        validate_model_graph(&models, &scalar_codecs)?;

        let mut roots = BTreeMap::new();
        let mut root_ids = BTreeSet::new();
        for mut root in wire.roots {
            canonicalize_root(&mut root)?;
            validate_nonempty(&root.id, "manifest root id")?;
            validate_nonempty(&root.name, "manifest root name")?;
            validate_nonempty(&root.model, "manifest root model")?;
            if !root_ids.insert(root.id.clone()) {
                return Err(ClientCompileError::manifest(
                    "client.manifest.duplicate_root_id",
                    format!("duplicate manifest root id `{}`", root.id),
                ));
            }
            if !models.contains_key(&root.model) {
                return Err(ClientCompileError::manifest(
                    "client.manifest.root_model",
                    format!(
                        "manifest root `{}` references missing model `{}`",
                        root.name, root.model
                    ),
                ));
            }
            validate_unique_arguments(&root, &scalar_codecs)?;
            validate_root_contract(&root, &models)?;
            let key = (root.operation, root.name.clone());
            if roots.insert(key, root).is_some() {
                return Err(ClientCompileError::manifest(
                    "client.manifest.duplicate_root",
                    "duplicate manifest operation root",
                ));
            }
        }

        let mut projectors = wire.projectors;
        canonicalize_projectors(&mut projectors)?;
        validate_projectors(&projectors, &models)?;
        let (projection_programs, projection_bindings) = validate_projection_manifest(
            wire.projection_programs,
            wire.projection_bindings,
            &models,
        )?;
        let mut commands = wire.commands;
        canonicalize_commands(&mut commands)?;
        let mut command_validation = super::super::command_manifest::validate_command_manifest(
            &commands,
            &models,
            &roots,
            &scalar_codecs,
            &projectors,
            wire.capabilities.causal_receipts,
            &wire.protocol_operations,
        )?;
        command_validation
            .commands_requiring_revalidation
            .extend(validate_command_projections(
                &commands,
                &projection_programs,
                &projection_bindings,
                &models,
            )?);
        command_validation
            .commands_requiring_revalidation
            .extend(validate_direct_projections(
                &commands,
                &models,
                &projectors,
            )?);
        let trusted_presets = derive_trusted_preset_descriptors(&models, &commands)?;
        validate_leaderboards(&wire.leaderboards, &roots, &typenames)?;
        let live_leaderboards = wire
            .leaderboards
            .iter()
            .any(|leaderboard| leaderboard.roots.iter().any(|root| root.live));
        validate_derived_capabilities(&wire.capabilities, &roots, &commands, live_leaderboards)?;
        if wire.schema_fingerprint != computed_schema_fingerprint {
            return Err(ClientCompileError::manifest(
                "client.manifest.schema_fingerprint",
                format!(
                    "manifest schema fingerprint mismatch: declared `{}`, computed `{computed_schema_fingerprint}`; regenerate the selected client manifest",
                    wire.schema_fingerprint
                ),
            ));
        }

        Ok(Self {
            service_id: wire.service_id,
            surface: wire.surface,
            schema_fingerprint: wire.schema_fingerprint,
            protocol_fingerprint: wire.protocol_fingerprint,
            execution: wire.execution,
            capabilities: wire.capabilities,
            scalar_codecs,
            models,
            roots,
            commands,
            trusted_presets,
            commands_requiring_revalidation: command_validation.commands_requiring_revalidation,
            protocol_operations: wire.protocol_operations,
            projectors,
            projection_programs,
            projection_bindings,
            leaderboards: wire.leaderboards,
        })
    }

    pub(crate) fn root(&self, operation: RootOperation, name: &str) -> Option<&ManifestRoot> {
        self.roots.get(&(operation, name.to_string()))
    }
}

fn schema_fingerprint(wire: &ManifestWire) -> Result<String, ClientCompileError> {
    let material = ManifestSchemaMaterial {
        manifest_version: wire.manifest_version,
        protocol_version: wire.protocol_version,
        service_id: &wire.service_id,
        surface: &wire.surface,
        execution: &wire.execution,
        capabilities: &wire.capabilities,
        scalar_codecs: &wire.scalar_codecs,
        models: &wire.models,
        roots: &wire.roots,
        commands: &wire.commands,
        protocol_operations: &wire.protocol_operations,
        projectors: &wire.projectors,
        projection_programs: &wire.projection_programs,
        projection_bindings: &wire.projection_bindings,
        leaderboards: &wire.leaderboards,
    };
    serde_json::to_vec(&material)
        .map(|bytes| hash_bytes(&bytes))
        .map_err(|error| {
            ClientCompileError::manifest(
                "client.manifest.schema_fingerprint",
                format!("could not recompute manifest schema fingerprint: {error}"),
            )
        })
}

#[cfg(test)]
pub(crate) fn refresh_schema_fingerprint(value: &mut JsonValue) {
    let wire: ManifestWire =
        serde_json::from_value(value.clone()).expect("test manifest must match the v2 wire shape");
    value["schema_fingerprint"] =
        JsonValue::String(schema_fingerprint(&wire).expect("test manifest must be serializable"));
}
