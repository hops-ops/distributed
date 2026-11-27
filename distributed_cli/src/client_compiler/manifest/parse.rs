use std::collections::{BTreeMap, BTreeSet};

use super::*;

/// The validated, indexed form of one [`DistributedClientManifest`].
#[derive(Clone, Debug)]
pub(crate) struct ClientManifest {
    pub(crate) service_id: String,
    pub(crate) surface: ClientSurfaceIdentity,
    pub(crate) schema_fingerprint: String,
    pub(crate) protocol_fingerprint: String,
    pub(crate) execution: ClientExecutionLimits,
    pub(crate) capabilities: ClientCapabilities,
    pub(crate) scalar_codecs: BTreeMap<String, String>,
    pub(crate) models: BTreeMap<String, ClientModel>,
    pub(crate) roots: BTreeMap<(ClientRootOperation, String), ClientRoot>,
    pub(crate) commands: Vec<ClientCommand>,
    /// Exact scope-wide descriptor inventory derived from command-local
    /// presets plus every client-visible row-policy claim/column codec.
    pub(crate) trusted_presets: Vec<ClientTrustedPresetDescriptor>,
    pub(crate) commands_requiring_revalidation: BTreeSet<String>,
    pub(crate) protocol_operations: ClientProtocolOperations,
    pub(crate) projectors: Vec<ClientProjector>,
    pub(crate) projection_programs: Vec<ClientProjectionProgram>,
    pub(crate) projection_bindings: Vec<ClientProjectionBinding>,
    pub(crate) leaderboards: Vec<ClientLeaderboard>,
}

impl ClientManifest {
    /// Validate one manifest as an exact executable contract: the versions
    /// and protocol family this compiler was built with, the surface the
    /// caller selected, and content matching its declared fingerprint.
    pub(crate) fn parse(
        mut wire: DistributedClientManifest,
        selector: &ClientSurfaceIdentity,
    ) -> Result<Self, ClientCompileError> {
        let computed_schema_fingerprint = wire.computed_schema_fingerprint().map_err(|error| {
            ClientCompileError::manifest(
                "client.manifest.schema_fingerprint",
                format!("could not recompute manifest schema fingerprint: {error}"),
            )
        })?;
        if wire.manifest_version != DISTRIBUTED_CLIENT_MANIFEST_VERSION {
            return Err(ClientCompileError::manifest(
                "client.manifest.version",
                format!(
                    "client compiler requires manifest_version {DISTRIBUTED_CLIENT_MANIFEST_VERSION}, received {}",
                    wire.manifest_version
                ),
            ));
        }
        if wire.protocol_version != DISTRIBUTED_CLIENT_PROTOCOL_VERSION {
            return Err(ClientCompileError::manifest(
                "client.manifest.protocol_version",
                format!(
                    "client compiler requires protocol_version {DISTRIBUTED_CLIENT_PROTOCOL_VERSION}, received {}",
                    wire.protocol_version
                ),
            ));
        }
        validate_nonempty(&wire.service_id, "manifest.service_id")?;
        validate_hash(&wire.schema_fingerprint, "manifest.schema_fingerprint")?;
        validate_hash(&wire.protocol_fingerprint, "manifest.protocol_fingerprint")?;
        validate_execution_limits(&wire.execution)?;
        let protocol_fingerprint = client_protocol_fingerprint().map_err(|error| {
            ClientCompileError::manifest("client.manifest.protocol_fingerprint", error.to_string())
        })?;
        if wire.protocol_fingerprint != protocol_fingerprint {
            return Err(ClientCompileError::manifest(
                "client.manifest.protocol_fingerprint",
                format!(
                    "client compiler protocol contract is `{protocol_fingerprint}`, received `{}`; regenerate the manifest and use a matching distributed version",
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
        let trusted_presets =
            trusted_preset_descriptors(models.values(), &commands).map_err(|error| {
                ClientCompileError::manifest(
                    "client.manifest.trusted_preset_inventory",
                    error.to_string(),
                )
            })?;
        let leaderboards = external_leaderboards(&wire.external)?;
        validate_leaderboards(&leaderboards, &roots, &typenames)?;
        let live_leaderboards = leaderboards
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
            leaderboards,
        })
    }

    pub(crate) fn root(&self, operation: ClientRootOperation, name: &str) -> Option<&ClientRoot> {
        self.roots.get(&(operation, name.to_string()))
    }
}
