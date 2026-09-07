use std::collections::{BTreeSet, HashSet};

use serde::{Deserialize, Serialize};

use super::command::CommandSpec;
use super::error::{ApplicationError, ApplicationResult};
use super::identity::{canonical_json, sha256_fingerprint, LogicalId};
use super::module::{ModelSpec, Module, ModuleManifest, ProjectionSpec, SurfaceSpec};

/// Wire/schema version for the complete logical application manifest.
pub const APPLICATION_MANIFEST_SCHEMA_VERSION: u32 = 1;

/// Bounds applied before a portable application artifact is accepted.
///
/// A complete manifest intentionally carries authoritative module declarations,
/// their flattened application inventory, and selected Surface contracts. Keep
/// the total bounded while leaving room for a production-sized application.
pub const MAX_APPLICATION_MANIFEST_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_MANIFEST_COLLECTION_ITEMS: usize = 4096;
pub const MAX_MANIFEST_STRING_BYTES: usize = 4096;
/// Per-value JSON bound. Several independently bounded contracts may comprise
/// one application manifest, but no opaque value receives the whole budget.
pub const MAX_MANIFEST_JSON_BYTES: usize = 1024 * 1024;
pub const MAX_MANIFEST_JSON_DEPTH: usize = 32;
/// Reserved manifest extension carrying the framework release that compiled it.
pub const FRAMEWORK_COMPATIBILITY_EXTENSION_ID: &str = "distributed.framework";
/// Schema version for the framework compatibility extension value.
pub const FRAMEWORK_COMPATIBILITY_EXTENSION_VERSION: u32 = 1;

/// Framework-owned release identity emitted by typed application introspection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FrameworkCompatibility {
    pub release: String,
}

impl FrameworkCompatibility {
    pub fn current() -> Self {
        Self {
            release: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestFingerprint {
    pub logical: String,
    pub canonical: String,
}

/// Portable provenance carried by the artifact envelope. Provenance is part
/// of canonical artifact bytes, but volatile source metadata is deliberately
/// excluded from the logical fingerprint (see [`ApplicationManifest::logical_fingerprint`]).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestProvenance {
    pub generator: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
    #[serde(default)]
    pub sources: Vec<String>,
}

impl Default for ManifestProvenance {
    fn default() -> Self {
        Self {
            generator: "distributed.application.compiler.v1".into(),
            source_revision: None,
            sources: Vec::new(),
        }
    }
}

/// A named, versioned extension value in the explicit application
/// declaration. Extensions are data-only and are included in application
/// identity; executable/runtime configuration must use a deployment layer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationExtension {
    pub id: String,
    pub version: u32,
    pub value: serde_json::Value,
}

impl ApplicationExtension {
    pub fn try_new(
        id: impl Into<String>,
        version: u32,
        value: serde_json::Value,
    ) -> ApplicationResult<Self> {
        let id = LogicalId::try_new("application extension", id)?.into_string();
        if version == 0 {
            return Err(ApplicationError::InvalidSpec(
                "application extension version must be non-zero".into(),
            ));
        }
        let extension = Self { id, version, value };
        validate_json_contract("application extension", &extension.value)?;
        Ok(extension)
    }
}

/// The sole complete logical application manifest owner.
///
/// Physical tables, service endpoints, transports, observability, and
/// executable handlers are intentionally absent. Those belong to named
/// schema/deployment/runtime layers and cannot become portable application
/// identity by accident.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationManifest {
    /// This field is required during decoding: canonical input may not omit
    /// the explicit schema version and receive a legacy default.
    pub schema_version: u32,
    pub name: String,
    #[serde(default)]
    pub modules: Vec<ModuleManifest>,
    #[serde(default)]
    pub commands: Vec<CommandSpec>,
    #[serde(default)]
    pub events: Vec<super::command::EventSpec>,
    #[serde(default)]
    pub projections: Vec<ProjectionSpec>,
    #[serde(default)]
    pub models: Vec<ModelSpec>,
    #[serde(default)]
    pub surfaces: Vec<SurfaceSpec>,
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    #[serde(default)]
    pub extensions: Vec<ApplicationExtension>,
    #[serde(default)]
    pub fingerprints: ManifestFingerprint,
    #[serde(default)]
    pub provenance: ManifestProvenance,
}

impl ApplicationManifest {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            schema_version: APPLICATION_MANIFEST_SCHEMA_VERSION,
            name: name.into(),
            modules: Vec::new(),
            commands: Vec::new(),
            events: Vec::new(),
            projections: Vec::new(),
            models: Vec::new(),
            surfaces: Vec::new(),
            required_capabilities: Vec::new(),
            extensions: vec![framework_compatibility_extension()],
            fingerprints: ManifestFingerprint::default(),
            provenance: ManifestProvenance::default(),
        }
    }

    /// Compile explicit modules and logical selected surfaces into one
    /// manifest owner. A concrete `Surface` must first be compiled into a
    /// `SurfaceSpec` by the shared contract compiler.
    pub fn try_from_modules(
        name: impl Into<String>,
        modules: impl IntoIterator<Item = Module>,
        surfaces: impl IntoIterator<Item = SurfaceSpec>,
    ) -> ApplicationResult<Self> {
        let modules = modules.into_iter().collect::<Vec<_>>();
        let mut module_manifests = modules
            .iter()
            .map(|module| module.manifest().clone())
            .collect::<Vec<_>>();
        module_manifests.sort_by(|left, right| left.id.cmp(&right.id));
        validate_unique_ids(
            "module",
            module_manifests.iter().map(|module| module.id.clone()),
        )?;

        let mut commands = Vec::new();
        let mut events = Vec::new();
        let mut projections = Vec::new();
        let mut models = Vec::new();
        let mut required_capabilities = Vec::new();
        let mut module_surfaces = Vec::new();
        for module in &module_manifests {
            commands.extend(module.commands.clone());
            events.extend(module.events.clone());
            projections.extend(module.projections.clone());
            models.extend(module.models.clone());
            required_capabilities.extend(module.required_capabilities.clone());
            module_surfaces.extend(module.surfaces.clone());
        }
        let explicit_surfaces = surfaces.into_iter().collect::<Vec<_>>();
        let mut surfaces = module_surfaces;
        surfaces.extend(explicit_surfaces);
        surfaces.sort_by(|left, right| left.id.cmp(&right.id));
        surfaces = dedup_surfaces(surfaces)?;
        projections.extend(
            surfaces
                .iter()
                .flat_map(|surface| surface.projections.iter().cloned()),
        );
        models.extend(
            surfaces
                .iter()
                .flat_map(|surface| surface.models.iter().cloned()),
        );

        let mut manifest = Self::new(name);
        manifest.modules = module_manifests;
        manifest.commands = dedup_commands(commands)?;
        manifest.events = dedup_events(events)?;
        manifest.projections = dedup_projections(projections)?;
        manifest.models = dedup_models(models)?;
        manifest.surfaces = surfaces;
        manifest.required_capabilities = required_capabilities;
        manifest.canonicalize_collections();
        manifest.validate()?;
        manifest.refresh_fingerprints()?;
        Ok(manifest)
    }

    pub fn with_provenance(mut self, provenance: ManifestProvenance) -> Self {
        self.provenance = provenance;
        self.fingerprints = ManifestFingerprint::default();
        self
    }

    pub fn with_source_revision(mut self, revision: impl Into<String>) -> Self {
        self.provenance.source_revision = Some(revision.into());
        self.fingerprints = ManifestFingerprint::default();
        self
    }

    pub fn with_extension(mut self, extension: ApplicationExtension) -> Self {
        self.extensions.push(extension);
        self.fingerprints = ManifestFingerprint::default();
        self
    }

    /// Return the framework release record injected by [`ApplicationManifest::new`].
    pub fn framework_compatibility(&self) -> ApplicationResult<FrameworkCompatibility> {
        let extension = self
            .extensions
            .iter()
            .find(|extension| extension.id == FRAMEWORK_COMPATIBILITY_EXTENSION_ID)
            .ok_or_else(|| {
                ApplicationError::InvalidSpec(
                    "application manifest is missing framework compatibility".into(),
                )
            })?;
        if extension.version != FRAMEWORK_COMPATIBILITY_EXTENSION_VERSION {
            return Err(ApplicationError::InvalidSpec(format!(
                "framework compatibility extension version must be {FRAMEWORK_COMPATIBILITY_EXTENSION_VERSION}"
            )));
        }
        let compatibility: FrameworkCompatibility = serde_json::from_value(extension.value.clone())
            .map_err(|error| {
                ApplicationError::InvalidSpec(format!(
                    "invalid framework compatibility record: {error}"
                ))
            })?;
        validate_portable_text("framework release", &compatibility.release)?;
        Ok(compatibility)
    }

    pub fn module_ids(&self) -> Vec<&str> {
        self.modules
            .iter()
            .map(|module| module.id.as_str())
            .collect()
    }

    /// Return exact deterministic manifest bytes, including the explicit
    /// schema version and complete fingerprint material.
    pub fn canonical_bytes(&self) -> ApplicationResult<Vec<u8>> {
        // Encoding is also an acceptance boundary: an in-memory attacker may
        // not manufacture a byte artifact with empty nested fingerprints and
        // rely on the outer manifest fingerprint to make it look complete.
        self.validate_inner(true, false)?;
        let mut canonical = self.clone();
        canonical.canonicalize_collections();
        canonical.fingerprints = ManifestFingerprint::default();
        let mut logical = canonical.clone();
        logical.provenance = ManifestProvenance::default();
        logical
            .extensions
            .retain(|extension| extension.id != FRAMEWORK_COMPATIBILITY_EXTENSION_ID);
        let logical_bytes = serde_json::to_vec(&canonical_json(&serde_json::to_value(&logical)?))?;
        canonical.fingerprints.logical = sha256_fingerprint(&logical_bytes);
        let canonical_without_canonical =
            serde_json::to_vec(&canonical_json(&serde_json::to_value(&canonical)?))?;
        canonical.fingerprints.canonical = sha256_fingerprint(&canonical_without_canonical);
        let bytes = serde_json::to_vec(&canonical_json(&serde_json::to_value(&canonical)?))?;
        if bytes.len() > MAX_APPLICATION_MANIFEST_BYTES {
            return Err(ApplicationError::InvalidSpec(format!(
                "application manifest exceeds {MAX_APPLICATION_MANIFEST_BYTES} bytes"
            )));
        }
        Ok(bytes)
    }

    pub fn refresh_fingerprints(&mut self) -> ApplicationResult<()> {
        self.fingerprints = ManifestFingerprint::default();
        let bytes = self.canonical_bytes()?;
        let canonical: Self = serde_json::from_slice(&bytes)
            .map_err(|error| ApplicationError::Canonical(error.to_string()))?;
        self.fingerprints = canonical.fingerprints;
        Ok(())
    }

    pub fn encode(&self) -> ApplicationResult<Vec<u8>> {
        self.canonical_bytes()
    }

    pub fn from_canonical_bytes(bytes: &[u8]) -> ApplicationResult<Self> {
        if bytes.is_empty() || bytes.len() > MAX_APPLICATION_MANIFEST_BYTES {
            return Err(ApplicationError::InvalidSpec(format!(
                "application manifest bytes must be between 1 and {MAX_APPLICATION_MANIFEST_BYTES}"
            )));
        }
        let manifest: Self = serde_json::from_slice(bytes)
            .map_err(|error| ApplicationError::Canonical(error.to_string()))?;
        if manifest.schema_version != APPLICATION_MANIFEST_SCHEMA_VERSION {
            return Err(ApplicationError::UnsupportedVersion {
                expected: APPLICATION_MANIFEST_SCHEMA_VERSION,
                actual: manifest.schema_version,
            });
        }
        manifest.validate_inner(true, true)?;
        if manifest.canonical_bytes()? != bytes {
            return Err(ApplicationError::NonCanonical("application manifest"));
        }
        Ok(manifest)
    }

    pub fn decode(bytes: &[u8]) -> ApplicationResult<Self> {
        Self::from_canonical_bytes(bytes)
    }

    pub fn fingerprint(&self) -> ApplicationResult<String> {
        let bytes = self.canonical_bytes()?;
        let manifest = Self::from_canonical_bytes(&bytes)?;
        Ok(manifest.fingerprints.canonical)
    }

    pub fn validate(&self) -> ApplicationResult<()> {
        self.validate_inner(true, false)
    }

    /// Return the logical identity, excluding generator/source provenance.
    /// The canonical artifact fingerprint still changes when those fields do.
    pub fn logical_fingerprint(&self) -> ApplicationResult<String> {
        let bytes = self.canonical_bytes()?;
        let manifest = Self::from_canonical_bytes(&bytes)?;
        Ok(manifest.fingerprints.logical)
    }

    fn validate_inner(
        &self,
        require_nested_fingerprints: bool,
        require_manifest_fingerprints: bool,
    ) -> ApplicationResult<()> {
        if self.schema_version != APPLICATION_MANIFEST_SCHEMA_VERSION {
            return Err(ApplicationError::UnsupportedVersion {
                expected: APPLICATION_MANIFEST_SCHEMA_VERSION,
                actual: self.schema_version,
            });
        }
        LogicalId::try_new("application", self.name.clone())?;
        validate_collection_len("modules", self.modules.len())?;
        validate_collection_len("commands", self.commands.len())?;
        validate_collection_len("events", self.events.len())?;
        validate_collection_len("projections", self.projections.len())?;
        validate_collection_len("models", self.models.len())?;
        validate_collection_len("surfaces", self.surfaces.len())?;
        validate_collection_len("extensions", self.extensions.len())?;
        validate_unique_ids(
            "module",
            self.modules.iter().map(|module| module.id.clone()),
        )?;
        validate_unique_ids(
            "command",
            self.commands.iter().map(|command| command.id.clone()),
        )?;
        validate_unique_ids("event", self.events.iter().map(|event| event.name.clone()))?;
        validate_unique_ids(
            "projection",
            self.projections
                .iter()
                .map(|projection| projection.id.clone()),
        )?;
        validate_unique_ids("model", self.models.iter().map(|model| model.id.clone()))?;
        validate_unique_ids(
            "surface",
            self.surfaces.iter().map(|surface| surface.id.clone()),
        )?;

        let model_ids = self
            .models
            .iter()
            .map(|model| model.id.as_str())
            .collect::<HashSet<_>>();
        let projection_ids = self
            .projections
            .iter()
            .map(|projection| projection.id.as_str())
            .collect::<HashSet<_>>();
        for module in &self.modules {
            validate_module(module, require_nested_fingerprints)?;
        }
        for command in &self.commands {
            command.validate()?;
            validate_fingerprint(
                "command",
                &command.id,
                &command.fingerprint,
                command,
                require_nested_fingerprints,
            )?;
            if let Some(model) = &command.projected_model {
                require_reference("model", model, &model_ids)?;
            }
        }
        for event in &self.events {
            validate_event(event)?;
        }
        for projection in &self.projections {
            validate_projection(
                projection,
                &model_ids,
                &projection_ids,
                require_nested_fingerprints,
            )?;
        }
        for model in &self.models {
            validate_model(model, &model_ids, require_nested_fingerprints)?;
        }
        for surface in &self.surfaces {
            validate_surface(
                surface,
                &model_ids,
                &projection_ids,
                require_nested_fingerprints,
            )?;
        }
        for capability in &self.required_capabilities {
            validate_portable_text("capability", capability)?;
        }
        let mut extension_ids = BTreeSet::new();
        for extension in &self.extensions {
            if !extension_ids.insert(extension.id.clone()) {
                return Err(ApplicationError::Duplicate {
                    kind: "application extension",
                    identity: extension.id.clone(),
                });
            }
            LogicalId::try_new("application extension", extension.id.clone())?;
            if extension.version == 0 {
                return Err(ApplicationError::InvalidSpec(
                    "application extension version must be non-zero".into(),
                ));
            }
            validate_json_contract("application extension", &extension.value)?;
        }
        let compatibility = self.framework_compatibility()?;
        if compatibility != FrameworkCompatibility::current() {
            return Err(ApplicationError::InvalidSpec(format!(
                "application manifest framework release `{}` does not match compiling framework `{}`",
                compatibility.release,
                env!("CARGO_PKG_VERSION")
            )));
        }
        validate_portable_text("manifest generator", &self.provenance.generator)?;
        if let Some(revision) = &self.provenance.source_revision {
            validate_artifact_text("source revision", revision)?;
        }
        for source in &self.provenance.sources {
            validate_artifact_text("manifest source", source)?;
        }

        let expected = expected_fingerprints(self)?;
        if require_manifest_fingerprints
            && (self.fingerprints.logical.is_empty() || self.fingerprints.canonical.is_empty())
        {
            return Err(ApplicationError::NonCanonical(
                "application manifest fingerprint material",
            ));
        }
        if (!self.fingerprints.logical.is_empty() && self.fingerprints.logical != expected.logical)
            || (!self.fingerprints.canonical.is_empty()
                && self.fingerprints.canonical != expected.canonical)
        {
            return Err(ApplicationError::NonCanonical(
                "application manifest fingerprint material",
            ));
        }
        validate_manifest_ownership(self)?;
        Ok(())
    }

    fn canonicalize_collections(&mut self) {
        self.modules.sort_by(|left, right| left.id.cmp(&right.id));
        self.commands.sort_by(|left, right| left.id.cmp(&right.id));
        self.events.sort_by(|left, right| {
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
        self.projections
            .sort_by(|left, right| left.id.cmp(&right.id));
        self.models.sort_by(|left, right| left.id.cmp(&right.id));
        self.surfaces.sort_by(|left, right| left.id.cmp(&right.id));
        self.required_capabilities.sort();
        self.required_capabilities.dedup();
        self.extensions.sort_by(|left, right| {
            (left.id.as_str(), left.version).cmp(&(right.id.as_str(), right.version))
        });
        self.provenance.sources.sort();
        self.provenance.sources.dedup();
    }
}

fn expected_fingerprints(manifest: &ApplicationManifest) -> ApplicationResult<ManifestFingerprint> {
    let mut canonical = manifest.clone();
    canonical.canonicalize_collections();
    canonical.fingerprints = ManifestFingerprint::default();
    let mut logical = canonical.clone();
    logical.provenance = ManifestProvenance::default();
    logical
        .extensions
        .retain(|extension| extension.id != FRAMEWORK_COMPATIBILITY_EXTENSION_ID);
    let logical_bytes = serde_json::to_vec(&canonical_json(&serde_json::to_value(&logical)?))?;
    let logical = sha256_fingerprint(&logical_bytes);
    canonical.fingerprints.logical = logical.clone();
    let with_logical = serde_json::to_vec(&canonical_json(&serde_json::to_value(&canonical)?))?;
    let canonical_fingerprint = sha256_fingerprint(&with_logical);
    Ok(ManifestFingerprint {
        logical,
        canonical: canonical_fingerprint,
    })
}

fn framework_compatibility_extension() -> ApplicationExtension {
    ApplicationExtension {
        id: FRAMEWORK_COMPATIBILITY_EXTENSION_ID.to_string(),
        version: FRAMEWORK_COMPATIBILITY_EXTENSION_VERSION,
        value: serde_json::json!({
            "release": env!("CARGO_PKG_VERSION"),
        }),
    }
}

fn validate_module(
    module: &ModuleManifest,
    require_nested_fingerprints: bool,
) -> ApplicationResult<()> {
    LogicalId::try_new("module", module.id.clone())?;
    validate_collection_len("module commands", module.commands.len())?;
    validate_collection_len("module events", module.events.len())?;
    validate_collection_len("module projections", module.projections.len())?;
    validate_collection_len("module models", module.models.len())?;
    validate_collection_len("module surfaces", module.surfaces.len())?;
    validate_collection_len(
        "module required capabilities",
        module.required_capabilities.len(),
    )?;
    validate_fingerprint(
        "module",
        &module.id,
        &module.fingerprint,
        module,
        require_nested_fingerprints,
    )?;
    validate_unique_ids(
        "module command",
        module.commands.iter().map(|item| item.id.clone()),
    )?;
    validate_unique_ids(
        "module projection",
        module.projections.iter().map(|item| item.id.clone()),
    )?;
    validate_unique_ids(
        "module event",
        module.events.iter().map(|item| item.name.clone()),
    )?;
    validate_unique_ids(
        "module model",
        module.models.iter().map(|item| item.id.clone()),
    )?;
    validate_unique_ids(
        "module surface",
        module.surfaces.iter().map(|item| item.id.clone()),
    )?;
    validate_sorted_unique(
        "module commands",
        &module
            .commands
            .iter()
            .map(|item| item.id.clone())
            .collect::<Vec<_>>(),
    )?;
    validate_sorted_unique(
        "module events",
        &module
            .events
            .iter()
            .map(|item| item.name.clone())
            .collect::<Vec<_>>(),
    )?;
    validate_sorted_unique(
        "module projections",
        &module
            .projections
            .iter()
            .map(|item| item.id.clone())
            .collect::<Vec<_>>(),
    )?;
    validate_sorted_unique(
        "module models",
        &module
            .models
            .iter()
            .map(|item| item.id.clone())
            .collect::<Vec<_>>(),
    )?;
    validate_sorted_unique(
        "module surfaces",
        &module
            .surfaces
            .iter()
            .map(|item| item.id.clone())
            .collect::<Vec<_>>(),
    )?;
    let module_model_ids = module
        .models
        .iter()
        .map(|model| model.id.as_str())
        .chain(
            module
                .surfaces
                .iter()
                .flat_map(|surface| surface.models.iter().map(|model| model.id.as_str())),
        )
        .collect::<HashSet<_>>();
    let module_projection_ids = module
        .projections
        .iter()
        .map(|projection| projection.id.as_str())
        .chain(module.surfaces.iter().flat_map(|surface| {
            surface
                .projections
                .iter()
                .map(|projection| projection.id.as_str())
        }))
        .collect::<HashSet<_>>();
    for command in &module.commands {
        command.validate()?;
        validate_fingerprint(
            "command",
            &command.id,
            &command.fingerprint,
            command,
            require_nested_fingerprints,
        )?;
    }
    let emitted_events = dedup_events(
        module
            .commands
            .iter()
            .flat_map(|command| command.emits.iter().cloned())
            .collect(),
    )?;
    if emitted_events != module.events {
        return Err(ApplicationError::Collision {
            kind: "event",
            identity: module.id.clone(),
            reason: "module event inventory is not closed over command declarations".into(),
        });
    }
    for event in &module.events {
        validate_event(event)?;
    }
    for projection in &module.projections {
        validate_projection(
            projection,
            &module_model_ids,
            &module_projection_ids,
            require_nested_fingerprints,
        )?;
    }
    for surface in &module.surfaces {
        validate_surface(
            surface,
            &module_model_ids,
            &module_projection_ids,
            require_nested_fingerprints,
        )?;
    }
    for model in &module.models {
        validate_model(model, &module_model_ids, require_nested_fingerprints)?;
    }
    for capability in &module.required_capabilities {
        validate_portable_text("module capability", capability)?;
    }
    Ok(())
}

fn validate_event(event: &super::command::EventSpec) -> ApplicationResult<()> {
    LogicalId::try_new("event", event.name.clone())?;
    if event.version == 0 || event.body_version == 0 || event.body_codec_version == 0 {
        return Err(ApplicationError::InvalidSpec(format!(
            "event `{}` versions must be non-zero",
            event.name
        )));
    }
    validate_portable_text("event body type", &event.body_type)?;
    validate_portable_text("event body schema", &event.body_schema)?;
    validate_sha256_text("event body fingerprint", &event.body_fingerprint)?;
    validate_portable_text("event body codec", &event.body_codec)?;
    Ok(())
}

fn validate_projection(
    projection: &ProjectionSpec,
    model_ids: &HashSet<&str>,
    projection_ids: &HashSet<&str>,
    require_nested_fingerprints: bool,
) -> ApplicationResult<()> {
    LogicalId::try_new("projection", projection.id.clone())?;
    validate_fingerprint(
        "projection",
        &projection.id,
        &projection.fingerprint,
        projection,
        require_nested_fingerprints,
    )?;
    validate_collection_len("projection facts", projection.facts.len())?;
    validate_collection_len("projection models", projection.models.len())?;
    validate_collection_len("projection dependencies", projection.dependencies.len())?;
    validate_collection_len("modeled projections", projection.modeled.len())?;
    validate_sorted_unique("projection facts", &projection.facts)?;
    validate_sorted_unique("projection models", &projection.models)?;
    validate_sorted_unique("projection dependencies", &projection.dependencies)?;
    validate_sorted_unique(
        "modeled projection program IDs",
        &projection.modeled_programs,
    )?;
    for fact in &projection.facts {
        validate_portable_text("projection fact", fact)?;
    }
    for model in &projection.models {
        validate_portable_text("projection model", model)?;
        require_reference("model", model, model_ids)?;
    }
    validate_json_contract("projection partition", &projection.partition)?;
    let mut modeled_ids = Vec::with_capacity(projection.modeled.len());
    for modeled in &projection.modeled {
        validate_json_contract("modeled projection", modeled)?;
        let fields = modeled.as_object().ok_or_else(|| {
            ApplicationError::InvalidSpec("modeled projection must be an object".into())
        })?;
        let program_id = fields
            .get("program_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                ApplicationError::InvalidSpec(
                    "modeled projection must retain its program identity".into(),
                )
            })?;
        validate_portable_text("modeled projection program ID", program_id)?;
        modeled_ids.push(program_id.to_owned());
        let output_models = fields
            .get("output_models")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| {
                ApplicationError::InvalidSpec(
                    "modeled projection must retain its output model inventory".into(),
                )
            })?;
        for output_model in output_models {
            let output_model = output_model.as_str().ok_or_else(|| {
                ApplicationError::InvalidSpec(
                    "modeled projection output model identities must be strings".into(),
                )
            })?;
            require_reference("model", output_model, model_ids)?;
        }
    }
    modeled_ids.sort();
    if modeled_ids != projection.modeled_programs {
        return Err(ApplicationError::Collision {
            kind: "projection",
            identity: projection.id.clone(),
            reason: "modeled projection program identities are stale or incomplete".into(),
        });
    }
    if let Some(catalog) = &projection.catalog_fingerprint {
        validate_sha256_text("projection catalog fingerprint", catalog)?;
    }
    for dependency in &projection.dependencies {
        if let Some(identity) = dependency.strip_prefix("projection:") {
            LogicalId::try_new("projection", identity.to_owned())?;
            require_reference("projection", identity, projection_ids)?;
        } else {
            validate_portable_text("projection dependency", dependency)?;
        }
    }
    Ok(())
}

fn validate_model(
    model: &ModelSpec,
    model_ids: &HashSet<&str>,
    require_nested_fingerprints: bool,
) -> ApplicationResult<()> {
    LogicalId::try_new("model", model.id.clone())?;
    validate_fingerprint(
        "model",
        &model.id,
        &model.fingerprint,
        model,
        require_nested_fingerprints,
    )?;
    validate_portable_text("model table", &model.table)?;
    validate_portable_text("model object", &model.object)?;
    validate_collection_len("model fields", model.fields.len())?;
    validate_collection_len("model relationships", model.relationships.len())?;
    if model.role_limit.is_some_and(|limit| limit == 0) {
        return Err(ApplicationError::InvalidSpec(format!(
            "model `{}` has a zero role limit",
            model.id
        )));
    }
    validate_row_policy(&model.row_policy)?;
    let field_names = model
        .fields
        .iter()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    validate_sorted_unique("model fields", &field_names)?;
    let relationship_names = model
        .relationships
        .iter()
        .map(|relationship| relationship.name.clone())
        .collect::<Vec<_>>();
    validate_sorted_unique("model relationships", &relationship_names)?;
    if model.primary_key.is_empty() {
        return Err(ApplicationError::InvalidSpec(format!(
            "model `{}` must declare a primary key",
            model.id
        )));
    }
    validate_sorted_unique("model primary key", &model.primary_key)?;
    let field_names = field_names
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    for field in &model.fields {
        validate_portable_text("model field", &field.name)?;
        validate_portable_text("model field scalar", &field.scalar)?;
    }
    for key in &model.primary_key {
        validate_portable_text("model primary key field", key)?;
        if !field_names.contains(key.as_str()) {
            return Err(ApplicationError::Missing {
                kind: "model field",
                identity: key.clone(),
            });
        }
    }
    for relationship in &model.relationships {
        validate_portable_text("relationship name", &relationship.name)?;
        validate_portable_text("relationship target model", &relationship.target_model)?;
        validate_portable_text("relationship target object", &relationship.target_object)?;
        if !matches!(
            relationship.kind.as_str(),
            "hasmany" | "belongsto" | "manytomany"
        ) {
            return Err(ApplicationError::InvalidSpec(format!(
                "relationship `{}` has unknown kind `{}`",
                relationship.name, relationship.kind
            )));
        }
        require_reference("model", &relationship.target_model, model_ids)?;
        validate_collection_len("relationship arguments", relationship.arguments.len())?;
        let argument_names = relationship
            .arguments
            .iter()
            .map(|argument| argument.name.clone())
            .collect::<Vec<_>>();
        validate_unique("relationship arguments", &argument_names)?;
        for argument in &relationship.arguments {
            validate_surface_argument(argument)?;
        }
        for dependency in &relationship.dependencies {
            validate_portable_text("relationship dependency", dependency)?;
        }
        validate_sorted_unique("relationship dependencies", &relationship.dependencies)?;
        validate_json_contract("relationship keys", &relationship.keys)?;
        validate_relationship_keys(&relationship.keys)?;
        if let Some(aggregate) = &relationship.aggregate {
            validate_portable_text("relationship aggregate", &aggregate.name)?;
            validate_portable_text("relationship aggregate type", &aggregate.type_name)?;
            validate_collection_len(
                "relationship aggregate arguments",
                aggregate.arguments.len(),
            )?;
            let argument_names = aggregate
                .arguments
                .iter()
                .map(|argument| argument.name.clone())
                .collect::<Vec<_>>();
            validate_unique("relationship aggregate arguments", &argument_names)?;
            for argument in &aggregate.arguments {
                validate_surface_argument(argument)?;
            }
            for dependency in &aggregate.dependencies {
                validate_portable_text("relationship aggregate dependency", dependency)?;
            }
            validate_sorted_unique(
                "relationship aggregate dependencies",
                &aggregate.dependencies,
            )?;
        }
    }
    Ok(())
}

fn validate_surface(
    surface: &SurfaceSpec,
    model_ids: &HashSet<&str>,
    projection_ids: &HashSet<&str>,
    require_nested_fingerprints: bool,
) -> ApplicationResult<()> {
    LogicalId::try_new("surface", surface.id.clone())?;
    validate_fingerprint(
        "surface",
        &surface.id,
        &surface.fingerprint,
        surface,
        require_nested_fingerprints,
    )?;
    validate_collection_len("surface models", surface.models.len())?;
    validate_collection_len("surface roots", surface.roots.len())?;
    validate_collection_len("surface commands", surface.commands.len())?;
    validate_collection_len("surface projections", surface.projections.len())?;
    validate_sorted_unique(
        "surface models",
        &surface
            .models
            .iter()
            .map(|model| model.id.clone())
            .collect::<Vec<_>>(),
    )?;
    let root_ids = surface
        .roots
        .iter()
        .map(|root| format!("{}:{}", root.operation, root.name))
        .collect::<Vec<_>>();
    validate_sorted_unique("surface roots", &root_ids)?;
    validate_sorted_unique(
        "surface commands",
        &surface
            .commands
            .iter()
            .map(|command| command.id.clone())
            .collect::<Vec<_>>(),
    )?;
    validate_sorted_unique(
        "surface projections",
        &surface
            .projections
            .iter()
            .map(|projection| projection.id.clone())
            .collect::<Vec<_>>(),
    )?;
    validate_json_contract("surface canonical contract", &surface.contract)?;
    validate_surface_selection(surface)?;
    validate_portable_text("surface dialect", &surface.dialect)?;
    if surface.max_limit == 0 || surface.default_limit > surface.max_limit {
        return Err(ApplicationError::InvalidSpec(format!(
            "surface `{}` has invalid pagination bounds",
            surface.id
        )));
    }
    let surface_models = surface
        .models
        .iter()
        .map(|model| model.id.as_str())
        .collect::<HashSet<_>>();
    let model_scope = if model_ids.is_empty() {
        &surface_models
    } else {
        model_ids
    };
    for model in &surface.models {
        validate_model(model, model_scope, require_nested_fingerprints)?;
    }
    for root in &surface.roots {
        if !matches!(root.operation.as_str(), "query" | "subscription") {
            return Err(ApplicationError::InvalidSpec(format!(
                "surface root `{}` has unsupported operation `{}`",
                root.name, root.operation
            )));
        }
        validate_portable_text("surface root operation", &root.operation)?;
        validate_portable_text("surface root name", &root.name)?;
        validate_portable_text("surface root object", &root.object)?;
        if !matches!(root.kind.as_str(), "list" | "by_pk" | "aggregate") {
            return Err(ApplicationError::InvalidSpec(format!(
                "surface root `{}` has unknown kind `{}`",
                root.name, root.kind
            )));
        }
        LogicalId::try_new("surface root model", root.model.clone())?;
        require_reference("model", &root.model, model_scope)?;
        validate_collection_len("surface root arguments", root.arguments.len())?;
        let argument_names = root
            .arguments
            .iter()
            .map(|argument| argument.name.clone())
            .collect::<Vec<_>>();
        validate_unique("surface root arguments", &argument_names)?;
        for argument in &root.arguments {
            validate_surface_argument(argument)?;
        }
        for dependency in &root.dependencies {
            validate_portable_text("surface root dependency", dependency)?;
        }
        if let Some(max) = root.max_limit {
            if max == 0 || root.default_limit.is_some_and(|default| default > max) {
                return Err(ApplicationError::InvalidSpec(format!(
                    "surface root `{}` has invalid pagination bounds",
                    root.name
                )));
            }
        }
    }
    for command in &surface.commands {
        LogicalId::try_new("command", command.id.clone())?;
        validate_portable_text("surface command field", &command.field_name)?;
        validate_roles("surface command role", &command.roles)?;
        if let Some(input) = &command.input {
            validate_command_type_spec(input)?;
        }
        if let Some(output) = &command.output {
            validate_command_type_spec(output)?;
        }
        validate_json_contract("surface command defaults", &command.defaults)?;
        validate_json_contract("surface command effects", &command.effects)?;
        validate_json_contract("surface command confirmations", &command.confirmations)?;
        validate_json_contract(
            "surface command projection contract",
            &command.projection_contract,
        )?;
        validate_json_contract("surface command applies", &command.applies)?;
        if let Some(model) = &command.projected_model {
            require_reference("model", model, model_scope)?;
        }
        if let Some(direct_projection) = &command.direct_projection {
            validate_json_contract("surface command direct projection", direct_projection)?;
        }
    }
    for projection in &surface.projections {
        validate_projection(
            projection,
            model_scope,
            projection_ids,
            require_nested_fingerprints,
        )?;
    }
    validate_surface_contract(surface)?;
    Ok(())
}

fn validate_surface_selection(surface: &SurfaceSpec) -> ApplicationResult<()> {
    match surface.selection.as_str() {
        "catalog" => {
            if !surface.eligible_roles.is_empty() || !surface.schema_roles.is_empty() {
                return Err(ApplicationError::InvalidSpec(format!(
                    "catalog surface `{}` cannot expose roles",
                    surface.id
                )));
            }
        }
        "role" => {
            if surface.eligible_roles.len() != 1
                || surface.schema_roles.len() != 1
                || surface.eligible_roles != surface.schema_roles
            {
                return Err(ApplicationError::InvalidSpec(format!(
                    "role surface `{}` must name exactly one identical eligible and schema role",
                    surface.id
                )));
            }
        }
        value
            if value
                .strip_prefix("application:")
                .is_some_and(|name| !name.is_empty()) =>
        {
            let name = value.strip_prefix("application:").expect("matched above");
            LogicalId::try_new("surface application", name.to_owned())?;
            if surface.eligible_roles.is_empty() {
                return Err(ApplicationError::InvalidSpec(format!(
                    "application surface `{}` must expose at least one eligible role",
                    surface.id
                )));
            }
            if surface.schema_roles.is_empty() {
                return Err(ApplicationError::InvalidSpec(format!(
                    "application surface `{}` must expose at least one schema role",
                    surface.id
                )));
            }
            if surface.schema_roles.iter().any(|role| {
                !surface
                    .eligible_roles
                    .iter()
                    .any(|eligible| eligible == role)
            }) {
                return Err(ApplicationError::InvalidSpec(format!(
                    "application surface `{}` schema roles must be a subset of eligible roles",
                    surface.id
                )));
            }
        }
        _ => {
            return Err(ApplicationError::InvalidSpec(format!(
                "surface `{}` has an invalid selection identity",
                surface.id
            )))
        }
    }
    validate_roles("surface eligible role", &surface.eligible_roles)?;
    validate_roles("surface schema role", &surface.schema_roles)
}

fn validate_roles(kind: &'static str, roles: &[String]) -> ApplicationResult<()> {
    let mut previous: Option<&str> = None;
    for role in roles {
        LogicalId::try_new(kind, role.clone())?;
        if previous.is_some_and(|previous| previous >= role.as_str()) {
            return Err(ApplicationError::NonCanonical("role ordering"));
        }
        previous = Some(role);
    }
    Ok(())
}

fn validate_surface_argument(
    argument: &super::module::SurfaceArgumentSpec,
) -> ApplicationResult<()> {
    validate_portable_text("surface argument", &argument.name)?;
    validate_portable_text("surface argument kind", &argument.kind)?;
    validate_portable_text("surface argument type", &argument.type_name)?;
    if !matches!(
        argument.kind.as_str(),
        "filter" | "order" | "limit" | "offset" | "primary_key"
    ) {
        return Err(ApplicationError::InvalidSpec(format!(
            "surface argument `{}` has unknown kind `{}`",
            argument.name, argument.kind
        )));
    }
    Ok(())
}

fn validate_row_policy(value: &serde_json::Value) -> ApplicationResult<()> {
    let Some(fields) = value.as_object() else {
        return Err(ApplicationError::InvalidSpec(
            "row policy must be a tagged object".into(),
        ));
    };
    let Some(kind) = fields.get("kind").and_then(serde_json::Value::as_str) else {
        return Err(ApplicationError::InvalidSpec(
            "row policy must declare its kind".into(),
        ));
    };
    match kind {
        "unrestricted" | "server_only" if fields.len() == 1 => Ok(()),
        "predicate" if fields.len() == 2 => {
            let expression = fields.get("expression").ok_or_else(|| {
                ApplicationError::InvalidSpec(
                    "predicate row policy must retain its expression".into(),
                )
            })?;
            validate_json_contract("row policy expression", expression)
        }
        _ => Err(ApplicationError::InvalidSpec(
            "row policy contains unknown or redundant material".into(),
        )),
    }
}

fn validate_relationship_keys(value: &serde_json::Value) -> ApplicationResult<()> {
    let Some(fields) = value.as_object() else {
        return Err(ApplicationError::InvalidSpec(
            "relationship keys must be a tagged object".into(),
        ));
    };
    let Some(kind) = fields.get("kind").and_then(serde_json::Value::as_str) else {
        return Err(ApplicationError::InvalidSpec(
            "relationship keys must declare their kind".into(),
        ));
    };
    if kind == "embedded" {
        if fields.len() != 1 {
            return Err(ApplicationError::InvalidSpec(
                "embedded relationship keys contain redundant material".into(),
            ));
        }
        return Ok(());
    }
    if !matches!(kind, "direct" | "through" | "through_opaque") {
        return Err(ApplicationError::InvalidSpec(format!(
            "relationship keys have unknown kind `{kind}`"
        )));
    }
    let local = fields
        .get("local")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            ApplicationError::InvalidSpec("relationship keys need local columns".into())
        })?;
    let remote = fields
        .get("remote")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            ApplicationError::InvalidSpec("relationship keys need remote columns".into())
        })?;
    if local.is_empty() || local.len() != remote.len() {
        return Err(ApplicationError::InvalidSpec(
            "relationship key columns must be non-empty and paired".into(),
        ));
    }
    for column in local.iter().chain(remote) {
        let name = column.as_str().ok_or_else(|| {
            ApplicationError::InvalidSpec("relationship key columns must be strings".into())
        })?;
        validate_portable_text("relationship key column", name)?;
    }
    let expected_len = match kind {
        "direct" => 3,
        "through" => 6,
        "through_opaque" => 4,
        _ => unreachable!(),
    };
    if fields.len() != expected_len {
        return Err(ApplicationError::InvalidSpec(
            "relationship keys contain missing or redundant material".into(),
        ));
    }
    if kind == "through" {
        for field in ["table", "source_foreign_key", "target_foreign_key"] {
            if !fields.contains_key(field) {
                return Err(ApplicationError::InvalidSpec(format!(
                    "through relationship keys need `{field}`"
                )));
            }
        }
        let table = fields
            .get("table")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                ApplicationError::InvalidSpec("relationship key `table` must be a string".into())
            })?;
        validate_portable_text("relationship key identity", table)?;
        validate_through_key_list(fields, "source_foreign_key", local.len())?;
        validate_through_key_list(fields, "target_foreign_key", remote.len())?;
    }
    if kind == "through_opaque" {
        if !fields.contains_key("dependency") {
            return Err(ApplicationError::InvalidSpec(
                "opaque relationship keys need a dependency identity".into(),
            ));
        }
        let value = fields
            .get("dependency")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                ApplicationError::InvalidSpec(
                    "relationship key `dependency` must be a string".into(),
                )
            })?;
        validate_portable_text("relationship key identity", value)?;
    }
    Ok(())
}

fn validate_through_key_list(
    fields: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    expected_len: usize,
) -> ApplicationResult<()> {
    let columns = fields
        .get(field)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            ApplicationError::InvalidSpec(format!(
                "relationship key `{field}` must be a string list"
            ))
        })?;
    if columns.len() != expected_len || columns.is_empty() {
        return Err(ApplicationError::InvalidSpec(format!(
            "relationship key `{field}` must list one through column per identity column"
        )));
    }
    for column in columns {
        let name = column.as_str().ok_or_else(|| {
            ApplicationError::InvalidSpec(format!(
                "relationship key `{field}` columns must be strings"
            ))
        })?;
        validate_portable_text("relationship key identity", name)?;
    }
    Ok(())
}

fn validate_command_type_spec(
    definition: &super::command::CommandTypeSpec,
) -> ApplicationResult<()> {
    validate_command_type_spec_at_depth(definition, 0)
}

fn validate_command_type_spec_at_depth(
    definition: &super::command::CommandTypeSpec,
    depth: usize,
) -> ApplicationResult<()> {
    if depth > MAX_MANIFEST_JSON_DEPTH {
        return Err(ApplicationError::InvalidSpec(format!(
            "surface command type exceeds nesting depth {MAX_MANIFEST_JSON_DEPTH}"
        )));
    }
    validate_portable_text("surface command type", &definition.name)?;
    validate_collection_len("surface command type fields", definition.fields.len())?;
    let field_names = definition
        .fields
        .iter()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    validate_unique("surface command type fields", &field_names)?;
    for field in &definition.fields {
        validate_portable_text("surface command field", &field.name)?;
        validate_portable_text("surface command field type", &field.type_name)?;
        if let Some(nested) = &field.nested {
            validate_command_type_spec_at_depth(nested, depth + 1)?;
        }
    }
    Ok(())
}

fn validate_surface_contract(surface: &SurfaceSpec) -> ApplicationResult<()> {
    let expected = surface_contract_from_spec(surface)?;
    if canonical_json(&surface.contract) != expected {
        return Err(ApplicationError::NonCanonical("surface contract material"));
    }
    Ok(())
}

fn validate_manifest_ownership(manifest: &ApplicationManifest) -> ApplicationResult<()> {
    let mut module_commands = manifest
        .modules
        .iter()
        .flat_map(|module| module.commands.iter().cloned())
        .collect::<Vec<_>>();
    module_commands.sort_by(|left, right| left.id.cmp(&right.id));
    let module_commands = dedup_commands(module_commands)?;
    if module_commands != manifest.commands {
        return Err(ApplicationError::Collision {
            kind: "command",
            identity: manifest.name.clone(),
            reason: "application command inventory does not equal explicit module ownership".into(),
        });
    }

    let mut module_events = manifest
        .modules
        .iter()
        .flat_map(|module| module.events.iter().cloned())
        .collect::<Vec<_>>();
    let module_events = dedup_events(std::mem::take(&mut module_events))?;
    if module_events != manifest.events {
        return Err(ApplicationError::Collision {
            kind: "event",
            identity: manifest.name.clone(),
            reason: "application event inventory does not equal explicit module ownership".into(),
        });
    }

    let mut owned_projections = manifest
        .modules
        .iter()
        .flat_map(|module| module.projections.iter().cloned())
        .chain(
            manifest
                .surfaces
                .iter()
                .flat_map(|surface| surface.projections.iter().cloned()),
        )
        .collect::<Vec<_>>();
    let owned_projections = dedup_projections(std::mem::take(&mut owned_projections))?;
    if owned_projections != manifest.projections {
        return Err(ApplicationError::Collision {
            kind: "projection",
            identity: manifest.name.clone(),
            reason: "application projection inventory is not closed over modules and surfaces"
                .into(),
        });
    }

    let mut owned_models = manifest
        .modules
        .iter()
        .flat_map(|module| module.models.iter().cloned())
        .chain(
            manifest
                .surfaces
                .iter()
                .flat_map(|surface| surface.models.iter().cloned()),
        )
        .collect::<Vec<_>>();
    let owned_models = dedup_models(std::mem::take(&mut owned_models))?;
    if owned_models != manifest.models {
        return Err(ApplicationError::Collision {
            kind: "model",
            identity: manifest.name.clone(),
            reason: "application model inventory is not closed over modules and surfaces".into(),
        });
    }

    for module in &manifest.modules {
        for owned_surface in &module.surfaces {
            let Some(surface) = manifest
                .surfaces
                .iter()
                .find(|surface| surface.id == owned_surface.id)
            else {
                return Err(ApplicationError::Missing {
                    kind: "surface",
                    identity: owned_surface.id.clone(),
                });
            };
            if surface != owned_surface {
                return Err(ApplicationError::Collision {
                    kind: "surface",
                    identity: surface.id.clone(),
                    reason: "surface ownership has incompatible canonical material".into(),
                });
            }
        }
    }

    let commands = manifest
        .commands
        .iter()
        .map(|command| (command.id.as_str(), command))
        .collect::<std::collections::BTreeMap<_, _>>();
    let models = manifest
        .models
        .iter()
        .map(|model| (model.id.as_str(), model))
        .collect::<std::collections::BTreeMap<_, _>>();
    let projections = manifest
        .projections
        .iter()
        .map(|projection| (projection.id.as_str(), projection))
        .collect::<std::collections::BTreeMap<_, _>>();
    for surface in &manifest.surfaces {
        for exposed in &surface.models {
            let Some(authoritative) = models.get(exposed.id.as_str()) else {
                return Err(ApplicationError::Missing {
                    kind: "model",
                    identity: exposed.id.clone(),
                });
            };
            if *authoritative != exposed {
                return Err(ApplicationError::Collision {
                    kind: "model",
                    identity: exposed.id.clone(),
                    reason: "surface model differs from the application model declaration".into(),
                });
            }
        }
        for exposed in &surface.projections {
            let Some(authoritative) = projections.get(exposed.id.as_str()) else {
                return Err(ApplicationError::Missing {
                    kind: "projection",
                    identity: exposed.id.clone(),
                });
            };
            if *authoritative != exposed {
                return Err(ApplicationError::Collision {
                    kind: "projection",
                    identity: exposed.id.clone(),
                    reason:
                        "surface projection differs from the application projection declaration"
                            .into(),
                });
            }
        }
        let expected_command_ids = surface_command_closure(surface, &manifest.commands)?;
        let actual_command_ids = surface
            .commands
            .iter()
            .map(|command| command.id.clone())
            .collect::<Vec<_>>();
        if actual_command_ids != expected_command_ids {
            return Err(ApplicationError::Collision {
                kind: "command",
                identity: surface.id.clone(),
                reason: "surface command inventory is not the exact authorized command closure"
                    .into(),
            });
        }
        for exposed in &surface.commands {
            let Some(authoritative) = commands.get(exposed.id.as_str()) else {
                return Err(ApplicationError::Missing {
                    kind: "command",
                    identity: exposed.id.clone(),
                });
            };
            let expected_roles = surface_command_roles(surface, authoritative);
            validate_surface_command_ownership(exposed, authoritative, &expected_roles)?;
        }
        for root in &surface.roots {
            if !models.contains_key(root.model.as_str()) {
                return Err(ApplicationError::Missing {
                    kind: "model",
                    identity: root.model.clone(),
                });
            }
        }
    }
    Ok(())
}

fn validate_surface_command_ownership(
    exposed: &super::module::SurfaceCommandSpec,
    authoritative: &super::command::CommandSpec,
    expected_roles: &[String],
) -> ApplicationResult<()> {
    if exposed.field_name != authoritative.field_name
        || exposed.roles != expected_roles
        || exposed.input.as_ref() != Some(&authoritative.input)
        || exposed.output.as_ref() != Some(&authoritative.output)
        || exposed.consistency != authoritative.consistency
        || canonical_json(&exposed.defaults) != canonical_json(&authoritative.defaults)
        || canonical_json(&exposed.effects) != canonical_json(&authoritative.effects)
        || canonical_json(&exposed.applies) != canonical_json(&authoritative.applies)
        || canonical_json(&exposed.projection_contract)
            != canonical_json(&authoritative.projection_contract)
        || exposed.direct_projection != authoritative.direct_projection
        || exposed.projected_model != authoritative.projected_model
    {
        return Err(ApplicationError::Collision {
            kind: "command",
            identity: authoritative.id.clone(),
            reason: "surface command is not compatible with its application declaration".into(),
        });
    }
    let authoritative_confirmations = serde_json::to_value(&authoritative.confirmations)?;
    if canonical_json(&exposed.confirmations) != canonical_json(&authoritative_confirmations) {
        return Err(ApplicationError::Collision {
            kind: "command",
            identity: authoritative.id.clone(),
            reason: "surface command confirmation material is stale".into(),
        });
    }
    Ok(())
}

fn surface_command_closure(
    surface: &SurfaceSpec,
    commands: &[super::command::CommandSpec],
) -> ApplicationResult<Vec<String>> {
    let mut expected = commands
        .iter()
        .filter(|command| match surface.selection.as_str() {
            "catalog" => true,
            "role" => surface.eligible_roles.first().is_some_and(|role| {
                command.roles.is_empty() || command.roles.iter().any(|allowed| allowed == role)
            }),
            value if value.starts_with("application:") => {
                command.roles.is_empty()
                    || surface
                        .schema_roles
                        .iter()
                        .all(|role| command.roles.iter().any(|allowed| allowed == role))
            }
            _ => false,
        })
        .map(|command| command.id.clone())
        .collect::<Vec<_>>();
    expected.sort();
    expected.dedup();
    Ok(expected)
}

fn surface_command_roles(
    surface: &SurfaceSpec,
    authoritative: &super::command::CommandSpec,
) -> Vec<String> {
    match surface.selection.as_str() {
        "catalog" => authoritative.roles.clone(),
        "role" => surface.eligible_roles.clone(),
        value if value.starts_with("application:") => surface.eligible_roles.clone(),
        _ => Vec::new(),
    }
}

fn surface_contract_from_spec(surface: &SurfaceSpec) -> ApplicationResult<serde_json::Value> {
    let selection = match surface.selection.as_str() {
        "catalog" => serde_json::json!({"kind": "catalog"}),
        "role" => serde_json::json!({
            "kind": "role",
            "name": surface.eligible_roles.first().ok_or_else(|| {
                ApplicationError::InvalidSpec("role surface has no role identity".into())
            })?,
        }),
        value => serde_json::json!({
            "kind": "application",
            "name": value.strip_prefix("application:").ok_or_else(|| {
                ApplicationError::InvalidSpec("surface selection is not canonical".into())
            })?,
            "eligible_roles": surface.eligible_roles,
            "schema_roles": surface.schema_roles,
        }),
    };
    let models = surface
        .models
        .iter()
        .map(|model| {
            serde_json::json!({
                "model_name": model.id,
                "table_name": model.table,
                "object_name": model.object,
                "columns": model.fields,
                "relationships": model.relationships,
                "primary_key": model.primary_key,
                "row_policy": model.row_policy,
                "role_limit": model.role_limit,
                "aggregations": model.aggregations,
            })
        })
        .collect::<Vec<_>>();
    let roots = surface
        .roots
        .iter()
        .map(|root| {
            serde_json::json!({
                "operation": root.operation,
                "name": root.name,
                "kind": root.kind,
                "object": root.object,
                "model_name": root.model,
                "arguments": root.arguments,
                "dependencies": root.dependencies,
                "default_limit": root.default_limit,
                "max_limit": root.max_limit,
            })
        })
        .collect::<Vec<_>>();
    let commands = surface
        .commands
        .iter()
        .map(|command| {
            serde_json::json!({
                "command_name": command.id,
                "field_name": command.field_name,
                "roles": command.roles,
                "input": surface_command_type_value(command.input.as_ref()),
                "output": surface_command_type_value(command.output.as_ref()),
                "consistency": command.consistency,
                "input_defaults": command.defaults,
                "effects": command.effects,
                "confirmations": command.confirmations,
                "projected_model": command.projected_model,
                "direct_projection": command.direct_projection,
                "projections": command.projection_contract,
                "confirmation_unavailable": command.confirmation_unavailable,
            })
        })
        .collect::<Vec<_>>();
    let projectors = surface
        .projections
        .iter()
        .map(|projection| {
            serde_json::json!({
                "name": projection.id,
                "facts": projection.facts,
                "models": projection.models,
                "dependencies": projection.dependencies,
                "change_epoch": projection.change_epoch,
                "partition": projection.partition,
                "kind": if projection.direct { "direct" } else { "async" },
                "modeled": projection.modeled,
            })
        })
        .collect::<Vec<_>>();
    Ok(canonical_json(&serde_json::json!({
        "version": 1,
        "selection": selection,
        "dialect": surface.dialect,
        "aggregates": surface.aggregates,
        "subscriptions": surface.subscriptions,
        "default_limit": surface.default_limit,
        "max_limit": surface.max_limit,
        "models": models,
        "roots": roots,
        "comparison_ops": surface.comparison_ops,
        "commands": commands,
        "commands_attached": surface.commands_attached,
        "projectors": projectors,
        "projectors_attached": surface.projectors_attached,
    })))
}

fn surface_command_type_value(
    definition: Option<&super::command::CommandTypeSpec>,
) -> serde_json::Value {
    let Some(definition) = definition else {
        return serde_json::Value::Null;
    };
    serde_json::json!({
        "name": definition.name,
        "fields": definition.fields.iter().map(|field| {
            serde_json::json!({
                "name": field.name,
                "type_name": field.type_name,
                "nullable": field.nullable,
                "list": field.list,
                "item_nullable": field.item_nullable,
                "nested": field.nested.as_deref().map(|nested| surface_command_type_value(Some(nested))),
            })
        }).collect::<Vec<_>>(),
    })
}

fn validate_fingerprint<T: Serialize>(
    kind: &'static str,
    identity: &str,
    fingerprint: &str,
    value: &T,
    required: bool,
) -> ApplicationResult<()> {
    if fingerprint.is_empty() {
        if required {
            return Err(ApplicationError::NonCanonical(match kind {
                "module" => "module fingerprint",
                "surface" => "surface fingerprint",
                "projection" => "projection fingerprint",
                "model" => "model fingerprint",
                "command" => "command fingerprint",
                _ => "nested fingerprint",
            }));
        }
        return Ok(());
    }
    let mut value = serde_json::to_value(value)?;
    if let serde_json::Value::Object(fields) = &mut value {
        fields.insert(
            "fingerprint".into(),
            serde_json::Value::String(String::new()),
        );
    }
    let expected = sha256_fingerprint(&serde_json::to_vec(&canonical_json(&value))?);
    if expected != fingerprint {
        return Err(ApplicationError::NonCanonical(match kind {
            "module" => "module fingerprint",
            "surface" => "surface fingerprint",
            "projection" => "projection fingerprint",
            "model" => "model fingerprint",
            "command" => "command fingerprint",
            _ => "nested fingerprint",
        }));
    }
    let _ = identity;
    Ok(())
}

fn require_reference<T: std::hash::BuildHasher>(
    kind: &'static str,
    identity: &str,
    references: &HashSet<&str, T>,
) -> ApplicationResult<()> {
    if !references.contains(identity) {
        return Err(ApplicationError::Missing {
            kind,
            identity: identity.into(),
        });
    }
    Ok(())
}

fn validate_collection_len(kind: &'static str, length: usize) -> ApplicationResult<()> {
    if length > MAX_MANIFEST_COLLECTION_ITEMS {
        return Err(ApplicationError::InvalidSpec(format!(
            "{kind} count exceeds {MAX_MANIFEST_COLLECTION_ITEMS}"
        )));
    }
    Ok(())
}

fn validate_portable_text(kind: &'static str, value: &str) -> ApplicationResult<()> {
    if value.trim().is_empty()
        || value.trim() != value
        || value.len() > MAX_MANIFEST_STRING_BYTES
        || value.contains('\0')
    {
        return Err(ApplicationError::InvalidIdentity {
            kind,
            value: value.into(),
            reason: "must be a bounded portable logical value",
        });
    }
    Ok(())
}

fn validate_artifact_text(kind: &'static str, value: &str) -> ApplicationResult<()> {
    if value.trim().is_empty()
        || value.trim() != value
        || value.len() > MAX_MANIFEST_STRING_BYTES
        || value.contains('\0')
    {
        return Err(ApplicationError::InvalidSpec(format!(
            "{kind} must be a bounded artifact provenance value"
        )));
    }
    Ok(())
}

fn validate_sha256_text(kind: &'static str, value: &str) -> ApplicationResult<()> {
    validate_portable_text(kind, value)?;
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(ApplicationError::InvalidSpec(format!(
            "{kind} must use the sha256:<64 lowercase hex> form"
        )));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ApplicationError::InvalidSpec(format!(
            "{kind} must use the sha256:<64 lowercase hex> form"
        )));
    }
    Ok(())
}

fn validate_json_contract(kind: &'static str, value: &serde_json::Value) -> ApplicationResult<()> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > MAX_MANIFEST_JSON_BYTES {
        return Err(ApplicationError::InvalidSpec(format!(
            "{kind} exceeds {MAX_MANIFEST_JSON_BYTES} JSON bytes"
        )));
    }
    fn walk(kind: &'static str, value: &serde_json::Value, depth: usize) -> ApplicationResult<()> {
        if depth > MAX_MANIFEST_JSON_DEPTH {
            return Err(ApplicationError::InvalidSpec(format!(
                "{kind} exceeds JSON depth {MAX_MANIFEST_JSON_DEPTH}"
            )));
        }
        match value {
            serde_json::Value::String(value) => {
                if value.len() > MAX_MANIFEST_STRING_BYTES || value.contains('\0') {
                    return Err(ApplicationError::InvalidSpec(format!(
                        "{kind} contains oversized or NUL string material"
                    )));
                }
            }
            serde_json::Value::Array(values) => {
                validate_collection_len(kind, values.len())?;
                for value in values {
                    walk(kind, value, depth + 1)?;
                }
            }
            serde_json::Value::Object(fields) => {
                validate_collection_len(kind, fields.len())?;
                for (key, value) in fields {
                    if key.len() > MAX_MANIFEST_STRING_BYTES || key.contains('\0') {
                        return Err(ApplicationError::InvalidSpec(format!(
                            "{kind} contains oversized or NUL object-key material"
                        )));
                    }
                    walk(kind, value, depth + 1)?;
                }
            }
            serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {
            }
        }
        Ok(())
    }
    walk(kind, value, 0)
}

fn dedup_commands(mut values: Vec<CommandSpec>) -> ApplicationResult<Vec<CommandSpec>> {
    values.sort_by(|left, right| left.id.cmp(&right.id));
    validate_unique_ids("command", values.iter().map(|value| value.id.clone()))?;
    Ok(values)
}

fn dedup_events(
    mut values: Vec<super::command::EventSpec>,
) -> ApplicationResult<Vec<super::command::EventSpec>> {
    values.sort_by(|left, right| {
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
    let mut out = Vec::new();
    for value in values {
        if let Some(existing) = out
            .iter()
            .find(|existing: &&super::command::EventSpec| existing.name == value.name)
        {
            if *existing != value {
                return Err(ApplicationError::Collision {
                    kind: "event",
                    identity: value.name,
                    reason: "same identity has incompatible event schemas".into(),
                });
            }
        } else {
            out.push(value);
        }
    }
    Ok(out)
}

fn dedup_projections(mut values: Vec<ProjectionSpec>) -> ApplicationResult<Vec<ProjectionSpec>> {
    values.sort_by(|left, right| left.id.cmp(&right.id));
    validate_unique_ids("projection", values.iter().map(|value| value.id.clone()))?;
    Ok(values)
}

fn dedup_models(mut values: Vec<ModelSpec>) -> ApplicationResult<Vec<ModelSpec>> {
    values.sort_by(|left, right| left.id.cmp(&right.id));
    let mut out = Vec::new();
    for value in values {
        if let Some(existing) = out
            .iter()
            .find(|existing: &&ModelSpec| existing.id == value.id)
        {
            if *existing != value {
                return Err(ApplicationError::Collision {
                    kind: "model",
                    identity: value.id,
                    reason: "same identity has incompatible model schemas".into(),
                });
            }
        } else {
            out.push(value);
        }
    }
    Ok(out)
}

fn dedup_surfaces(mut values: Vec<SurfaceSpec>) -> ApplicationResult<Vec<SurfaceSpec>> {
    values.sort_by(|left, right| left.id.cmp(&right.id));
    validate_unique_ids("surface", values.iter().map(|value| value.id.clone()))?;
    Ok(values)
}

fn validate_unique_ids(
    kind: &'static str,
    ids: impl IntoIterator<Item = String>,
) -> ApplicationResult<()> {
    let mut seen = BTreeSet::new();
    for identity in ids {
        if !seen.insert(identity.clone()) {
            return Err(ApplicationError::Duplicate { kind, identity });
        }
    }
    Ok(())
}

fn validate_unique(kind: &'static str, identities: &[String]) -> ApplicationResult<()> {
    let mut seen = BTreeSet::new();
    for identity in identities {
        if !seen.insert(identity) {
            return Err(ApplicationError::Duplicate {
                kind,
                identity: identity.clone(),
            });
        }
    }
    Ok(())
}

fn validate_sorted_unique(kind: &'static str, identities: &[String]) -> ApplicationResult<()> {
    validate_unique(kind, identities)?;
    if identities.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ApplicationError::NonCanonical(kind));
    }
    Ok(())
}

#[cfg(test)]
mod size_limit_tests {
    use super::*;

    #[test]
    fn json_contract_can_exceed_the_old_256_kib_limit() {
        let chunk = "x".repeat(MAX_MANIFEST_STRING_BYTES);
        let within_budget = serde_json::json!(vec![chunk.clone(); 96]);
        assert!(serde_json::to_vec(&within_budget).unwrap().len() > 256 * 1024);
        validate_json_contract("surface canonical contract", &within_budget).unwrap();

        let over_budget = serde_json::json!(vec![chunk; 257]);
        assert!(serde_json::to_vec(&over_budget).unwrap().len() > MAX_MANIFEST_JSON_BYTES);
        assert!(validate_json_contract("surface canonical contract", &over_budget).is_err());
    }

    #[test]
    fn manifest_accepts_multiple_bounded_contracts_beyond_one_mib() {
        let value = serde_json::json!(vec!["x".repeat(MAX_MANIFEST_STRING_BYTES); 200]);
        assert!(serde_json::to_vec(&value).unwrap().len() < MAX_MANIFEST_JSON_BYTES);
        let mut manifest = ApplicationManifest::new("large-app");
        for index in 0..2 {
            manifest.extensions.push(
                ApplicationExtension::try_new(format!("large.contract.{index}"), 1, value.clone())
                    .unwrap(),
            );
        }

        let bytes = manifest.canonical_bytes().unwrap();
        assert!(bytes.len() > 1024 * 1024);
        assert!(bytes.len() < MAX_APPLICATION_MANIFEST_BYTES);
        ApplicationManifest::from_canonical_bytes(&bytes).unwrap();
    }

    #[test]
    fn manifest_rejects_multiple_contracts_beyond_four_mib() {
        let value = serde_json::json!(vec!["x".repeat(MAX_MANIFEST_STRING_BYTES); 230]);
        assert!(serde_json::to_vec(&value).unwrap().len() < MAX_MANIFEST_JSON_BYTES);
        let mut manifest = ApplicationManifest::new("oversized-app");
        for index in 0..5 {
            manifest.extensions.push(
                ApplicationExtension::try_new(format!("large.contract.{index}"), 1, value.clone())
                    .unwrap(),
            );
        }

        let error = manifest.canonical_bytes().unwrap_err().to_string();
        assert!(error.contains("application manifest exceeds 4194304 bytes"));
    }
}
