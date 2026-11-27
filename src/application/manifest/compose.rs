//! Composing modules and selected surfaces into one manifest, and the
//! inventories derived from them.

use super::*;

impl ApplicationManifest {
    pub(super) fn validate_inventory_expansion(&self) -> ApplicationResult<()> {
        // Check expansion before cloning any nested declarations. Duplicate
        // declarations still consume the reconstruction budget.
        validate_collection_len("modules", self.modules.len())?;
        validate_collection_len("surfaces", self.surfaces.len())?;
        for (kind, count) in [
            (
                "derived commands",
                self.modules.iter().map(|m| m.commands.len()).sum(),
            ),
            (
                "derived events",
                self.modules.iter().map(|m| m.events.len()).sum(),
            ),
            (
                "derived projections",
                self.modules
                    .iter()
                    .map(|m| m.projections.len())
                    .chain(self.surfaces.iter().map(|s| s.contract.projections.len()))
                    .sum(),
            ),
            (
                "derived models",
                self.modules
                    .iter()
                    .map(|m| m.models.len())
                    .chain(self.surfaces.iter().map(|s| s.contract.models.len()))
                    .sum(),
            ),
        ] {
            validate_collection_len(kind, count)?;
        }
        Ok(())
    }

    pub(super) fn reconstruct_inventories(&mut self) -> ApplicationResult<()> {
        self.validate_inventory_expansion()?;
        self.commands = dedup_commands(
            self.modules
                .iter()
                .flat_map(|m| m.commands.iter().cloned())
                .collect(),
        )?;
        self.events = dedup_events(
            self.modules
                .iter()
                .flat_map(|m| m.events.iter().cloned())
                .collect(),
        )?;
        self.projections = dedup_projections(
            self.modules
                .iter()
                .flat_map(|m| m.projections.iter().cloned())
                .chain(
                    self.surfaces
                        .iter()
                        .flat_map(|s| s.contract.projections.iter().cloned()),
                )
                .collect(),
        )?;
        self.models = dedup_models(
            self.modules
                .iter()
                .flat_map(|m| m.models.iter().cloned())
                .chain(
                    self.surfaces
                        .iter()
                        .flat_map(|s| s.contract.models.iter().cloned()),
                )
                .collect(),
        )?;
        Ok(())
    }

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
        let mut module_surfaces = Vec::new();
        for module in &module_manifests {
            commands.extend(module.commands.clone());
            events.extend(module.events.clone());
            projections.extend(module.projections.clone());
            models.extend(module.models.clone());
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
                .flat_map(|surface| surface.contract.projections.iter().cloned()),
        );
        models.extend(
            surfaces
                .iter()
                .flat_map(|surface| surface.contract.models.iter().cloned()),
        );

        let mut manifest = Self::new(name);
        manifest.modules = module_manifests;
        manifest.commands = dedup_commands(commands)?;
        manifest.events = dedup_events(events)?;
        manifest.projections = dedup_projections(projections)?;
        manifest.models = dedup_models(models)?;
        manifest.surfaces = surfaces;
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

    pub(super) fn canonicalize_collections(&mut self) {
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
        self.extensions.sort_by(|left, right| {
            (left.id.as_str(), left.version).cmp(&(right.id.as_str(), right.version))
        });
        self.provenance.sources.sort();
        self.provenance.sources.dedup();
    }
}

pub(super) fn framework_compatibility_extension() -> ApplicationExtension {
    ApplicationExtension {
        id: FRAMEWORK_COMPATIBILITY_EXTENSION_ID.to_string(),
        version: FRAMEWORK_COMPATIBILITY_EXTENSION_VERSION,
        value: serde_json::json!({
            "release": env!("CARGO_PKG_VERSION"),
        }),
    }
}

pub(super) fn dedup_commands(mut values: Vec<CommandSpec>) -> ApplicationResult<Vec<CommandSpec>> {
    values.sort_by(|left, right| left.id.cmp(&right.id));
    validate_unique_ids("command", values.iter().map(|value| value.id.clone()))?;
    Ok(values)
}

pub(super) fn dedup_events(
    mut values: Vec<crate::application::command::EventSpec>,
) -> ApplicationResult<Vec<crate::application::command::EventSpec>> {
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
            .find(|existing: &&crate::application::command::EventSpec| existing.name == value.name)
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

pub(super) fn dedup_projections(
    mut values: Vec<ProjectionSpec>,
) -> ApplicationResult<Vec<ProjectionSpec>> {
    values.sort_by(|left, right| left.id.cmp(&right.id));
    validate_unique_ids("projection", values.iter().map(|value| value.id.clone()))?;
    Ok(values)
}

pub(super) fn dedup_models(mut values: Vec<ModelSpec>) -> ApplicationResult<Vec<ModelSpec>> {
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

pub(super) fn dedup_surfaces(mut values: Vec<SurfaceSpec>) -> ApplicationResult<Vec<SurfaceSpec>> {
    values.sort_by(|left, right| left.id.cmp(&right.id));
    validate_unique_ids("surface", values.iter().map(|value| value.id.clone()))?;
    Ok(values)
}

pub(super) fn validate_unique_ids(
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
