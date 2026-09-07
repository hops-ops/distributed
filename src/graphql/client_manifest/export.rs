use super::*;

#[derive(Clone)]
pub struct DistributedClientSurfaceExport {
    service_id: String,
    identity: ClientSurfaceIdentity,
    surface: Arc<Surface>,
    execution: ClientExecutionLimits,
}

/// Do not transitively format the selected Surface: it retains a private full
/// catalog solely for server-side policy validation.
impl std::fmt::Debug for DistributedClientSurfaceExport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DistributedClientSurfaceExport")
            .field("service_id", &self.service_id)
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

impl DistributedClientSurfaceExport {
    fn new(
        service_id: impl Into<String>,
        identity: ClientSurfaceIdentity,
        surface: impl Into<Arc<Surface>>,
        execution: ClientExecutionLimits,
    ) -> Self {
        Self {
            service_id: service_id.into(),
            identity,
            surface: surface.into(),
            execution,
        }
    }

    /// Safe, low-boilerplate export path: authorization identity is derived
    /// from the selected Surface and cannot be caller-asserted.
    pub fn from_selected(
        service_id: impl Into<String>,
        surface: impl Into<Arc<Surface>>,
    ) -> Result<Self, ClientManifestError> {
        Self::from_selected_with_execution(service_id, surface, ClientExecutionLimits::default())
    }

    pub(crate) fn from_selected_with_execution(
        service_id: impl Into<String>,
        surface: impl Into<Arc<Surface>>,
        execution: ClientExecutionLimits,
    ) -> Result<Self, ClientManifestError> {
        let surface = surface.into();
        let service_id = service_id.into();
        let identity = match &surface.selection {
            SurfaceSelection::Catalog => {
                return Err(ClientManifestError(
                    "client exports require an explicitly role- or application-selected Surface"
                        .into(),
                ));
            }
            SurfaceSelection::Role { name } => ClientSurfaceIdentity::role(name),
            SurfaceSelection::Application {
                name,
                eligible_roles,
                schema_roles,
            } => ClientSurfaceIdentity::application_with_schema_roles(
                name,
                eligible_roles.clone(),
                schema_roles.clone(),
            ),
        };
        validate_service_provenance(&service_id, &surface)?;
        Ok(Self::new(service_id, identity, surface, execution))
    }

    /// Build a selected client contract without executable service provenance.
    ///
    /// This is the contract-only compiler boundary. It accepts the same
    /// already-selected Surface IR as the runtime export but never constructs
    /// a repository, lock manager, Service, or handler mount.
    pub fn from_contract(
        service_id: impl Into<String>,
        surface: impl Into<Arc<Surface>>,
    ) -> Result<Self, ClientManifestError> {
        let surface = surface.into();
        let service_id = service_id.into();
        let identity = match &surface.selection {
            SurfaceSelection::Catalog => {
                return Err(ClientManifestError(
                    "client exports require an explicitly role- or application-selected Surface"
                        .into(),
                ));
            }
            SurfaceSelection::Role { name } => ClientSurfaceIdentity::role(name),
            SurfaceSelection::Application {
                name,
                eligible_roles,
                schema_roles,
            } => ClientSurfaceIdentity::application_with_schema_roles(
                name,
                eligible_roles.clone(),
                schema_roles.clone(),
            ),
        };
        if surface.service_binding.is_some() {
            return Err(ClientManifestError(
                "contract-only client export cannot carry executable Service provenance".into(),
            ));
        }
        Ok(Self::new(
            service_id,
            identity,
            surface,
            ClientExecutionLimits::default(),
        ))
    }

    pub fn manifest(&self) -> Result<DistributedClientManifest, ClientManifestError> {
        client_manifest_from_surface_with_execution(
            &self.service_id,
            self.identity.clone(),
            &self.surface,
            self.execution.clone(),
        )
    }

    pub fn service_id(&self) -> &str {
        &self.service_id
    }

    pub fn identity(&self) -> &ClientSurfaceIdentity {
        &self.identity
    }

    pub fn surface(&self) -> &Arc<Surface> {
        &self.surface
    }

    pub fn manifest_json_pretty(&self) -> Result<String, ClientManifestError> {
        Ok(serde_json::to_string_pretty(&self.manifest()?)?)
    }

    /// Restrict a compiled manifest to an explicit read-model allow-list.
    pub fn manifest_for_read_models(
        &self,
        read_models: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<DistributedClientManifest, ClientManifestError> {
        prune_client_manifest(self.manifest()?, read_models)
    }
}

/// Keep only selected read models and their projection/command graph.
pub fn prune_client_manifest(
    mut manifest: DistributedClientManifest,
    read_models: impl IntoIterator<Item = impl Into<String>>,
) -> Result<DistributedClientManifest, ClientManifestError> {
    let allowed: BTreeSet<String> = read_models.into_iter().map(Into::into).collect();
    if allowed.is_empty() {
        return Err(ClientManifestError(
            "read_models allow-list must not be empty".into(),
        ));
    }
    for id in &allowed {
        if !manifest
            .models
            .iter()
            .any(|model| model.id == *id || model.typename == *id)
        {
            return Err(ClientManifestError(format!(
                "read model `{id}` is not visible on this surface"
            )));
        }
    }
    manifest
        .models
        .retain(|model| allowed.contains(&model.id) || allowed.contains(&model.typename));
    let kept: BTreeSet<String> = manifest
        .models
        .iter()
        .flat_map(|model| [model.id.clone(), model.typename.clone()])
        .collect();
    manifest.roots.retain(|root| kept.contains(&root.model));
    for projector in &mut manifest.projectors {
        projector.models.retain(|model| kept.contains(model));
    }
    manifest
        .projectors
        .retain(|projector| !projector.models.is_empty());
    for program in &mut manifest.projection_programs {
        for arm in &mut program.arms {
            arm.operations
                .retain(|operation| kept.contains(&operation.model));
            for operation in &mut arm.operations {
                operation.relationships.retain(|rel| {
                    kept.contains(&rel.source_model) && kept.contains(&rel.target_model)
                });
                operation
                    .invalidations
                    .retain(|invalidation| match invalidation {
                        ClientProjectionInvalidation::Model { model } => kept.contains(model),
                        ClientProjectionInvalidation::Relationship {
                            source_model,
                            target_model,
                            ..
                        } => kept.contains(source_model) && kept.contains(target_model),
                    });
            }
        }
        program.arms.retain(|arm| !arm.operations.is_empty());
    }
    manifest
        .projection_programs
        .retain(|program| !program.arms.is_empty());
    let kept_programs: BTreeSet<_> = manifest
        .projection_programs
        .iter()
        .map(|program| program.program_id.clone())
        .collect();
    manifest
        .projection_bindings
        .retain(|binding| kept_programs.contains(&binding.program_id));
    for command in &mut manifest.commands {
        if let Some(direct) = &command.extensions.direct_projection {
            if !kept.contains(&direct.model) {
                command.extensions.direct_projection = None;
            }
        }
        let Some(projection) = command.extensions.projection.as_mut() else {
            continue;
        };
        projection
            .pure_reduces
            .retain(|reduce| kept.contains(&reduce.model));
        projection.program_arms.retain(|arm| {
            manifest.projection_programs.iter().any(|program| {
                program.program_id == arm.program_id
                    && program.arms.iter().any(|program_arm| {
                        program_arm.event == arm.event
                            && program_arm.arm == arm.arm
                            && program_arm
                                .operations
                                .iter()
                                .any(|operation| kept.contains(&operation.model))
                    })
            })
        });
        projection.event_set = projection
            .program_arms
            .iter()
            .map(|arm| arm.event.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        projection
            .preview_occurrences
            .retain(|occurrence| projection.event_set.contains(&occurrence.event));
        for (index, occurrence) in projection.preview_occurrences.iter_mut().enumerate() {
            occurrence.ordinal = u32::try_from(index).unwrap_or(u32::MAX);
        }
        if projection.program_arms.is_empty()
            && projection.preview_occurrences.is_empty()
            && projection.pure_reduces.is_empty()
        {
            command.extensions.projection = None;
        }
    }
    Ok(manifest)
}

fn validate_service_provenance(
    service_id: &str,
    surface: &Surface,
) -> Result<(), ClientManifestError> {
    let has_typed_commands = !surface.commands.is_empty();
    match (&surface.service_binding, has_typed_commands) {
        (Some(binding), _) if binding.service_id != service_id => Err(ClientManifestError(
            format!(
                "client export service ID `{service_id}` does not match typed Surface provenance `{}`",
                binding.service_id
            ),
        )),
        (None, true) => Err(ClientManifestError(
            "typed client export requires Surface provenance from Surface::with_service"
                .into(),
        )),
        _ => Ok(()),
    }
}
