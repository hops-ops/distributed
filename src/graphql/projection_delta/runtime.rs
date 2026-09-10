use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use serde::Serialize;

use super::authorization::{
    ProjectionAuthorization, ProjectionPartitionAuthority, ProjectionPartitionScopeEncoder,
    ProjectionVisibilityEvaluator, SelectedSurfaceAuthorization, SelectedSurfacePolicyVisibility,
};
use super::lower::{
    ProjectionDeltaAuthority, ProjectionDeltaPlanOccurrence, ProjectionDeltaRequestAuthority,
};
use super::types::{
    AuthorizationTransition, ProjectionDeltaCacheScopeToken, ProjectionDeltaVisibility,
    ProjectionMutationSource, MAX_PROJECTION_DELTA_OPERATIONS,
};
use super::{
    ProjectionDelta, ProjectionDeltaError, ProjectionDeltaMutation, ProjectionDeltaPartition,
    ProjectionDeltaScope, ProjectionDeltaSurfaceIdentity,
};
use crate::command_ledger::{CausationId, PrincipalPartitionId};
use crate::graphql::client_manifest::DistributedClientSurfaceExport;
use crate::graphql::protocol::{
    CommandProjectionLifecycleProofV1, CommandProjectionMetadataV1, CommandProjectionObligationV1,
    OpaqueProtocolToken, ProtocolTokenCodec, ProtocolTokenError, ProtocolTokenPurpose,
    MAX_COMMAND_PROJECTION_OBLIGATIONS,
};
use crate::graphql::surface::{Surface, SurfaceModeledProjection};
use crate::projection::placement::{ProjectionBinding, ProjectionExecutorRoute};
use crate::projection_protocol::{
    ProjectionCausationEvidenceBatch, ProjectionObservation, ProjectionObservationKind,
    ProjectionScopeCodec, ProjectorTopologyId,
    ResolvedProjectionKey as ProtocolResolvedProjectionKey,
    ResolvedProjectionKeyField as ProtocolResolvedProjectionKeyField,
};
use crate::{
    DomainEventOccurrence, ProjectionEventSelector, ProjectionProgram,
    ProjectionRelationshipEffectKind, ResolvedProjectionKey, ResolvedProjectionMutation,
    ResolvedProjectionPartition, ResolvedProjectionPartitionRef, ResolvedProjectionPlan,
    ResolvedProjectionRelationshipEffect,
};

/// Maximum lifetime of a request-scoped modeled projection authority.
///
/// The command ledger's default replay retention is thirty days. Keeping the
/// token ceiling at that same boundary prevents a caller from minting
/// effectively permanent scope material while still allowing the complete
/// default replay window.
pub(crate) const MAX_PROJECTION_AUTHORITY_LIFETIME_MS: u64 = 30 * 24 * 60 * 60 * 1_000;

#[derive(Serialize)]
struct ProjectionLifecycleTokenMaterial<'a> {
    domain: &'static str,
    version: u32,
    issued_at_unix_ms: u64,
    expires_at_unix_ms: u64,
    identity: &'a super::ProjectionDeltaIdentity,
    projection_ref: u32,
    projection: &'a super::ProjectionDeltaProjectionIdentity,
    route: &'a ProjectionExecutorRoute,
    topology: &'a ProjectorTopologyId,
}

/// Immutable request seed captured after GraphQL surface and bearer
/// authentication have both been verified.
///
/// The command attempt supplies only its framework-minted causation ID and
/// configured replay retention later. Application handlers never receive this
/// authority or any token-minting primitive.
#[derive(Clone)]
pub(crate) struct ProtocolProjectionRequestSeed {
    export: DistributedClientSurfaceExport,
    visibility_surface: Arc<Surface>,
    registry: Arc<ProtocolProjectionProgramRegistry>,
    principal_scope: PrincipalPartitionId,
    authorization_generation: String,
    trusted_presets: Vec<crate::graphql::protocol::DistributedTrustedPreset>,
    issued_at_unix_ms: u64,
}

impl std::fmt::Debug for ProtocolProjectionRequestSeed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProtocolProjectionRequestSeed")
            .field("principal_scope", &"<redacted>")
            .field("authorization_generation", &"<redacted>")
            .field("issued_at_unix_ms", &self.issued_at_unix_ms)
            .finish_non_exhaustive()
    }
}

impl ProtocolProjectionRequestSeed {
    pub(crate) fn new(
        export: DistributedClientSurfaceExport,
        registry: Arc<ProtocolProjectionProgramRegistry>,
        principal_scope: PrincipalPartitionId,
        authorization_generation: impl Into<String>,
        trusted_presets: Vec<crate::graphql::protocol::DistributedTrustedPreset>,
        issued_at_unix_ms: u64,
    ) -> Result<Self, ProjectionRuntimeAuthorityError> {
        let authorization_generation = authorization_generation.into();
        if authorization_generation.trim().is_empty() {
            return Err(ProjectionRuntimeAuthorityError::InvalidAuthority);
        }
        let manifest = export
            .manifest()
            .map_err(|_| ProjectionRuntimeAuthorityError::InvalidAuthority)?;
        let expected = crate::graphql::client_manifest::trusted_preset_descriptors(&manifest)
            .map_err(|_| ProjectionRuntimeAuthorityError::InvalidAuthority)?;
        let mut preset_names = BTreeSet::new();
        if trusted_presets.len() != expected.len()
            || trusted_presets.iter().any(|preset| {
                !preset_names.insert(preset.name.as_str())
                    || !expected.iter().any(|descriptor| {
                        descriptor.name == preset.name
                            && descriptor.codec == preset.codec
                            && trusted_preset_value_matches(&preset.codec, &preset.value)
                    })
            })
        {
            return Err(ProjectionRuntimeAuthorityError::InvalidAuthority);
        }
        Ok(Self {
            visibility_surface: Arc::clone(export.surface()),
            export,
            registry,
            principal_scope,
            authorization_generation,
            trusted_presets,
            issued_at_unix_ms,
        })
    }

    pub(crate) fn with_visibility_surface(
        mut self,
        visibility_surface: Arc<Surface>,
    ) -> Result<Self, ProjectionRuntimeAuthorityError> {
        validate_visibility_surface(self.export.surface(), &visibility_surface)?;
        self.visibility_surface = visibility_surface;
        Ok(self)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn metadata_for_actual(
        &self,
        codec: ProtocolTokenCodec,
        cache_scope: &OpaqueProtocolToken,
        causation_id: CausationId,
        replay_retention: Duration,
        occurrences: &[DomainEventOccurrence],
        sealed_events: &[ProjectionEventSelector],
    ) -> Result<CommandProjectionMetadataV1, ProjectionRuntimeAuthorityError> {
        self.metadata_for_actual_at(
            codec,
            cache_scope,
            causation_id,
            replay_retention,
            occurrences,
            sealed_events,
            crate::time::now(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn metadata_for_actual_at(
        &self,
        codec: ProtocolTokenCodec,
        cache_scope: &OpaqueProtocolToken,
        causation_id: CausationId,
        replay_retention: Duration,
        occurrences: &[DomainEventOccurrence],
        sealed_events: &[ProjectionEventSelector],
        completion_time: SystemTime,
    ) -> Result<CommandProjectionMetadataV1, ProjectionRuntimeAuthorityError> {
        reject_oversized_occurrence_batch(occurrences)?;
        let completion_unix_ms = unix_time_ms(completion_time)?;
        if completion_unix_ms < self.issued_at_unix_ms {
            return Err(ProjectionRuntimeAuthorityError::InvalidAuthority);
        }
        let retention_ms = replay_retention
            .as_millis()
            .try_into()
            .map_err(|_| ProjectionRuntimeAuthorityError::InvalidAuthority)?;
        let expires_at_unix_ms = completion_unix_ms
            .checked_add(retention_ms)
            .ok_or(ProjectionRuntimeAuthorityError::InvalidAuthority)?;
        let request = ProtocolProjectionDeltaRequestAuthority::try_new(
            self.export.clone(),
            codec,
            self.principal_scope.clone(),
            self.authorization_generation.clone(),
            cache_scope,
            causation_id,
            completion_unix_ms,
            completion_unix_ms,
            expires_at_unix_ms,
        )?
        .with_visibility_surface(Arc::clone(&self.visibility_surface))?
        .with_trusted_presets(self.trusted_presets.clone());
        self.registry
            .metadata_for_actual(&request, occurrences, sealed_events)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "evidence validation binds independent request, command, causation, metadata, candidate, and lifecycle authorities"
    )]
    pub(crate) fn modeled_evidence(
        &self,
        codec: &ProtocolTokenCodec,
        cache_scope: &OpaqueProtocolToken,
        command_name: &str,
        causation_id: &str,
        metadata: &CommandProjectionMetadataV1,
        batch: &ProjectionCausationEvidenceBatch,
        disposition: ModeledProjectionStatusDisposition,
    ) -> Result<Vec<ModeledProjectionEvidence>, ProjectionRuntimeAuthorityError> {
        match disposition {
            ModeledProjectionStatusDisposition::Applicable => {
                self.validate_modeled_metadata(
                    codec,
                    cache_scope,
                    command_name,
                    causation_id,
                    metadata,
                )?;
            }
            ModeledProjectionStatusDisposition::Revalidate => {
                self.validate_modeled_lifecycle_metadata(
                    codec,
                    cache_scope,
                    command_name,
                    causation_id,
                    metadata,
                )?;
            }
        }
        let mut evidence = Vec::with_capacity(metadata.obligations.len());
        for obligation in &metadata.obligations {
            let identity = metadata
                .delta
                .projections
                .get(obligation.projection_ref as usize)
                .ok_or(ProjectionRuntimeAuthorityError::InvalidMetadata)?;
            let entry = self.projection_entry(identity, disposition)?;
            let terminal_failure = batch
                .terminal_failure_topologies
                .iter()
                .any(|topology| topology == entry.codec.topology());
            if terminal_failure {
                evidence.push(ModeledProjectionEvidence::TerminalFailure);
                continue;
            }

            let mut observed = None;
            for candidate in &batch.observations {
                if candidate.causation_id != causation_id
                    || candidate.scope.topology() != entry.codec.topology()
                    || candidate.scope.model() != obligation.model
                    // A modeled observation is proof for the exact semantic
                    // program that authored it. Physical projector names are
                    // shared across deployments and cannot establish this
                    // identity after an upgrade or cold restart. Legacy
                    // observations have no program ID and remain readable,
                    // but cannot satisfy a modeled obligation.
                    || candidate.program_id.map(|id| id.to_string())
                        != Some(identity.program_id.clone())
                    || !entry
                        .binding
                        .outputs()
                        .iter()
                        .any(|output| output.model() == candidate.scope.model())
                {
                    continue;
                }
                let token = crate::graphql::protocol::issue_projection_obligation_token(
                    codec,
                    &metadata.delta.identity.cache_scope_token,
                    &metadata.delta.identity.schema_fingerprint,
                    causation_id,
                    candidate.scope.topology().name(),
                    candidate.scope.model(),
                    candidate.kind,
                    &candidate.scope,
                )
                .map_err(ProjectionRuntimeAuthorityError::Token)?;
                if token != obligation.scope_token {
                    continue;
                }
                if observed.replace(candidate.clone()).is_some() {
                    return Err(ProjectionRuntimeAuthorityError::InvalidObservation);
                }
            }
            evidence.push(match observed {
                Some(observation) => ModeledProjectionEvidence::Observed(observation),
                None => ModeledProjectionEvidence::Pending,
            });
        }
        Ok(evidence)
    }

    pub(crate) fn modeled_evidence_topologies(
        &self,
        codec: &ProtocolTokenCodec,
        cache_scope: &OpaqueProtocolToken,
        command_name: &str,
        causation_id: &str,
        metadata: &CommandProjectionMetadataV1,
    ) -> Result<ModeledProjectionStatusPlan, ProjectionRuntimeAuthorityError> {
        let disposition = if structurally_empty_modeled_metadata(metadata) {
            match self.validate_modeled_status_authority(codec, cache_scope, causation_id, metadata)
            {
                Ok(()) => self.empty_command_disposition(command_name)?,
                Err(_) => {
                    self.validate_empty_modeled_revalidation_metadata(
                        codec,
                        cache_scope,
                        command_name,
                        causation_id,
                        metadata,
                    )?;
                    ModeledProjectionStatusDisposition::Revalidate
                }
            }
        } else {
            match self.validate_modeled_metadata(
                codec,
                cache_scope,
                command_name,
                causation_id,
                metadata,
            ) {
                Ok(()) => ModeledProjectionStatusDisposition::Applicable,
                Err(_) => {
                    self.validate_modeled_lifecycle_metadata(
                        codec,
                        cache_scope,
                        command_name,
                        causation_id,
                        metadata,
                    )?;
                    ModeledProjectionStatusDisposition::Revalidate
                }
            }
        };
        let mut topologies = Vec::new();
        for identity in &metadata.delta.projections {
            let entry = self.projection_entry(identity, disposition)?;
            let topology = entry.codec.topology();
            if !topologies
                .iter()
                .any(|existing: &ProjectorTopologyId| existing == topology)
            {
                topologies.push(topology.clone());
            }
        }
        for obligation in &metadata.obligations {
            let identity = metadata
                .delta
                .projections
                .get(obligation.projection_ref as usize)
                .ok_or(ProjectionRuntimeAuthorityError::InvalidMetadata)?;
            let entry = self.projection_entry(identity, disposition)?;
            if !entry
                .binding
                .outputs()
                .iter()
                .any(|output| output.model() == obligation.model)
            {
                return Err(ProjectionRuntimeAuthorityError::InvalidObservation);
            }
        }
        topologies.sort_by_key(ProjectorTopologyId::canonical_bytes);
        Ok(ModeledProjectionStatusPlan {
            topologies,
            disposition,
        })
    }

    pub(crate) fn validate_modeled_metadata(
        &self,
        codec: &ProtocolTokenCodec,
        cache_scope: &OpaqueProtocolToken,
        command_name: &str,
        causation_id: &str,
        metadata: &CommandProjectionMetadataV1,
    ) -> Result<(), ProjectionRuntimeAuthorityError> {
        self.validate_modeled_status_authority(codec, cache_scope, causation_id, metadata)?;
        self.validate_command_projection_inventory(command_name, metadata)?;
        for identity in &metadata.delta.projections {
            self.current_projection_entry(identity)?;
        }
        for obligation in &metadata.obligations {
            let identity = metadata
                .delta
                .projections
                .get(obligation.projection_ref as usize)
                .ok_or(ProjectionRuntimeAuthorityError::InvalidMetadata)?;
            let entry = self.current_projection_entry(identity)?;
            if !entry
                .binding
                .outputs()
                .iter()
                .any(|output| output.model() == obligation.model)
            {
                return Err(ProjectionRuntimeAuthorityError::InvalidObservation);
            }
        }
        Ok(())
    }

    fn projection_entry(
        &self,
        identity: &super::ProjectionDeltaProjectionIdentity,
        disposition: ModeledProjectionStatusDisposition,
    ) -> Result<&ProtocolProjectionProgram, ProjectionRuntimeAuthorityError> {
        match disposition {
            ModeledProjectionStatusDisposition::Applicable => {
                self.current_projection_entry(identity)
            }
            ModeledProjectionStatusDisposition::Revalidate => {
                self.lifecycle_projection_entry(identity)
            }
        }
    }

    fn current_projection_entry(
        &self,
        identity: &super::ProjectionDeltaProjectionIdentity,
    ) -> Result<&ProtocolProjectionProgram, ProjectionRuntimeAuthorityError> {
        let projection = self.current_projection(identity)?;
        self.registry.exact(projection)
    }

    fn current_projection(
        &self,
        identity: &super::ProjectionDeltaProjectionIdentity,
    ) -> Result<&SurfaceModeledProjection, ProjectionRuntimeAuthorityError> {
        let projection = self
            .export
            .surface()
            .projection_owners()
            .iter()
            .flat_map(|owner| &owner.modeled)
            .find(|projection| {
                // Draining bindings remain lifecycle/evidence authorities, but
                // they are never current delta-application authorities. This
                // stays true even when the durable receipt's outer scope still
                // exactly matches the selected client surface.
                projection.is_causally_eligible()
                    && projection.program_id().to_string() == identity.program_id
                    && projection.binding_id().to_string() == identity.binding_id
                    && projection.epoch().as_str() == identity.epoch
                    && projection.selected_program().is_some_and(|selected| {
                        selected.ir_version == identity.program_ir_version
                            && selected.operation_semantics_version
                                == identity.operation_semantics_version
                    })
            })
            .ok_or(ProjectionRuntimeAuthorityError::IneligibleProjection)?;
        self.registry.exact(projection)?;
        Ok(projection)
    }

    fn lifecycle_projection_entry(
        &self,
        identity: &super::ProjectionDeltaProjectionIdentity,
    ) -> Result<&ProtocolProjectionProgram, ProjectionRuntimeAuthorityError> {
        let projection = self.lifecycle_projection(identity)?;
        self.registry.exact(projection)
    }

    fn lifecycle_projection(
        &self,
        identity: &super::ProjectionDeltaProjectionIdentity,
    ) -> Result<&SurfaceModeledProjection, ProjectionRuntimeAuthorityError> {
        let projection = self
            .export
            .surface()
            .projection_owners()
            .iter()
            .flat_map(|owner| &owner.modeled)
            .find(|projection| {
                projection.is_causal_evidence_eligible()
                    && projection.program_id().to_string() == identity.program_id
                    && projection.binding_id().to_string() == identity.binding_id
                    && projection.epoch().as_str() == identity.epoch
                    && projection.selected_program().is_some_and(|selected| {
                        selected.ir_version == identity.program_ir_version
                            && selected.operation_semantics_version
                                == identity.operation_semantics_version
                    })
            })
            .ok_or(ProjectionRuntimeAuthorityError::IneligibleProjection)?;
        self.registry.exact(projection)?;
        Ok(projection)
    }

    fn validate_modeled_status_authority(
        &self,
        codec: &ProtocolTokenCodec,
        cache_scope: &OpaqueProtocolToken,
        causation_id: &str,
        metadata: &CommandProjectionMetadataV1,
    ) -> Result<(), ProjectionRuntimeAuthorityError> {
        let now_unix_ms = unix_time_ms(crate::time::now())?;
        metadata
            .validate_not_expired(now_unix_ms)
            .map_err(|error| match error {
                crate::graphql::protocol::CommandProjectionMetadataError::Expired => {
                    ProjectionRuntimeAuthorityError::Expired
                }
                _ => ProjectionRuntimeAuthorityError::InvalidMetadata,
            })?;
        let causation_id = CausationId::parse_stored(causation_id.to_owned())
            .map_err(|_| ProjectionRuntimeAuthorityError::InvalidAuthority)?;
        let authority = ProtocolProjectionDeltaRequestAuthority::try_new(
            self.export.clone(),
            codec.clone(),
            self.principal_scope.clone(),
            self.authorization_generation.clone(),
            cache_scope,
            causation_id,
            now_unix_ms,
            metadata.issued_at_unix_ms,
            metadata.expires_at_unix_ms,
        )?
        .with_visibility_surface(Arc::clone(&self.visibility_surface))?
        .with_trusted_presets(self.trusted_presets.clone());
        let manifest = self
            .export
            .manifest()
            .map_err(|_| ProjectionRuntimeAuthorityError::InvalidAuthority)?;
        metadata
            .delta
            .validate_replay_scope(&manifest, &authority)
            .map_err(ProjectionRuntimeAuthorityError::Delta)
    }

    pub(crate) fn validate_modeled_lifecycle_metadata(
        &self,
        codec: &ProtocolTokenCodec,
        cache_scope: &OpaqueProtocolToken,
        command_name: &str,
        causation_id: &str,
        metadata: &CommandProjectionMetadataV1,
    ) -> Result<(), ProjectionRuntimeAuthorityError> {
        if structurally_empty_modeled_metadata(metadata) {
            return self.validate_empty_modeled_revalidation_metadata(
                codec,
                cache_scope,
                command_name,
                causation_id,
                metadata,
            );
        }
        self.validate_command_projection_inventory(command_name, metadata)?;
        let now_unix_ms = unix_time_ms(crate::time::now())?;
        metadata
            .validate_not_expired(now_unix_ms)
            .map_err(|error| match error {
                crate::graphql::protocol::CommandProjectionMetadataError::Expired => {
                    ProjectionRuntimeAuthorityError::Expired
                }
                _ => ProjectionRuntimeAuthorityError::InvalidMetadata,
            })?;
        let causation_id = CausationId::parse_stored(causation_id.to_owned())
            .map_err(|_| ProjectionRuntimeAuthorityError::InvalidAuthority)?;
        let authority = ProtocolProjectionDeltaRequestAuthority::try_new(
            self.export.clone(),
            codec.clone(),
            self.principal_scope.clone(),
            self.authorization_generation.clone(),
            cache_scope,
            causation_id,
            now_unix_ms,
            metadata.issued_at_unix_ms,
            metadata.expires_at_unix_ms,
        )?
        .with_visibility_surface(Arc::clone(&self.visibility_surface))?
        .with_trusted_presets(self.trusted_presets.clone());
        let manifest = self
            .export
            .manifest()
            .map_err(|_| ProjectionRuntimeAuthorityError::InvalidAuthority)?;
        let identity = &metadata.delta.identity;
        // Lifecycle authority may be exercised against an unchanged outer
        // schema/cache scope when applicability failed because an exact
        // binding is Draining. The proof loop below still requires every
        // route/topology/program identity to match and at least one Draining
        // binding, so an Active-only same-scope receipt cannot enter this path.
        if identity.manifest_version != manifest.manifest_version
            || identity.client_protocol_version != manifest.protocol_version
            || identity.surface != ProjectionDeltaSurfaceIdentity::from(&manifest.surface)
            || identity.protocol_fingerprint != manifest.protocol_fingerprint
            || identity.authorization_generation != authority.authorization_generation()
            || identity.command_causation_id != authority.command_causation_id().as_str()
        {
            return Err(ProjectionRuntimeAuthorityError::Delta(
                ProjectionDeltaError::ReplayScopeMismatch,
            ));
        }
        let mut saw_draining = false;
        for (projection_ref, (projection_identity, proof)) in metadata
            .delta
            .projections
            .iter()
            .zip(&metadata.lifecycle_proofs)
            .enumerate()
        {
            if proof.projection_ref as usize != projection_ref {
                return Err(ProjectionRuntimeAuthorityError::InvalidMetadata);
            }
            let projection = self.lifecycle_projection(projection_identity)?;
            let entry = self.registry.exact(projection)?;
            saw_draining |= projection.state()
                == crate::projection::placement::ProjectionBindingState::Draining;
            codec
                .verify(
                    &proof.token,
                    ProtocolTokenPurpose::ProjectionLifecycle,
                    &ProjectionLifecycleTokenMaterial {
                        domain: "distributed.graphql.projection-lifecycle",
                        version: 1,
                        issued_at_unix_ms: metadata.issued_at_unix_ms,
                        expires_at_unix_ms: metadata.expires_at_unix_ms,
                        identity,
                        projection_ref: proof.projection_ref,
                        projection: projection_identity,
                        route: projection.route(),
                        topology: entry.codec.topology(),
                    },
                )
                .map_err(ProjectionRuntimeAuthorityError::Token)?;
        }
        if !saw_draining {
            return Err(ProjectionRuntimeAuthorityError::IneligibleProjection);
        }
        Ok(())
    }

    fn command(
        &self,
        command_name: &str,
    ) -> Result<&crate::graphql::surface::SurfaceCommand, ProjectionRuntimeAuthorityError> {
        self.export
            .surface()
            .commands()
            .iter()
            .find(|command| command.command_name == command_name)
            .ok_or(ProjectionRuntimeAuthorityError::InvalidAuthority)
    }

    fn empty_command_disposition(
        &self,
        command_name: &str,
    ) -> Result<ModeledProjectionStatusDisposition, ProjectionRuntimeAuthorityError> {
        let command = self.command(command_name)?;
        if command.projections.selectors.is_empty() {
            return Err(ProjectionRuntimeAuthorityError::InvalidAuthority);
        }
        for selector in &command.projections.selectors {
            let matching = self
                .export
                .surface()
                .projection_owners()
                .iter()
                .flat_map(|owner| &owner.modeled)
                .filter(|projection| {
                    projection.selected_program().is_some_and(|program| {
                        program.arms.iter().any(|arm| &arm.selector == selector)
                    })
                })
                .collect::<Vec<_>>();
            if matching.is_empty()
                || matching
                    .iter()
                    .any(|projection| !projection.is_causally_eligible())
            {
                return Ok(ModeledProjectionStatusDisposition::Revalidate);
            }
        }
        Ok(ModeledProjectionStatusDisposition::Applicable)
    }

    fn validate_command_projection_inventory(
        &self,
        command_name: &str,
        metadata: &CommandProjectionMetadataV1,
    ) -> Result<(), ProjectionRuntimeAuthorityError> {
        let command = self.command(command_name)?;
        if command.projections.selectors.is_empty() {
            return Err(ProjectionRuntimeAuthorityError::InvalidAuthority);
        }
        for identity in &metadata.delta.projections {
            let authorized = self
                .export
                .surface()
                .projection_owners()
                .iter()
                .flat_map(|owner| &owner.modeled)
                .any(|projection| {
                    projection.program_id().to_string() == identity.program_id
                        && projection.binding_id().to_string() == identity.binding_id
                        && projection.epoch().as_str() == identity.epoch
                        && projection.selected_program().is_some_and(|program| {
                            program.ir_version == identity.program_ir_version
                                && program.operation_semantics_version
                                    == identity.operation_semantics_version
                                && program.arms.iter().any(|arm| {
                                    command
                                        .projections
                                        .selectors
                                        .iter()
                                        .any(|selector| selector == &arm.selector)
                                })
                        })
                });
            if !authorized {
                return Err(ProjectionRuntimeAuthorityError::InvalidAuthority);
            }
        }
        Ok(())
    }

    fn validate_empty_modeled_revalidation_metadata(
        &self,
        codec: &ProtocolTokenCodec,
        cache_scope: &OpaqueProtocolToken,
        command_name: &str,
        causation_id: &str,
        metadata: &CommandProjectionMetadataV1,
    ) -> Result<(), ProjectionRuntimeAuthorityError> {
        if !structurally_empty_modeled_metadata(metadata) {
            return Err(ProjectionRuntimeAuthorityError::InvalidMetadata);
        }
        // The current selected command contract is the authority for a
        // zero-occurrence receipt. It must still declare modeled selectors,
        // even though lifecycle-only or scope-drift work is revalidation-only.
        self.empty_command_disposition(command_name)?;
        let now_unix_ms = unix_time_ms(crate::time::now())?;
        metadata
            .validate_not_expired(now_unix_ms)
            .map_err(|error| match error {
                crate::graphql::protocol::CommandProjectionMetadataError::Expired => {
                    ProjectionRuntimeAuthorityError::Expired
                }
                _ => ProjectionRuntimeAuthorityError::InvalidMetadata,
            })?;
        let causation_id = CausationId::parse_stored(causation_id.to_owned())
            .map_err(|_| ProjectionRuntimeAuthorityError::InvalidAuthority)?;
        let authority = ProtocolProjectionDeltaRequestAuthority::try_new(
            self.export.clone(),
            codec.clone(),
            self.principal_scope.clone(),
            self.authorization_generation.clone(),
            cache_scope,
            causation_id,
            now_unix_ms,
            metadata.issued_at_unix_ms,
            metadata.expires_at_unix_ms,
        )?
        .with_visibility_surface(Arc::clone(&self.visibility_surface))?
        .with_trusted_presets(self.trusted_presets.clone());
        let manifest = self
            .export
            .manifest()
            .map_err(|_| ProjectionRuntimeAuthorityError::InvalidAuthority)?;
        let identity = &metadata.delta.identity;
        if identity.manifest_version != manifest.manifest_version
            || identity.client_protocol_version != manifest.protocol_version
            || identity.surface != ProjectionDeltaSurfaceIdentity::from(&manifest.surface)
            || identity.protocol_fingerprint != manifest.protocol_fingerprint
            || identity.authorization_generation != authority.authorization_generation()
            || identity.command_causation_id != authority.command_causation_id().as_str()
        {
            return Err(ProjectionRuntimeAuthorityError::Delta(
                ProjectionDeltaError::ReplayScopeMismatch,
            ));
        }
        Ok(())
    }
}

fn structurally_empty_modeled_metadata(metadata: &CommandProjectionMetadataV1) -> bool {
    metadata.delta.projections.is_empty()
        && metadata.delta.occurrences.is_empty()
        && metadata.delta.operations.is_empty()
        && metadata.delta.recoveries.is_empty()
        && metadata.lifecycle_proofs.is_empty()
        && metadata.obligations.is_empty()
}

fn trusted_preset_value_matches(codec: &str, value: &serde_json::Value) -> bool {
    match codec {
        "string" | "string_unvalidated_timestamp" => value.is_string(),
        "base64" => value.as_str().is_some_and(|raw| {
            base64::engine::general_purpose::STANDARD
                .decode(raw)
                .is_ok_and(|decoded| {
                    base64::engine::general_purpose::STANDARD.encode(decoded) == raw
                })
        }),
        "boolean" => value.is_boolean(),
        "int32" => value
            .as_i64()
            .is_some_and(|value| i32::try_from(value).is_ok()),
        "json_number_precision_limited" => value
            .as_i64()
            .is_some_and(|value| (-9_007_199_254_740_991..=9_007_199_254_740_991).contains(&value)),
        "float64" => value.as_f64().is_some_and(f64::is_finite),
        "json" => true,
        _ => false,
    }
}

#[cfg(test)]
mod trusted_preset_tests {
    use super::trusted_preset_value_matches;

    #[test]
    fn base64_preset_inventory_requires_canonical_standard_encoding() {
        assert!(trusted_preset_value_matches(
            "base64",
            &serde_json::json!("cHJvamVjdGlvbg==")
        ));
        for invalid in ["cHJvamVjdGlvbg", "cHJvamVjdGlvbg===", "%%%%"] {
            assert!(!trusted_preset_value_matches(
                "base64",
                &serde_json::Value::String(invalid.into())
            ));
        }
    }
}

fn unix_time_ms(now: SystemTime) -> Result<u64, ProjectionRuntimeAuthorityError> {
    now.duration_since(UNIX_EPOCH)
        .map_err(|_| ProjectionRuntimeAuthorityError::InvalidAuthority)?
        .as_millis()
        .try_into()
        .map_err(|_| ProjectionRuntimeAuthorityError::InvalidAuthority)
}

fn reject_oversized_occurrence_batch(
    occurrences: &[DomainEventOccurrence],
) -> Result<(), ProjectionRuntimeAuthorityError> {
    if occurrences.len() > MAX_PROJECTION_DELTA_OPERATIONS {
        return Err(ProjectionRuntimeAuthorityError::Delta(
            ProjectionDeltaError::TooManyOccurrences {
                len: occurrences.len(),
                max: MAX_PROJECTION_DELTA_OPERATIONS,
            },
        ));
    }
    Ok(())
}

/// One finite physical observation proven to correspond to a role-safe delta
/// operation by the mounted projection registry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ModeledProjectionObservationScope {
    pub(crate) operation_index: usize,
    pub(crate) projection_ref: u32,
    pub(crate) projector: String,
    pub(crate) model: String,
    pub(crate) kind: crate::projection_protocol::ProjectionObservationKind,
    pub(crate) scope: crate::projection_protocol::ProjectionRecordScope,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[expect(
    clippy::large_enum_variant,
    reason = "evidence entries are tightly bounded and retain an owned canonical observation without per-entry allocation"
)]
pub(crate) enum ModeledProjectionEvidence {
    Pending,
    Observed(ProjectionObservation),
    TerminalFailure,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ModeledProjectionStatusDisposition {
    Applicable,
    Revalidate,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ModeledProjectionStatusPlan {
    pub(crate) topologies: Vec<ProjectorTopologyId>,
    pub(crate) disposition: ModeledProjectionStatusDisposition,
}

/// Exact raw catalog programs retained by the GraphQL engine solely for
/// authoritative post-handler lowering.
///
/// Role/application surfaces keep only their filtered selected programs. This
/// registry supplies the matching raw resolver and physical scope codec after
/// the command has produced real domain-event occurrences.
#[derive(Clone, Default)]
pub(crate) struct ProtocolProjectionProgramRegistry {
    entries: BTreeMap<ProjectionRegistryKey, ProtocolProjectionProgram>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ProjectionRegistryKey {
    program_id: String,
    binding_id: String,
    epoch: String,
}

#[derive(Clone)]
struct ProtocolProjectionProgram {
    program: ProjectionProgram,
    binding: ProjectionBinding,
    route: ProjectionExecutorRoute,
    codec: ProjectionScopeCodec,
}

impl ProtocolProjectionProgramRegistry {
    pub(crate) fn try_from_surface(
        surface: &Surface,
    ) -> Result<Self, ProjectionRuntimeAuthorityError> {
        let mut entries = BTreeMap::new();
        for modeled in surface
            .projection_owners()
            .iter()
            .flat_map(|owner| &owner.modeled)
        {
            let Some((program, binding)) = modeled.raw() else {
                return Err(ProjectionRuntimeAuthorityError::Registry(
                    "projection registry requires the unselected catalog Surface".into(),
                ));
            };
            if program
                .id()
                .map_err(|error| ProjectionRuntimeAuthorityError::Registry(error.to_string()))?
                != modeled.program_id()
                || binding.id() != modeled.binding_id()
                || binding.program_id() != modeled.program_id()
            {
                return Err(ProjectionRuntimeAuthorityError::Registry(
                    "modeled projection digest differs from its catalog program or binding".into(),
                ));
            }
            let topology = binding.physical_topology().ok_or_else(|| {
                ProjectionRuntimeAuthorityError::Registry(format!(
                    "modeled projection binding `{}` has no physical observation topology",
                    binding.id()
                ))
            })?;
            let topology =
                ProjectorTopologyId::new(topology.version(), topology.name(), topology.digest())
                    .map_err(|error| {
                        ProjectionRuntimeAuthorityError::Registry(error.to_string())
                    })?;
            let codec = ProjectionScopeCodec::with_models(
                topology,
                binding
                    .outputs()
                    .iter()
                    .map(|output| (output.model(), output.schema())),
            )
            .map_err(|error| ProjectionRuntimeAuthorityError::Registry(error.to_string()))?;
            let key = registry_key(modeled);
            let entry = ProtocolProjectionProgram {
                program: program.clone(),
                binding: binding.clone(),
                route: modeled.route().clone(),
                codec,
            };
            if entries.insert(key, entry).is_some() {
                return Err(ProjectionRuntimeAuthorityError::Registry(
                    "modeled projection registry repeats one exact program/binding/epoch".into(),
                ));
            }
        }
        Ok(Self { entries })
    }

    pub(crate) fn metadata_for_actual(
        &self,
        request: &ProtocolProjectionDeltaRequestAuthority,
        occurrences: &[DomainEventOccurrence],
        sealed_events: &[ProjectionEventSelector],
    ) -> Result<CommandProjectionMetadataV1, ProjectionRuntimeAuthorityError> {
        reject_oversized_occurrence_batch(occurrences)?;
        for occurrence in occurrences {
            if !sealed_events
                .iter()
                .any(|selector| selector.matches(occurrence))
            {
                return Err(ProjectionRuntimeAuthorityError::EventOutsideContract);
            }
        }

        let authority = ProjectionDeltaAuthority::try_new(request.export(), request)
            .map_err(ProjectionRuntimeAuthorityError::Delta)?;
        let mut modeled = Vec::with_capacity(MAX_PROJECTION_DELTA_OPERATIONS);
        for projection in request
            .export()
            .surface()
            .projection_owners()
            .iter()
            .flat_map(|owner| &owner.modeled)
            .filter(|projection| projection.is_causally_eligible())
        {
            if modeled.len() == MAX_PROJECTION_DELTA_OPERATIONS {
                return Err(ProjectionRuntimeAuthorityError::Delta(
                    ProjectionDeltaError::TooManyProjections {
                        len: MAX_PROJECTION_DELTA_OPERATIONS + 1,
                        max: MAX_PROJECTION_DELTA_OPERATIONS,
                    },
                ));
            }
            modeled.push(projection);
        }
        let sources = modeled
            .iter()
            .map(|projection| authority.source(projection))
            .collect::<Result<Vec<_>, _>>()
            .map_err(ProjectionRuntimeAuthorityError::Delta)?;

        let mut resolved_occurrences =
            Vec::<Vec<(usize, ResolvedProjectionPlan)>>::with_capacity(occurrences.len());
        for occurrence in occurrences {
            let mut resolved =
                Vec::with_capacity(modeled.len().min(MAX_PROJECTION_DELTA_OPERATIONS));
            for (source_index, projection) in modeled.iter().enumerate() {
                let selected = projection
                    .selected_program()
                    .ok_or(ProjectionRuntimeAuthorityError::IneligibleProjection)?;
                if !selected
                    .arms
                    .iter()
                    .any(|arm| arm.selector.matches(occurrence))
                {
                    continue;
                }
                let entry = self.exact(projection)?;
                let plan = ResolvedProjectionPlan::resolve(&entry.program, occurrence)
                    .map_err(|error| ProjectionRuntimeAuthorityError::Plan(error.to_string()))?;
                if resolved.len() == MAX_PROJECTION_DELTA_OPERATIONS {
                    return Err(ProjectionRuntimeAuthorityError::Delta(
                        ProjectionDeltaError::TooManyProjections {
                            len: MAX_PROJECTION_DELTA_OPERATIONS + 1,
                            max: MAX_PROJECTION_DELTA_OPERATIONS,
                        },
                    ));
                }
                resolved.push((source_index, plan));
            }
            if !resolved.is_empty() {
                resolved_occurrences.push(resolved);
            }
        }

        let batch = resolved_occurrences
            .iter()
            .map(|resolved| {
                ProjectionDeltaPlanOccurrence::actual(
                    resolved
                        .iter()
                        .map(|(source_index, plan)| (&sources[*source_index], plan))
                        .collect(),
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(ProjectionRuntimeAuthorityError::Delta)?;
        let delta = authority
            .lower(&batch)
            .map_err(ProjectionRuntimeAuthorityError::Delta)?;

        let mut used = BTreeSet::new();
        for projection in &delta.projections {
            let index = modeled
                .iter()
                .position(|modeled| {
                    modeled.program_id().to_string() == projection.program_id
                        && modeled.binding_id().to_string() == projection.binding_id
                        && modeled.epoch().as_str() == projection.epoch
                })
                .ok_or(ProjectionRuntimeAuthorityError::InvalidAuthority)?;
            used.insert(index);
        }
        let eligible = used
            .into_iter()
            .map(|index| modeled[index])
            .collect::<Vec<_>>();
        let lifecycle_proofs = self.lifecycle_proofs(request, &delta, &eligible)?;
        let observations =
            self.observation_scopes(request, &delta, &modeled, &resolved_occurrences)?;
        let opaque_revalidation = request
            .export()
            .surface()
            .projection_owners()
            .iter()
            .filter(|owner| owner.modeled.is_empty() && !owner.is_direct())
            .any(|owner| {
                occurrences.iter().any(|occurrence| {
                    owner
                        .facts
                        .iter()
                        .any(|fact| fact == &occurrence.descriptor().name)
                })
            });
        request.metadata(
            delta,
            lifecycle_proofs,
            &eligible,
            &observations,
            opaque_revalidation,
        )
    }

    fn lifecycle_proofs(
        &self,
        request: &ProtocolProjectionDeltaRequestAuthority,
        delta: &ProjectionDelta,
        eligible: &[&SurfaceModeledProjection],
    ) -> Result<Vec<CommandProjectionLifecycleProofV1>, ProjectionRuntimeAuthorityError> {
        delta
            .projections
            .iter()
            .enumerate()
            .map(|(projection_ref, identity)| {
                let projection = eligible
                    .iter()
                    .find(|projection| {
                        projection.program_id().to_string() == identity.program_id
                            && projection.binding_id().to_string() == identity.binding_id
                            && projection.epoch().as_str() == identity.epoch
                    })
                    .ok_or(ProjectionRuntimeAuthorityError::InvalidAuthority)?;
                let entry = self.exact(projection)?;
                request.lifecycle_proof(
                    delta,
                    projection_ref
                        .try_into()
                        .map_err(|_| ProjectionRuntimeAuthorityError::InvalidMetadata)?,
                    identity,
                    projection,
                    entry,
                )
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn lifecycle_proofs_for_test(
        &self,
        request: &ProtocolProjectionDeltaRequestAuthority,
        delta: &ProjectionDelta,
        eligible: &[&SurfaceModeledProjection],
    ) -> Result<Vec<CommandProjectionLifecycleProofV1>, ProjectionRuntimeAuthorityError> {
        self.lifecycle_proofs(request, delta, eligible)
    }

    fn observation_scopes(
        &self,
        request: &ProtocolProjectionDeltaRequestAuthority,
        delta: &ProjectionDelta,
        modeled: &[&SurfaceModeledProjection],
        resolved_occurrences: &[Vec<(usize, ResolvedProjectionPlan)>],
    ) -> Result<Vec<ModeledProjectionObservationScope>, ProjectionRuntimeAuthorityError> {
        let visibility = SelectedSurfacePolicyVisibility::try_new(request.surface.as_ref())
            .map_err(ProjectionRuntimeAuthorityError::Delta)?;
        let authorization = SelectedSurfaceAuthorization::try_new(
            request.surface.as_ref(),
            &visibility,
            request,
            delta.identity.surface.clone(),
            request.authorization_generation.clone(),
            request.principal_scope.clone(),
            request.cache_scope.clone(),
        )
        .map_err(ProjectionRuntimeAuthorityError::Delta)?;
        let mut observations = Vec::with_capacity(
            delta
                .operations
                .len()
                .min(MAX_COMMAND_PROJECTION_OBLIGATIONS),
        );
        for (operation_index, operation) in delta.operations.iter().enumerate() {
            if matches!(
                operation.mutation,
                ProjectionDeltaMutation::InvalidateModel { .. }
                    | ProjectionDeltaMutation::InvalidateRelationship { .. }
            ) {
                continue;
            }
            let resolved = resolved_occurrences
                .get(operation.occurrence_ordinal as usize)
                .ok_or(ProjectionRuntimeAuthorityError::InvalidObservation)?;
            for projection_ref in &operation.projection_refs {
                let identity = delta
                    .projections
                    .get(*projection_ref as usize)
                    .ok_or(ProjectionRuntimeAuthorityError::InvalidObservation)?;
                let modeled_index = modeled
                    .iter()
                    .position(|modeled| {
                        modeled.program_id().to_string() == identity.program_id
                            && modeled.binding_id().to_string() == identity.binding_id
                            && modeled.epoch().as_str() == identity.epoch
                    })
                    .ok_or(ProjectionRuntimeAuthorityError::InvalidObservation)?;
                let plan = resolved
                    .iter()
                    .find(|(index, _)| *index == modeled_index)
                    .map(|(_, plan)| plan)
                    .ok_or(ProjectionRuntimeAuthorityError::InvalidObservation)?;
                let entry = self.exact(modeled[modeled_index])?;
                let partition = resolved_partition_json(plan.partition())?;
                let projector = entry.codec.topology().name().to_owned();
                let mut matched = false;
                for mutation in plan.mutations() {
                    if !mutation_proves_delta_operation(
                        mutation,
                        &operation.mutation,
                        &authorization,
                    )
                    .map_err(ProjectionRuntimeAuthorityError::Delta)?
                    {
                        continue;
                    }
                    matched = true;
                    let model = mutation.target().model();
                    if !entry
                        .binding
                        .outputs()
                        .iter()
                        .any(|output| output.model() == model)
                    {
                        return Err(ProjectionRuntimeAuthorityError::InvalidObservation);
                    }
                    if observations.len() == MAX_COMMAND_PROJECTION_OBLIGATIONS {
                        return Err(ProjectionRuntimeAuthorityError::InvalidMetadata);
                    }
                    let physical_scope = entry
                        .codec
                        .encode_resolved_obligation_scope(
                            &projector,
                            model,
                            &protocol_resolved_key(mutation.key())?,
                            partition.as_ref(),
                        )
                        .map_err(|error| {
                            ProjectionRuntimeAuthorityError::Registry(error.to_string())
                        })?;
                    observations.push(ModeledProjectionObservationScope {
                        operation_index,
                        projection_ref: *projection_ref,
                        projector: projector.clone(),
                        model: model.to_owned(),
                        kind: ProjectionObservationKind::Record,
                        scope: physical_scope,
                    });
                }
                if !matched {
                    return Err(ProjectionRuntimeAuthorityError::InvalidObservation);
                }
            }
        }
        Ok(observations)
    }

    fn exact(
        &self,
        projection: &SurfaceModeledProjection,
    ) -> Result<&ProtocolProjectionProgram, ProjectionRuntimeAuthorityError> {
        let entry = self.entries.get(&registry_key(projection)).ok_or_else(|| {
            ProjectionRuntimeAuthorityError::Registry(format!(
                "selected modeled projection `{}` / `{}` / `{}` is not mounted",
                projection.program_id(),
                projection.binding_id(),
                projection.epoch().as_str()
            ))
        })?;
        if entry.route != *projection.route()
            || entry.binding.id() != projection.binding_id()
            || entry.binding.program_id() != projection.program_id()
        {
            return Err(ProjectionRuntimeAuthorityError::Registry(
                "selected modeled projection differs from its exact mounted binding".into(),
            ));
        }
        Ok(entry)
    }
}

fn mutation_proves_delta_operation(
    mutation: &ResolvedProjectionMutation,
    delta: &ProjectionDeltaMutation,
    authorization: &impl ProjectionAuthorization,
) -> Result<bool, ProjectionDeltaError> {
    match delta {
        ProjectionDeltaMutation::Upsert { scope, .. }
        | ProjectionDeltaMutation::Patch { scope, .. }
        | ProjectionDeltaMutation::Delete { scope } => authorized_key_matches(
            scope,
            mutation.target().model(),
            mutation.key(),
            authorization,
        ),
        ProjectionDeltaMutation::Link {
            relationship,
            source,
            target,
        } => {
            for effect in mutation.provenance().relationship_effects() {
                if effect.kind() == ProjectionRelationshipEffectKind::Link
                    && authorized_effect_matches(
                        relationship,
                        source,
                        target,
                        effect,
                        authorization,
                    )?
                {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        ProjectionDeltaMutation::Unlink {
            relationship,
            source,
            target,
        } => {
            for effect in mutation.provenance().relationship_effects() {
                if effect.kind() == ProjectionRelationshipEffectKind::Unlink
                    && authorized_effect_matches(
                        relationship,
                        source,
                        target,
                        effect,
                        authorization,
                    )?
                {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        ProjectionDeltaMutation::InvalidateModel { .. }
        | ProjectionDeltaMutation::InvalidateRelationship { .. } => Ok(false),
    }
}

fn authorized_effect_matches(
    relationship: &str,
    source: &ProjectionDeltaScope,
    target: &ProjectionDeltaScope,
    effect: &ResolvedProjectionRelationshipEffect,
    authorization: &impl ProjectionAuthorization,
) -> Result<bool, ProjectionDeltaError> {
    let descriptor = effect.relationship();
    let Some(mapped) = authorization.relationship(
        descriptor.source_model(),
        descriptor.relationship(),
        descriptor.target_model(),
    ) else {
        return Ok(false);
    };
    let Some(source_key) = effect.source_key() else {
        return Ok(false);
    };
    let Some(target_key) = effect.target_key() else {
        return Ok(false);
    };
    Ok(mapped.wire_relationship == relationship
        && mapped.source_wire_model == source.model
        && mapped.target_wire_model == target.model
        && authorized_key_matches(source, descriptor.source_model(), source_key, authorization)?
        && authorized_key_matches(target, descriptor.target_model(), target_key, authorization)?)
}

fn authorized_key_matches(
    scope: &ProjectionDeltaScope,
    logical_model: &str,
    key: &ResolvedProjectionKey,
    authorization: &impl ProjectionAuthorization,
) -> Result<bool, ProjectionDeltaError> {
    let Some(mapped) = authorization.record_key(logical_model, key)? else {
        return Ok(false);
    };
    Ok(mapped.wire_model == scope.model && mapped.fields == scope.key)
}

fn protocol_resolved_key(
    key: &ResolvedProjectionKey,
) -> Result<ProtocolResolvedProjectionKey, ProjectionRuntimeAuthorityError> {
    Ok(ProtocolResolvedProjectionKey {
        fields: key
            .fields()
            .iter()
            .map(|field| {
                Ok(ProtocolResolvedProjectionKeyField {
                    field: field.name().to_owned(),
                    value: projection_value_json(field.value().as_ref())?,
                })
            })
            .collect::<Result<Vec<_>, ProjectionRuntimeAuthorityError>>()?,
    })
}

fn registry_key(projection: &SurfaceModeledProjection) -> ProjectionRegistryKey {
    ProjectionRegistryKey {
        program_id: projection.program_id().to_string(),
        binding_id: projection.binding_id().to_string(),
        epoch: projection.epoch().as_str().to_owned(),
    }
}

fn resolved_partition_json(
    partition: &ResolvedProjectionPartition,
) -> Result<Option<serde_json::Value>, ProjectionRuntimeAuthorityError> {
    match partition.as_ref() {
        ResolvedProjectionPartitionRef::Unit => Ok(None),
        ResolvedProjectionPartitionRef::Value(value) => {
            projection_value_json(value.as_ref()).map(Some)
        }
    }
}

fn delta_value_json(
    value: &super::DeltaValue,
) -> Result<serde_json::Value, ProjectionRuntimeAuthorityError> {
    match value {
        super::DeltaValue::Null => Ok(serde_json::Value::Null),
        super::DeltaValue::Boolean(value) => Ok(serde_json::Value::Bool(*value)),
        super::DeltaValue::I64(value) => value
            .parse::<i64>()
            .map(|value| serde_json::Value::Number(value.into()))
            .map_err(|_| ProjectionRuntimeAuthorityError::InvalidObservation),
        super::DeltaValue::U64(value) => value
            .parse::<u64>()
            .map(|value| serde_json::Value::Number(value.into()))
            .map_err(|_| ProjectionRuntimeAuthorityError::InvalidObservation),
        super::DeltaValue::F64(value) => value
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(serde_json::Value::Number)
            .ok_or(ProjectionRuntimeAuthorityError::InvalidObservation),
        super::DeltaValue::String(value) => Ok(serde_json::Value::String(value.clone())),
        super::DeltaValue::Enum { variant, .. } => Ok(serde_json::Value::String(variant.clone())),
        super::DeltaValue::List(values) => values
            .iter()
            .map(delta_value_json)
            .collect::<Result<Vec<_>, _>>()
            .map(serde_json::Value::Array),
        super::DeltaValue::Object(fields) => fields
            .iter()
            .map(|field| Ok((field.field.clone(), delta_value_json(&field.value)?)))
            .collect::<Result<serde_json::Map<_, _>, _>>()
            .map(serde_json::Value::Object),
    }
}

fn projection_value_json(
    value: crate::ProjectionValueRef<'_>,
) -> Result<serde_json::Value, ProjectionRuntimeAuthorityError> {
    let delta = super::DeltaValue::try_from_projection_ref(value)
        .map_err(ProjectionRuntimeAuthorityError::Delta)?;
    delta_value_json(&delta)
}

/// Production request authority for actual role-safe projection lowering.
///
/// Construction seals the exact selected Surface, verified principal
/// partition, authorization generation, cache scope, causation, keyed codec,
/// and lifetime. No application-provided logical partition or wire identity
/// can be substituted later.
pub(crate) struct ProtocolProjectionDeltaRequestAuthority {
    export: DistributedClientSurfaceExport,
    surface: Arc<Surface>,
    visibility_surface: Arc<Surface>,
    codec: ProtocolTokenCodec,
    principal_scope: PrincipalPartitionId,
    authorization_generation: String,
    cache_scope: ProjectionDeltaCacheScopeToken,
    causation_id: CausationId,
    trusted_presets: Vec<crate::graphql::protocol::DistributedTrustedPreset>,
    issued_at_unix_ms: u64,
    expires_at_unix_ms: u64,
}

impl ProtocolProjectionDeltaRequestAuthority {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        export: DistributedClientSurfaceExport,
        codec: ProtocolTokenCodec,
        principal_scope: PrincipalPartitionId,
        authorization_generation: impl Into<String>,
        cache_scope: &OpaqueProtocolToken,
        causation_id: CausationId,
        now_unix_ms: u64,
        issued_at_unix_ms: u64,
        expires_at_unix_ms: u64,
    ) -> Result<Self, ProjectionRuntimeAuthorityError> {
        let authorization_generation = authorization_generation.into();
        let lifetime = expires_at_unix_ms.checked_sub(issued_at_unix_ms);
        if authorization_generation.trim().is_empty()
            || issued_at_unix_ms > now_unix_ms
            || now_unix_ms >= expires_at_unix_ms
            || !lifetime.is_some_and(|lifetime| {
                lifetime > 0 && lifetime <= MAX_PROJECTION_AUTHORITY_LIFETIME_MS
            })
        {
            return Err(ProjectionRuntimeAuthorityError::InvalidAuthority);
        }
        export
            .manifest()
            .map_err(|_| ProjectionRuntimeAuthorityError::InvalidAuthority)?;
        let surface = export.surface().clone();
        SelectedSurfacePolicyVisibility::try_new(&surface)
            .map_err(|_| ProjectionRuntimeAuthorityError::InvalidAuthority)?;
        let cache_scope = ProjectionDeltaCacheScopeToken::from_protocol(cache_scope)
            .map_err(|_| ProjectionRuntimeAuthorityError::InvalidAuthority)?;
        Ok(Self {
            export,
            visibility_surface: Arc::clone(&surface),
            surface,
            codec,
            principal_scope,
            authorization_generation,
            cache_scope,
            causation_id,
            trusted_presets: Vec::new(),
            issued_at_unix_ms,
            expires_at_unix_ms,
        })
    }

    pub(crate) fn with_visibility_surface(
        mut self,
        visibility_surface: Arc<Surface>,
    ) -> Result<Self, ProjectionRuntimeAuthorityError> {
        validate_visibility_surface(&self.surface, &visibility_surface)?;
        self.visibility_surface = visibility_surface;
        Ok(self)
    }

    pub(crate) fn export(&self) -> &DistributedClientSurfaceExport {
        &self.export
    }

    pub(crate) fn with_trusted_presets(
        mut self,
        trusted_presets: Vec<crate::graphql::protocol::DistributedTrustedPreset>,
    ) -> Self {
        self.trusted_presets = trusted_presets;
        self
    }

    fn lifecycle_proof(
        &self,
        delta: &ProjectionDelta,
        projection_ref: u32,
        identity: &super::ProjectionDeltaProjectionIdentity,
        projection: &SurfaceModeledProjection,
        entry: &ProtocolProjectionProgram,
    ) -> Result<CommandProjectionLifecycleProofV1, ProjectionRuntimeAuthorityError> {
        let token = self
            .codec
            .issue(
                ProtocolTokenPurpose::ProjectionLifecycle,
                &ProjectionLifecycleTokenMaterial {
                    domain: "distributed.graphql.projection-lifecycle",
                    version: 1,
                    issued_at_unix_ms: self.issued_at_unix_ms,
                    expires_at_unix_ms: self.expires_at_unix_ms,
                    identity: &delta.identity,
                    projection_ref,
                    projection: identity,
                    route: projection.route(),
                    topology: entry.codec.topology(),
                },
            )
            .map_err(ProjectionRuntimeAuthorityError::Token)?;
        Ok(CommandProjectionLifecycleProofV1 {
            projection_ref,
            token,
        })
    }

    /// Derive canonical obligations only for finite, observable targets.
    ///
    /// Invalidations and recovery-only paths request revalidation but never
    /// invent a record obligation.
    pub(crate) fn metadata(
        &self,
        delta: ProjectionDelta,
        lifecycle_proofs: Vec<CommandProjectionLifecycleProofV1>,
        eligible: &[&SurfaceModeledProjection],
        observations: &[ModeledProjectionObservationScope],
        force_revalidate: bool,
    ) -> Result<CommandProjectionMetadataV1, ProjectionRuntimeAuthorityError> {
        delta
            .validate_replay_scope(
                &self
                    .export
                    .manifest()
                    .map_err(|_| ProjectionRuntimeAuthorityError::InvalidAuthority)?,
                self,
            )
            .map_err(ProjectionRuntimeAuthorityError::Delta)?;
        self.validate_active_projection_inventory(&delta, eligible)?;

        if observations.len() > MAX_COMMAND_PROJECTION_OBLIGATIONS {
            return Err(ProjectionRuntimeAuthorityError::InvalidMetadata);
        }
        let mut obligations = Vec::with_capacity(observations.len());
        let manifest = self
            .export
            .manifest()
            .map_err(|_| ProjectionRuntimeAuthorityError::InvalidAuthority)?;
        for observation in observations {
            let projection = delta
                .projections
                .get(observation.projection_ref as usize)
                .ok_or(ProjectionRuntimeAuthorityError::InvalidObservation)?;
            let operation = delta
                .operations
                .get(observation.operation_index)
                .ok_or(ProjectionRuntimeAuthorityError::InvalidObservation)?;
            let eligible_projection = eligible.iter().find(|modeled| {
                modeled.program_id().to_string() == projection.program_id
                    && modeled.binding_id().to_string() == projection.binding_id
                    && modeled.epoch().as_str() == projection.epoch
            });
            if observation.scope.model() != observation.model
                || observation.scope.topology().name() != observation.projector
                || operation
                    .projection_refs
                    .binary_search(&observation.projection_ref)
                    .is_err()
                || matches!(
                    operation.mutation,
                    ProjectionDeltaMutation::InvalidateModel { .. }
                        | ProjectionDeltaMutation::InvalidateRelationship { .. }
                )
                || eligible_projection.is_none()
                || !eligible_projection
                    .expect("presence was checked above")
                    .output_models()
                    .iter()
                    .any(|model| model == &observation.model)
            {
                return Err(ProjectionRuntimeAuthorityError::InvalidObservation);
            }
            if obligations.len() == MAX_COMMAND_PROJECTION_OBLIGATIONS {
                return Err(ProjectionRuntimeAuthorityError::InvalidMetadata);
            }
            let token = crate::graphql::protocol::issue_projection_obligation_token(
                &self.codec,
                self.cache_scope.as_str(),
                &manifest.schema_fingerprint,
                self.causation_id.as_str(),
                &observation.projector,
                &observation.model,
                observation.kind,
                &observation.scope,
            )
            .map_err(ProjectionRuntimeAuthorityError::Token)?;
            obligations.push(CommandProjectionObligationV1 {
                projection_ref: observation.projection_ref,
                model: observation.model.clone(),
                scope_token: token,
            });
        }
        let revalidate = force_revalidate
            || !delta.recoveries.is_empty()
            || delta.operations.iter().any(|operation| {
                matches!(
                    operation.mutation,
                    ProjectionDeltaMutation::InvalidateModel { .. }
                        | ProjectionDeltaMutation::InvalidateRelationship { .. }
                )
            });
        CommandProjectionMetadataV1::try_new(
            self.issued_at_unix_ms,
            self.expires_at_unix_ms,
            delta,
            lifecycle_proofs,
            obligations,
            revalidate,
        )
        .map_err(|_| ProjectionRuntimeAuthorityError::InvalidMetadata)
    }

    #[cfg(test)]
    pub(crate) fn verify_partition_token(
        &self,
        token: &str,
        surface: &ProjectionDeltaSurfaceIdentity,
        partition: &ResolvedProjectionPartition,
        now_unix_ms: u64,
    ) -> Result<(), ProjectionRuntimeAuthorityError> {
        if now_unix_ms >= self.expires_at_unix_ms {
            return Err(ProjectionRuntimeAuthorityError::Expired);
        }
        let manifest = self
            .export
            .manifest()
            .map_err(|_| ProjectionRuntimeAuthorityError::InvalidAuthority)?;
        if surface != &ProjectionDeltaSurfaceIdentity::from(&manifest.surface) {
            return Err(ProjectionRuntimeAuthorityError::InvalidAuthority);
        }
        let token =
            OpaqueProtocolToken::parse(token).map_err(ProjectionRuntimeAuthorityError::Token)?;
        self.codec
            .verify(
                &token,
                ProtocolTokenPurpose::ProjectionPartition,
                &PartitionTokenMaterial {
                    domain: "distributed.graphql.projection-partition",
                    version: 1,
                    principal_scope: self.principal_scope.as_str(),
                    surface,
                    authorization_generation: &self.authorization_generation,
                    cache_scope: self.cache_scope.as_str(),
                    expires_at_unix_ms: self.expires_at_unix_ms,
                    canonical_partition: partition.canonical_bytes(),
                },
            )
            .map_err(ProjectionRuntimeAuthorityError::Token)
    }

    fn validate_active_projection_inventory(
        &self,
        delta: &ProjectionDelta,
        eligible: &[&SurfaceModeledProjection],
    ) -> Result<(), ProjectionRuntimeAuthorityError> {
        let mut expected = eligible
            .iter()
            .map(|projection| {
                if !projection.is_causally_eligible() {
                    return Err(ProjectionRuntimeAuthorityError::IneligibleProjection);
                }
                let selected = projection
                    .selected_program()
                    .ok_or(ProjectionRuntimeAuthorityError::IneligibleProjection)?;
                Ok(super::ProjectionDeltaProjectionIdentity {
                    program_id: projection.program_id().to_string(),
                    binding_id: projection.binding_id().to_string(),
                    epoch: projection.epoch().as_str().to_owned(),
                    program_ir_version: selected.ir_version,
                    operation_semantics_version: selected.operation_semantics_version,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        expected.sort();
        expected.dedup();
        if expected != delta.projections {
            return Err(ProjectionRuntimeAuthorityError::IneligibleProjection);
        }
        Ok(())
    }
}

impl ProjectionPartitionScopeEncoder for ProtocolProjectionDeltaRequestAuthority {
    fn encode(
        &self,
        authority: ProjectionPartitionAuthority<'_>,
        partition: &ResolvedProjectionPartition,
    ) -> Result<Option<ProjectionDeltaPartition>, ProjectionDeltaError> {
        if authority.principal_scope != &self.principal_scope
            || authority.authorization_generation != self.authorization_generation
            || authority.cache_scope != &self.cache_scope
        {
            return Err(ProjectionDeltaError::AuthorizationMapping);
        }
        let token = self
            .codec
            .issue(
                ProtocolTokenPurpose::ProjectionPartition,
                &PartitionTokenMaterial {
                    domain: "distributed.graphql.projection-partition",
                    version: 1,
                    principal_scope: self.principal_scope.as_str(),
                    surface: authority.surface,
                    authorization_generation: &self.authorization_generation,
                    cache_scope: self.cache_scope.as_str(),
                    expires_at_unix_ms: self.expires_at_unix_ms,
                    canonical_partition: partition.canonical_bytes(),
                },
            )
            .map_err(|_| ProjectionDeltaError::AuthorizationMapping)?;
        Ok(Some(ProjectionDeltaPartition::Opaque {
            token: token.as_str().to_owned(),
        }))
    }
}

impl ProjectionVisibilityEvaluator for ProtocolProjectionDeltaRequestAuthority {
    fn record_transition(
        &self,
        source: ProjectionMutationSource,
        mutation: &ResolvedProjectionMutation,
    ) -> Result<AuthorizationTransition, ProjectionDeltaError> {
        let Some(model) = self
            .visibility_surface
            .models
            .get(mutation.target().model())
        else {
            return Ok(AuthorizationTransition {
                before: ProjectionDeltaVisibility::Denied,
                after: ProjectionDeltaVisibility::Denied,
            });
        };
        match &model.row_policy {
            crate::graphql::surface::SurfaceRowPolicy::Unrestricted => {
                Ok(AuthorizationTransition {
                    before: ProjectionDeltaVisibility::Authorized,
                    after: ProjectionDeltaVisibility::Authorized,
                })
            }
            crate::graphql::surface::SurfaceRowPolicy::Predicate(predicate)
                if source == ProjectionMutationSource::Actual
                    && mutation.kind().is_complete_write() =>
            {
                let after = evaluate_complete_after_policy(
                    model,
                    mutation,
                    predicate,
                    &self.trusted_presets,
                );
                Ok(AuthorizationTransition {
                    // No physical pre-state was read. A complete after-row
                    // needs no such claim to be safely upserted when it is
                    // authorized now.
                    before: ProjectionDeltaVisibility::Unknown,
                    after,
                })
            }
            crate::graphql::surface::SurfaceRowPolicy::Predicate(_)
            | crate::graphql::surface::SurfaceRowPolicy::ServerOnly => {
                Ok(AuthorizationTransition {
                    before: ProjectionDeltaVisibility::Unknown,
                    after: ProjectionDeltaVisibility::Unknown,
                })
            }
        }
    }

    fn relationship_transition(
        &self,
        source: ProjectionMutationSource,
        effect: &ResolvedProjectionRelationshipEffect,
    ) -> Result<AuthorizationTransition, ProjectionDeltaError> {
        SelectedSurfacePolicyVisibility::try_new(&self.visibility_surface)?
            .relationship_transition(source, effect)
    }
}

fn validate_visibility_surface(
    wire_surface: &Surface,
    visibility_surface: &Surface,
) -> Result<(), ProjectionRuntimeAuthorityError> {
    if matches!(
        visibility_surface.selection,
        crate::graphql::surface::SurfaceSelection::Catalog
    ) || wire_surface.dialect != visibility_surface.dialect
    {
        return Err(ProjectionRuntimeAuthorityError::InvalidAuthority);
    }
    for (model_name, wire_model) in &wire_surface.models {
        let Some(visibility_model) = visibility_surface.models.get(model_name) else {
            return Err(ProjectionRuntimeAuthorityError::InvalidAuthority);
        };
        if wire_model.model_name != visibility_model.model_name
            || wire_model.table_name != visibility_model.table_name
            || wire_model.object_name != visibility_model.object_name
            || wire_model.primary_key != visibility_model.primary_key
            || wire_model
                .columns
                .iter()
                .any(|column| !visibility_model.columns.contains(column))
            || wire_model
                .relationships
                .iter()
                .any(|relationship| !visibility_model.relationships.contains(relationship))
        {
            return Err(ProjectionRuntimeAuthorityError::InvalidAuthority);
        }
    }
    Ok(())
}

fn evaluate_complete_after_policy(
    model: &crate::graphql::surface::SurfaceModel,
    mutation: &ResolvedProjectionMutation,
    predicate: &crate::graphql::filter::FilterExpr,
    trusted_presets: &[crate::graphql::protocol::DistributedTrustedPreset],
) -> ProjectionDeltaVisibility {
    let mut row = BTreeMap::new();
    for column in &model.schema.columns {
        let value = mutation
            .fields()
            .iter()
            .find(|field| field.name() == column.field_name)
            .and_then(|field| match field.value() {
                crate::ResolvedProjectionValue::Value(value) => {
                    projection_value_json(value.as_ref()).ok()
                }
                crate::ResolvedProjectionValue::Absent | crate::ResolvedProjectionValue::Unset => {
                    None
                }
            })
            .or_else(|| {
                mutation
                    .key()
                    .fields()
                    .iter()
                    .find(|field| field.name() == column.field_name)
                    .and_then(|field| projection_value_json(field.value().as_ref()).ok())
            });
        if let Some(value) = value {
            row.insert(column.column_name.as_str(), value);
        }
    }
    match evaluate_filter(predicate, &row, trusted_presets) {
        Some(true) => ProjectionDeltaVisibility::Authorized,
        Some(false) => ProjectionDeltaVisibility::Denied,
        None => ProjectionDeltaVisibility::Unknown,
    }
}

fn evaluate_filter(
    predicate: &crate::graphql::filter::FilterExpr,
    row: &BTreeMap<&str, serde_json::Value>,
    trusted_presets: &[crate::graphql::protocol::DistributedTrustedPreset],
) -> Option<bool> {
    use crate::graphql::filter::{CmpOp, FilterExpr};

    match predicate {
        FilterExpr::And(items) => {
            let evaluated = items
                .iter()
                .map(|item| evaluate_filter(item, row, trusted_presets))
                .collect::<Vec<_>>();
            if evaluated.contains(&Some(false)) {
                Some(false)
            } else if evaluated.iter().all(|value| *value == Some(true)) {
                Some(true)
            } else {
                None
            }
        }
        FilterExpr::Or(items) => {
            let evaluated = items
                .iter()
                .map(|item| evaluate_filter(item, row, trusted_presets))
                .collect::<Vec<_>>();
            if evaluated.contains(&Some(true)) {
                Some(true)
            } else if evaluated.iter().all(|value| *value == Some(false)) {
                Some(false)
            } else {
                None
            }
        }
        FilterExpr::Not(item) => evaluate_filter(item, row, trusted_presets).map(|value| !value),
        FilterExpr::Cmp { column, op, rhs } => {
            let left = row.get(column.as_str())?;
            let right = policy_operand(rhs, trusted_presets)?;
            match op {
                CmpOp::Eq => Some(left == &right),
                CmpOp::Neq => Some(left != &right),
                CmpOp::Gt
                | CmpOp::Gte
                | CmpOp::Lt
                | CmpOp::Lte
                | CmpOp::Like
                | CmpOp::Ilike
                | CmpOp::Contains
                | CmpOp::ContainedIn
                | CmpOp::HasKey => None,
            }
        }
        FilterExpr::In {
            column,
            values,
            negated,
        } => {
            let left = row.get(column.as_str())?;
            let mut unknown = false;
            let matched =
                values
                    .iter()
                    .any(|operand| match policy_operand(operand, trusted_presets) {
                        Some(right) => left == &right,
                        None => {
                            unknown = true;
                            false
                        }
                    });
            if matched {
                Some(!negated)
            } else if unknown {
                None
            } else {
                Some(*negated)
            }
        }
        FilterExpr::IsNull { column, is_null } => row
            .get(column.as_str())
            .map(|value| value.is_null() == *is_null),
        FilterExpr::Rel { .. } => None,
    }
}

fn policy_operand(
    operand: &crate::graphql::filter::Operand,
    trusted_presets: &[crate::graphql::protocol::DistributedTrustedPreset],
) -> Option<serde_json::Value> {
    use crate::graphql::filter::{LitValue, Operand};

    match operand {
        Operand::Claim(claim) => trusted_presets
            .iter()
            .find(|preset| preset.name == claim.header)
            .map(|preset| preset.value.clone()),
        Operand::Lit(LitValue::String(value)) => Some(serde_json::Value::String(value.clone())),
        Operand::Lit(LitValue::I64(value)) => Some(serde_json::Value::Number((*value).into())),
        Operand::Lit(LitValue::F64(value)) => {
            serde_json::Number::from_f64(*value).map(serde_json::Value::Number)
        }
        Operand::Lit(LitValue::Bool(value)) => Some(serde_json::Value::Bool(*value)),
        Operand::Lit(LitValue::Json(value)) => Some(value.clone()),
        Operand::Lit(LitValue::Null) => Some(serde_json::Value::Null),
    }
}

impl ProjectionDeltaRequestAuthority for ProtocolProjectionDeltaRequestAuthority {
    fn authorization_generation(&self) -> &str {
        &self.authorization_generation
    }

    fn principal_scope(&self) -> &PrincipalPartitionId {
        &self.principal_scope
    }

    fn cache_scope(&self) -> &ProjectionDeltaCacheScopeToken {
        &self.cache_scope
    }

    fn command_causation_id(&self) -> &CausationId {
        &self.causation_id
    }
}

#[derive(Serialize)]
struct PartitionTokenMaterial<'a> {
    domain: &'static str,
    version: u16,
    principal_scope: &'a str,
    surface: &'a ProjectionDeltaSurfaceIdentity,
    authorization_generation: &'a str,
    cache_scope: &'a str,
    expires_at_unix_ms: u64,
    canonical_partition: &'a [u8],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ProjectionRuntimeAuthorityError {
    InvalidAuthority,
    Expired,
    IneligibleProjection,
    InvalidObservation,
    EventOutsideContract,
    Registry(String),
    Plan(String),
    Delta(ProjectionDeltaError),
    Token(ProtocolTokenError),
    InvalidMetadata,
}

impl std::fmt::Display for ProjectionRuntimeAuthorityError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidAuthority => "invalid projection request authority",
            Self::Expired => "projection request authority has expired",
            Self::IneligibleProjection => {
                "projection is not active and causally eligible for this request"
            }
            Self::InvalidObservation => {
                "projection observation does not match the authoritative delta"
            }
            Self::EventOutsideContract => {
                "actual domain event is outside the command's sealed emitted-event set"
            }
            Self::Registry(_) => "modeled projection registry validation failed",
            Self::Plan(_) => "actual domain event projection planning failed",
            Self::Delta(_) => "projection delta validation failed",
            Self::Token(_) => "projection scope token validation failed",
            Self::InvalidMetadata => "command projection metadata is invalid",
        })
    }
}

impl std::error::Error for ProjectionRuntimeAuthorityError {}
