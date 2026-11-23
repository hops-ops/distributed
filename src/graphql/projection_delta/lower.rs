use std::collections::{BTreeMap, BTreeSet};

use crate::projection::placement::{
    ProjectionBindingState, ProjectionExecutionClass, ProjectionPlacement,
};
use crate::projection_protocol::MAX_PROJECTION_PARTITION_BYTES;
use crate::{
    MutationKind, ProjectionInvalidation, ProjectionRelationshipEffectKind, ResolvedProjectionKey,
    ResolvedProjectionMutation, ResolvedProjectionPartition, ResolvedProjectionPartitionRef,
    ResolvedProjectionPlan, ResolvedProjectionRelationshipEffect, ResolvedProjectionValue,
};

use super::authorization::{
    ProjectionAuthorization, ProjectionPartitionScopeEncoder, ProjectionVisibilityEvaluator,
    SelectedSurfaceAuthorization,
};
use super::canonical::{canonicalize_operations, canonicalize_recoveries};
use super::types::{
    AuthorizationTransition, ProjectionDeltaCacheScopeToken, ProjectionDeltaVisibility,
    ProjectionMutationSource, MAX_PROJECTION_DELTA_OPERATIONS,
};
use super::{
    DeltaField, DeltaValue, ProjectionDelta, ProjectionDeltaError, ProjectionDeltaIdentity,
    ProjectionDeltaMutation, ProjectionDeltaOccurrence, ProjectionDeltaOperation,
    ProjectionDeltaPartition, ProjectionDeltaProjectionIdentity, ProjectionDeltaRecovery,
    ProjectionDeltaRecoveryCondition, ProjectionDeltaRecoveryTarget, ProjectionDeltaScope,
    ProjectionDeltaSurfaceIdentity, PROJECTION_DELTA_WIRE_VERSION,
};

/// One sealed lowering authority derived from an unforgeable selected client
/// export and one exact request authorization scope.
///
/// The manifest is generated internally from the same selected Surface used
/// for all mappings. Callers cannot independently pair a Surface, manifest,
/// context, or projection source from different authorization selections.
pub(crate) struct ProjectionDeltaAuthority<'a, R> {
    surface: &'a crate::graphql::surface::Surface,
    manifest: crate::graphql::client_manifest::DistributedClientManifest,
    context: ProjectionDeltaContext,
    authorization: SelectedSurfaceAuthorization<'a, R, R>,
}

/// One authenticated request authority supplied by protocol integration.
///
/// Task 13 deliberately provides no production/default implementation. The
/// implementation owns visibility evidence, authenticated partition encoding,
/// the verified principal partition, authorization generation, and durable
/// causation as one inseparable object.
pub(crate) trait ProjectionDeltaRequestAuthority:
    ProjectionVisibilityEvaluator + ProjectionPartitionScopeEncoder
{
    fn authorization_generation(&self) -> &str;
    fn principal_scope(&self) -> &crate::command_ledger::PrincipalPartitionId;
    fn cache_scope(&self) -> &ProjectionDeltaCacheScopeToken;
    fn command_causation_id(&self) -> &crate::command_ledger::CausationId;
}

impl<'a, R> ProjectionDeltaAuthority<'a, R>
where
    R: ProjectionDeltaRequestAuthority,
{
    pub(crate) fn try_new(
        export: &'a crate::graphql::client_manifest::DistributedClientSurfaceExport,
        request: &'a R,
    ) -> Result<Self, ProjectionDeltaError> {
        let manifest = export
            .manifest()
            .map_err(|_| ProjectionDeltaError::ProjectionIdentityMismatch)?;
        let context = ProjectionDeltaContext::from_manifest(
            &manifest,
            request.authorization_generation(),
            request.cache_scope(),
            request.command_causation_id(),
        )?;
        let surface = export.surface().as_ref();
        let authorization = SelectedSurfaceAuthorization::try_new(
            surface,
            request,
            request,
            context.surface.clone(),
            request.authorization_generation(),
            request.principal_scope().clone(),
            request.cache_scope().clone(),
        )?;
        Ok(Self {
            surface,
            manifest,
            context,
            authorization,
        })
    }

    /// Pin one exact selected projection from this authority's own export.
    pub(crate) fn source(
        &self,
        projection: &crate::graphql::surface::SurfaceModeledProjection,
    ) -> Result<ProjectionDeltaSource, ProjectionDeltaError> {
        let selected_projection = self
            .surface
            .projectors
            .iter()
            .flat_map(|owner| &owner.modeled)
            .find(|modeled| {
                modeled.program_id() == projection.program_id()
                    && modeled.binding_id() == projection.binding_id()
            })
            .ok_or(ProjectionDeltaError::ProjectionIdentityMismatch)?;
        ProjectionDeltaSource::try_from_export(
            selected_projection,
            &self.manifest,
            self.context.wire_identity(),
        )
    }

    /// Lower zero or more actual/preview occurrences under this exact
    /// authority. An empty actual batch is an authoritative empty delta.
    pub(crate) fn lower(
        &self,
        batch: &[ProjectionDeltaPlanOccurrence<'_>],
    ) -> Result<ProjectionDelta, ProjectionDeltaError> {
        lower_projection_delta(&self.context, batch, &self.authorization)
    }
}

/// Exact eligible binding pins captured only by [`ProjectionDeltaAuthority`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProjectionDeltaSource {
    authority: ProjectionDeltaIdentity,
    program_id: String,
    binding_id: String,
    epoch: String,
    program_ir_version: u16,
    operation_semantics_version: u16,
    placement: ProjectionPlacement,
    execution_class: ProjectionExecutionClass,
    state: ProjectionBindingState,
    selected: crate::graphql::surface::SurfaceSelectedProjectionProgram,
}

impl ProjectionDeltaSource {
    fn try_from_export(
        projection: &crate::graphql::surface::SurfaceModeledProjection,
        manifest: &crate::graphql::client_manifest::DistributedClientManifest,
        authority: ProjectionDeltaIdentity,
    ) -> Result<Self, ProjectionDeltaError> {
        let selected = projection
            .selected_program()
            .ok_or(ProjectionDeltaError::IneligibleBinding)?;
        let program_id = projection.program_id().to_string();
        let binding_id = projection.binding_id().to_string();
        let manifest_program = manifest
            .projection_programs
            .iter()
            .find(|program| program.program_id == program_id)
            .ok_or(ProjectionDeltaError::ProjectionIdentityMismatch)?;
        let manifest_binding = manifest
            .projection_bindings
            .iter()
            .find(|binding| binding.binding_id == binding_id)
            .ok_or(ProjectionDeltaError::ProjectionIdentityMismatch)?;
        let manifest_state = manifest_binding.state;
        let manifest_placement = manifest_binding.placement;
        let manifest_execution = manifest_binding.execution_class;
        if manifest_binding.program_id != program_id
            || manifest_binding.epoch != projection.epoch().as_str()
            || manifest_program.name != selected.name
            || manifest_program.program_version != selected.version
            || manifest_program.ir_version != selected.ir_version
            || manifest_program.operation_semantics_version != selected.operation_semantics_version
            || manifest_state != projection.state()
            || manifest_placement != projection.placement()
            || manifest_execution != projection.execution_class()
        {
            return Err(ProjectionDeltaError::ProjectionIdentityMismatch);
        }
        let source = Self {
            authority,
            program_id,
            binding_id,
            epoch: projection.epoch().as_str().to_owned(),
            program_ir_version: selected.ir_version,
            operation_semantics_version: selected.operation_semantics_version,
            placement: projection.placement(),
            execution_class: projection.execution_class(),
            state: projection.state(),
            selected: selected.clone(),
        };
        source.validate_for(ProjectionMutationSource::Actual)?;
        Ok(source)
    }

    fn wire_identity(&self) -> ProjectionDeltaProjectionIdentity {
        ProjectionDeltaProjectionIdentity {
            program_id: self.program_id.clone(),
            binding_id: self.binding_id.clone(),
            epoch: self.epoch.clone(),
            program_ir_version: self.program_ir_version,
            operation_semantics_version: self.operation_semantics_version,
        }
    }

    fn validate_for(&self, source: ProjectionMutationSource) -> Result<(), ProjectionDeltaError> {
        if self.placement != ProjectionPlacement::Eventual
            || self.execution_class != ProjectionExecutionClass::Causal
        {
            return Err(ProjectionDeltaError::IneligibleBinding);
        }
        if source == ProjectionMutationSource::Preview
            && self.state != ProjectionBindingState::Active
        {
            return Err(ProjectionDeltaError::IneligibleBinding);
        }
        self.wire_identity().validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProjectionDeltaContext {
    manifest_version: u32,
    client_protocol_version: u32,
    surface: ProjectionDeltaSurfaceIdentity,
    schema_fingerprint: String,
    protocol_fingerprint: String,
    authorization_generation: String,
    cache_scope_token: String,
    command_causation_id: String,
}

impl ProjectionDeltaContext {
    fn from_manifest(
        manifest: &crate::graphql::client_manifest::DistributedClientManifest,
        authorization_generation: impl Into<String>,
        cache_scope_token: &ProjectionDeltaCacheScopeToken,
        command_causation_id: &crate::command_ledger::CausationId,
    ) -> Result<Self, ProjectionDeltaError> {
        let context = Self {
            manifest_version: manifest.manifest_version,
            client_protocol_version: manifest.protocol_version,
            surface: ProjectionDeltaSurfaceIdentity::from(&manifest.surface),
            schema_fingerprint: manifest.schema_fingerprint.clone(),
            protocol_fingerprint: manifest.protocol_fingerprint.clone(),
            authorization_generation: authorization_generation.into(),
            cache_scope_token: cache_scope_token.as_str().to_owned(),
            command_causation_id: command_causation_id.as_str().to_owned(),
        };
        context.wire_identity().validate()?;
        Ok(context)
    }

    fn wire_identity(&self) -> ProjectionDeltaIdentity {
        ProjectionDeltaIdentity {
            manifest_version: self.manifest_version,
            client_protocol_version: self.client_protocol_version,
            surface: self.surface.clone(),
            schema_fingerprint: self.schema_fingerprint.clone(),
            protocol_fingerprint: self.protocol_fingerprint.clone(),
            authorization_generation: self.authorization_generation.clone(),
            cache_scope_token: self.cache_scope_token.clone(),
            command_causation_id: self.command_causation_id.clone(),
        }
    }
}

/// One zero-based command occurrence and all selected program plans.
#[derive(Clone, Debug)]
pub(crate) struct ProjectionDeltaPlanOccurrence<'a> {
    source: ProjectionMutationSource,
    occurrence_id: String,
    plans: Vec<(&'a ProjectionDeltaSource, &'a ResolvedProjectionPlan)>,
}

impl<'a> ProjectionDeltaPlanOccurrence<'a> {
    pub(crate) fn actual(
        plans: Vec<(&'a ProjectionDeltaSource, &'a ResolvedProjectionPlan)>,
    ) -> Result<Self, ProjectionDeltaError> {
        Self::try_new(ProjectionMutationSource::Actual, plans)
    }

    #[cfg(test)]
    pub(crate) fn preview(
        plans: Vec<(&'a ProjectionDeltaSource, &'a ResolvedProjectionPlan)>,
    ) -> Result<Self, ProjectionDeltaError> {
        Self::try_new(ProjectionMutationSource::Preview, plans)
    }

    fn try_new(
        source: ProjectionMutationSource,
        mut plans: Vec<(&'a ProjectionDeltaSource, &'a ResolvedProjectionPlan)>,
    ) -> Result<Self, ProjectionDeltaError> {
        let occurrence = plans
            .first()
            .map(|(_, plan)| plan.occurrence().clone())
            .ok_or(ProjectionDeltaError::InvalidOperation(
                "projection occurrence must contain at least one canonical resolved plan",
            ))?;
        let occurrence_id = occurrence.id().to_owned();
        let mut programs = BTreeSet::new();
        for (projection, plan) in &plans {
            projection.validate_for(source)?;
            if plan.occurrence() != &occurrence
                || plan.occurrence().causation_id()
                    != Some(projection.authority.command_causation_id.as_str())
            {
                return Err(ProjectionDeltaError::ProjectionIdentityMismatch);
            }
            validate_plan(projection, plan, &occurrence_id)?;
            if !programs.insert(&projection.program_id) {
                return Err(ProjectionDeltaError::InvalidOperation(
                    "one occurrence cannot select multiple bindings for one program",
                ));
            }
        }
        plans.sort_by_key(|(projection, _)| projection.wire_identity());
        Ok(Self {
            source,
            occurrence_id,
            plans,
        })
    }
}

fn lower_projection_delta(
    context: &ProjectionDeltaContext,
    batch: &[ProjectionDeltaPlanOccurrence<'_>],
    authorization: &impl ProjectionAuthorization,
) -> Result<ProjectionDelta, ProjectionDeltaError> {
    if batch.len() > MAX_PROJECTION_DELTA_OPERATIONS {
        return Err(ProjectionDeltaError::TooManyOccurrences {
            len: batch.len(),
            max: MAX_PROJECTION_DELTA_OPERATIONS,
        });
    }
    if let Some(source) = batch.first().map(|occurrence| occurrence.source) {
        if batch.iter().any(|occurrence| occurrence.source != source) {
            return Err(ProjectionDeltaError::InvalidOperation(
                "actual and preview occurrences cannot share one projection delta",
            ));
        }
    }
    let identity = context.wire_identity();
    identity.validate()?;
    let mut sources = Vec::with_capacity(MAX_PROJECTION_DELTA_OPERATIONS);
    for source in batch
        .iter()
        .flat_map(|occurrence| occurrence.plans.iter().map(|(source, _)| *source))
    {
        if source.authority != identity {
            return Err(ProjectionDeltaError::ProjectionIdentityMismatch);
        }
        if sources.iter().any(|existing: &&ProjectionDeltaSource| {
            existing.program_id == source.program_id
                && existing.binding_id == source.binding_id
                && existing.epoch == source.epoch
                && existing.program_ir_version == source.program_ir_version
                && existing.operation_semantics_version == source.operation_semantics_version
        }) {
            continue;
        }
        if sources.len() == MAX_PROJECTION_DELTA_OPERATIONS {
            return Err(ProjectionDeltaError::TooManyProjections {
                len: MAX_PROJECTION_DELTA_OPERATIONS + 1,
                max: MAX_PROJECTION_DELTA_OPERATIONS,
            });
        }
        sources.push(source);
    }
    sources.sort_by_key(|source| source.wire_identity());
    let projections = sources
        .iter()
        .map(|source| source.wire_identity())
        .collect::<Vec<_>>();
    let projection_indexes = projections
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, identity)| (identity, index as u32))
        .collect::<BTreeMap<_, _>>();

    let mut partition_cache = BTreeMap::<Vec<u8>, Option<ProjectionDeltaPartition>>::new();
    let mut occurrences = Vec::with_capacity(batch.len());
    let mut operations = Vec::new();
    let mut recoveries = Vec::new();
    for (ordinal, occurrence) in batch.iter().enumerate() {
        occurrences.push(ProjectionDeltaOccurrence {
            causation_id: context.command_causation_id.clone(),
            ordinal: ordinal as u32,
            occurrence_id: occurrence.occurrence_id.clone(),
        });
        for (source, plan) in &occurrence.plans {
            if source.authority != identity {
                return Err(ProjectionDeltaError::ProjectionIdentityMismatch);
            }
            source.validate_for(occurrence.source)?;
            validate_plan(source, plan, &occurrence.occurrence_id)?;
            let partition =
                encoded_partition(plan.partition(), authorization, &mut partition_cache)?;
            let projection_ref = projection_indexes[&source.wire_identity()];
            for mutation in plan.mutations() {
                let selected_operations = selected_operations(source, mutation)?;
                if selected_operations.is_empty() {
                    continue;
                }
                lower_relationship_consequences(
                    ordinal as u32,
                    projection_ref,
                    occurrence.source,
                    mutation,
                    &selected_operations,
                    partition.clone(),
                    authorization,
                    &mut operations,
                    &mut recoveries,
                )?;
                lower_record_consequence(
                    ordinal as u32,
                    projection_ref,
                    occurrence.source,
                    mutation,
                    &selected_operations,
                    partition.clone(),
                    authorization,
                    &mut operations,
                    &mut recoveries,
                )?;
                enforce_raw_bounds(&operations, &recoveries)?;
            }
        }
    }
    let operations = canonicalize_operations(operations)?;
    let recoveries = canonicalize_recoveries(recoveries, &operations);
    let delta = ProjectionDelta {
        wire_version: PROJECTION_DELTA_WIRE_VERSION,
        identity,
        projections,
        occurrences,
        operations,
        recoveries,
    };
    delta.validate()?;
    Ok(delta)
}

fn encoded_partition(
    partition: &ResolvedProjectionPartition,
    authorization: &impl ProjectionAuthorization,
    cache: &mut BTreeMap<Vec<u8>, Option<ProjectionDeltaPartition>>,
) -> Result<Option<ProjectionDeltaPartition>, ProjectionDeltaError> {
    let canonical = partition.canonical_bytes();
    if canonical.len() > MAX_PROJECTION_PARTITION_BYTES {
        return Err(ProjectionDeltaError::PartitionTooLarge {
            len: canonical.len(),
            max: MAX_PROJECTION_PARTITION_BYTES,
        });
    }
    if let Some(encoded) = cache.get(canonical) {
        return Ok(encoded.clone());
    }
    let encoded = authorization.partition(partition)?;
    match (partition.as_ref(), &encoded) {
        (ResolvedProjectionPartitionRef::Unit, Some(ProjectionDeltaPartition::Unit))
        | (ResolvedProjectionPartitionRef::Value(_), None)
        | (
            ResolvedProjectionPartitionRef::Value(_),
            Some(ProjectionDeltaPartition::Opaque { .. }),
        ) => {}
        _ => return Err(ProjectionDeltaError::AuthorizationMapping),
    }
    if let Some(encoded) = &encoded {
        encoded.validate()?;
    }
    cache.insert(canonical.to_vec(), encoded.clone());
    Ok(encoded)
}

fn validate_plan(
    source: &ProjectionDeltaSource,
    plan: &ResolvedProjectionPlan,
    occurrence_id: &str,
) -> Result<(), ProjectionDeltaError> {
    if plan.program_id().to_string() != source.program_id || plan.occurrence().id() != occurrence_id
    {
        return Err(ProjectionDeltaError::ProjectionIdentityMismatch);
    }
    let selected_arm = source
        .selected
        .arms
        .iter()
        .find(|arm| arm.arm_id == plan.arm_id() && arm.selector.matches(plan.occurrence()))
        .ok_or(ProjectionDeltaError::ProjectionIdentityMismatch)?;
    for mutation in plan.mutations() {
        if mutation.provenance().program_id() != plan.program_id()
            || mutation.provenance().occurrence().occurrence_id() != occurrence_id
            || mutation.provenance().arm_id() != selected_arm.arm_id
            || mutation.provenance().operation_ids().len()
                != mutation.provenance().staging_ordinals().len()
        {
            return Err(ProjectionDeltaError::ProjectionIdentityMismatch);
        }
        for (operation_id, ordinal) in mutation
            .provenance()
            .operation_ids()
            .iter()
            .zip(mutation.provenance().staging_ordinals())
        {
            if let Some(selected) = selected_arm
                .operations
                .iter()
                .find(|operation| operation.operation_id == *operation_id)
            {
                if selected.staging_ordinal != *ordinal {
                    return Err(ProjectionDeltaError::ProjectionIdentityMismatch);
                }
            }
        }
    }
    Ok(())
}

fn selected_operations<'a>(
    source: &'a ProjectionDeltaSource,
    mutation: &ResolvedProjectionMutation,
) -> Result<Vec<&'a crate::graphql::surface::SurfaceProjectionOperation>, ProjectionDeltaError> {
    let arm = source
        .selected
        .arms
        .iter()
        .find(|arm| arm.arm_id == mutation.provenance().arm_id())
        .ok_or(ProjectionDeltaError::ProjectionIdentityMismatch)?;
    let ordinals = mutation
        .provenance()
        .operation_ids()
        .iter()
        .zip(mutation.provenance().staging_ordinals())
        .collect::<BTreeMap<_, _>>();
    Ok(arm
        .operations
        .iter()
        .filter(|operation| {
            ordinals
                .get(&operation.operation_id)
                .is_some_and(|ordinal| **ordinal == operation.staging_ordinal)
        })
        .collect())
}

#[expect(
    clippy::too_many_arguments,
    reason = "the lowering boundary keeps authority inputs and bounded outputs explicit"
)]
fn lower_record_consequence(
    occurrence_ordinal: u32,
    projection_ref: u32,
    source: ProjectionMutationSource,
    mutation: &ResolvedProjectionMutation,
    selected_operations: &[&crate::graphql::surface::SurfaceProjectionOperation],
    partition: Option<ProjectionDeltaPartition>,
    authorization: &impl ProjectionAuthorization,
    operations: &mut Vec<ProjectionDeltaOperation>,
    recoveries: &mut Vec<ProjectionDeltaRecovery>,
) -> Result<(), ProjectionDeltaError> {
    let row_selected = selected_operations
        .iter()
        .any(|operation| operation.model == mutation.target().model());
    if !row_selected {
        return Ok(());
    }
    let Some(model) = authorization.model(mutation.target().model()) else {
        return Ok(());
    };
    if selected_operations
        .iter()
        .any(|operation| operation.force_revalidate)
    {
        return emit_model_recovery(
            occurrence_ordinal,
            projection_ref,
            partition,
            model.wire_model,
            operations,
            recoveries,
        );
    }
    let transition = authorization.record_transition(source, mutation)?;
    let complete_after_only = source == ProjectionMutationSource::Actual
        && transition.before == ProjectionDeltaVisibility::Unknown
        && transition.after == ProjectionDeltaVisibility::Authorized
        && complete_write(mutation.kind());
    match (transition.before, transition.after) {
        (ProjectionDeltaVisibility::Authorized, ProjectionDeltaVisibility::Authorized) => {}
        (ProjectionDeltaVisibility::Unknown, ProjectionDeltaVisibility::Authorized)
            if complete_after_only => {}
        (ProjectionDeltaVisibility::Denied, ProjectionDeltaVisibility::Denied) => return Ok(()),
        (ProjectionDeltaVisibility::Unknown, _) | (_, ProjectionDeltaVisibility::Unknown) => {
            push_recovery(
                recoveries,
                recovery(
                    occurrence_ordinal,
                    projection_ref,
                    ProjectionDeltaRecoveryTarget::Model {
                        partition,
                        model: model.wire_model,
                    },
                ),
            )?;
            return Ok(());
        }
        _ => {
            let scope = authorized_scope(
                partition.clone(),
                mutation.target().model(),
                mutation.key(),
                authorization,
            )?;
            push_recovery(
                recoveries,
                recovery(
                    occurrence_ordinal,
                    projection_ref,
                    scope
                        .map(|scope| ProjectionDeltaRecoveryTarget::Record { scope })
                        .unwrap_or(ProjectionDeltaRecoveryTarget::Model {
                            partition,
                            model: model.wire_model,
                        }),
                ),
            )?;
            return Ok(());
        }
    }
    let scope = authorized_scope(
        partition.clone(),
        mutation.target().model(),
        mutation.key(),
        authorization,
    )?;
    let Some(scope) = scope else {
        return emit_model_recovery(
            occurrence_ordinal,
            projection_ref,
            partition,
            model.wire_model,
            operations,
            recoveries,
        );
    };
    if mutation.kind() == MutationKind::Delete {
        return push_operation(
            operations,
            ProjectionDeltaOperation {
                occurrence_ordinal,
                projection_refs: vec![projection_ref],
                mutation: ProjectionDeltaMutation::Delete { scope },
            },
        );
    }

    let allowed_fields = selected_operations
        .iter()
        .flat_map(|operation| operation.fields.iter().map(|field| field.name()))
        .collect::<BTreeSet<_>>();
    let replacement = model.replacement_fields;
    let replacement_set = replacement
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut set = Vec::new();
    let mut unset = Vec::new();
    let mut incomplete = false;
    for field in mutation.fields() {
        if !allowed_fields.contains(field.name()) {
            continue;
        }
        let Some(mapped) = authorization.field(mutation.target().model(), field.name()) else {
            continue;
        };
        if !replacement_set.contains(mapped.wire_field.as_str()) {
            continue;
        }
        match field.value() {
            ResolvedProjectionValue::Value(value) => set.push(DeltaField {
                field: mapped.wire_field,
                value: DeltaValue::try_from_projection_ref(value.as_ref())?,
            }),
            ResolvedProjectionValue::Absent => incomplete = true,
            ResolvedProjectionValue::Unset => unset.push(mapped.wire_field),
        }
    }
    set.sort_by(|left, right| left.field.cmp(&right.field));
    unset.sort();
    let set_names = set
        .iter()
        .map(|field| field.field.as_str())
        .collect::<Vec<_>>();
    let exact_complete = complete_write(mutation.kind())
        && !incomplete
        && unset.is_empty()
        && set_names == replacement.iter().map(String::as_str).collect::<Vec<_>>();
    if exact_complete {
        return push_operation(
            operations,
            ProjectionDeltaOperation {
                occurrence_ordinal,
                projection_refs: vec![projection_ref],
                mutation: ProjectionDeltaMutation::Upsert {
                    scope,
                    fields: set,
                    replace: replacement,
                },
            },
        );
    }
    if complete_after_only {
        return emit_model_recovery(
            occurrence_ordinal,
            projection_ref,
            partition,
            model.wire_model,
            operations,
            recoveries,
        );
    }
    let patch_emitted = !set.is_empty() || !unset.is_empty();
    if patch_emitted {
        push_operation(
            operations,
            ProjectionDeltaOperation {
                occurrence_ordinal,
                projection_refs: vec![projection_ref],
                mutation: ProjectionDeltaMutation::Patch {
                    scope: scope.clone(),
                    set,
                    unset,
                    if_present: true,
                },
            },
        )?;
    }
    push_recovery(
        recoveries,
        recovery_with_condition(
            occurrence_ordinal,
            projection_ref,
            if patch_emitted {
                ProjectionDeltaRecoveryCondition::IfRecordMissing
            } else {
                ProjectionDeltaRecoveryCondition::Always
            },
            ProjectionDeltaRecoveryTarget::Record { scope },
        ),
    )
}

fn complete_write(kind: MutationKind) -> bool {
    matches!(
        kind,
        MutationKind::Insert
            | MutationKind::Upsert
            | MutationKind::Recreate
            | MutationKind::InsertRelated
            | MutationKind::UpsertRelated
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "relationship authorization is intentionally independent of row lowering"
)]
fn lower_relationship_consequences(
    occurrence_ordinal: u32,
    projection_ref: u32,
    source: ProjectionMutationSource,
    mutation: &ResolvedProjectionMutation,
    selected_operations: &[&crate::graphql::surface::SurfaceProjectionOperation],
    partition: Option<ProjectionDeltaPartition>,
    authorization: &impl ProjectionAuthorization,
    operations: &mut Vec<ProjectionDeltaOperation>,
    recoveries: &mut Vec<ProjectionDeltaRecovery>,
) -> Result<(), ProjectionDeltaError> {
    for operation in selected_operations {
        for selected_effect in &operation.relationship_effects {
            let Some(effect) =
                mutation
                    .provenance()
                    .relationship_effects()
                    .iter()
                    .find(|resolved| {
                        resolved.ordinal() == selected_effect.ordinal()
                            && resolved.relationship() == selected_effect.relationship()
                    })
            else {
                return Err(ProjectionDeltaError::ProjectionIdentityMismatch);
            };
            lower_relationship_effect(
                occurrence_ordinal,
                projection_ref,
                source,
                effect,
                selected_effect.kind(),
                partition.clone(),
                authorization,
                operations,
                recoveries,
            )?;
        }
    }
    let selected_invalidations = selected_operations
        .iter()
        .flat_map(|operation| &operation.invalidations)
        .collect::<BTreeSet<_>>();
    for invalidation in selected_invalidations {
        match invalidation {
            ProjectionInvalidation::Model { model } => {
                if let Some(model) = authorization.model(model) {
                    emit_model_recovery(
                        occurrence_ordinal,
                        projection_ref,
                        partition.clone(),
                        model.wire_model,
                        operations,
                        recoveries,
                    )?;
                }
            }
            ProjectionInvalidation::Relationship {
                source_model,
                relationship,
                target_model,
            } => {
                let proven = selected_operations.iter().any(|operation| {
                    operation.relationship_effects.iter().any(|effect| {
                        let descriptor = effect.relationship();
                        effect.kind() == ProjectionRelationshipEffectKind::Invalidate
                            && descriptor.source_model() == source_model
                            && descriptor.relationship() == relationship
                            && descriptor.target_model() == target_model
                    })
                });
                if proven {
                    continue;
                }
                let explicit = mutation.provenance().invalidations().contains(invalidation);
                if !explicit {
                    return Err(ProjectionDeltaError::ProjectionIdentityMismatch);
                }
                let raw_effect =
                    mutation
                        .provenance()
                        .relationship_effects()
                        .iter()
                        .find(|effect| {
                            let descriptor = effect.relationship();
                            effect.kind() == ProjectionRelationshipEffectKind::Invalidate
                                && descriptor.source_model() == source_model
                                && descriptor.relationship() == relationship
                                && descriptor.target_model() == target_model
                        });
                if let Some(raw_effect) = raw_effect {
                    lower_relationship_effect(
                        occurrence_ordinal,
                        projection_ref,
                        source,
                        raw_effect,
                        ProjectionRelationshipEffectKind::Invalidate,
                        partition.clone(),
                        authorization,
                        operations,
                        recoveries,
                    )?;
                } else if let Some(model) = authorization.model(source_model) {
                    emit_model_recovery(
                        occurrence_ordinal,
                        projection_ref,
                        partition.clone(),
                        model.wire_model,
                        operations,
                        recoveries,
                    )?;
                }
            }
        }
    }
    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "relationship authorization is explicit for both endpoints and outputs"
)]
fn lower_relationship_effect(
    occurrence_ordinal: u32,
    projection_ref: u32,
    source_kind: ProjectionMutationSource,
    effect: &ResolvedProjectionRelationshipEffect,
    selected_kind: ProjectionRelationshipEffectKind,
    partition: Option<ProjectionDeltaPartition>,
    authorization: &impl ProjectionAuthorization,
    operations: &mut Vec<ProjectionDeltaOperation>,
    recoveries: &mut Vec<ProjectionDeltaRecovery>,
) -> Result<(), ProjectionDeltaError> {
    let relationship = effect.relationship();
    let Some(mapped) = authorization.relationship(
        relationship.source_model(),
        relationship.relationship(),
        relationship.target_model(),
    ) else {
        return Ok(());
    };
    let source = effect
        .source_key()
        .map(|key| {
            authorized_scope(
                partition.clone(),
                relationship.source_model(),
                key,
                authorization,
            )
        })
        .transpose()?
        .flatten();
    let target = effect
        .target_key()
        .map(|key| {
            authorized_scope(
                partition.clone(),
                relationship.target_model(),
                key,
                authorization,
            )
        })
        .transpose()?
        .flatten();
    if source
        .as_ref()
        .is_some_and(|scope| scope.model != mapped.source_wire_model)
        || target
            .as_ref()
            .is_some_and(|scope| scope.model != mapped.target_wire_model)
    {
        return Err(ProjectionDeltaError::AuthorizationMapping);
    }
    let transition = authorization.relationship_transition(source_kind, effect)?;
    if transition
        == (AuthorizationTransition {
            before: ProjectionDeltaVisibility::Authorized,
            after: ProjectionDeltaVisibility::Authorized,
        })
    {
        match (selected_kind, source.clone(), target) {
            (ProjectionRelationshipEffectKind::Link, Some(source), Some(target)) => {
                return push_operation(
                    operations,
                    ProjectionDeltaOperation {
                        occurrence_ordinal,
                        projection_refs: vec![projection_ref],
                        mutation: ProjectionDeltaMutation::Link {
                            relationship: mapped.wire_relationship,
                            source,
                            target,
                        },
                    },
                );
            }
            (ProjectionRelationshipEffectKind::Unlink, Some(source), Some(target)) => {
                return push_operation(
                    operations,
                    ProjectionDeltaOperation {
                        occurrence_ordinal,
                        projection_refs: vec![projection_ref],
                        mutation: ProjectionDeltaMutation::Unlink {
                            relationship: mapped.wire_relationship,
                            source,
                            target,
                        },
                    },
                );
            }
            _ => {}
        }
    }
    if transition
        == (AuthorizationTransition {
            before: ProjectionDeltaVisibility::Denied,
            after: ProjectionDeltaVisibility::Denied,
        })
    {
        return Ok(());
    }
    if transition.before == ProjectionDeltaVisibility::Unknown
        || transition.after == ProjectionDeltaVisibility::Unknown
    {
        return if let Some(model) = authorization.model(relationship.source_model()) {
            emit_model_recovery(
                occurrence_ordinal,
                projection_ref,
                partition,
                model.wire_model,
                operations,
                recoveries,
            )
        } else {
            Ok(())
        };
    }
    emit_relationship_recovery(
        occurrence_ordinal,
        projection_ref,
        partition,
        relationship.source_model(),
        mapped,
        source,
        authorization,
        operations,
        recoveries,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "narrow relationship fallback keeps all safe authority pieces explicit"
)]
fn emit_relationship_recovery(
    occurrence_ordinal: u32,
    projection_ref: u32,
    partition: Option<ProjectionDeltaPartition>,
    source_logical_model: &str,
    relationship: super::authorization::AuthorizedRelationship,
    source: Option<ProjectionDeltaScope>,
    authorization: &impl ProjectionAuthorization,
    operations: &mut Vec<ProjectionDeltaOperation>,
    recoveries: &mut Vec<ProjectionDeltaRecovery>,
) -> Result<(), ProjectionDeltaError> {
    if let Some(source) = source {
        push_operation(
            operations,
            ProjectionDeltaOperation {
                occurrence_ordinal,
                projection_refs: vec![projection_ref],
                mutation: ProjectionDeltaMutation::InvalidateRelationship {
                    relationship: relationship.wire_relationship.clone(),
                    source: source.clone(),
                },
            },
        )?;
        push_recovery(
            recoveries,
            recovery(
                occurrence_ordinal,
                projection_ref,
                ProjectionDeltaRecoveryTarget::Relationship {
                    relationship: relationship.wire_relationship,
                    source,
                },
            ),
        )
    } else if let Some(model) = authorization.model(source_logical_model) {
        emit_model_recovery(
            occurrence_ordinal,
            projection_ref,
            partition,
            model.wire_model,
            operations,
            recoveries,
        )
    } else {
        Ok(())
    }
}

fn emit_model_recovery(
    occurrence_ordinal: u32,
    projection_ref: u32,
    partition: Option<ProjectionDeltaPartition>,
    model: String,
    operations: &mut Vec<ProjectionDeltaOperation>,
    recoveries: &mut Vec<ProjectionDeltaRecovery>,
) -> Result<(), ProjectionDeltaError> {
    push_operation(
        operations,
        ProjectionDeltaOperation {
            occurrence_ordinal,
            projection_refs: vec![projection_ref],
            mutation: ProjectionDeltaMutation::InvalidateModel {
                partition: partition.clone(),
                model: model.clone(),
            },
        },
    )?;
    push_recovery(
        recoveries,
        recovery(
            occurrence_ordinal,
            projection_ref,
            ProjectionDeltaRecoveryTarget::Model { partition, model },
        ),
    )
}

fn authorized_scope(
    partition: Option<ProjectionDeltaPartition>,
    logical_model: &str,
    key: &ResolvedProjectionKey,
    authorization: &impl ProjectionAuthorization,
) -> Result<Option<ProjectionDeltaScope>, ProjectionDeltaError> {
    let key = authorization.record_key(logical_model, key)?;
    Ok(match (partition, key) {
        (Some(partition), Some(key)) => Some(ProjectionDeltaScope {
            partition,
            model: key.wire_model,
            key: key.fields,
        }),
        _ => None,
    })
}

fn push_operation(
    operations: &mut Vec<ProjectionDeltaOperation>,
    operation: ProjectionDeltaOperation,
) -> Result<(), ProjectionDeltaError> {
    if operations.len() == MAX_PROJECTION_DELTA_OPERATIONS {
        return Err(ProjectionDeltaError::TooManyOperations {
            len: operations.len() + 1,
            max: MAX_PROJECTION_DELTA_OPERATIONS,
        });
    }
    operations.push(operation);
    Ok(())
}

fn push_recovery(
    recoveries: &mut Vec<ProjectionDeltaRecovery>,
    recovery: ProjectionDeltaRecovery,
) -> Result<(), ProjectionDeltaError> {
    if recoveries.len() == MAX_PROJECTION_DELTA_OPERATIONS {
        return Err(ProjectionDeltaError::TooManyRecoveries {
            len: recoveries.len() + 1,
            max: MAX_PROJECTION_DELTA_OPERATIONS,
        });
    }
    recoveries.push(recovery);
    Ok(())
}

fn enforce_raw_bounds(
    operations: &[ProjectionDeltaOperation],
    recoveries: &[ProjectionDeltaRecovery],
) -> Result<(), ProjectionDeltaError> {
    if operations.len() > MAX_PROJECTION_DELTA_OPERATIONS {
        return Err(ProjectionDeltaError::TooManyOperations {
            len: operations.len(),
            max: MAX_PROJECTION_DELTA_OPERATIONS,
        });
    }
    if recoveries.len() > MAX_PROJECTION_DELTA_OPERATIONS {
        return Err(ProjectionDeltaError::TooManyRecoveries {
            len: recoveries.len(),
            max: MAX_PROJECTION_DELTA_OPERATIONS,
        });
    }
    Ok(())
}

fn recovery(
    occurrence_ordinal: u32,
    projection_ref: u32,
    target: ProjectionDeltaRecoveryTarget,
) -> ProjectionDeltaRecovery {
    recovery_with_condition(
        occurrence_ordinal,
        projection_ref,
        ProjectionDeltaRecoveryCondition::Always,
        target,
    )
}

fn recovery_with_condition(
    occurrence_ordinal: u32,
    projection_ref: u32,
    condition: ProjectionDeltaRecoveryCondition,
    target: ProjectionDeltaRecoveryTarget,
) -> ProjectionDeltaRecovery {
    ProjectionDeltaRecovery {
        occurrence_ordinal,
        projection_refs: vec![projection_ref],
        condition,
        target,
    }
}
