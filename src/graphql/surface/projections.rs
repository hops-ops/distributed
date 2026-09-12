use std::collections::{BTreeMap, BTreeSet};

use crate::projection::catalog::{ActiveProjectionBindings, ProjectionCatalog, ProjectionEpoch};
use crate::projection::placement::{
    ProjectionBinding, ProjectionBindingId, ProjectionBindingState, ProjectionExecutionClass,
    ProjectionExecutorRoute, ProjectionPlacement,
};
use crate::{
    ProjectionField, ProjectionInvalidation, ProjectionKeyField, ProjectionMutationKind,
    ProjectionOperation, ProjectionPartition, ProjectionProgram, ProjectionProgramId,
    ProjectionRelationshipEffect,
};

use super::types::{SurfaceProjectionOwner, SurfaceProjectionOwnerKind};
use super::{SurfaceModel, SurfaceRelationshipKeys};

/// One role-safe selected operation from an authoritative projection program.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) struct SurfaceProjectionOperation {
    pub operation_id: String,
    pub staging_ordinal: u32,
    pub kind: ProjectionMutationKind,
    pub model: String,
    pub storage: String,
    pub key: Vec<ProjectionKeyField>,
    pub fields: Vec<ProjectionField>,
    pub relationship_effects: Vec<ProjectionRelationshipEffect>,
    pub invalidations: Vec<ProjectionInvalidation>,
    pub force_revalidate: bool,
}

/// One exact selected event arm. Its selector is server-only; client export
/// receives only a digest-derived event reference.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) struct SurfaceProjectionArm {
    pub arm_id: String,
    pub selector: crate::ProjectionEventSelector,
    pub operations: Vec<SurfaceProjectionOperation>,
}

/// Role-safe program inventory retained after Surface selection.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) struct SurfaceSelectedProjectionProgram {
    pub name: String,
    pub version: u64,
    pub ir_version: u16,
    pub operation_semantics_version: u16,
    pub partition: ProjectionPartition,
    pub arms: Vec<SurfaceProjectionArm>,
}

/// One exact modeled projection registration carried through Surface
/// authorization.
///
/// On the catalog Surface the private raw tuple is present for validation.
/// Role/application selection replaces it with a field-filtered descriptor and
/// drops the raw program, binding schemas, event body paths for denied fields,
/// executor route, and physical topology.
#[derive(Clone)]
pub struct SurfaceModeledProjection {
    program_id: ProjectionProgramId,
    binding_id: ProjectionBindingId,
    owner: String,
    placement: ProjectionPlacement,
    execution_class: ProjectionExecutionClass,
    state: ProjectionBindingState,
    epoch: ProjectionEpoch,
    route: ProjectionExecutorRoute,
    output_models: Vec<String>,
    raw_program: Option<ProjectionProgram>,
    raw_binding: Option<ProjectionBinding>,
    server_executor: Option<crate::projection::lower::ProjectionServerExecutorDescriptor>,
    selected: Option<SurfaceSelectedProjectionProgram>,
}

impl PartialEq for SurfaceModeledProjection {
    fn eq(&self, other: &Self) -> bool {
        self.program_id == other.program_id
            && self.binding_id == other.binding_id
            && self.owner == other.owner
            && self.placement == other.placement
            && self.execution_class == other.execution_class
            && self.state == other.state
            && self.epoch == other.epoch
            && self.route == other.route
            && self.output_models == other.output_models
            && self.raw_program == other.raw_program
            && self.raw_binding == other.raw_binding
            && server_executor_eq(
                self.server_executor.as_ref(),
                other.server_executor.as_ref(),
            )
            && self.selected == other.selected
    }
}

impl Eq for SurfaceModeledProjection {}

impl std::fmt::Debug for SurfaceModeledProjection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SurfaceModeledProjection")
            .field("program_id", &self.program_id)
            .field("binding_id", &self.binding_id)
            .field("placement", &self.placement)
            .field("execution_class", &self.execution_class)
            .field("state", &self.state)
            .field("epoch", &self.epoch.as_str())
            .field("output_models", &self.output_models)
            .finish_non_exhaustive()
    }
}

impl SurfaceModeledProjection {
    /// Return the complete portable behavior contract for this selected
    /// registration. Executor routes, server executor closures, and physical
    /// topology are intentionally excluded; the program IR and binding
    /// compatibility fields remain canonical identity material.
    pub(crate) fn canonical_contract_value(&self) -> Result<serde_json::Value, String> {
        let program = if let Some(raw_program) = &self.raw_program {
            serde_json::to_value(raw_program).map_err(|error| error.to_string())?
        } else if let Some(selected) = &self.selected {
            serde_json::to_value(selected).map_err(|error| error.to_string())?
        } else {
            return Err("modeled projection has no canonical program material".to_owned());
        };
        let program = crate::application::compact_projection_program_contract(program)
            .map_err(|error| error.to_string())?;
        let binding = self.raw_binding.as_ref().map(|binding| {
            serde_json::json!({
                "identity_version": binding.identity_version(),
                "program_ir_version": binding.program_ir_version(),
                "operation_semantics_version": binding.operation_semantics_version(),
                "program_id": binding.program_id().to_string(),
                "events": binding.events(),
                "source": binding.source(),
                "owner": binding.owner(),
                "placement": binding.placement(),
                "execution_class": binding.execution_class(),
                "partition": binding.partition(),
                "outputs": binding.outputs(),
                "relationships": binding.relationships(),
            })
        });
        Ok(serde_json::json!({
            "program_id": self.program_id.to_string(),
            "binding_id": self.binding_id.to_string(),
            "owner": self.owner,
            "placement": self.placement,
            "execution_class": self.execution_class,
            "state": self.state,
            "epoch": self.epoch.as_str(),
            "output_models": self.output_models,
            "program": program,
            "binding": binding,
        }))
    }

    #[cfg(test)]
    pub(crate) fn selected_for_client_manifest_test(
        program_id: ProjectionProgramId,
        binding_id: ProjectionBindingId,
        placement: ProjectionPlacement,
        execution_class: ProjectionExecutionClass,
        state: ProjectionBindingState,
        output_models: Vec<String>,
        selected: Option<SurfaceSelectedProjectionProgram>,
    ) -> Self {
        Self {
            program_id,
            binding_id,
            owner: "client-manifest-test".into(),
            placement,
            execution_class,
            state,
            epoch: ProjectionEpoch::new("client-manifest-test-v1")
                .expect("test epoch is canonical"),
            route: ProjectionExecutorRoute::local("client-manifest-test")
                .expect("test route is canonical"),
            output_models,
            raw_program: None,
            raw_binding: None,
            server_executor: None,
            selected,
        }
    }

    /// Resolve one exact registration through a validated catalog and active
    /// binding view.
    ///
    /// This is the only public authority path. Arbitrary
    /// `ProjectionBindingActivation::new(...)` values cannot bypass catalog
    /// writer-conflict, route, placement, or epoch-takeover validation.
    pub fn try_from_catalog(
        program: ProjectionProgram,
        catalog: &ProjectionCatalog,
        active: &ActiveProjectionBindings,
        binding_id: ProjectionBindingId,
    ) -> Result<Self, String> {
        let active_bytes = active
            .canonical_bytes()
            .map_err(|error| error.to_string())?;
        let validated = ActiveProjectionBindings::from_canonical_bytes(catalog, &active_bytes)
            .map_err(|error| error.to_string())?;
        let binding = catalog
            .binding(binding_id)
            .ok_or_else(|| format!("unknown modeled projection binding `{binding_id}`"))?
            .clone();
        let activation = validated
            .bindings()
            .iter()
            .find(|activation| activation.binding_id() == binding_id)
            .ok_or_else(|| format!("modeled projection binding `{binding_id}` is not live"))?;
        let route = activation
            .route()
            .cloned()
            .ok_or_else(|| format!("modeled projection binding `{binding_id}` has no route"))?;
        let program_id = program.id().map_err(|error| error.to_string())?;
        if binding.program_id() != program_id || activation.program_id() != program_id {
            return Err("modeled projection binding does not match its program digest".into());
        }
        let output_models = binding
            .outputs()
            .iter()
            .map(|output| output.model().to_owned())
            .collect();
        Ok(Self {
            program_id,
            binding_id,
            owner: binding.owner().name().to_owned(),
            placement: binding.placement(),
            execution_class: binding.execution_class(),
            state: activation.state(),
            epoch: activation.epoch().clone(),
            route,
            output_models,
            raw_program: Some(program),
            raw_binding: Some(binding),
            server_executor: None,
            selected: None,
        })
    }

    /// Resolve one generated portable executor through the exact catalog
    /// binding that will host it.
    ///
    /// This is the mountable form used by the projection service runtime.
    /// Program digest, output schemas, route, and epoch are all validated
    /// before a transport subscription can be planned.
    pub fn try_from_descriptor<D>(
        descriptor: crate::projection::lower::ProjectionDescriptor<D>,
        catalog: &ProjectionCatalog,
        active: &ActiveProjectionBindings,
        binding_id: ProjectionBindingId,
    ) -> Result<Self, String> {
        let program = descriptor.program().map_err(|error| error.to_string())?;
        let executor = descriptor
            .server_executor()
            .map_err(|error| error.to_string())?;
        let mut modeled = Self::try_from_catalog(program, catalog, active, binding_id)?;
        let (_, binding) = modeled
            .raw()
            .ok_or_else(|| "mountable modeled projection lost its raw binding".to_owned())?;
        if executor.program_id != modeled.program_id
            || executor.epoch != modeled.epoch.as_str()
            || executor.outputs.models.len() != binding.outputs().len()
            || executor.outputs.models.iter().any(|output| {
                !binding.outputs().iter().any(|bound| {
                    bound.model() == output.model
                        && bound.storage() == output.storage
                        && bound.schema() == &output.schema
                })
            })
        {
            return Err(
                "generated projection executor differs from its exact binding digest, epoch, or outputs"
                    .into(),
            );
        }
        modeled.server_executor = Some(executor);
        Ok(modeled)
    }

    /// Return the semantic program identity.
    pub fn program_id(&self) -> ProjectionProgramId {
        self.program_id
    }

    /// Return the exact deployment binding identity.
    pub fn binding_id(&self) -> ProjectionBindingId {
        self.binding_id
    }

    /// Return active or draining state.
    pub fn state(&self) -> ProjectionBindingState {
        self.state
    }

    /// Return the exact physical incarnation.
    pub fn epoch(&self) -> &ProjectionEpoch {
        &self.epoch
    }

    /// Return the exact validated executor route.
    pub fn route(&self) -> &ProjectionExecutorRoute {
        &self.route
    }

    /// Whether this exact selected registration may mint **async causal work**
    /// (confirmations / obligations waiting on eventual projectors).
    ///
    /// Direct / same-transaction projected rows are not in this set: the command
    /// response already carries the authoritative row. Previews for Direct still
    /// use [`Self::is_preview_eligible`].
    pub fn is_causally_eligible(&self) -> bool {
        self.state == ProjectionBindingState::Active
            && self.placement == ProjectionPlacement::Eventual
            && self.execution_class == ProjectionExecutionClass::Causal
    }

    /// Whether this registration may contribute **client cache previews** from
    /// `.applies` / event→mutation IR.
    ///
    /// Same mutation IR as the server; apply site differs (command handler
    /// Projected vs eventual event handler). Background-only consumers are
    /// excluded. Direct and Eventual causal placements both qualify when Active
    /// and a selected program is present for composition.
    pub fn is_preview_eligible(&self) -> bool {
        self.state == ProjectionBindingState::Active
            && self.execution_class == ProjectionExecutionClass::Causal
            && self.selected_program().is_some()
    }

    /// Whether this exact live registration may validate causal work minted
    /// before or during a rollout.
    #[cfg(feature = "graphql")]
    pub(crate) fn is_causal_evidence_eligible(&self) -> bool {
        matches!(
            self.state,
            ProjectionBindingState::Active | ProjectionBindingState::Draining
        ) && self.placement == ProjectionPlacement::Eventual
            && self.execution_class == ProjectionExecutionClass::Causal
    }

    pub(crate) fn placement(&self) -> ProjectionPlacement {
        self.placement
    }

    pub(crate) fn execution_class(&self) -> ProjectionExecutionClass {
        self.execution_class
    }

    pub(crate) fn output_models(&self) -> &[String] {
        &self.output_models
    }

    pub(crate) fn event_names(&self) -> Vec<String> {
        self.raw_program
            .as_ref()
            .map(|program| {
                program
                    .arms()
                    .iter()
                    .map(|arm| arm.selector().event_name().to_owned())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect()
            })
            .or_else(|| {
                self.selected.as_ref().map(|program| {
                    program
                        .arms
                        .iter()
                        .map(|arm| arm.selector.event_name().to_owned())
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect()
                })
            })
            .unwrap_or_default()
    }

    pub(crate) fn selected_program(&self) -> Option<&SurfaceSelectedProjectionProgram> {
        self.selected.as_ref()
    }

    pub(super) fn validate_for_surface(
        &self,
        owner_name: &str,
        kind: SurfaceProjectionOwnerKind,
        models: &BTreeMap<String, SurfaceModel>,
    ) -> Result<(), String> {
        let (program, binding) = self.raw().ok_or_else(|| {
            "selected modeled projection cannot be reattached to a catalog Surface".to_owned()
        })?;
        if self.owner != owner_name {
            return Err(format!(
                "modeled projection owner `{owner_name}` differs from binding owner `{}`",
                self.owner
            ));
        }
        let expected_placement = match kind {
            SurfaceProjectionOwnerKind::Async => ProjectionPlacement::Eventual,
            SurfaceProjectionOwnerKind::Direct => ProjectionPlacement::Direct,
        };
        if binding.placement() != expected_placement {
            return Err(format!(
                "modeled projection owner `{owner_name}` has incompatible {:?} placement",
                binding.placement()
            ));
        }
        let output_models = binding
            .outputs()
            .iter()
            .map(|output| output.model())
            .collect::<BTreeSet<_>>();
        for output in binding.outputs() {
            let Some(model) = models.get(output.model()) else {
                return Err(format!(
                    "modeled projection owner `{owner_name}` targets unknown surface model `{}`",
                    output.model()
                ));
            };
            if output.storage() != model.table_name
                || !output.schema().has_same_storage_contract(&model.schema)
            {
                return Err(format!(
                    "modeled projection owner `{owner_name}` output `{}` does not match the authoritative Surface schema",
                    output.model()
                ));
            }
        }
        for arm in program.arms() {
            for operation in arm.operations() {
                if !output_models.contains(operation.target().model()) {
                    return Err(format!(
                        "modeled projection owner `{owner_name}` operation `{}` targets an undeclared binding output",
                        operation.operation_id()
                    ));
                }
            }
        }
        if kind == SurfaceProjectionOwnerKind::Direct {
            validate_direct_program(owner_name, program, binding, models)?;
        }
        Ok(())
    }

    pub(super) fn select_for_models(
        &self,
        models: &BTreeMap<String, SurfaceModel>,
    ) -> Result<Option<Self>, String> {
        let (program, _) = self
            .raw()
            .ok_or_else(|| "modeled projection was already Surface-selected".to_owned())?;
        let output_models = self
            .output_models
            .iter()
            .filter(|model| models.contains_key(*model))
            .cloned()
            .collect::<Vec<_>>();
        if output_models.is_empty() {
            return Ok(None);
        }
        // Direct and Eventual both export selected arms: client previews compose
        // the same portable mutation IR. Server apply site differs (handler-owned
        // same-tx Projected vs eventual event handler).
        let mut arms = Vec::new();
        for arm in program.arms() {
            let operations = arm
                .operations()
                .iter()
                .filter_map(|operation| select_operation(operation, models))
                .collect::<Vec<_>>();
            if !operations.is_empty() {
                arms.push(SurfaceProjectionArm {
                    arm_id: arm.arm_id().to_owned(),
                    selector: arm.selector().clone(),
                    operations,
                });
            }
        }
        let selected = if arms.is_empty() {
            None
        } else {
            Some(SurfaceSelectedProjectionProgram {
                name: program.name().to_owned(),
                version: program.version(),
                ir_version: program.ir_version(),
                operation_semantics_version: program.operation_semantics_version(),
                partition: program.partition().clone(),
                arms,
            })
        };
        Ok(Some(Self {
            output_models,
            raw_program: None,
            raw_binding: None,
            server_executor: None,
            selected,
            ..self.clone()
        }))
    }

    pub(crate) fn raw(&self) -> Option<(&ProjectionProgram, &ProjectionBinding)> {
        Some((self.raw_program.as_ref()?, self.raw_binding.as_ref()?))
    }

    #[cfg(feature = "graphql")]
    pub(crate) fn server_executor(
        &self,
    ) -> Option<&crate::projection::lower::ProjectionServerExecutorDescriptor> {
        self.server_executor.as_ref()
    }
}

/// Validate the one active physical contract represented by each direct
/// modeled owner before any command inventory is bound to it.
///
/// Draining registrations remain visible for rollout completion but do not
/// participate in the contract used to mint new same-transaction work.
pub(crate) fn validate_direct_modeled_owner_compatibility(
    owners: &[SurfaceProjectionOwner],
) -> Result<(), String> {
    for owner in owners {
        if owner.kind != SurfaceProjectionOwnerKind::Direct || owner.modeled.is_empty() {
            continue;
        }
        let active = owner
            .modeled
            .iter()
            .filter(|modeled| modeled.state() == ProjectionBindingState::Active)
            .collect::<Vec<_>>();
        let active_epochs = active
            .iter()
            .map(|modeled| modeled.epoch().as_str())
            .collect::<BTreeSet<_>>();
        if active_epochs.len() > 1 {
            return Err(format!(
                "direct modeled projection owner `{}` has mixed active change epochs: {}",
                owner.name,
                active_epochs
                    .into_iter()
                    .map(|epoch| format!("`{epoch}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        let Some(first) = active.first() else {
            continue;
        };
        let (_, first_binding) = first.raw().ok_or_else(|| {
            "selected modeled projection cannot be reattached to a catalog Surface".to_owned()
        })?;
        let mut binding_owners = BTreeSet::new();
        for modeled in &active {
            let (_, binding) = modeled.raw().ok_or_else(|| {
                "selected modeled projection cannot be reattached to a catalog Surface".to_owned()
            })?;
            binding_owners.insert(binding.owner().name());
            if binding.placement() != ProjectionPlacement::Direct {
                return Err(format!(
                    "direct modeled projection owner `{}` has incompatible {:?} placement",
                    owner.name,
                    binding.placement()
                ));
            }
            if binding.physical_topology() != first_binding.physical_topology() {
                return Err(format!(
                    "direct modeled projection owner `{}` has incompatible active physical topologies",
                    owner.name
                ));
            }
            if binding.partition() != first_binding.partition() {
                return Err(format!(
                    "direct modeled projection owner `{}` has incompatible active partition protocols",
                    owner.name
                ));
            }
            if modeled.route() != first.route() {
                return Err(format!(
                    "direct modeled projection owner `{}` has incompatible active executor routes",
                    owner.name
                ));
            }
        }
        if binding_owners != BTreeSet::from([owner.name.as_str()]) {
            return Err(format!(
                "direct modeled projection owner `{}` has incompatible active binding owners: {}",
                owner.name,
                binding_owners
                    .into_iter()
                    .map(|binding_owner| format!("`{binding_owner}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    }
    Ok(())
}

pub(crate) fn modeled_owner_partition_contract(
    owner: &SurfaceProjectionOwner,
) -> Result<crate::projection_protocol::ProjectionPartitionSpec, String> {
    let active = owner
        .modeled
        .iter()
        .filter(|modeled| modeled.state() == ProjectionBindingState::Active)
        .collect::<Vec<_>>();
    let Some(first) = active.first() else {
        return Ok(crate::projection_protocol::ProjectionPartitionSpec::modeled_inactive());
    };
    let (first_program, first_binding) = first.raw().ok_or_else(|| {
        "selected modeled projection cannot be reattached to a catalog Surface".to_owned()
    })?;
    validate_program_partition_binding(&owner.name, first_program, first_binding)?;
    for modeled in &active {
        let (program, binding) = modeled.raw().ok_or_else(|| {
            "selected modeled projection cannot be reattached to a catalog Surface".to_owned()
        })?;
        validate_program_partition_binding(&owner.name, program, binding)?;
        if binding.partition() != first_binding.partition()
            || program.partition() != first_program.partition()
        {
            return Err(format!(
                "modeled projection owner `{}` has incompatible active partition contracts",
                owner.name
            ));
        }
    }
    match first_program.partition() {
        ProjectionPartition::Unit => {
            Ok(crate::projection_protocol::ProjectionPartitionSpec::unit())
        }
        ProjectionPartition::Expression(_) => {
            crate::projection_protocol::ProjectionPartitionSpec::modeled_expression(
                first_binding.partition().expression().clone(),
                first_binding.partition().codec(),
                first_binding.partition().codec_version(),
            )
            .map_err(|error| {
                format!(
                    "modeled projection owner `{}` has invalid active partition contract: {error}",
                    owner.name
                )
            })
        }
    }
}

pub(crate) fn compile_projection_owner_topology<'a>(
    owner: &SurfaceProjectionOwner,
    schemas: impl IntoIterator<Item = &'a crate::table::TableSchema>,
) -> Result<
    (
        crate::projection_protocol::ProjectorTopologyId,
        Vec<crate::projection_protocol::ProjectionModelOwnership>,
    ),
    String,
> {
    let schemas = schemas.into_iter().collect::<Vec<_>>();
    if owner.modeled.is_empty() {
        return crate::projection_protocol::compile_projection_topology(
            &owner.name,
            &owner.facts,
            &owner.models,
            &owner.partition,
            schemas,
        )
        .map_err(|error| error.to_string());
    }

    let live = owner
        .modeled
        .iter()
        .filter(|modeled| modeled.state() == ProjectionBindingState::Active)
        .collect::<Vec<_>>();
    let registrations = if live.is_empty() {
        owner.modeled.iter().collect::<Vec<_>>()
    } else {
        live
    };
    let first = registrations.first().ok_or_else(|| {
        format!(
            "modeled projection owner `{}` has no catalog registrations",
            owner.name
        )
    })?;
    let (_, first_binding) = first.raw().ok_or_else(|| {
        "selected modeled projection cannot be reattached to a catalog Surface".to_owned()
    })?;
    let first_physical = first_binding.physical_topology().ok_or_else(|| {
        format!(
            "modeled projection owner `{}` binding `{}` has no physical observation topology",
            owner.name,
            first.binding_id()
        )
    })?;
    let topology = crate::projection_protocol::ProjectorTopologyId::new(
        first_physical.version(),
        first_physical.name(),
        first_physical.digest(),
    )
    .map_err(|error| error.to_string())?;
    let mut active_models = BTreeSet::new();
    for modeled in registrations {
        let (_, binding) = modeled.raw().ok_or_else(|| {
            "selected modeled projection cannot be reattached to a catalog Surface".to_owned()
        })?;
        if binding.physical_topology() != Some(first_physical) {
            return Err(format!(
                "modeled projection owner `{}` has incompatible live physical topologies",
                owner.name
            ));
        }
        active_models.extend(modeled.output_models().iter().cloned());
    }
    let schemas = schemas
        .into_iter()
        .filter(|schema| active_models.contains(&schema.model_name))
        .map(|schema| (schema.model_name.clone(), schema))
        .collect::<BTreeMap<_, _>>();
    if schemas.len() != active_models.len() {
        return Err(format!(
            "modeled projection owner `{}` does not have authoritative schemas for every live output",
            owner.name
        ));
    }
    let mut tables = BTreeSet::new();
    let mut ownership = Vec::with_capacity(schemas.len());
    for (model, schema) in schemas {
        if !tables.insert(schema.table_name.as_str()) {
            return Err(format!(
                "modeled projection owner `{}` assigns more than one live model to physical table `{}`",
                owner.name, schema.table_name
            ));
        }
        ownership.push(
            crate::projection_protocol::ProjectionModelOwnership::new(
                model.clone(),
                schema.table_name.clone(),
            )
            .map_err(|error| error.to_string())?,
        );
    }
    Ok((topology, ownership))
}

fn validate_program_partition_binding(
    owner: &str,
    program: &ProjectionProgram,
    binding: &ProjectionBinding,
) -> Result<(), String> {
    let expected = serde_json::to_value(program.partition()).map_err(|error| {
        format!("modeled projection owner `{owner}` cannot encode its program partition: {error}")
    })?;
    if binding.partition().expression() != &expected {
        return Err(format!(
            "modeled projection owner `{owner}` binding `{}` partition differs from its program",
            binding.id()
        ));
    }
    Ok(())
}

fn server_executor_eq(
    left: Option<&crate::projection::lower::ProjectionServerExecutorDescriptor>,
    right: Option<&crate::projection::lower::ProjectionServerExecutorDescriptor>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => {
            left.name == right.name
                && left.version == right.version
                && left.epoch == right.epoch
                && left.program_id == right.program_id
                && left.outputs == right.outputs
        }
        (None, Some(_)) | (Some(_), None) => false,
    }
}

fn select_operation(
    operation: &ProjectionOperation,
    models: &BTreeMap<String, SurfaceModel>,
) -> Option<SurfaceProjectionOperation> {
    let model = models.get(operation.target().model())?;
    if operation.target().storage() != model.table_name {
        return None;
    }
    let key_visible = operation
        .key()
        .iter()
        .all(|key| logical_field_visible(model, key.name()));
    let fields = operation
        .fields()
        .iter()
        .filter(|field| logical_field_visible(model, field.name()))
        .cloned()
        .collect::<Vec<_>>();
    let mut relationship_effects = Vec::new();
    let mut relationship_recovery = Vec::new();
    for effect in operation.relationship_effects() {
        if relationship_visible(&effect, models) {
            relationship_effects.push(effect.clone());
            continue;
        }
        if relationship_surface_visible(effect, models) {
            let relationship = effect.relationship();
            let source = models
                .get(relationship.source_model())
                .expect("visible relationship source model");
            if !effect.source_key().is_empty()
                && effect
                    .source_key()
                    .iter()
                    .all(|key| logical_field_visible(source, key.name()))
            {
                relationship_effects.push(
                    ProjectionRelationshipEffect::invalidate(
                        effect.ordinal(),
                        relationship.clone(),
                        effect.source_key().to_vec(),
                    )
                    .expect("selected source key remains complete"),
                );
                relationship_recovery.push(
                    ProjectionInvalidation::relationship(
                        relationship.source_model(),
                        relationship.relationship(),
                        relationship.target_model(),
                    )
                    .expect("selected relationship identity is non-empty"),
                );
            } else {
                relationship_recovery.push(
                    ProjectionInvalidation::model(relationship.source_model())
                        .expect("selected relationship source identity is non-empty"),
                );
            }
        }
    }
    let mut invalidations = operation
        .invalidations()
        .iter()
        .filter(|invalidation| invalidation_visible(invalidation, models))
        .cloned()
        .collect::<Vec<_>>();
    invalidations.extend(relationship_recovery);
    invalidations.sort();
    invalidations.dedup();
    let row_consequence = operation.kind() == ProjectionMutationKind::Delete || !fields.is_empty();
    let hidden_only_row_change = operation.kind() != ProjectionMutationKind::Delete
        && !operation.fields().is_empty()
        && fields.is_empty();
    let force_revalidate = (row_consequence && !key_visible) || hidden_only_row_change;
    if force_revalidate {
        invalidations.push(
            ProjectionInvalidation::model(&model.model_name)
                .expect("selected model identity is non-empty"),
        );
        invalidations.sort();
        invalidations.dedup();
    }
    Some(SurfaceProjectionOperation {
        operation_id: operation.operation_id().to_owned(),
        staging_ordinal: operation.staging_ordinal(),
        kind: operation.kind(),
        model: operation.target().model().to_owned(),
        storage: operation.target().storage().to_owned(),
        key: if key_visible {
            operation.key().to_vec()
        } else {
            Vec::new()
        },
        fields: if force_revalidate { Vec::new() } else { fields },
        relationship_effects,
        invalidations,
        force_revalidate,
    })
}

fn logical_field_visible(model: &SurfaceModel, logical: &str) -> bool {
    model
        .schema
        .columns
        .iter()
        .find(|column| !column.skipped && column.field_name == logical)
        .is_some_and(|column| {
            model
                .columns
                .iter()
                .any(|selected| selected.name == column.column_name)
        })
}

fn relationship_visible(
    effect: &&ProjectionRelationshipEffect,
    models: &BTreeMap<String, SurfaceModel>,
) -> bool {
    let relationship = effect.relationship();
    let Some(source) = models.get(relationship.source_model()) else {
        return false;
    };
    let Some(target) = models.get(relationship.target_model()) else {
        return false;
    };
    let Some(selected) = source
        .relationships
        .iter()
        .find(|selected| selected.name == relationship.relationship())
    else {
        return false;
    };
    if selected.target_model != target.model_name
        || matches!(selected.keys, SurfaceRelationshipKeys::Embedded)
    {
        return false;
    }
    effect
        .source_key()
        .iter()
        .all(|key| logical_field_visible(source, key.name()))
        && effect
            .target_key()
            .iter()
            .all(|key| logical_field_visible(target, key.name()))
}

fn relationship_surface_visible(
    effect: &ProjectionRelationshipEffect,
    models: &BTreeMap<String, SurfaceModel>,
) -> bool {
    let relationship = effect.relationship();
    models
        .get(relationship.source_model())
        .is_some_and(|source| {
            source.relationships.iter().any(|selected| {
                selected.name == relationship.relationship()
                    && selected.target_model == relationship.target_model()
                    && models.contains_key(relationship.target_model())
            })
        })
}

fn invalidation_visible(
    invalidation: &&ProjectionInvalidation,
    models: &BTreeMap<String, SurfaceModel>,
) -> bool {
    match invalidation {
        ProjectionInvalidation::Model { model } => models.contains_key(model),
        ProjectionInvalidation::Relationship {
            source_model,
            relationship,
            target_model,
        } => models.get(source_model).is_some_and(|source| {
            source.relationships.iter().any(|selected| {
                selected.name == *relationship && selected.target_model == *target_model
            })
        }),
    }
}

fn validate_direct_program(
    owner: &str,
    program: &ProjectionProgram,
    binding: &ProjectionBinding,
    models: &BTreeMap<String, SurfaceModel>,
) -> Result<(), String> {
    let [output] = binding.outputs() else {
        return Err(format!(
            "direct modeled projection `{owner}` must own exactly one output model"
        ));
    };
    let model = models
        .get(output.model())
        .expect("modeled output presence was validated above");
    let complete_fields = model
        .schema
        .columns
        .iter()
        .filter(|column| !column.skipped)
        .map(|column| column.field_name.as_str())
        .collect::<BTreeSet<_>>();
    let logical_primary_key = model
        .schema
        .primary_key
        .columns
        .iter()
        .map(|physical| {
            model
                .schema
                .columns
                .iter()
                .find(|column| !column.skipped && column.column_name == *physical)
                .map(|column| column.field_name.as_str())
                .ok_or_else(|| {
                    format!(
                        "direct modeled projection `{owner}` cannot map physical primary key `{physical}` to one logical field"
                    )
                })
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    for arm in program.arms() {
        let [operation] = arm.operations() else {
            return Err(format!(
                "direct modeled projection `{owner}` arm `{}` must contain exactly one operation",
                arm.arm_id()
            ));
        };
        let operation_fields = operation
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<BTreeSet<_>>();
        let operation_key = operation
            .key()
            .iter()
            .map(|field| field.name())
            .collect::<BTreeSet<_>>();
        if operation.kind() != ProjectionMutationKind::Upsert
            || operation.target().model() != output.model()
            || operation.target().storage() != output.storage()
            || operation_fields != complete_fields
            || operation_key != logical_primary_key
        {
            return Err(format!(
                "direct modeled projection `{owner}` arm `{}` is not one complete logical full-row upsert",
                arm.arm_id()
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graphql::{build_surface, surface_for_role, RoleGrant, SurfaceOptions};
    use crate::table::{
        ColumnType, PrimaryKey, RelationshipDef, RelationshipKind, TableColumn, TableKind,
        TableSchema,
    };
    use crate::{
        ProjectionAssignment, ProjectionExpression, ProjectionRelationship, ProjectionValue,
    };

    fn schema(
        model: &str,
        table: &str,
        fields: &[&str],
        relationships: Vec<RelationshipDef>,
    ) -> TableSchema {
        TableSchema {
            model_name: model.into(),
            table_name: table.into(),
            columns: fields
                .iter()
                .enumerate()
                .map(|(index, field)| TableColumn {
                    primary_key: index == 0,
                    ..TableColumn::new(*field, *field, ColumnType::Text)
                })
                .collect(),
            primary_key: PrimaryKey::new([fields[0]]),
            version_column: None,
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            relationships,
            kind: TableKind::ReadModel,
        }
    }

    fn models(grants: BTreeMap<String, RoleGrant>) -> BTreeMap<String, SurfaceModel> {
        let todo = schema(
            "TodoView",
            "todos",
            &["todo_id", "owner_id", "title"],
            vec![RelationshipDef {
                references: None,
                field_name: "owner".into(),
                kind: RelationshipKind::BelongsTo,
                target_model: "UserView".into(),
                foreign_key: Some("owner_id".into()),
                through: None,
                target_foreign_key: None,
            }],
        );
        let user = schema("UserView", "users", &["user_id", "name"], Vec::new());
        surface_for_role(
            &build_surface(&[todo, user], &SurfaceOptions::sqlite()).unwrap(),
            "user",
            &grants,
        )
        .unwrap()
        .models
    }

    fn key(ordinal: u32, name: &str) -> ProjectionKeyField {
        ProjectionKeyField::try_new(
            ordinal,
            name,
            ProjectionExpression::constant(ProjectionValue::string("id")),
        )
        .unwrap()
    }

    fn operation(
        field: &str,
        relationship_effects: Vec<ProjectionRelationshipEffect>,
    ) -> ProjectionOperation {
        ProjectionOperation::try_new(
            "patch-and-link",
            0,
            ProjectionMutationKind::Patch,
            crate::ProjectionTarget::try_new("TodoView", "todos").unwrap(),
            vec![key(0, "todo_id")],
            vec![ProjectionField::try_new(
                0,
                field,
                ProjectionAssignment::Set(ProjectionExpression::constant(ProjectionValue::string(
                    "changed",
                ))),
            )
            .unwrap()],
            relationship_effects,
            Vec::new(),
        )
        .unwrap()
    }

    fn owner_link(source_field: &str, target_field: &str) -> ProjectionRelationshipEffect {
        ProjectionRelationshipEffect::link(
            0,
            ProjectionRelationship::try_new("TodoView", "owner", "UserView").unwrap(),
            vec![key(0, source_field)],
            vec![key(0, target_field)],
        )
        .unwrap()
    }

    #[test]
    fn hidden_row_change_adds_model_recovery_without_erasing_safe_edge() {
        let selected = select_operation(
            &operation("title", vec![owner_link("todo_id", "user_id")]),
            &models(BTreeMap::from([
                (
                    "TodoView".into(),
                    RoleGrant::columns(["todo_id", "owner_id"]),
                ),
                ("UserView".into(), RoleGrant::columns(["user_id"])),
            ])),
        )
        .unwrap();

        assert!(selected.force_revalidate);
        assert!(selected.fields.is_empty());
        assert_eq!(selected.relationship_effects.len(), 1);
        assert!(selected
            .invalidations
            .contains(&ProjectionInvalidation::model("TodoView").unwrap()));
    }

    #[test]
    fn denied_relationship_consequence_is_omitted_without_broad_recovery() {
        let selected = select_operation(
            &operation("title", vec![owner_link("todo_id", "user_id")]),
            &models(BTreeMap::from([(
                "TodoView".into(),
                RoleGrant::all_columns(),
            )])),
        )
        .unwrap();

        assert!(!selected.force_revalidate);
        assert!(selected.relationship_effects.is_empty());
        assert!(selected.invalidations.is_empty());
        assert_eq!(selected.fields.len(), 1);
    }

    #[test]
    fn unsafe_visible_relationship_uses_narrow_or_source_model_recovery() {
        let narrow = select_operation(
            &operation("title", vec![owner_link("todo_id", "user_id")]),
            &models(BTreeMap::from([
                ("TodoView".into(), RoleGrant::all_columns()),
                ("UserView".into(), RoleGrant::columns(["name"])),
            ])),
        )
        .unwrap();
        assert_eq!(
            narrow.relationship_effects[0].kind(),
            crate::ProjectionRelationshipEffectKind::Invalidate
        );
        assert!(narrow.invalidations.contains(
            &ProjectionInvalidation::relationship("TodoView", "owner", "UserView",).unwrap()
        ));
        assert!(!narrow.force_revalidate);

        let source_recovery = select_operation(
            &operation("owner_id", vec![owner_link("title", "user_id")]),
            &models(BTreeMap::from([
                (
                    "TodoView".into(),
                    RoleGrant::columns(["todo_id", "owner_id"]),
                ),
                ("UserView".into(), RoleGrant::columns(["user_id"])),
            ])),
        )
        .unwrap();
        assert!(source_recovery.relationship_effects.is_empty());
        assert!(source_recovery
            .invalidations
            .contains(&ProjectionInvalidation::model("TodoView").unwrap()));
        assert!(!source_recovery.force_revalidate);
        assert_eq!(source_recovery.fields.len(), 1);
    }

    #[test]
    fn partial_multi_model_selection_keeps_authorized_operations_only() {
        let selected_models = models(BTreeMap::from([(
            "TodoView".into(),
            RoleGrant::all_columns(),
        )]));
        let todo = operation("title", Vec::new());
        let user = ProjectionOperation::try_new(
            "patch-user",
            1,
            ProjectionMutationKind::Patch,
            crate::ProjectionTarget::try_new("UserView", "users").unwrap(),
            vec![key(0, "user_id")],
            vec![ProjectionField::try_new(
                0,
                "name",
                ProjectionAssignment::Set(ProjectionExpression::constant(ProjectionValue::string(
                    "changed",
                ))),
            )
            .unwrap()],
            Vec::new(),
            Vec::new(),
        )
        .unwrap();

        assert_eq!(
            select_operation(&todo, &selected_models)
                .expect("authorized model operation")
                .model,
            "TodoView"
        );
        assert!(
            select_operation(&user, &selected_models).is_none(),
            "an operation against a denied output model must not survive Surface selection"
        );
    }
}
