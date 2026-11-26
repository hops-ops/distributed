//! Compile event→mutation projections for service registration.

use crate::domain_event::DomainEventDescriptor;
use crate::projection::lower::{
    finish_lowering, lower_model_mutation, LoweredProjectionPlan, ProjectionLoweringError,
    ProjectionOutputInventory, ProjectionOutputModel, ProjectionReadModelMetadata,
};
use crate::projection::{
    MutationKind, ProjectionEnvelopeField, ProjectionEventSelector, ProjectionExpression,
    ProjectionPartition, ProjectionProgram, ProjectionProgramError, ProjectionValueType,
    ResolvedProjectionPlan,
};
use crate::read_model::RelationalReadModel;

use super::bind::{envelope_binding, MutationInputBinding, ProjectionHandler};
use super::program::{complete_row, MutationColumn, MutationOperation, MutationProgram};

/// Compile event→mutation arms into one projection program.
///
/// Prefer [`crate::projection!`] for a unit partition. Use this when the
/// partition is an expression or the arms are built by hand.
///
/// # Errors
///
/// Returns the first unbound input, invalid operation or duplicate selector.
pub fn compile_projection(
    name: &str,
    version: u64,
    partition: ProjectionPartition,
    handlers: impl IntoIterator<Item = ProjectionHandler>,
) -> Result<ProjectionProgram, ProjectionProgramError> {
    let arms = handlers
        .into_iter()
        .map(|handler| handler.arm())
        .collect::<Result<Vec<_>, _>>()?;
    ProjectionProgram::try_new(name, version, partition, arms)
}

/// Bind an exact event contract to a mutation with explicit inputs. The arm is
/// named after the event.
///
/// # Errors
///
/// Propagates selector or binding construction failures.
pub fn bind_event_to_mutation(
    descriptor: &DomainEventDescriptor,
    inputs: Vec<MutationInputBinding>,
    program: MutationProgram,
) -> Result<ProjectionHandler, ProjectionProgramError> {
    ProjectionHandler::try_new(
        descriptor.name.replace('.', "-"),
        ProjectionEventSelector::try_from_descriptor(descriptor)?,
        inputs,
        program,
    )
}

/// How one mutation input is filled from a domain-event occurrence.
///
/// Authoring surface: `input: { <key>: body | aggregate_id }` inside
/// [`crate::projection!`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionInputSource {
    /// `input.<key>` is the event body: `input.<key>.<field>` reads the body
    /// member `<field>`.
    Body,
    /// `input.<key>` is the envelope aggregate id (typical delete-by-pk).
    AggregateId,
}

impl ProjectionInputSource {
    /// The binding that fills `input.<key>` from this source.
    ///
    /// # Errors
    ///
    /// Rejects an empty key.
    pub fn binding(self, key: &str) -> Result<MutationInputBinding, ProjectionProgramError> {
        match self {
            Self::Body => MutationInputBinding::try_new(
                vec![key.to_owned()],
                ProjectionExpression::body_path(ProjectionValueType::Json, Vec::<String>::new())?,
            ),
            Self::AggregateId => envelope_binding([key], ProjectionEnvelopeField::AggregateId),
        }
    }
}

/// Bind an event to a mutation whose one input `input_key` comes from `source`.
///
/// # Errors
///
/// Propagates binding construction failures.
pub fn bind_event_apply_mutation(
    descriptor: &DomainEventDescriptor,
    program: MutationProgram,
    input_key: &str,
    source: ProjectionInputSource,
) -> Result<ProjectionHandler, ProjectionProgramError> {
    bind_event_to_mutation(descriptor, vec![source.binding(input_key)?], program)
}

/// Lower a resolved plan for a single ordinary (non-related) model `M`.
///
/// # Errors
///
/// Propagates ORM lowering failures.
pub fn lower_single_model<M>(
    plan: &ResolvedProjectionPlan,
) -> Result<LoweredProjectionPlan, ProjectionLoweringError>
where
    M: ProjectionReadModelMetadata,
{
    let records = plan
        .mutations()
        .iter()
        .map(lower_model_mutation::<M>)
        .collect::<Result<Vec<_>, _>>()?;
    finish_lowering(plan, records)
}

/// Inventory for a single output model.
///
/// # Errors
///
/// Propagates schema validation failures.
pub fn inventory_single_model<M>() -> Result<ProjectionOutputInventory, ProjectionLoweringError>
where
    M: ProjectionReadModelMetadata,
{
    Ok(ProjectionOutputInventory::new(
        vec![ProjectionOutputModel::of::<M>()?],
        Vec::new(),
    ))
}

/// A complete-row upsert of model `M` from the input object `input_root`.
///
/// # Errors
///
/// Propagates schema or validation failures.
pub fn state_upsert_program_for_model<M>(
    name: &str,
    version: u64,
    operation_id: &str,
    input_root: &str,
) -> Result<MutationProgram, ProjectionProgramError>
where
    M: RelationalReadModel,
{
    let operation = complete_row(
        operation_id.to_owned(),
        MutationKind::Upsert,
        M::schema(),
        &[input_root],
    )?;
    MutationProgram::try_new(name, version, vec![operation])
}

/// A delete of model `M` by primary key, each key column read from the
/// same-named top-level input.
///
/// # Errors
///
/// Propagates schema or validation failures.
pub fn delete_by_pk_program_for_model<M>(
    name: &str,
    version: u64,
    operation_id: &str,
) -> Result<MutationProgram, ProjectionProgramError>
where
    M: RelationalReadModel,
{
    let schema = M::schema();
    let key = schema
        .primary_key
        .columns
        .iter()
        .map(|column| {
            MutationColumn::try_new(
                column.clone(),
                ProjectionValueType::String,
                [column.clone()],
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let operation = MutationOperation::try_new(
        operation_id,
        MutationKind::Delete,
        crate::projection::ProjectionTarget::try_new(
            schema.model_name.clone(),
            schema.table_name.clone(),
        )?,
        key,
        Vec::new(),
    )?;
    MutationProgram::try_new(name, version, vec![operation])
}
