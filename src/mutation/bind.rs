//! Bind mutation inputs to the event that fills them.

use crate::projection::{
    ProjectionArm, ProjectionEnvelopeField, ProjectionEventSelector, ProjectionExpression,
    ProjectionExpressionRef, ProjectionProgramError, ProjectionValueType,
};

use super::program::MutationProgram;

/// One mutation input path and the event expression that fills it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MutationInputBinding {
    path: Vec<String>,
    expression: ProjectionExpression,
}

impl MutationInputBinding {
    /// Fill the mutation input at `path` from `expression`.
    ///
    /// # Errors
    ///
    /// Rejects an empty path and empty path segments.
    pub fn try_new(
        path: Vec<String>,
        expression: ProjectionExpression,
    ) -> Result<Self, ProjectionProgramError> {
        if path.is_empty() || path.iter().any(String::is_empty) {
            return Err(ProjectionProgramError::EmptyName("mutation input path"));
        }
        Ok(Self { path, expression })
    }

    /// Return the mutation input path.
    pub fn path(&self) -> &[String] {
        &self.path
    }

    /// Return the event-side expression.
    pub fn expression(&self) -> &ProjectionExpression {
        &self.expression
    }
}

/// Fill the mutation input at `input_path` from the event body at `body_path`.
///
/// # Errors
///
/// Rejects invalid paths.
pub fn body_field_binding(
    input_path: impl IntoIterator<Item = impl Into<String>>,
    body_path: impl IntoIterator<Item = impl Into<String>>,
    value_type: ProjectionValueType,
) -> Result<MutationInputBinding, ProjectionProgramError> {
    MutationInputBinding::try_new(
        input_path.into_iter().map(Into::into).collect(),
        ProjectionExpression::body_path(value_type, body_path)?,
    )
}

/// Fill the mutation input at `input_path` from an envelope field.
///
/// # Errors
///
/// Rejects an invalid path.
pub fn envelope_binding(
    input_path: impl IntoIterator<Item = impl Into<String>>,
    field: ProjectionEnvelopeField,
) -> Result<MutationInputBinding, ProjectionProgramError> {
    MutationInputBinding::try_new(
        input_path.into_iter().map(Into::into).collect(),
        ProjectionExpression::envelope(field),
    )
}

/// The event expression for the mutation input at `path`.
///
/// A path bound itself takes its binding. Otherwise the longest bound prefix
/// that reads an event body object is followed for the remaining segments,
/// typed as `value_type`.
pub(super) fn bind_input(
    inputs: &[MutationInputBinding],
    path: &[String],
    value_type: &ProjectionValueType,
) -> Result<ProjectionExpression, ProjectionProgramError> {
    if let Some(exact) = inputs.iter().find(|input| input.path == path) {
        return Ok(exact.expression.clone());
    }
    let object = inputs
        .iter()
        .filter(|input| path.starts_with(&input.path))
        .max_by_key(|input| input.path.len());
    if let Some(input) = object {
        if let ProjectionExpressionRef::BodyPath { path: body, .. } = input.expression.as_ref() {
            return ProjectionExpression::body_path(
                value_type.clone(),
                body.iter().chain(&path[input.path.len()..]).cloned(),
            );
        }
    }
    Err(ProjectionProgramError::MissingInput {
        path: path.join("."),
    })
}

/// One arm of a projection: an exact event, the mutation it applies and how
/// the event fills the mutation's inputs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionHandler {
    arm_id: String,
    selector: ProjectionEventSelector,
    inputs: Vec<MutationInputBinding>,
    program: MutationProgram,
}

impl ProjectionHandler {
    /// Bind `selector`'s event to `program` through `inputs`.
    ///
    /// # Errors
    ///
    /// Rejects an empty arm identifier and repeated input paths.
    pub fn try_new(
        arm_id: impl Into<String>,
        selector: ProjectionEventSelector,
        inputs: Vec<MutationInputBinding>,
        program: MutationProgram,
    ) -> Result<Self, ProjectionProgramError> {
        let arm_id = arm_id.into();
        if arm_id.is_empty() {
            return Err(ProjectionProgramError::EmptyName("projection arm ID"));
        }
        for (index, input) in inputs.iter().enumerate() {
            if inputs[..index]
                .iter()
                .any(|earlier| earlier.path == input.path)
            {
                return Err(ProjectionProgramError::DuplicateName {
                    kind: "mutation input binding",
                    name: input.path.join("."),
                });
            }
        }
        Ok(Self {
            arm_id,
            selector,
            inputs,
            program,
        })
    }

    /// Return the exact event selector.
    pub fn selector(&self) -> &ProjectionEventSelector {
        &self.selector
    }

    /// Return the bound mutation.
    pub fn program(&self) -> &MutationProgram {
        &self.program
    }

    /// The projection arm this handler is.
    ///
    /// # Errors
    ///
    /// Returns an unbound input or an invalid operation.
    pub fn arm(&self) -> Result<ProjectionArm, ProjectionProgramError> {
        ProjectionArm::try_new(
            self.arm_id.clone(),
            self.selector.clone(),
            self.program.bind(&self.inputs)?,
        )
    }
}
