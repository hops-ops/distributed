//! Event-independent read-model mutations and their binding to events.
//!
//! A mutation states which input fills which column of a row. It names no
//! event. [`crate::projection!`] binds events to a mutation's inputs, and the
//! result is ordinary [`crate::projection::ProjectionProgram`] arms: there is
//! one program type and one expression type, and a mutation input is resolved
//! to an event expression when the arm is built.

#![deny(missing_docs)]

mod bind;
mod descriptor;
mod program;

#[cfg(test)]
mod tests;

pub use crate::projection::MutationKind;
pub use bind::{body_field_binding, envelope_binding, MutationInputBinding, ProjectionHandler};
pub use descriptor::{
    bind_event_apply_mutation, bind_event_to_mutation, compile_projection,
    delete_by_pk_program_for_model, inventory_single_model, lower_single_model,
    state_upsert_program_for_model, ProjectionInputSource,
};
pub use program::{
    projection_value_type_for_column, Mutation, MutationColumn, MutationInput, MutationOperation,
    MutationProgram, MutationStep,
};
