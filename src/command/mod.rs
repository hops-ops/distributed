//! Typed command consistency and prepared completions.
//!
//! This module deliberately separates declaration from durable completion. A
//! handler may prepare a typed payload, but only the framework-owned
//! command-ledger committer may turn a preparation into an [`Succeeded`],
//! [`Eventual`], or [`Atomic`] value.

#![cfg_attr(not(feature = "graphql"), allow(dead_code))]

pub(crate) mod input;
mod types;

pub use distributed_macros::{CommandInput, CommandOutput};

pub(crate) use types::scalar_type_name;
pub use types::{
    __input_has_field, CommandInputType, CommandOutputType, CommandTypeDef, CommandTypeField,
    CommandUnsignedInteger,
};

mod direct_projection;
mod effect_wire;
mod effects;
mod outcomes;
mod projection_proof;
mod projections;
mod typed_command;

#[cfg(test)]
mod tests;

pub(crate) use direct_projection::{
    CommandDirectProjectionTarget, CommandProjectedModel, ResolvedDirectProjectionTarget,
};
// Public seams used by derives / `command_input_defaults!` only.
pub use effect_wire::{
    __command_input_defaults, __input_default_now, __input_default_ulid, __input_default_uuid_v7,
    CommandInputDefault, CompiledInputDefault, CompiledInputDefaults, EffectInputFieldMarker,
    EffectInputObjectKind, EffectInputPathKind, EffectInputTerminalKind, EffectModelFieldMarker,
    EffectNullable, EffectPathNullability, EffectRelationshipMarker, EffectRequired,
    EffectWireBigInt, EffectWireBoolean, EffectWireBytea, EffectWireFloat, EffectWireJson,
    EffectWireList, EffectWireObject, EffectWireString, EffectWireTimestamp, EffectWireUnsupported,
    InputDefaultGenerator,
};
pub use effects::EffectExpression;
pub use outcomes::{
    Atomic, CommandConsistency, CommandOutcome, Eventual, PrepareCommandError, PreparedCommand,
    Succeeded,
};
pub(crate) use projection_proof::{
    validate_resolved_direct_plan, CommandCommitProofError, ProjectionCommitProof,
};
pub use projections::{
    __command_projection_event_descriptor, __command_projection_event_preview,
    __command_projection_events, __command_projection_preview_constant,
    __command_projection_state_preview, __run_wasm_pure, CommandEventSet, CommandProjectionBody,
    CommandProjectionEventSet, CommandProjectionPreview, CommandProjectionPreviewSource,
    CommandProjectionPureArg, CommandProjectionPureReduce, WasmPure,
};
pub(crate) use projections::{authenticated_user_field_preview, CommandProjectionEvents};
pub use typed_command::{command_transition, typed_command, TypedCommand};
pub(crate) use typed_command::{TypedCommandContract, TypedServiceCommandBinding};
