//! Typed command consistency, prepared completions, and portable client effects.
//!
//! This module deliberately separates declaration from durable completion. A
//! handler may prepare a typed payload, but it cannot choose which projection
//! confirmations count. That finite plan belongs to the command declaration,
//! and only the framework-owned command-ledger committer may turn a preparation into
//! an [`Succeeded`], [`Eventual`], or [`Atomic`] value.

#![cfg_attr(not(feature = "graphql"), allow(dead_code))]

pub(crate) mod input;
mod types;

pub(crate) use types::scalar_type_name;
pub use types::{CommandInputType, CommandOutputType, CommandTypeDef, CommandTypeField};

mod direct_projection;
mod effect_wire;
mod effects;
mod outcomes;
mod projection_obligations;
mod projection_proof;
mod projections;
mod typed_command;

#[cfg(test)]
mod tests;

pub use direct_projection::CompiledDirectProjectionTarget;
pub(crate) use direct_projection::{
    compiled_direct_projection_target, CommandDirectProjectionTarget, CommandProjectedModel,
    ResolvedDirectProjectionTarget,
};
// Public seams used by derives / `command_input_defaults!` only. Separately
// authored command_effects! / confirmations constructors are gone.
pub use effect_wire::{
    __command_input_defaults, __effect_key, __effect_key_assignment, __effect_key_field,
    __effect_relationship, __input_default_ulid, __input_default_uuid_v7, CombineEffectNullability,
    CompiledEffectKeyField, CompiledInputDefault, CompiledInputDefaults,
    EffectInputDescendableKind, EffectInputFieldMarker, EffectInputObjectKind, EffectInputPath,
    EffectInputPathKind, EffectInputTerminalKind, EffectModelFieldMarker, EffectNullable,
    EffectPathNullability, EffectRelationshipMarker, EffectRequired, EffectWireBigInt,
    EffectWireBoolean, EffectWireBytea, EffectWireChecked, EffectWireCompatible, EffectWireFloat,
    EffectWireJson, EffectWireList, EffectWireLiteral, EffectWireObject, EffectWireString,
    EffectWireTimestamp, EffectWireUnsupported, TypedEffectExpression, TypedEffectKey,
    TypedEffectRelationship,
};
pub(crate) use effects::{
    CommandEffect, CommandEffects, EffectExpression, EffectFieldValue, EffectKey,
    EffectRelationship,
};
pub use outcomes::{
    Atomic, CommandConsistency, CommandOutcome, Eventual, PrepareCommandError, PreparedCommand,
    Succeeded,
};
#[cfg(test)]
pub(crate) use projection_obligations::InputDefaultGenerator;
pub(crate) use projection_obligations::{
    validate_projection_confirmation_count, CommandInputDefault, CommandProjectionConfirmation,
};
pub(crate) use projections::CommandProjectionEvents;
pub use projections::{
    __command_projection_event_descriptor, __command_projection_event_preview,
    __command_projection_events, __command_projection_preview_constant,
    __command_projection_state_known_values, __command_projection_state_preview, CommandEventSet,
    CommandProjectionEventSet, CommandProjectionPreview, CommandProjectionPreviewSource,
    CommandProjectionPureArg, CommandProjectionPureReduce,
};
// Re-exported for unit tests that resolve obligations through this module path.
#[cfg_attr(not(test), allow(unused_imports))]
pub(crate) use projection_obligations::{
    ProjectionObligationResolutionError, ProjectorTopologyIdentity,
};
pub(crate) use projection_proof::{
    validate_resolved_direct_plan, CommandCommitProofError, ProjectionCommitProof,
};
pub use typed_command::{command_transition, typed_command, TypedCommand};
pub(crate) use typed_command::{TypedCommandContract, TypedServiceCommandBinding};
