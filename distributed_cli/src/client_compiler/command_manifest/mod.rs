use std::collections::BTreeSet;

mod effects;
mod projectors;
mod protocol;
mod shape;
mod support;
mod validation;

pub(crate) use validation::validate_command_manifest;

/// Semantic command validation which cannot be expressed by serde alone.
///
/// A command is included in `commands_requiring_revalidation` when its
/// authorized manifest intentionally withholds an exact optimistic effect or
/// finite confirmation plan.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct CommandManifestValidation {
    pub(crate) commands_requiring_revalidation: BTreeSet<String>,
}
