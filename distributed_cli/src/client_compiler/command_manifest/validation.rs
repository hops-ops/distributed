use std::collections::{BTreeMap, BTreeSet};

use crate::client_compiler::manifest::{
    validate_exact_operation_hash, ManifestCommand, ManifestCommandShape, ManifestModel,
    ManifestProjector, ManifestProtocolOperations, ManifestRoot, ManifestTypeDef, RootOperation,
};
use crate::client_compiler::ClientCompileError;

use super::effects::{validate_defaults, validate_trusted_preset_inventory};
use super::projectors::validate_projector_inventory;
use super::protocol::validate_protocol_operations;
use super::shape::{
    canonical_command_operation, occupied_surface_types, projected_output_typename, validate_shape,
    CommandTypeKind,
};
use super::support::{command_error, graphql_name, invalid, nonempty, unique_nonempty};
use super::CommandManifestValidation;

pub(crate) fn validate_command_manifest(
    commands: &[ManifestCommand],
    models: &BTreeMap<String, ManifestModel>,
    roots: &BTreeMap<(RootOperation, String), ManifestRoot>,
    scalar_codecs: &BTreeMap<String, String>,
    projectors: &[ManifestProjector],
    causal_receipts: bool,
    protocol_operations: &ManifestProtocolOperations,
) -> Result<CommandManifestValidation, ClientCompileError> {
    if causal_receipts != !commands.is_empty() {
        return Err(invalid(
            "client.manifest.command_capability",
            "manifest command inventory and capabilities.causal_receipts must agree",
        ));
    }
    validate_protocol_operations(protocol_operations, !commands.is_empty())?;
    validate_projector_inventory(projectors, models)?;
    let occupied_types = occupied_surface_types(models, roots, scalar_codecs);

    let mut command_names = BTreeSet::new();
    let mut mutation_fields = BTreeSet::new();
    let mut type_definitions = BTreeMap::new();
    for command in commands {
        validate_command(
            command,
            models,
            scalar_codecs,
            &occupied_types,
            &mut type_definitions,
        )?;
        if !command_names.insert(command.name.as_str()) {
            return Err(invalid(
                "client.manifest.duplicate_command",
                format!("duplicate manifest command `{}`", command.name),
            ));
        }
        if !mutation_fields.insert(command.mutation_field.as_str()) {
            return Err(invalid(
                "client.manifest.duplicate_command_field",
                format!(
                    "duplicate manifest command mutation field `{}`",
                    command.mutation_field
                ),
            ));
        }
    }
    Ok(CommandManifestValidation::default())
}

fn validate_command(
    command: &ManifestCommand,
    models: &BTreeMap<String, ManifestModel>,
    scalar_codecs: &BTreeMap<String, String>,
    occupied_types: &BTreeSet<String>,
    type_definitions: &mut BTreeMap<String, (CommandTypeKind, ManifestTypeDef)>,
) -> Result<(), ClientCompileError> {
    if command.version != 1 {
        return Err(command_error(
            command,
            "client.manifest.command_version",
            "version must be 1",
        ));
    }
    nonempty(&command.name, "command name")?;
    graphql_name(&command.mutation_field, "command mutation field")?;
    unique_nonempty(
        &command.grants,
        &format!("command `{}` grant", command.name),
    )?;

    validate_shape(
        &command.input,
        CommandTypeKind::Input,
        scalar_codecs,
        &format!("command `{}` input", command.name),
        occupied_types,
        None,
        type_definitions,
    )?;
    if matches!(command.output, ManifestCommandShape::None) {
        return Err(command_error(
            command,
            "client.manifest.command_output",
            "cannot declare an empty output",
        ));
    }
    validate_shape(
        &command.output,
        CommandTypeKind::Output,
        scalar_codecs,
        &format!("command `{}` output", command.name),
        occupied_types,
        projected_output_typename(command, models),
        type_definitions,
    )?;

    let canonical = canonical_command_operation(command);
    if command.operation != canonical {
        return Err(command_error(
            command,
            "client.manifest.command_operation",
            "operation does not byte-match the canonical typed command operation",
        ));
    }
    validate_exact_operation_hash(&command.operation, &command.operation_hash, "command")?;

    let extensions = &command.extensions;
    if extensions.version != 2 {
        return Err(command_error(
            command,
            "client.manifest.command_extensions",
            "extensions.version must be 2; legacy command authority has no decoder",
        ));
    }
    let consistency = &extensions.consistency;
    if consistency.version != 1 {
        return Err(command_error(
            command,
            "client.manifest.command_consistency",
            "consistency.version must be 1",
        ));
    }
    if let Some(defaults) = &extensions.input_defaults {
        validate_defaults(command, defaults.version, &defaults.defaults)?;
    }
    validate_trusted_preset_inventory(command)
}
