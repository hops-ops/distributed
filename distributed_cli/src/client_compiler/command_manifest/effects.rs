use std::collections::BTreeSet;

use crate::client_compiler::manifest::{
    ManifestCommand, ManifestCommandShape, ManifestEffectExpression, ManifestInputDefault,
    ManifestProjectionPreviewSource,
};
use crate::client_compiler::ClientCompileError;

use super::support::command_error;

pub(super) fn validate_trusted_preset_inventory(
    command: &ManifestCommand,
) -> Result<(), ClientCompileError> {
    fn expression_names<'a>(expression: &'a ManifestEffectExpression, out: &mut BTreeSet<&'a str>) {
        if let ManifestEffectExpression::TrustedPreset { name } = expression {
            out.insert(name);
        }
    }
    let mut referenced = BTreeSet::new();
    if let Some(direct) = &command.extensions.direct_projection {
        if let Some(partition) = &direct.partition {
            expression_names(partition, &mut referenced);
        }
    }
    if let Some(projection) = &command.extensions.projection {
        for occurrence in &projection.preview_occurrences {
            for value in &occurrence.values {
                if let ManifestProjectionPreviewSource::TrustedPreset { name, .. } = &value.source {
                    referenced.insert(name);
                }
            }
        }
    }

    let declared = command
        .extensions
        .trusted_presets
        .iter()
        .map(|descriptor| descriptor.name.as_str())
        .collect::<BTreeSet<_>>();
    if declared != referenced {
        return Err(command_error(
            command,
            "client.manifest.trusted_preset_inventory",
            "trusted_presets must exactly describe every trusted preset expression",
        ));
    }
    Ok(())
}

pub(super) fn validate_defaults(
    command: &ManifestCommand,
    version: u32,
    defaults: &[ManifestInputDefault],
) -> Result<(), ClientCompileError> {
    if version != 1 || defaults.is_empty() {
        return Err(command_error(
            command,
            "client.manifest.input_defaults",
            "input_defaults must be version 1 with at least one entry",
        ));
    }
    let ManifestCommandShape::Object { definition } = &command.input else {
        return Err(command_error(
            command,
            "client.manifest.input_default_path",
            "generated defaults require a typed object input",
        ));
    };
    let mut paths = BTreeSet::new();
    for default in defaults {
        let [field_name] = default.path.as_slice() else {
            return Err(command_error(
                command,
                "client.manifest.input_default_path",
                "generated default must target exactly one top-level input field",
            ));
        };
        if !paths.insert(field_name.as_str()) {
            return Err(command_error(
                command,
                "client.manifest.input_default_path",
                format!("repeats generated default path `{field_name}`"),
            ));
        }
        let field = definition
            .fields
            .iter()
            .find(|field| field.name == *field_name)
            .ok_or_else(|| {
                command_error(
                    command,
                    "client.manifest.input_default_path",
                    format!("generated default references unknown input field `{field_name}`"),
                )
            })?;
        if field.nullable
            || field.list
            || field.nested.is_some()
            || !matches!(field.type_name.as_str(), "String" | "ID")
        {
            return Err(command_error(
                command,
                "client.manifest.input_default_path",
                format!(
                    "generated default `{field_name}` requires a non-null, non-list String/ID field"
                ),
            ));
        }
    }
    Ok(())
}
