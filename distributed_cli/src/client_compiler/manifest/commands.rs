use super::*;

pub(crate) fn canonicalize_commands(
    commands: &mut [ClientCommand],
) -> Result<(), ClientCompileError> {
    for command in commands.iter_mut() {
        canonicalize_string_set(
            &mut command.grants,
            &format!("command `{}` grant", command.name),
        )?;
        for descriptor in &command.extensions.trusted_presets {
            validate_nonempty(&descriptor.name, "trusted preset name")?;
            validate_nonempty(&descriptor.codec, "trusted preset codec")?;
            if descriptor.name.len() > 128
                || descriptor.name.trim() != descriptor.name
                || descriptor.name.chars().any(char::is_control)
            {
                return Err(ClientCompileError::manifest(
                    "client.manifest.trusted_preset_name",
                    format!(
                        "manifest command `{}` has an invalid trusted preset name",
                        command.name
                    ),
                ));
            }
        }
        command.extensions.trusted_presets.sort();
        if command
            .extensions
            .trusted_presets
            .windows(2)
            .any(|pair| pair[0].name == pair[1].name)
        {
            return Err(ClientCompileError::manifest(
                "client.manifest.trusted_preset_inventory",
                format!(
                    "manifest command `{}` repeats a trusted preset descriptor",
                    command.name
                ),
            ));
        }
        if let Some(defaults) = &mut command.extensions.input_defaults {
            for default in &mut defaults.defaults {
                validate_nonempty_strings(
                    &default.path,
                    &format!("command `{}` input default path", command.name),
                )?;
            }
            defaults
                .defaults
                .sort_by(|left, right| left.path.cmp(&right.path));
            if defaults
                .defaults
                .windows(2)
                .any(|pair| pair[0].path == pair[1].path)
            {
                return Err(ClientCompileError::manifest(
                    "client.manifest.input_default_path",
                    format!(
                        "manifest command `{}` repeats an input default path",
                        command.name
                    ),
                ));
            }
        }
        if let Some(direct) = &mut command.extensions.direct_projection {
            if let Some(partition) = &mut direct.partition {
                canonicalize_effect_expression(partition);
            }
        }
    }
    commands.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(())
}

pub(crate) fn canonicalize_effect_expression(expression: &mut EffectExpression) {
    if let EffectExpression::Constant { value } = expression {
        *value = canonical_json_value(std::mem::take(value));
    }
}
