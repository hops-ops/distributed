use std::collections::{BTreeMap, BTreeSet};

use super::*;

pub(crate) fn canonicalize_projectors(
    projectors: &mut [ClientProjector],
) -> Result<(), ClientCompileError> {
    for projector in projectors.iter_mut() {
        canonicalize_string_set(
            &mut projector.facts,
            &format!("projector `{}` fact", projector.name),
        )?;
        canonicalize_string_set(
            &mut projector.models,
            &format!("projector `{}` model", projector.name),
        )?;
        canonicalize_string_set(
            &mut projector.dependencies,
            &format!("projector `{}` dependency", projector.name),
        )?;
    }
    projectors.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(())
}

pub(crate) fn validate_projectors(
    projectors: &[ClientProjector],
    models: &BTreeMap<String, ClientModel>,
) -> Result<(), ClientCompileError> {
    let mut names = BTreeSet::new();
    for projector in projectors {
        if projector.version != 1 {
            return Err(ClientCompileError::manifest(
                "client.manifest.projector_version",
                format!("projector `{}` must use version 1", projector.name),
            ));
        }
        validate_nonempty(&projector.name, "manifest projector name")?;
        if !names.insert(projector.name.as_str()) {
            return Err(ClientCompileError::manifest(
                "client.manifest.duplicate_projector",
                format!("duplicate manifest projector `{}`", projector.name),
            ));
        }
        if projector.models.is_empty() {
            return Err(ClientCompileError::manifest(
                "client.manifest.projector_inventory",
                format!(
                    "projection owner `{}` must declare at least one model",
                    projector.name
                ),
            ));
        }
        let mut expected_dependencies = BTreeSet::new();
        for model in &projector.models {
            let Some(model_contract) = models.get(model) else {
                return Err(ClientCompileError::manifest(
                    "client.manifest.projector_model",
                    format!(
                        "projector `{}` references absent model `{model}`",
                        projector.name
                    ),
                ));
            };
            expected_dependencies.insert(model_contract.source_table.as_str());
        }
        let actual_dependencies = projector
            .dependencies
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        if actual_dependencies != expected_dependencies {
            return Err(ClientCompileError::manifest(
                "client.manifest.projector_dependency",
                format!(
                    "projector `{}` dependencies must exactly cover its model source tables",
                    projector.name
                ),
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_direct_projections(
    commands: &[ClientCommand],
    models: &BTreeMap<String, ClientModel>,
    projectors: &[ClientProjector],
) -> Result<BTreeSet<String>, ClientCompileError> {
    let mut requiring_revalidation = BTreeSet::new();
    for command in commands {
        let consistency = command.extensions.consistency.kind;
        let direct = command.extensions.direct_projection.as_ref();
        match (consistency, direct) {
            (CommandConsistency::Atomic, None) => {
                return Err(ClientCompileError::manifest(
                    "client.manifest.direct_projection_required",
                    format!(
                        "manifest projected command `{}` requires exactly one direct_projection target",
                        command.name
                    ),
                ));
            }
            (CommandConsistency::Atomic, Some(direct)) => {
                if validate_direct_projection(command, direct, models, projectors)? {
                    requiring_revalidation.insert(command.name.clone());
                }
            }
            (_, Some(_)) => {
                return Err(ClientCompileError::manifest(
                    "client.manifest.direct_projection_unexpected",
                    format!(
                        "manifest non-projected command `{}` cannot declare direct_projection",
                        command.name
                    ),
                ));
            }
            (_, None) => {}
        }
    }
    Ok(requiring_revalidation)
}

pub(crate) fn validate_direct_projection(
    command: &ClientCommand,
    direct: &CommandDirectProjectionExtension,
    models: &BTreeMap<String, ClientModel>,
    projectors: &[ClientProjector],
) -> Result<bool, ClientCompileError> {
    if direct.topology.version != 1 {
        return Err(ClientCompileError::manifest(
            "client.manifest.direct_projection_topology",
            format!(
                "manifest command `{}` direct projection topology version must be 1",
                command.name
            ),
        ));
    }
    validate_projection_name(
        &direct.topology.name,
        &format!(
            "manifest command `{}` direct projection topology name",
            command.name
        ),
    )?;
    validate_hash(
        &direct.topology.digest,
        &format!(
            "manifest command `{}` direct projection topology digest",
            command.name
        ),
    )?;
    validate_projection_epoch(
        &direct.change_epoch,
        &format!(
            "manifest command `{}` direct projection change_epoch",
            command.name
        ),
    )?;
    validate_graphql_name(
        &direct.model,
        &format!(
            "manifest command `{}` direct projection model",
            command.name
        ),
    )?;
    let model = models.get(&direct.model).ok_or_else(|| {
        ClientCompileError::manifest(
            "client.manifest.direct_projection_model",
            format!(
                "manifest command `{}` direct projection references absent model `{}`",
                command.name, direct.model
            ),
        )
    })?;
    if !matches!(&model.normalization, ModelNormalization::Normalized { .. }) {
        return Err(ClientCompileError::manifest(
            "client.manifest.direct_projection_model",
            format!(
                "manifest command `{}` direct projection model `{}` has no complete authorized normalized identity",
                command.name, direct.model
            ),
        ));
    }
    let ClientCommandShape::Object { definition } = &command.output else {
        return Err(ClientCompileError::manifest(
            "client.manifest.direct_projection_output",
            format!(
                "manifest projected command `{}` must return its exact model object",
                command.name
            ),
        ));
    };
    if definition.name != model.typename || definition.fields.len() != model.fields.len() {
        return Err(ClientCompileError::manifest(
            "client.manifest.direct_projection_output",
            format!(
                "manifest projected command `{}` output `{}` does not exactly match model `{}` typename `{}`",
                command.name, definition.name, direct.model, model.typename
            ),
        ));
    }
    for field in &definition.fields {
        let matches = model.fields.iter().any(|model_field| {
            field.name == model_field.name
                && field.type_name == model_field.scalar
                && field.nullable == model_field.nullable
                && !field.list
                && !field.item_nullable
                && field.codec.as_deref() == Some(model_field.codec.as_str())
                && field.nested.is_none()
        });
        if !matches {
            return Err(ClientCompileError::manifest(
                "client.manifest.direct_projection_output",
                format!(
                    "manifest projected command `{}` output field `{}.{}` differs from model `{}`",
                    command.name, definition.name, field.name, direct.model
                ),
            ));
        }
    }

    let visible_owners = projectors
        .iter()
        .filter(|projector| projector.models.iter().any(|model| model == &direct.model))
        .collect::<Vec<_>>();
    match visible_owners.as_slice() {
        [] => {
            if projectors
                .iter()
                .any(|projector| projector.name == direct.topology.name)
            {
                return Err(ClientCompileError::manifest(
                    "client.manifest.direct_projection_owner",
                    format!(
                        "manifest command `{}` topology `{}` does not own model `{}`",
                        command.name, direct.topology.name, direct.model
                    ),
                ));
            }
            // A role surface may intentionally omit the topology because it
            // also owns denied models. The exact digest is sufficient and does
            // not disclose those hidden model/fact/table identities.
        }
        [owner] if owner.name == direct.topology.name => {}
        [owner] => {
            return Err(ClientCompileError::manifest(
                "client.manifest.direct_projection_owner",
                format!(
                    "manifest command `{}` names topology `{}` but visible projector `{}` owns model `{}`",
                    command.name, direct.topology.name, owner.name, direct.model
                ),
            ));
        }
        owners => {
            return Err(ClientCompileError::manifest(
                "client.manifest.direct_projection_owner",
                format!(
                    "manifest command `{}` model `{}` has ambiguous visible projector ownership: {}",
                    command.name,
                    direct.model,
                    owners
                        .iter()
                        .map(|owner| owner.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ));
        }
    }

    direct
        .partition
        .as_ref()
        .map(|partition| validate_direct_projection_partition(command, partition))
        .transpose()
        .map(|requires_revalidation| requires_revalidation.unwrap_or(false))
}

pub(crate) fn validate_projection_name(value: &str, label: &str) -> Result<(), ClientCompileError> {
    validate_nonempty(value, label)?;
    if value.len() > 128
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(ClientCompileError::manifest(
            "client.manifest.direct_projection_topology",
            format!("{label} must be at most 128 bytes without whitespace or control characters"),
        ));
    }
    Ok(())
}

pub(crate) fn validate_projection_epoch(
    value: &str,
    label: &str,
) -> Result<(), ClientCompileError> {
    validate_nonempty(value, label)?;
    if value.len() > 128 || value.chars().any(char::is_control) {
        return Err(ClientCompileError::manifest(
            "client.manifest.direct_projection_epoch",
            format!("{label} must be at most 128 bytes without control characters"),
        ));
    }
    Ok(())
}

pub(crate) fn validate_direct_projection_partition(
    command: &ClientCommand,
    partition: &EffectExpression,
) -> Result<bool, ClientCompileError> {
    match partition {
        EffectExpression::Input { path } => {
            if path.is_empty() {
                return Err(ClientCompileError::manifest(
                    "client.manifest.direct_projection_partition",
                    format!(
                        "manifest command `{}` direct projection partition input path must not be empty",
                        command.name
                    ),
                ));
            }
            validate_nonempty_strings(
                path,
                &format!(
                    "manifest command `{}` direct projection partition input path",
                    command.name
                ),
            )?;
            let ClientCommandShape::Object { definition } = &command.input else {
                return Err(ClientCompileError::manifest(
                    "client.manifest.direct_projection_partition",
                    format!(
                        "manifest command `{}` direct projection partition input requires a typed object input",
                        command.name
                    ),
                ));
            };
            let mut current = definition;
            for (index, segment) in path.iter().enumerate() {
                let field = current
                    .fields
                    .iter()
                    .find(|field| field.name == *segment)
                    .ok_or_else(|| {
                        ClientCompileError::manifest(
                            "client.manifest.direct_projection_partition",
                            format!(
                                "manifest command `{}` direct projection references unknown input path `{}`",
                                command.name,
                                path.join(".")
                            ),
                        )
                    })?;
                if index + 1 == path.len() {
                    if field.list || field.nested.is_some() {
                        return Err(ClientCompileError::manifest(
                            "client.manifest.direct_projection_partition",
                            format!(
                                "manifest command `{}` direct projection partition path `{}` must resolve to a scalar",
                                command.name,
                                path.join(".")
                            ),
                        ));
                    }
                    return Ok(false);
                }
                if field.list {
                    return Err(ClientCompileError::manifest(
                        "client.manifest.direct_projection_partition",
                        format!(
                            "manifest command `{}` direct projection partition path `{}` descends through a list",
                            command.name,
                            path.join(".")
                        ),
                    ));
                }
                current = field.nested.as_deref().ok_or_else(|| {
                    ClientCompileError::manifest(
                        "client.manifest.direct_projection_partition",
                        format!(
                            "manifest command `{}` direct projection partition path `{}` descends through a scalar",
                            command.name,
                            path.join(".")
                        ),
                    )
                })?;
            }
            unreachable!("non-empty direct projection path resolves or returns an error")
        }
        EffectExpression::TrustedPreset { name } => {
            let declared = command
                .extensions
                .trusted_presets
                .iter()
                .any(|descriptor| descriptor.name == *name && descriptor.codec == "string");
            if !declared {
                return Err(ClientCompileError::manifest(
                    "client.manifest.direct_projection_partition",
                    format!(
                        "manifest command `{}` direct projection partition trusted preset `{name}` must declare the string codec",
                        command.name
                    ),
                ));
            }
            Ok(false)
        }
        EffectExpression::Constant { .. } | EffectExpression::Null => Ok(false),
    }
}
