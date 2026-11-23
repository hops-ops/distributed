use super::*;

/// The scope-wide trusted-preset inventory of a client surface: every
/// command's declared presets plus every claim a client-visible row policy
/// compares with a field, each with one wire codec, sorted by name.
///
/// # Errors
///
/// A preset name that is invalid or used with two codecs, or a row policy
/// naming an absent field, relationship or model, or a non-local codec.
pub fn trusted_preset_descriptors<'a>(
    models: impl IntoIterator<Item = &'a ClientModel>,
    commands: &[ClientCommand],
) -> Result<Vec<ClientTrustedPresetDescriptor>, ClientManifestError> {
    let models: BTreeMap<&str, &ClientModel> = models
        .into_iter()
        .map(|model| (model.id.as_str(), model))
        .collect();
    let mut descriptors = BTreeMap::<String, String>::new();

    for command in commands {
        for descriptor in &command.extensions.trusted_presets {
            insert_trusted_preset_descriptor(
                &mut descriptors,
                descriptor,
                &format!("command `{}`", command.name),
            )?;
        }
    }
    for model in models.values() {
        if let ClientRowPolicy::Predicate { expression } = &model.row_policy {
            collect_row_policy_trusted_presets(expression, model, &models, &mut descriptors)?;
        }
    }

    Ok(descriptors
        .into_iter()
        .map(|(name, codec)| ClientTrustedPresetDescriptor { name, codec })
        .collect())
}

fn collect_row_policy_trusted_presets(
    expression: &FilterExpr,
    model: &ClientModel,
    models: &BTreeMap<&str, &ClientModel>,
    descriptors: &mut BTreeMap<String, String>,
) -> Result<(), ClientManifestError> {
    match expression {
        FilterExpr::And(expressions) | FilterExpr::Or(expressions) => {
            for expression in expressions {
                collect_row_policy_trusted_presets(expression, model, models, descriptors)?;
            }
        }
        FilterExpr::Not(expression) => {
            collect_row_policy_trusted_presets(expression, model, models, descriptors)?;
        }
        FilterExpr::Cmp {
            column,
            rhs: Operand::Claim(claim),
            ..
        } => {
            insert_row_policy_trusted_preset(model, column, &claim.header, descriptors)?;
        }
        FilterExpr::In { column, values, .. } => {
            for value in values {
                if let Operand::Claim(claim) = value {
                    insert_row_policy_trusted_preset(model, column, &claim.header, descriptors)?;
                }
            }
        }
        FilterExpr::Rel { field, predicate } => {
            let relationship = model
                .relationships
                .iter()
                .find(|relationship| relationship.name == *field)
                .ok_or_else(|| {
                    ClientManifestError(format!(
                        "model `{}` client-visible row policy references absent relationship `{field}`",
                        model.id
                    ))
                })?;
            let target = models
                .get(relationship.target_model.as_str())
                .copied()
                .ok_or_else(|| {
                    ClientManifestError(format!(
                        "model `{}` client-visible row policy relationship `{field}` targets absent model `{}`",
                        model.id, relationship.target_model
                    ))
                })?;
            collect_row_policy_trusted_presets(predicate, target, models, descriptors)?;
        }
        FilterExpr::Cmp { .. } | FilterExpr::IsNull { .. } => {}
    }
    Ok(())
}

fn insert_row_policy_trusted_preset(
    model: &ClientModel,
    column: &str,
    name: &str,
    descriptors: &mut BTreeMap<String, String>,
) -> Result<(), ClientManifestError> {
    let field = model
        .fields
        .iter()
        .find(|field| field.name == column)
        .ok_or_else(|| {
            ClientManifestError(format!(
                "model `{}` client-visible row policy references absent field `{column}`",
                model.id
            ))
        })?;
    if matches!(field.codec.as_str(), "base64" | "json") {
        return Err(ClientManifestError(format!(
            "model `{}` row-policy claim `{name}` targets `{column}` with non-local codec `{}`",
            model.id, field.codec
        )));
    }
    insert_trusted_preset_descriptor(
        descriptors,
        &ClientTrustedPresetDescriptor {
            name: name.into(),
            codec: field.codec.clone(),
        },
        &format!("model `{}` row policy field `{column}`", model.id),
    )
}

fn insert_trusted_preset_descriptor(
    descriptors: &mut BTreeMap<String, String>,
    descriptor: &ClientTrustedPresetDescriptor,
    owner: &str,
) -> Result<(), ClientManifestError> {
    if descriptor.name.is_empty()
        || descriptor.name.len() > 128
        || descriptor.name.trim() != descriptor.name
        || descriptor.name.chars().any(char::is_control)
    {
        return Err(ClientManifestError(format!(
            "{owner} has an invalid trusted preset name"
        )));
    }
    match descriptors.entry(descriptor.name.clone()) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(descriptor.codec.clone());
        }
        std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &descriptor.codec => {
        }
        std::collections::btree_map::Entry::Occupied(entry) => {
            return Err(ClientManifestError(format!(
                "trusted preset `{}` uses incompatible codecs `{}` and `{}` across the selected client surface ({owner})",
                descriptor.name,
                entry.get(),
                descriptor.codec
            )));
        }
    }
    Ok(())
}
pub(super) fn validate_surface_structure(surface: &Surface) -> Result<(), ClientManifestError> {
    fn unique_nonempty<'a>(
        values: impl IntoIterator<Item = &'a str>,
        label: &str,
    ) -> Result<(), ClientManifestError> {
        let mut seen = BTreeSet::new();
        for value in values {
            if value.trim().is_empty() {
                return Err(ClientManifestError(format!("{label} id must not be empty")));
            }
            if !seen.insert(value) {
                return Err(ClientManifestError(format!(
                    "duplicate {label} id `{value}`"
                )));
            }
        }
        Ok(())
    }

    unique_nonempty(
        surface
            .models
            .values()
            .map(|model| model.model_name.as_str()),
        "model",
    )?;
    unique_nonempty(
        surface
            .models
            .values()
            .map(|model| model.table_name.as_str()),
        "model source table",
    )?;
    unique_nonempty(
        surface
            .models
            .values()
            .map(|model| model.object_name.as_str()),
        "model typename",
    )?;
    for (key, model) in &surface.models {
        if key != &model.model_name {
            return Err(ClientManifestError(format!(
                "surface model map key `{key}` does not match model id `{}`",
                model.model_name
            )));
        }
        unique_nonempty(
            model.columns.iter().map(|field| field.name.as_str()),
            "field",
        )?;
        unique_nonempty(
            model.relationships.iter().map(|field| field.name.as_str()),
            "relationship",
        )?;
        if let SurfaceRowPolicy::Predicate(predicate) = &model.row_policy {
            predicate
                .validate_row_policy_literals()
                .map_err(ClientManifestError)?;
            if !predicate.is_client_portable() {
                return Err(ClientManifestError(format!(
                    "model `{}` exposes a row policy with a JavaScript-unsafe integer; select it through surface_for_role so it becomes server-only",
                    model.model_name
                )));
            }
        }
        for relationship in &model.relationships {
            if !surface.models.contains_key(&relationship.target_model) {
                return Err(ClientManifestError(format!(
                    "model `{}` relationship `{}` targets absent model `{}`",
                    model.model_name, relationship.name, relationship.target_model
                )));
            }
            unique_nonempty(
                relationship.dependencies.iter().map(String::as_str),
                &format!(
                    "model `{}` relationship `{}` dependency",
                    model.model_name, relationship.name
                ),
            )?;
        }
    }
    unique_nonempty(
        surface.query_fields.iter().map(|root| root.name.as_str()),
        "query root",
    )?;
    unique_nonempty(
        surface
            .subscription_fields
            .iter()
            .map(|root| root.name.as_str()),
        "subscription root",
    )?;
    unique_nonempty(
        surface
            .commands
            .iter()
            .map(|command| command.command_name.as_str()),
        "command",
    )?;
    unique_nonempty(
        surface
            .commands
            .iter()
            .map(|command| command.field_name.as_str()),
        "command mutation field",
    )?;
    for command in &surface.commands {
        unique_nonempty(
            command.roles.iter().map(String::as_str),
            &format!("command `{}` role", command.command_name),
        )?;
    }
    unique_nonempty(
        surface
            .projectors
            .iter()
            .map(|projector| projector.name.as_str()),
        "projector",
    )?;
    for projector in &surface.projectors {
        unique_nonempty(
            projector.facts.iter().map(String::as_str),
            &format!("projector `{}` fact", projector.name),
        )?;
        unique_nonempty(
            projector.models.iter().map(String::as_str),
            &format!("projector `{}` model", projector.name),
        )?;
        for model in &projector.models {
            if !surface.models.contains_key(model) {
                return Err(ClientManifestError(format!(
                    "projector `{}` targets absent model `{model}`",
                    projector.name
                )));
            }
        }
    }
    Ok(())
}
