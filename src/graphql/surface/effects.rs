use super::*;

pub(in crate::graphql::surface) fn validate_effect_key(
    models: &BTreeMap<String, SurfaceModel>,
    command: &SurfaceCommand,
    model_name: &str,
    key: &EffectKey,
) -> Result<(), String> {
    let model = models.get(model_name).ok_or_else(|| {
        format!(
            "typed command `{}` effect references unknown model `{model_name}`",
            command.command_name
        )
    })?;
    let fields: Vec<&str> = key
        .fields
        .iter()
        .map(|field| field.field.as_str())
        .collect();
    if fields
        != model
            .primary_key
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
    {
        return Err(format!(
            "typed command `{}` effect key for `{model_name}` must exactly match ordered primary key ({})",
            command.command_name,
            model.primary_key.join(", ")
        ));
    }
    for field in &key.fields {
        let Some(column) = model
            .columns
            .iter()
            .find(|column| column.name == field.field)
        else {
            return Err(format!(
                "typed command `{}` effect key for `{model_name}` references primary-key field `{}` that is missing or hidden on the selected Surface",
                command.command_name, field.field
            ));
        };
        validate_effect_expression(command, &field.value, column)?;
    }
    Ok(())
}

pub(in crate::graphql::surface) fn validate_command_effects(
    models: &BTreeMap<String, SurfaceModel>,
    command: &SurfaceCommand,
) -> Result<(), String> {
    let Some(effects) = &command.effects else {
        return Ok(());
    };
    for operation in &effects.operations {
        validate_effect_operation(models, command, operation)?;
    }
    Ok(())
}

pub(in crate::graphql::surface) fn validate_effect_operation(
    models: &BTreeMap<String, SurfaceModel>,
    command: &SurfaceCommand,
    operation: &CommandEffect,
) -> Result<(), String> {
    let validate_addressable = |model_name: &str| -> Result<(), String> {
        let model = models.get(model_name).ok_or_else(|| {
            format!(
                "typed command `{}` effect references unknown model `{model_name}`",
                command.command_name
            )
        })?;
        if !model_has_client_normalized_identity(model) {
            return Err(format!(
                "typed command `{}` cannot use a key-addressed optimistic effect for embedded model `{model_name}`; the selected Surface requires a complete visible, supported, non-null, non-BigInt primary key",
                command.command_name
            ));
        }
        Ok(())
    };
    let validate_fields = |model_name: &str, fields: &[EffectFieldValue]| -> Result<(), String> {
        let model = models.get(model_name).ok_or_else(|| {
            format!(
                "typed command `{}` effect references unknown model `{model_name}`",
                command.command_name
            )
        })?;
        let mut seen = BTreeSet::new();
        for field in fields {
            if !seen.insert(&field.field) {
                return Err(format!(
                    "typed command `{}` effect repeats `{model_name}.{}`",
                    command.command_name, field.field
                ));
            }
            let Some(column) = model
                .columns
                .iter()
                .find(|candidate| candidate.name == field.field)
            else {
                return Err(format!(
                    "typed command `{}` effect references unknown field `{model_name}.{}`",
                    command.command_name, field.field
                ));
            };
            if model.primary_key.iter().any(|key| key == &field.field) {
                return Err(format!(
                    "typed command `{}` effect cannot assign primary-key field `{model_name}.{}`; upsert identity materializes from `key` and rekeying is unsupported",
                    command.command_name, field.field
                ));
            }
            validate_effect_expression(command, &field.value, column)?;
        }
        Ok(())
    };
    let validate_relationship = |relationship: &EffectRelationship| -> Result<(), String> {
        let source = models.get(&relationship.source_model).ok_or_else(|| {
            format!(
                "typed command `{}` effect references unknown model `{}`",
                command.command_name, relationship.source_model
            )
        })?;
        let declared = source
            .relationships
            .iter()
            .find(|candidate| candidate.name == relationship.field)
            .ok_or_else(|| {
                format!(
                    "typed command `{}` effect references unknown relationship `{}.{}`",
                    command.command_name, relationship.source_model, relationship.field
                )
            })?;
        if declared.target_model != relationship.target_model {
            return Err(format!(
                "typed command `{}` relationship `{}.{}` targets `{}`, not `{}`",
                command.command_name,
                relationship.source_model,
                relationship.field,
                declared.target_model,
                relationship.target_model
            ));
        }
        Ok(())
    };

    match operation {
        CommandEffect::Upsert { model, key, fields }
        | CommandEffect::Patch { model, key, fields } => {
            validate_addressable(model)?;
            validate_effect_key(models, command, model, key)?;
            validate_fields(model, fields)
        }
        CommandEffect::Delete { model, key } => {
            validate_addressable(model)?;
            validate_effect_key(models, command, model, key)
        }
        CommandEffect::Link {
            relationship,
            source,
            target,
        }
        | CommandEffect::Unlink {
            relationship,
            source,
            target,
        } => {
            validate_relationship(relationship)?;
            validate_addressable(&relationship.source_model)?;
            validate_addressable(&relationship.target_model)?;
            validate_effect_key(models, command, &relationship.source_model, source)?;
            validate_effect_key(models, command, &relationship.target_model, target)
        }
        CommandEffect::InvalidateModel { model } => {
            if !models.contains_key(model) {
                return Err(format!(
                    "typed command `{}` invalidates unknown model `{model}`",
                    command.command_name
                ));
            }
            Ok(())
        }
        CommandEffect::InvalidateRelationship {
            relationship,
            source,
        } => {
            validate_relationship(relationship)?;
            validate_addressable(&relationship.source_model)?;
            validate_effect_key(models, command, &relationship.source_model, source)
        }
    }
}

pub(in crate::graphql::surface) fn validate_effect_expression(
    command: &SurfaceCommand,
    expression: &EffectExpression,
    expected: &ColumnField,
) -> Result<(), String> {
    match expression {
        EffectExpression::Input { path } => {
            let SurfaceCommandShape::Typed(input) = &command.input else {
                return Err(format!(
                    "typed command `{}` effect uses input on an untyped command",
                    command.command_name
                ));
            };
            if path.is_empty() {
                return Err(format!(
                    "typed command `{}` effect input path must not be empty",
                    command.command_name
                ));
            }
            let mut definition = input;
            let mut inherited_nullable = false;
            let mut leaf = None;
            for (index, segment) in path.iter().enumerate() {
                let Some(field) = definition
                    .fields
                    .iter()
                    .find(|field| field.name == *segment)
                else {
                    return Err(format!(
                        "typed command `{}` effect references unknown input path `{}`",
                        command.command_name,
                        path.join(".")
                    ));
                };
                let last = index + 1 == path.len();
                if last {
                    leaf = Some(field);
                    break;
                }
                if field.list {
                    return Err(format!(
                        "typed command `{}` effect input path `{}` cannot descend through list field `{}`",
                        command.command_name,
                        path.join("."),
                        segment
                    ));
                }
                inherited_nullable |= field.nullable;
                let Some(nested) = field.nested.as_deref() else {
                    return Err(format!(
                        "typed command `{}` effect input path `{}` descends through scalar field `{}`",
                        command.command_name,
                        path.join("."),
                        segment
                    ));
                };
                definition = nested;
            }
            let field = leaf.expect("non-empty input paths always resolve a leaf or return");
            let json_container_leaf =
                expected.scalar == "JSON" && (field.list || field.nested.is_some());
            if !json_container_leaf && (field.list || field.type_name != expected.scalar) {
                return Err(format!(
                    "typed command `{}` effect input `{}` has GraphQL type `{}`, but model field `{}` requires `{}`",
                    command.command_name,
                    path.join("."),
                    field.type_name,
                    expected.name,
                    expected.scalar
                ));
            }
            if (inherited_nullable || field.nullable) && !expected.nullable {
                return Err(format!(
                    "typed command `{}` nullable effect input `{}` cannot populate non-null model field `{}`",
                    command.command_name,
                    path.join("."),
                    expected.name
                ));
            }
        }
        EffectExpression::TrustedPreset { name } => {
            if name.is_empty()
                || name.len() > 128
                || name.trim() != name
                || name.chars().any(char::is_control)
            {
                return Err(format!(
                    "typed command `{}` trusted preset name must be 1..=128 bytes, have no surrounding whitespace, and contain no control characters",
                    command.command_name
                ));
            }
        }
        EffectExpression::Constant { value } => {
            let compatible = constant_matches_scalar(value, expected);
            if !compatible {
                return Err(format!(
                    "typed command `{}` constant effect value is incompatible with model field `{}` (`{}`)",
                    command.command_name, expected.name, expected.scalar
                ));
            }
        }
        EffectExpression::Null => {
            if !expected.nullable {
                return Err(format!(
                    "typed command `{}` null effect value cannot populate non-null model field `{}`",
                    command.command_name, expected.name
                ));
            }
        }
        EffectExpression::InvalidConstant { error } => {
            return Err(format!(
                "typed command `{}` constant effect value failed to serialize: {error}",
                command.command_name
            ));
        }
    }
    Ok(())
}

pub(in crate::graphql::surface) fn constant_matches_scalar(
    value: &serde_json::Value,
    expected: &ColumnField,
) -> bool {
    use base64::Engine as _;

    if expected.scalar == "JSON" {
        return true;
    }
    // `serde_json` represents non-finite floats as JSON null. SQL null has a
    // separate typed IR variant, so a constant null is invalid for every
    // non-JSON scalar even when the target column is nullable.
    if value.is_null() {
        return false;
    }
    match (expected.scalar.as_str(), value) {
        ("Boolean", serde_json::Value::Bool(_)) => true,
        ("BigInt", serde_json::Value::Number(number)) => number.is_i64() || number.is_u64(),
        ("Int", serde_json::Value::Number(number)) => {
            number
                .as_i64()
                .is_some_and(|value| i32::try_from(value).is_ok())
                || number
                    .as_u64()
                    .is_some_and(|value| i32::try_from(value).is_ok())
        }
        ("Float", serde_json::Value::Number(_)) => true,
        ("String" | "ID", serde_json::Value::String(_)) => true,
        ("Timestamptz", serde_json::Value::String(value)) => is_rfc3339_timestamp(value),
        ("Bytea", serde_json::Value::String(value)) => base64::engine::general_purpose::STANDARD
            .decode(value)
            .is_ok(),
        _ => false,
    }
}

/// Small dependency-free RFC 3339 validator for deterministic manifest
/// constants. Runtime database decoding remains dialect-owned.
pub(in crate::graphql::surface) fn is_rfc3339_timestamp(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() < 20
        || bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || !matches!(bytes.get(10), Some(b'T' | b't'))
        || bytes.get(13) != Some(&b':')
        || bytes.get(16) != Some(&b':')
    {
        return false;
    }
    let digits = |range: std::ops::Range<usize>| -> Option<u32> {
        std::str::from_utf8(bytes.get(range)?).ok()?.parse().ok()
    };
    let (Some(year), Some(month), Some(day), Some(hour), Some(minute), Some(second)) = (
        digits(0..4),
        digits(5..7),
        digits(8..10),
        digits(11..13),
        digits(14..16),
        digits(17..19),
    ) else {
        return false;
    };
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return false,
    };
    if day == 0 || day > max_day || hour > 23 || minute > 59 || second > 60 {
        return false;
    }

    let mut cursor = 19;
    if bytes.get(cursor) == Some(&b'.') {
        cursor += 1;
        let fraction_start = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
        }
        if cursor == fraction_start {
            return false;
        }
    }
    match bytes.get(cursor) {
        Some(b'Z' | b'z') => cursor + 1 == bytes.len(),
        Some(b'+' | b'-') => {
            if cursor + 6 != bytes.len() || bytes.get(cursor + 3) != Some(&b':') {
                return false;
            }
            let offset_hour = std::str::from_utf8(&bytes[cursor + 1..cursor + 3])
                .ok()
                .and_then(|value| value.parse::<u32>().ok());
            let offset_minute = std::str::from_utf8(&bytes[cursor + 4..cursor + 6])
                .ok()
                .and_then(|value| value.parse::<u32>().ok());
            matches!((offset_hour, offset_minute), (Some(0..=23), Some(0..=59)))
        }
        _ => false,
    }
}

pub(in crate::graphql::surface) fn sanitize_command_effects_for_models(
    commands: &mut [SurfaceCommand],
    models: &BTreeMap<String, SurfaceModel>,
) {
    for command in commands {
        let Some(effects) = &command.effects else {
            continue;
        };
        if effects
            .operations
            .iter()
            .any(|operation| !effect_operation_visible(operation, models))
        {
            // Keep the command, but erase the entire optimistic plan. Partial
            // optimism is harder to reason about than conservative revalidation
            // and could leak denied model/field/preset names.
            command.effects = Some(CommandEffects::revalidate());
        }
    }
}

pub(in crate::graphql::surface) fn sanitize_command_confirmations(
    commands: &mut [SurfaceCommand],
    projectors: &[SurfaceProjectionOwner],
    models: &BTreeMap<String, SurfaceModel>,
) {
    for command in commands {
        let all_visible = command.confirmations.iter().all(|confirmation| {
            models.contains_key(&confirmation.model)
                && projectors.iter().any(|projector| {
                    projector.name == confirmation.projector
                        && projector
                            .models
                            .iter()
                            .any(|model| model == &confirmation.model)
                })
                && confirmation.key.fields.iter().all(|field| {
                    models.get(&confirmation.model).is_some_and(|model| {
                        model
                            .columns
                            .iter()
                            .any(|column| column.name == field.field)
                    }) && effect_expression_visible(&field.value)
                })
                && confirmation
                    .partition
                    .as_ref()
                    .is_none_or(effect_expression_visible)
        });
        if !all_visible {
            command.confirmations.clear();
            command.confirmation_unavailable = true;
            // Optimism and causal confirmation must be authorized as one plan.
            // Keeping either a subset of edges or a partial optimistic write
            // could disclose hidden topology and produce a state the server
            // never promised to confirm.
            command.effects = Some(CommandEffects::revalidate());
        }
    }
}

pub(in crate::graphql::surface) fn effect_operation_visible(
    operation: &CommandEffect,
    models: &BTreeMap<String, SurfaceModel>,
) -> bool {
    let key_visible = |model_name: &str, key: &EffectKey| {
        models.get(model_name).is_some_and(|model| {
            key.fields.iter().all(|field| {
                model
                    .columns
                    .iter()
                    .any(|column| column.name == field.field)
                    && effect_expression_visible(&field.value)
            })
        })
    };
    let fields_visible = |model_name: &str, fields: &[EffectFieldValue]| {
        models.get(model_name).is_some_and(|model| {
            fields.iter().all(|field| {
                model
                    .columns
                    .iter()
                    .any(|column| column.name == field.field)
                    && effect_expression_visible(&field.value)
            })
        })
    };
    let relationship_visible = |relationship: &EffectRelationship| {
        models.get(&relationship.source_model).is_some_and(|model| {
            model.relationships.iter().any(|candidate| {
                candidate.name == relationship.field
                    && candidate.target_model == relationship.target_model
                    && models.contains_key(&relationship.target_model)
            })
        })
    };
    match operation {
        CommandEffect::Upsert { model, key, fields }
        | CommandEffect::Patch { model, key, fields } => {
            key_visible(model, key) && fields_visible(model, fields)
        }
        CommandEffect::Delete { model, key } => key_visible(model, key),
        CommandEffect::Link {
            relationship,
            source,
            target,
        }
        | CommandEffect::Unlink {
            relationship,
            source,
            target,
        } => {
            relationship_visible(relationship)
                && key_visible(&relationship.source_model, source)
                && key_visible(&relationship.target_model, target)
        }
        CommandEffect::InvalidateModel { model } => models.contains_key(model),
        CommandEffect::InvalidateRelationship {
            relationship,
            source,
        } => relationship_visible(relationship) && key_visible(&relationship.source_model, source),
    }
}

pub(in crate::graphql::surface) fn effect_expression_visible(
    expression: &EffectExpression,
) -> bool {
    // A selected client surface may expose the descriptor name, but never its
    // value. Runtime values are read from the verified Session and travel only
    // in the cache-scope-bound protocol envelope.
    !matches!(expression, EffectExpression::InvalidConstant { .. })
}

pub(in crate::graphql::surface) fn reject_occupied_command_types(
    definition: &SurfaceTypeDef,
    occupied_types: &BTreeSet<String>,
) -> Result<(), String> {
    if occupied_types.contains(&definition.name) {
        return Err(format!(
            "command type `{}` collides with a Surface GraphQL type",
            definition.name
        ));
    }
    for field in &definition.fields {
        if let Some(nested) = &field.nested {
            reject_occupied_command_types(nested, occupied_types)?;
        }
    }
    Ok(())
}

pub(in crate::graphql::surface) fn canonicalize_type_def(
    definition: &mut SurfaceTypeDef,
) -> Result<(), String> {
    if !is_valid_graphql_name(&definition.name) {
        return Err(format!(
            "command type `{}` is not a valid GraphQL name",
            definition.name
        ));
    }
    if definition.fields.is_empty() {
        return Err(format!(
            "command type `{}` must declare at least one field",
            definition.name
        ));
    }
    let mut fields = BTreeSet::new();
    for field in &mut definition.fields {
        if !is_valid_graphql_name(&field.name) {
            return Err(format!(
                "command type `{}` field `{}` is not a valid GraphQL name",
                definition.name, field.name
            ));
        }
        if !fields.insert(field.name.clone()) {
            return Err(format!(
                "command type `{}` declares duplicate field `{}`",
                definition.name, field.name
            ));
        }
        if !field.list && field.item_nullable {
            return Err(format!(
                "command type `{}` field `{}` marks non-list items nullable",
                definition.name, field.name
            ));
        }
        if field.unsigned_integer.is_some()
            && (field.type_name != "BigInt" || field.nested.is_some())
        {
            return Err(format!(
                "command type `{}` field `{}` has an invalid unsigned refinement",
                definition.name, field.name
            ));
        }
        if let Some(nested) = &mut field.nested {
            canonicalize_type_def(nested)?;
            if field.type_name != nested.name {
                return Err(format!(
                    "command type `{}` field `{}` names `{}` but embeds `{}`",
                    definition.name, field.name, field.type_name, nested.name
                ));
            }
        } else if !is_command_scalar(&field.type_name) {
            return Err(format!(
                "command type `{}` field `{}` references unknown type `{}` without a structural definition",
                definition.name, field.name, field.type_name
            ));
        }
    }
    definition.fields.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(())
}

pub(in crate::graphql::surface) fn register_type_def(
    definition: &SurfaceTypeDef,
    input: bool,
    type_defs: &mut BTreeMap<String, (bool, SurfaceTypeDef)>,
) -> Result<(), String> {
    if let Some((existing_input, existing)) = type_defs.get(&definition.name) {
        if *existing_input != input || existing != definition {
            return Err(format!(
                "ambiguous duplicate command type id `{}`",
                definition.name
            ));
        }
    } else {
        type_defs.insert(definition.name.clone(), (input, definition.clone()));
    }
    for field in &definition.fields {
        if let Some(nested) = &field.nested {
            register_type_def(nested, input, type_defs)?;
        }
    }
    Ok(())
}

pub(in crate::graphql::surface) fn is_command_scalar(name: &str) -> bool {
    matches!(name, "Boolean" | "Float" | "ID" | "Int" | "String") || CUSTOM_SCALARS.contains(&name)
}

pub(in crate::graphql::surface) fn filter_is_surface_visible(
    predicate: &FilterExpr,
    model_name: &str,
    models: &BTreeMap<String, SurfaceModel>,
) -> bool {
    let Some(model) = models.get(model_name) else {
        return false;
    };
    match predicate {
        FilterExpr::And(items) | FilterExpr::Or(items) => items
            .iter()
            .all(|item| filter_is_surface_visible(item, model_name, models)),
        FilterExpr::Not(item) => filter_is_surface_visible(item, model_name, models),
        FilterExpr::Cmp { column, rhs, .. } => model
            .columns
            .iter()
            .find(|field| field.name == *column)
            .is_some_and(|field| policy_operand_is_client_typed(rhs, &field.scalar)),
        FilterExpr::In { column, values, .. } => model
            .columns
            .iter()
            .find(|field| field.name == *column)
            .is_some_and(|field| {
                values
                    .iter()
                    .all(|value| policy_operand_is_client_typed(value, &field.scalar))
            }),
        FilterExpr::IsNull { column, .. } => {
            model.columns.iter().any(|field| field.name == *column)
        }
        FilterExpr::Rel { field, predicate } => model
            .relationships
            .iter()
            .find(|relationship| relationship.name == *field)
            .is_some_and(|relationship| {
                matches!(
                    relationship.keys,
                    SurfaceRelationshipKeys::Direct { .. }
                        | SurfaceRelationshipKeys::Through { .. }
                ) && filter_is_surface_visible(predicate, &relationship.target_model, models)
            }),
    }
}

pub(in crate::graphql::surface) fn policy_operand_is_client_typed(
    operand: &Operand,
    scalar: &str,
) -> bool {
    !matches!(operand, Operand::Claim(_))
        || matches!(
            scalar,
            "BigInt" | "Boolean" | "Float" | "ID" | "Int" | "String" | "Timestamptz"
        )
}
