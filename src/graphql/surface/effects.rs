use super::*;

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
