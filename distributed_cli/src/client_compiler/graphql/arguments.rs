use super::*;

pub(super) fn single_root_field<'a>(
    mut fields: Vec<MergedField<'a>>,
    operation: &OperationDefinition,
    document: &ClientDocument,
) -> Result<MergedField<'a>, ClientCompileError> {
    if fields.len() != 1 {
        return Err(source_error(
            "client.operation.single_root",
            format!(
                "causal protocol v1 requires exactly one query root; found {}",
                fields.len()
            ),
            document,
            operation.selection_set.pos,
        ));
    }
    Ok(fields.pop().expect("length checked"))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn compile_arguments(
    field: &Field,
    owner: &str,
    allowed_arguments: &[ClientArgument],
    model: &ClientModel,
    manifest: &ClientManifest,
    variables: &[CompiledVariable],
    used_variables: &mut BTreeSet<String>,
    document: &ClientDocument,
) -> Result<BTreeMap<String, CompiledArgument>, ClientCompileError> {
    let manifest_arguments = allowed_arguments
        .iter()
        .map(|argument| (argument.name.as_str(), argument))
        .collect::<BTreeMap<_, _>>();
    let variables = variables
        .iter()
        .map(|variable| (variable.name.as_str(), variable))
        .collect::<BTreeMap<_, _>>();
    let mut result = BTreeMap::new();
    for (name, value) in &field.arguments {
        let name_string = name.node.as_str();
        let Some(manifest_argument) = manifest_arguments.get(name_string) else {
            return Err(source_error(
                "client.argument.denied_or_unknown",
                format!("argument `{name_string}` is absent from selected field `{owner}`",),
                document,
                name.pos,
            ));
        };
        if result.contains_key(name_string) {
            return Err(source_error(
                "client.argument.duplicate",
                format!("root argument `{name_string}` appears more than once"),
                document,
                name.pos,
            ));
        }
        let compiled = match &value.node {
            Value::Variable(variable) => {
                let Some(definition) = variables.get(variable.as_str()) else {
                    return Err(source_error(
                        "client.variable.undefined",
                        format!(
                            "argument `{name_string}` references undefined variable `${variable}`"
                        ),
                        document,
                        value.pos,
                    ));
                };
                let expected_type =
                    Type::new(&manifest_argument.graphql_type()).expect("manifest type validated");
                if !variable_type_compatible(&definition.graphql_type, &expected_type) {
                    let actual_type = definition.graphql_type.to_string();
                    let expected_type = expected_type.to_string();
                    return Err(source_error(
                        "client.variable.type_mismatch",
                        format!(
                            "variable `${variable}` used for `{name_string}` has type `{actual_type}`; selected manifest requires `{expected_type}`"
                        ),
                        document,
                        value.pos,
                    ));
                }
                used_variables.insert(variable.to_string());
                CompiledArgument::Variable(variable.to_string())
            }
            literal => {
                let literal =
                    canonicalize_argument_literal(literal, manifest_argument, model, manifest);
                validate_literal(&literal, manifest_argument, document, value.pos)?;
                compile_argument_source(
                    &literal,
                    name_string,
                    &variables,
                    used_variables,
                    document,
                    value.pos,
                )?
            }
        };
        result.insert(name_string.to_string(), compiled);
    }
    for argument in allowed_arguments {
        if !argument.nullable && !result.contains_key(&argument.name) {
            return Err(source_error(
                "client.argument.required",
                format!(
                    "field `{owner}` requires argument `{}` of type `{}`",
                    argument.name,
                    argument.graphql_type()
                ),
                document,
                field.name.pos,
            ));
        }
    }
    Ok(result)
}

pub(super) fn validate_used_variables(
    variables: &[CompiledVariable],
    used: &BTreeSet<String>,
    document: &ClientDocument,
    position: Pos,
) -> Result<(), ClientCompileError> {
    if let Some(variable) = variables
        .iter()
        .find(|variable| !used.contains(&variable.name))
    {
        return Err(source_error(
            "client.variable.unused",
            format!(
                "variable `${}` is defined but is not used by the compiled operation",
                variable.name,
            ),
            document,
            position,
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn reject_directives(
    directives: &[Positioned<Directive>],
    owner: &str,
    document: &ClientDocument,
) -> Result<(), ClientCompileError> {
    let Some(directive) = directives.first() else {
        return Ok(());
    };
    let name = directive.node.name.node.as_str();
    let (code, message) = if matches!(name, "skip" | "include") {
        (
            "client.directive.conditional_unsupported",
            format!(
                "conditional directive `@{name}` on {owner} requires a field-presence plan and is not supported yet"
            ),
        )
    } else {
        (
            "client.directive.unsupported",
            format!("directive `@{name}` on {owner} is not supported"),
        )
    };
    Err(source_error(code, message, document, directive.pos))
}

pub(super) fn compile_argument_source(
    value: &Value,
    argument: &str,
    variables: &BTreeMap<&str, &CompiledVariable>,
    used_variables: &mut BTreeSet<String>,
    document: &ClientDocument,
    position: Pos,
) -> Result<CompiledArgument, ClientCompileError> {
    if !contains_variable(value) {
        return Ok(CompiledArgument::Literal {
            value: value_to_json(value, document, position)?,
            wire: render_value(value, document, position)?,
        });
    }
    match value {
        Value::Variable(variable) => {
            if !variables.contains_key(variable.as_str()) {
                return Err(source_error(
                    "client.variable.undefined",
                    format!(
                        "argument `{argument}` references undefined nested variable `${variable}`"
                    ),
                    document,
                    position,
                ));
            }
            used_variables.insert(variable.to_string());
            Ok(CompiledArgument::Variable(variable.to_string()))
        }
        Value::List(values) => values
            .iter()
            .map(|value| {
                compile_argument_source(
                    value,
                    argument,
                    variables,
                    used_variables,
                    document,
                    position,
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .map(CompiledArgument::List),
        Value::Object(values) => values
            .iter()
            .map(|(name, value)| {
                Ok((
                    name.to_string(),
                    compile_argument_source(
                        value,
                        argument,
                        variables,
                        used_variables,
                        document,
                        position,
                    )?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, ClientCompileError>>()
            .map(CompiledArgument::Object),
        _ => unreachable!("non-container value with no variable returned above"),
    }
}

pub(super) fn contains_variable(value: &Value) -> bool {
    match value {
        Value::Variable(_) => true,
        Value::List(values) => values.iter().any(contains_variable),
        Value::Object(values) => values.values().any(contains_variable),
        _ => false,
    }
}

pub(super) fn canonicalize_argument_literal(
    value: &Value,
    argument: &ClientArgument,
    model: &ClientModel,
    manifest: &ClientManifest,
) -> Value {
    let value = if argument.kind == ClientArgumentKind::Filter {
        canonicalize_filter_literal(value, model, &model.filter_input, manifest)
    } else {
        value.clone()
    };
    let value =
        if argument.list && !matches!(&value, Value::List(_) | Value::Null | Value::Variable(_)) {
            Value::List(vec![value])
        } else {
            value
        };
    match &argument.codec {
        Some(codec) if argument.list => match value {
            Value::List(items) => Value::List(
                items
                    .iter()
                    .map(|item| canonicalize_scalar_literal(item, &argument.type_name, codec))
                    .collect(),
            ),
            value => value,
        },
        Some(codec) => canonicalize_scalar_literal(&value, &argument.type_name, codec),
        None => value,
    }
}

pub(super) fn canonicalize_filter_literal(
    value: &Value,
    model: &ClientModel,
    input: &ClientFilterInput,
    manifest: &ClientManifest,
) -> Value {
    let Value::Object(fields) = value else {
        return value.clone();
    };
    let mut canonical = fields.clone();
    for (name, value) in fields {
        let value = match name.as_str() {
            "_and" | "_or" => canonicalize_filter_literal_list(value, model, input, manifest),
            "_not" => canonicalize_filter_literal(value, model, input, manifest),
            field_name if input.fields.iter().any(|field| field.name == field_name) => model
                .field(field_name)
                .map(|field| canonicalize_filter_comparison_literal(value, field))
                .unwrap_or_else(|| value.clone()),
            relationship_name
                if input
                    .relationships
                    .iter()
                    .any(|relationship| relationship.field == relationship_name) =>
            {
                model
                    .relationship(relationship_name)
                    .and_then(|relationship| manifest.models.get(&relationship.target_model))
                    .map(|target| {
                        canonicalize_filter_literal(value, target, &target.filter_input, manifest)
                    })
                    .unwrap_or_else(|| value.clone())
            }
            _ => value.clone(),
        };
        canonical.insert(name.clone(), value);
    }
    Value::Object(canonical)
}

pub(super) fn canonicalize_filter_literal_list(
    value: &Value,
    model: &ClientModel,
    input: &ClientFilterInput,
    manifest: &ClientManifest,
) -> Value {
    match value {
        Value::List(items) => Value::List(
            items
                .iter()
                .map(|item| canonicalize_filter_literal(item, model, input, manifest))
                .collect(),
        ),
        Value::Null | Value::Variable(_) => value.clone(),
        value => Value::List(vec![canonicalize_filter_literal(
            value, model, input, manifest,
        )]),
    }
}

pub(super) fn canonicalize_filter_comparison_literal(value: &Value, field: &ClientField) -> Value {
    let Value::Object(operators) = value else {
        return value.clone();
    };
    let mut canonical = operators.clone();
    for (operator, operand) in operators {
        let operand = match operator.as_str() {
            "_in" | "_nin" => {
                let operand =
                    if matches!(operand, Value::List(_) | Value::Null | Value::Variable(_)) {
                        operand.clone()
                    } else {
                        Value::List(vec![operand.clone()])
                    };
                match operand {
                    Value::List(items) => Value::List(
                        items
                            .iter()
                            .map(|item| {
                                canonicalize_scalar_literal(item, &field.scalar, &field.codec)
                            })
                            .collect(),
                    ),
                    operand => operand,
                }
            }
            "_is_null" | "_has_key" => operand.clone(),
            _ => canonicalize_scalar_literal(operand, &field.scalar, &field.codec),
        };
        canonical.insert(operator.clone(), operand);
    }
    Value::Object(canonical)
}

pub(super) fn canonicalize_scalar_literal(value: &Value, scalar: &str, codec: &str) -> Value {
    match (scalar, codec, value) {
        ("ID", "string", Value::Number(number)) if json_number_is_negative_zero(number) => {
            Value::String("0".into())
        }
        ("ID", "string", Value::Number(number)) if json_number_is_safe_integer(number) => {
            Value::String(number.to_string())
        }
        ("Bytea", "base64", Value::String(value)) => canonical_standard_base64(value)
            .map(Value::String)
            .unwrap_or_else(|| Value::String(value.clone())),
        ("Float", "float64", Value::Number(number)) => canonicalize_float_number(number)
            .map(Value::Number)
            .unwrap_or_else(|| Value::Number(number.clone())),
        ("Int", "int32", Value::Number(number))
        | ("BigInt", "json_number_precision_limited", Value::Number(number))
            if json_number_is_negative_zero(number) =>
        {
            Value::Number(serde_json::Number::from(0))
        }
        ("JSON", "json", value) => canonicalize_json_literal(value),
        _ => value.clone(),
    }
}

pub(super) fn canonicalize_json_literal(value: &Value) -> Value {
    match value {
        Value::Number(number) => canonicalize_json_number(number)
            .map(Value::Number)
            .unwrap_or_else(|| Value::Number(number.clone())),
        Value::List(values) => Value::List(values.iter().map(canonicalize_json_literal).collect()),
        Value::Object(values) => Value::Object(
            values
                .iter()
                .map(|(name, value)| (name.clone(), canonicalize_json_literal(value)))
                .collect(),
        ),
        _ => value.clone(),
    }
}

pub(super) fn canonicalize_json_number(number: &serde_json::Number) -> Option<serde_json::Number> {
    if json_number_is_negative_zero(number) {
        return Some(serde_json::Number::from(0));
    }
    if number.as_i64().is_some() || number.as_u64().is_some() {
        return Some(number.clone());
    }
    let value = number.as_f64().filter(|value| value.is_finite())?;
    if value.fract() == 0.0 {
        if value.abs() > 9_007_199_254_740_991.0 {
            return None;
        }
        return if value.is_sign_negative() {
            Some(serde_json::Number::from(value as i64))
        } else {
            Some(serde_json::Number::from(value as u64))
        };
    }
    serde_json::Number::from_f64(value)
}

pub(super) fn canonicalize_float_number(number: &serde_json::Number) -> Option<serde_json::Number> {
    let value = number.as_f64()?;
    if value == 0.0 {
        Some(serde_json::Number::from(0))
    } else {
        serde_json::Number::from_f64(value)
    }
}

pub(super) fn validate_literal(
    value: &Value,
    argument: &ClientArgument,
    document: &ClientDocument,
    position: Pos,
) -> Result<(), ClientCompileError> {
    if matches!(value, Value::Null) {
        if argument.nullable {
            return Ok(());
        }
        return Err(source_error(
            "client.argument.null",
            format!(
                "argument `{}` is non-null in the selected manifest",
                argument.name
            ),
            document,
            position,
        ));
    }
    if argument.list && !matches!(value, Value::List(_)) {
        return Err(source_error(
            "client.argument.list_literal",
            format!(
                "argument `{}` requires an explicit list literal or a variable of type `{}`",
                argument.name,
                argument.graphql_type()
            ),
            document,
            position,
        ));
    }
    if let Some(codec) = &argument.codec {
        let valid = if argument.list {
            let Value::List(items) = value else {
                unreachable!("list shape checked above")
            };
            items.iter().all(|item| {
                value_to_json(item, document, position).is_ok_and(|item| {
                    scalar_json_literal_matches(&item, &argument.type_name, codec, false)
                })
            })
        } else {
            value_to_json(value, document, position).is_ok_and(|value| {
                scalar_json_literal_matches(&value, &argument.type_name, codec, false)
            })
        };
        return if valid {
            Ok(())
        } else {
            Err(source_error(
                "client.argument.literal_type",
                format!(
                    "literal for argument `{}` does not match scalar `{}` / codec `{}`",
                    argument.name, argument.type_name, codec
                ),
                document,
                position,
            ))
        };
    }
    if argument.list {
        return Ok(());
    }
    let valid = match argument.type_name.as_str() {
        "Boolean" => matches!(value, Value::Boolean(_)),
        "Int" => matches!(value, Value::Number(number) if number.is_i64() || number.is_u64()),
        "Float" | "BigInt" => matches!(value, Value::Number(_)),
        "ID" | "String" | "Bytea" | "Timestamptz" => matches!(value, Value::String(_)),
        "JSON" => !matches!(value, Value::Binary(_) | Value::Variable(_)),
        _ => matches!(value, Value::Object(_) | Value::Enum(_) | Value::List(_)),
    };
    if valid {
        Ok(())
    } else {
        Err(source_error(
            "client.argument.literal_type",
            format!(
                "literal for argument `{}` does not match manifest type `{}`",
                argument.name,
                argument.graphql_type()
            ),
            document,
            position,
        ))
    }
}

pub(super) fn value_to_json(
    value: &Value,
    document: &ClientDocument,
    position: Pos,
) -> Result<JsonValue, ClientCompileError> {
    match value {
        Value::Variable(variable) => Err(source_error(
            "client.argument.nested_variable",
            format!("nested variable `${variable}` is not a literal"),
            document,
            position,
        )),
        Value::Null => Ok(JsonValue::Null),
        Value::Number(value) => Ok(JsonValue::Number(value.clone())),
        Value::String(value) => Ok(JsonValue::String(value.clone())),
        Value::Boolean(value) => Ok(JsonValue::Bool(*value)),
        Value::Binary(_) => Err(source_error(
            "client.literal.binary",
            "binary GraphQL literals are not portable to the JavaScript replica",
            document,
            position,
        )),
        Value::Enum(value) => Ok(JsonValue::String(value.to_string())),
        Value::List(values) => Ok(JsonValue::Array(
            values
                .iter()
                .map(|value| value_to_json(value, document, position))
                .collect::<Result<_, _>>()?,
        )),
        Value::Object(values) => {
            let mut object = JsonMap::new();
            let mut values = values.iter().collect::<Vec<_>>();
            values.sort_by(|left, right| left.0.cmp(right.0));
            for (name, value) in values {
                object.insert(name.to_string(), value_to_json(value, document, position)?);
            }
            Ok(JsonValue::Object(object))
        }
    }
}
