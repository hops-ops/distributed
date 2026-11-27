use super::*;

pub(super) fn validate_execution_limits(
    root: &CompiledRoot,
    kind: ClientRootKind,
    execution: &ClientExecutionLimits,
    document: &ClientDocument,
    position: Pos,
) -> Result<(), ClientCompileError> {
    let depth = compiled_object_depth(&root.selection, 1);
    if depth > execution.max_depth {
        return Err(source_error(
            "client.operation.depth_limit",
            format!(
                "compiled operation depth {depth} exceeds the selected service max_depth {}",
                execution.max_depth
            ),
            document,
            position,
        ));
    }

    let rows = compiled_root_rows(root, kind);
    if rows > execution.max_rows {
        return Err(source_error(
            "client.operation.row_limit",
            format!(
                "compiled operation may return {rows} rows, exceeding the selected service max_rows {}",
                execution.max_rows
            ),
            document,
            position,
        ));
    }
    Ok(())
}

pub(super) fn compiled_object_depth(selection: &CompiledObject, parent_depth: u64) -> u64 {
    selection
        .members
        .iter()
        .map(|member| {
            let field_depth = parent_depth.saturating_add(1);
            match member {
                CompiledMember::Scalar(_) => field_depth,
                CompiledMember::Branch(branch) => {
                    field_depth.max(compiled_object_depth(&branch.selection, field_depth))
                }
            }
        })
        .max()
        .unwrap_or(parent_depth)
}

/// The service's row cost ([`row_cost`]) of a compiled root: every list
/// counts its page size, so nested lists cost about the product of their
/// limits.
///
/// A literal `limit` counts as given (capped by the field's `max_limit`); an
/// absent one, or a variable whose value is unknown until run time, counts as
/// the field's `default_limit`. The service checks the actual values again
/// for every request, so this build-time check refuses only what the defaults
/// already put over budget.
pub(super) fn compiled_root_rows(root: &CompiledRoot, kind: ClientRootKind) -> u64 {
    let limit = page_limit(&root.arguments, root.coverage.as_ref());
    match kind {
        ClientRootKind::List => row_cost::page(limit, compiled_object_rows(&root.selection, limit)),
        ClientRootKind::ByPk => row_cost::one(compiled_object_rows(&root.selection, limit)),
        ClientRootKind::Aggregate => compiled_object_rows(&root.selection, limit),
    }
}

/// Rows nested under one object. `nodes_limit` is the page size of an
/// aggregate's `nodes`, which take their arguments from the aggregate field.
fn compiled_object_rows(selection: &CompiledObject, nodes_limit: u64) -> u64 {
    selection
        .members
        .iter()
        .map(|member| match member {
            CompiledMember::Scalar(_) => 0,
            CompiledMember::Branch(branch) => {
                let limit = page_limit(&branch.arguments, branch.coverage.as_ref());
                match branch.semantic {
                    CompiledBranchSemantic::Relationship => {
                        let nested = compiled_object_rows(&branch.selection, limit);
                        match branch.cardinality {
                            Cardinality::Many => row_cost::page(limit, nested),
                            Cardinality::One => row_cost::one(nested),
                        }
                    }
                    CompiledBranchSemantic::Aggregate => {
                        compiled_object_rows(&branch.selection, limit)
                    }
                    CompiledBranchSemantic::AggregateFields => 0,
                    CompiledBranchSemantic::AggregateNodes => row_cost::page(
                        nodes_limit,
                        compiled_object_rows(&branch.selection, nodes_limit),
                    ),
                }
            }
        })
        .fold(0, u64::saturating_add)
}

fn page_limit(
    arguments: &BTreeMap<String, CompiledArgument>,
    coverage: Option<&CompiledCoverage>,
) -> u64 {
    let Some(coverage) = coverage else {
        return 1;
    };
    let default = coverage.default_limit.unwrap_or(1);
    let literal = coverage
        .limit_argument
        .as_ref()
        .and_then(|name| arguments.get(name))
        .and_then(|argument| match argument {
            CompiledArgument::Literal { value, .. } => value.as_u64(),
            _ => None,
        });
    let limit = literal.unwrap_or(default);
    coverage.max_limit.map_or(limit, |max| limit.min(max))
}

pub(super) fn filter_depth_from_selection(selection_depth: usize, root_offset: usize) -> u64 {
    u64::try_from(selection_depth.saturating_sub(root_offset))
        .expect("compiler selection depth fits in u64")
}

#[allow(clippy::too_many_arguments)]
pub(super) fn validate_filter_source(
    source: &CompiledArgument,
    model: &ClientModel,
    input: &ClientFilterInput,
    expected_type: &str,
    variables: &[CompiledVariable],
    manifest: &ClientManifest,
    execution: &ClientExecutionLimits,
    document: &ClientDocument,
    position: Pos,
    depth: u64,
    constraints: &mut BTreeMap<String, VariableUseConstraint>,
) -> Result<(), ClientCompileError> {
    validate_filter_depth(model, execution, depth, document, position)?;
    match source {
        CompiledArgument::Literal { value, .. } => validate_filter_literal(
            value, model, input, manifest, execution, document, position, depth,
        ),
        CompiledArgument::Variable(name) => {
            validate_nested_variable(name, expected_type, variables, document, position)?;
            constrain_variable(constraints, name, VariableUseConstraint::filter(depth));
            Ok(())
        }
        CompiledArgument::List(_) => Err(source_error(
            "client.filter.object_required",
            format!("filter for model `{}` must be an object or null", model.id),
            document,
            position,
        )),
        CompiledArgument::Object(values) => {
            for (name, value) in values {
                match name.as_str() {
                    "_and" | "_or" => match value {
                        CompiledArgument::List(items) => {
                            validate_filter_width(
                                name,
                                items.len(),
                                execution.max_bool_width,
                                document,
                                position,
                            )?;
                            for item in items {
                                if let CompiledArgument::Variable(variable) = item {
                                    validate_nested_variable(
                                        variable,
                                        &format!("{}!", input.type_name),
                                        variables,
                                        document,
                                        position,
                                    )?;
                                    constrain_variable(
                                        constraints,
                                        variable,
                                        VariableUseConstraint::filter(depth.saturating_add(1)),
                                    );
                                } else {
                                    validate_filter_source(
                                        item,
                                        model,
                                        input,
                                        &input.type_name,
                                        variables,
                                        manifest,
                                        execution,
                                        document,
                                        position,
                                        depth.saturating_add(1),
                                        constraints,
                                    )?;
                                }
                            }
                        }
                        CompiledArgument::Variable(variable) => {
                            validate_nested_variable(
                                variable,
                                &format!("[{}!]", input.type_name),
                                variables,
                                document,
                                position,
                            )?;
                            constrain_variable(
                                constraints,
                                variable,
                                VariableUseConstraint::list(
                                    execution.max_bool_width,
                                    Some(VariableUseConstraint::filter(depth.saturating_add(1))),
                                ),
                            );
                        }
                        CompiledArgument::Literal { value, .. } => {
                            validate_filter_literal(
                                &json_singleton(name, value.clone()),
                                model,
                                input,
                                manifest,
                                execution,
                                document,
                                position,
                                depth,
                            )?;
                        }
                        CompiledArgument::Object(_) => {
                            return Err(source_error(
                                "client.filter.boolean_list",
                                format!("filter operator `{name}` must contain a list of objects"),
                                document,
                                position,
                            ));
                        }
                    },
                    "_not" => validate_filter_source(
                        value,
                        model,
                        input,
                        &input.type_name,
                        variables,
                        manifest,
                        execution,
                        document,
                        position,
                        depth.saturating_add(1),
                        constraints,
                    )?,
                    field_name => {
                        if let Some(filter_field) =
                            input.fields.iter().find(|field| field.name == field_name)
                        {
                            let field = model.field(field_name).ok_or_else(|| {
                                ClientCompileError::manifest(
                                    "client.manifest.filter_field",
                                    format!(
                                        "filter plan for model `{}` references absent field `{field_name}`",
                                        model.id
                                    ),
                                )
                            })?;
                            validate_filter_comparison_source(
                                value,
                                model,
                                field,
                                filter_field,
                                variables,
                                execution,
                                document,
                                position,
                                constraints,
                            )?;
                        } else if let Some(relationship_input) = input
                            .relationships
                            .iter()
                            .find(|relationship| relationship.field == field_name)
                        {
                            let relationship = model.relationship(field_name).ok_or_else(|| {
                                ClientCompileError::manifest(
                                    "client.manifest.filter_relationship",
                                    format!(
                                        "filter plan for model `{}` references absent relationship `{field_name}`",
                                        model.id
                                    ),
                                )
                            })?;
                            let target = manifest.models.get(&relationship.target_model).ok_or_else(|| {
                                ClientCompileError::manifest(
                                    "client.manifest.filter_relationship",
                                    format!(
                                        "filter relationship `{}.{field_name}` has an absent target",
                                        model.id
                                    ),
                                )
                            })?;
                            validate_filter_source(
                                value,
                                target,
                                &target.filter_input,
                                &relationship_input.target_type,
                                variables,
                                manifest,
                                execution,
                                document,
                                position,
                                depth.saturating_add(1),
                                constraints,
                            )?;
                        } else {
                            return Err(source_error(
                                "client.filter.field_denied_or_unknown",
                                format!(
                                    "filter field `{field_name}` is absent from selected model `{}`",
                                    model.id
                                ),
                                document,
                                position,
                            ));
                        }
                    }
                }
            }
            Ok(())
        }
    }
}

pub(super) fn validate_filter_depth(
    model: &ClientModel,
    execution: &ClientExecutionLimits,
    depth: u64,
    document: &ClientDocument,
    position: Pos,
) -> Result<(), ClientCompileError> {
    if depth > MAX_OBJECT_DEPTH as u64 {
        return Err(source_error(
            "client.filter.safety_depth",
            format!(
                "filter for model `{}` exceeds the compiler safety depth {MAX_OBJECT_DEPTH}",
                model.id
            ),
            document,
            position,
        ));
    }
    if depth > execution.max_depth {
        return Err(source_error(
            "client.filter.depth_limit",
            format!(
                "filter for model `{}` reaches semantic depth {depth}, exceeding max_depth {}",
                model.id, execution.max_depth
            ),
            document,
            position,
        ));
    }
    Ok(())
}

pub(super) fn validate_filter_width(
    operator: &str,
    actual: usize,
    maximum: u64,
    document: &ClientDocument,
    position: Pos,
) -> Result<(), ClientCompileError> {
    let actual = u64::try_from(actual).unwrap_or(u64::MAX);
    if actual <= maximum {
        return Ok(());
    }
    Err(source_error(
        "client.filter.width_limit",
        format!(
            "filter operator `{operator}` contains {actual} items, exceeding its limit {maximum}"
        ),
        document,
        position,
    ))
}

pub(super) fn constrain_variable(
    constraints: &mut BTreeMap<String, VariableUseConstraint>,
    name: &str,
    constraint: VariableUseConstraint,
) {
    constraints
        .entry(name.to_string())
        .and_modify(|existing| existing.intersect(&constraint))
        .or_insert(constraint);
}

#[allow(clippy::too_many_arguments)]
pub(super) fn validate_filter_comparison_source(
    source: &CompiledArgument,
    model: &ClientModel,
    field: &ClientField,
    semantics: &ClientFilterField,
    variables: &[CompiledVariable],
    execution: &ClientExecutionLimits,
    document: &ClientDocument,
    position: Pos,
    constraints: &mut BTreeMap<String, VariableUseConstraint>,
) -> Result<(), ClientCompileError> {
    if let CompiledArgument::Literal { value, .. } = source {
        let operators = value.as_object().ok_or_else(|| {
            source_error(
                "client.filter.comparison_object",
                format!(
                    "filter field `{}.{}` must contain a comparison object",
                    model.id, field.name
                ),
                document,
                position,
            )
        })?;
        for (operator, operand) in operators {
            if !semantics
                .operators
                .iter()
                .any(|allowed| allowed == operator)
            {
                return Err(source_error(
                    "client.filter.operator_denied_or_unknown",
                    format!(
                        "filter operator `{operator}` is absent from selected field `{}.{}`",
                        model.id, field.name
                    ),
                    document,
                    position,
                ));
            }
            match operator.as_str() {
                "_in" | "_nin" => {
                    let items = operand.as_array().ok_or_else(|| {
                        source_error(
                            "client.filter.list_required",
                            format!("filter operator `{operator}` requires a list"),
                            document,
                            position,
                        )
                    })?;
                    validate_filter_width(
                        operator,
                        items.len(),
                        execution.max_in_list,
                        document,
                        position,
                    )?;
                    for item in items {
                        validate_filter_scalar_literal(
                            item, model, field, false, document, position,
                        )?;
                    }
                }
                "_is_null" if !operand.is_boolean() && !operand.is_null() => {
                    return Err(source_error(
                        "client.filter.boolean_required",
                        "filter operator `_is_null` requires a boolean or null",
                        document,
                        position,
                    ));
                }
                "_is_null" => {}
                "_has_key" => validate_filter_typed_literal(
                    operand, model, field, "String", "string", true, document, position,
                )?,
                _ => {
                    validate_filter_scalar_literal(operand, model, field, true, document, position)?
                }
            }
        }
        return Ok(());
    }
    let CompiledArgument::Object(operators) = source else {
        return Err(source_error(
            "client.filter.comparison_object",
            format!(
                "filter field `{}.{}` must contain a comparison object",
                model.id, field.name
            ),
            document,
            position,
        ));
    };
    for (operator, operand) in operators {
        if !semantics
            .operators
            .iter()
            .any(|allowed| allowed == operator)
        {
            return Err(source_error(
                "client.filter.operator_denied_or_unknown",
                format!(
                    "filter operator `{operator}` is absent from selected field `{}.{}`",
                    model.id, field.name
                ),
                document,
                position,
            ));
        }
        match (operator.as_str(), operand) {
            ("_in" | "_nin", CompiledArgument::Variable(variable)) => {
                validate_nested_variable(
                    variable,
                    &format!("[{}!]", field.scalar),
                    variables,
                    document,
                    position,
                )?;
                constrain_variable(
                    constraints,
                    variable,
                    VariableUseConstraint::list(execution.max_in_list, None),
                );
            }
            ("_in" | "_nin", CompiledArgument::List(items)) => {
                validate_filter_width(
                    operator,
                    items.len(),
                    execution.max_in_list,
                    document,
                    position,
                )?;
                for item in items {
                    match item {
                        CompiledArgument::Variable(variable) => validate_nested_variable(
                            variable,
                            &format!("{}!", field.scalar),
                            variables,
                            document,
                            position,
                        )?,
                        item => validate_compiled_filter_literal(
                            item, model, field, false, document, position,
                        )?,
                    }
                }
            }
            (
                "_in" | "_nin",
                CompiledArgument::Literal {
                    value: JsonValue::Array(items),
                    ..
                },
            ) => {
                validate_filter_width(
                    operator,
                    items.len(),
                    execution.max_in_list,
                    document,
                    position,
                )?;
                for item in items {
                    validate_filter_scalar_literal(item, model, field, false, document, position)?;
                }
            }
            ("_in" | "_nin", _) => {
                return Err(source_error(
                    "client.filter.list_required",
                    format!("filter operator `{operator}` requires a list"),
                    document,
                    position,
                ));
            }
            ("_is_null", CompiledArgument::Variable(variable)) => {
                validate_nested_variable(variable, "Boolean", variables, document, position)?;
            }
            (
                "_is_null",
                CompiledArgument::Literal {
                    value: JsonValue::Bool(_) | JsonValue::Null,
                    ..
                },
            ) => {}
            ("_is_null", _) => {
                return Err(source_error(
                    "client.filter.boolean_required",
                    "filter operator `_is_null` requires a boolean or null",
                    document,
                    position,
                ));
            }
            ("_has_key", CompiledArgument::Variable(variable)) => {
                validate_nested_variable(variable, "String", variables, document, position)?;
            }
            ("_has_key", operand) => validate_compiled_filter_typed_literal(
                operand, model, field, "String", "string", true, document, position,
            )?,
            (_, CompiledArgument::Variable(variable)) => {
                validate_nested_variable(variable, &field.scalar, variables, document, position)?;
            }
            (_, operand) => {
                validate_compiled_filter_literal(operand, model, field, true, document, position)?
            }
        }
    }
    Ok(())
}

pub(super) fn validate_compiled_filter_literal(
    source: &CompiledArgument,
    model: &ClientModel,
    field: &ClientField,
    nullable: bool,
    document: &ClientDocument,
    position: Pos,
) -> Result<(), ClientCompileError> {
    validate_compiled_filter_typed_literal(
        source,
        model,
        field,
        &field.scalar,
        &field.codec,
        nullable,
        document,
        position,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn validate_compiled_filter_typed_literal(
    source: &CompiledArgument,
    model: &ClientModel,
    field: &ClientField,
    scalar: &str,
    codec: &str,
    nullable: bool,
    document: &ClientDocument,
    position: Pos,
) -> Result<(), ClientCompileError> {
    let Some(value) = compiled_literal_json(source) else {
        return Err(source_error(
            "client.filter.operand",
            format!(
                "filter operand on `{}.{}` cannot contain a nested variable",
                model.id, field.name
            ),
            document,
            position,
        ));
    };
    validate_filter_typed_literal(
        &value, model, field, scalar, codec, nullable, document, position,
    )
}

pub(super) fn compiled_literal_json(source: &CompiledArgument) -> Option<JsonValue> {
    match source {
        CompiledArgument::Literal { value, .. } => Some(value.clone()),
        CompiledArgument::Variable(_) => None,
        CompiledArgument::List(items) => items
            .iter()
            .map(compiled_literal_json)
            .collect::<Option<Vec<_>>>()
            .map(JsonValue::Array),
        CompiledArgument::Object(fields) => fields
            .iter()
            .map(|(name, value)| Some((name.clone(), compiled_literal_json(value)?)))
            .collect::<Option<JsonMap<String, JsonValue>>>()
            .map(JsonValue::Object),
    }
}

pub(super) fn validate_filter_scalar_literal(
    value: &JsonValue,
    model: &ClientModel,
    field: &ClientField,
    nullable: bool,
    document: &ClientDocument,
    position: Pos,
) -> Result<(), ClientCompileError> {
    validate_filter_typed_literal(
        value,
        model,
        field,
        &field.scalar,
        &field.codec,
        nullable,
        document,
        position,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn validate_filter_typed_literal(
    value: &JsonValue,
    model: &ClientModel,
    field: &ClientField,
    scalar: &str,
    codec: &str,
    nullable: bool,
    document: &ClientDocument,
    position: Pos,
) -> Result<(), ClientCompileError> {
    if scalar_json_literal_matches(value, scalar, codec, nullable) {
        return Ok(());
    }
    Err(source_error(
        "client.filter.literal_type",
        format!(
            "filter literal for `{}.{}` does not match scalar `{}` / codec `{}`{}",
            model.id,
            field.name,
            scalar,
            codec,
            if nullable { " or null" } else { "" }
        ),
        document,
        position,
    ))
}

pub(super) fn scalar_json_literal_matches(
    value: &JsonValue,
    scalar: &str,
    codec: &str,
    nullable: bool,
) -> bool {
    match (scalar, codec, value) {
        (_, _, JsonValue::Null) => nullable,
        ("ID" | "String", "string", JsonValue::String(_))
        | ("Timestamptz", "string_unvalidated_timestamp", JsonValue::String(_))
        | ("Boolean", "boolean", JsonValue::Bool(_)) => true,
        ("Bytea", "base64", JsonValue::String(value)) => canonical_standard_base64(value).is_some(),
        ("Int", "int32", JsonValue::Number(number)) => {
            number
                .as_i64()
                .is_some_and(|number| i32::try_from(number).is_ok())
                || number
                    .as_u64()
                    .is_some_and(|number| i32::try_from(number).is_ok())
        }
        ("Float", "float64", JsonValue::Number(number)) => {
            number.as_f64().is_some_and(f64::is_finite)
        }
        ("BigInt", "json_number_precision_limited", JsonValue::Number(number)) => {
            json_number_is_safe_integer(number)
        }
        ("JSON", "json", value) => json_value_roundtrips_javascript(value),
        _ => false,
    }
}

pub(super) fn json_value_roundtrips_javascript(value: &JsonValue) -> bool {
    match value {
        JsonValue::Number(number) => json_number_roundtrips_javascript(number),
        JsonValue::Array(values) => values.iter().all(json_value_roundtrips_javascript),
        JsonValue::Object(values) => values.values().all(json_value_roundtrips_javascript),
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::String(_) => true,
    }
}

pub(super) fn json_number_roundtrips_javascript(number: &serde_json::Number) -> bool {
    let Some(value) = number.as_f64().filter(|value| value.is_finite()) else {
        return false;
    };
    value.fract() != 0.0 || value.abs() <= 9_007_199_254_740_991.0
}

pub(super) fn json_number_is_safe_integer(number: &serde_json::Number) -> bool {
    const JS_MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
    json_number_is_negative_zero(number)
        || number
            .as_i64()
            .is_some_and(|number| number.unsigned_abs() <= JS_MAX_SAFE_INTEGER)
        || number
            .as_u64()
            .is_some_and(|number| number <= JS_MAX_SAFE_INTEGER)
}

pub(super) fn json_number_is_negative_zero(number: &serde_json::Number) -> bool {
    number
        .as_f64()
        .is_some_and(|number| number == 0.0 && number.is_sign_negative())
}

pub(super) fn canonical_standard_base64(value: &str) -> Option<String> {
    base64::engine::general_purpose::STANDARD
        .decode(value.as_bytes())
        .ok()
        .map(|bytes| base64::engine::general_purpose::STANDARD.encode(bytes))
}

pub(super) fn validate_nested_variable(
    name: &str,
    expected: &str,
    variables: &[CompiledVariable],
    document: &ClientDocument,
    position: Pos,
) -> Result<(), ClientCompileError> {
    let variable = variables
        .iter()
        .find(|variable| variable.name == name)
        .expect("nested variable existence checked while compiling arguments");
    let expected_type = Type::new(expected).ok_or_else(|| {
        ClientCompileError::manifest(
            "client.manifest.argument_type",
            format!("invalid nested variable target type `{expected}`"),
        )
    })?;
    if variable_type_compatible(&variable.graphql_type, &expected_type) {
        return Ok(());
    }
    Err(source_error(
        "client.variable.type_mismatch",
        format!(
            "nested variable `${name}` has type `{}`; selected filter/order position requires `{expected}`",
            variable.graphql_type
        ),
        document,
        position,
    ))
}

pub(super) fn json_singleton(name: &str, value: JsonValue) -> JsonValue {
    let mut object = JsonMap::new();
    object.insert(name.to_string(), value);
    JsonValue::Object(object)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn validate_filter_literal(
    value: &JsonValue,
    model: &ClientModel,
    input: &ClientFilterInput,
    manifest: &ClientManifest,
    execution: &ClientExecutionLimits,
    document: &ClientDocument,
    position: Pos,
    depth: u64,
) -> Result<(), ClientCompileError> {
    validate_filter_depth(model, execution, depth, document, position)?;
    if value.is_null() {
        return Ok(());
    }
    let object = value.as_object().ok_or_else(|| {
        source_error(
            "client.filter.object_required",
            format!("filter for model `{}` must be an object or null", model.id),
            document,
            position,
        )
    })?;
    for (name, value) in object {
        match name.as_str() {
            "_and" | "_or" => {
                if value.is_null() {
                    continue;
                }
                let items = value.as_array().ok_or_else(|| {
                    source_error(
                        "client.filter.boolean_list",
                        format!("filter operator `{name}` must contain a list of objects"),
                        document,
                        position,
                    )
                })?;
                validate_filter_width(
                    name,
                    items.len(),
                    execution.max_bool_width,
                    document,
                    position,
                )?;
                for item in items {
                    validate_filter_literal(
                        item,
                        model,
                        input,
                        manifest,
                        execution,
                        document,
                        position,
                        depth.saturating_add(1),
                    )?;
                }
            }
            "_not" => validate_filter_literal(
                value,
                model,
                input,
                manifest,
                execution,
                document,
                position,
                depth.saturating_add(1),
            )?,
            field_name => {
                if let Some(field) = input.fields.iter().find(|field| field.name == field_name) {
                    let operators = value.as_object().ok_or_else(|| {
                        source_error(
                            "client.filter.comparison_object",
                            format!(
                                "filter field `{}.{field_name}` must contain a comparison object",
                                model.id
                            ),
                            document,
                            position,
                        )
                    })?;
                    for (operator, operand) in operators {
                        if !field.operators.iter().any(|allowed| allowed == operator) {
                            return Err(source_error(
                                "client.filter.operator_denied_or_unknown",
                                format!(
                                    "filter operator `{operator}` is absent from selected field `{}.{field_name}`",
                                    model.id
                                ),
                                document,
                                position,
                            ));
                        }
                        let model_field = model.field(field_name).ok_or_else(|| {
                            ClientCompileError::manifest(
                                "client.manifest.filter_field",
                                format!(
                                    "filter plan for model `{}` references absent field `{field_name}`",
                                    model.id
                                ),
                            )
                        })?;
                        match operator.as_str() {
                            "_in" | "_nin" => {
                                let items = operand.as_array().ok_or_else(|| {
                                    source_error(
                                        "client.filter.list_required",
                                        format!("filter operator `{operator}` requires a list"),
                                        document,
                                        position,
                                    )
                                })?;
                                validate_filter_width(
                                    operator,
                                    items.len(),
                                    execution.max_in_list,
                                    document,
                                    position,
                                )?;
                                for item in items {
                                    validate_filter_scalar_literal(
                                        item,
                                        model,
                                        model_field,
                                        false,
                                        document,
                                        position,
                                    )?;
                                }
                            }
                            "_is_null" if !operand.is_boolean() && !operand.is_null() => {
                                return Err(source_error(
                                    "client.filter.boolean_required",
                                    "filter operator `_is_null` requires a boolean or null",
                                    document,
                                    position,
                                ));
                            }
                            "_is_null" => {}
                            _ => validate_filter_scalar_literal(
                                operand,
                                model,
                                model_field,
                                true,
                                document,
                                position,
                            )?,
                        }
                    }
                } else if let Some(relationship_input) = input
                    .relationships
                    .iter()
                    .find(|relationship| relationship.field == field_name)
                {
                    let relationship = model.relationship(field_name).ok_or_else(|| {
                        ClientCompileError::manifest(
                            "client.manifest.filter_relationship",
                            format!(
                                "filter plan for model `{}` references absent relationship `{field_name}`",
                                model.id
                            ),
                        )
                    })?;
                    let target =
                        manifest
                            .models
                            .get(&relationship.target_model)
                            .ok_or_else(|| {
                                ClientCompileError::manifest(
                                    "client.manifest.filter_relationship",
                                    format!(
                                "filter relationship `{}.{field_name}` has an absent target",
                                model.id
                            ),
                                )
                            })?;
                    if relationship_input.target_type != target.filter_input.type_name {
                        return Err(ClientCompileError::manifest(
                            "client.manifest.filter_relationship",
                            format!(
                                "filter relationship `{}.{field_name}` targets input `{}` but model `{}` declares `{}`",
                                model.id,
                                relationship_input.target_type,
                                target.id,
                                target.filter_input.type_name
                            ),
                        ));
                    }
                    validate_filter_literal(
                        value,
                        target,
                        &target.filter_input,
                        manifest,
                        execution,
                        document,
                        position,
                        depth.saturating_add(1),
                    )?;
                } else {
                    return Err(source_error(
                        "client.filter.field_denied_or_unknown",
                        format!(
                            "filter field `{field_name}` is absent from selected model `{}`",
                            model.id
                        ),
                        document,
                        position,
                    ));
                }
            }
        }
    }
    Ok(())
}

pub(super) fn validate_order_literal(
    value: &JsonValue,
    semantics: &ClientOrderSemantics,
    document: &ClientDocument,
    position: Pos,
) -> Result<(), ClientCompileError> {
    if value.is_null() {
        return Ok(());
    }
    let entries = value.as_array().ok_or_else(|| {
        source_error(
            "client.order.list_required",
            "order_by must be a list or null",
            document,
            position,
        )
    })?;
    for entry in entries {
        let object = entry.as_object().ok_or_else(|| {
            source_error(
                "client.order.object_required",
                "each order_by entry must be an object",
                document,
                position,
            )
        })?;
        if object.len() != 1 {
            return Err(source_error(
                "client.order.ambiguous",
                "each order_by entry must contain exactly one field to declare priority",
                document,
                position,
            ));
        }
        let (field, direction) = object.iter().next().expect("length checked");
        if !semantics.fields.iter().any(|allowed| allowed == field) {
            return Err(source_error(
                "client.order.field_denied_or_unknown",
                format!("order_by field `{field}` is absent from the selected model"),
                document,
                position,
            ));
        }
        let direction = direction.as_str().ok_or_else(|| {
            source_error(
                "client.order.direction",
                format!("order_by field `{field}` must use a declared direction enum"),
                document,
                position,
            )
        })?;
        if !semantics.values.iter().any(|allowed| allowed == direction) {
            return Err(source_error(
                "client.order.direction",
                format!("order_by direction `{direction}` is absent from the selected manifest"),
                document,
                position,
            ));
        }
    }
    Ok(())
}

pub(super) fn validate_order_source(
    source: &CompiledArgument,
    semantics: &ClientOrderSemantics,
    argument: Option<&ClientArgument>,
    variables: &[CompiledVariable],
    document: &ClientDocument,
    position: Pos,
) -> Result<(), ClientCompileError> {
    match source {
        CompiledArgument::Literal { value, .. } => {
            validate_order_literal(value, semantics, document, position)
        }
        CompiledArgument::Variable(name) => {
            let expected = argument.map(ClientArgument::graphql_type).ok_or_else(|| {
                ClientCompileError::manifest(
                    "client.manifest.order_argument",
                    "order semantics exist without an order argument",
                )
            })?;
            validate_nested_variable(name, &expected, variables, document, position)
        }
        CompiledArgument::List(entries) => {
            for entry in entries {
                match entry {
                    CompiledArgument::Variable(name) => {
                        let item_type = argument
                            .map(|argument| argument.type_name.as_str())
                            .ok_or_else(|| {
                                ClientCompileError::manifest(
                                    "client.manifest.order_argument",
                                    "order semantics exist without an order argument",
                                )
                            })?;
                        validate_nested_variable(
                            name,
                            &format!("{item_type}!"),
                            variables,
                            document,
                            position,
                        )?;
                    }
                    CompiledArgument::Object(fields) => validate_order_entry_source(
                        fields, semantics, variables, document, position,
                    )?,
                    CompiledArgument::Literal { value, .. } => {
                        validate_order_literal(
                            &JsonValue::Array(vec![value.clone()]),
                            semantics,
                            document,
                            position,
                        )?;
                    }
                    CompiledArgument::List(_) => {
                        return Err(source_error(
                            "client.order.object_required",
                            "each order_by entry must be an object",
                            document,
                            position,
                        ));
                    }
                }
            }
            Ok(())
        }
        CompiledArgument::Object(_) => Err(source_error(
            "client.order.list_required",
            "order_by must be a list or null",
            document,
            position,
        )),
    }
}

pub(super) fn validate_order_entry_source(
    fields: &BTreeMap<String, CompiledArgument>,
    semantics: &ClientOrderSemantics,
    variables: &[CompiledVariable],
    document: &ClientDocument,
    position: Pos,
) -> Result<(), ClientCompileError> {
    if fields.len() != 1 {
        return Err(source_error(
            "client.order.ambiguous",
            "each order_by entry must contain exactly one field to declare priority",
            document,
            position,
        ));
    }
    let (field, direction) = fields.iter().next().expect("length checked");
    if !semantics.fields.iter().any(|allowed| allowed == field) {
        return Err(source_error(
            "client.order.field_denied_or_unknown",
            format!("order_by field `{field}` is absent from the selected model"),
            document,
            position,
        ));
    }
    match direction {
        CompiledArgument::Variable(name) => {
            validate_nested_variable(name, "order_by", variables, document, position)
        }
        CompiledArgument::Literal { value, .. } => {
            let direction = value.as_str().ok_or_else(|| {
                source_error(
                    "client.order.direction",
                    format!("order_by field `{field}` must use a declared direction enum"),
                    document,
                    position,
                )
            })?;
            if semantics.values.iter().any(|allowed| allowed == direction) {
                Ok(())
            } else {
                Err(source_error(
                    "client.order.direction",
                    format!(
                        "order_by direction `{direction}` is absent from the selected manifest"
                    ),
                    document,
                    position,
                ))
            }
        }
        CompiledArgument::List(_) | CompiledArgument::Object(_) => Err(source_error(
            "client.order.direction",
            format!("order_by field `{field}` must use a declared direction enum"),
            document,
            position,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(members: Vec<CompiledMember>) -> CompiledObject {
        CompiledObject {
            typename: "todo".into(),
            storage: CompiledStorage::Embedded,
            members,
        }
    }

    fn branch(
        semantic: CompiledBranchSemantic,
        cardinality: Cardinality,
        limit: Option<u64>,
        selection: CompiledObject,
    ) -> CompiledMember {
        let mut arguments = BTreeMap::new();
        if let Some(limit) = limit {
            arguments.insert(
                "limit".to_owned(),
                CompiledArgument::Literal {
                    value: JsonValue::from(limit),
                    wire: limit.to_string(),
                },
            );
        }
        CompiledMember::Branch(Box::new(CompiledBranch {
            semantic,
            response_key: "items".into(),
            field: "items".into(),
            cardinality,
            nullable: false,
            arguments,
            dependencies: Vec::new(),
            coverage: Some(CompiledCoverage {
                kind: "offset".into(),
                offset_argument: None,
                limit_argument: Some("limit".into()),
                default_limit: Some(100),
                max_limit: Some(1000),
            }),
            filter: None,
            order: None,
            pagination: None,
            relationship: None,
            selection,
        }))
    }

    /// The compiler's arithmetic is the service's: nested lists multiply,
    /// one-to-one relationships add one row, aggregates count their nodes.
    #[test]
    fn row_cost_matches_the_service() {
        let many = |limit, selection| {
            branch(
                CompiledBranchSemantic::Relationship,
                Cardinality::Many,
                limit,
                selection,
            )
        };
        // orders(limit: 1000) { items(limit: 1000) { id } }
        let nested = object(vec![many(Some(1000), object(Vec::new()))]);
        assert_eq!(compiled_object_rows(&nested, 1), 1000);
        assert_eq!(
            row_cost::page(1000, compiled_object_rows(&nested, 1)),
            1_001_000
        );
        // An absent limit is the default page.
        assert_eq!(
            compiled_object_rows(&object(vec![many(None, object(Vec::new()))]), 1),
            100
        );
        // A belongs-to costs one row plus what it carries.
        let one = branch(
            CompiledBranchSemantic::Relationship,
            Cardinality::One,
            None,
            object(Vec::new()),
        );
        assert_eq!(compiled_object_rows(&object(vec![one]), 1), 1);
        // items_aggregate(limit: 7) { aggregate { count } nodes { id } }
        let aggregate = branch(
            CompiledBranchSemantic::Aggregate,
            Cardinality::One,
            Some(7),
            object(vec![
                branch(
                    CompiledBranchSemantic::AggregateFields,
                    Cardinality::One,
                    None,
                    object(Vec::new()),
                ),
                branch(
                    CompiledBranchSemantic::AggregateNodes,
                    Cardinality::Many,
                    None,
                    object(Vec::new()),
                ),
            ]),
        );
        assert_eq!(compiled_object_rows(&object(vec![aggregate]), 1), 7);
    }
}
