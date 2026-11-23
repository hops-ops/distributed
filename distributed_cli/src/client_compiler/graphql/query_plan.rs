use super::*;

pub(super) fn inject_wire_fields(
    members: &mut Vec<CompiledMember>,
    model: &ClientModel,
    document: &ClientDocument,
    position: Pos,
) -> Result<(), ClientCompileError> {
    let identity = model
        .identity()
        .expect("embedded model rejected before injection");
    let mut response_keys = members
        .iter()
        .map(|member| match member {
            CompiledMember::Scalar(scalar) => scalar.response_key.clone(),
            CompiledMember::Branch(branch) => branch.response_key.clone(),
        })
        .collect::<BTreeSet<_>>();
    for identity_field in identity {
        if members.iter().any(|member| match member {
            CompiledMember::Scalar(scalar) => scalar.field == identity_field.name,
            CompiledMember::Branch(_) => false,
        }) {
            continue;
        }
        let field = model.field(&identity_field.name).ok_or_else(|| {
            source_error(
                "client.selection.identity_denied",
                format!(
                    "normalized identity `{}` is not selectable on model `{}`",
                    identity_field.name, model.id
                ),
                document,
                position,
            )
        })?;
        let response_key = allocate_wire_alias(&identity_field.name, &mut response_keys);
        members.push(CompiledMember::Scalar(compiled_scalar(
            &response_key,
            field,
            false,
        )));
    }
    if !members.iter().any(|member| match member {
        CompiledMember::Scalar(scalar) => scalar.field == "__typename",
        CompiledMember::Branch(_) => false,
    }) {
        let response_key = allocate_wire_alias("typename", &mut response_keys);
        members.push(CompiledMember::Scalar(CompiledScalar {
            response_key,
            field: "__typename".into(),
            codec: "string".into(),
            nullable: false,
            expose: false,
        }));
    }
    Ok(())
}

pub(super) fn query_plan_field_dependencies(
    model: &ClientModel,
    filter: Option<&ClientFilterSemantics>,
    order: Option<&ClientOrderSemantics>,
    declared_arguments: &[ClientArgument],
    compiled_arguments: &BTreeMap<String, CompiledArgument>,
) -> BTreeSet<String> {
    let mut fields = BTreeSet::new();
    let mut relationships = BTreeSet::new();
    if let Some(filter) = filter {
        if let ClientRowPolicy::Predicate { expression } = &filter.row_policy {
            collect_policy_fields(expression, &mut fields, &mut relationships);
        }
        if let Some(argument) = declared_arguments
            .iter()
            .find(|argument| argument.kind == ClientArgumentKind::Filter)
            .and_then(|argument| compiled_arguments.get(&argument.name))
        {
            collect_filter_source_fields(argument, filter, &mut fields, &mut relationships);
        }
    }
    if let Some(order) = order {
        if let Some(argument) = declared_arguments
            .iter()
            .find(|argument| argument.kind == ClientArgumentKind::Order)
            .and_then(|argument| compiled_arguments.get(&argument.name))
        {
            collect_order_source_fields(argument, order, &mut fields);
        }
    }
    for relationship_name in relationships {
        if let Some(relationship) = model.relationship(&relationship_name) {
            let (local, _) = relationship_key_fields(&relationship.key_mapping);
            fields.extend(local.iter().cloned());
        }
    }
    fields
}

pub(super) fn collect_filter_source_fields(
    source: &CompiledArgument,
    semantics: &ClientFilterSemantics,
    fields: &mut BTreeSet<String>,
    relationships: &mut BTreeSet<String>,
) {
    match source {
        CompiledArgument::Variable(_) => {
            fields.extend(semantics.fields.iter().map(|field| field.name.clone()));
            relationships.extend(semantics.relationships.iter().cloned());
        }
        CompiledArgument::Literal { value, .. } => {
            collect_client_filter_fields(value, semantics, fields, relationships);
        }
        CompiledArgument::List(items) => {
            for item in items {
                collect_filter_source_fields(item, semantics, fields, relationships);
            }
        }
        CompiledArgument::Object(values) => {
            for (name, value) in values {
                match name.as_str() {
                    "_and" | "_or" | "_not" => {
                        collect_filter_source_fields(value, semantics, fields, relationships);
                    }
                    field
                        if semantics
                            .fields
                            .iter()
                            .any(|candidate| candidate.name == field) =>
                    {
                        fields.insert(field.to_string());
                    }
                    relationship
                        if semantics
                            .relationships
                            .iter()
                            .any(|candidate| candidate == relationship) =>
                    {
                        relationships.insert(relationship.to_string());
                    }
                    _ => {}
                }
            }
        }
    }
}

pub(super) fn collect_order_source_fields(
    source: &CompiledArgument,
    semantics: &ClientOrderSemantics,
    fields: &mut BTreeSet<String>,
) {
    match source {
        CompiledArgument::Variable(_) => fields.extend(semantics.fields.iter().cloned()),
        CompiledArgument::Literal { value, .. } => {
            if let Some(entries) = value.as_array() {
                for entry in entries {
                    if let Some(object) = entry.as_object() {
                        fields.extend(object.keys().cloned());
                    }
                }
            }
        }
        CompiledArgument::List(items) => {
            for item in items {
                collect_order_source_fields(item, semantics, fields);
            }
        }
        CompiledArgument::Object(values) => fields.extend(values.keys().cloned()),
    }
}

pub(super) fn collect_policy_fields(
    expression: &FilterExpr,
    fields: &mut BTreeSet<String>,
    relationships: &mut BTreeSet<String>,
) {
    match expression {
        FilterExpr::And(expressions) | FilterExpr::Or(expressions) => {
            for expression in expressions {
                collect_policy_fields(expression, fields, relationships);
            }
        }
        FilterExpr::Not(expression) => collect_policy_fields(expression, fields, relationships),
        FilterExpr::Cmp { column, .. }
        | FilterExpr::In { column, .. }
        | FilterExpr::IsNull { column, .. } => {
            fields.insert(column.clone());
        }
        FilterExpr::Rel { field, .. } => {
            relationships.insert(field.clone());
        }
    }
}

pub(super) fn collect_client_filter_fields(
    value: &JsonValue,
    semantics: &ClientFilterSemantics,
    fields: &mut BTreeSet<String>,
    relationships: &mut BTreeSet<String>,
) {
    let Some(object) = value.as_object() else {
        return;
    };
    for (name, value) in object {
        match name.as_str() {
            "_and" | "_or" => {
                if let Some(items) = value.as_array() {
                    for item in items {
                        collect_client_filter_fields(item, semantics, fields, relationships);
                    }
                }
            }
            "_not" => collect_client_filter_fields(value, semantics, fields, relationships),
            field
                if semantics
                    .fields
                    .iter()
                    .any(|candidate| candidate.name == field) =>
            {
                fields.insert(field.to_string());
            }
            relationship
                if semantics
                    .relationships
                    .iter()
                    .any(|candidate| candidate == relationship) =>
            {
                relationships.insert(relationship.to_string());
            }
            _ => {}
        }
    }
}

pub(super) fn inject_dependency_fields(
    selection: &mut CompiledObject,
    model: &ClientModel,
    dependencies: &BTreeSet<String>,
    document: &ClientDocument,
    position: Pos,
) -> Result<(), ClientCompileError> {
    let mut response_keys = selection
        .members
        .iter()
        .map(|member| match member {
            CompiledMember::Scalar(scalar) => scalar.response_key.clone(),
            CompiledMember::Branch(branch) => branch.response_key.clone(),
        })
        .collect::<BTreeSet<_>>();
    for dependency in dependencies {
        if selection.members.iter().any(|member| match member {
            CompiledMember::Scalar(scalar) => scalar.field == *dependency,
            CompiledMember::Branch(_) => false,
        }) {
            continue;
        }
        let field = model.field(dependency).ok_or_else(|| {
            source_error(
                "client.selection.dependency_denied",
                format!(
                    "query-index dependency `{dependency}` is not selectable on model `{}`",
                    model.id
                ),
                document,
                position,
            )
        })?;
        let response_key = allocate_wire_alias(dependency, &mut response_keys);
        selection
            .members
            .push(CompiledMember::Scalar(compiled_scalar(
                &response_key,
                field,
                false,
            )));
    }
    Ok(())
}

pub(super) fn allocate_wire_alias(field: &str, used: &mut BTreeSet<String>) -> String {
    let stem = format!("_distributed_{field}");
    if used.insert(stem.clone()) {
        return stem;
    }
    for suffix in 2_u64.. {
        let candidate = format!("{stem}_{suffix}");
        if used.insert(candidate.clone()) {
            return candidate;
        }
    }
    unreachable!("finite set always admits another suffix")
}

pub(super) fn compile_coverage(
    pagination: Option<&ClientPaginationSemantics>,
    arguments: &[ClientArgument],
    owner: &str,
    document: &ClientDocument,
    position: Pos,
) -> Result<Option<CompiledCoverage>, ClientCompileError> {
    let Some(pagination) = pagination else {
        return Ok(Some(complete_coverage()));
    };
    if pagination.kind != "offset" || pagination.coverage != "window" {
        return Err(source_error(
            "client.pagination.unsupported",
            format!(
                "field `{owner}` uses unsupported pagination contract kind=`{}` coverage=`{}`",
                pagination.kind, pagination.coverage
            ),
            document,
            position,
        ));
    }
    Ok(Some(CompiledCoverage {
        kind: "offset".into(),
        offset_argument: arguments
            .iter()
            .find(|argument| argument.kind == ClientArgumentKind::Offset)
            .map(|argument| argument.name.clone()),
        limit_argument: arguments
            .iter()
            .find(|argument| argument.kind == ClientArgumentKind::Limit)
            .map(|argument| argument.name.clone()),
        default_limit: Some(pagination.default_limit),
        max_limit: Some(pagination.max_limit),
    }))
}

pub(super) fn complete_coverage() -> CompiledCoverage {
    CompiledCoverage {
        kind: "complete".into(),
        offset_argument: None,
        limit_argument: None,
        default_limit: None,
        max_limit: None,
    }
}

pub(super) fn compiled_pagination_plan(coverage: &CompiledCoverage) -> CompiledPaginationPlan {
    match coverage.kind.as_str() {
        "complete" => CompiledPaginationPlan {
            kind: "complete".into(),
            insert: "local".into(),
            delete: "local".into(),
            reorder: "local".into(),
            stable_update: "local".into(),
        },
        "offset" => CompiledPaginationPlan {
            kind: "offset".into(),
            // Runtime locality is still fail-closed: these operations are
            // applied only when observed coverage proves a non-full first
            // page. Full, shifted, or ambiguous windows revalidate.
            insert: "local".into(),
            delete: "local".into(),
            reorder: "local".into(),
            stable_update: "local".into(),
        },
        other => CompiledPaginationPlan {
            kind: other.into(),
            insert: "revalidate".into(),
            delete: "revalidate".into(),
            reorder: "revalidate".into(),
            stable_update: "revalidate".into(),
        },
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn compile_query_plans(
    model: &ClientModel,
    filter: Option<&ClientFilterSemantics>,
    order: Option<&ClientOrderSemantics>,
    declared_arguments: &[ClientArgument],
    compiled_arguments: &BTreeMap<String, CompiledArgument>,
    variables: &[CompiledVariable],
    manifest: &ClientManifest,
    document: &ClientDocument,
    field: &Field,
    pagination: Option<&CompiledCoverage>,
    filter_base_depth: u64,
    list_index: bool,
) -> Result<CompiledQueryPlans, ClientCompileError> {
    let filter_argument = declared_arguments
        .iter()
        .find(|argument| argument.kind == ClientArgumentKind::Filter);
    let order_argument = declared_arguments
        .iter()
        .find(|argument| argument.kind == ClientArgumentKind::Order);

    let filter_plan = match filter {
        Some(semantics) => {
            let mut variable_constraints = BTreeMap::new();
            let input = filter_argument
                .and_then(|argument| compiled_arguments.get(&argument.name))
                .cloned();
            if let Some(input) = &input {
                let input_type = filter_argument
                    .map(ClientArgument::graphql_type)
                    .ok_or_else(|| {
                        ClientCompileError::manifest(
                            "client.manifest.filter_argument",
                            format!(
                                "model `{}` has filter semantics without an argument",
                                model.id
                            ),
                        )
                    })?;
                validate_filter_source(
                    input,
                    model,
                    &model.filter_input,
                    &input_type,
                    variables,
                    manifest,
                    &manifest.execution,
                    document,
                    argument_position(field, filter_argument.map(|value| value.name.as_str())),
                    filter_base_depth,
                    &mut variable_constraints,
                )?;
            }
            let fields = semantics
                .fields
                .iter()
                .map(|filter_field| {
                    let field = model.field(&filter_field.name).ok_or_else(|| {
                        ClientCompileError::manifest(
                            "client.manifest.filter_field",
                            format!(
                                "filter plan for model `{}` references absent field `{}`",
                                model.id, filter_field.name
                            ),
                        )
                    })?;
                    Ok(CompiledFilterField {
                        name: field.name.clone(),
                        scalar: field.scalar.clone(),
                        codec: field.codec.clone(),
                        nullable: field.nullable,
                        operators: filter_field.operators.clone(),
                    })
                })
                .collect::<Result<Vec<_>, ClientCompileError>>()?;
            let relationships = semantics
                .relationships
                .iter()
                .map(|name| {
                    model
                        .relationship(name)
                        .map(compiled_relationship_plan)
                        .ok_or_else(|| {
                            ClientCompileError::manifest(
                                "client.manifest.filter_relationship",
                                format!(
                                    "filter plan for model `{}` references absent relationship `{name}`",
                                    model.id
                                ),
                            )
                        })
                })
                .collect::<Result<Vec<_>, ClientCompileError>>()?;
            Some(CompiledFilterPlan {
                input,
                fields,
                relationships,
                row_policy: semantics.row_policy.clone(),
                variable_constraints,
            })
        }
        None => None,
    };

    let order_plan = match order {
        Some(semantics) => {
            let input = order_argument
                .and_then(|argument| compiled_arguments.get(&argument.name))
                .cloned();
            if let Some(input) = &input {
                validate_order_source(
                    input,
                    semantics,
                    order_argument,
                    variables,
                    document,
                    argument_position(field, order_argument.map(|value| value.name.as_str())),
                )?;
            }
            let fields = semantics
                .fields
                .iter()
                .map(|name| compiled_order_field(model, name))
                .collect::<Result<Vec<_>, _>>()?;
            let identity = model
                .identity()
                .unwrap_or_default()
                .iter()
                .map(|identity| compiled_order_field(model, &identity.name))
                .collect::<Result<Vec<_>, _>>()?;
            Some(CompiledOrderPlan {
                input,
                fields,
                identity,
            })
        }
        None if list_index => Some(CompiledOrderPlan {
            input: None,
            fields: Vec::new(),
            identity: model
                .identity()
                .unwrap_or_default()
                .iter()
                .map(|identity| compiled_order_field(model, &identity.name))
                .collect::<Result<Vec<_>, _>>()?,
        }),
        None => None,
    };

    let pagination_plan = if list_index {
        pagination.map(compiled_pagination_plan)
    } else {
        None
    };

    Ok((filter_plan, order_plan, pagination_plan))
}

pub(super) fn compiled_order_field(
    model: &ClientModel,
    name: &str,
) -> Result<CompiledOrderField, ClientCompileError> {
    let field = model.field(name).ok_or_else(|| {
        ClientCompileError::manifest(
            "client.manifest.order_field",
            format!(
                "order plan for model `{}` references absent field `{name}`",
                model.id
            ),
        )
    })?;
    Ok(CompiledOrderField {
        name: field.name.clone(),
        scalar: field.scalar.clone(),
        codec: field.codec.clone(),
        nullable: field.nullable,
    })
}

pub(super) fn argument_position(field: &Field, name: Option<&str>) -> Pos {
    name.and_then(|name| {
        field
            .arguments
            .iter()
            .find(|(argument, _)| argument.node.as_str() == name)
            .map(|(_, value)| value.pos)
    })
    .unwrap_or(field.name.pos)
}
