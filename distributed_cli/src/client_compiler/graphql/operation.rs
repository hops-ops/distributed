use super::*;

pub(crate) fn compile_document(
    document: &ClientDocument,
    manifest: &ClientManifest,
) -> Result<CompiledOperation, ClientCompileError> {
    if document.source.len() > MAX_SOURCE_BYTES {
        return Err(source_error(
            "client.document.size",
            format!("GraphQL document exceeds the supported {MAX_SOURCE_BYTES}-byte bound"),
            document,
            Pos::default(),
        ));
    }
    let parsed = parse_query(&document.source).map_err(|error| {
        let pos = error.positions().next().unwrap_or_default();
        source_error(
            "client.graphql.parse",
            format!("invalid GraphQL document: {error}"),
            document,
            pos,
        )
    })?;
    let (name, operation) = match &parsed.operations {
        DocumentOperations::Single(operation) => {
            return Err(source_error(
                "client.operation.named_required",
                "client operations must be explicitly named",
                document,
                operation.pos,
            ));
        }
        DocumentOperations::Multiple(operations) if operations.len() == 1 => {
            let (name, operation) = operations.iter().next().expect("length checked");
            (name.as_str().to_string(), operation)
        }
        DocumentOperations::Multiple(operations) => {
            let position = operations
                .values()
                .map(|operation| operation.pos)
                .min()
                .unwrap_or_default();
            return Err(source_error(
                "client.operation.one_per_document",
                format!(
                    "each client document must contain exactly one named operation; found {}",
                    operations.len()
                ),
                document,
                position,
            ));
        }
    };
    if operation.node.ty != OperationType::Query {
        return Err(source_error(
            "client.operation.query_required",
            "application documents must declare a query; commands are generated from the manifest",
            document,
            operation.pos,
        ));
    }
    let compiler_directives = compiler_directives(&operation.node, document)?;
    let variables = compile_variables(&operation.node, document)?;
    let mut expander = FragmentExpander::new(&parsed.fragments, document);
    let root_fields =
        expander.merge_object(&[&operation.node.selection_set], "Query", 1, "root field")?;
    let root_field = single_root_field(root_fields, &operation.node, document)?;

    let root_name = root_field.first.node.name.node.as_str();
    let root_manifest = manifest
        .root(ClientRootOperation::Query, root_name)
        .ok_or_else(|| {
            source_error(
                "client.root.denied_or_unknown",
                format!("query root `{root_name}` is absent from the selected manifest surface"),
                document,
                root_field.first.node.name.pos,
            )
        })?;
    let model = manifest.models.get(&root_manifest.model).ok_or_else(|| {
        ClientCompileError::manifest(
            "client.manifest.root_model",
            format!(
                "root `{root_name}` references absent model `{}`",
                root_manifest.model
            ),
        )
    })?;
    validate_reachable_fragment_graph(&parsed.fragments, document, &operation.node.selection_set)?;
    let mut used_variables = BTreeSet::new();
    let compiled_arguments = compile_arguments(
        &root_field.first.node,
        &root_manifest.name,
        &root_manifest.arguments,
        model,
        manifest,
        &variables,
        &mut used_variables,
        document,
    )?;
    let mut selection = match root_manifest.kind {
        ClientRootKind::List | ClientRootKind::ByPk => compile_model_object(
            &root_field,
            model,
            &ModelCompileContext::new(manifest, &variables, document),
            &mut used_variables,
            &mut expander,
            2,
        )?,
        ClientRootKind::Aggregate => {
            let semantics = root_manifest.aggregate.as_ref().ok_or_else(|| {
                ClientCompileError::manifest(
                    "client.manifest.aggregate_semantics",
                    format!("aggregate root `{root_name}` has no aggregate semantics"),
                )
            })?;
            compile_aggregate_object(
                &root_field,
                model,
                semantics,
                &root_manifest.arguments,
                &compiled_arguments,
                root_manifest.filter.as_ref(),
                root_manifest.order.as_ref(),
                &root_manifest.dependencies,
                manifest,
                &variables,
                &mut used_variables,
                document,
                &mut expander,
                2,
            )?
        }
    };
    if root_manifest.kind == ClientRootKind::List {
        let dependencies = query_plan_field_dependencies(
            model,
            root_manifest.filter.as_ref(),
            root_manifest.order.as_ref(),
            &root_manifest.arguments,
            &compiled_arguments,
        );
        inject_dependency_fields(
            &mut selection,
            model,
            &dependencies,
            document,
            root_field.first.pos,
        )?;
    }
    validate_used_variables(&variables, &used_variables, document, operation.pos)?;
    expander.reject_unused_fragments()?;

    let cardinality = match root_manifest.kind {
        ClientRootKind::List => Cardinality::Many,
        ClientRootKind::ByPk => Cardinality::One,
        ClientRootKind::Aggregate => Cardinality::One,
    };
    let coverage = match root_manifest.kind {
        ClientRootKind::Aggregate => Some(complete_coverage()),
        ClientRootKind::List | ClientRootKind::ByPk => compile_coverage(
            root_manifest.pagination.as_ref(),
            &root_manifest.arguments,
            &root_manifest.name,
            document,
            root_field.first.pos,
        )?,
    };
    let (filter, order, pagination) = compile_query_plans(
        model,
        root_manifest.filter.as_ref(),
        root_manifest.order.as_ref(),
        &root_manifest.arguments,
        &compiled_arguments,
        &variables,
        manifest,
        document,
        &root_field.first.node,
        coverage.as_ref(),
        0,
        root_manifest.kind == ClientRootKind::List,
    )?;
    let root = CompiledRoot {
        response_key: root_field.first.node.response_key().node.to_string(),
        field: root_name.to_string(),
        cardinality,
        nullable: matches!(
            root_manifest.kind,
            ClientRootKind::ByPk | ClientRootKind::Aggregate
        ),
        arguments: compiled_arguments,
        dependencies: root_manifest.dependencies.clone(),
        coverage,
        filter,
        order,
        pagination,
        selection,
    };
    validate_execution_limits(
        &root,
        root_manifest.kind,
        &manifest.execution,
        document,
        operation.pos,
    )?;

    let query_document = render_operation(OperationType::Query, &name, &variables, &root)?;
    let query_hash = hash_bytes(query_document.as_bytes());
    let live = if compiler_directives.live {
        Some(compile_live(
            &name,
            &variables,
            &root,
            root_manifest,
            manifest,
            document,
            operation.pos,
        )?)
    } else {
        None
    };
    let variable_constraints = operation_variable_constraints(&root);
    let variable_codec = compile_variable_codec(&variables, manifest, &variable_constraints)?;
    let module_stem = module_stem(&name);
    Ok(CompiledOperation {
        name: name.clone(),
        source_path: document.path.clone(),
        source_line: operation.pos.line.max(1),
        source_column: operation.pos.column.max(1),
        module_path: format!("operations/{module_stem}.ts"),
        export_name: format!("Operation_{name}"),
        query_document,
        query_hash,
        load: compiler_directives.load,
        live,
        variables,
        variable_codec,
        root,
    })
}

#[derive(Default)]
pub(super) struct CompilerDirectives {
    load: bool,
    live: bool,
}

pub(super) fn compiler_directives(
    operation: &OperationDefinition,
    document: &ClientDocument,
) -> Result<CompilerDirectives, ClientCompileError> {
    let mut result = CompilerDirectives::default();
    let mut seen = BTreeSet::new();
    for directive in &operation.directives {
        let name = directive.node.name.node.as_str();
        if !seen.insert(name) {
            return Err(source_error(
                "client.directive.duplicate",
                format!("directive `@{name}` appears more than once"),
                document,
                directive.pos,
            ));
        }
        if !directive.node.arguments.is_empty() {
            return Err(source_error(
                "client.directive.arguments",
                format!("compiler directive `@{name}` does not accept arguments"),
                document,
                directive.pos,
            ));
        }
        match name {
            "load" => result.load = true,
            "live" => result.live = true,
            "skip" | "include" => {
                return Err(source_error(
                    "client.directive.conditional_unsupported",
                    format!(
                        "conditional directive `@{name}` requires a field-presence plan and is not supported yet"
                    ),
                    document,
                    directive.pos,
                ));
            }
            _ => {
                return Err(source_error(
                    "client.directive.unsupported",
                    format!("operation directive `@{name}` is not supported"),
                    document,
                    directive.pos,
                ));
            }
        }
    }
    Ok(result)
}

pub(super) fn compile_live(
    query_name: &str,
    variables: &[CompiledVariable],
    root: &CompiledRoot,
    query_manifest: &ClientRoot,
    manifest: &ClientManifest,
    document: &ClientDocument,
    position: Pos,
) -> Result<CompiledLiveOperation, ClientCompileError> {
    if !manifest.capabilities.live_queries || !query_manifest.live {
        return Err(source_error(
            "client.live.unavailable",
            format!(
                "`@live` was requested for `{query_name}`, but selected root `{}` is not live-capable",
                query_manifest.name
            ),
            document,
            position,
        ));
    }
    let subscription = manifest
        .root(ClientRootOperation::Subscription, &query_manifest.name)
        .ok_or_else(|| {
            source_error(
                "client.live.root_missing",
                format!(
                    "`@live` requires subscription root `{}` on the same selected surface",
                    query_manifest.name
                ),
                document,
                position,
            )
        })?;
    if subscription.model != query_manifest.model
        || subscription.kind != query_manifest.kind
        || !arguments_compatible(&subscription.arguments, &query_manifest.arguments)
        || !subscription.live
        || subscription.dependencies != query_manifest.dependencies
        || subscription.pagination != query_manifest.pagination
    {
        return Err(source_error(
            "client.live.root_mismatch",
            format!(
                "subscription root `{}` does not exactly match query model, cardinality, arguments, dependencies, pagination, and live contract",
                subscription.name
            ),
            document,
            position,
        ));
    }
    let live_name = format!("{query_name}_Live");
    let document = render_operation(OperationType::Subscription, &live_name, variables, root)?;
    Ok(CompiledLiveOperation {
        hash: hash_bytes(document.as_bytes()),
        document,
    })
}

pub(super) fn arguments_compatible(left: &[ClientArgument], right: &[ClientArgument]) -> bool {
    let canonical = |arguments: &[ClientArgument]| {
        arguments
            .iter()
            .map(|argument| {
                (
                    argument.name.clone(),
                    argument.kind,
                    argument.graphql_type(),
                )
            })
            .collect::<BTreeSet<_>>()
    };
    canonical(left) == canonical(right)
}

pub(super) fn render_operation(
    operation_type: OperationType,
    name: &str,
    variables: &[CompiledVariable],
    root: &CompiledRoot,
) -> Result<String, ClientCompileError> {
    let variable_definitions = if variables.is_empty() {
        String::new()
    } else {
        format!(
            "({})",
            variables
                .iter()
                .map(render_variable)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        )
    };
    let arguments = render_compiled_arguments(&root.arguments);
    let root_prefix = if root.response_key == root.field {
        root.field.clone()
    } else {
        format!("{}: {}", root.response_key, root.field)
    };
    let mut lines = vec![format!("{operation_type} {name}{variable_definitions} {{")];
    lines.push(format!("  {root_prefix}{arguments} {{"));
    render_object_selection(&mut lines, &root.selection, 4);
    lines.push("  }".into());
    lines.push("}".into());
    Ok(format!("{}\n", lines.join("\n")))
}

pub(super) fn render_object_selection(
    lines: &mut Vec<String>,
    object: &CompiledObject,
    indent: usize,
) {
    let padding = " ".repeat(indent);
    for member in &object.members {
        match member {
            CompiledMember::Scalar(field) => {
                let prefix = if field.response_key == field.field {
                    field.field.clone()
                } else {
                    format!("{}: {}", field.response_key, field.field)
                };
                lines.push(format!("{padding}{prefix}"));
            }
            CompiledMember::Branch(branch) => {
                let prefix = if branch.response_key == branch.field {
                    branch.field.clone()
                } else {
                    format!("{}: {}", branch.response_key, branch.field)
                };
                let arguments = render_compiled_arguments(&branch.arguments);
                lines.push(format!("{padding}{prefix}{arguments} {{"));
                render_object_selection(lines, &branch.selection, indent + 2);
                lines.push(format!("{padding}}}"));
            }
        }
    }
}

pub(super) fn render_compiled_arguments(arguments: &BTreeMap<String, CompiledArgument>) -> String {
    if arguments.is_empty() {
        return String::new();
    }
    format!(
        "({})",
        arguments
            .iter()
            .map(|(name, value)| format!("{name}: {}", render_compiled_argument(value)))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

pub(super) fn render_compiled_argument(value: &CompiledArgument) -> String {
    match value {
        CompiledArgument::Literal { wire, .. } => wire.clone(),
        CompiledArgument::Variable(variable) => format!("${variable}"),
        CompiledArgument::List(values) => format!(
            "[{}]",
            values
                .iter()
                .map(render_compiled_argument)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        CompiledArgument::Object(values) => format!(
            "{{{}}}",
            values
                .iter()
                .map(|(name, value)| format!("{name}: {}", render_compiled_argument(value)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

pub(super) fn render_variable(variable: &CompiledVariable) -> Result<String, ClientCompileError> {
    Ok(format!(
        "${}: {}{}",
        variable.name,
        variable.graphql_type,
        variable
            .default
            .as_ref()
            .map(|default| format!(" = {}", default.wire))
            .unwrap_or_default()
    ))
}

pub(super) fn render_value(
    value: &Value,
    document: &ClientDocument,
    position: Pos,
) -> Result<String, ClientCompileError> {
    match value {
        Value::Variable(variable) => Ok(format!("${variable}")),
        Value::Null => Ok("null".into()),
        Value::Boolean(value) => Ok(value.to_string()),
        Value::Number(value) => Ok(value.to_string()),
        Value::String(value) => Ok(Value::String(value.clone()).to_string()),
        Value::Binary(_) => Err(source_error(
            "client.literal.binary",
            "binary GraphQL literals are not portable to the JavaScript replica",
            document,
            position,
        )),
        Value::Enum(value) => Ok(value.to_string()),
        Value::List(values) => Ok(format!(
            "[{}]",
            values
                .iter()
                .map(|value| render_value(value, document, position))
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        )),
        Value::Object(values) => {
            let sorted = values.iter().collect::<BTreeMap<_, _>>();
            Ok(format!(
                "{{{}}}",
                sorted
                    .into_iter()
                    .map(|(name, value)| {
                        Ok(format!(
                            "{name}: {}",
                            render_value(value, document, position)?
                        ))
                    })
                    .collect::<Result<Vec<_>, ClientCompileError>>()?
                    .join(", ")
            ))
        }
    }
}

pub(super) fn variable_type_compatible(variable: &Type, argument: &Type) -> bool {
    if !argument.nullable && variable.nullable {
        return false;
    }
    match (&variable.base, &argument.base) {
        (BaseType::Named(variable), BaseType::Named(argument)) => variable == argument,
        (BaseType::List(variable), BaseType::List(argument)) => {
            variable_type_compatible(variable, argument)
        }
        _ => false,
    }
}

pub(crate) fn typescript_scalar(
    field: &CompiledScalar,
) -> Result<&'static str, ClientCompileError> {
    match field.codec.as_str() {
        "boolean" => Ok("boolean"),
        "float64" | "int32" | "json_number_precision_limited" => Ok("number"),
        "string" | "base64" | "string_unvalidated_timestamp" => Ok("string"),
        "json" => Ok("unknown"),
        codec => Err(ClientCompileError::manifest(
            "client.scalar.codec_unsupported",
            format!(
                "field `{}` uses unsupported TypeScript codec `{codec}`",
                field.field
            ),
        )),
    }
}

pub(super) fn module_stem(name: &str) -> String {
    let mut result = String::new();
    for (index, character) in name.chars().enumerate() {
        if character == '_' {
            if !result.ends_with('-') {
                result.push('-');
            }
        } else if character.is_ascii_uppercase() {
            if index > 0 && !result.ends_with('-') {
                result.push('-');
            }
            result.push(character.to_ascii_lowercase());
        } else {
            result.push(character.to_ascii_lowercase());
        }
    }
    result
}

pub(super) fn source_error(
    code: &'static str,
    message: impl Into<String>,
    document: &ClientDocument,
    position: Pos,
) -> ClientCompileError {
    ClientCompileError::source(
        code,
        message,
        &document.path,
        position.line.max(1),
        position.column.max(1),
    )
}

#[cfg(test)]
mod local_tests {
    use super::module_stem;

    #[test]
    fn module_names_are_portable() {
        assert_eq!(module_stem("TodosForUser"), "todos-for-user");
        assert_eq!(module_stem("todos_for_user"), "todos-for-user");
    }
}
