use super::*;

pub(super) fn compile_variables(
    operation: &OperationDefinition,
    document: &ClientDocument,
) -> Result<Vec<CompiledVariable>, ClientCompileError> {
    if operation.variable_definitions.len() > MAX_VARIABLES {
        return Err(source_error(
            "client.variables.bound",
            format!("operation exceeds the supported {MAX_VARIABLES}-variable bound"),
            document,
            operation.selection_set.pos,
        ));
    }
    let mut variables = Vec::with_capacity(operation.variable_definitions.len());
    let mut seen = BTreeSet::new();
    for variable in &operation.variable_definitions {
        let definition = &variable.node;
        let name = definition.name.node.as_str();
        if !seen.insert(name) {
            return Err(source_error(
                "client.variable.duplicate",
                format!("variable `${name}` is defined more than once"),
                document,
                definition.name.pos,
            ));
        }
        reject_directives(&definition.directives, "variable definition", document)?;
        let default = definition
            .default_value
            .as_ref()
            .map(|default| {
                let value = Value::from(default.node.clone());
                Ok(CompiledVariableDefault {
                    value: value_to_json(&value, document, default.pos)?,
                    wire: render_value(&value, document, default.pos)?,
                })
            })
            .transpose()?;
        variables.push(CompiledVariable {
            name: name.to_string(),
            graphql_type: definition.var_type.node.clone(),
            default,
        });
    }
    variables.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(variables)
}

#[derive(Clone, Copy)]
pub(super) struct FilterInputCandidate<'a> {
    model: &'a ClientModel,
    semantics: &'a ClientFilterInput,
}

#[derive(Clone, Copy)]
pub(super) struct OrderInputCandidate<'a> {
    model: &'a ClientModel,
    semantics: &'a ClientOrderSemantics,
}

pub(super) fn compile_variable_codec(
    variables: &[CompiledVariable],
    manifest: &ClientManifest,
    constraints: &BTreeMap<String, VariableUseConstraint>,
) -> Result<CompiledVariableCodec, ClientCompileError> {
    let mut filters = BTreeMap::<String, FilterInputCandidate<'_>>::new();
    let mut orders = BTreeMap::<String, OrderInputCandidate<'_>>::new();

    for model in manifest.models.values() {
        register_filter_input(
            &model.filter_input.type_name,
            model,
            &model.filter_input,
            &mut filters,
        )?;
    }

    for root in manifest.roots.values() {
        let model = manifest.models.get(&root.model).ok_or_else(|| {
            ClientCompileError::manifest(
                "client.manifest.root_model",
                format!(
                    "root `{}` references absent model `{}`",
                    root.name, root.model
                ),
            )
        })?;
        register_order_input_candidate(&root.arguments, root.order.as_ref(), model, &mut orders)?;
    }

    for source in manifest.models.values() {
        for relationship in &source.relationships {
            let target = manifest
                .models
                .get(&relationship.target_model)
                .ok_or_else(|| {
                    ClientCompileError::manifest(
                        "client.manifest.relationship_target",
                        format!(
                            "relationship `{}.{}` references absent model `{}`",
                            source.id, relationship.name, relationship.target_model
                        ),
                    )
                })?;
            register_order_input_candidate(
                &relationship.arguments,
                relationship.order.as_ref(),
                target,
                &mut orders,
            )?;
            if let Some(aggregate) = &relationship.aggregate {
                register_order_input_candidate(
                    &aggregate.arguments,
                    relationship.order.as_ref(),
                    target,
                    &mut orders,
                )?;
            }
        }
    }

    if let Some(name) = filters.keys().find(|name| orders.contains_key(*name)) {
        return Err(ClientCompileError::manifest(
            "client.variable.input_type_conflict",
            format!(
                "selected manifest uses input type `{name}` for both filter and order contracts"
            ),
        ));
    }

    let order_values = orders
        .values()
        .next()
        .map(|candidate| candidate.semantics.values.clone())
        .unwrap_or_default();
    if orders
        .values()
        .any(|candidate| candidate.semantics.values != order_values)
    {
        return Err(ClientCompileError::manifest(
            "client.variable.order_enum_conflict",
            "selected order input contracts disagree on the direction enum",
        ));
    }

    let mut inputs = BTreeMap::new();
    let mut visiting = BTreeSet::new();
    let mut compiled_variables = BTreeMap::new();
    for variable in variables {
        let input_type = compile_variable_input_type(
            &variable.graphql_type,
            manifest,
            &filters,
            &orders,
            &order_values,
            &mut inputs,
            &mut visiting,
            constraints.get(&variable.name),
        )?;
        compiled_variables.insert(variable.name.clone(), input_type);
    }
    let defaults = variables
        .iter()
        .filter_map(|variable| {
            variable
                .default
                .as_ref()
                .map(|default| (variable.name.clone(), default.value.clone()))
        })
        .collect();
    Ok(CompiledVariableCodec {
        version: 2,
        limits: CompiledVariableCodecLimits {
            max_depth: manifest.execution.max_depth,
            max_bool_width: manifest.execution.max_bool_width,
            max_in_list: manifest.execution.max_in_list,
        },
        variables: compiled_variables,
        defaults,
        inputs,
    })
}

pub(super) fn operation_variable_constraints(
    root: &CompiledRoot,
) -> BTreeMap<String, VariableUseConstraint> {
    let mut constraints = BTreeMap::new();
    merge_filter_constraints(root.filter.as_ref(), &mut constraints);
    merge_object_variable_constraints(&root.selection, &mut constraints);
    constraints
}

pub(super) fn merge_object_variable_constraints(
    object: &CompiledObject,
    constraints: &mut BTreeMap<String, VariableUseConstraint>,
) {
    for member in &object.members {
        let CompiledMember::Branch(branch) = member else {
            continue;
        };
        merge_filter_constraints(branch.filter.as_ref(), constraints);
        merge_object_variable_constraints(&branch.selection, constraints);
    }
}

pub(super) fn merge_filter_constraints(
    filter: Option<&CompiledFilterPlan>,
    constraints: &mut BTreeMap<String, VariableUseConstraint>,
) {
    let Some(filter) = filter else {
        return;
    };
    for (name, constraint) in &filter.variable_constraints {
        constraints
            .entry(name.clone())
            .and_modify(|existing| existing.intersect(constraint))
            .or_insert_with(|| constraint.clone());
    }
}

pub(super) fn register_order_input_candidate<'a>(
    arguments: &[ClientArgument],
    order: Option<&'a ClientOrderSemantics>,
    model: &'a ClientModel,
    orders: &mut BTreeMap<String, OrderInputCandidate<'a>>,
) -> Result<(), ClientCompileError> {
    if let Some(semantics) = order {
        if let Some(argument) = arguments
            .iter()
            .find(|argument| argument.kind == ClientArgumentKind::Order)
        {
            register_order_input(&argument.type_name, model, semantics, orders)?;
        }
    }
    Ok(())
}

pub(super) fn register_filter_input<'a>(
    name: &str,
    model: &'a ClientModel,
    semantics: &'a ClientFilterInput,
    candidates: &mut BTreeMap<String, FilterInputCandidate<'a>>,
) -> Result<(), ClientCompileError> {
    if let Some(existing) = candidates.get(name) {
        if existing.model.id != model.id || existing.semantics != semantics {
            return Err(ClientCompileError::manifest(
                "client.variable.input_type_conflict",
                format!("filter input type `{name}` has multiple selected structural contracts"),
            ));
        }
        return Ok(());
    }
    candidates.insert(name.to_string(), FilterInputCandidate { model, semantics });
    Ok(())
}

pub(super) fn register_order_input<'a>(
    name: &str,
    model: &'a ClientModel,
    semantics: &'a ClientOrderSemantics,
    candidates: &mut BTreeMap<String, OrderInputCandidate<'a>>,
) -> Result<(), ClientCompileError> {
    if let Some(existing) = candidates.get(name) {
        if existing.model.id != model.id || existing.semantics != semantics {
            return Err(ClientCompileError::manifest(
                "client.variable.input_type_conflict",
                format!("order input type `{name}` has multiple selected structural contracts"),
            ));
        }
        return Ok(());
    }
    candidates.insert(name.to_string(), OrderInputCandidate { model, semantics });
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn compile_variable_input_type(
    graphql_type: &Type,
    manifest: &ClientManifest,
    filters: &BTreeMap<String, FilterInputCandidate<'_>>,
    orders: &BTreeMap<String, OrderInputCandidate<'_>>,
    order_values: &[String],
    inputs: &mut BTreeMap<String, CompiledInputDefinition>,
    visiting: &mut BTreeSet<String>,
    constraint: Option<&VariableUseConstraint>,
) -> Result<CompiledInputType, ClientCompileError> {
    let nullable = graphql_type.nullable;
    match &graphql_type.base {
        BaseType::List(item) => {
            if constraint.is_some_and(|constraint| constraint.filter_base_depth.is_some()) {
                return Err(ClientCompileError::manifest(
                    "client.variable.constraint_type",
                    "filterBaseDepth cannot apply to a list variable",
                ));
            }
            Ok(CompiledInputType::List {
                nullable,
                max_items: constraint.and_then(|constraint| constraint.max_items),
                item: Box::new(compile_variable_input_type(
                    item,
                    manifest,
                    filters,
                    orders,
                    order_values,
                    inputs,
                    visiting,
                    constraint.and_then(|constraint| constraint.item.as_deref()),
                )?),
            })
        }
        BaseType::Named(name) => {
            let name = name.as_str();
            if let Some(codec) = manifest.scalar_codecs.get(name) {
                require_leaf_constraint(name, constraint)?;
                return Ok(CompiledInputType::Scalar {
                    scalar: name.to_string(),
                    codec: codec.clone(),
                    nullable,
                });
            }
            if filters.contains_key(name) {
                if constraint.is_some_and(|constraint| {
                    constraint.max_items.is_some() || constraint.item.is_some()
                }) {
                    return Err(ClientCompileError::manifest(
                        "client.variable.constraint_type",
                        format!("filter input `{name}` received list constraints"),
                    ));
                }
                compile_filter_input_definition(name, filters, inputs, visiting)?;
                return Ok(CompiledInputType::Input {
                    name: name.to_string(),
                    nullable,
                    filter_base_depth: constraint
                        .and_then(|constraint| constraint.filter_base_depth),
                });
            }
            if orders.contains_key(name) {
                require_leaf_constraint(name, constraint)?;
                compile_order_input_definition(name, orders, inputs)?;
                return Ok(CompiledInputType::Input {
                    name: name.to_string(),
                    nullable,
                    filter_base_depth: None,
                });
            }
            if name == "order_by" && !order_values.is_empty() {
                require_leaf_constraint(name, constraint)?;
                return Ok(CompiledInputType::Enum {
                    name: name.to_string(),
                    values: order_values.to_vec(),
                    nullable,
                });
            }
            Err(ClientCompileError::manifest(
                "client.variable.input_type_unsupported",
                format!(
                    "variable input type `{name}` has no compiler-owned scalar, filter, order, or enum contract"
                ),
            ))
        }
    }
}

pub(super) fn require_leaf_constraint(
    name: &str,
    constraint: Option<&VariableUseConstraint>,
) -> Result<(), ClientCompileError> {
    if constraint.is_none_or(|constraint| {
        constraint.filter_base_depth.is_none()
            && constraint.max_items.is_none()
            && constraint.item.is_none()
    }) {
        return Ok(());
    }
    Err(ClientCompileError::manifest(
        "client.variable.constraint_type",
        format!("variable type `{name}` received incompatible input constraints"),
    ))
}

pub(super) fn compile_filter_input_definition(
    name: &str,
    candidates: &BTreeMap<String, FilterInputCandidate<'_>>,
    inputs: &mut BTreeMap<String, CompiledInputDefinition>,
    visiting: &mut BTreeSet<String>,
) -> Result<(), ClientCompileError> {
    if inputs.contains_key(name) || !visiting.insert(name.to_string()) {
        return Ok(());
    }
    let candidate = *candidates.get(name).ok_or_else(|| {
        ClientCompileError::manifest(
            "client.variable.filter_input",
            format!("filter input type `{name}` has no selected contract"),
        )
    })?;
    let fields = candidate
        .semantics
        .fields
        .iter()
        .map(|semantics| {
            let field = candidate.model.field(&semantics.name).ok_or_else(|| {
                ClientCompileError::manifest(
                    "client.variable.filter_field",
                    format!(
                        "filter input `{name}` references absent field `{}.{}`",
                        candidate.model.id, semantics.name
                    ),
                )
            })?;
            Ok(CompiledFilterInputField {
                field: field.name.clone(),
                scalar: field.scalar.clone(),
                codec: field.codec.clone(),
                nullable: field.nullable,
                operators: semantics.operators.clone(),
            })
        })
        .collect::<Result<Vec<_>, ClientCompileError>>()?;

    let mut relationships = Vec::with_capacity(candidate.semantics.relationships.len());
    for relationship_input in &candidate.semantics.relationships {
        candidate
            .model
            .relationship(&relationship_input.field)
            .ok_or_else(|| {
                ClientCompileError::manifest(
                    "client.variable.filter_relationship",
                    format!(
                        "filter input `{name}` references absent relationship `{}.{}`",
                        candidate.model.id, relationship_input.field
                    ),
                )
            })?;
        let target = candidates
            .contains_key(&relationship_input.target_type)
            .then(|| {
                compile_filter_input_definition(
                    &relationship_input.target_type,
                    candidates,
                    inputs,
                    visiting,
                )?;
                Ok(CompiledFilterInputTarget::Input {
                    name: relationship_input.target_type.clone(),
                })
            })
            .transpose()?
            .unwrap_or(CompiledFilterInputTarget::Opaque);
        relationships.push(CompiledFilterInputRelationship {
            field: relationship_input.field.clone(),
            target,
        });
    }
    inputs.insert(
        name.to_string(),
        CompiledInputDefinition::Filter {
            model: candidate.model.id.clone(),
            fields,
            relationships,
        },
    );
    visiting.remove(name);
    Ok(())
}

pub(super) fn compile_order_input_definition(
    name: &str,
    candidates: &BTreeMap<String, OrderInputCandidate<'_>>,
    inputs: &mut BTreeMap<String, CompiledInputDefinition>,
) -> Result<(), ClientCompileError> {
    if inputs.contains_key(name) {
        return Ok(());
    }
    let candidate = *candidates.get(name).ok_or_else(|| {
        ClientCompileError::manifest(
            "client.variable.order_input",
            format!("order input type `{name}` has no selected contract"),
        )
    })?;
    let fields = candidate
        .semantics
        .fields
        .iter()
        .map(|field_name| {
            let field = candidate.model.field(field_name).ok_or_else(|| {
                ClientCompileError::manifest(
                    "client.variable.order_field",
                    format!(
                        "order input `{name}` references absent field `{}.{field_name}`",
                        candidate.model.id
                    ),
                )
            })?;
            Ok(CompiledOrderInputField {
                field: field.name.clone(),
                scalar: field.scalar.clone(),
                codec: field.codec.clone(),
                nullable: field.nullable,
            })
        })
        .collect::<Result<Vec<_>, ClientCompileError>>()?;
    inputs.insert(
        name.to_string(),
        CompiledInputDefinition::Order {
            model: candidate.model.id.clone(),
            fields,
            values: candidate.semantics.values.clone(),
        },
    );
    Ok(())
}
