use super::*;

pub(super) struct ModelCompileContext<'a> {
    manifest: &'a ClientManifest,
    variables: &'a [CompiledVariable],
    document: &'a ClientDocument,
}

impl<'a> ModelCompileContext<'a> {
    pub(super) fn new(
        manifest: &'a ClientManifest,
        variables: &'a [CompiledVariable],
        document: &'a ClientDocument,
    ) -> Self {
        Self {
            manifest,
            variables,
            document,
        }
    }
}

pub(super) fn compile_model_object<'ast>(
    field: &MergedField<'ast>,
    model: &ClientModel,
    context: &ModelCompileContext<'_>,
    used_variables: &mut BTreeSet<String>,
    expander: &mut FragmentExpander<'ast, '_>,
    depth: usize,
) -> Result<CompiledObject, ClientCompileError> {
    let manifest = context.manifest;
    let variables = context.variables;
    let document = context.document;
    let selected_fields = expander.merge_object(
        &field.selection_sets,
        &model.typename,
        depth,
        "selected field",
    )?;
    if selected_fields.is_empty() {
        return Err(source_error(
            "client.selection.empty",
            format!(
                "object field `{}` must select at least one field",
                field.first.node.name.node
            ),
            document,
            field.first.node.selection_set.pos,
        ));
    }
    let mut result = Vec::with_capacity(selected_fields.len());
    let mut relationship_source_fields = BTreeSet::new();
    for selected in selected_fields {
        let response_key = selected.first.node.response_key().node.as_str();
        let field_name = selected.first.node.name.node.as_str();
        if let Some(relationship) = model.relationship(field_name) {
            if selected.selection_sets.is_empty() {
                return Err(source_error(
                    "client.selection.object_required",
                    format!(
                        "relationship `{}` on model `{}` requires an object selection",
                        relationship.name, model.id
                    ),
                    document,
                    selected.first.node.selection_set.pos,
                ));
            }
            let target = manifest
                .models
                .get(&relationship.target_model)
                .ok_or_else(|| {
                    ClientCompileError::manifest(
                        "client.manifest.relationship_target",
                        format!(
                            "relationship `{}.{}` references absent target model `{}`",
                            model.id, relationship.name, relationship.target_model
                        ),
                    )
                })?;
            let arguments = compile_arguments(
                &selected.first.node,
                &format!("{}.{}", model.id, relationship.name),
                &relationship.arguments,
                target,
                manifest,
                variables,
                used_variables,
                document,
            )?;
            let relationship_plan = compiled_relationship_plan(relationship);
            let (local_keys, remote_keys) = relationship_key_fields(&relationship_plan.key_mapping);
            relationship_source_fields.extend(local_keys.iter().cloned());
            let mut selection = compile_model_object(
                &selected,
                target,
                context,
                used_variables,
                expander,
                depth + 1,
            )?;
            let mut dependencies = BTreeSet::new();
            if relationship.list {
                dependencies.extend(query_plan_field_dependencies(
                    target,
                    relationship.filter.as_ref(),
                    relationship.order.as_ref(),
                    &relationship.arguments,
                    &arguments,
                ));
            }
            dependencies.extend(remote_keys.iter().cloned());
            if !dependencies.is_empty() {
                inject_dependency_fields(
                    &mut selection,
                    target,
                    &dependencies,
                    document,
                    selected.first.pos,
                )?;
            }
            let cardinality = if relationship.list {
                Cardinality::Many
            } else {
                Cardinality::One
            };
            let coverage = compile_coverage(
                relationship.pagination.as_ref(),
                &relationship.arguments,
                &format!("{}.{}", model.id, relationship.name),
                document,
                selected.first.pos,
            )?;
            let (filter, order, pagination) = compile_query_plans(
                target,
                relationship.filter.as_ref(),
                relationship.order.as_ref(),
                &relationship.arguments,
                &arguments,
                variables,
                manifest,
                document,
                &selected.first.node,
                coverage.as_ref(),
                filter_depth_from_selection(depth, 1),
                relationship.list,
            )?;
            result.push(CompiledMember::Branch(Box::new(CompiledBranch {
                semantic: CompiledBranchSemantic::Relationship,
                response_key: response_key.to_string(),
                field: relationship.name.clone(),
                cardinality,
                nullable: relationship.nullable,
                arguments,
                dependencies: relationship.dependencies.clone(),
                coverage,
                filter,
                order,
                pagination,
                relationship: Some(relationship_plan),
                selection,
            })));
            continue;
        }
        if let Some((relationship, aggregate)) = model.relationships.iter().find_map(|candidate| {
            candidate
                .aggregate
                .as_ref()
                .filter(|aggregate| aggregate.name == field_name)
                .map(|aggregate| (candidate, aggregate))
        }) {
            if selected.selection_sets.is_empty() {
                return Err(source_error(
                    "client.selection.object_required",
                    format!(
                        "relationship aggregate `{}` on model `{}` requires an object selection",
                        aggregate.name, model.id
                    ),
                    document,
                    selected.first.node.selection_set.pos,
                ));
            }
            let target = manifest
                .models
                .get(&relationship.target_model)
                .ok_or_else(|| {
                    ClientCompileError::manifest(
                        "client.manifest.relationship_target",
                        format!(
                            "relationship aggregate `{}.{}` references absent target model `{}`",
                            model.id, aggregate.name, relationship.target_model
                        ),
                    )
                })?;
            let arguments = compile_arguments(
                &selected.first.node,
                &format!("{}.{}", model.id, aggregate.name),
                &aggregate.arguments,
                target,
                manifest,
                variables,
                used_variables,
                document,
            )?;
            let selection = compile_aggregate_object(
                &selected,
                target,
                &aggregate.semantics,
                &aggregate.arguments,
                &arguments,
                relationship.filter.as_ref(),
                relationship.order.as_ref(),
                &aggregate.dependencies,
                manifest,
                variables,
                used_variables,
                document,
                expander,
                depth + 1,
            )?;
            let (filter, order, pagination) = compile_query_plans(
                target,
                relationship.filter.as_ref(),
                relationship.order.as_ref(),
                &aggregate.arguments,
                &arguments,
                variables,
                manifest,
                document,
                &selected.first.node,
                None,
                filter_depth_from_selection(depth, 1),
                false,
            )?;
            result.push(CompiledMember::Branch(Box::new(CompiledBranch {
                semantic: CompiledBranchSemantic::Aggregate,
                response_key: response_key.to_string(),
                field: aggregate.name.clone(),
                cardinality: Cardinality::One,
                nullable: true,
                arguments,
                dependencies: aggregate.dependencies.clone(),
                coverage: Some(complete_coverage()),
                filter,
                order,
                pagination,
                relationship: None,
                selection,
            })));
            continue;
        }
        let manifest_field = if field_name == "__typename" {
            None
        } else {
            Some(model.field(field_name).ok_or_else(|| {
                source_error(
                    "client.selection.denied_or_unknown",
                    format!(
                        "field `{field_name}` is absent from selected model `{}`",
                        model.id
                    ),
                    document,
                    selected.first.node.name.pos,
                )
            })?)
        };
        if !selected.first.node.arguments.is_empty() {
            return Err(source_error(
                "client.selection.field_arguments",
                format!("scalar field `{field_name}` must not have arguments"),
                document,
                selected.first.pos,
            ));
        }
        if !selected.selection_sets.is_empty() {
            return Err(source_error(
                "client.selection.scalar_nested",
                format!("scalar field `{field_name}` cannot have a nested selection"),
                document,
                selected.first.node.selection_set.pos,
            ));
        }
        result.push(CompiledMember::Scalar(match manifest_field {
            Some(field) => compiled_scalar(response_key, field, true),
            None => CompiledScalar {
                response_key: response_key.to_string(),
                field: "__typename".into(),
                codec: "string".into(),
                nullable: false,
                expose: true,
            },
        }));
    }
    if model.identity().is_some() {
        inject_wire_fields(&mut result, model, document, field.first.pos)?;
    }
    let mut object = CompiledObject {
        typename: model.typename.clone(),
        storage: compiled_storage(model),
        members: result,
    };
    inject_dependency_fields(
        &mut object,
        model,
        &relationship_source_fields,
        document,
        field.first.pos,
    )?;
    Ok(object)
}

pub(super) fn compiled_storage(model: &ClientModel) -> CompiledStorage {
    match model.identity() {
        Some(identity) => CompiledStorage::Normalized {
            model_id: model.id.clone(),
            identity_fields: identity.iter().map(|field| field.name.clone()).collect(),
        },
        None => CompiledStorage::Embedded,
    }
}

pub(super) fn compiled_relationship_plan(
    relationship: &ClientRelationship,
) -> CompiledRelationshipPlan {
    let maintenance = match &relationship.key_mapping {
        RelationshipKeyMapping::Direct { .. } | RelationshipKeyMapping::Through { .. } => {
            relationship.maintenance
        }
        RelationshipKeyMapping::ThroughOpaque { .. } | RelationshipKeyMapping::Embedded => {
            ClientRelationshipMaintenance::Revalidate
        }
    };
    CompiledRelationshipPlan {
        field: relationship.name.clone(),
        target_model: relationship.target_model.clone(),
        kind: relationship.kind,
        key_mapping: relationship.key_mapping.clone(),
        maintenance,
        dependencies: relationship.dependencies.clone(),
    }
}

pub(super) fn relationship_key_fields(mapping: &RelationshipKeyMapping) -> (&[String], &[String]) {
    match mapping {
        RelationshipKeyMapping::Direct { local, remote }
        | RelationshipKeyMapping::Through { local, remote, .. }
        | RelationshipKeyMapping::ThroughOpaque { local, remote, .. } => (local, remote),
        RelationshipKeyMapping::Embedded => (&[], &[]),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn compile_aggregate_object<'ast>(
    field: &MergedField<'ast>,
    model: &ClientModel,
    semantics: &ClientAggregateSemantics,
    arguments: &[ClientArgument],
    compiled_arguments: &BTreeMap<String, CompiledArgument>,
    filter_semantics: Option<&ClientFilterSemantics>,
    order_semantics: Option<&ClientOrderSemantics>,
    dependencies: &[String],
    manifest: &ClientManifest,
    variables: &[CompiledVariable],
    used_variables: &mut BTreeSet<String>,
    document: &ClientDocument,
    expander: &mut FragmentExpander<'ast, '_>,
    depth: usize,
) -> Result<CompiledObject, ClientCompileError> {
    let selected_fields = expander.merge_object(
        &field.selection_sets,
        &semantics.wrapper_typename,
        depth,
        "aggregate field",
    )?;
    if selected_fields.is_empty() {
        return Err(source_error(
            "client.selection.empty",
            format!(
                "aggregate field `{}` must select `aggregate`, `nodes`, or `__typename`",
                field.first.node.name.node
            ),
            document,
            field.first.node.selection_set.pos,
        ));
    }
    let mut members = Vec::with_capacity(selected_fields.len());
    for selected in selected_fields {
        let response_key = selected.first.node.response_key().node.to_string();
        let field_name = selected.first.node.name.node.as_str();
        if !selected.first.node.arguments.is_empty() {
            return Err(source_error(
                "client.selection.field_arguments",
                format!("aggregate member `{field_name}` must not have arguments"),
                document,
                selected.first.pos,
            ));
        }
        match field_name {
            "aggregate" if semantics.count => {
                if selected.selection_sets.is_empty() {
                    return Err(source_error(
                        "client.selection.object_required",
                        "aggregate summary requires an object selection",
                        document,
                        selected.first.node.selection_set.pos,
                    ));
                }
                members.push(CompiledMember::Branch(Box::new(CompiledBranch {
                    semantic: CompiledBranchSemantic::AggregateFields,
                    response_key,
                    field: "aggregate".into(),
                    cardinality: Cardinality::One,
                    nullable: true,
                    arguments: BTreeMap::new(),
                    dependencies: dependencies.to_vec(),
                    coverage: Some(complete_coverage()),
                    filter: None,
                    order: None,
                    pagination: None,
                    relationship: None,
                    selection: compile_aggregate_fields_object(
                        &selected,
                        semantics,
                        document,
                        expander,
                        depth + 1,
                    )?,
                })));
            }
            "nodes" if semantics.nodes => {
                if selected.selection_sets.is_empty() {
                    return Err(source_error(
                        "client.selection.object_required",
                        "aggregate nodes require an object selection",
                        document,
                        selected.first.node.selection_set.pos,
                    ));
                }
                let coverage = compile_coverage(
                    Some(&semantics.nodes_pagination),
                    arguments,
                    &format!("{}.nodes", field.first.node.name.node),
                    document,
                    selected.first.pos,
                )?;
                let mut selection = compile_model_object(
                    &selected,
                    model,
                    &ModelCompileContext::new(manifest, variables, document),
                    used_variables,
                    expander,
                    depth + 1,
                )?;
                let dependency_fields = query_plan_field_dependencies(
                    model,
                    filter_semantics,
                    order_semantics,
                    arguments,
                    compiled_arguments,
                );
                inject_dependency_fields(
                    &mut selection,
                    model,
                    &dependency_fields,
                    document,
                    selected.first.pos,
                )?;
                let (filter, order, pagination) = compile_query_plans(
                    model,
                    filter_semantics,
                    order_semantics,
                    arguments,
                    compiled_arguments,
                    variables,
                    manifest,
                    document,
                    &field.first.node,
                    coverage.as_ref(),
                    filter_depth_from_selection(depth, 2),
                    true,
                )?;
                members.push(CompiledMember::Branch(Box::new(CompiledBranch {
                    semantic: CompiledBranchSemantic::AggregateNodes,
                    response_key,
                    field: "nodes".into(),
                    cardinality: Cardinality::Many,
                    nullable: false,
                    arguments: BTreeMap::new(),
                    dependencies: dependencies.to_vec(),
                    coverage,
                    filter,
                    order,
                    pagination,
                    relationship: None,
                    selection,
                })));
            }
            "__typename" => {
                if !selected.selection_sets.is_empty() {
                    return Err(source_error(
                        "client.selection.scalar_nested",
                        "scalar field `__typename` cannot have a nested selection",
                        document,
                        selected.first.node.selection_set.pos,
                    ));
                }
                members.push(CompiledMember::Scalar(CompiledScalar {
                    response_key,
                    field: "__typename".into(),
                    codec: "string".into(),
                    nullable: false,
                    expose: true,
                }));
            }
            "aggregate" | "nodes" => {
                return Err(source_error(
                    "client.selection.aggregate_denied",
                    format!(
                        "aggregate member `{field_name}` is absent from the selected manifest semantics"
                    ),
                    document,
                    selected.first.node.name.pos,
                ));
            }
            _ => {
                return Err(source_error(
                    "client.selection.denied_or_unknown",
                    format!(
                        "field `{field_name}` is absent from aggregate type `{}`",
                        semantics.wrapper_typename
                    ),
                    document,
                    selected.first.node.name.pos,
                ));
            }
        }
    }
    Ok(CompiledObject {
        typename: semantics.wrapper_typename.clone(),
        storage: CompiledStorage::Embedded,
        members,
    })
}

pub(super) fn compile_aggregate_fields_object<'ast>(
    field: &MergedField<'ast>,
    semantics: &ClientAggregateSemantics,
    document: &ClientDocument,
    expander: &mut FragmentExpander<'ast, '_>,
    depth: usize,
) -> Result<CompiledObject, ClientCompileError> {
    let selected_fields = expander.merge_object(
        &field.selection_sets,
        &semantics.fields_typename,
        depth,
        "aggregate summary field",
    )?;
    if selected_fields.is_empty() {
        return Err(source_error(
            "client.selection.empty",
            "aggregate summary must select at least one field",
            document,
            field.first.node.selection_set.pos,
        ));
    }
    let mut members = Vec::with_capacity(selected_fields.len());
    for selected in selected_fields {
        let response_key = selected.first.node.response_key().node.to_string();
        let field_name = selected.first.node.name.node.as_str();
        if !selected.first.node.arguments.is_empty() || !selected.selection_sets.is_empty() {
            return Err(source_error(
                "client.selection.aggregate_metric_shape",
                format!("aggregate metric `{field_name}` must be a scalar leaf"),
                document,
                selected.first.pos,
            ));
        }
        match field_name {
            "count" if semantics.count => members.push(CompiledMember::Scalar(CompiledScalar {
                response_key,
                field: "count".into(),
                codec: "int32".into(),
                nullable: false,
                expose: true,
            })),
            "__typename" => members.push(CompiledMember::Scalar(CompiledScalar {
                response_key,
                field: "__typename".into(),
                codec: "string".into(),
                nullable: false,
                expose: true,
            })),
            "sum" | "avg" | "min" | "max" => {
                return Err(source_error(
                    "client.selection.aggregate_metric_unsupported",
                    format!(
                        "aggregate metric `{field_name}` needs a typed metric-object contract before it can be compiled"
                    ),
                    document,
                    selected.first.node.name.pos,
                ));
            }
            _ => {
                return Err(source_error(
                    "client.selection.denied_or_unknown",
                    format!(
                        "field `{field_name}` is absent from aggregate summary type `{}`",
                        semantics.fields_typename
                    ),
                    document,
                    selected.first.node.name.pos,
                ));
            }
        }
    }
    Ok(CompiledObject {
        typename: semantics.fields_typename.clone(),
        storage: CompiledStorage::Embedded,
        members,
    })
}

pub(super) fn compiled_scalar(
    response_key: &str,
    field: &ClientField,
    expose: bool,
) -> CompiledScalar {
    CompiledScalar {
        response_key: response_key.to_string(),
        field: field.name.clone(),
        codec: field.codec.clone(),
        nullable: field.nullable,
        expose,
    }
}
