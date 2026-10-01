use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value as JsonValue;

use super::*;

pub(crate) fn validate_input_default_generators_in_json(
    value: &JsonValue,
) -> Result<(), ClientCompileError> {
    let Some(commands) = value.get("commands").and_then(JsonValue::as_array) else {
        return Ok(());
    };
    for (command_index, command) in commands.iter().enumerate() {
        let command_name = command
            .get("name")
            .and_then(JsonValue::as_str)
            .unwrap_or("<unknown>");
        let Some(defaults) = command
            .pointer("/extensions/input_defaults/defaults")
            .and_then(JsonValue::as_array)
        else {
            continue;
        };
        for (default_index, default) in defaults.iter().enumerate() {
            if !matches!(
                default.get("generator").and_then(JsonValue::as_str),
                Some("uuid_v7" | "ulid")
            ) {
                return Err(ClientCompileError::manifest(
                    "client.manifest.input_default_generator",
                    format!(
                        "manifest command `{command_name}` input default {default_index} must use uuid_v7 or ulid (commands[{command_index}])"
                    ),
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn canonicalize_surface(
    surface: &mut ManifestSurface,
) -> Result<(), ClientCompileError> {
    match surface {
        ManifestSurface::Role { name } => {
            validate_nonempty(name, "manifest.surface.name")?;
        }
        ManifestSurface::Application {
            name,
            eligible_roles,
            schema_roles,
        } => {
            validate_nonempty(name, "manifest.surface.name")?;
            if eligible_roles.is_empty() {
                return Err(ClientCompileError::manifest(
                    "client.manifest.surface_eligible_roles",
                    format!("application surface `{name}` must declare at least one role"),
                ));
            }
            canonicalize_string_set(
                eligible_roles,
                &format!("application surface `{name}` eligible role"),
            )?;
            if schema_roles.is_empty() {
                return Err(ClientCompileError::manifest(
                    "client.manifest.surface_schema_roles",
                    format!("application surface `{name}` must declare at least one schema role"),
                ));
            }
            canonicalize_string_set(
                schema_roles,
                &format!("application surface `{name}` schema role"),
            )?;
            if schema_roles
                .iter()
                .any(|role| !eligible_roles.iter().any(|eligible| eligible == role))
            {
                return Err(ClientCompileError::manifest(
                    "client.manifest.surface_schema_roles",
                    format!(
                        "application surface `{name}` schema roles must be a subset of eligible roles"
                    ),
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn canonicalize_string_set(
    values: &mut [String],
    label: &str,
) -> Result<(), ClientCompileError> {
    validate_nonempty_strings(values, label)?;
    values.sort();
    if values.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(ClientCompileError::manifest(
            "client.manifest.duplicate_entry",
            format!("{label} entries must be unique"),
        ));
    }
    Ok(())
}

pub(crate) fn validate_nonempty_strings(
    values: &[String],
    label: &str,
) -> Result<(), ClientCompileError> {
    for value in values {
        validate_nonempty(value, label)?;
    }
    Ok(())
}

pub(crate) fn require_dependency(
    dependencies: &[String],
    required: &str,
    owner: &str,
) -> Result<(), ClientCompileError> {
    if dependencies.iter().any(|dependency| dependency == required) {
        return Ok(());
    }
    Err(ClientCompileError::manifest(
        "client.manifest.dependency",
        format!("{owner} must include invalidation dependency `{required}`"),
    ))
}

pub(crate) fn validate_graphql_name(value: &str, label: &str) -> Result<(), ClientCompileError> {
    if !super::is_graphql_name(value) || value.starts_with("__") {
        return Err(ClientCompileError::manifest(
            "client.manifest.graphql_name",
            format!("{label} `{value}` must be a valid GraphQL name"),
        ));
    }
    Ok(())
}

pub(crate) fn validate_surface(
    actual: &ManifestSurface,
    expected: &ClientSurfaceSelector,
) -> Result<(), ClientCompileError> {
    let matches = match (actual, expected) {
        (
            ManifestSurface::Role { name: actual },
            ClientSurfaceSelector::Role { name: expected },
        ) => !expected.trim().is_empty() && actual == expected,
        (
            ManifestSurface::Application {
                name: actual,
                eligible_roles,
                schema_roles,
            },
            ClientSurfaceSelector::Application {
                name: expected,
                eligible_roles: expected_eligible_roles,
                schema_roles: expected_schema_roles,
            },
        ) => {
            !expected.trim().is_empty()
                && actual == expected
                && eligible_roles == expected_eligible_roles
                && schema_roles == expected_schema_roles
                && !eligible_roles.is_empty()
                && eligible_roles.iter().all(|role| !role.trim().is_empty())
                && !schema_roles.is_empty()
                && schema_roles.iter().all(|role| !role.trim().is_empty())
                && schema_roles
                    .iter()
                    .all(|role| eligible_roles.iter().any(|eligible| eligible == role))
        }
        _ => false,
    };
    if matches {
        return Ok(());
    }
    let actual_label = match actual {
        ManifestSurface::Role { name } => format!("role `{name}`"),
        ManifestSurface::Application { name, .. } => format!("application `{name}`"),
    };
    let expected_label = match expected {
        ClientSurfaceSelector::Role { name } => format!("role `{name}`"),
        ClientSurfaceSelector::Application {
            name,
            eligible_roles,
            schema_roles,
        } => format!(
            "application `{name}` (eligible roles [{}], schema roles [{}])",
            eligible_roles.join(", "),
            schema_roles.join(", ")
        ),
    };
    Err(ClientCompileError::manifest(
        "client.manifest.surface_mismatch",
        format!(
            "selected manifest surface is {actual_label}; compiler was explicitly requested for {expected_label}"
        ),
    ))
}

pub(crate) fn validate_capabilities(
    capabilities: &ManifestCapabilities,
) -> Result<(), ClientCompileError> {
    if capabilities.query_fallback != "revalidate" {
        return Err(ClientCompileError::manifest(
            "client.manifest.query_fallback",
            format!(
                "unsupported query fallback `{}`; manifest v7 requires `revalidate`",
                capabilities.query_fallback
            ),
        ));
    }
    if capabilities.live_resume && !capabilities.live_queries {
        return Err(ClientCompileError::manifest(
            "client.manifest.live_resume",
            "capabilities.live_resume requires capabilities.live_queries",
        ));
    }
    if capabilities.tombstones && !capabilities.record_revisions {
        return Err(ClientCompileError::manifest(
            "client.manifest.tombstone_capability",
            "capabilities.tombstones requires capabilities.record_revisions",
        ));
    }
    Ok(())
}

pub(crate) fn validate_execution_limits(
    execution: &ManifestExecutionLimits,
) -> Result<(), ClientCompileError> {
    const JS_MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
    for (name, value) in [
        ("max_depth", execution.max_depth),
        ("max_bool_width", execution.max_bool_width),
        ("max_in_list", execution.max_in_list),
    ] {
        if value > JS_MAX_SAFE_INTEGER {
            return Err(ClientCompileError::manifest(
                "client.manifest.execution_js_integer",
                format!("execution.{name} `{value}` exceeds JavaScript's exact integer range"),
            ));
        }
    }
    if execution.complexity.version != 1 {
        return Err(ClientCompileError::manifest(
            "client.manifest.complexity_version",
            format!(
                "unsupported query complexity contract version {}; distributed requires version 1",
                execution.complexity.version
            ),
        ));
    }
    let weights = [
        ("scalar", execution.complexity.scalar),
        ("belongs_to", execution.complexity.belongs_to),
        ("has_many", execution.complexity.has_many),
        ("m2m", execution.complexity.m2m),
        ("aggregate", execution.complexity.aggregate),
        ("list_root", execution.complexity.list_root),
        ("by_pk", execution.complexity.by_pk),
        ("list_fanout", execution.complexity.list_fanout),
    ];
    if let Some((name, _)) = weights.into_iter().find(|(_, value)| *value == 0) {
        return Err(ClientCompileError::manifest(
            "client.manifest.complexity_weight",
            format!("query complexity weight `{name}` must be greater than zero"),
        ));
    }
    Ok(())
}

pub(crate) fn validate_derived_capabilities(
    capabilities: &ManifestCapabilities,
    roots: &BTreeMap<(RootOperation, String), ManifestRoot>,
    commands: &[ManifestCommand],
) -> Result<(), ClientCompileError> {
    let has_live_roots = roots
        .keys()
        .any(|(operation, _)| *operation == RootOperation::Subscription);
    if capabilities.live_queries != has_live_roots {
        return Err(ClientCompileError::manifest(
            "client.manifest.live_capability",
            "capabilities.live_queries must exactly describe the subscription-root inventory",
        ));
    }
    let has_commands = !commands.is_empty();
    if capabilities.causal_receipts != has_commands || !capabilities.cache_scope {
        return Err(ClientCompileError::manifest(
            "client.manifest.command_capability",
            "causal_receipts must agree with command inventory and cache_scope must be enabled for every generated surface",
        ));
    }
    if capabilities.confirmed_persistence {
        return Err(ClientCompileError::manifest(
            "client.manifest.persistence_capability",
            "manifest v7 does not yet support confirmed client persistence",
        ));
    }
    Ok(())
}

pub(crate) fn validate_scalar_codecs(
    codecs: Vec<ManifestScalarCodec>,
) -> Result<BTreeMap<String, String>, ClientCompileError> {
    if codecs.is_empty() {
        return Err(ClientCompileError::manifest(
            "client.manifest.scalar_codecs",
            "manifest.scalar_codecs must declare the complete authorized scalar inventory",
        ));
    }
    let supported = [
        ("BigInt", "json_number_precision_limited"),
        ("Boolean", "boolean"),
        ("Bytea", "base64"),
        ("Float", "float64"),
        ("ID", "string"),
        ("Int", "int32"),
        ("JSON", "json"),
        ("String", "string"),
        ("Timestamptz", "string_unvalidated_timestamp"),
    ]
    .into_iter()
    .collect::<BTreeMap<_, _>>();
    let mut result = BTreeMap::new();
    for entry in codecs {
        validate_nonempty(&entry.scalar, "manifest scalar")?;
        validate_nonempty(&entry.codec, "manifest scalar codec")?;
        let expected = supported.get(entry.scalar.as_str()).ok_or_else(|| {
            ClientCompileError::manifest(
                "client.manifest.scalar_unsupported",
                format!(
                    "scalar `{}` has no fail-closed TypeScript codec in this compiler",
                    entry.scalar
                ),
            )
        })?;
        if entry.codec != *expected {
            return Err(ClientCompileError::manifest(
                "client.manifest.codec_unsupported",
                format!(
                    "scalar `{}` declares codec `{}`; compiler requires `{expected}`",
                    entry.scalar, entry.codec
                ),
            ));
        }
        if result
            .insert(entry.scalar.clone(), entry.codec.clone())
            .is_some()
        {
            return Err(ClientCompileError::manifest(
                "client.manifest.duplicate_scalar",
                format!("manifest repeats scalar codec `{}`", entry.scalar),
            ));
        }
    }
    if result.len() != supported.len()
        || supported.keys().any(|scalar| !result.contains_key(*scalar))
    {
        return Err(ClientCompileError::manifest(
            "client.manifest.scalar_codecs",
            "manifest.scalar_codecs must contain the exact v4 scalar inventory",
        ));
    }
    Ok(result)
}

pub(crate) fn canonicalize_model(model: &mut ManifestModel) -> Result<(), ClientCompileError> {
    canonicalize_string_set(
        &mut model.dependencies,
        &format!("model `{}` dependency", model.id),
    )?;
    model
        .fields
        .sort_by(|left, right| left.name.cmp(&right.name));
    model
        .relationships
        .sort_by(|left, right| left.name.cmp(&right.name));
    canonicalize_filter_input(&mut model.filter_input)?;
    canonicalize_row_policy(&mut model.row_policy);
    for relationship in &mut model.relationships {
        canonicalize_arguments(
            &mut relationship.arguments,
            &format!(
                "model `{}` relationship `{}` argument",
                model.id, relationship.name
            ),
        )?;
        canonicalize_string_set(
            &mut relationship.dependencies,
            &format!(
                "model `{}` relationship `{}` dependency",
                model.id, relationship.name
            ),
        )?;
        if let Some(filter) = &mut relationship.filter {
            canonicalize_filter_semantics(filter)?;
        }
        if let Some(order) = &mut relationship.order {
            canonicalize_order_semantics(order)?;
        }
        if let Some(aggregate) = &mut relationship.aggregate {
            canonicalize_arguments(
                &mut aggregate.arguments,
                &format!(
                    "model `{}` relationship `{}` aggregate argument",
                    model.id, relationship.name
                ),
            )?;
            canonicalize_aggregate_semantics(&mut aggregate.semantics)?;
            canonicalize_string_set(
                &mut aggregate.dependencies,
                &format!(
                    "model `{}` relationship `{}` aggregate dependency",
                    model.id, relationship.name
                ),
            )?;
        }
    }
    Ok(())
}

pub(crate) fn canonicalize_row_policy(policy: &mut ManifestRowPolicy) {
    if let ManifestRowPolicy::Predicate { expression } = policy {
        canonicalize_filter_expression(expression);
    }
}

pub(crate) fn canonicalize_filter_expression(expression: &mut ManifestFilterExpr) {
    match expression {
        ManifestFilterExpr::And(expressions) | ManifestFilterExpr::Or(expressions) => {
            for expression in expressions {
                canonicalize_filter_expression(expression);
            }
        }
        ManifestFilterExpr::Not(expression) => canonicalize_filter_expression(expression),
        ManifestFilterExpr::Cmp { rhs, .. } => canonicalize_operand(rhs),
        ManifestFilterExpr::In { values, .. } => {
            for operand in values {
                canonicalize_operand(operand);
            }
        }
        ManifestFilterExpr::Rel { predicate, .. } => {
            canonicalize_filter_expression(predicate);
        }
        ManifestFilterExpr::IsNull { .. } => {}
    }
}

pub(crate) fn canonicalize_operand(operand: &mut ManifestOperand) {
    if let ManifestOperand::Lit(ManifestLitValue::Json(value)) = operand {
        *value = canonical_json_value(std::mem::take(value));
    }
}

pub(crate) fn canonicalize_filter_semantics(
    semantics: &mut ManifestFilterSemantics,
) -> Result<(), ClientCompileError> {
    canonicalize_filter_fields(&mut semantics.fields)?;
    canonicalize_string_set(&mut semantics.relationships, "filter relationship")?;
    canonicalize_row_policy(&mut semantics.row_policy);
    Ok(())
}

pub(crate) fn canonicalize_filter_input(
    input: &mut ManifestFilterInput,
) -> Result<(), ClientCompileError> {
    canonicalize_filter_fields(&mut input.fields)?;
    input
        .relationships
        .sort_by(|left, right| left.field.cmp(&right.field));
    Ok(())
}

pub(crate) fn canonicalize_filter_fields(
    fields: &mut [ManifestFilterField],
) -> Result<(), ClientCompileError> {
    fields.sort_by(|left, right| left.name.cmp(&right.name));
    for field in fields {
        canonicalize_string_set(
            &mut field.operators,
            &format!("filter field `{}` operator", field.name),
        )?;
    }
    Ok(())
}

pub(crate) fn canonicalize_order_semantics(
    semantics: &mut ManifestOrderSemantics,
) -> Result<(), ClientCompileError> {
    canonicalize_string_set(&mut semantics.fields, "order field")?;
    canonicalize_string_set(&mut semantics.values, "order value")
}

pub(crate) fn canonicalize_aggregate_semantics(
    semantics: &mut ManifestAggregateSemantics,
) -> Result<(), ClientCompileError> {
    canonicalize_string_set(&mut semantics.sum, "aggregate sum field")?;
    canonicalize_string_set(&mut semantics.avg, "aggregate avg field")?;
    canonicalize_string_set(&mut semantics.min, "aggregate min field")?;
    canonicalize_string_set(&mut semantics.max, "aggregate max field")
}

pub(crate) fn canonicalize_arguments(
    arguments: &mut [ManifestArgument],
    _label: &str,
) -> Result<(), ClientCompileError> {
    arguments.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(())
}

pub(crate) fn validate_model(
    model: &ManifestModel,
    scalar_codecs: &BTreeMap<String, String>,
) -> Result<(), ClientCompileError> {
    validate_graphql_name(&model.id, "manifest model id")?;
    validate_graphql_name(&model.typename, "manifest model typename")?;
    validate_nonempty(&model.source_table, "manifest model source_table")?;
    validate_graphql_name(
        &model.filter_input.type_name,
        "manifest model filter input type",
    )?;
    validate_nonempty_strings(
        &model.dependencies,
        &format!("model `{}` dependency", model.id),
    )?;
    if !model
        .dependencies
        .iter()
        .any(|dependency| dependency == &model.source_table)
    {
        return Err(ClientCompileError::manifest(
            "client.manifest.model_dependency",
            format!(
                "model `{}` dependencies must include source table `{}`",
                model.id, model.source_table
            ),
        ));
    }
    if model.tombstones && !model.record_revisions {
        return Err(ClientCompileError::manifest(
            "client.manifest.model_tombstones",
            format!(
                "model `{}` cannot expose tombstones without record revisions",
                model.id
            ),
        ));
    }
    let mut names = BTreeSet::new();
    for field in &model.fields {
        validate_graphql_name(&field.name, "manifest model field")?;
        validate_graphql_name(&field.scalar, "manifest field scalar")?;
        validate_nonempty(&field.codec, "manifest field codec")?;
        match scalar_codecs.get(&field.scalar) {
            Some(codec) if codec == &field.codec => {}
            Some(codec) => {
                return Err(ClientCompileError::manifest(
                    "client.manifest.field_codec",
                    format!(
                        "model `{}` field `{}` codec `{}` does not match scalar `{}` inventory codec `{codec}`",
                        model.id, field.name, field.codec, field.scalar
                    ),
                ));
            }
            None => {
                return Err(ClientCompileError::manifest(
                    "client.manifest.field_scalar",
                    format!(
                        "model `{}` field `{}` uses scalar `{}` absent from manifest.scalar_codecs",
                        model.id, field.name, field.scalar
                    ),
                ));
            }
        }
        if !names.insert(field.name.as_str()) {
            return Err(ClientCompileError::manifest(
                "client.manifest.duplicate_field",
                format!("model `{}` repeats field `{}`", model.id, field.name),
            ));
        }
    }
    for relationship in &model.relationships {
        validate_graphql_name(&relationship.name, "manifest relationship")?;
        if !names.insert(relationship.name.as_str()) {
            return Err(ClientCompileError::manifest(
                "client.manifest.duplicate_member",
                format!(
                    "model `{}` repeats field/relationship `{}`",
                    model.id, relationship.name
                ),
            ));
        }
        validate_graphql_name(
            &relationship.target_model,
            "manifest relationship target model",
        )?;
        validate_graphql_name(
            &relationship.target_typename,
            "manifest relationship target typename",
        )?;
        validate_unique_arguments_for(
            &relationship.arguments,
            scalar_codecs,
            &format!("model `{}` relationship `{}`", model.id, relationship.name),
        )?;
        validate_nonempty_strings(
            &relationship.dependencies,
            &format!(
                "model `{}` relationship `{}` dependency",
                model.id, relationship.name
            ),
        )?;
    }
    match &model.normalization {
        ManifestNormalization::Embedded => {}
        ManifestNormalization::Normalized { fields, encoding } => {
            if encoding != "canonical_json_tuple_v1" {
                return Err(ClientCompileError::manifest(
                    "client.manifest.identity_encoding",
                    format!(
                        "model `{}` uses unsupported identity encoding `{encoding}`",
                        model.id
                    ),
                ));
            }
            if fields.is_empty() {
                return Err(ClientCompileError::manifest(
                    "client.manifest.empty_identity",
                    format!("normalized model `{}` has no identity fields", model.id),
                ));
            }
            let mut identities = BTreeSet::new();
            for identity in fields {
                validate_graphql_name(&identity.name, "manifest identity field")?;
                validate_nonempty(&identity.codec, "manifest identity codec")?;
                if !identities.insert(identity.name.as_str()) {
                    return Err(ClientCompileError::manifest(
                        "client.manifest.duplicate_identity",
                        format!(
                            "model `{}` repeats identity field `{}`",
                            model.id, identity.name
                        ),
                    ));
                }
                let Some(field) = model.field(&identity.name) else {
                    return Err(ClientCompileError::manifest(
                        "client.manifest.identity_field",
                        format!(
                            "model `{}` identity field `{}` is absent from its authorized fields",
                            model.id, identity.name
                        ),
                    ));
                };
                if field.nullable || field.codec != identity.codec {
                    return Err(ClientCompileError::manifest(
                        "client.manifest.identity_codec",
                        format!(
                            "model `{}` identity field `{}` must be non-null and match codec `{}`",
                            model.id, identity.name, identity.codec
                        ),
                    ));
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_model_graph(
    models: &BTreeMap<String, ManifestModel>,
    scalar_codecs: &BTreeMap<String, String>,
) -> Result<(), ClientCompileError> {
    for model in models.values() {
        validate_row_policy(&model.row_policy, model, models)?;
        validate_filter_input(&model.filter_input, model, models)?;
        for relationship in &model.relationships {
            let target = models.get(&relationship.target_model).ok_or_else(|| {
                ClientCompileError::manifest(
                    "client.manifest.relationship_target",
                    format!(
                        "model `{}` relationship `{}` targets absent model `{}`",
                        model.id, relationship.name, relationship.target_model
                    ),
                )
            })?;
            if relationship.target_typename != target.typename {
                return Err(ClientCompileError::manifest(
                    "client.manifest.relationship_typename",
                    format!(
                        "model `{}` relationship `{}` target typename `{}` does not match model `{}` typename `{}`",
                        model.id,
                        relationship.name,
                        relationship.target_typename,
                        target.id,
                        target.typename
                    ),
                ));
            }
            require_dependency(
                &relationship.dependencies,
                &model.source_table,
                &format!("relationship {}.{}", model.id, relationship.name),
            )?;
            require_dependency(
                &relationship.dependencies,
                &target.source_table,
                &format!("relationship {}.{}", model.id, relationship.name),
            )?;
            if let ManifestRelationshipKeyMapping::Through { table, .. } = &relationship.key_mapping
            {
                require_dependency(
                    &relationship.dependencies,
                    table,
                    &format!("relationship {}.{}", model.id, relationship.name),
                )?;
            }
            match relationship.kind {
                ManifestRelationshipKind::BelongsTo if relationship.list => {
                    return Err(ClientCompileError::manifest(
                        "client.manifest.relationship_cardinality",
                        format!(
                            "belongs_to relationship `{}.{}` cannot be a list",
                            model.id, relationship.name
                        ),
                    ));
                }
                ManifestRelationshipKind::HasMany | ManifestRelationshipKind::ManyToMany
                    if !relationship.list =>
                {
                    return Err(ClientCompileError::manifest(
                        "client.manifest.relationship_cardinality",
                        format!(
                            "{:?} relationship `{}.{}` must be a list",
                            relationship.kind, model.id, relationship.name
                        ),
                    ));
                }
                _ => {}
            }
            if relationship.list && relationship.nullable {
                return Err(ClientCompileError::manifest(
                    "client.manifest.relationship_nullability",
                    format!(
                        "list relationship `{}.{}` must be a non-null collection",
                        model.id, relationship.name
                    ),
                ));
            }
            validate_key_mapping(model, target, relationship)?;
            validate_relationship_semantics(model, target, relationship, models, scalar_codecs)?;
        }
    }
    Ok(())
}

pub(crate) fn validate_key_mapping(
    source: &ManifestModel,
    target: &ManifestModel,
    relationship: &ManifestRelationship,
) -> Result<(), ClientCompileError> {
    let validate_fields = |local: &[String], remote: &[String]| {
        if local.is_empty() || local.len() != remote.len() {
            return Err(ClientCompileError::manifest(
                "client.manifest.relationship_key_mapping",
                format!(
                    "relationship `{}.{}` key mapping must contain equally sized non-empty local and remote fields",
                    source.id, relationship.name
                ),
            ));
        }
        for field in local {
            if source.field(field).is_none() {
                return Err(ClientCompileError::manifest(
                    "client.manifest.relationship_local_key",
                    format!(
                        "relationship `{}.{}` references absent local field `{field}`",
                        source.id, relationship.name
                    ),
                ));
            }
        }
        for field in remote {
            if target.field(field).is_none() {
                return Err(ClientCompileError::manifest(
                    "client.manifest.relationship_remote_key",
                    format!(
                        "relationship `{}.{}` references absent target field `{field}`",
                        source.id, relationship.name
                    ),
                ));
            }
        }
        for (local_field, remote_field) in local.iter().zip(remote) {
            let local_contract = source
                .field(local_field)
                .expect("local relationship field checked above");
            let remote_contract = target
                .field(remote_field)
                .expect("remote relationship field checked above");
            if local_contract.scalar == "BigInt"
                || remote_contract.scalar == "BigInt"
                || local_contract.codec != remote_contract.codec
            {
                return Err(ClientCompileError::manifest(
                    "client.manifest.relationship_key_codec",
                    format!(
                        "relationship {}.{} local and target keys must use matching portable codecs",
                        source.id, relationship.name
                    ),
                ));
            }
        }
        Ok(())
    };
    let local_maintenance = matches!(
        relationship.key_mapping,
        ManifestRelationshipKeyMapping::Direct { .. }
            | ManifestRelationshipKeyMapping::Through { .. }
    );
    if local_maintenance
        != matches!(
            relationship.maintenance,
            ManifestRelationshipMaintenance::Local
        )
    {
        return Err(ClientCompileError::manifest(
            "client.manifest.relationship_maintenance",
            format!(
                "relationship `{}.{}` maintenance does not match its key mapping",
                source.id, relationship.name
            ),
        ));
    }
    match &relationship.key_mapping {
        ManifestRelationshipKeyMapping::Direct { local, remote } => validate_fields(local, remote),
        ManifestRelationshipKeyMapping::Through {
            local,
            remote,
            table,
            source_foreign_key,
            target_foreign_key,
        } => {
            validate_fields(local, remote)?;
            validate_nonempty(table, "relationship through table")?;
            if source_foreign_key.is_empty() || source_foreign_key.len() != local.len() {
                return Err(ClientCompileError::manifest(
                    "client.manifest.relationship_keys",
                    "relationship through source foreign key must list one through column per local key",
                ));
            }
            if target_foreign_key.is_empty() || target_foreign_key.len() != remote.len() {
                return Err(ClientCompileError::manifest(
                    "client.manifest.relationship_keys",
                    "relationship through target foreign key must list one through column per remote key",
                ));
            }
            validate_nonempty_strings(
                source_foreign_key,
                "relationship through source foreign key",
            )?;
            validate_nonempty_strings(
                target_foreign_key,
                "relationship through target foreign key",
            )
        }
        ManifestRelationshipKeyMapping::ThroughOpaque {
            local,
            remote,
            dependency,
        } => {
            validate_fields(local, remote)?;
            validate_nonempty(dependency, "opaque relationship dependency")?;
            if !relationship
                .dependencies
                .iter()
                .any(|candidate| candidate == dependency)
            {
                return Err(ClientCompileError::manifest(
                    "client.manifest.relationship_dependency",
                    format!(
                        "relationship `{}.{}` opaque dependency `{dependency}` is absent from its dependency set",
                        source.id, relationship.name
                    ),
                ));
            }
            Ok(())
        }
        ManifestRelationshipKeyMapping::Embedded => Ok(()),
    }
}

pub(crate) fn validate_relationship_semantics(
    source: &ManifestModel,
    target: &ManifestModel,
    relationship: &ManifestRelationship,
    models: &BTreeMap<String, ManifestModel>,
    scalar_codecs: &BTreeMap<String, String>,
) -> Result<(), ClientCompileError> {
    validate_filter_argument_type(
        &relationship.arguments,
        target,
        &format!("relationship `{}.{}`", source.id, relationship.name),
    )?;
    let has_filter_argument = relationship
        .arguments
        .iter()
        .any(|argument| argument.kind == ManifestArgumentKind::Filter);
    let has_order_argument = relationship
        .arguments
        .iter()
        .any(|argument| argument.kind == ManifestArgumentKind::Order);
    let has_limit_argument = relationship
        .arguments
        .iter()
        .any(|argument| argument.kind == ManifestArgumentKind::Limit);
    let has_offset_argument = relationship
        .arguments
        .iter()
        .any(|argument| argument.kind == ManifestArgumentKind::Offset);
    if relationship.filter.is_some() != has_filter_argument
        || relationship.order.is_some() != has_order_argument
        || relationship.pagination.is_some() != (has_limit_argument && has_offset_argument)
    {
        return Err(ClientCompileError::manifest(
            "client.manifest.relationship_arguments",
            format!(
                "relationship {}.{} arguments do not match its filter/order/pagination semantics",
                source.id, relationship.name
            ),
        ));
    }
    if relationship.list {
        let filter = relationship.filter.as_ref().ok_or_else(|| {
            ClientCompileError::manifest(
                "client.manifest.relationship_filter",
                format!(
                    "list relationship `{}.{}` requires filter semantics",
                    source.id, relationship.name
                ),
            )
        })?;
        validate_filter_semantics(filter, target, models)?;
        let order = relationship.order.as_ref().ok_or_else(|| {
            ClientCompileError::manifest(
                "client.manifest.relationship_order",
                format!(
                    "list relationship `{}.{}` requires order semantics",
                    source.id, relationship.name
                ),
            )
        })?;
        validate_order_semantics(order, target)?;
        validate_pagination(
            relationship.pagination.as_ref().ok_or_else(|| {
                ClientCompileError::manifest(
                    "client.manifest.relationship_pagination",
                    format!(
                        "list relationship `{}.{}` requires pagination semantics",
                        source.id, relationship.name
                    ),
                )
            })?,
            &format!("relationship `{}.{}`", source.id, relationship.name),
        )?;
    } else if relationship.filter.is_some()
        || relationship.order.is_some()
        || relationship.pagination.is_some()
    {
        return Err(ClientCompileError::manifest(
            "client.manifest.relationship_list_semantics",
            format!(
                "singular relationship `{}.{}` cannot declare filter, order, or pagination semantics",
                source.id, relationship.name
            ),
        ));
    }
    if relationship.live && !relationship.list {
        return Err(ClientCompileError::manifest(
            "client.manifest.relationship_live",
            format!(
                "singular relationship `{}.{}` cannot be marked live",
                source.id, relationship.name
            ),
        ));
    }
    if let Some(aggregate) = &relationship.aggregate {
        if !relationship.list {
            return Err(ClientCompileError::manifest(
                "client.manifest.relationship_aggregate",
                format!(
                    "singular relationship `{}.{}` cannot expose an aggregate",
                    source.id, relationship.name
                ),
            ));
        }
        validate_graphql_name(&aggregate.name, "relationship aggregate name")?;
        validate_graphql_name(
            &aggregate.semantics.wrapper_typename,
            "relationship aggregate wrapper type",
        )?;
        validate_graphql_name(
            &aggregate.semantics.fields_typename,
            "relationship aggregate fields type",
        )?;
        validate_unique_arguments_for(
            &aggregate.arguments,
            scalar_codecs,
            &format!("relationship aggregate {}.{}", source.id, aggregate.name),
        )?;
        validate_nonempty_strings(&aggregate.dependencies, "relationship aggregate dependency")?;
        for dependency in &relationship.dependencies {
            require_dependency(
                &aggregate.dependencies,
                dependency,
                &format!("relationship aggregate {}.{}", source.id, aggregate.name),
            )?;
        }
        validate_aggregate_semantics(&aggregate.semantics, target)?;
    }
    Ok(())
}

pub(crate) fn validate_filter_input(
    input: &ManifestFilterInput,
    model: &ManifestModel,
    models: &BTreeMap<String, ManifestModel>,
) -> Result<(), ClientCompileError> {
    validate_filter_fields(&input.fields, model)?;
    let expected_relationships = model
        .relationships
        .iter()
        .map(|relationship| relationship.name.as_str())
        .collect::<BTreeSet<_>>();
    let actual_relationships = input
        .relationships
        .iter()
        .map(|relationship| relationship.field.as_str())
        .collect::<BTreeSet<_>>();
    if actual_relationships != expected_relationships
        || input.relationships.len() != expected_relationships.len()
    {
        return Err(ClientCompileError::manifest(
            "client.manifest.filter_input_relationships",
            format!(
                "model `{}` filter input must describe every authorized relationship exactly once",
                model.id
            ),
        ));
    }
    for relationship_input in &input.relationships {
        validate_graphql_name(
            &relationship_input.field,
            "manifest filter input relationship field",
        )?;
        validate_graphql_name(
            &relationship_input.target_type,
            "manifest filter input relationship target type",
        )?;
        let relationship = model
            .relationship(&relationship_input.field)
            .expect("filter input relationship inventory checked above");
        let target = models.get(&relationship.target_model).ok_or_else(|| {
            ClientCompileError::manifest(
                "client.manifest.filter_input_target",
                format!(
                    "model `{}` filter relationship `{}` targets absent model `{}`",
                    model.id, relationship.name, relationship.target_model
                ),
            )
        })?;
        if relationship_input.target_type != target.filter_input.type_name {
            return Err(ClientCompileError::manifest(
                "client.manifest.filter_input_target_type",
                format!(
                    "model `{}` filter relationship `{}` target type `{}` does not match model `{}` filter input `{}`",
                    model.id,
                    relationship.name,
                    relationship_input.target_type,
                    target.id,
                    target.filter_input.type_name
                ),
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_filter_fields(
    fields: &[ManifestFilterField],
    model: &ManifestModel,
) -> Result<(), ClientCompileError> {
    let expected_fields = model
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<BTreeSet<_>>();
    let actual_fields = fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<BTreeSet<_>>();
    if actual_fields != expected_fields || fields.len() != expected_fields.len() {
        return Err(ClientCompileError::manifest(
            "client.manifest.filter_fields",
            format!(
                "filter input for model `{}` must describe every authorized scalar field exactly once",
                model.id
            ),
        ));
    }
    for field in fields {
        validate_nonempty_strings(
            &field.operators,
            &format!("model `{}` filter operator", model.id),
        )?;
        if field.operators.is_empty() {
            return Err(ClientCompileError::manifest(
                "client.manifest.filter_operators",
                format!(
                    "model `{}` filter field `{}` has no supported operators",
                    model.id, field.name
                ),
            ));
        }
        if field.operators.iter().any(|operator| {
            !matches!(
                operator.as_str(),
                "_eq"
                    | "_neq"
                    | "_gt"
                    | "_gte"
                    | "_lt"
                    | "_lte"
                    | "_in"
                    | "_nin"
                    | "_is_null"
                    | "_like"
                    | "_ilike"
                    | "_icontains"
                    | "_contains"
                    | "_contained_in"
                    | "_has_key"
            )
        }) {
            return Err(ClientCompileError::manifest(
                "client.manifest.filter_operator",
                format!(
                    "model `{}` filter field `{}` declares an unknown comparison operator",
                    model.id, field.name
                ),
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_filter_semantics(
    semantics: &ManifestFilterSemantics,
    model: &ManifestModel,
    models: &BTreeMap<String, ManifestModel>,
) -> Result<(), ClientCompileError> {
    if semantics.fields != model.filter_input.fields {
        return Err(ClientCompileError::manifest(
            "client.manifest.filter_contract",
            format!(
                "filter semantics for model `{}` do not match its authoritative filter input fields",
                model.id
            ),
        ));
    }
    let input_relationships = model
        .filter_input
        .relationships
        .iter()
        .map(|relationship| relationship.field.as_str())
        .collect::<Vec<_>>();
    if semantics
        .relationships
        .iter()
        .map(String::as_str)
        .ne(input_relationships)
    {
        return Err(ClientCompileError::manifest(
            "client.manifest.filter_contract",
            format!(
                "filter semantics for model `{}` do not match its authoritative filter input relationships",
                model.id
            ),
        ));
    }
    if semantics.row_policy != model.row_policy {
        return Err(ClientCompileError::manifest(
            "client.manifest.filter_row_policy",
            format!(
                "filter semantics for model `{}` do not preserve its row policy",
                model.id
            ),
        ));
    }
    validate_row_policy(&semantics.row_policy, model, models)
}

pub(crate) fn validate_filter_argument_type(
    arguments: &[ManifestArgument],
    model: &ManifestModel,
    owner: &str,
) -> Result<(), ClientCompileError> {
    let Some(argument) = arguments
        .iter()
        .find(|argument| argument.kind == ManifestArgumentKind::Filter)
    else {
        return Ok(());
    };
    if argument.list || argument.type_name != model.filter_input.type_name {
        return Err(ClientCompileError::manifest(
            "client.manifest.filter_argument_type",
            format!(
                "{owner} filter argument `{}` must use non-list input `{}`, received `{}`",
                argument.name, model.filter_input.type_name, argument.type_name
            ),
        ));
    }
    Ok(())
}

pub(crate) fn validate_order_semantics(
    semantics: &ManifestOrderSemantics,
    model: &ManifestModel,
) -> Result<(), ClientCompileError> {
    let expected_fields = model
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<BTreeSet<_>>();
    let actual_fields = semantics
        .fields
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if actual_fields != expected_fields || semantics.fields.len() != expected_fields.len() {
        return Err(ClientCompileError::manifest(
            "client.manifest.order_fields",
            format!(
                "order semantics for model `{}` must describe every authorized scalar field exactly once",
                model.id
            ),
        ));
    }
    let expected_values = [
        "asc",
        "asc_nulls_first",
        "asc_nulls_last",
        "desc",
        "desc_nulls_first",
        "desc_nulls_last",
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    let actual_values = semantics
        .values
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if actual_values != expected_values || semantics.values.len() != expected_values.len() {
        return Err(ClientCompileError::manifest(
            "client.manifest.order_values",
            format!(
                "order semantics for model `{}` must use the manifest v7 direction set",
                model.id
            ),
        ));
    }
    Ok(())
}

pub(crate) fn validate_pagination(
    pagination: &ManifestPagination,
    owner: &str,
) -> Result<(), ClientCompileError> {
    if pagination.kind != "offset" || pagination.coverage != "window" {
        return Err(ClientCompileError::manifest(
            "client.manifest.pagination",
            format!("{owner} pagination must use kind `offset` and coverage `window`"),
        ));
    }
    if pagination.default_limit > pagination.max_limit {
        return Err(ClientCompileError::manifest(
            "client.manifest.pagination_limit",
            format!("{owner} pagination default_limit must not exceed max_limit"),
        ));
    }
    Ok(())
}

pub(crate) fn validate_aggregate_semantics(
    aggregate: &ManifestAggregateSemantics,
    model: &ManifestModel,
) -> Result<(), ClientCompileError> {
    validate_graphql_name(&aggregate.wrapper_typename, "aggregate wrapper typename")?;
    validate_graphql_name(&aggregate.fields_typename, "aggregate fields typename")?;
    validate_pagination(
        &aggregate.nodes_pagination,
        &format!("aggregate nodes for model `{}`", model.id),
    )?;
    if !aggregate.count || !aggregate.nodes {
        return Err(ClientCompileError::manifest(
            "client.manifest.aggregate_capability",
            format!(
                "aggregate semantics for model `{}` must retain count and nodes",
                model.id
            ),
        ));
    }
    for (label, fields) in [
        ("sum", &aggregate.sum),
        ("avg", &aggregate.avg),
        ("min", &aggregate.min),
        ("max", &aggregate.max),
    ] {
        for field in fields {
            if model.field(field).is_none() {
                return Err(ClientCompileError::manifest(
                    "client.manifest.aggregate_field",
                    format!(
                        "aggregate {label} for model `{}` references absent field `{field}`",
                        model.id
                    ),
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_row_policy(
    policy: &ManifestRowPolicy,
    model: &ManifestModel,
    models: &BTreeMap<String, ManifestModel>,
) -> Result<(), ClientCompileError> {
    match policy {
        ManifestRowPolicy::Unrestricted | ManifestRowPolicy::ServerOnly => Ok(()),
        ManifestRowPolicy::Predicate { expression } => {
            validate_filter_expression(expression, model, models)
        }
    }
}

pub(crate) fn derive_trusted_preset_descriptors(
    models: &BTreeMap<String, ManifestModel>,
    commands: &[ManifestCommand],
) -> Result<Vec<ManifestTrustedPresetDescriptor>, ClientCompileError> {
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
        if let ManifestRowPolicy::Predicate { expression } = &model.row_policy {
            collect_row_policy_trusted_presets(expression, model, models, &mut descriptors)?;
        }
    }
    Ok(descriptors
        .into_iter()
        .map(|(name, codec)| ManifestTrustedPresetDescriptor { name, codec })
        .collect())
}

pub(crate) fn collect_row_policy_trusted_presets(
    expression: &ManifestFilterExpr,
    model: &ManifestModel,
    models: &BTreeMap<String, ManifestModel>,
    descriptors: &mut BTreeMap<String, String>,
) -> Result<(), ClientCompileError> {
    match expression {
        ManifestFilterExpr::And(expressions) | ManifestFilterExpr::Or(expressions) => {
            for expression in expressions {
                collect_row_policy_trusted_presets(expression, model, models, descriptors)?;
            }
        }
        ManifestFilterExpr::Not(expression) => {
            collect_row_policy_trusted_presets(expression, model, models, descriptors)?;
        }
        ManifestFilterExpr::Cmp {
            column,
            rhs: ManifestOperand::Claim(claim),
            ..
        } => {
            insert_row_policy_trusted_preset(model, column, &claim.header, descriptors)?;
        }
        ManifestFilterExpr::In { column, values, .. } => {
            for value in values {
                if let ManifestOperand::Claim(claim) = value {
                    insert_row_policy_trusted_preset(model, column, &claim.header, descriptors)?;
                }
            }
        }
        ManifestFilterExpr::Rel { field, predicate } => {
            let relationship = model.relationship(field).ok_or_else(|| {
                ClientCompileError::manifest(
                    "client.manifest.row_policy_relationship",
                    format!(
                        "model `{}` row policy references absent relationship `{field}`",
                        model.id
                    ),
                )
            })?;
            let target = models.get(&relationship.target_model).ok_or_else(|| {
                ClientCompileError::manifest(
                    "client.manifest.row_policy_relationship",
                    format!(
                        "model `{}` row policy relationship `{field}` targets absent model `{}`",
                        model.id, relationship.target_model
                    ),
                )
            })?;
            collect_row_policy_trusted_presets(predicate, target, models, descriptors)?;
        }
        ManifestFilterExpr::Cmp { .. } | ManifestFilterExpr::IsNull { .. } => {}
    }
    Ok(())
}

pub(crate) fn insert_row_policy_trusted_preset(
    model: &ManifestModel,
    column: &str,
    name: &str,
    descriptors: &mut BTreeMap<String, String>,
) -> Result<(), ClientCompileError> {
    let field = model.field(column).ok_or_else(|| {
        ClientCompileError::manifest(
            "client.manifest.row_policy_column",
            format!(
                "model `{}` row policy references absent field `{column}`",
                model.id
            ),
        )
    })?;
    if matches!(field.codec.as_str(), "base64" | "json") {
        return Err(ClientCompileError::manifest(
            "client.manifest.row_policy_portability",
            format!(
                "model `{}` row-policy claim `{name}` targets `{column}` with non-local codec `{}`",
                model.id, field.codec
            ),
        ));
    }
    insert_trusted_preset_descriptor(
        descriptors,
        &ManifestTrustedPresetDescriptor {
            name: name.into(),
            codec: field.codec.clone(),
        },
        &format!("model `{}` row policy field `{column}`", model.id),
    )
}

pub(crate) fn insert_trusted_preset_descriptor(
    descriptors: &mut BTreeMap<String, String>,
    descriptor: &ManifestTrustedPresetDescriptor,
    owner: &str,
) -> Result<(), ClientCompileError> {
    validate_trusted_preset_name(&descriptor.name, "trusted preset name")?;
    match descriptors.entry(descriptor.name.clone()) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(descriptor.codec.clone());
        }
        std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &descriptor.codec => {
        }
        std::collections::btree_map::Entry::Occupied(entry) => {
            return Err(ClientCompileError::manifest(
                "client.manifest.trusted_preset_inventory",
                format!(
                    "trusted preset `{}` uses incompatible codecs `{}` and `{}` across the selected client surface ({owner})",
                    descriptor.name,
                    entry.get(),
                    descriptor.codec
                ),
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_trusted_preset_name(
    value: &str,
    description: &str,
) -> Result<(), ClientCompileError> {
    validate_nonempty(value, description)?;
    if value.len() > 128 || value.trim() != value || value.chars().any(char::is_control) {
        return Err(ClientCompileError::manifest(
            "client.manifest.trusted_preset_name",
            format!("{description} must be 1..=128 bytes with no surrounding whitespace or control characters"),
        ));
    }
    Ok(())
}

pub(crate) fn validate_filter_expression(
    expression: &ManifestFilterExpr,
    model: &ManifestModel,
    models: &BTreeMap<String, ManifestModel>,
) -> Result<(), ClientCompileError> {
    match expression {
        ManifestFilterExpr::And(expressions) | ManifestFilterExpr::Or(expressions) => {
            for expression in expressions {
                validate_filter_expression(expression, model, models)?;
            }
            Ok(())
        }
        ManifestFilterExpr::Not(expression) => {
            validate_filter_expression(expression, model, models)
        }
        ManifestFilterExpr::Cmp { column, rhs, .. } => {
            validate_policy_column(model, column)?;
            validate_operand(rhs)
        }
        ManifestFilterExpr::In { column, values, .. } => {
            validate_policy_column(model, column)?;
            for operand in values {
                validate_operand(operand)?;
            }
            Ok(())
        }
        ManifestFilterExpr::IsNull { column, .. } => validate_policy_column(model, column),
        ManifestFilterExpr::Rel { field, predicate } => {
            let relationship = model.relationship(field).ok_or_else(|| {
                ClientCompileError::manifest(
                    "client.manifest.row_policy_relationship",
                    format!(
                        "model `{}` row policy references absent relationship `{field}`",
                        model.id
                    ),
                )
            })?;
            let target = models.get(&relationship.target_model).ok_or_else(|| {
                ClientCompileError::manifest(
                    "client.manifest.row_policy_relationship",
                    format!(
                        "model `{}` row policy relationship `{field}` has an absent target",
                        model.id
                    ),
                )
            })?;
            validate_filter_expression(predicate, target, models)
        }
    }
}

pub(crate) fn validate_policy_column(
    model: &ManifestModel,
    column: &str,
) -> Result<(), ClientCompileError> {
    if model.field(column).is_none() {
        return Err(ClientCompileError::manifest(
            "client.manifest.row_policy_column",
            format!(
                "model `{}` row policy references absent field `{column}`",
                model.id
            ),
        ));
    }
    Ok(())
}

pub(crate) fn validate_operand(operand: &ManifestOperand) -> Result<(), ClientCompileError> {
    const JS_MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

    fn json_is_portable(value: &JsonValue) -> bool {
        match value {
            JsonValue::Null | JsonValue::Bool(_) | JsonValue::String(_) => true,
            JsonValue::Number(number) => {
                if let Some(value) = number.as_i64() {
                    value.unsigned_abs() <= JS_MAX_SAFE_INTEGER
                } else if let Some(value) = number.as_u64() {
                    value <= JS_MAX_SAFE_INTEGER
                } else {
                    number.as_f64().is_some_and(f64::is_finite)
                }
            }
            JsonValue::Array(values) => values.iter().all(json_is_portable),
            JsonValue::Object(values) => values.values().all(json_is_portable),
        }
    }

    if let ManifestOperand::Claim(claim) = operand {
        validate_trusted_preset_name(&claim.header, "row policy claim header")?;
        return Ok(());
    }
    let portable = match operand {
        ManifestOperand::Claim(_) => unreachable!("claim returned above"),
        ManifestOperand::Lit(ManifestLitValue::I64(value)) => {
            value.unsigned_abs() <= JS_MAX_SAFE_INTEGER
        }
        ManifestOperand::Lit(ManifestLitValue::Json(value)) => json_is_portable(value),
        ManifestOperand::Lit(_) => true,
    };
    if !portable {
        return Err(ClientCompileError::manifest(
            "client.manifest.row_policy_portability",
            "client-visible row policies cannot contain JavaScript-unsafe numbers",
        ));
    }
    Ok(())
}
