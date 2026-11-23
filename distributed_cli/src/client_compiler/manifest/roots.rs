use std::collections::{BTreeMap, BTreeSet};

use super::*;

pub(crate) fn canonicalize_root(root: &mut ClientRoot) -> Result<(), ClientCompileError> {
    canonicalize_arguments(
        &mut root.arguments,
        &format!("manifest root `{}` argument", root.name),
    )?;
    canonicalize_string_set(
        &mut root.dependencies,
        &format!("manifest root `{}` dependency", root.name),
    )?;
    if let Some(filter) = &mut root.filter {
        canonicalize_filter_semantics(filter)?;
    }
    if let Some(order) = &mut root.order {
        canonicalize_order_semantics(order)?;
    }
    if let Some(aggregate) = &mut root.aggregate {
        canonicalize_aggregate_semantics(aggregate)?;
    }
    Ok(())
}

pub(crate) fn validate_root_contract(
    root: &ClientRoot,
    models: &BTreeMap<String, ClientModel>,
) -> Result<(), ClientCompileError> {
    let expected_id = format!(
        "{}:{}",
        match root.operation {
            ClientRootOperation::Query => "query",
            ClientRootOperation::Subscription => "subscription",
        },
        root.name
    );
    if root.id != expected_id {
        return Err(ClientCompileError::manifest(
            "client.manifest.root_id",
            format!(
                "root `{}` id must be `{expected_id}`, received `{}`",
                root.name, root.id
            ),
        ));
    }
    validate_graphql_name(&root.name, "manifest root name")?;
    let model = models.get(&root.model).ok_or_else(|| {
        ClientCompileError::manifest(
            "client.manifest.root_model",
            format!(
                "manifest root `{}` references missing model `{}`",
                root.name, root.model
            ),
        )
    })?;
    validate_filter_argument_type(
        &root.arguments,
        model,
        &format!("manifest root `{}`", root.name),
    )?;
    validate_nonempty_strings(
        &root.dependencies,
        &format!("manifest root `{}` dependency", root.name),
    )?;
    require_dependency(
        &root.dependencies,
        &model.source_table,
        &format!("manifest root {}", root.name),
    )?;
    if root.operation == ClientRootOperation::Subscription && root.kind != ClientRootKind::List {
        return Err(ClientCompileError::manifest(
            "client.manifest.subscription_kind",
            format!(
                "subscription root `{}` must use list cardinality in manifest v7",
                root.name
            ),
        ));
    }
    if root.operation == ClientRootOperation::Subscription && !root.live {
        return Err(ClientCompileError::manifest(
            "client.manifest.subscription_live",
            format!("subscription root `{}` must be marked live", root.name),
        ));
    }

    let has_filter_argument = root
        .arguments
        .iter()
        .any(|argument| argument.kind == ClientArgumentKind::Filter);
    let has_order_argument = root
        .arguments
        .iter()
        .any(|argument| argument.kind == ClientArgumentKind::Order);
    let has_limit_argument = root
        .arguments
        .iter()
        .any(|argument| argument.kind == ClientArgumentKind::Limit);
    let has_offset_argument = root
        .arguments
        .iter()
        .any(|argument| argument.kind == ClientArgumentKind::Offset);
    if root.filter.is_some() != has_filter_argument
        || root.order.is_some() != has_order_argument
        || root.pagination.is_some() != (has_limit_argument && has_offset_argument)
    {
        return Err(ClientCompileError::manifest(
            "client.manifest.root_arguments",
            format!(
                "root `{}` arguments do not match its filter/order/pagination semantics",
                root.name
            ),
        ));
    }
    if let Some(filter) = &root.filter {
        validate_filter_semantics(filter, model, models)?;
    }
    if let Some(order) = &root.order {
        validate_order_semantics(order, model)?;
    }
    match root.kind {
        ClientRootKind::List => {
            root.filter.as_ref().ok_or_else(|| {
                ClientCompileError::manifest(
                    "client.manifest.root_filter",
                    format!("list root `{}` requires filter semantics", root.name),
                )
            })?;
            root.order.as_ref().ok_or_else(|| {
                ClientCompileError::manifest(
                    "client.manifest.root_order",
                    format!("list root `{}` requires order semantics", root.name),
                )
            })?;
            validate_pagination(
                root.pagination.as_ref().ok_or_else(|| {
                    ClientCompileError::manifest(
                        "client.manifest.root_pagination",
                        format!("list root `{}` requires pagination semantics", root.name),
                    )
                })?,
                &format!("root `{}`", root.name),
            )?;
            if root.aggregate.is_some() {
                return Err(ClientCompileError::manifest(
                    "client.manifest.root_aggregate",
                    format!(
                        "list root `{}` cannot declare aggregate semantics",
                        root.name
                    ),
                ));
            }
        }
        ClientRootKind::ByPk => {
            if root.pagination.is_some()
                || root.aggregate.is_some()
                || root.filter.is_some()
                || root.order.is_some()
            {
                return Err(ClientCompileError::manifest(
                    "client.manifest.by_pk_semantics",
                    format!(
                        "by-pk root `{}` cannot declare filter, order, pagination, or aggregate semantics",
                        root.name
                    ),
                ));
            }
            if root.arguments.iter().any(|argument| {
                argument.kind != ClientArgumentKind::PrimaryKey
                    || argument.nullable
                    || argument.list
            }) {
                return Err(ClientCompileError::manifest(
                    "client.manifest.by_pk_arguments",
                    format!(
                        "by-pk root `{}` may contain only non-null scalar primary-key arguments",
                        root.name
                    ),
                ));
            }
        }
        ClientRootKind::Aggregate => {
            if root.pagination.is_some() || root.order.is_some() {
                return Err(ClientCompileError::manifest(
                    "client.manifest.aggregate_root_semantics",
                    format!(
                        "aggregate root `{}` cannot declare order or pagination semantics",
                        root.name
                    ),
                ));
            }
            validate_aggregate_semantics(
                root.aggregate.as_ref().ok_or_else(|| {
                    ClientCompileError::manifest(
                        "client.manifest.aggregate_root",
                        format!(
                            "aggregate root `{}` requires aggregate semantics",
                            root.name
                        ),
                    )
                })?,
                model,
            )?;
        }
    }
    Ok(())
}

pub(crate) fn validate_unique_arguments(
    root: &ClientRoot,
    scalar_codecs: &BTreeMap<String, String>,
) -> Result<(), ClientCompileError> {
    validate_unique_arguments_for(
        &root.arguments,
        scalar_codecs,
        &format!("manifest root `{}`", root.name),
    )
}

pub(crate) fn validate_unique_arguments_for(
    arguments: &[ClientArgument],
    scalar_codecs: &BTreeMap<String, String>,
    owner: &str,
) -> Result<(), ClientCompileError> {
    let mut names = BTreeSet::new();
    let mut kinds = BTreeSet::new();
    for argument in arguments {
        validate_graphql_name(&argument.name, "manifest argument")?;
        validate_graphql_name(&argument.type_name, "manifest argument type")?;
        if !names.insert(argument.name.as_str()) {
            return Err(ClientCompileError::manifest(
                "client.manifest.duplicate_argument",
                format!("{owner} repeats argument `{}`", argument.name),
            ));
        }
        if argument.kind != ClientArgumentKind::PrimaryKey && !kinds.insert(argument.kind) {
            return Err(ClientCompileError::manifest(
                "client.manifest.duplicate_argument_kind",
                format!("{owner} repeats {:?} argument semantics", argument.kind),
            ));
        }
        if matches!(
            argument.kind,
            ClientArgumentKind::Limit | ClientArgumentKind::Offset
        ) && (argument.list || argument.type_name != "Int")
        {
            return Err(ClientCompileError::manifest(
                "client.manifest.pagination_argument",
                format!(
                    "{owner} pagination argument `{}` must use scalar Int",
                    argument.name
                ),
            ));
        }
        match (scalar_codecs.get(&argument.type_name), &argument.codec) {
            (Some(expected), Some(actual)) if actual == expected => {}
            (Some(expected), Some(actual)) => {
                return Err(ClientCompileError::manifest(
                    "client.manifest.argument_codec",
                    format!(
                        "{owner} argument `{}` codec `{actual}` does not match scalar `{}` inventory codec `{expected}`",
                        argument.name, argument.type_name
                    ),
                ));
            }
            (Some(_), None) => {
                return Err(ClientCompileError::manifest(
                    "client.manifest.argument_codec",
                    format!(
                        "{owner} scalar argument `{}` is missing its codec",
                        argument.name
                    ),
                ));
            }
            (None, Some(actual)) => {
                return Err(ClientCompileError::manifest(
                    "client.manifest.argument_codec",
                    format!(
                        "{owner} argument `{}` declares codec `{actual}` for non-scalar type `{}`",
                        argument.name, argument.type_name
                    ),
                ));
            }
            (None, None) => {}
        }
    }
    Ok(())
}
