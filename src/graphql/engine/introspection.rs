use super::*;

/// True only when the selected query operation contains introspection root
/// fields and nothing application-owned.
///
/// GraphiQL needs a deeper schema-introspection budget than application
/// operations. Selection is deliberately fail-closed: mixed roots, missing or
/// recursive fragments, over-budget documents, ambiguous operations,
/// mutations, and subscriptions all stay on the normal
/// manifest-fingerprinted schema.
pub(crate) fn is_pure_introspection_request(request: &mut Request) -> bool {
    if request.introspection_mode == async_graphql::IntrospectionMode::Disabled {
        return false;
    }
    let operation_name = request.operation_name.clone();
    // `Request::set_parsed_query` is public and async-graphql executes that
    // cached AST when present. Inspect (and, for ordinary requests, populate)
    // the exact AST execution will consume rather than reparsing `query`.
    let Ok(document) = request.parsed_query() else {
        return false;
    };
    let Some(operation) = selected_operation(document, operation_name.as_deref()) else {
        return false;
    };
    if operation.ty != async_graphql::parser::types::OperationType::Query {
        return false;
    }

    fn selection_is_introspection_only(
        selection: &async_graphql::parser::types::SelectionSet,
        document: &async_graphql::parser::types::ExecutableDocument,
        visiting: &mut BTreeSet<String>,
        completed: &mut HashMap<String, bool>,
        remaining_selections: &mut usize,
        depth: usize,
    ) -> bool {
        if depth > REQUEST_ANALYSIS_MAX_DEPTH
            || selection.items.is_empty()
            || selection.items.len() > *remaining_selections
        {
            return false;
        }
        *remaining_selections -= selection.items.len();
        selection.items.iter().all(|item| match &item.node {
            async_graphql::parser::types::Selection::Field(field) => matches!(
                field.node.name.node.as_str(),
                "__schema" | "__type" | "__typename"
            ),
            async_graphql::parser::types::Selection::InlineFragment(fragment) => {
                selection_is_introspection_only(
                    &fragment.node.selection_set.node,
                    document,
                    visiting,
                    completed,
                    remaining_selections,
                    depth + 1,
                )
            }
            async_graphql::parser::types::Selection::FragmentSpread(spread) => {
                let name = spread.node.fragment_name.node.to_string();
                if let Some(valid) = completed.get(&name) {
                    return *valid;
                }
                if !visiting.insert(name.clone()) {
                    return false;
                }
                let valid = document
                    .fragments
                    .get(&spread.node.fragment_name.node)
                    .is_some_and(|fragment| {
                        selection_is_introspection_only(
                            &fragment.node.selection_set.node,
                            document,
                            visiting,
                            completed,
                            remaining_selections,
                            depth + 1,
                        )
                    });
                visiting.remove(&name);
                completed.insert(name, valid);
                valid
            }
        })
    }

    let mut remaining_selections = REQUEST_ANALYSIS_MAX_SELECTIONS;
    selection_is_introspection_only(
        &operation.selection_set.node,
        document,
        &mut BTreeSet::new(),
        &mut HashMap::new(),
        &mut remaining_selections,
        0,
    )
}

/// Resolve whether GraphiQL should be enabled from environment variables.
///
/// Policy:
/// - `GRAPHIQL` if set: on unless value is `0` / `false` / `off` / `no` (case-insensitive)
/// - else: **off** when `RUST_ENV` / `ENV` / `APP_ENV` is `production` or `prod`
/// - else: **on** (local/dev default)
///
/// Pure inputs so tests do not mutate process env. See [`graphiql_enabled_from_env`].
pub fn graphiql_enabled_from_env_vars(
    graphiql: Option<&str>,
    rust_env: Option<&str>,
    env: Option<&str>,
    app_env: Option<&str>,
) -> bool {
    if let Some(v) = graphiql {
        return !matches!(
            v.to_ascii_lowercase().as_str(),
            "0" | "false" | "off" | "no"
        );
    }
    let prod = rust_env
        .or(env)
        .or(app_env)
        .unwrap_or("")
        .to_ascii_lowercase();
    !matches!(prod.as_str(), "production" | "prod")
}

/// Read process env and apply [`graphiql_enabled_from_env_vars`].
pub fn graphiql_enabled_from_env() -> bool {
    graphiql_enabled_from_env_vars(
        std::env::var("GRAPHIQL").ok().as_deref(),
        std::env::var("RUST_ENV").ok().as_deref(),
        std::env::var("ENV").ok().as_deref(),
        std::env::var("APP_ENV").ok().as_deref(),
    )
}

#[cfg(test)]
mod graphiql_env_tests {
    use super::graphiql_enabled_from_env_vars;

    #[test]
    fn production_rust_env_disables_graphiql() {
        assert!(!graphiql_enabled_from_env_vars(
            None,
            Some("production"),
            None,
            None
        ));
        assert!(!graphiql_enabled_from_env_vars(
            None,
            Some("prod"),
            None,
            None
        ));
        assert!(!graphiql_enabled_from_env_vars(
            None,
            Some("PRODUCTION"),
            None,
            None
        ));
    }

    /// Without `GRAPHIQL`, GraphiQL is on unless `RUST_ENV`, `ENV` or
    /// `APP_ENV` names production.
    #[test]
    fn graphiql_is_on_unless_env_or_app_env_names_production() {
        struct Case {
            name: &'static str,
            rust_env: Option<&'static str>,
            env: Option<&'static str>,
            app_env: Option<&'static str>,
            enabled: bool,
        }
        const CASES: &[Case] = &[
            Case {
                name: "RUST_ENV=development enables it",
                rust_env: Some("development"),
                env: None,
                app_env: None,
                enabled: true,
            },
            Case {
                name: "no environment variable enables it",
                rust_env: None,
                env: None,
                app_env: None,
                enabled: true,
            },
            Case {
                name: "ENV=production disables it",
                rust_env: None,
                env: Some("production"),
                app_env: None,
                enabled: false,
            },
            Case {
                name: "APP_ENV=prod disables it",
                rust_env: None,
                env: None,
                app_env: Some("prod"),
                enabled: false,
            },
        ];
        for case in CASES {
            assert_eq!(
                graphiql_enabled_from_env_vars(None, case.rust_env, case.env, case.app_env),
                case.enabled,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn explicit_graphiql_overrides_production() {
        assert!(graphiql_enabled_from_env_vars(
            Some("1"),
            Some("production"),
            None,
            None
        ));
        assert!(!graphiql_enabled_from_env_vars(
            Some("0"),
            Some("development"),
            None,
            None
        ));
        assert!(!graphiql_enabled_from_env_vars(
            Some("false"),
            None,
            None,
            None
        ));
    }
}
