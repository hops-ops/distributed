use super::*;

#[cfg(all(test, any(feature = "sqlite", feature = "postgres")))]
mod client_surface_parity_tests {
    use std::any::TypeId;
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use sha2::{Digest, Sha256};

    use super::*;
    use crate::command::CommandConsistency;
    use crate::command::{CommandEffects, TypedCommandContract};
    use crate::command::{CommandInputType, CommandOutputType, CommandTypeDef, CommandTypeField};
    use crate::graphql::commands::TypedCommandInventory;
    #[cfg(feature = "sqlite")]
    use crate::graphql::ModelNormalization;
    use crate::graphql::{
        claim, col, ClientRootOperation, DistributedClientSurfaceExport, RoleGrant,
    };
    use crate::table::{ColumnType, PrimaryKey, TableColumn, TableKind, TableSchema};
    #[cfg(feature = "sqlite")]
    use crate::table::{RelationshipDef, RelationshipKind};

    fn orders() -> TableSchema {
        TableSchema {
            model_name: "OrderView".into(),
            table_name: "orders".into(),
            columns: vec![
                TableColumn {
                    primary_key: true,
                    ..TableColumn::new("order_id", "order_id", ColumnType::Text)
                },
                TableColumn::new("status", "status", ColumnType::Text),
                TableColumn {
                    jsonb: true,
                    ..TableColumn::new("metadata", "metadata", ColumnType::Json)
                },
            ],
            primary_key: PrimaryKey::new(["order_id"]),
            version_column: Some("_sourced_version".into()),
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: TableKind::ReadModel,
        }
    }

    fn duplicated_introspection_fragment_dag(depth: usize) -> String {
        let mut document = String::from("query Introspection { ...F0 }\n");
        for index in 0..depth {
            document.push_str(&format!(
                "fragment F{index} on Query {{ ...F{} ...F{} }}\n",
                index + 1,
                index + 1
            ));
        }
        document.push_str(&format!("fragment F{depth} on Query {{ __typename }}\n"));
        document
    }

    fn test_command<I, O>(
        command_name: &str,
        field_name: &str,
        roles: &[&str],
    ) -> TypedCommandContract
    where
        I: CommandInputType + 'static,
        O: CommandOutputType + 'static,
    {
        TypedCommandContract {
            name: command_name.into(),
            field_name: field_name.into(),
            roles: roles.iter().map(|role| (*role).into()).collect(),
            input: I::command_type().with_type_id(TypeId::of::<I>()),
            output: O::command_type().with_type_id(TypeId::of::<O>()),
            input_type_id: TypeId::of::<I>(),
            output_type_id: TypeId::of::<O>(),
            consistency: CommandConsistency::Succeeded,
            input_defaults: Vec::new(),
            effects: CommandEffects::revalidate(),
            confirmations: Vec::new(),
            projected_model: None,
            direct_projection: None,
            projections: Default::default(),
        }
    }

    fn test_service_binding(
        service_id: &str,
        commands: &TypedCommandInventory,
    ) -> TypedServiceCommandBinding {
        TypedServiceCommandBinding::from_contracts(service_id, &commands.contracts_for_binding())
            .unwrap()
    }

    fn type_field_names(sdl: &str, type_name: &str) -> BTreeSet<String> {
        definition_field_names(sdl, "type", type_name)
    }

    fn input_field_names(sdl: &str, type_name: &str) -> BTreeSet<String> {
        definition_field_names(sdl, "input", type_name)
    }

    fn definition_field_names(sdl: &str, declaration: &str, type_name: &str) -> BTreeSet<String> {
        let marker = format!("{declaration} {type_name} {{");
        let body = sdl
            .split_once(&marker)
            .unwrap_or_else(|| panic!("missing `{marker}` in SDL:\n{sdl}"))
            .1
            .split_once('}')
            .expect("type block should close")
            .0;
        body.lines()
            .filter_map(|line| {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    return None;
                }
                line.split(['(', ':'])
                    .next()
                    .map(str::trim)
                    .filter(|name| !name.is_empty())
                    .map(str::to_string)
            })
            .collect()
    }

    #[cfg(feature = "sqlite")]
    fn protocol_engine(namespace: &str) -> GraphqlEngine {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect_lazy("sqlite::memory:")
            .unwrap();
        let project = ReadModelCatalog::new("orders-service").table_schema(orders());
        GraphqlEngine::from_schema_catalog(&project, pool)
            .unwrap()
            .roles(&["user"])
            .grant_all("user")
            .protocol_token_key([7; 32])
            .protocol_namespace(namespace)
            .build()
            .unwrap()
    }

    #[cfg(feature = "sqlite")]
    fn policy_protocol_engine(namespace: &str, claim_key: &str) -> GraphqlEngine {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect_lazy("sqlite::memory:")
            .unwrap();
        let project = ReadModelCatalog::new("orders-service").table_schema(orders());
        let mut builder = GraphqlEngine::from_schema_catalog(&project, pool)
            .unwrap()
            .roles(&["user"])
            .grant_all("user");
        builder
            .permissions
            .get_mut(&("OrderView".into(), "user".into()))
            .unwrap()
            .permission
            .row_filter = Some(col("status").eq(claim(claim_key)));
        builder
            .protocol_token_key([7; 32])
            .protocol_namespace(namespace)
            .build()
            .unwrap()
    }

    #[cfg(feature = "sqlite")]
    fn preset_protocol_engine() -> GraphqlEngine {
        let mut engine = protocol_engine("preset-test");
        Arc::get_mut(&mut engine.inner)
            .expect("test owns the only engine Arc")
            .protocol
            .as_mut()
            .expect("protocol")
            .roles
            .get_mut("user")
            .expect("user protocol surface")
            .surface
            .trusted_presets = vec![
            ClientTrustedPresetDescriptor {
                name: "x-default-status".into(),
                codec: "string".into(),
            },
            ClientTrustedPresetDescriptor {
                name: "x-order-id".into(),
                codec: "string".into(),
            },
        ];
        engine
    }

    #[cfg(feature = "sqlite")]
    fn distributed_extension(response: &Response) -> serde_json::Value {
        serde_json::to_value(
            response
                .extensions
                .get("distributed")
                .expect("configured protocol response must carry one envelope"),
        )
        .unwrap()
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn client_manifest_exports_the_exact_runtime_execution_limits() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect_lazy("sqlite::memory:")
            .unwrap();
        let project = ReadModelCatalog::new("orders-service").table_schema(orders());
        let engine = GraphqlEngine::from_schema_catalog(&project, pool)
            .unwrap()
            .roles(&["user"])
            .grant_all("user")
            .max_depth(6)
            .max_complexity(37)
            .build()
            .unwrap();

        let manifest = engine.client_manifest_for_role("user").unwrap();
        assert_eq!(manifest.execution.max_depth, 6);
        assert_eq!(manifest.execution.max_complexity, 37);
        assert_eq!(manifest.execution.complexity.version, 1);
        assert_eq!(manifest.execution.complexity.scalar, 1);
        assert_eq!(manifest.execution.complexity.list_fanout, 5);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn graphiql_isolated_introspection_does_not_change_the_client_contract() {
        let build = |graphiql| {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .connect_lazy("sqlite::memory:")
                .unwrap();
            let project = ReadModelCatalog::new("orders-service").table_schema(orders());
            GraphqlEngine::from_schema_catalog(&project, pool)
                .unwrap()
                .roles(&["user"])
                .grant_all("user")
                .client_application_surface("console", ["user"], ["user"])
                .protocol_token_key([7; 32])
                .graphiql(graphiql)
                .build()
                .unwrap()
        };
        let without_graphiql = build(false);
        let with_graphiql = build(true);
        let generated = without_graphiql
            .client_manifest_for_application("console", &["user"], &["user"])
            .unwrap();
        let runtime = with_graphiql
            .client_manifest_for_application("console", &["user"], &["user"])
            .unwrap();

        assert_eq!(generated, runtime);
        assert_eq!(runtime.execution.max_depth, 8);
        assert_eq!(runtime.execution.max_complexity, 500);
        assert_eq!(
            with_graphiql.inner.schemas.get("user").unwrap().sdl(),
            with_graphiql
                .inner
                .graphiql_schemas
                .get("user")
                .unwrap()
                .sdl()
        );

        let mut session = Session::new();
        session.set("x-roles", "user");
        let deep_introspection = r#"
            query GraphiqlIntrospection {
              __type(name: "OrderView") {
                fields {
                  type {
                    ofType {
                      ofType {
                        ofType {
                          ofType {
                            ofType {
                              ofType {
                                ofType {
                                  name
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
        "#;
        let strict = without_graphiql
            .execute(&session, Request::new(deep_introspection))
            .await;
        assert!(
            strict.is_err(),
            "the normal schema must retain the manifest-fingerprinted depth limit"
        );
        let relaxed = with_graphiql
            .execute(&session, Request::new(deep_introspection))
            .await;
        assert!(
            !relaxed.is_err(),
            "pure GraphiQL introspection should use its isolated allowance: {:?}",
            relaxed.errors
        );

        let mut dag_request = Request::new(duplicated_introspection_fragment_dag(32));
        assert!(
            !has_multiple_protocol_query_roots(&with_graphiql.inner, "user", &mut dag_request),
            "protocol root analysis must memoize shared fragment DAGs"
        );
        let executable_dag = duplicated_introspection_fragment_dag(12);
        let dag_response = with_graphiql
            .execute(&session, Request::new(&executable_dag))
            .await;
        assert!(
            !dag_response.is_err(),
            "memoized introspection DAG should execute: {:?}",
            dag_response.errors
        );
        let dag_stream = with_graphiql
            .execute_stream(&session, Request::new(executable_dag))
            .collect::<Vec<_>>()
            .await;
        assert_eq!(dag_stream.len(), 1);
        assert!(
            !dag_stream[0].is_err(),
            "memoized introspection DAG should execute through the streaming path"
        );

        let disabled = with_graphiql
            .execute(
                &session,
                Request::new("{ __schema { queryType { name } } }").disable_introspection(),
            )
            .await;
        assert_eq!(
            disabled.data,
            Value::Null,
            "GraphiQL must not override request-level introspection denial"
        );
        let streamed = with_graphiql
            .execute_stream(
                &session,
                Request::new("{ __schema { queryType { name } } }").disable_introspection(),
            )
            .collect::<Vec<_>>()
            .await;
        assert_eq!(streamed.len(), 1);
        assert_eq!(
            streamed[0].data,
            Value::Null,
            "the streaming path must preserve request-level introspection denial"
        );
    }

    #[test]
    fn graphiql_relaxation_is_selected_only_for_pure_introspection() {
        let classify = |mut request| is_pure_introspection_request(&mut request);
        assert!(classify(Request::new(
            "query Introspection { __schema { queryType { name } } }"
        )));
        assert!(classify(
            Request::new(
                "query App { orders { order_id } } query Introspection { __type(name: \"OrderView\") { name } }"
            )
            .operation_name("Introspection")
        ));
        assert!(classify(Request::new(
            "query ThroughFragment { ...Introspection } fragment Introspection on Query { __schema { queryType { name } } }"
        )));
        assert!(!classify(Request::new(
            "query Mixed { __typename orders { order_id } }"
        )));
        assert!(!classify(Request::new(
            "query App { orders { order_id } } query Introspection { __schema { queryType { name } } }"
        )));
        assert!(!classify(Request::new(
            "mutation NotIntrospection { __typename }"
        )));
        assert!(!classify(Request::new(
            "query ThroughFragment { ...Missing }"
        )));
        assert!(!classify(
            Request::new("{ __schema { queryType { name } } }").disable_introspection()
        ));

        let mut cached_application =
            Request::new("query Introspection { __schema { queryType { name } } }");
        cached_application.set_parsed_query(
            async_graphql::parser::parse_query("query App { orders { order_id } }").unwrap(),
        );
        assert!(
            !is_pure_introspection_request(&mut cached_application),
            "classification must inspect the same cached AST async-graphql executes"
        );

        assert!(
            classify(Request::new(duplicated_introspection_fragment_dag(32))),
            "shared fragment DAGs must be memoized rather than expanded exponentially"
        );

        let over_budget = (0..=REQUEST_ANALYSIS_MAX_SELECTIONS)
            .map(|index| format!("f{index}: __typename"))
            .collect::<Vec<_>>()
            .join(" ");
        assert!(
            !classify(Request::new(format!("query TooWide {{ {over_budget} }}"))),
            "untrusted classifier work must remain explicitly bounded"
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn configured_protocol_attaches_stable_role_and_identity_bound_envelopes() {
        use crate::graphql::identity::VerifiedPrincipal;

        let engine = protocol_engine("public/graphql");
        let manifest = engine.client_manifest_for_role("user").unwrap();
        let role_info = &engine
            .inner
            .protocol
            .as_ref()
            .unwrap()
            .roles
            .get("user")
            .unwrap();
        assert_eq!(
            role_info.surface.schema_fingerprint,
            manifest.schema_fingerprint
        );
        assert_eq!(
            role_info.surface.protocol_fingerprint,
            manifest.protocol_fingerprint
        );

        let mut session = Session::new();
        session.set("x-roles", "user");
        session.set("x-tenant", "tenant-a");
        let principal = VerifiedPrincipal::test_oidc(
            "https://issuer.example",
            "principal-a",
            &["orders-service"],
        );
        let first = engine
            .execute(
                &session,
                Request::new("{ __typename }").data(principal.clone()),
            )
            .await;
        let second = engine
            .execute(
                &session,
                Request::new("{ __typename }").data(principal.clone()),
            )
            .await;
        let first = distributed_extension(&first);
        let second = distributed_extension(&second);
        assert_eq!(first, second);
        assert_eq!(first["protocolVersion"], 1);
        assert_eq!(first["schemaHash"], manifest.schema_fingerprint);
        assert_eq!(
            first["operation"],
            "sha256:7f56e67dd21ab3f30d1ff8b7bed08893f0a0db86449836189b361dd1e56ddb4b"
        );
        let scope = first["cacheScope"].as_str().unwrap();
        assert!(scope.starts_with("v1.cache-scope."));
        assert!(!scope.contains("principal-a"));
        assert!(!scope.contains("tenant-a"));

        let generated_role_request = |name: &str, schema_hash: &str| -> Request {
            serde_json::from_value(serde_json::json!({
                "query": "{ __typename }",
                "extensions": {
                    "distributed": {
                        "client": {
                            "surface": {"kind": "role", "name": name},
                            "schemaHash": schema_hash
                        }
                    }
                }
            }))
            .expect("generated role request")
        };
        let generated_response = engine
            .execute(
                &session,
                generated_role_request("user", &manifest.schema_fingerprint)
                    .data(principal.clone()),
            )
            .await;
        assert_eq!(first, distributed_extension(&generated_response));
        for invalid in [
            generated_role_request("admin", &manifest.schema_fingerprint),
            generated_role_request("user", "sha256:stale-generation"),
        ] {
            let response = engine.execute(&session, invalid).await;
            assert!(response.is_err());
            assert!(!response.extensions.contains_key("distributed"));
        }

        let mut other_session = session.clone();
        other_session.set("user-agent", "a totally different browser");
        let other_session_response = engine
            .execute(
                &other_session,
                Request::new("{ __typename }").data(principal.clone()),
            )
            .await;
        assert_eq!(
            first["cacheScope"],
            distributed_extension(&other_session_response)["cacheScope"]
        );

        let mut other_user = session.clone();
        other_user.set("x-user-id", "user-b");
        let other_user_response = engine
            .execute(
                &other_user,
                Request::new("{ __typename }").data(principal.clone()),
            )
            .await;
        assert_ne!(
            first["cacheScope"],
            distributed_extension(&other_user_response)["cacheScope"]
        );

        let other_principal = VerifiedPrincipal::test_oidc(
            "https://issuer.example",
            "principal-b",
            &["orders-service"],
        );
        let other_principal_response = engine
            .execute(
                &session,
                Request::new("{ __typename }").data(other_principal),
            )
            .await;
        assert_ne!(
            first["cacheScope"],
            distributed_extension(&other_principal_response)["cacheScope"]
        );

        let other_namespace = protocol_engine("internal/graphql");
        let namespaced_response = other_namespace
            .execute(&session, Request::new("{ __typename }").data(principal))
            .await;
        assert_ne!(
            first["cacheScope"],
            distributed_extension(&namespaced_response)["cacheScope"]
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn multi_role_without_named_surface_is_rejected() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect_lazy("sqlite::memory:")
            .unwrap();
        let project = ReadModelCatalog::new("orders-service").table_schema(orders());
        let engine = GraphqlEngine::from_schema_catalog(&project, pool)
            .unwrap()
            .roles(&["admin", "user"])
            .grant_all("admin")
            .grant_all("user")
            .protocol_token_key([7; 32])
            .build()
            .unwrap();
        let mut dual = Session::new();
        dual.set("x-roles", "admin,user");
        dual.set("x-user-id", "person-1");
        let response = engine.execute(&dual, Request::new("{ __typename }")).await;
        assert!(
            response.is_err(),
            "multi-role must name a surface: {response:?}"
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn anonymous_application_surface_opens_without_asserted_roles() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect_lazy("sqlite::memory:")
            .unwrap();
        let project = ReadModelCatalog::new("orders-service").table_schema(orders());
        let engine = GraphqlEngine::from_schema_catalog(&project, pool)
            .unwrap()
            .roles(&["anonymous", "user"])
            .grant_all("anonymous")
            .grant_all("user")
            .client_application_surface("public", ["anonymous"], ["anonymous"])
            .protocol_token_key([7; 32])
            .build()
            .unwrap();
        let manifest = engine
            .client_manifest_for_application("public", &["anonymous"], &["anonymous"])
            .unwrap();
        let request: Request = serde_json::from_value(serde_json::json!({
            "query": "{ __typename }",
            "extensions": {
                "distributed": {
                    "client": {
                        "surface": {
                            "kind": "application",
                            "name": "public",
                            "eligible_roles": ["anonymous"],
                            "schema_roles": ["anonymous"]
                        },
                        "schemaHash": manifest.schema_fingerprint
                    }
                }
            }
        }))
        .unwrap();
        let session = Session::new();
        let response = engine.execute(&session, request).await;
        assert!(
            !response.is_err(),
            "anonymous surface must open with empty identity: {:?}",
            response.errors
        );
        assert_eq!(
            distributed_extension(&response)["schemaHash"],
            manifest.schema_fingerprint
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn multi_role_principal_opens_portable_schema_subset_application_surface() {
        // eligible {admin,user} + schema privilege {user}: admin asserted alone
        // may open; execution uses privilege pack user (not admin grants).
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect_lazy("sqlite::memory:")
            .unwrap();
        let project = ReadModelCatalog::new("orders-service").table_schema(orders());
        let mut builder = GraphqlEngine::from_schema_catalog(&project, pool)
            .unwrap()
            .roles(&["admin", "user"])
            .grant_all("admin")
            .grant_all("user");
        // Restricted user policy; unrestricted admin. Schema privilege stays
        // user-only so the application contract keeps a portable claim preset.
        builder
            .permissions
            .get_mut(&("OrderView".into(), "user".into()))
            .unwrap()
            .permission
            .row_filter = Some(col("status").eq(claim("x-user-id")));
        let engine = builder
            .client_application_surface_with_schema_roles("console", ["admin", "user"], ["user"])
            .protocol_token_key([7; 32])
            .build()
            .unwrap();

        let manifest = engine
            .client_manifest_for_application("console", &["admin", "user"], &["user"])
            .expect("registered multi-role application manifest");
        assert_eq!(
            manifest.surface,
            crate::graphql::ClientSurfaceIdentity::application_with_schema_roles(
                "console",
                ["admin", "user"],
                ["user"],
            )
        );
        // Wire roles are eligible; schema privilege is user-only so x-user-id
        // remains a trusted preset (portable owner-style policy).
        let application_presets = &engine.inner.protocol.as_ref().unwrap().applications["console"]
            .surface
            .trusted_presets;
        assert_eq!(
            application_presets,
            &vec![ClientTrustedPresetDescriptor {
                name: "x-user-id".into(),
                codec: "string".into(),
            }]
        );
        // Intersecting admin∩user would drop the portable claim (admin has no
        // row filter). Confirm the user-only role surface still has the claim.
        assert_eq!(
            engine.inner.protocol.as_ref().unwrap().roles["user"]
                .surface
                .trusted_presets,
            vec![ClientTrustedPresetDescriptor {
                name: "x-user-id".into(),
                codec: "string".into(),
            }]
        );

        let request = |schema_hash: &str| -> Request {
            serde_json::from_value(serde_json::json!({
                "query": "{ __typename }",
                "extensions": {
                    "distributed": {
                        "client": {
                            "surface": {
                                "kind": "application",
                                "name": "console",
                                "eligible_roles": ["admin", "user"],
                                "schema_roles": ["user"]
                            },
                            "schemaHash": schema_hash
                        }
                    }
                }
            }))
            .expect("application protocol request")
        };

        // Admin alone is eligible — singleton set is enough when admin is
        // on the surface eligible list.
        let mut admin = Session::new();
        admin.set("x-roles", "admin");
        admin.set("x-user-id", "person-1");
        let admin_response = engine
            .execute(&admin, request(&manifest.schema_fingerprint))
            .await;
        let admin_envelope = distributed_extension(&admin_response);
        assert_eq!(admin_envelope["schemaHash"], manifest.schema_fingerprint);
        assert_eq!(
            admin_envelope["trustedPresets"],
            serde_json::json!([{"name": "x-user-id", "codec": "string", "value": "person-1"}])
        );

        let mut user = Session::new();
        user.set("x-roles", "user");
        user.set("x-user-id", "person-1");
        let user_response = engine
            .execute(&user, request(&manifest.schema_fingerprint))
            .await;
        let user_envelope = distributed_extension(&user_response);
        assert_eq!(user_envelope["schemaHash"], manifest.schema_fingerprint);
        assert_ne!(
            user_envelope["cacheScope"], admin_envelope["cacheScope"],
            "same surface privilege still scopes cache by asserted role set"
        );
        // Privilege pack is user for both openers (not admin unrestricted).
        assert_eq!(
            engine.inner.protocol.as_ref().unwrap().applications["console"].privilege_key,
            "user"
        );

        // user-only eligible surface: admin alone is denied; dual asserted
        // roles (x-roles includes user) may open.
        let pool2 = sqlx::sqlite::SqlitePoolOptions::new()
            .connect_lazy("sqlite::memory:")
            .unwrap();
        let project2 = ReadModelCatalog::new("orders-service").table_schema(orders());
        let user_only = GraphqlEngine::from_schema_catalog(&project2, pool2)
            .unwrap()
            .roles(&["admin", "user"])
            .grant_all("admin")
            .grant_all("user")
            .client_application_surface("console", ["user"], ["user"])
            .protocol_token_key([7; 32])
            .build()
            .unwrap();
        let user_only_manifest = user_only
            .client_manifest_for_application("console", &["user"], &["user"])
            .unwrap();
        let user_only_request = || -> Request {
            serde_json::from_value(serde_json::json!({
                "query": "{ __typename }",
                "extensions": {
                    "distributed": {
                        "client": {
                            "surface": {
                                "kind": "application",
                                "name": "console",
                                "eligible_roles": ["user"],
                                "schema_roles": ["user"]
                            },
                            "schemaHash": user_only_manifest.schema_fingerprint
                        }
                    }
                }
            }))
            .unwrap()
        };
        let denied = user_only.execute(&admin, user_only_request()).await;
        assert!(denied.is_err());
        assert!(!denied.extensions.contains_key("distributed"));
        let mut dual = admin.clone();
        dual.set("x-roles", "admin,user");
        let allowed = user_only.execute(&dual, user_only_request()).await;
        assert_eq!(
            distributed_extension(&allowed)["schemaHash"],
            user_only_manifest.schema_fingerprint
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn named_application_protocol_selection_is_registered_exact_and_role_bound() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect_lazy("sqlite::memory:")
            .unwrap();
        let project = ReadModelCatalog::new("orders-service").table_schema(orders());
        let engine = GraphqlEngine::from_schema_catalog(&project, pool)
            .unwrap()
            .roles(&["admin", "user"])
            .grant_all("admin")
            .grant_all("user")
            .client_application_surface("console", ["admin", "user"], ["admin", "user"])
            .protocol_token_key([7; 32])
            .build()
            .unwrap();
        let manifest = engine
            .client_manifest_for_application("console", &["user", "admin"], &["user", "admin"])
            .expect("registered application manifest");
        assert!(engine
            .client_manifest_for_application("console", &["user"], &["user"])
            .is_err());

        let request = |schema_hash: &str, roles: serde_json::Value| -> Request {
            serde_json::from_value(serde_json::json!({
                "query": "{ __typename }",
                "extensions": {
                    "distributed": {
                        "client": {
                            "surface": {
                                "kind": "application",
                                "name": "console",
                                "eligible_roles": roles,
                                "schema_roles": roles
                            },
                            "schemaHash": schema_hash
                        }
                    }
                }
            }))
            .expect("generated application request")
        };

        let mut user = Session::new();
        user.set("x-roles", "user");
        user.set("x-user-id", "person-1");
        let user_response = engine
            .execute(
                &user,
                request(
                    &manifest.schema_fingerprint,
                    serde_json::json!(["admin", "user"]),
                ),
            )
            .await;
        let user_envelope = distributed_extension(&user_response);
        assert_eq!(user_envelope["schemaHash"], manifest.schema_fingerprint);

        let mut admin = user.clone();
        admin.set("x-roles", "admin");
        let admin_response = engine
            .execute(
                &admin,
                request(
                    &manifest.schema_fingerprint,
                    serde_json::json!(["admin", "user"]),
                ),
            )
            .await;
        let admin_envelope = distributed_extension(&admin_response);
        assert_eq!(admin_envelope["schemaHash"], manifest.schema_fingerprint);
        assert_ne!(
            user_envelope["cacheScope"], admin_envelope["cacheScope"],
            "one application schema never erases the concrete authorized role"
        );

        for invalid in [
            request(
                &manifest.schema_fingerprint,
                serde_json::json!(["user", "admin"]),
            ),
            request(
                "sha256:stale-generation",
                serde_json::json!(["admin", "user"]),
            ),
        ] {
            let response = engine.execute(&user, invalid).await;
            assert!(response.is_err());
            assert!(!response.extensions.contains_key("distributed"));
        }
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn trusted_presets_are_session_derived_typed_and_scope_bound() {
        let engine = preset_protocol_engine();
        assert_eq!(
            engine.inner.protocol.as_ref().unwrap().roles["user"]
                .surface
                .trusted_presets,
            vec![
                ClientTrustedPresetDescriptor {
                    name: "x-default-status".into(),
                    codec: "string".into(),
                },
                ClientTrustedPresetDescriptor {
                    name: "x-order-id".into(),
                    codec: "string".into(),
                },
            ]
        );

        let mut session = Session::new();
        session.set("x-roles", "user");
        session.set("x-user-id", "person-1");
        session.set("x-order-id", "order-1");
        session.set("x-default-status", "assigned");
        let first = engine
            .execute(&session, Request::new("{ __typename }"))
            .await;
        let first = distributed_extension(&first);
        assert_eq!(
            first["trustedPresets"],
            serde_json::json!([
                {"name": "x-default-status", "codec": "string", "value": "assigned"},
                {"name": "x-order-id", "codec": "string", "value": "order-1"}
            ])
        );

        let mut changed = session.clone();
        changed.set("x-default-status", "queued");
        let changed = engine
            .execute(&changed, Request::new("{ __typename }"))
            .await;
        let changed = distributed_extension(&changed);
        assert_ne!(first["cacheScope"], changed["cacheScope"]);
        assert_eq!(changed["trustedPresets"][0]["value"], "queued");

        let mut missing = Session::new();
        missing.set("x-roles", "user");
        missing.set("x-user-id", "person-1");
        missing.set("x-order-id", "order-1");
        let response = engine
            .execute(&missing, Request::new("{ __typename }"))
            .await;
        assert!(response.is_err());
        assert!(!response.extensions.contains_key("distributed"));
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn row_policy_presets_follow_sql_claim_case_normalization() {
        let engine = policy_protocol_engine("mixed-case-policy", "X-Tenant");
        assert_eq!(
            engine.inner.protocol.as_ref().unwrap().roles["user"]
                .surface
                .trusted_presets,
            vec![ClientTrustedPresetDescriptor {
                name: "X-Tenant".into(),
                codec: "string".into(),
            }]
        );

        let mut session = Session::new();
        session.set("x-roles", "user");
        session.set("x-user-id", "person-1");
        // `operand_to_bind` accepts the normalized lowercase header for this
        // mixed-case policy claim. The cache-scope envelope must expose the
        // same resolved value or client policy evaluation would fail closed.
        session.set("x-tenant", "tenant-1");
        let response = engine
            .execute(&session, Request::new("{ __typename }"))
            .await;
        assert_eq!(
            distributed_extension(&response)["trustedPresets"],
            serde_json::json!([
                {"name": "X-Tenant", "codec": "string", "value": "tenant-1"}
            ])
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn required_presets_reject_query_and_stream_without_internal_errors() {
        let mut engine = preset_protocol_engine();
        Arc::get_mut(&mut engine.inner)
            .unwrap()
            .protocol
            .as_mut()
            .unwrap()
            .roles
            .get_mut("user")
            .unwrap()
            .surface
            .trusted_presets = vec![ClientTrustedPresetDescriptor {
            name: "x-required-number".into(),
            codec: "int32".into(),
        }];
        for value in [None, Some("not-a-number"), Some("01"), Some("2147483648")] {
            let mut session = Session::new();
            session.set("x-roles", "user");
            if let Some(value) = value {
                session.set("x-required-number", value);
            }
            let query = engine
                .execute(&session, Request::new("{ __typename }"))
                .await;
            let streamed = engine
                .execute_stream(&session, Request::new("{ __typename }"))
                .collect::<Vec<_>>()
                .await;
            assert_eq!(streamed.len(), 1);
            for response in std::iter::once(query).chain(streamed) {
                assert_eq!(response.errors.len(), 1);
                let error = serde_json::to_value(&response.errors[0]).unwrap();
                assert_eq!(error["extensions"]["code"], "BAD_REQUEST", "{error}");
                assert_eq!(
                    error["message"],
                    "required session input is missing or invalid"
                );
                assert_eq!(response.data, Value::Null);
                assert!(!response.extensions.contains_key("distributed"));
            }
        }
        let mut valid = Session::new();
        valid.set("x-roles", "user");
        valid.set("x-required-number", "42");
        let response = engine.execute(&valid, Request::new("{ __typename }")).await;
        assert!(!response.is_err(), "{:?}", response.errors);
        assert_eq!(
            distributed_extension(&response)["trustedPresets"][0]["value"],
            42
        );
        let responses = engine
            .execute_stream(&valid, Request::new("{ __typename }"))
            .collect::<Vec<_>>()
            .await;
        assert_eq!(responses.len(), 1);
        assert!(!responses[0].is_err());
        assert_eq!(
            distributed_extension(&responses[0])["trustedPresets"][0]["value"],
            42
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn cache_scope_tracks_only_relevant_claims_and_private_policy() {
        use crate::graphql::identity::VerifiedPrincipal;

        let engine = policy_protocol_engine("public/graphql", "x-tenant");
        let principal = VerifiedPrincipal::test_oidc(
            "https://issuer.example",
            "principal-a",
            &["orders-service"],
        );
        let mut session = Session::new();
        session.set("x-roles", "user");
        session.set("x-user-id", "user-a");
        session.set("x-tenant", "tenant-a");
        session.set("x-organization", "organization-a");
        let response = engine
            .execute(
                &session,
                Request::new("{ __typename }").data(principal.clone()),
            )
            .await;
        let envelope = distributed_extension(&response);

        let mut irrelevant = session.clone();
        irrelevant.set("cookie", "rotated-cookie");
        irrelevant.set("x-organization", "organization-b");
        let response = engine
            .execute(
                &irrelevant,
                Request::new("{ __typename }").data(principal.clone()),
            )
            .await;
        assert_eq!(
            envelope["cacheScope"],
            distributed_extension(&response)["cacheScope"]
        );

        let mut other_tenant = session.clone();
        other_tenant.set("x-tenant", "tenant-b");
        let response = engine
            .execute(
                &other_tenant,
                Request::new("{ __typename }").data(principal.clone()),
            )
            .await;
        assert_ne!(
            envelope["cacheScope"],
            distributed_extension(&response)["cacheScope"]
        );

        let other_policy = policy_protocol_engine("public/graphql", "x-organization");
        let response = other_policy
            .execute(
                &session,
                Request::new("{ __typename }").data(principal.clone()),
            )
            .await;
        assert_ne!(
            envelope["cacheScope"],
            distributed_extension(&response)["cacheScope"]
        );

        let mut anonymous = session;
        anonymous.set("x-roles", "anonymous");
        let response = engine
            .execute(&anonymous, Request::new("{ __typename }").data(principal))
            .await;
        assert_ne!(
            envelope["cacheScope"],
            distributed_extension(&response)["cacheScope"]
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn protocol_stream_uses_one_request_accumulator_and_raw_engine_has_no_envelope() {
        use crate::graphql::identity::VerifiedPrincipal;

        let engine = protocol_engine("public/graphql");
        let mut session = Session::new();
        session.set("x-roles", "user");
        let principal = VerifiedPrincipal::test_oidc(
            "https://issuer.example",
            "principal-a",
            &["orders-service"],
        );
        let mut responses =
            engine.execute_stream(&session, Request::new("{ __typename }").data(principal));
        let response = responses.next().await.expect("one query response");
        assert!(response.extensions.contains_key("distributed"));
        assert!(responses.next().await.is_none());

        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect_lazy("sqlite::memory:")
            .unwrap();
        let project = ReadModelCatalog::new("orders-service").table_schema(orders());
        let raw = GraphqlEngine::from_schema_catalog(&project, pool)
            .unwrap()
            .roles(&["user"])
            .grant_all("user")
            .build()
            .unwrap();
        let response = raw.execute(&session, Request::new("{ __typename }")).await;
        assert!(!response.extensions.contains_key("distributed"));
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn protocol_configuration_requires_real_key_and_service_identity() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect_lazy("sqlite::memory:")
            .unwrap();
        let result = GraphqlEngine::builder(pool.clone())
            .service_id("orders-service")
            .protocol_token_key([0; 32])
            .build();
        let error = match result {
            Ok(_) => panic!("all-zero protocol key must fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("must not be all zero"));

        let result = GraphqlEngine::builder(pool)
            .protocol_token_key([7; 32])
            .build();
        let error = match result {
            Ok(_) => panic!("protocol key without service identity must fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("stable service ID"));
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn one_role_surface_drives_runtime_sdl_manifest_and_limits() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect_lazy("sqlite::memory:")
            .unwrap();
        let project = ReadModelCatalog::new("orders-service").table_schema(orders());
        let commands = TypedCommandInventory::from_contracts(&[test_command::<
            ChangeOrderInput,
            ChangeOrderPayload,
        >(
            "order.refresh",
            "orders_refresh",
            &["user"],
        )])
        .unwrap();
        let mut builder = GraphqlEngine::from_schema_catalog(&project, pool)
            .unwrap()
            .roles(&["user"])
            .grant_all("user")
            .default_limit(7)
            .max_limit(19)
            .client_projectors([SurfaceProjector::new("project_orders")
                .facts(["order.changed"])
                .models(["OrderView"])]);
        builder.command_binding = Some(test_service_binding("orders-service", &commands));
        builder.typed_commands = commands;
        let engine = builder.build().unwrap();

        let stored = engine.surface_for_role("user").unwrap();
        assert_eq!(engine.service_id(), Some("orders-service"));
        let export = engine.client_surface_for_role("user").unwrap();
        assert!(Arc::ptr_eq(&stored, export.surface()));
        let manifest = export.manifest().unwrap();
        assert_eq!(manifest, export.manifest().unwrap());
        assert_eq!(manifest.service_id, "orders-service");
        let static_sdl = engine.ir_sdl_for_role("user").unwrap();
        let runtime_sdl = engine.sdl_for_role("user").unwrap();

        for type_name in [
            "Query",
            "Subscription",
            "Mutation",
            "OrderView",
            "orders_aggregate",
            "orders_aggregate_fields",
        ] {
            assert_eq!(
                type_field_names(&static_sdl, type_name),
                type_field_names(&runtime_sdl, type_name),
                "runtime/static field drift for {type_name}"
            );
        }

        let query_roots: BTreeSet<String> = manifest
            .roots
            .iter()
            .filter(|root| root.operation == ClientRootOperation::Query)
            .map(|root| root.name.clone())
            .collect();
        let subscription_roots: BTreeSet<String> = manifest
            .roots
            .iter()
            .filter(|root| root.operation == ClientRootOperation::Subscription)
            .map(|root| root.name.clone())
            .collect();
        let runtime_query_roots = type_field_names(&runtime_sdl, "Query")
            .into_iter()
            .filter(|field| field != "commandStatus")
            .collect();
        assert_eq!(query_roots, runtime_query_roots);
        assert_eq!(
            subscription_roots,
            type_field_names(&runtime_sdl, "Subscription")
        );
        assert_eq!(manifest.commands.len(), 1);
        assert_eq!(manifest.commands[0].name, "order.refresh");
        assert_eq!(
            manifest
                .protocol_operations
                .command_status
                .as_ref()
                .unwrap()
                .name,
            "Distributed_CommandStatus"
        );
        assert_eq!(
            type_field_names(&runtime_sdl, "Mutation"),
            BTreeSet::from(["orders_refresh".into()])
        );
        assert_eq!(
            manifest.models[0]
                .fields
                .iter()
                .map(|field| field.name.clone())
                .collect::<BTreeSet<_>>(),
            type_field_names(&runtime_sdl, "OrderView")
        );
        assert_eq!(subscription_roots, BTreeSet::from(["orders".into()]));
        assert!(!runtime_sdl.contains("type Subscription {\n\torders_by_pk"));

        let list = manifest
            .roots
            .iter()
            .find(|root| root.id == "query:orders")
            .unwrap();
        assert_eq!(list.pagination.as_ref().unwrap().default_limit, 7);
        assert_eq!(list.pagination.as_ref().unwrap().max_limit, 19);
        assert_eq!(manifest.projectors[0].name, "project_orders");
        let runtime_json = input_field_names(&runtime_sdl, "JSON_comparison_exp");
        assert_eq!(
            runtime_json,
            input_field_names(&static_sdl, "JSON_comparison_exp")
        );
        let metadata_ops: BTreeSet<String> = list
            .filter
            .as_ref()
            .unwrap()
            .fields
            .iter()
            .find(|field| field.name == "metadata")
            .unwrap()
            .operators
            .iter()
            .cloned()
            .collect();
        assert_eq!(metadata_ops, runtime_json);
        for forbidden in ["_contains", "_contained_in", "_has_key"] {
            assert!(!metadata_ops.contains(forbidden));
        }
        assert_eq!(
            manifest.schema_fingerprint,
            "sha256:cce38712ab7d84a934fa10bf1272d292d20f746aea5bf2e485fe15b2527517c0"
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn manual_engine_client_export_requires_explicit_service_id() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect_lazy("sqlite::memory:")
            .unwrap();
        let engine = GraphqlEngine::builder(pool)
            .register_schema_exposed(orders())
            .unwrap()
            .roles(&["user"])
            .grant_all("user")
            .build()
            .unwrap();

        let error = engine.client_surface_for_role("user").unwrap_err();
        assert!(error
            .to_string()
            .contains("GraphqlEngineBuilder::service_id"));
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn empty_role_static_runtime_and_manifest_are_truthful() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect_lazy("sqlite::memory:")
            .unwrap();
        let project = ReadModelCatalog::new("orders-service").table_schema(orders());
        let engine = GraphqlEngine::from_schema_catalog(&project, pool)
            .unwrap()
            .roles(&["empty"])
            .build()
            .unwrap();

        let static_sdl = engine.ir_sdl_for_role("empty").unwrap();
        let runtime_sdl = engine.sdl_for_role("empty").unwrap();
        assert_eq!(
            type_field_names(&static_sdl, "Query"),
            BTreeSet::from(["_empty".into()])
        );
        assert_eq!(
            type_field_names(&static_sdl, "Query"),
            type_field_names(&runtime_sdl, "Query")
        );
        let manifest = engine.client_manifest_for_role("empty").unwrap();
        assert!(manifest.roots.is_empty());
        assert!(!manifest.capabilities.live_queries);
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn postgres_role_surface_drives_runtime_sdl_and_manifest() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://postgres:postgres@localhost/distributed_test")
            .unwrap();
        let project = ReadModelCatalog::new("orders-service").table_schema(orders());
        let engine = GraphqlEngine::from_schema_catalog(&project, pool)
            .unwrap()
            .roles(&["user"])
            .grant_all("user")
            .default_limit(7)
            .max_limit(19)
            .build()
            .unwrap();

        let export = engine.client_surface_for_role("user").unwrap();
        let manifest = export.manifest().unwrap();
        assert_eq!(manifest, export.manifest().unwrap());
        let static_sdl = engine.ir_sdl_for_role("user").unwrap();
        let runtime_sdl = engine.sdl_for_role("user").unwrap();
        for type_name in [
            "Query",
            "Subscription",
            "OrderView",
            "orders_aggregate",
            "orders_aggregate_fields",
        ] {
            assert_eq!(
                type_field_names(&static_sdl, type_name),
                type_field_names(&runtime_sdl, type_name),
                "Postgres runtime/static field drift for {type_name}"
            );
        }

        let query_roots: BTreeSet<String> = manifest
            .roots
            .iter()
            .filter(|root| root.operation == ClientRootOperation::Query)
            .map(|root| root.name.clone())
            .collect();
        let runtime_query_roots = type_field_names(&runtime_sdl, "Query")
            .into_iter()
            .filter(|field| field != "commandStatus")
            .collect();
        assert_eq!(query_roots, runtime_query_roots);
        assert_eq!(
            manifest.models[0]
                .fields
                .iter()
                .map(|field| field.name.clone())
                .collect::<BTreeSet<_>>(),
            type_field_names(&runtime_sdl, "OrderView")
        );

        let runtime_json = input_field_names(&runtime_sdl, "JSON_comparison_exp");
        assert_eq!(
            runtime_json,
            input_field_names(&static_sdl, "JSON_comparison_exp")
        );
        let metadata_ops: BTreeSet<String> = manifest
            .roots
            .iter()
            .find(|root| root.id == "query:orders")
            .unwrap()
            .filter
            .as_ref()
            .unwrap()
            .fields
            .iter()
            .find(|field| field.name == "metadata")
            .unwrap()
            .operators
            .iter()
            .cloned()
            .collect();
        assert_eq!(metadata_ops, runtime_json);
        for required in ["_contains", "_contained_in", "_has_key"] {
            assert!(metadata_ops.contains(required));
        }
        assert_eq!(manifest.service_id, "orders-service");
        assert_eq!(
            manifest.schema_fingerprint,
            "sha256:9c0150e725493dc1e5ddbb3fdfc8b651cad399f099908cbcac326e514389c48a"
        );
    }

    fn customers() -> TableSchema {
        TableSchema {
            model_name: "CustomerView".into(),
            table_name: "customers".into(),
            columns: vec![
                TableColumn {
                    primary_key: true,
                    ..TableColumn::new("customer_id", "customer_id", ColumnType::Text)
                },
                TableColumn::new("display_name", "display_name", ColumnType::Text),
                TableColumn::new("internal_note", "internal_note", ColumnType::Text),
            ],
            primary_key: PrimaryKey::new(["customer_id"]),
            version_column: None,
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: TableKind::ReadModel,
        }
    }

    fn type_field(
        name: &str,
        type_name: &str,
        nullable: bool,
        list: bool,
        nested: Option<CommandTypeDef>,
    ) -> CommandTypeField {
        CommandTypeField {
            name: name.into(),
            type_name: type_name.into(),
            nullable,
            list,
            item_nullable: false,
            nested: nested.map(Box::new),
        }
    }

    struct ChangeOrderInput;

    impl CommandInputType for ChangeOrderInput {
        fn command_type() -> CommandTypeDef {
            let patch = CommandTypeDef::new(
                "OrderPatchInput",
                vec![
                    type_field("status", "String", false, false, None),
                    type_field("metadata", "JSON", true, false, None),
                ],
            );
            CommandTypeDef::new(
                "ChangeOrderInput",
                vec![
                    type_field("patch", "OrderPatchInput", false, false, Some(patch)),
                    type_field("order_id", "String", false, false, None),
                ],
            )
        }
    }

    struct ChangeOrderPayload;

    impl CommandOutputType for ChangeOrderPayload {
        fn command_type() -> CommandTypeDef {
            let changed_order = CommandTypeDef::new(
                "ChangedOrder",
                vec![
                    type_field("status", "String", false, false, None),
                    type_field("order_id", "String", false, false, None),
                ],
            );
            CommandTypeDef::new(
                "ChangeOrderPayload",
                vec![
                    type_field("warnings", "String", true, true, None),
                    type_field("order", "ChangedOrder", false, false, Some(changed_order)),
                    type_field("accepted", "Boolean", false, false, None),
                ],
            )
        }
    }

    fn matrix_project() -> ReadModelCatalog {
        ReadModelCatalog::new("acceptance-service")
            .table_schema(orders())
            .table_schema(customers())
    }

    fn matrix_commands() -> TypedCommandInventory {
        TypedCommandInventory::from_contracts(&[
            test_command::<ChangeOrderInput, ChangeOrderPayload>(
                "order.change",
                "orders_change",
                &["restricted", "admin"],
            ),
            test_command::<ChangeOrderInput, ChangeOrderPayload>(
                "order.force_archive",
                "orders_force_archive",
                &["admin"],
            ),
        ])
        .unwrap()
    }

    fn matrix_projectors() -> Vec<SurfaceProjector> {
        vec![
            SurfaceProjector::new("project_customers")
                .facts(["customer.changed"])
                .models(["CustomerView"]),
            SurfaceProjector::new("project_orders")
                .facts(["order.changed"])
                .models(["OrderView"]),
        ]
    }

    fn restricted_read() -> ReadPermission {
        read()
            .columns(["order_id", "status"])
            .rows(col("status").eq("OPEN"))
            .limit(5)
    }

    fn insert_permission(
        builder: &mut GraphqlEngineBuilder,
        model: &str,
        role: &str,
        permission: ReadPermission,
    ) {
        assert!(builder
            .permissions
            .insert((model.into(), role.into()), RoleModelPerm { permission },)
            .is_none());
    }

    fn matrix_engine(pool: GraphqlPool) -> GraphqlEngine {
        let project = matrix_project();
        let mut builder = GraphqlEngine::from_schema_catalog(&project, pool)
            .unwrap()
            .roles(&["restricted", "admin"])
            .default_limit(11)
            .max_limit(23)
            .client_projectors(matrix_projectors());
        let commands = matrix_commands();
        builder.command_binding = Some(test_service_binding("acceptance-service", &commands));
        builder.typed_commands = commands;
        insert_permission(&mut builder, "OrderView", "restricted", restricted_read());
        insert_permission(
            &mut builder,
            "OrderView",
            "admin",
            read().all_columns().aggregations(),
        );
        insert_permission(
            &mut builder,
            "CustomerView",
            "admin",
            read().all_columns().aggregations(),
        );
        builder.build().unwrap()
    }

    fn independent_manifest(dialect: SurfaceDialect, role: &str) -> DistributedClientManifest {
        let project = matrix_project();
        let options = SurfaceOptions {
            dialect,
            aggregates: true,
            subscriptions: true,
            default_limit: 11,
            max_limit: 23,
        };
        let commands = matrix_commands();
        let full = build_surface(&project.tables, &options)
            .unwrap()
            .with_typed_commands(&commands)
            .unwrap()
            .with_service_binding(Some(test_service_binding("acceptance-service", &commands)))
            .with_projectors(matrix_projectors())
            .unwrap();
        let grants = match role {
            "restricted" => BTreeMap::from([(
                "OrderView".into(),
                RoleGrant::columns(["order_id", "status"])
                    .rows(col("status").eq("OPEN"))
                    .limit(5),
            )]),
            "admin" => BTreeMap::from([
                (
                    "OrderView".into(),
                    RoleGrant::all_columns().with_aggregations(),
                ),
                (
                    "CustomerView".into(),
                    RoleGrant::all_columns().with_aggregations(),
                ),
            ]),
            other => panic!("unexpected matrix role `{other}`"),
        };
        let selected = surface_for_role(&full, role, &grants).unwrap();
        DistributedClientSurfaceExport::from_selected(project.name.clone(), selected)
            .unwrap()
            .manifest()
            .unwrap()
    }

    fn definition_inventory(sdl: &str) -> BTreeMap<String, BTreeSet<String>> {
        let mut inventory: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let mut current: Option<String> = None;
        for line in sdl.lines() {
            let line = line.trim();
            if current.is_none() {
                let declaration = line
                    .strip_prefix("type ")
                    .or_else(|| line.strip_prefix("input "));
                if let Some(declaration) = declaration {
                    if line.contains('{') {
                        let name = declaration
                            .split([' ', '{'])
                            .next()
                            .expect("definition name")
                            .to_string();
                        inventory.entry(name.clone()).or_default();
                        current = Some(name);
                    }
                }
                continue;
            }
            if line == "}" {
                current = None;
                continue;
            }
            if line.is_empty() || line.starts_with('#') || line.starts_with('"') {
                continue;
            }
            let field = line
                .split(['(', ':'])
                .next()
                .map(str::trim)
                .filter(|field| !field.is_empty());
            if let (Some(definition), Some(field)) = (&current, field) {
                inventory
                    .get_mut(definition)
                    .expect("current definition")
                    .insert(field.into());
            }
        }
        inventory
    }

    fn sha256(bytes: &[u8]) -> String {
        format!("sha256:{:x}", Sha256::digest(bytes))
    }

    #[derive(Clone, Copy)]
    struct ArtifactGoldens {
        manifest: &'static str,
        static_sdl: &'static str,
        runtime_sdl: &'static str,
    }

    async fn assert_role_matrix(
        engine: &GraphqlEngine,
        dialect: SurfaceDialect,
        role: &str,
        expected: ArtifactGoldens,
    ) {
        let stored = engine.surface_for_role(role).unwrap();
        let export = engine.client_surface_for_role(role).unwrap();
        assert!(Arc::ptr_eq(&stored, export.surface()));
        let manifest = export.manifest().unwrap();
        assert_eq!(manifest, export.manifest().unwrap());
        assert_eq!(manifest, independent_manifest(dialect, role));

        let static_sdl = engine.ir_sdl_for_role(role).unwrap();
        let runtime_sdl = engine.sdl_for_role(role).unwrap();
        assert_eq!(
            definition_inventory(&static_sdl),
            definition_inventory(&runtime_sdl),
            "runtime/static definition drift for role `{role}`"
        );

        let query_roots: BTreeSet<String> = manifest
            .roots
            .iter()
            .filter(|root| root.operation == ClientRootOperation::Query)
            .map(|root| root.name.clone())
            .collect();
        let subscription_roots: BTreeSet<String> = manifest
            .roots
            .iter()
            .filter(|root| root.operation == ClientRootOperation::Subscription)
            .map(|root| root.name.clone())
            .collect();
        let runtime_read_roots = type_field_names(&runtime_sdl, "Query")
            .into_iter()
            .filter(|field| field != "commandStatus")
            .collect::<BTreeSet<_>>();
        assert_eq!(query_roots, runtime_read_roots);
        assert_eq!(
            subscription_roots,
            type_field_names(&runtime_sdl, "Subscription")
        );
        let expected_commands = if role == "admin" { 2 } else { 1 };
        assert_eq!(manifest.commands.len(), expected_commands);
        assert_eq!(
            manifest
                .protocol_operations
                .command_status
                .as_ref()
                .unwrap()
                .name,
            "Distributed_CommandStatus"
        );
        for model in &manifest.models {
            let expected_fields: BTreeSet<String> = model
                .fields
                .iter()
                .map(|field| field.name.clone())
                .chain(model.relationships.iter().flat_map(|relationship| {
                    std::iter::once(relationship.name.clone()).chain(
                        relationship
                            .aggregate
                            .iter()
                            .map(|aggregate| aggregate.name.clone()),
                    )
                }))
                .collect();
            assert_eq!(
                expected_fields,
                type_field_names(&static_sdl, &model.typename),
                "manifest/static model drift for {}",
                model.typename
            );
            assert_eq!(
                expected_fields,
                type_field_names(&runtime_sdl, &model.typename),
                "manifest/runtime model drift for {}",
                model.typename
            );
        }

        let model_ids: BTreeSet<_> = manifest
            .models
            .iter()
            .map(|model| model.id.as_str())
            .collect();
        let command_names: BTreeSet<_> = manifest
            .commands
            .iter()
            .map(|command| command.name.as_str())
            .collect();
        let projector_names: BTreeSet<_> = manifest
            .projectors
            .iter()
            .map(|projector| projector.name.as_str())
            .collect();
        match role {
            "restricted" => {
                assert_eq!(model_ids, BTreeSet::from(["OrderView"]));
                assert_eq!(command_names, BTreeSet::from(["order.change"]));
                assert_eq!(
                    type_field_names(&runtime_sdl, "Mutation"),
                    BTreeSet::from(["orders_change".into()])
                );
                assert_eq!(projector_names, BTreeSet::from(["project_orders"]));
                assert!(!query_roots.contains("orders_aggregate"));
                assert_eq!(manifest.models[0].fields.len(), 2);
                assert_eq!(
                    manifest
                        .roots
                        .iter()
                        .find(|root| root.id == "query:orders")
                        .unwrap()
                        .pagination
                        .as_ref()
                        .unwrap()
                        .default_limit,
                    5
                );
            }
            "admin" => {
                assert_eq!(model_ids, BTreeSet::from(["CustomerView", "OrderView"]));
                assert_eq!(
                    command_names,
                    BTreeSet::from(["order.change", "order.force_archive"])
                );
                assert_eq!(
                    type_field_names(&runtime_sdl, "Mutation"),
                    BTreeSet::from(["orders_change".into(), "orders_force_archive".into()])
                );
                assert_eq!(
                    projector_names,
                    BTreeSet::from(["project_customers", "project_orders"])
                );
                assert!(query_roots.contains("customers_aggregate"));
                assert!(query_roots.contains("orders_aggregate"));
            }
            other => panic!("unexpected matrix role `{other}`"),
        }

        let manifest_json = serde_json::to_vec(&manifest).unwrap();
        let actual_manifest = sha256(&manifest_json);
        let actual_static_sdl = sha256(static_sdl.as_bytes());
        let actual_runtime_sdl = sha256(runtime_sdl.as_bytes());
        assert_eq!(
            (
                actual_manifest.as_str(),
                actual_static_sdl.as_str(),
                actual_runtime_sdl.as_str()
            ),
            (expected.manifest, expected.static_sdl, expected.runtime_sdl),
            "{dialect:?}/{role}"
        );
    }

    async fn assert_nested_command_validates(engine: &GraphqlEngine) {
        let operation = "mutation Client_orders_change($commandId: ID!, $input: ChangeOrderInput!) { orders_change(commandId: $commandId, input: $input) { accepted order { order_id status } warnings } }";
        async_graphql::parser::parse_query(operation)
            .expect("generated command operation must parse");

        let request = Request::new(operation).variables(async_graphql::Variables::from_json(
            serde_json::json!({
                "commandId": "0190a000-0000-7000-8000-000000000042",
                "input": {
                    "order_id": "order-1",
                    "patch": {
                        "metadata": {"source": "acceptance"},
                        "status": "READY"
                    }
                }
            }),
        ));
        let mut session = Session::new();
        session.set("x-roles", "admin");
        let response = engine.execute(&session, request).await;
        assert_eq!(response.errors.len(), 1, "{response:?}");
        assert_eq!(
            response.errors[0].message,
            "command host not configured (use graphql_router_with_host or graphql_router_with_service)"
        );
    }

    #[cfg(feature = "sqlite")]
    const SQLITE_RESTRICTED_GOLDENS: ArtifactGoldens = ArtifactGoldens {
        manifest: "sha256:c1c3dd3f242f82225b486542b1737976f791e019f50e015f8755f48d70685f9a",
        static_sdl: "sha256:03252ba251b1ddac611fe567d816f780f0876f9f6ce263be95a3480f88fc2283",
        runtime_sdl: "sha256:3d099b8c0b27f0dcbdd677199f767fcc5993071e06b2c0a7d4aa02caf4e5f4ac",
    };

    #[cfg(feature = "sqlite")]
    const SQLITE_ADMIN_GOLDENS: ArtifactGoldens = ArtifactGoldens {
        manifest: "sha256:94345b9e29bccaa77aa5083eb73014a1a102db7ce1f03f022a2f0e242b5c84d2",
        static_sdl: "sha256:128b85bcd6485d14de62b0976e9f12e8b35ad9e7d96a5627d1edcfcabdb591b3",
        runtime_sdl: "sha256:c0b6d600d353357ab4f393897fb6b7ee51f69546f7a4cc4b6786e42abe621f5c",
    };

    #[cfg(feature = "postgres")]
    const POSTGRES_RESTRICTED_GOLDENS: ArtifactGoldens = ArtifactGoldens {
        manifest: "sha256:c1c3dd3f242f82225b486542b1737976f791e019f50e015f8755f48d70685f9a",
        static_sdl: "sha256:03252ba251b1ddac611fe567d816f780f0876f9f6ce263be95a3480f88fc2283",
        runtime_sdl: "sha256:3d099b8c0b27f0dcbdd677199f767fcc5993071e06b2c0a7d4aa02caf4e5f4ac",
    };

    #[cfg(feature = "postgres")]
    const POSTGRES_ADMIN_GOLDENS: ArtifactGoldens = ArtifactGoldens {
        manifest: "sha256:8ff2691f33789c8267b1338603b2ee3544841f8b17cc5b90ea2e851381dd36de",
        static_sdl: "sha256:afe92660c1700845ed5f3e0ddaacc4b481799c39f86b8eacecb46d1b8f99d421",
        runtime_sdl: "sha256:de4885736fdf22a57ccc55160d63b6d1a7fdb33728dec399e8a81d54ce7e8c09",
    };

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn sqlite_restricted_admin_full_artifact_matrix() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect_lazy("sqlite::memory:")
            .unwrap();
        let engine = matrix_engine(pool.into());
        assert_role_matrix(
            &engine,
            SurfaceDialect::Sqlite,
            "restricted",
            SQLITE_RESTRICTED_GOLDENS,
        )
        .await;
        assert_role_matrix(
            &engine,
            SurfaceDialect::Sqlite,
            "admin",
            SQLITE_ADMIN_GOLDENS,
        )
        .await;
        assert_nested_command_validates(&engine).await;
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn postgres_restricted_admin_full_artifact_matrix() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://postgres:postgres@localhost/distributed_test")
            .unwrap();
        let engine = matrix_engine(pool.into());
        assert_role_matrix(
            &engine,
            SurfaceDialect::Postgres,
            "restricted",
            POSTGRES_RESTRICTED_GOLDENS,
        )
        .await;
        assert_role_matrix(
            &engine,
            SurfaceDialect::Postgres,
            "admin",
            POSTGRES_ADMIN_GOLDENS,
        )
        .await;
        assert_nested_command_validates(&engine).await;
    }

    #[cfg(feature = "sqlite")]
    fn composite_records() -> TableSchema {
        TableSchema {
            model_name: "CompositeRecord".into(),
            table_name: "composite_records".into(),
            columns: vec![
                TableColumn {
                    primary_key: true,
                    ..TableColumn::new("tenant_id", "tenant_id", ColumnType::Text)
                },
                TableColumn {
                    primary_key: true,
                    ..TableColumn::new("record_id", "record_id", ColumnType::Text)
                },
                TableColumn::new("value", "value", ColumnType::Text),
            ],
            primary_key: PrimaryKey::new(["tenant_id", "record_id"]),
            version_column: None,
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: TableKind::ReadModel,
        }
    }

    #[cfg(feature = "sqlite")]
    fn root_arguments(sdl: &str, field: &str) -> BTreeMap<String, String> {
        let marker = format!("{field}(");
        let arguments = sdl
            .split_once(&marker)
            .unwrap_or_else(|| panic!("missing root field `{field}` in SDL:\n{sdl}"))
            .1
            .split_once(')')
            .expect("root arguments should close")
            .0;
        arguments
            .split(',')
            .filter_map(|argument| argument.trim().split_once(':'))
            .map(|(name, ty)| (name.trim().to_string(), ty.trim().to_string()))
            .collect()
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn isolated_composite_key_root_has_runtime_static_manifest_parity() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE composite_records (\
                tenant_id TEXT NOT NULL, \
                record_id TEXT NOT NULL, \
                value TEXT NOT NULL, \
                PRIMARY KEY (tenant_id, record_id)\
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO composite_records (tenant_id, record_id, value) VALUES \
                ('tenant-a', 'record-1', 'first'), \
                ('tenant-a', 'record-2', 'second')",
        )
        .execute(&pool)
        .await
        .unwrap();
        let project = ReadModelCatalog::new("composite-service").table_schema(composite_records());
        let engine = GraphqlEngine::from_schema_catalog(&project, pool.clone())
            .unwrap()
            .roles(&["admin"])
            .grant_all("admin")
            .build()
            .unwrap();
        let static_sdl = engine.ir_sdl_for_role("admin").unwrap();
        let runtime_sdl = engine.sdl_for_role("admin").unwrap();
        let manifest = engine.client_manifest_for_role("admin").unwrap();
        let by_pk = manifest
            .roots
            .iter()
            .find(|root| root.id == "query:composite_records_by_pk")
            .unwrap();
        assert_eq!(
            by_pk
                .arguments
                .iter()
                .map(|argument| argument.name.as_str())
                .collect::<Vec<_>>(),
            vec!["tenant_id", "record_id"]
        );
        let manifest_arguments: BTreeMap<String, String> = by_pk
            .arguments
            .iter()
            .map(|argument| {
                let mut ty = if argument.list {
                    format!("[{}!]", argument.type_name)
                } else {
                    argument.type_name.clone()
                };
                if !argument.nullable {
                    ty.push('!');
                }
                (argument.name.clone(), ty)
            })
            .collect();
        assert_eq!(
            manifest_arguments,
            BTreeMap::from([
                ("record_id".into(), "String!".into()),
                ("tenant_id".into(), "String!".into()),
            ])
        );
        assert_eq!(
            manifest_arguments,
            root_arguments(&static_sdl, "composite_records_by_pk")
        );
        assert_eq!(
            manifest_arguments,
            root_arguments(&runtime_sdl, "composite_records_by_pk")
        );
        let ModelNormalization::Normalized { fields, encoding } = &manifest.models[0].normalization
        else {
            panic!("isolated composite key must be normalized")
        };
        assert_eq!(
            fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            vec!["tenant_id", "record_id"]
        );
        assert_eq!(encoding, "canonical_json_tuple_v1");

        let mut session = Session::new();
        session.set("x-roles", "admin");
        let response = engine
            .execute(
                &session,
                Request::new(
                    r#"{
                        selected: composite_records_by_pk(
                            tenant_id: "tenant-a"
                            record_id: "record-2"
                        ) {
                            tenant_id
                            record_id
                            value
                        }
                        missing: composite_records_by_pk(
                            tenant_id: "tenant-a"
                            record_id: "record-missing"
                        ) {
                            tenant_id
                            record_id
                            value
                        }
                    }"#,
                ),
            )
            .await;
        assert!(response.errors.is_empty(), "{response:?}");
        assert_eq!(
            response.data.into_json().unwrap(),
            serde_json::json!({
                "selected": {
                    "tenant_id": "tenant-a",
                    "record_id": "record-2",
                    "value": "second"
                },
                "missing": null
            })
        );
    }

    #[cfg(feature = "sqlite")]
    fn policy_parents() -> TableSchema {
        TableSchema {
            model_name: "PolicyParentView".into(),
            table_name: "policy_parents".into(),
            columns: vec![TableColumn {
                primary_key: true,
                ..TableColumn::new("parent_id", "parent_id", ColumnType::Text)
            }],
            primary_key: PrimaryKey::new(["parent_id"]),
            version_column: None,
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            relationships: vec![RelationshipDef {
                references: None,
                field_name: "children".into(),
                kind: RelationshipKind::HasMany,
                target_model: "PolicyChildView".into(),
                foreign_key: Some("parent_id".into()),
                through: None,
                target_foreign_key: None,
            }],
            kind: TableKind::ReadModel,
        }
    }

    #[cfg(feature = "sqlite")]
    fn policy_children() -> TableSchema {
        TableSchema {
            model_name: "PolicyChildView".into(),
            table_name: "policy_children".into(),
            columns: vec![
                TableColumn {
                    primary_key: true,
                    ..TableColumn::new("child_id", "child_id", ColumnType::Text)
                },
                TableColumn::new("parent_id", "parent_id", ColumnType::Text),
                TableColumn::new("label", "label", ColumnType::Text),
                TableColumn::new("visibility", "visibility", ColumnType::Text),
            ],
            primary_key: PrimaryKey::new(["child_id"]),
            version_column: None,
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: TableKind::ReadModel,
        }
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn relationship_where_applies_the_target_models_row_policy() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query("CREATE TABLE policy_parents (parent_id TEXT PRIMARY KEY NOT NULL)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE policy_children (\
                child_id TEXT PRIMARY KEY NOT NULL, \
                parent_id TEXT NOT NULL, \
                label TEXT NOT NULL, \
                visibility TEXT NOT NULL\
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO policy_parents (parent_id) VALUES \
                ('parent-allowed'), \
                ('parent-denied')",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO policy_children (child_id, parent_id, label, visibility) VALUES \
                ('child-allowed', 'parent-allowed', 'match', 'allowed'), \
                ('child-denied', 'parent-denied', 'match', 'denied')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let project = ReadModelCatalog::new("relationship-policy-service")
            .table_schema(policy_parents())
            .table_schema(policy_children());
        let mut builder = GraphqlEngine::from_schema_catalog(&project, pool)
            .unwrap()
            .roles(&["restricted"]);
        insert_permission(
            &mut builder,
            "PolicyParentView",
            "restricted",
            read().all_columns(),
        );
        insert_permission(
            &mut builder,
            "PolicyChildView",
            "restricted",
            read().all_columns().rows(col("visibility").eq("allowed")),
        );
        let engine = builder.build().unwrap();
        let mut session = Session::new();
        session.set("x-roles", "restricted");

        let response = engine
            .execute(
                &session,
                Request::new(
                    r#"{
                        policy_parents(
                            where: { children: { label: { _eq: "match" } } }
                        ) {
                            parent_id
                        }
                    }"#,
                ),
            )
            .await;

        assert!(response.errors.is_empty(), "{response:?}");
        assert_eq!(
            response.data.into_json().unwrap(),
            serde_json::json!({
                "policy_parents": [
                    {"parent_id": "parent-allowed"}
                ]
            })
        );
    }

    #[cfg(feature = "sqlite")]
    fn metrics() -> TableSchema {
        TableSchema {
            model_name: "MetricView".into(),
            table_name: "metrics".into(),
            columns: vec![
                TableColumn {
                    primary_key: true,
                    ..TableColumn::new("metric_id", "metric_id", ColumnType::Text)
                },
                TableColumn::new("value", "value", ColumnType::Float),
            ],
            primary_key: PrimaryKey::new(["metric_id"]),
            version_column: None,
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: TableKind::ReadModel,
        }
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn engine_rejects_non_finite_row_policy_literals_and_accepts_finite_values() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            for predicate in [col("value").eq(value), col("value").is_in([value])] {
                let pool = sqlx::sqlite::SqlitePoolOptions::new()
                    .connect_lazy("sqlite::memory:")
                    .unwrap();
                let project = ReadModelCatalog::new("metrics-service").table_schema(metrics());
                let mut builder = GraphqlEngine::from_schema_catalog(&project, pool)
                    .unwrap()
                    .roles(&["restricted"]);
                insert_permission(
                    &mut builder,
                    "MetricView",
                    "restricted",
                    read().all_columns().rows(predicate),
                );
                let error = builder
                    .build()
                    .err()
                    .expect("non-finite row policy literal must fail");
                assert!(error.to_string().contains("must be finite"), "{error}");
            }
        }

        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect_lazy("sqlite::memory:")
            .unwrap();
        let project = ReadModelCatalog::new("metrics-service").table_schema(metrics());
        let mut builder = GraphqlEngine::from_schema_catalog(&project, pool)
            .unwrap()
            .roles(&["restricted"]);
        insert_permission(
            &mut builder,
            "MetricView",
            "restricted",
            read().all_columns().rows(FilterExpr::And(vec![
                col("value").eq(1.25_f64),
                col("value").is_in([-1.25_f64, 0.0, 99.5]),
            ])),
        );
        builder.build().unwrap();
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn engine_rejects_mistyped_cmp_and_every_in_row_policy_literal() {
        let invalid = [
            (
                FilterExpr::Cmp {
                    column: "metric_id".into(),
                    op: crate::graphql::filter::CmpOp::Eq,
                    rhs: Operand::Lit(crate::graphql::LitValue::Json(serde_json::json!(
                        "metric-1"
                    ))),
                },
                "literal kind `json`",
            ),
            (
                FilterExpr::In {
                    column: "value".into(),
                    values: vec![
                        Operand::from(1.0),
                        Operand::Lit(crate::graphql::LitValue::Json(serde_json::json!(2.0))),
                    ],
                    negated: false,
                },
                "IN operand 1",
            ),
            (
                FilterExpr::Cmp {
                    column: "metric_id".into(),
                    op: crate::graphql::filter::CmpOp::HasKey,
                    rhs: Operand::from("tenant"),
                },
                "operator `HasKey`",
            ),
        ];
        for (predicate, expected) in invalid {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .connect_lazy("sqlite::memory:")
                .unwrap();
            let project = ReadModelCatalog::new("metrics-service").table_schema(metrics());
            let mut builder = GraphqlEngine::from_schema_catalog(&project, pool)
                .unwrap()
                .roles(&["restricted"]);
            insert_permission(
                &mut builder,
                "MetricView",
                "restricted",
                read().all_columns().rows(predicate),
            );
            let error = builder
                .build()
                .err()
                .expect("mistyped row-policy literal must fail");
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn sqlite_nocase_can_equate_unequal_code_unit_strings() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let equal: i64 = sqlx::query_scalar("SELECT 'A' = 'a' COLLATE NOCASE")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(equal, 1);
        assert_ne!("A", "a");
    }
}
