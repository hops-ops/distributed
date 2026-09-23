use std::sync::Arc;

use distributed::application::CommandDefinition;
use distributed::graphql::{
    build_surface, surface_for_application_contract, DistributedClientSurfaceExport, GraphqlEngine,
    GraphqlPoolSource, IdentityConfig, OidcConfig, Surface, SurfaceOptions,
};
use distributed::microsvc::Service;
use distributed::{
    Application, ApplicationManifest, InMemoryLockManager, InMemoryRepository, LockError,
    LockManager, Module, SurfaceSpec,
};
use e2e_readmodels::{AuthUsers, BlobGames, ChatMessages, Todos};

use crate::application::{
    DISTRIBUTED_ADMIN_CLIENT_SURFACE, DISTRIBUTED_CLIENT_SURFACE,
    DISTRIBUTED_PUBLIC_CLIENT_SURFACE, E2E_UI_APPLICATION, E2E_UI_MODULE_IDS,
};
use crate::modules::projections;

// Stable only for this local copyable fixture. Real deployments must inject
// their own per-deployment key rather than copying this development value.
const E2E_PROTOCOL_TOKEN_KEY: [u8; 32] = [0xe2; 32];

#[derive(Clone, Default)]
pub(crate) struct ClientSurfaceLocks(Arc<InMemoryLockManager>);

impl LockManager for ClientSurfaceLocks {
    type Lock = distributed::InMemoryLock;

    fn get_lock(&self, id: &str) -> Result<Arc<Self::Lock>, LockError> {
        self.0.get_lock(id)
    }
}

/// GraphQL over todos + chat + blob + AuthUsers.
pub fn build_graphql_engine(
    pool: impl Into<GraphqlPoolSource>,
    service: &Service,
    identity: IdentityConfig,
    change_rx: Option<tokio::sync::broadcast::Receiver<distributed::ReadModelChange>>,
) -> Result<GraphqlEngine, String> {
    build_graphql_engine_with_graphiql(pool, service, identity, change_rx, graphiql_enabled())
}

pub(crate) fn build_graphql_engine_with_graphiql(
    pool: impl Into<GraphqlPoolSource>,
    service: &Service,
    identity: IdentityConfig,
    change_rx: Option<tokio::sync::broadcast::Receiver<distributed::ReadModelChange>>,
    graphiql: bool,
) -> Result<GraphqlEngine, String> {
    build_engine(pool, service, identity, change_rx, graphiql, None)
}

pub(crate) fn build_graphql_engine_with_delivery(
    pool: impl Into<GraphqlPoolSource>,
    service: &Service,
    identity: IdentityConfig,
    change_rx: Option<tokio::sync::broadcast::Receiver<distributed::ReadModelChange>>,
    versions: Option<distributed::graphql::delivery::GatewayVersionStore>,
) -> Result<GraphqlEngine, String> {
    build_engine(
        pool,
        service,
        identity,
        change_rx,
        graphiql_enabled(),
        versions,
    )
}

fn build_engine(
    pool: impl Into<GraphqlPoolSource>,
    service: &Service,
    identity: IdentityConfig,
    change_rx: Option<tokio::sync::broadcast::Receiver<distributed::ReadModelChange>>,
    graphiql: bool,
    versions: Option<distributed::graphql::delivery::GatewayVersionStore>,
) -> Result<GraphqlEngine, String> {
    let projections = projections::projection_owners();
    let mut b = GraphqlEngine::builder(pool)
        .protocol_token_key(E2E_PROTOCOL_TOKEN_KEY)
        .roles(&["user", "admin", "anonymous"])
        .client_application_surface_with_schema_roles(
            DISTRIBUTED_CLIENT_SURFACE,
            ["admin", "user"],
            ["user"],
        )
        .client_application_surface(DISTRIBUTED_ADMIN_CLIENT_SURFACE, ["admin"], ["admin"])
        .client_application_surface(
            DISTRIBUTED_PUBLIC_CLIENT_SURFACE,
            ["anonymous"],
            ["anonymous"],
        )
        .model::<Todos>(Todos::permissions())
        .model::<ChatMessages>(ChatMessages::permissions())
        .model::<BlobGames>(BlobGames::permissions())
        .model::<AuthUsers>(AuthUsers::permissions())
        .service(service)
        .client_projection_owners([
            projections.todo.into(),
            projections.chat.into(),
            projections.blob.into(),
        ])
        .identity(identity)
        .graphiql(graphiql);
    if let Some(rx) = change_rx {
        b = b.change_stream(rx);
    }
    if let Some(versions) = versions {
        b = b.gateway_versions(versions);
    }
    b.build().map_err(|e| e.to_string())
}

fn pool_free_client_surface(application: &str, roles: &[&str]) -> DistributedClientSurfaceExport {
    pool_free_client_surface_contract(application, roles, roles)
}

fn pool_free_client_surface_contract(
    application: &str,
    eligible_roles: &[&str],
    schema_roles: &[&str],
) -> DistributedClientSurfaceExport {
    let full = pool_free_full_surface();
    let eligible = eligible_roles
        .iter()
        .map(|role| (*role).to_string())
        .collect::<Vec<_>>();
    let schema = schema_roles
        .iter()
        .map(|role| (*role).to_string())
        .collect::<Vec<_>>();
    let grants = e2e_readmodels::application_grants();
    let selected =
        surface_for_application_contract(&full, application, &eligible, &schema, &grants)
            .expect("e2e-ui application Surface should select");
    DistributedClientSurfaceExport::from_selected("e2e-ui", selected)
        .expect("e2e-ui application Surface should export")
}

fn pool_free_full_surface() -> Surface {
    let project = e2e_readmodels::distributed_manifest();
    let service = pool_free_service();
    let projections = projections::projection_owners();
    build_surface(&project.tables, &SurfaceOptions::sqlite())
        .expect("e2e-ui client Surface should build")
        .with_projection_owners([
            projections.todo.into(),
            projections.chat.into(),
            projections.blob.into(),
        ])
        .expect("e2e-ui projector topology should bind")
        .with_service(&service)
        .expect("e2e-ui typed Service inventory should bind")
}

fn pool_free_service() -> Service {
    let repository = InMemoryRepository::new();
    crate::modules::compose::build_service(
        repository.clone(),
        ClientSurfaceLocks::default(),
        repository,
    )
}

/// Logical application artifact consumed by `distributed describe` and the
/// coherent lifecycle builder. Runtime handlers and repositories are erased.
pub fn application_manifest() -> ApplicationManifest {
    let surface = SurfaceSpec::from_surface(E2E_UI_APPLICATION, &pool_free_full_surface())
        .expect("e2e-ui full Surface should compile into an application artifact");

    let commands = pool_free_service()
        .command_specs()
        .expect("e2e-ui typed commands should compile into portable specs")
        .into_iter()
        .map(|command| {
            let exposed = surface
                .commands
                .iter()
                .find(|exposed| exposed.id == command.id)
                .expect("full e2e-ui Surface should expose every application command");
            command
                .with_surface_binding(exposed)
                .expect("e2e-ui Surface command should bind to its portable declaration")
        })
        .collect::<Vec<_>>();
    let modules = E2E_UI_MODULE_IDS
        .iter()
        .map(|module_id| {
            let prefix = format!("{module_id}.");
            let definitions = commands
                .iter()
                .filter(|command| command.id.starts_with(&prefix))
                .cloned()
                .map(CommandDefinition::contract)
                .collect::<Vec<_>>();
            Module::new(*module_id)
                .command_definitions(definitions)
                .build()
                .expect("e2e-ui application module should compile")
        })
        .collect::<Vec<_>>();

    Application::new(E2E_UI_APPLICATION)
        .modules(modules)
        .surface(surface)
        .build()
        .expect("e2e-ui application manifest should compile")
        .manifest()
        .clone()
}

/// Pool-free normal application export consumed by `distributed client-manifest`.
pub fn distributed_client_surface() -> DistributedClientSurfaceExport {
    pool_free_client_surface_contract(DISTRIBUTED_CLIENT_SURFACE, &["admin", "user"], &["user"])
}

pub fn distributed_admin_client_surface() -> DistributedClientSurfaceExport {
    pool_free_client_surface(DISTRIBUTED_ADMIN_CLIENT_SURFACE, &["admin"])
}

pub fn distributed_public_client_surface() -> DistributedClientSurfaceExport {
    pool_free_client_surface(DISTRIBUTED_PUBLIC_CLIENT_SURFACE, &["anonymous"])
}

pub fn dev_identity() -> IdentityConfig {
    IdentityConfig::dev_headers()
}

pub fn graphiql_enabled() -> bool {
    match std::env::var("GRAPHIQL") {
        Ok(v) => {
            let v = v.trim();
            !(v == "0" || v.eq_ignore_ascii_case("false") || v.eq_ignore_ascii_case("off"))
        }
        Err(_) => true,
    }
}

fn env_clean(name: &str) -> String {
    let mut s = std::env::var(name).unwrap_or_default().trim().to_string();
    for _ in 0..2 {
        if s.len() >= 2
            && ((s.starts_with('\'') && s.ends_with('\''))
                || (s.starts_with('"') && s.ends_with('"')))
        {
            s = s[1..s.len() - 1].trim().to_string();
        } else {
            break;
        }
    }
    s
}

pub fn identity_from_env() -> IdentityConfig {
    let iss = env_clean("OIDC_ISSUER");
    let aud = env_clean("OIDC_AUDIENCE");
    if iss.is_empty() || aud.is_empty() {
        eprintln!("e2e-ui: OIDC_* unset — using DevHeaders (local only)");
        return dev_identity();
    }
    let jwks = env_clean("OIDC_JWKS_URI");
    eprintln!("e2e-ui: OidcBearer issuer={iss} audience={aud}");
    oidc_bearer_config(
        iss,
        aud,
        if jwks.is_empty() { None } else { Some(jwks) },
        None,
    )
}

pub fn oidc_bearer_config(
    issuer: impl Into<String>,
    audience: impl Into<String>,
    jwks_uri: Option<String>,
    static_jwks: Option<String>,
) -> IdentityConfig {
    let mut oidc = OidcConfig::new(issuer, audience);
    if let Some(uri) = jwks_uri.filter(|s| !s.is_empty()) {
        oidc.jwks_uri = Some(uri);
    }
    if let Some(jwks) = static_jwks {
        oidc = oidc.with_static_jwks(jwks);
    }
    let cid = env_clean("OIDC_CLIENT_ID");
    if !cid.is_empty() {
        oidc.extra_audiences = vec![cid];
    }
    oidc.claim_map.engine_roles = vec!["user".into(), "admin".into()];
    oidc.claim_map.role_claims = vec![
        "groups".into(),
        "roles".into(),
        "realm_access.roles".into(),
        "urn:zitadel:iam:org:project:roles".into(),
    ];
    oidc.require_auth = false;
    IdentityConfig::oidc_bearer(oidc)
}

#[cfg(test)]
mod client_surface_tests {
    use super::*;
    use crate::application::{DISTRIBUTED_CLIENT_SURFACE, DISTRIBUTED_PUBLIC_CLIENT_SURFACE};
    use crate::modules::compose::build_service;
    use distributed::InMemoryRepository;

    #[test]
    fn application_manifest_compiles_real_modules_and_surfaces() {
        let manifest = application_manifest();
        manifest
            .validate()
            .expect("valid e2e-ui application manifest");
        assert_eq!(manifest.name, "e2e-ui");
        assert_eq!(
            manifest.module_ids(),
            vec!["blob", "chat", "identity", "todo"]
        );
        assert_eq!(manifest.surfaces.len(), 1);
        assert!(!manifest.commands.is_empty());
    }

    #[test]
    fn pool_free_user_and_admin_exports_compile_real_manifests() {
        distributed_client_surface()
            .manifest()
            .expect("normal application client manifest");
        distributed_admin_client_surface()
            .manifest()
            .expect("elevated application client manifest");
    }

    #[test]
    fn application_todos_keep_portable_owner_row_policy_for_optimistic_list_inserts() {
        use distributed::graphql::ClientRowPolicy;

        let manifest = distributed_client_surface().manifest().unwrap();
        let todos = manifest
            .models
            .iter()
            .find(|model| model.typename == "Todos")
            .expect("Todos model on application surface");
        match &todos.row_policy {
            ClientRowPolicy::Predicate { expression } => {
                let text = serde_json::to_string(expression).expect("serialize row policy");
                assert!(
                    text.contains("x-user-id") && text.contains("owner_id"),
                    "owner claim predicate must be client-portable: {text}"
                );
            }
            other => panic!(
                "Todos must not collapse to server-only row policy (blocks optimistic create list membership); got {other:?}"
            ),
        }

        let blob = manifest
            .models
            .iter()
            .find(|model| model.typename == "BlobGames")
            .expect("BlobGames model on application surface");
        assert!(
            matches!(blob.row_policy, ClientRowPolicy::Predicate { .. }),
            "BlobGames should keep portable owner row policy"
        );
    }

    #[test]
    fn todo_commands_auto_derive_optimism_without_applies() {
        use distributed::graphql::{ClientProjectionPreviewSource, ClientProjectionValue};

        let manifest = distributed_client_surface().manifest().unwrap();
        let create = manifest
            .commands
            .iter()
            .find(|command| command.mutation_field == "todos_create")
            .expect("todos_create command");
        let projection = create
            .extensions
            .projection
            .as_ref()
            .expect("todos_create must export projection extension");
        assert!(
            !projection.preview_occurrences.is_empty(),
            "auto-optimism must invent preview occurrences from emits + projection arms"
        );
        let sources: Vec<_> = projection
            .preview_occurrences
            .iter()
            .flat_map(|occurrence| occurrence.values.iter().map(|value| &value.source))
            .collect();
        assert!(
            sources.iter().any(|source| matches!(
                source,
                ClientProjectionPreviewSource::Input { path } if path == &["title"]
            )),
            "create title must map from command input: {sources:?}"
        );
        assert!(
            sources.iter().any(|source| matches!(
                source,
                ClientProjectionPreviewSource::GeneratedDefault { path } if path == &["todo_id"]
            )),
            "create todo_id must map from generated default: {sources:?}"
        );
        assert!(
            sources.iter().any(|source| matches!(
                source,
                ClientProjectionPreviewSource::TrustedPreset { name, codec }
                    if name == "x-user-id" && codec == "string"
            )),
            "create owner_id must map from row-policy claim: {sources:?}"
        );
        assert!(
            sources.iter().any(|source| matches!(
                source,
                ClientProjectionPreviewSource::Constant {
                    value: ClientProjectionValue::String(value),
                } if value == "open"
            )),
            "create status must come from the sourced transition: {sources:?}"
        );
        assert!(
            sources
                .iter()
                .any(|source| matches!(source, ClientProjectionPreviewSource::Null)),
            "create assignee_id must come from the sourced transition: {sources:?}"
        );

        // Sparse update commands only need the known input slots.
        let rename = manifest
            .commands
            .iter()
            .find(|command| command.mutation_field == "todos_rename")
            .expect("todos_rename command");
        let rename_projection = rename
            .extensions
            .projection
            .as_ref()
            .expect("todos_rename projection");
        let rename_sources: Vec<_> = rename_projection
            .preview_occurrences
            .iter()
            .flat_map(|occurrence| occurrence.values.iter().map(|value| &value.source))
            .collect();
        assert!(
            rename_sources.iter().any(|source| matches!(
                source,
                ClientProjectionPreviewSource::Input { path } if path == &["title"]
            )),
            "rename title must map from input without .applies: {rename_sources:?}"
        );
        assert!(
            rename_sources.iter().any(|source| matches!(
                source,
                ClientProjectionPreviewSource::Input { path } if path == &["todo_id"]
            )),
            "rename todo_id must map from input without .applies: {rename_sources:?}"
        );

        for (mutation_field, expected_status) in [
            ("todos_complete", "completed"),
            ("todos_reopen", "open"),
            ("todos_archive", "archived"),
        ] {
            let command = manifest
                .commands
                .iter()
                .find(|command| command.mutation_field == mutation_field)
                .unwrap_or_else(|| panic!("{mutation_field} command"));
            let status_sources: Vec<_> = command
                .extensions
                .projection
                .as_ref()
                .unwrap_or_else(|| panic!("{mutation_field} projection"))
                .preview_occurrences
                .iter()
                .flat_map(|occurrence| occurrence.values.iter().map(|value| &value.source))
                .collect();
            assert!(
                status_sources.iter().any(|source| matches!(
                    source,
                    ClientProjectionPreviewSource::Constant {
                        value: ClientProjectionValue::String(value),
                    } if value == expected_status
                )),
                "{mutation_field} status must come from the sourced transition: {status_sources:?}"
            );
        }

        let purge = manifest
            .commands
            .iter()
            .find(|command| command.mutation_field == "todos_purge")
            .expect("todos_purge command");
        let purge_projection = purge
            .extensions
            .projection
            .as_ref()
            .expect("todos_purge projection");
        let purge_sources: Vec<_> = purge_projection
            .preview_occurrences
            .iter()
            .flat_map(|occurrence| occurrence.values.iter().map(|value| &value.source))
            .collect();
        assert!(
            purge_sources.iter().any(|source| matches!(
                source,
                ClientProjectionPreviewSource::Input { path } if path == &["todo_id"]
            )),
            "purge aggregate id must map from input without envelope .applies: {purge_sources:?}"
        );
    }

    #[test]
    fn chat_and_blob_commands_auto_derive_optimism_without_applies() {
        use distributed::graphql::ClientProjectionPreviewSource;

        let manifest = distributed_client_surface().manifest().unwrap();

        let post = manifest
            .commands
            .iter()
            .find(|command| command.mutation_field == "chat_messages_post")
            .expect("chat_messages_post command");
        let post_projection = post
            .extensions
            .projection
            .as_ref()
            .expect("chat post projection");
        let post_sources: Vec<_> = post_projection
            .preview_occurrences
            .iter()
            .flat_map(|occurrence| occurrence.values.iter().map(|value| &value.source))
            .collect();
        assert!(
            !post_projection.preview_occurrences.is_empty(),
            "chat post must auto-derive preview occurrences"
        );
        assert!(
            post_sources.iter().any(|source| matches!(
                source,
                ClientProjectionPreviewSource::Input { path } if path == &["body"]
            )),
            "chat body from input: {post_sources:?}"
        );
        assert!(
            post_sources.iter().any(|source| matches!(
                source,
                ClientProjectionPreviewSource::Input { path } if path == &["message_id"]
            )),
            "chat message_id from input: {post_sources:?}"
        );
        assert!(
            post_sources.iter().any(|source| matches!(
                source,
                ClientProjectionPreviewSource::TrustedPreset { name, codec }
                    if name == "x-user-id" && codec == "string"
            )),
            "chat author_id must bind the authenticated user without .applies: {post_sources:?}"
        );

        let blob_move = manifest
            .commands
            .iter()
            .find(|command| command.mutation_field == "blob_games_move")
            .expect("blob_games_move command");
        let move_projection = blob_move
            .extensions
            .projection
            .as_ref()
            .expect("blob move projection");
        assert!(
            !move_projection.preview_occurrences.is_empty(),
            "blob move still exports projection arms for Atomic sealing"
        );
        // Thin input: only game_id + direction. Board fields come from pure
        // reduce (`blob.simulate_move` over the known cache row) + Atomic seal.
        let move_input = match &blob_move.input {
            distributed::graphql::ClientCommandShape::Object { definition } => definition,
            other => panic!("blob move should be object input, got {other:?}"),
        };
        let field_names: Vec<_> = move_input
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect();
        assert_eq!(
            field_names,
            vec!["direction", "game_id"],
            "blob move input must stay thin (no fat board fields on the wire)"
        );
    }

    #[test]
    fn chat_manifest_uses_unit_partition_so_lobby_live_can_stay_active() {
        let manifest = distributed_client_surface().manifest().unwrap();
        let program = manifest
            .projection_programs
            .iter()
            .find(|program| program.name == "project_chat_messages")
            .expect("Chat projection program should be exported");
        assert!(
            program.arms.iter().all(|arm| matches!(
                &arm.partition,
                distributed::graphql::ClientProjectionPartition::Unit
            )),
            "lobby chat uses unit partition so the chat_messages live query can advertise \
             supported index evidence (room isolation stays in the GraphQL where clause). \
             Surface-wide live_resume may still be false when owner-scoped models share the surface."
        );
    }

    #[test]
    fn blob_projection_owner_has_no_async_fact_route() {
        let manifest = distributed_client_surface().manifest().unwrap();
        let owner = manifest
            .projectors
            .iter()
            .find(|projector| projector.name == "project_blob")
            .expect("Blob direct owner should be exported");
        assert!(owner.facts.is_empty());
        assert!(!owner.causal_confirmation);

        let repository = InMemoryRepository::new();
        let service = build_service(
            repository.clone(),
            crate::modules::graphql::ClientSurfaceLocks::default(),
            repository,
        );
        let plan = service.subscription_plan();
        for event in [
            "todo.created",
            "todo.renamed",
            "todo.completed",
            "todo.reopened",
            "todo.archived",
            "todo.force_archived",
            "todo.purged",
            "chat_message.posted",
        ] {
            assert!(
                plan.events.iter().any(|candidate| candidate == event),
                "eventual modeled projection must subscribe to {event}"
            );
        }
        for fact in [
            "blob.started",
            "blob.initialized",
            "blob.level_started",
            "blob.moved",
        ] {
            assert!(
                !plan.events.iter().any(|event| event == fact),
                "direct-only Blob ownership must not register an async route for {fact}"
            );
        }
    }

    #[tokio::test]
    async fn graphiql_does_not_change_the_postgres_runtime_client_manifest() {
        let generated = distributed_client_surface().manifest().unwrap();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://postgres:postgres@localhost/distributed")
            .unwrap();
        let repository = distributed::PostgresRepository::new(pool.clone());
        let service = build_service(
            repository.clone(),
            distributed::PostgresLockManager::new(pool),
            repository.clone(),
        );
        let engine = crate::modules::graphql::build_graphql_engine_with_graphiql(
            &repository,
            &service,
            dev_identity(),
            None,
            true,
        )
        .expect("engine");
        let runtime = engine
            .client_manifest_for_application(
                DISTRIBUTED_CLIENT_SURFACE,
                &["admin", "user"],
                &["user"],
            )
            .unwrap();

        assert_eq!(generated, runtime);

        let make_request = || {
            serde_json::from_value(serde_json::json!({
                "query": "{ todos @skip(if: true) { todo_id } }",
                "extensions": {
                    "distributed": {
                        "client": {
                            "surface": {
                                "kind": "application",
                                "name": DISTRIBUTED_CLIENT_SURFACE,
                                "eligible_roles": ["admin", "user"],
                                "schema_roles": ["user"]
                            },
                            "schemaHash": generated.schema_fingerprint
                        }
                    }
                }
            }))
            .expect("generated application request")
        };
        let mut session = distributed::microsvc::Session::new();
        session.set("x-roles", "user");
        session.set("x-user-id", "person-1");
        let response = engine.execute(&session, make_request()).await;
        assert!(
            !response.is_err(),
            "the runtime must accept the generated application surface: {:?}",
            response.errors
        );
        // Multi-role admin principal may open the same portable contract.
        let mut admin = session.clone();
        admin.set("x-roles", "admin,user");
        let admin_response = engine.execute(&admin, make_request()).await;
        assert!(
            !admin_response.is_err(),
            "admin with user asserted roles must open e2e-ui: {:?}",
            admin_response.errors
        );
        let envelope = response
            .extensions
            .get("distributed")
            .expect("distributed protocol envelope");
        let envelope = serde_json::to_value(envelope).expect("serialized protocol envelope");
        assert_eq!(
            envelope["schemaHash"], generated.schema_fingerprint,
            "the authoritative response must attest the generated schema"
        );
    }

    /// Empty-session open of e2e-ui-public + chat query (anonymous privilege).
    ///
    /// Bare protocol path for unauthenticated lobby peeks; UI route `/public`
    /// documents the same surface name and extension shape.
    #[tokio::test]
    async fn public_surface_opens_and_queries_chat_without_identity() {
        let generated = distributed_public_client_surface().manifest().unwrap();
        assert_eq!(
            generated.surface,
            distributed::graphql::ClientSurfaceIdentity::application(
                DISTRIBUTED_PUBLIC_CLIENT_SURFACE,
                ["anonymous"],
                ["anonymous"],
            )
        );
        let repository = distributed::SqliteRepository::connect_and_migrate("sqlite::memory:")
            .await
            .expect("sqlite memory repo");
        let registry = e2e_readmodels::distributed_manifest()
            .table_registry()
            .expect("registry");
        repository
            .bootstrap_table_schema_for_dev(&registry)
            .await
            .expect("bootstrap tables");
        let service = build_service(
            repository.clone(),
            crate::modules::graphql::ClientSurfaceLocks::default(),
            repository.clone(),
        );
        let engine = crate::modules::graphql::build_graphql_engine_with_graphiql(
            &repository,
            &service,
            dev_identity(),
            None,
            false,
        )
        .expect("engine");
        let runtime = engine
            .client_manifest_for_application(
                DISTRIBUTED_PUBLIC_CLIENT_SURFACE,
                &["anonymous"],
                &["anonymous"],
            )
            .expect("public surface registered");
        assert_eq!(generated.schema_fingerprint, runtime.schema_fingerprint);

        let request = serde_json::from_value(serde_json::json!({
            "query": "{ chat_messages(limit: 5, offset: 0) { message_id body room_id } }",
            "extensions": {
                "distributed": {
                    "client": {
                        "surface": {
                            "kind": "application",
                            "name": DISTRIBUTED_PUBLIC_CLIENT_SURFACE,
                            "eligible_roles": ["anonymous"],
                            "schema_roles": ["anonymous"]
                        },
                        "schemaHash": generated.schema_fingerprint
                    }
                }
            }
        }))
        .expect("public application request");

        // No x-user-id, no x-roles — unauthenticated principal.
        let session = distributed::microsvc::Session::new();
        let response = engine.execute(&session, request).await;
        assert!(
            !response.is_err(),
            "anonymous open + chat query must succeed: {:?}",
            response.errors
        );
        let data = response.data.into_json().expect("json data");
        assert!(
            data.get("chat_messages")
                .and_then(|v| v.as_array())
                .is_some(),
            "expected chat_messages array: {data}"
        );
        let envelope = response
            .extensions
            .get("distributed")
            .expect("distributed protocol envelope");
        let envelope = serde_json::to_value(envelope).expect("serialized protocol envelope");
        assert_eq!(envelope["schemaHash"], generated.schema_fingerprint);
    }

    #[test]
    fn module_inventory_lists_todo_chat_blob_identity() {
        assert_eq!(
            crate::E2E_UI_MODULE_IDS,
            &["todo", "chat", "blob", "identity"]
        );
        assert_eq!(crate::application::MODULE_DECLARATIONS.len(), 4);
    }
}
