//! Rust service-crate templates: `Cargo.toml` and the `src/**` sources
//! (lib/main/manifest/service/models/handlers/read_models). Pure — each method
//! returns the file contents as a `String`.

use super::names::{message_owner, rust_string, toml_string, MessageHandler, ModelScaffold};
use super::Scaffold;
use crate::{MetricsTarget, ServiceTransport, StoreTarget};

impl Scaffold {
    pub(super) fn cargo_toml(&self) -> String {
        let distributed_path = toml_string(&self.distributed_dependency_path);
        let features = self
            .distributed_features()
            .into_iter()
            .map(toml_string)
            .collect::<Vec<_>>()
            .join(", ");
        // Keep in lockstep with the `distributed` crate's own axum major: the
        // Knative main.rs passes distributed's `Router` to this axum's `serve`.
        let axum = if self.transport == ServiceTransport::Knative {
            "axum = \"0.8\"\n"
        } else {
            ""
        };
        let tracing_deps = if self.tracing {
            r#"opentelemetry = { version = "0.32", default-features = false, features = ["trace"] }
opentelemetry-otlp = { version = "0.32", default-features = false, features = ["trace", "grpc-tonic"] }
opentelemetry_sdk = { version = "0.32", default-features = false, features = ["trace"] }
tracing-opentelemetry = { version = "0.33", default-features = false }
tracing-subscriber = { version = "0.3", features = ["env-filter", "fmt"] }
"#
        } else {
            ""
        };

        format!(
            r#"[package]
name = {package_name}
version = "0.1.0"
edition = "2021"

[workspace]

[package.metadata.distributed.application]
entrypoint = "application_manifest"

[package.metadata.distributed.runtime]
binary = {package_name}

[dependencies]
distributed = {{ path = {distributed_path}, features = [{features}] }}
{axum}serde = {{ version = "1", features = ["derive"] }}
serde_json = "1"
tokio = {{ version = "1", features = ["macros", "net", "rt-multi-thread"] }}
{tracing_deps}
"#,
            package_name = toml_string(&self.names.package_name),
        )
    }

    fn distributed_features(&self) -> Vec<&'static str> {
        let mut features = Vec::new();
        match self.transport {
            ServiceTransport::Http => features.push("http"),
            ServiceTransport::Knative => features.push("http"),
        }
        match self.store {
            StoreTarget::Postgres => features.push("postgres"),
            StoreTarget::Sqlite => features.push("sqlite"),
            StoreTarget::InMemory => {}
        }
        if self.metrics == Some(MetricsTarget::Prometheus) {
            features.push("metrics");
        }
        if self.tracing {
            features.push("otel");
        }
        if self.query_api {
            features.push("graphql");
        }
        features
    }

    pub(super) fn lib_rs(&self) -> String {
        let models = if !self.models.is_empty() || (self.query_api && !self.commands.is_empty()) {
            "pub mod models;\n"
        } else {
            ""
        };
        let read_models = if self.include_read_models {
            "pub mod read_models;\n"
        } else {
            ""
        };
        let query = if self.query_api {
            "pub mod query;\n"
        } else {
            ""
        };
        format!(
            r#"pub mod handlers;
pub mod manifest;
{models}{read_models}{query}pub mod service;

pub use manifest::{{application_manifest, read_model_catalog, service_descriptor}};
"#
        )
    }

    pub(super) fn main_rs(&self) -> String {
        let error_type = if self.tracing || self.query_api {
            "Box<dyn std::error::Error + Send + Sync + 'static>"
        } else {
            "Box<dyn std::error::Error>"
        };
        let tracing_init = if self.tracing {
            "    let tracer_provider = init_tracing()?;\n"
        } else {
            ""
        };
        let tracing_shutdown = if self.tracing {
            r#"    let shutdown = tracer_provider.shutdown();
    result?;
    shutdown?;
"#
        } else {
            "    result?;\n"
        };
        let tracing_setup = self.tracing_setup_rs(error_type);
        let service_init = if self.query_api {
            format!(
                "    let service = {crate}::service::build_with_graphql().await?;\n",
                crate = self.names.crate_ident
            )
        } else {
            format!(
                "    let service = {crate}::service::in_memory();\n",
                crate = self.names.crate_ident
            )
        };
        let serve_block = match self.transport {
            ServiceTransport::Http => {
                "    let result = distributed::microsvc::serve(service, &addr).await;\n".to_string()
            }
            ServiceTransport::Knative => r#"    let result = async {
        let listener = tokio::net::TcpListener::bind(&addr).await?;
        let app = distributed::microsvc::cloud_events_router(service);
        axum::serve(listener, app).await
    }
    .await;
"#
            .to_string(),
        };

        format!(
            r#"#[tokio::main]
async fn main() -> Result<(), {error_type}> {{
{tracing_init}    let addr = std::env::var("BIND")
        .or_else(|_| std::env::var("BIND_ADDR"))
        .unwrap_or_else(|_| "127.0.0.1:3000".to_string());
{service_init}{serve_block}{tracing_shutdown}    Ok(())
}}

{tracing_setup}"#,
        )
    }

    fn tracing_setup_rs(&self, error_type: &str) -> String {
        if !self.tracing {
            return String::new();
        }

        r#"fn init_tracing() -> Result<opentelemetry_sdk::trace::SdkTracerProvider, __ERROR_TYPE__> {
    use opentelemetry::trace::TracerProvider as _;
    use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

    let exporter = opentelemetry_otlp::SpanExporter::builder().build()?;
    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .build();
    let tracer = tracer_provider.tracer(env!("CARGO_PKG_NAME"));
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .try_init()?;

    Ok(tracer_provider)
}
"#
        .replace("__ERROR_TYPE__", error_type)
    }

    pub(super) fn manifest_rs(&self) -> String {
        let read_model_import = if self.include_read_models && !self.read_models.is_empty() {
            format!(
                "use crate::read_models::{{{}}};\n\n",
                self.read_models
                    .iter()
                    .map(|model| model.view_ident.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        } else {
            String::new()
        };
        let read_model_registration = self
            .read_models
            .iter()
            .map(|model| format!("        .read_model::<{}>()\n", model.view_ident))
            .collect::<String>();
        format!(
            r#"use distributed::{{
    Application, ApplicationManifest, ReadModelCatalog, SurfaceSpec,
}};

{read_model_import}pub fn application_manifest() -> ApplicationManifest {{
    let catalog = read_model_catalog();
    let surface = distributed::graphql::build_surface(
        &catalog.tables,
        &distributed::graphql::SurfaceOptions::sqlite(),
    )
    .expect("read-model catalog should compile into an application Surface");
    let surface = SurfaceSpec::from_surface({project_name}, &surface)
        .expect("application Surface contract should be portable");
    Application::new({project_name})
        .surface(surface)
        .build()
        .expect("application manifest should compile")
        .manifest()
        .clone()
}}

pub fn read_model_catalog() -> ReadModelCatalog {{
    ReadModelCatalog::new({project_name})
{read_model_registration}}}

pub fn service_descriptor() -> distributed::microsvc::ServiceDescriptor {{
    crate::service::descriptor()
}}
"#,
            project_name = rust_string(&self.names.package_name),
        )
    }

    pub(super) fn service_rs(&self) -> String {
        let repo_import = if self.query_api {
            match self.store {
                StoreTarget::Postgres => "PostgresRepository",
                _ => "SqliteRepository",
            }
        } else {
            "InMemoryRepository"
        };
        let mut manifest_imports = vec![
            "microsvc::{Routes, Service}",
            repo_import,
            "ServiceDescriptor",
        ];
        if self.metrics == Some(MetricsTarget::Prometheus) {
            manifest_imports.push("MetricsEndpointDescriptor");
        }
        if self.tracing {
            manifest_imports.push("TracingDescriptor");
        }
        if self.query_api && !self.commands.is_empty() {
            manifest_imports.push("AggregateRepository");
        }
        let manifest_imports = manifest_imports.join(", ");
        let registrations = self
            .commands
            .iter()
            .map(|handler| format!("        command handlers::{},\n", handler.module_ident))
            .chain(
                self.events
                    .iter()
                    .map(|handler| format!("        event handlers::{},\n", handler.module_ident)),
            )
            .collect::<String>();
        let event_registrations = self
            .events
            .iter()
            .map(|handler| format!("        event handlers::{},\n", handler.module_ident))
            .collect::<String>();
        let typed_route_attachments = self
            .commands
            .iter()
            .map(|handler| {
                let model_type = self
                    .command_model(handler)
                    .map(|model| model.type_ident.as_str())
                    .unwrap_or("CommandAggregate");
                format!(
                    r#"    let service = service.routes(
        Routes::new()
            .with_repo(AggregateRepository::<_, crate::models::{model_type}>::new(repo.clone()))
            .typed_command(handlers::{module}::command())
            .handle(handlers::{module}::handle),
    );
"#,
                    module = handler.module_ident,
                )
            })
            .collect::<String>();
        let event_route_attachment = if self.events.is_empty() {
            String::new()
        } else {
            format!(
                r#"    let service = service.routes(distributed::routes!(
        Routes::new().with_dependencies(repo.clone()),
{event_registrations}    ));
"#
            )
        };
        let manifest_commands = self
            .commands
            .iter()
            .map(|handler| {
                format!(
                    "        .command(handlers::{}::COMMAND)\n",
                    handler.module_ident
                )
            })
            .collect::<String>();
        let manifest_events = self
            .events
            .iter()
            .map(|handler| {
                format!(
                    "        .event(handlers::{}::EVENT)\n",
                    handler.module_ident
                )
            })
            .collect::<String>();
        let transport = match self.transport {
            ServiceTransport::Http => "http",
            ServiceTransport::Knative => "knative",
        };
        let manifest_metrics = if self.metrics == Some(MetricsTarget::Prometheus) {
            "        .metrics(MetricsEndpointDescriptor::prometheus_default())\n"
        } else {
            ""
        };
        let manifest_tracing = if self.tracing {
            "        .tracing(TracingDescriptor::otlp())\n"
        } else {
            ""
        };

        if self.query_api {
            let (repo_ty, connect_default) = match self.store {
                StoreTarget::Postgres => (
                    "PostgresRepository",
                    r#""postgres://postgres:postgres@127.0.0.1:5432/postgres""#,
                ),
                _ => ("SqliteRepository", r#""sqlite::memory:""#),
            };
            let protocol_key_read = if self.commands.is_empty() {
                String::new()
            } else {
                "    let protocol_token_key = graphql_protocol_token_key()?;\n".into()
            };
            let protocol_key_argument = if self.commands.is_empty() {
                ""
            } else {
                ", protocol_token_key"
            };
            let protocol_key_helper = if self.commands.is_empty() {
                String::new()
            } else {
                r#"
fn graphql_protocol_token_key(
) -> Result<[u8; 32], Box<dyn std::error::Error + Send + Sync>> {
    use std::io::{Error, ErrorKind};

    let secret = std::env::var("DISTRIBUTED_GRAPHQL_PROTOCOL_TOKEN_KEY").map_err(|_| {
        Error::new(
            ErrorKind::NotFound,
            "DISTRIBUTED_GRAPHQL_PROTOCOL_TOKEN_KEY must be a stable 32-byte deployment secret",
        )
    })?;
    let bytes = secret.into_bytes();
    let key: [u8; 32] = bytes.try_into().map_err(|_| {
        Error::new(
            ErrorKind::InvalidInput,
            "DISTRIBUTED_GRAPHQL_PROTOCOL_TOKEN_KEY must be exactly 32 UTF-8 bytes",
        )
    })?;
    if key.iter().all(|byte| *byte == 0) {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "DISTRIBUTED_GRAPHQL_PROTOCOL_TOKEN_KEY must not be all zero",
        )
        .into());
    }
    Ok(key)
}
"#
                .to_string()
            };
            return format!(
                r#"use std::sync::Arc;

use distributed::{{{manifest_imports}}};

use crate::handlers;

pub type ServiceRepo = {repo_ty};

pub fn build(repo: ServiceRepo) -> Arc<Service> {{
    Arc::new(build_service(repo))
}}

fn build_service(repo: ServiceRepo) -> Service {{
    let service = Service::new()
        .named({service_name});
{typed_route_attachments}{event_route_attachment}    service
}}

/// Build the service with GraphQL mounted at `POST /graphql`.
///
/// Reads `DATABASE_URL` (defaults to {connect_default} for local dev).
pub async fn build_with_graphql() -> Result<Arc<Service>, Box<dyn std::error::Error + Send + Sync>> {{
    let database_url =
        std::env::var("DATABASE_URL").unwrap_or_else(|_| {connect_default}.to_string());
    let repo = ServiceRepo::connect_and_migrate(&database_url).await?;
    let service = build_service(repo.clone());
{protocol_key_read}    let engine = crate::query::build_engine(&repo, &service{protocol_key_argument})?;
    Ok(Arc::new(service.try_with_graphql(engine)?))
}}

{protocol_key_helper}

pub fn descriptor() -> ServiceDescriptor {{
    ServiceDescriptor::new({service_name})
{manifest_commands}{manifest_events}{manifest_metrics}{manifest_tracing}        .transport({transport})
}}
"#,
                service_name = rust_string(&self.names.package_name),
                transport = rust_string(transport),
            );
        }

        format!(
            r#"use std::sync::Arc;

use distributed::{{{manifest_imports}}};

use crate::handlers;

pub type ServiceRepo = InMemoryRepository;

pub fn in_memory() -> Arc<Service> {{
    build(InMemoryRepository::new())
}}

pub fn build(repo: ServiceRepo) -> Arc<Service> {{
    let routes = distributed::routes!(
        Routes::new().with_dependencies(repo),
{registrations}    );
    Arc::new(Service::new().named({service_name}){http_command_opt_in}.routes(routes))
}}

pub fn descriptor() -> ServiceDescriptor {{
    ServiceDescriptor::new({service_name})
{manifest_commands}{manifest_events}{manifest_metrics}{manifest_tracing}        .transport({transport})
}}
"#,
            service_name = rust_string(&self.names.package_name),
            transport = rust_string(transport),
            http_command_opt_in = match self.transport {
                ServiceTransport::Http => ".with_http_command_routes()",
                ServiceTransport::Knative => "",
            },
        )
    }

    pub(super) fn models_mod_rs(&self) -> String {
        let modules = self
            .models
            .iter()
            .map(|model| {
                format!(
                    "pub mod {module_ident};\npub use {module_ident}::{type_ident};\n",
                    module_ident = model.module_ident,
                    type_ident = model.type_ident,
                )
            })
            .collect::<Vec<_>>()
            .join("");

        let fallback_aggregate =
            if self.query_api && self.models.is_empty() && !self.commands.is_empty() {
                r#"
use distributed::{sourced, Entity, Snapshot};

#[derive(Default, Snapshot)]
pub struct CommandAggregate {
    pub entity: Entity,
    pub name: Option<String>,
    pub status: String,
}

#[sourced(entity)]
impl CommandAggregate {
    #[event("service.command_recorded")]
    pub fn record_command(&mut self, command: String, id: String, name: Option<String>) {
        self.entity.set_id(&id);
        self.name = name;
        self.status = command;
    }
}
"#
            } else {
                ""
            };

        let command_types = if self.query_api {
            r#"#[derive(Clone, Debug, Deserialize, distributed::CommandInput)]
pub struct CommandInput {
    pub id: String,
    pub name: Option<String>,
}

#[derive(Clone, Debug, Serialize, distributed::CommandOutput)]
pub struct CommandOutput {
    pub command: String,
    pub id: String,
    pub model: String,
    pub name: Option<String>,
}
"#
        } else {
            r#"#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandInput {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
}
"#
        };

        format!(
            r#"{modules}
use serde::{{Deserialize, Serialize}};

{fallback_aggregate}
{command_types}
"#
        )
    }

    pub(super) fn model_rs(&self, model: &ModelScaffold) -> String {
        format!(
            r#"use distributed::{{sourced, Entity, Snapshot}};

#[derive(Default, Snapshot)]
pub struct {model_struct} {{
    pub entity: Entity,
    pub name: Option<String>,
    pub status: String,
}}

#[sourced(entity)]
impl {model_struct} {{
    #[event({command_recorded_event})]
    pub fn record_command(&mut self, command: String, id: String, name: Option<String>) {{
        self.entity.set_id(&id);
        if let Some(name) = name {{
            self.name = Some(name);
        }}
        self.status = command;
    }}
}}
"#,
            model_struct = model.type_ident,
            command_recorded_event =
                rust_string(&format!("{}.command_recorded", model.message_prefix)),
        )
    }

    pub(super) fn handlers_mod_rs(&self) -> String {
        self.commands
            .iter()
            .chain(self.events.iter())
            .map(|handler| format!("pub mod {};\n", handler.module_ident))
            .collect()
    }

    pub(super) fn command_handler_rs(&self, handler: &MessageHandler) -> String {
        if self.query_api {
            let (model_type, model_name) = self
                .command_model(handler)
                .map(|model| (model.type_ident.as_str(), model.name.as_str()))
                .unwrap_or(("CommandAggregate", "CommandAggregate"));
            return format!(
                r#"use distributed::command::{{typed_command, Succeeded, PreparedCommand, TypedCommand}};
use distributed::microsvc::{{CausalCommandContext, HandlerError}};

use crate::models::{{CommandInput, CommandOutput, {model_type}}};

pub const COMMAND: &str = {message_name};
pub const MODEL: &str = {model_name};

pub fn command() -> TypedCommand<CommandInput, Succeeded<CommandOutput>> {{
    typed_command::<CommandInput, Succeeded<CommandOutput>>(COMMAND).roles(["user"])
}}

pub async fn handle(
    ctx: &CausalCommandContext<'_, {model_type}>,
    input: CommandInput,
) -> Result<PreparedCommand<Succeeded<CommandOutput>>, HandlerError> {{
    let mut aggregate = match ctx.load(&input.id).await? {{
        Some(aggregate) => aggregate,
        None => ctx.create(),
    }};
    aggregate.record_command(COMMAND.to_string(), input.id.clone(), input.name.clone())?;
    ctx.commit(aggregate)?.succeeded(CommandOutput {{
        command: COMMAND.to_string(),
        id: input.id,
        model: MODEL.to_string(),
        name: input.name,
    }})
}}
"#,
                message_name = rust_string(&handler.message_name),
                model_name = rust_string(model_name),
            );
        }

        if let Some(model) = self.command_model(handler) {
            format!(
                r#"use distributed::{{
    microsvc::{{Context, HandlerError}}, Aggregate, CommitBatch, StreamIdentity, StreamWrite,
    TransactionalCommit,
}};
use serde_json::{{json, Value}};

use crate::models::{{CommandInput, {model_type}}};
use crate::service::ServiceRepo;

pub const COMMAND: &str = {message_name};
pub const MODEL: &str = {model_name};

pub fn guard(ctx: &Context<ServiceRepo>) -> bool {{
    ctx.has_fields(&["id"])
}}

pub async fn handle(ctx: &Context<'_, ServiceRepo>) -> Result<Value, HandlerError> {{
    let input = ctx.input::<CommandInput>()?;
    let mut aggregate = {model_type}::default();
    aggregate.record_command(COMMAND.to_string(), input.id.clone(), input.name.clone())?;
    let identity = StreamIdentity::new({model_type}::aggregate_type(), aggregate.entity().id())?;
    let stream = StreamWrite::new(identity, aggregate.entity_mut());
    ctx.repo().commit_batch(CommitBatch::new(vec![stream])).await?;
    Ok(json!({{ "command": COMMAND, "id": input.id, "model": MODEL, "name": input.name }}))
}}
"#,
                model_type = model.type_ident,
                message_name = rust_string(&handler.message_name),
                model_name = rust_string(&model.name),
            )
        } else {
            format!(
                r#"use distributed::microsvc::{{Context, HandlerError}};
use serde_json::{{json, Value}};

use crate::service::ServiceRepo;

pub const COMMAND: &str = {message_name};

pub fn guard(_ctx: &Context<ServiceRepo>) -> bool {{
    true
}}

pub async fn handle(ctx: &Context<'_, ServiceRepo>) -> Result<Value, HandlerError> {{
    let input = ctx.input::<Value>()?;
    Ok(json!({{ "command": COMMAND, "input": input }}))
}}
"#,
                message_name = rust_string(&handler.message_name),
            )
        }
    }

    pub(super) fn event_handler_rs(&self, handler: &MessageHandler) -> String {
        format!(
            r#"use distributed::microsvc::{{Context, HandlerError}};
use serde_json::{{json, Value}};

use crate::service::ServiceRepo;

pub const EVENT: &str = {message_name};

pub fn guard(_ctx: &Context<ServiceRepo>) -> bool {{
    true
}}

pub async fn handle(ctx: &Context<'_, ServiceRepo>) -> Result<Value, HandlerError> {{
    let input = ctx.input::<Value>()?;
    Ok(json!({{ "event": EVENT, "input": input }}))
}}
"#,
            message_name = rust_string(&handler.message_name),
        )
    }

    pub(super) fn read_models_mod_rs(&self) -> String {
        let views = self
            .read_models
            .iter()
            .map(|model| {
                format!(
                    r#"#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ReadModel)]
#[table({table_name})]
pub struct {view_struct} {{
    #[id("id")]
    pub id: String,
    pub name: String,
    pub status: String,
}}
"#,
                    table_name = rust_string(&model.table_name),
                    view_struct = model.view_ident,
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        format!(
            r#"use distributed::ReadModel;
use serde::{{Deserialize, Serialize}};

{views}
"#
        )
    }

    pub(super) fn command_model(&self, handler: &MessageHandler) -> Option<&ModelScaffold> {
        if self.models.is_empty() {
            return None;
        }
        let message_model = message_owner(&handler.message_name);
        self.models
            .iter()
            .find(|model| model.name == message_model)
            .or_else(|| self.models.first())
    }

    pub(super) fn query_mod_rs(&self) -> String {
        let mods = self
            .read_models
            .iter()
            .map(|m| format!("pub mod {};\n", m.module_ident))
            .collect::<String>();
        let tighten_hint = if self.read_models.is_empty() {
            String::new()
        } else {
            let names = self
                .read_models
                .iter()
                .map(|m| m.module_ident.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "// Tighten grants by replacing grant_all with:\n//   distributed::graphql_models!(builder, {names})\n// after filling permissions() in each model module.\n"
            )
        };
        let protocol_key_parameter = if self.commands.is_empty() {
            ""
        } else {
            ",\n    protocol_token_key: [u8; 32]"
        };
        let protocol_key_builder = if self.commands.is_empty() {
            ""
        } else {
            "\n        .protocol_token_key(protocol_token_key)"
        };
        format!(
            r#"//! GraphQL query exposure (deny-by-default permissions).
//!
//! One module per exposed read model. Command mutations are derived from the
//! executable service's typed causal inventory.

{mods}pub mod roles;

use distributed::graphql::{{GraphqlBuildError, GraphqlEngine, GraphqlPoolSource}};
use distributed::microsvc::Service;

/// Build the GraphQL engine for this service.
///
/// `DATABASE_URL` is used by `service::build_with_graphql`; defaults to an
/// in-memory SQLite database when unset (dev only).
pub fn build_engine(
    source: impl Into<GraphqlPoolSource>,
    service: &Service{protocol_key_parameter},
) -> Result<GraphqlEngine, GraphqlBuildError> {{
{tighten_hint}    // GraphiQL policy lives in `distributed::graphql::graphiql_enabled_from_env`
    // (GRAPHIQL override; RUST_ENV/ENV/APP_ENV production → off; else on).
    let graphiql = distributed::graphql::graphiql_enabled_from_env();
    // Public GraphQL identity (D6/D7): always OidcBearer + require_auth=true.
    // Set OIDC_ISSUER + OIDC_AUDIENCE (or OIDC_CLIENT_ID). Unset → placeholder
    // issuer (still OidcBearer; ambient headers never trusted). For local
    // GraphiQL ambient headers only, pass IdentityMode::DevHeaders explicitly.
    let identity = distributed::graphql::public_oidc_identity_from_env();
    GraphqlEngine::from_schema_catalog(&crate::read_model_catalog(), source)?
        .service(service){protocol_key_builder}
        .roles(roles::ALL)
        .grant_all(roles::USER)
        .graphiql(graphiql)
        .identity(identity)
        .build()
}}
"#
        )
    }

    pub(super) fn query_roles_rs(&self) -> String {
        r#"//! Role vocabulary for GraphQL permissions.

pub const USER: &str = "user";
pub const ANONYMOUS: &str = "anonymous";

/// Roles declared on the engine builder.
pub const ALL: &[&str] = &[USER, ANONYMOUS];
"#
        .to_string()
    }

    pub(super) fn query_model_rs(&self, model: &ModelScaffold) -> String {
        format!(
            r#"//! Permissions for `{view}`.

use distributed::graphql::{{read, ModelPermissions}};

use crate::read_models::{view};

pub type Model = {view};

pub fn permissions() -> ModelPermissions<{view}> {{
    ModelPermissions::new()
        // Deny-by-default until roles are granted. grant_all(USER) in mod.rs
        // covers the scaffold default; tighten .columns(...) / .rows(...) for prod.
        .grant(super::roles::USER, read().all_columns().aggregations())
}}
"#,
            view = model.view_ident,
        )
    }
}
