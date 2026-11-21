//! Scaffold generation: build a normalized [`Scaffold`] from a spec, then render
//! the project files. Pure — no filesystem, network, or CLI. The template bodies
//! live in submodules:
//!
//! - [`names`] — name/message normalization + validation (the portable rules);
//! - [`service_crate`] — the Rust service-crate templates;
//! - [`gitops`] — the `.gitops/{local,deploy,promote}` Helm/Knative charts;
//! - [`github`] — GitHub repo parsing + the release/preview/promote workflows.

mod github;
mod gitops;
mod names;
mod service_crate;

pub(crate) use github::parse_github_repo;

use std::collections::BTreeSet;

use names::{
    default_command_name, message_handlers_with_modules, model_scaffolds, MessageHandler,
    ModelScaffold, ScaffoldNames,
};

use crate::{
    BusTarget, GeneratedFile, GeneratedProject, GithubRepo, GitopsPromoteTarget, MetricsTarget,
    PostCreateAction, ScaffoldError, ServiceScaffoldSpec, ServiceTransport, StoreTarget,
};

/// Generate a Distributed service project from a spec. The public entry point.
pub fn generate_service_scaffold(
    spec: ServiceScaffoldSpec,
) -> Result<GeneratedProject, ScaffoldError> {
    Ok(Scaffold::from_spec(spec)?.generate())
}

/// Normalize a free-form service name to its kebab-case package/crate directory
/// name — the same normalization [`generate_service_scaffold`] applies. Callers
/// need this to compute a default output directory before generating.
pub fn package_name(name: &str) -> Result<String, ScaffoldError> {
    Ok(ScaffoldNames::new(name)?.package_name)
}

/// The normalized scaffold: spec values resolved into derived names, models, and
/// handlers. Template methods are implemented in the `service_crate` submodule.
pub(crate) struct Scaffold {
    pub(crate) names: ScaffoldNames,
    pub(crate) distributed_dependency_path: String,
    pub(crate) transport: ServiceTransport,
    pub(crate) store: StoreTarget,
    pub(crate) bus: Option<BusTarget>,
    pub(crate) metrics: Option<MetricsTarget>,
    pub(crate) include_read_models: bool,
    pub(crate) query_api: bool,
    pub(crate) tracing: bool,
    pub(crate) gitops: bool,
    pub(crate) gitops_promote: Option<GitopsPromoteTarget>,
    pub(crate) github: Option<GithubRepo>,
    pub(crate) github_preview: Option<GithubRepo>,
    pub(crate) github_promote: Option<GithubRepo>,
    pub(crate) models: Vec<ModelScaffold>,
    pub(crate) read_models: Vec<ModelScaffold>,
    pub(crate) commands: Vec<MessageHandler>,
    pub(crate) events: Vec<MessageHandler>,
}

impl Scaffold {
    fn from_spec(spec: ServiceScaffoldSpec) -> Result<Self, ScaffoldError> {
        let names = ScaffoldNames::new(&spec.name)?;
        let models = model_scaffolds(&spec.models)?;
        // GraphQL execution is SQL-backed. Keep the public pure generator aligned
        // with the CLI, which promotes its in-memory default to SQLite.
        let store = if spec.query_api && spec.store == StoreTarget::InMemory {
            StoreTarget::Sqlite
        } else {
            spec.store
        };
        let read_models = if spec.read_models {
            if models.is_empty() {
                vec![ModelScaffold::new(&names.package_name)?]
            } else {
                models.clone()
            }
        } else {
            Vec::new()
        };
        let mut module_idents = BTreeSet::new();
        let commands = message_handlers_with_modules(
            if spec.commands.is_empty() {
                vec![default_command_name(&names, &models)]
            } else {
                spec.commands.clone()
            },
            "command",
            &mut module_idents,
        )?;
        let events =
            message_handlers_with_modules(spec.events.clone(), "event", &mut module_idents)?;
        Ok(Self {
            names,
            distributed_dependency_path: spec.distributed_dependency_path,
            transport: spec.transport,
            store,
            bus: spec.bus,
            metrics: spec.metrics,
            include_read_models: spec.read_models,
            query_api: spec.query_api,
            tracing: spec.tracing,
            gitops: spec.gitops,
            gitops_promote: spec.gitops_promote,
            github: spec.github,
            github_preview: spec.github_preview,
            github_promote: spec.github_promote,
            models,
            read_models,
            commands,
            events,
        })
    }

    fn generate(self) -> GeneratedProject {
        let mut files = Vec::new();
        let mut post_create_actions = Vec::new();

        files.push(file("Cargo.toml", self.cargo_toml()));
        files.push(file("src/lib.rs", self.lib_rs()));
        files.push(file("src/main.rs", self.main_rs()));
        files.push(file("src/manifest.rs", self.manifest_rs()));
        files.push(file("src/service.rs", self.service_rs()));
        if !self.models.is_empty() || !self.commands.is_empty() {
            files.push(file("src/models/mod.rs", self.models_mod_rs()));
            for model in &self.models {
                files.push(file(
                    &format!("src/models/{}.rs", model.module_ident),
                    self.model_rs(model),
                ));
            }
        }
        files.push(file("src/handlers/mod.rs", self.handlers_mod_rs()));
        for command in &self.commands {
            files.push(file(
                &format!("src/handlers/{}.rs", command.module_ident),
                self.command_handler_rs(command),
            ));
        }
        for event in &self.events {
            files.push(file(
                &format!("src/handlers/{}.rs", event.module_ident),
                self.event_handler_rs(event),
            ));
        }
        if self.query_api {
            files.push(file("src/query/mod.rs", self.query_mod_rs()));
            files.push(file("src/query/roles.rs", self.query_roles_rs()));
            for model in &self.read_models {
                files.push(file(
                    &format!("src/query/{}.rs", model.module_ident),
                    self.query_model_rs(model),
                ));
            }
        }
        if self.include_read_models {
            files.push(file("src/read_models/mod.rs", self.read_models_mod_rs()));
        }

        // GitOps charts (.gitops/{local,deploy} + optional promote) and GitHub
        // workflow files (+ promotion charts) — staged in the same project.
        files.extend(self.gitops_files());
        files.extend(self.github_files());

        if let Some(repo) = &self.github {
            post_create_actions
                .push(PostCreateAction::EnsureGithubRepository { repo: repo.clone() });
        }

        GeneratedProject {
            files,
            warnings: Vec::new(),
            post_create_actions,
        }
    }
}

fn file(path: &str, contents: String) -> GeneratedFile {
    GeneratedFile {
        path: path.to_string(),
        contents,
        mode: None,
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        generate_service_scaffold, GeneratedProject, GithubRepo, MetricsTarget, PostCreateAction,
        ServiceScaffoldSpec, ServiceTransport, StoreTarget,
    };

    fn spec(name: &str) -> ServiceScaffoldSpec {
        ServiceScaffoldSpec {
            name: name.to_string(),
            transport: ServiceTransport::Http,
            store: StoreTarget::Postgres,
            bus: None,
            metrics: None,
            models: Vec::new(),
            read_models: false,
            query_api: false,
            tracing: false,
            commands: Vec::new(),
            events: Vec::new(),
            distributed_dependency_path: "../distributed".to_string(),
            gitops: false,
            gitops_promote: None,
            github: None,
            github_preview: None,
            github_promote: None,
        }
    }

    fn paths(project: &GeneratedProject) -> Vec<&str> {
        project.files.iter().map(|f| f.path.as_str()).collect()
    }

    fn contents<'a>(project: &'a GeneratedProject, path: &str) -> &'a str {
        project
            .files
            .iter()
            .find(|f| f.path == path)
            .map(|f| f.contents.as_str())
            .unwrap_or_else(|| panic!("missing file {path}"))
    }

    #[test]
    fn generates_the_core_service_crate() {
        let project = generate_service_scaffold(spec("orders")).unwrap();
        let paths = paths(&project);
        for expected in [
            "Cargo.toml",
            "src/lib.rs",
            "src/main.rs",
            "src/manifest.rs",
            "src/service.rs",
            "src/handlers/mod.rs",
        ] {
            assert!(paths.contains(&expected), "missing {expected} in {paths:?}");
        }
        assert!(paths
            .iter()
            .any(|p| p.starts_with("src/handlers/") && *p != "src/handlers/mod.rs"));
    }

    #[test]
    fn service_uses_the_route_bundle_api() {
        let project = generate_service_scaffold(spec("orders")).unwrap();
        let service = contents(&project, "src/service.rs");
        assert!(
            service.contains(".typed_command(handlers::")
                && service.contains(
                    "AggregateRepository::<_, crate::models::CommandAggregate>::new(repo.clone())"
                ),
            "service.rs should register typed commands on a route bundle:\n{service}"
        );
        assert!(
            !service.contains("command handlers::"),
            "commands are not registered through routes!:\n{service}"
        );
        assert!(
            service.contains(".with_http_command_routes()"),
            "HTTP scaffold must opt in to POST /{{command}} routes:\n{service}"
        );
        // `Service` is no longer generic and `register_handlers!` no longer exists.
        assert!(!service.contains("Service<"));
        assert!(!service.contains("register_handlers!"));
    }

    #[test]
    fn knative_commands_declare_the_transport_role_and_events_use_routes() {
        let mut s = spec("orders");
        s.transport = ServiceTransport::Knative;
        s.commands = vec!["orders.create".to_string()];
        s.events = vec!["orders.shipped".to_string()];
        let project = generate_service_scaffold(s).unwrap();
        let service = contents(&project, "src/service.rs");
        assert!(
            service.contains("distributed::routes!(")
                && service.contains("Routes::new().with_dependencies(repo.clone())")
                && service.contains("event handlers::"),
            "event handlers register through routes!:\n{service}"
        );
        assert!(
            !service.contains(".with_http_command_routes()"),
            "a Knative service takes its commands from triggers:\n{service}"
        );
        let command = project
            .files
            .iter()
            .find(|file| {
                file.contents
                    .contains("pub const COMMAND: &str = \"orders.create\";")
            })
            .expect("the command handler is generated");
        assert!(
            command
                .contents
                .contains(r#".roles(["user", TRANSPORT_ROLE])"#),
            "a trigger-delivered command must declare the transport role:\n{}",
            command.contents
        );
    }

    #[test]
    fn cargo_features_track_transport_and_store() {
        let mut s = spec("orders");
        s.store = StoreTarget::Sqlite;
        let project = generate_service_scaffold(s).unwrap();
        let cargo = contents(&project, "Cargo.toml");
        assert!(cargo.contains("\"http\""));
        assert!(cargo.contains("\"sqlite\""));
        assert!(cargo.contains("[package.metadata.distributed.application]"));
        assert!(cargo.contains("entrypoint = \"application_manifest\""));
        assert!(cargo.contains("[package.metadata.distributed.runtime]"));
        assert!(cargo.contains("binary = \"orders\""));
    }

    #[test]
    fn read_models_and_models_emit_modules() {
        let mut s = spec("orders");
        s.models = vec!["order".to_string()];
        s.read_models = true;
        let project = generate_service_scaffold(s).unwrap();
        let paths = paths(&project);
        assert!(paths.contains(&"src/models/mod.rs"));
        assert!(paths.contains(&"src/models/order.rs"));
        assert!(paths.contains(&"src/read_models/mod.rs"));
    }

    #[test]
    fn invalid_rust_identifier_names_are_rejected() {
        // A service whose normalized crate identifier would start with a digit
        // (or be a keyword) cannot compile — fail at generation, not `cargo build`.
        assert!(generate_service_scaffold(spec("3d")).is_err());

        let mut keyword = spec("orders");
        keyword.models = vec!["enum".to_string()];
        assert!(generate_service_scaffold(keyword).is_err());

        let mut digit_model = spec("orders");
        digit_model.models = vec!["3d".to_string()];
        assert!(generate_service_scaffold(digit_model).is_err());
    }

    #[test]
    fn deploy_templates_consume_helm_image_values() {
        // The deploy chart's values.yaml exposes image.repository/tag, so the
        // templates must reference them — hardcoding `:latest` makes the knob dead
        // and breaks release automation that patches values.yaml.
        let mut s = spec("orders");
        s.gitops = true;
        let project = generate_service_scaffold(s).unwrap();
        let deployment = contents(&project, ".gitops/deploy/templates/deployment.yaml");
        assert!(
            deployment.contains("{{ .Values.image.repository }}:{{ .Values.image.tag }}"),
            "deployment.yaml should template the image from values:\n{deployment}"
        );
        assert!(!deployment.contains(":latest"));
    }

    #[test]
    fn knative_triggers_with_colliding_names_are_deduplicated() {
        // Two command names that normalize to the same Trigger `metadata.name`
        // must still produce two distinctly-named Trigger resources, or
        // `kubectl apply` fails on the duplicate.
        let mut s = spec("orders");
        s.transport = ServiceTransport::Knative;
        s.gitops = true;
        s.commands = vec!["orders.created".to_string(), "orders.Created".to_string()];
        let project = generate_service_scaffold(s).unwrap();
        let triggers = contents(&project, ".gitops/deploy/templates/knative-triggers.yaml");
        assert!(triggers.contains("name: orders-created-command"));
        assert!(
            triggers.contains("name: orders-created-command-2"),
            "the colliding trigger should be suffixed:\n{triggers}"
        );
    }

    #[test]
    fn github_generates_workflows_and_a_post_create_action() {
        let mut s = spec("orders");
        s.github = Some(GithubRepo::parse("hops-ops/orders").unwrap());
        s.github_preview = Some(GithubRepo::parse("hops-ops/preview").unwrap());
        s.github_promote = Some(GithubRepo::parse("hops-ops/prod").unwrap());
        let project = generate_service_scaffold(s).unwrap();
        let paths = paths(&project);
        for expected in [
            ".github/workflows/version.yaml",
            ".github/workflows/release.yaml",
            ".github/workflows/preview.yaml",
            ".github/workflows/promote.yaml",
            ".gitops/preview/helm/Chart.yaml",
            ".gitops/promote/helm/templates/application.yaml",
            // GitHub presence implies the deploy chart the promotions target.
            ".gitops/deploy/Chart.yaml",
        ] {
            assert!(paths.contains(&expected), "missing {expected} in {paths:?}");
        }
        // The image repo is derived from the GitHub repo.
        let preview = contents(&project, ".github/workflows/preview.yaml");
        assert!(preview.contains("ghcr.io/hops-ops/orders"));
        assert_eq!(
            project.post_create_actions,
            vec![PostCreateAction::EnsureGithubRepository {
                repo: GithubRepo {
                    owner: "hops-ops".to_string(),
                    repo: "orders".to_string(),
                },
            }]
        );
        assert!(project.warnings.is_empty());
    }

    #[test]
    fn github_preview_is_independent_of_the_service_repo() {
        // Only --github-preview: a preview workflow + chart, the deploy chart it
        // targets, but NO version/release workflows and NO repo-create action.
        let mut s = spec("orders");
        s.github_preview = Some(GithubRepo::parse("hops-ops/preview").unwrap());
        let project = generate_service_scaffold(s).unwrap();
        let paths = paths(&project);
        assert!(paths.contains(&".github/workflows/preview.yaml"));
        assert!(paths.contains(&".gitops/preview/helm/Chart.yaml"));
        assert!(paths.contains(&".gitops/deploy/Chart.yaml"));
        assert!(!paths.contains(&".github/workflows/version.yaml"));
        assert!(!paths.contains(&".github/workflows/release.yaml"));
        assert!(project.post_create_actions.is_empty());
        // No service repo → the default image repository.
        let preview = contents(&project, ".github/workflows/preview.yaml");
        assert!(preview.contains("ghcr.io/hops-ops/orders"));
    }

    #[test]
    fn gitops_http_deploy_chart() {
        let mut s = spec("orders");
        s.gitops = true;
        let project = generate_service_scaffold(s).unwrap();
        let paths = paths(&project);
        assert!(paths.contains(&".gitops/local/Chart.yaml"));
        assert!(paths.contains(&".gitops/local/templates/deployment.yaml"));
        assert!(paths.contains(&".gitops/deploy/Chart.yaml"));
        assert!(paths.contains(&".gitops/deploy/templates/deployment.yaml"));
        assert!(paths.contains(&".gitops/deploy/templates/service.yaml"));
        assert!(!paths.contains(&".gitops/deploy/templates/servicemonitor.yaml"));
        assert!(!paths.contains(&".gitops/deploy/templates/prometheusrule.yaml"));
        assert!(!paths.iter().any(|p| p.contains("knative")));

        let local_values = contents(&project, ".gitops/local/values.yaml");
        assert!(local_values.starts_with("local: true\npreview: false\n"));
        assert!(local_values.contains("tag: \"latest\""));
        let deploy_values = contents(&project, ".gitops/deploy/values.yaml");
        assert!(deploy_values.starts_with("local: false\npreview: false\n"));
        assert!(deploy_values.contains("tag: \"\""));
        assert!(!local_values.contains("{{ if .Values.local }}"));
        assert!(!deploy_values.contains("{{ if .Values.local }}"));
        let deploy = contents(&project, ".gitops/deploy/templates/deployment.yaml");
        assert!(deploy.contains("image.tag must be an immutable build tag"));
        assert!(!paths.iter().any(|p| p.starts_with(".gitops/promote/")));
    }

    #[test]
    fn gitops_http_metrics_emits_prometheus_operator_resources() {
        let mut s = spec("orders");
        s.gitops = true;
        s.metrics = Some(MetricsTarget::Prometheus);
        let project = generate_service_scaffold(s).unwrap();
        let paths = paths(&project);
        assert!(paths.contains(&".gitops/deploy/templates/servicemonitor.yaml"));
        assert!(paths.contains(&".gitops/deploy/templates/prometheusrule.yaml"));

        let cargo = contents(&project, "Cargo.toml");
        assert!(cargo.contains("\"metrics\""));

        let values = contents(&project, ".gitops/deploy/values.yaml");
        assert!(values.contains("metrics:"));
        assert!(values.contains("serviceMonitor:"));
        assert!(values.contains("serviceMonitor:\n  enabled: false"));
        assert!(values.contains("prometheusRule:"));
        assert!(values.contains("prometheusRule:\n  enabled: false"));

        let monitor = contents(&project, ".gitops/deploy/templates/servicemonitor.yaml");
        assert!(monitor.contains("apiVersion: monitoring.coreos.com/v1"));
        assert!(monitor.contains("kind: ServiceMonitor"));
        assert!(monitor.contains("path: {{ .Values.metrics.path }}"));
    }

    #[test]
    fn knative_metrics_does_not_emit_service_monitor() {
        let mut s = spec("orders");
        s.transport = ServiceTransport::Knative;
        s.gitops = true;
        s.metrics = Some(MetricsTarget::Prometheus);
        let project = generate_service_scaffold(s).unwrap();
        let paths = paths(&project);
        assert!(!paths.contains(&".gitops/deploy/templates/servicemonitor.yaml"));
        assert!(!paths.contains(&".gitops/deploy/templates/prometheusrule.yaml"));
        let values = contents(&project, ".gitops/deploy/values.yaml");
        assert!(!values.contains("metrics:"), "values.yaml: {values}");
        assert!(!values.contains("serviceMonitor:"), "values.yaml: {values}");
        assert!(!values.contains("prometheusRule:"), "values.yaml: {values}");
    }

    #[test]
    fn query_api_emits_query_modules_and_graphql_feature() {
        let mut s = spec("orders");
        s.query_api = true;
        s.read_models = true;
        s.store = StoreTarget::Sqlite;
        s.models = vec!["Order".into()];
        s.gitops = true;
        let project = generate_service_scaffold(s).unwrap();
        let paths = paths(&project);
        assert!(paths.contains(&"src/query/mod.rs"));
        assert!(paths.contains(&"src/query/roles.rs"));
        assert!(!paths.contains(&"src/query/commands.rs"));
        assert!(
            paths.iter().any(|p| p.starts_with("src/query/")
                && *p != "src/query/mod.rs"
                && *p != "src/query/roles.rs"),
            "expected per-model query module: {paths:?}"
        );
        let cargo = contents(&project, "Cargo.toml");
        assert!(
            cargo.contains("graphql"),
            "Cargo.toml must enable graphql feature: {cargo}"
        );
        let service = contents(&project, "src/service.rs");
        assert!(
            service.contains("build_with_graphql") && service.contains("try_with_graphql"),
            "service must wire GraphQL: {service}"
        );
        assert!(
            !service.contains(".with_http_command_routes()"),
            "GraphQL scaffold must not expose unauthenticated direct command POST routes: {service}"
        );
        assert!(
            service.contains("pub type ServiceRepo = SqliteRepository;"),
            "query-api service must use the SQL repository type: {service}"
        );
        assert!(
            service.contains("let repo = ServiceRepo::connect_and_migrate(&database_url).await?;"),
            "build_with_graphql must open the persistent repository: {service}"
        );
        assert!(
            service.contains("crate::query::build_engine(&repo, &service"),
            "build_with_graphql must bind the exact executable service and preserve the repository's GraphQL storage identity: {service}"
        );
        assert!(
            service.contains("AggregateRepository::<_, crate::models::Order>::new(repo.clone())")
                && service.contains(".typed_command(handlers::"),
            "build_with_graphql routes must register typed causal commands over the persistent repository: {service}"
        );
        assert!(
            !service.contains("Routes::new().with_dependencies(InMemoryRepository::new())"),
            "build_with_graphql must not route commands to an in-memory repository: {service}"
        );
        let command_handler = project
            .files
            .iter()
            .find(|file| {
                file.path.starts_with("src/handlers/")
                    && file.path != "src/handlers/mod.rs"
                    && file.contents.contains("CausalCommandContext")
            })
            .expect("query-api scaffold must emit a typed causal command handler");
        assert!(
            command_handler
                .contents
                .contains("ctx.commit(aggregate)?.succeeded(")
                && !command_handler.contents.contains("ctx.stage("),
            "generated typed handlers must use the public fluent commit API: {}",
            command_handler.contents
        );
        let main = contents(&project, "src/main.rs");
        assert!(
            main.contains("build_with_graphql"),
            "main must call build_with_graphql: {main}"
        );
        let query_mod = contents(&project, "src/query/mod.rs");
        assert!(
            query_mod.contains(
                "use distributed::graphql::{GraphqlBuildError, GraphqlEngine, GraphqlPoolSource};"
            )
                && query_mod.contains("source: impl Into<GraphqlPoolSource>")
                && query_mod.contains("service: &Service")
                && query_mod.contains(".service(service)")
                && query_mod.contains(".protocol_token_key(protocol_token_key)")
                && query_mod.contains(".graphiql(")
                && query_mod.contains("graphiql_enabled_from_env")
                && query_mod.contains(".identity(")
                && query_mod.contains("public_oidc_identity_from_env"),
            "query build_engine must accept a causal pool source and wire GraphiQL + OIDC identity defaults: {query_mod}"
        );
        // D6: generated public scaffold must wire library OidcBearer helper (not DevHeaders).
        assert!(
            query_mod.contains("public_oidc_identity_from_env()")
                && !query_mod.contains("IdentityConfig::dev_headers"),
            "public scaffold must call public_oidc_identity_from_env, not DevHeaders: {query_mod}"
        );

        // GitOps chart injects DATABASE_URL for query API (mirrors tracing OTEL env).
        let values = contents(&project, ".gitops/deploy/values.yaml");
        assert!(
            values.contains("queryApi:") && values.contains("databaseUrl:"),
            "values.yaml must declare queryApi.databaseUrl: {values}"
        );
        assert!(values.contains("enabled: true"));
        let deployment = contents(&project, ".gitops/deploy/templates/deployment.yaml");
        assert!(
            deployment.contains("DATABASE_URL"),
            "deployment must inject DATABASE_URL: {deployment}"
        );
        assert!(
            deployment.contains(".Values.queryApi.databaseUrl"),
            "deployment must bind DATABASE_URL from values: {deployment}"
        );
        assert!(deployment.contains("if and .Values.queryApi.enabled .Values.queryApi.databaseUrl"));
    }

    #[test]
    fn public_query_api_scaffold_promotes_in_memory_store_to_sqlite() {
        let mut s = spec("orders");
        s.query_api = true;
        s.store = StoreTarget::InMemory;
        let project = generate_service_scaffold(s).unwrap();
        let cargo = contents(&project, "Cargo.toml");
        let service = contents(&project, "src/service.rs");

        assert!(cargo.contains("\"sqlite\""), "Cargo.toml: {cargo}");
        assert!(
            service.contains("pub type ServiceRepo = SqliteRepository;"),
            "query-api scaffold must use its enabled SQL repository: {service}"
        );
        assert!(!service.contains("pub type ServiceRepo = InMemoryRepository;"));
    }

    #[test]
    fn tracing_scaffold_enables_otel_feature_and_gitops_env_values() {
        let mut s = spec("orders");
        s.tracing = true;
        s.gitops = true;

        let project = generate_service_scaffold(s).unwrap();
        let cargo = contents(&project, "Cargo.toml");
        assert!(cargo.contains("\"otel\""));
        assert!(cargo.contains("opentelemetry-otlp"));
        assert!(cargo.contains("\"grpc-tonic\""));
        assert!(cargo.contains("tracing-subscriber"));

        let main = contents(&project, "src/main.rs");
        assert!(main.contains("let tracer_provider = init_tracing()?;"));
        assert!(main.contains("opentelemetry_otlp::SpanExporter::builder().build()?"));
        assert!(main.contains("tracing_opentelemetry::layer().with_tracer(tracer)"));
        assert!(main.contains("tracer_provider.shutdown()"));

        let service = contents(&project, "src/service.rs");
        assert!(service.contains(".tracing(TracingDescriptor::otlp())"));

        let values = contents(&project, ".gitops/deploy/values.yaml");
        assert!(values.contains("enabled: true"));
        assert!(values.contains("otlpEndpoint: \"\""));
        assert!(!values.contains("otlpProtocol"));

        let deployment = contents(&project, ".gitops/deploy/templates/deployment.yaml");
        assert!(deployment.contains("OTEL_SERVICE_NAME"));
        assert!(deployment.contains("value: \"orders\""));
        assert!(!deployment.contains(".Chart.Name"));
        assert!(deployment.contains("OTEL_EXPORTER_OTLP_PROTOCOL\n              value: \"grpc\""));
        assert!(!deployment.contains(".Values.observability.tracing.otlpProtocol"));
        assert!(deployment.contains("OTEL_EXPORTER_OTLP_ENDPOINT"));
        assert!(deployment.contains(".Values.observability.tracing.otlpEndpoint"));
        assert!(deployment.contains(
            "value: 0.0.0.0:3000\n            {{ if .Values.observability.tracing.enabled }}"
        ));
        assert!(!deployment.contains("{{- if .Values.observability.tracing.enabled }}"));
    }

    #[test]
    fn knative_deploy_emits_brokers_and_triggers() {
        let mut s = spec("orders");
        s.transport = ServiceTransport::Knative;
        s.gitops = true;
        s.events = vec!["orders.shipped".to_string()];
        let project = generate_service_scaffold(s).unwrap();
        let paths = paths(&project);
        assert!(paths.contains(&".gitops/deploy/templates/knative-service.yaml"));
        assert!(paths.contains(&".gitops/deploy/templates/knative-brokers.yaml"));
        let triggers = contents(&project, ".gitops/deploy/templates/knative-triggers.yaml");
        assert!(triggers.contains("type: orders.shipped"));
    }

    #[test]
    fn gitops_promote_flux() {
        let mut s = spec("orders");
        s.gitops_promote = Some(crate::GitopsPromoteTarget::Flux);
        let project = generate_service_scaffold(s).unwrap();
        let paths = paths(&project);
        assert!(paths.contains(&".gitops/promote/Chart.yaml"));
        assert!(paths.contains(&".gitops/promote/templates/helmrelease.yaml"));
    }

    #[test]
    fn gitops_promote_forwards_only_deploy_values() {
        let mut s = spec("orders");
        s.gitops_promote = Some(crate::GitopsPromoteTarget::Argo);
        let project = generate_service_scaffold(s).unwrap();
        let values = contents(&project, ".gitops/promote/values.yaml");
        assert!(values.contains("deploy:\n  values: {}"));
        let application = contents(&project, ".gitops/promote/templates/application.yaml");
        assert!(application.contains("path: .gitops/deploy"));
        assert!(application.contains("with .Values.deploy.values"));
        assert!(application.contains("toYaml . | nindent 8"));
        assert!(!application.contains(".Values.source.repoURL | toYaml"));
        assert!(application.contains("environment.name is required"));
    }

    #[test]
    fn invalid_name_is_an_error() {
        assert!(generate_service_scaffold(spec("   ")).is_err());
    }

    #[test]
    fn github_repo_parse_rejects_bad_input() {
        assert!(GithubRepo::parse("no-slash").is_err());
        assert!(GithubRepo::parse("a/b/c").is_err());
        assert_eq!(GithubRepo::parse("o/r").unwrap().slug(), "o/r");
    }
}
