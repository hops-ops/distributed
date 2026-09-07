//! The `distributed` command surface: clap types plus the [`run_distributed`]
//! dispatcher. Generation lives in the crate's `generate`/`atlas` modules and
//! the `describe`/`schema` harness in `manifest_harness`; this module maps
//! flags onto those types and owns filesystem / process side effects.
//!
//! Host CLIs (for example `hops`) may mount [`ServiceArgs`] under a nested
//! service command and dispatch with [`run`], re-exporting rather than
//! reimplementing service-related commands.

use clap::{ArgGroup, Args, Subcommand, ValueEnum};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::ffi::OsString;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::client_compiler::{
    compile_client, ClientCompileInput, ClientDocument, ClientSurfaceSelector, GeneratedClientFile,
    GeneratedClientProject,
};
use crate::contracts::{
    contracts_accept, contracts_check, unknown_scope_diagnostic, ContractAcceptScope,
    ContractCatalog,
};
use crate::js_framework::watch_local_javascript;
use crate::lifecycle::{
    activate_lifecycle_project_generation, discover_lifecycle_project, run_lifecycle_build,
    run_lifecycle_dev, run_lifecycle_project_build, run_lifecycle_project_dev,
    DiscoveredLifecycleProject, LifecycleActivation, LifecycleBuildOptions, LifecycleBuildRequest,
    LifecycleCheckBaseline, LifecycleDevOptions, LifecycleProjectDevOptions,
};
use crate::manifest_harness::{
    resolve_target_manifest_path, run_manifest_harness, HarnessMode, HarnessOptions,
};
use crate::skills::{embedded_skills, generate_skills, SkillsInitSpec, AGENTS_MD_FILE};
use crate::wasm_pures::build_declared_wasm_pures;
use crate::{
    generate_service_scaffold, package_name, render_atlas_schema, AtlasDatabaseUrl,
    AtlasSchemaSpec, BusTarget, FileMode, GeneratedFile, GithubRepo, GitopsPromoteTarget,
    MetricsTarget, PostCreateAction, ServiceScaffoldSpec, ServiceTransport, StoreTarget,
};

const DISTRIBUTED_MANIFEST_SCHEMA_VERSION: u64 = 1;
const DISTRIBUTED_CLIENT_MANIFEST_VERSION: u64 = 2;

/// Top-level standalone CLI arguments for the `distributed` binary.
#[derive(Args, Debug)]
pub struct DistributedArgs {
    #[command(subcommand)]
    pub command: DistributedCommands,
}

#[derive(Subcommand, Debug)]
pub enum DistributedCommands {
    /// Build one coherent application generation
    Build(BuildArgs),
    /// Run one coherent local application supervisor
    Dev(DevArgs),
    /// Aggregate contract lifecycle check and accept
    Contracts(ContractsArgs),
    /// Scaffold a new Distributed microservice crate
    #[command(alias = "create")]
    Scaffold(ScaffoldArgs),
    /// Print a service's explicit ApplicationManifest as JSON
    Describe(DescribeArgs),
    /// Compile role/application-scoped GraphQL operations into client artifacts
    Client(ClientArgs),
    /// Compile the service's authorized client Surface manifest as JSON
    ClientManifest(ClientManifestArgs),
    /// Render schema artifacts (SQL or an Atlas Operator resource) from a read-model catalog
    Schema(SchemaArgs),
    /// Extract the embedded Distributed agent skills into a project
    Skills(SkillsArgs),
    /// Internal bounded readiness probe used by `distributed dev`.
    #[command(name = "__probe", hide = true)]
    Probe(ProbeArgs),
    /// Internal local Distributed JavaScript source watcher.
    #[command(name = "__js-watch", hide = true)]
    JavascriptWatch(JavascriptWatchArgs),
}

#[derive(Args, Debug)]
/// Arguments for the supervisor's hidden bounded TCP readiness probe.
pub struct ProbeArgs {
    /// TCP address to connect to.
    #[arg(long)]
    pub address: String,
}

#[derive(Args, Debug)]
/// Arguments for the supervisor's hidden local JavaScript builder.
pub struct JavascriptWatchArgs {
    #[arg(long)]
    pub package_root: PathBuf,
    #[arg(long)]
    pub project_root: PathBuf,
}

/// Library adapter for embedding service-related commands under another CLI.
#[derive(Args, Debug)]
pub struct ServiceArgs {
    #[command(subcommand)]
    pub command: ServiceCommands,
}

#[derive(Subcommand, Debug)]
pub enum ServiceCommands {
    /// Build one coherent application generation
    Build(BuildArgs),
    /// Run one coherent local application supervisor
    Dev(DevArgs),
    /// Scaffold a new Distributed microservice crate
    #[command(alias = "create")]
    Scaffold(ScaffoldArgs),
    /// Print a service's explicit ApplicationManifest as JSON
    Describe(DescribeArgs),
    /// Compile role/application-scoped GraphQL operations into client artifacts
    Client(ClientArgs),
    /// Compile the service's authorized client Surface manifest as JSON
    ClientManifest(ClientManifestArgs),
    /// Render schema artifacts (SQL or an Atlas Operator resource) from a read-model catalog
    Schema(SchemaArgs),
    /// Extract the embedded Distributed agent skills into a project
    Skills(SkillsArgs),
    /// Internal bounded readiness probe used by `hops service dev`.
    #[command(name = "__probe", hide = true)]
    Probe(ProbeArgs),
    /// Internal local Distributed JavaScript source watcher.
    #[command(name = "__js-watch", hide = true)]
    JavascriptWatch(JavascriptWatchArgs),
}

#[derive(Args, Debug)]
pub struct BuildArgs {
    /// Project to build; defaults to the current directory
    #[arg(value_name = "PROJECT")]
    pub project: Option<PathBuf>,
    /// Deprecated alias for the project directory
    #[arg(long, hide = true, conflicts_with = "project")]
    pub root: Option<PathBuf>,
    /// Advanced contract-catalog override, relative to the project
    #[arg(long, hide = true)]
    pub catalog: Option<PathBuf>,
    /// Advanced lifecycle-config override, relative to the project
    #[arg(long, hide = true)]
    pub config: Option<PathBuf>,
    /// Advanced generation-store override, relative to the project
    #[arg(long)]
    pub out: Option<PathBuf>,
    /// Rebuild in isolation and report drift without activating
    #[arg(long)]
    pub check: bool,
    /// Maximum wait for another lifecycle build process
    #[arg(long, default_value_t = 10_000)]
    pub lock_timeout_ms: u64,
    /// Output format
    #[arg(long, value_enum, default_value = "human")]
    pub output: LifecycleOutput,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum LifecycleOutput {
    Human,
    Json,
}

#[derive(Args, Debug)]
pub struct DevArgs {
    /// Project to run; defaults to the current directory
    #[arg(value_name = "PROJECT")]
    pub project: Option<PathBuf>,
    /// Deprecated alias for the project directory
    #[arg(long, hide = true, conflicts_with = "project")]
    pub root: Option<PathBuf>,
    /// Advanced contract-catalog override, relative to the project
    #[arg(long, hide = true)]
    pub catalog: Option<PathBuf>,
    /// Advanced lifecycle-config override, relative to the project
    #[arg(long, hide = true)]
    pub config: Option<PathBuf>,
    /// Advanced generation-store override, relative to the project
    #[arg(long)]
    pub out: Option<PathBuf>,
    /// Maximum wait for another lifecycle build process
    #[arg(long, default_value_t = 10_000)]
    pub lock_timeout_ms: u64,
}

#[derive(Args, Debug)]
pub struct ContractsArgs {
    #[command(subcommand)]
    pub command: ContractsCommands,
}

#[derive(Subcommand, Debug)]
pub enum ContractsCommands {
    /// Read-only aggregate contract check (never writes tracked files)
    Check(ContractsCheckArgs),
    /// Exact-scope accept with staging, atomic replace, and rollback
    Accept(ContractsAcceptArgs),
}

#[derive(Args, Debug)]
pub struct ContractsCheckArgs {
    /// Catalog root directory
    #[arg(long, default_value = ".")]
    pub root: PathBuf,
    /// Path to the contract catalog JSON (relative to root or absolute)
    #[arg(long, default_value = "contracts/catalog.json")]
    pub catalog: PathBuf,
    /// Output format
    #[arg(long, value_enum, default_value = "human")]
    pub output: ContractsOutput,
}

#[derive(Args, Debug)]
pub struct ContractsAcceptArgs {
    /// Catalog root directory
    #[arg(long, default_value = ".")]
    pub root: PathBuf,
    /// Exact accept scope (no broad wildcards)
    #[arg(long)]
    pub scope: String,
    /// Staged payload file: JSON object mapping portable relative paths to UTF-8 contents
    #[arg(long)]
    pub staged: PathBuf,
    /// Output format
    #[arg(long, value_enum, default_value = "human")]
    pub output: ContractsOutput,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum ContractsOutput {
    Human,
    Json,
}

#[derive(Args, Debug)]
pub struct SkillsArgs {
    #[command(subcommand)]
    pub command: SkillsCommands,
}

#[derive(Subcommand, Debug)]
pub enum SkillsCommands {
    /// Materialize the embedded agent skills into .distributed/skills/ and wire
    /// them for discovery by agent harnesses
    Init(SkillsInitArgs),
    /// Print the name and description of every embedded skill
    List,
}

#[derive(Args, Debug)]
pub struct SkillsInitArgs {
    /// Directory that will contain the skills/ folder. Harness wiring
    /// (.claude/, .agents/, AGENTS.md) lands in its parent directory.
    #[arg(long, default_value = ".distributed")]
    pub path: PathBuf,
    /// Harnesses to wire for native skill discovery (comma-delimited).
    /// `auto` detects from the project; `none` writes canonical files only.
    #[arg(long, value_enum, value_delimiter = ',', default_value = "auto")]
    pub agents: Vec<AgentHarness>,
    /// Overwrite skill files whose on-disk content differs from the embedded
    /// content, and replace non-link entries at harness locations with
    /// symlinks. Without it, such paths are skipped with a warning.
    #[arg(long)]
    pub force: bool,
}

/// Which agent harnesses to wire. `codex`/`grok`/`openai`/`gemini`/`pi` are
/// aliases for `agents` — they all discover `.agents/skills/` — and exist so
/// users can name their tool.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum AgentHarness {
    /// Wire every harness with evidence in the project root; both when fresh.
    Auto,
    /// Canonical `.distributed/skills/` files only; no harness wiring.
    None,
    /// Claude Code: per-skill links under `.claude/skills/`.
    Claude,
    Codex,
    Grok,
    Openai,
    Gemini,
    Pi,
    /// The shared `.agents/skills/` convention + AGENTS.md managed block.
    Agents,
}

#[derive(Args, Debug)]
pub struct ScaffoldArgs {
    /// Service/package name to scaffold
    pub name: String,
    /// Output directory. Defaults to ./<name>.
    #[arg(long)]
    pub path: Option<PathBuf>,
    /// Service framework to scaffold
    #[arg(long, value_enum, default_value = "distributed")]
    pub framework: Framework,
    /// Compatibility alias for scaffold kind, e.g. distributed-microsvc.
    #[arg(long)]
    pub kind: Option<String>,
    /// Runtime transport to scaffold
    #[arg(long, value_enum, default_value = "http")]
    pub transport: Transport,
    /// Compatibility shortcut for --transport http.
    #[arg(long)]
    pub http: bool,
    /// Compatibility shortcut for --transport knative.
    #[arg(long)]
    pub knative: bool,
    /// Model aggregate to scaffold. May be repeated.
    #[arg(long)]
    pub model: Vec<String>,
    /// Generate placeholder read-model modules and register them in read_model_catalog().
    #[arg(long)]
    pub read_models: bool,
    /// Generate src/query/ GraphQL skeleton, enable the graphql feature, and wire with_graphql.
    #[arg(long)]
    pub query_api: bool,
    /// Enable Distributed tracing spans and GitOps OTLP environment values.
    #[arg(long, visible_alias = "otel")]
    pub tracing: bool,
    /// Command handler to scaffold. May be repeated.
    #[arg(long)]
    pub command: Vec<String>,
    /// Event handler to scaffold. May be repeated.
    #[arg(long)]
    pub event: Vec<String>,
    /// Message bus backend to scaffold.
    #[arg(long, value_enum)]
    pub bus: Option<Bus>,
    /// Metrics integration to scaffold.
    #[arg(long, value_enum)]
    pub metrics: Option<Metrics>,
    /// Generate independent local and cloud workload charts under
    /// .gitops/local and .gitops/deploy.
    #[arg(long)]
    pub gitops: bool,
    /// Generate a GitOps promotion chart for Argo CD or Flux.
    #[arg(long, value_enum)]
    pub gitops_promote: Option<GitopsPromote>,
    /// GitHub repository to create and configure with release workflows.
    #[arg(long, value_name = "OWNER/REPO")]
    pub github: Option<String>,
    /// GitOps preview environment repository to promote pull-request previews into.
    #[arg(long, value_name = "OWNER/REPO")]
    pub github_preview: Option<String>,
    /// GitOps permanent environment repository to promote version tags into.
    #[arg(long, value_name = "OWNER/REPO")]
    pub github_promote: Option<String>,
    /// Read-model/schema storage target
    #[arg(
        long,
        alias = "storage",
        visible_alias = "storage",
        visible_alias = "read-model",
        value_enum,
        default_value = "postgres"
    )]
    pub store: Store,
    /// Path to the local Distributed crate.
    #[arg(long)]
    pub distributed_path: Option<PathBuf>,
    /// Overwrite generated files in an existing directory.
    #[arg(long)]
    pub force: bool,
}

#[derive(Args, Debug)]
pub struct DescribeArgs {
    /// Service project directory. Defaults to the current directory.
    #[arg(long, default_value = ".")]
    pub path: PathBuf,
    /// Cargo.toml for the target service. Overrides --path.
    #[arg(long)]
    pub manifest_path: Option<PathBuf>,
    /// Cargo package to inspect when the manifest belongs to a workspace.
    #[arg(long)]
    pub package: Option<String>,
    /// Comma-delimited feature list for the target service.
    #[arg(long, value_delimiter = ',')]
    pub features: Vec<String>,
    /// Disable default features on the target service dependency.
    #[arg(long)]
    pub no_default_features: bool,
    /// Application manifest function to call. Defaults to <crate>::application_manifest.
    #[arg(long)]
    pub entrypoint: Option<String>,
    /// Output format.
    #[arg(long, value_enum, default_value = "json")]
    pub format: ManifestFormat,
    /// Path to the local Distributed crate.
    #[arg(long)]
    pub distributed_path: Option<PathBuf>,
    /// Exact wasm-pack JavaScript launcher owned by the resolved framework package.
    #[arg(long, hide = true)]
    pub wasm_pack_launcher: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub struct ClientManifestArgs {
    /// Service project directory. Defaults to the current directory.
    #[arg(long, default_value = ".")]
    pub path: PathBuf,
    /// Cargo.toml for the target service. Overrides --path.
    #[arg(long)]
    pub manifest_path: Option<PathBuf>,
    /// Cargo package to inspect when the manifest belongs to a workspace.
    #[arg(long)]
    pub package: Option<String>,
    /// Comma-delimited feature list for the target service.
    #[arg(long, value_delimiter = ',')]
    pub features: Vec<String>,
    /// Disable default features on the target service dependency.
    #[arg(long)]
    pub no_default_features: bool,
    /// Surface export function. Defaults to
    /// `<crate>::distributed_client_surface`.
    #[arg(long)]
    pub entrypoint: Option<String>,
    /// Path to the local Distributed crate.
    #[arg(long)]
    pub distributed_path: Option<PathBuf>,
}

#[derive(Args, Debug)]
#[command(group(
    ArgGroup::new("client_surface")
        .required(true)
        .multiple(false)
        .args(["role", "surface"])
))]
pub struct ClientArgs {
    /// Role/application-selected Distributed client manifest v7.
    #[arg(long)]
    pub manifest: PathBuf,
    /// Verify that the manifest is selected for this concrete role.
    #[arg(long)]
    pub role: Option<String>,
    /// Verify that the manifest is selected for this named application surface.
    #[arg(long)]
    pub surface: Option<String>,
    /// Explicit eligible application roles (repeat for each role).
    #[arg(long, value_name = "ROLE", requires = "surface")]
    pub eligible_role: Vec<String>,
    /// Explicit schema application roles (repeat for each role).
    #[arg(long, value_name = "ROLE", requires = "surface")]
    pub schema_role: Vec<String>,
    /// GraphQL document glob. Repeat for multiple source roots.
    #[arg(long, required = true, value_name = "GLOB")]
    pub documents: Vec<String>,
    /// Generated artifact directory.
    #[arg(long, default_value = "src/generated/distributed")]
    pub out: PathBuf,
    /// Verify generated bytes and file set without writing anything.
    #[arg(long)]
    pub check: bool,
}

#[derive(Args, Debug)]
pub struct SchemaArgs {
    /// Service project directory. Defaults to the current directory.
    #[arg(long, default_value = ".")]
    pub path: PathBuf,
    /// Cargo.toml for the target service. Overrides --path.
    #[arg(long)]
    pub manifest_path: Option<PathBuf>,
    /// Cargo package to inspect when the manifest belongs to a workspace.
    #[arg(long)]
    pub package: Option<String>,
    /// Comma-delimited feature list for the target service.
    #[arg(long, value_delimiter = ',')]
    pub features: Vec<String>,
    /// Disable default features on the target service dependency.
    #[arg(long)]
    pub no_default_features: bool,
    /// Read-model catalog function to call. Defaults to <crate>::read_model_catalog.
    #[arg(long)]
    pub entrypoint: Option<String>,
    /// SQL dialect to render.
    #[arg(long, value_enum, default_value = "postgres")]
    pub dialect: SchemaDialect,
    /// Output artifact format.
    #[arg(long, value_enum, default_value = "sql")]
    pub format: SchemaFormat,
    /// AtlasSchema metadata.name (required for --format atlas).
    #[arg(long)]
    pub name: Option<String>,
    /// AtlasSchema metadata.namespace (--format atlas).
    #[arg(long)]
    pub namespace: Option<String>,
    /// Kubernetes Secret holding the database URL (--format atlas, GitOps-friendly).
    #[arg(long, value_name = "SECRET")]
    pub db_secret: Option<String>,
    /// Key within --db-secret that holds the database URL.
    #[arg(long, default_value = "url")]
    pub db_secret_key: String,
    /// Inline database URL (--format atlas; prefer --db-secret for GitOps).
    #[arg(long)]
    pub db_url: Option<String>,
    /// Atlas devURL — a scratch database used to plan changes (--format atlas).
    #[arg(long)]
    pub dev_url: Option<String>,
    /// Output file. Defaults to stdout.
    #[arg(long, alias = "output", visible_alias = "output")]
    pub out: Option<PathBuf>,
    /// Path to the local Distributed crate.
    #[arg(long)]
    pub distributed_path: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Framework {
    Distributed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Transport {
    Http,
    Knative,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum GitopsPromote {
    Argo,
    Flux,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Bus {
    Rabbitmq,
    Kafka,
    Psql,
    Nats,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Metrics {
    Prometheus,
}

impl From<Metrics> for MetricsTarget {
    fn from(value: Metrics) -> Self {
        match value {
            Metrics::Prometheus => MetricsTarget::Prometheus,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Store {
    Postgres,
    Sqlite,
    InMemory,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum ManifestFormat {
    Json,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum SchemaDialect {
    Postgres,
    Sqlite,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum SchemaFormat {
    /// Raw migration SQL.
    Sql,
    /// An Atlas Operator `AtlasSchema` resource wrapping the desired-state SQL.
    Atlas,
    /// Dialect-independent GraphQL SDL (`schema.graphql` artifact).
    /// When set, `--dialect` is silently ignored (SDL has no dialect).
    Graphql,
}

// Map the CLI's clap enums onto the generation spec enums. These exist so
// `--help` / value parsing stay in the command surface while generation stays in
// the `generate`/`atlas` modules.
impl From<Transport> for ServiceTransport {
    fn from(transport: Transport) -> Self {
        match transport {
            Transport::Http => ServiceTransport::Http,
            Transport::Knative => ServiceTransport::Knative,
        }
    }
}

impl From<Store> for StoreTarget {
    fn from(store: Store) -> Self {
        match store {
            Store::Postgres => StoreTarget::Postgres,
            Store::Sqlite => StoreTarget::Sqlite,
            Store::InMemory => StoreTarget::InMemory,
        }
    }
}

impl From<Bus> for BusTarget {
    fn from(bus: Bus) -> Self {
        match bus {
            Bus::Rabbitmq => BusTarget::Rabbitmq,
            Bus::Kafka => BusTarget::Kafka,
            Bus::Psql => BusTarget::Psql,
            Bus::Nats => BusTarget::Nats,
        }
    }
}

impl From<GitopsPromote> for GitopsPromoteTarget {
    fn from(promote: GitopsPromote) -> Self {
        match promote {
            GitopsPromote::Argo => GitopsPromoteTarget::Argo,
            GitopsPromote::Flux => GitopsPromoteTarget::Flux,
        }
    }
}

/// Dispatch the standalone `distributed` binary command tree.
pub fn run_distributed(args: &DistributedArgs) -> Result<(), Box<dyn Error>> {
    match &args.command {
        DistributedCommands::Build(build) => run_build(build, &[]),
        DistributedCommands::Dev(dev) => run_dev(dev, &[]),
        DistributedCommands::Contracts(contracts) => run_contracts(contracts),
        DistributedCommands::Scaffold(scaffold) => run_scaffold(scaffold),
        DistributedCommands::Describe(describe) => run_describe(describe),
        DistributedCommands::Client(client) => run_client(client),
        DistributedCommands::ClientManifest(client) => run_client_manifest(client),
        DistributedCommands::Schema(schema) => run_schema(schema),
        DistributedCommands::Skills(skills) => match &skills.command {
            SkillsCommands::Init(init) => run_skills_init(init),
            SkillsCommands::List => run_skills_list(),
        },
        DistributedCommands::Probe(probe) => run_probe(probe),
        DistributedCommands::JavascriptWatch(watch) => run_javascript_watch(watch),
    }
}

fn run_javascript_watch(args: &JavascriptWatchArgs) -> Result<(), Box<dyn Error>> {
    watch_local_javascript(&args.package_root, &args.project_root).map_err(Into::into)
}

fn run_probe(args: &ProbeArgs) -> Result<(), Box<dyn Error>> {
    use std::net::{TcpStream, ToSocketAddrs};

    let addresses = args.address.to_socket_addrs()?.collect::<Vec<_>>();
    if addresses.is_empty() {
        return Err(format!("probe address `{}` resolved no endpoints", args.address).into());
    }
    let timeout = Duration::from_millis(200);
    let mut last_error = None;
    for address in addresses {
        match TcpStream::connect_timeout(&address, timeout) {
            Ok(_) => return Ok(()),
            Err(error) => last_error = Some(error),
        }
    }
    Err(format!(
        "probe could not connect to `{}`: {}",
        args.address,
        last_error.expect("at least one resolved probe endpoint")
    )
    .into())
}

/// Dispatch a parsed service command. Host CLIs (for example `hops service`)
/// call this without mounting the aggregate contracts surface.
pub fn run(args: &ServiceArgs) -> Result<(), Box<dyn Error>> {
    match &args.command {
        ServiceCommands::Build(build) => run_build(build, &["service"]),
        ServiceCommands::Dev(dev) => run_dev(dev, &["service"]),
        ServiceCommands::Scaffold(scaffold) => run_scaffold(scaffold),
        ServiceCommands::Describe(describe) => run_describe(describe),
        ServiceCommands::Client(client) => run_client(client),
        ServiceCommands::ClientManifest(client) => run_client_manifest(client),
        ServiceCommands::Schema(schema) => run_schema(schema),
        ServiceCommands::Skills(skills) => match &skills.command {
            SkillsCommands::Init(init) => run_skills_init(init),
            SkillsCommands::List => run_skills_list(),
        },
        ServiceCommands::Probe(probe) => run_probe(probe),
        ServiceCommands::JavascriptWatch(watch) => run_javascript_watch(watch),
    }
}

#[derive(Debug)]
enum ResolvedLifecycleProject {
    Discovered(Box<DiscoveredLifecycleProject>),
    Files {
        project: PathBuf,
        workspace: PathBuf,
        catalog: PathBuf,
        config: PathBuf,
        out: PathBuf,
    },
}

fn resolve_lifecycle_project(
    project: Option<&Path>,
    legacy_root: Option<&Path>,
    catalog: Option<&Path>,
    config: Option<&Path>,
    out: Option<&Path>,
    command_prefix: &[&str],
) -> Result<ResolvedLifecycleProject, Box<dyn Error>> {
    if project.is_some() && legacy_root.is_some() {
        return Err("pass a project path or --root, not both".into());
    }
    let requested = project.or(legacy_root).unwrap_or_else(|| Path::new("."));
    let project = requested.canonicalize().map_err(|error| {
        format!(
            "failed to resolve lifecycle project `{}`: {error}",
            requested.display()
        )
    })?;
    if !project.is_dir() {
        return Err(format!(
            "lifecycle project `{}` is not a directory",
            project.display()
        )
        .into());
    }

    let uses_file_adapter = legacy_root.is_some() || catalog.is_some() || config.is_some();
    if !uses_file_adapter {
        load_project_environment(&project)?;
        let executable = std::env::current_exe()?;
        return discover_lifecycle_project(&project, executable, command_prefix, out)
            .map(Box::new)
            .map(ResolvedLifecycleProject::Discovered)
            .map_err(Into::into);
    }
    if catalog.is_some() != config.is_some() && legacy_root.is_none() {
        return Err("advanced lifecycle overrides require both --catalog and --config".into());
    }

    let resolve_input = |provided: Option<&Path>, default: &str, label: &str| {
        let requested = provided.unwrap_or_else(|| Path::new(default));
        let candidate = if requested.is_absolute() {
            requested.to_path_buf()
        } else {
            project.join(requested)
        };
        candidate.canonicalize().map_err(|error| {
            let guidance = if provided.is_none() {
                format!(
                    "; expected this project to contain `{default}` (run from the project directory or pass the project path)"
                )
            } else {
                String::new()
            };
            format!(
                "failed to resolve lifecycle {label} `{}`: {error}{guidance}",
                candidate.display()
            )
        })
    };
    let catalog = resolve_input(catalog, "distributed.contracts.json", "contract catalog")?;
    let config = resolve_input(config, "distributed.lifecycle.json", "config")?;
    let requested_out = out.unwrap_or_else(|| Path::new("dist/distributed"));
    let out = if requested_out.is_absolute() {
        requested_out.to_path_buf()
    } else {
        project.join(requested_out)
    };

    let mut failures = Vec::new();
    let mut workspace = None;
    for candidate in project.ancestors() {
        if !catalog.starts_with(candidate)
            || !config.starts_with(candidate)
            || !out.starts_with(candidate)
        {
            continue;
        }
        match ContractCatalog::from_path_at_root(&catalog, candidate) {
            Ok(_) => {
                workspace = Some(candidate.to_path_buf());
                break;
            }
            Err(error) => failures.push(format!("{}: {error}", candidate.display())),
        }
    }
    let workspace = workspace.ok_or_else(|| {
        let detail = failures
            .last()
            .map(|failure| format!(" Last attempted root: {failure}"))
            .unwrap_or_default();
        format!(
            "unable to discover the workspace for lifecycle project `{}`; no project ancestor resolves every catalog source.{detail}",
            project.display()
        )
    })?;

    Ok(ResolvedLifecycleProject::Files {
        project,
        workspace,
        catalog,
        config,
        out,
    })
}

fn load_project_environment(project: &Path) -> Result<(), Box<dyn Error>> {
    let project_name = project
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty());
    let candidates = project_name
        .map(|name| vec![project.join(format!("{name}.env")), project.join(".env")])
        .unwrap_or_else(|| vec![project.join(".env")]);
    for candidate in candidates {
        if candidate.is_file() {
            dotenvy::from_path(&candidate).map_err(|error| {
                format!(
                    "failed to load project environment `{}`: {error}",
                    candidate.display()
                )
            })?;
        }
    }
    Ok(())
}

fn run_build(args: &BuildArgs, command_prefix: &[&str]) -> Result<(), Box<dyn Error>> {
    let resolved = resolve_lifecycle_project(
        args.project.as_deref(),
        args.root.as_deref(),
        args.catalog.as_deref(),
        args.config.as_deref(),
        args.out.as_deref(),
        command_prefix,
    )?;
    let (report, detail) = match &resolved {
        ResolvedLifecycleProject::Discovered(project) => {
            if args.check {
                prepare_project_javascript(project, true)?;
            } else {
                build_project_runtime(project)?;
                prepare_project_javascript(project, false)?;
                if let Some(ui) = &project.ui {
                    prepare_ui_dependencies(project, ui)?;
                }
                prepare_project_wasm_pures(project).map_err(|error| {
                    contextualize_project_generation_error(
                        project,
                        ProjectGenerationPhase::ApplicationPreflight,
                        error,
                    )
                })?;
            }
            eprintln!(
                "distributed build: introspecting typed application {} (the first run may compile its harness)",
                project.application_package
            );
            let report = run_lifecycle_project_build(
                &project.plan,
                &LifecycleBuildRequest {
                    check: args.check,
                    check_baseline: LifecycleCheckBaseline::ActiveGeneration,
                    lock_timeout: Duration::from_millis(args.lock_timeout_ms),
                    nodes: None,
                    activation_inputs: None,
                    cancel: None,
                    activation: LifecycleActivation::Deferred,
                },
            )
            .map_err(|error| {
                contextualize_project_generation_error(
                    project,
                    ProjectGenerationPhase::LifecycleBuild,
                    error,
                )
            })?;
            if !args.check {
                build_project_ui(project, &report)?;
                activate_lifecycle_project_generation(
                    &project.plan,
                    &report,
                    Duration::from_millis(args.lock_timeout_ms),
                )?;
            }
            (report, Some(project))
        }
        ResolvedLifecycleProject::Files {
            workspace,
            catalog,
            config,
            out,
            ..
        } => (
            run_lifecycle_build(&LifecycleBuildOptions {
                root: workspace.clone(),
                catalog: catalog.clone(),
                config: config.clone(),
                out: out.clone(),
                check: args.check,
                lock_timeout: Duration::from_millis(args.lock_timeout_ms),
                nodes: None,
                activation_inputs: None,
                cancel: None,
            })?,
            None,
        ),
    };
    match args.output {
        LifecycleOutput::Json => println!("{}", serde_json::to_string(&report)?),
        LifecycleOutput::Human => {
            let mode = if report.check { "check" } else { "build" };
            if let Some(project) = detail {
                println!(
                    "distributed {mode}: project={} application={} runtime={}{}",
                    project.name,
                    project.application_package,
                    project.runtime_binary,
                    project
                        .ui
                        .as_ref()
                        .map(|ui| format!(" ui={}", ui.display()))
                        .unwrap_or_default()
                );
            }
            println!(
                "lifecycle graph: {} generation={} release={} nodes={}",
                if report.ok { "ok" } else { "drift" },
                report.generation_id,
                report.release_id,
                report.order.len()
            );
            for drift in &report.drift {
                println!(
                    "drift node={} output={} workspace={} built={}",
                    drift.node_id,
                    drift.output,
                    drift.workspace_identity.as_deref().unwrap_or("missing"),
                    drift.built_identity
                );
            }
        }
    }
    if !report.ok {
        return Err("lifecycle build check detected drift".into());
    }
    Ok(())
}

fn build_project_runtime(project: &DiscoveredLifecycleProject) -> Result<(), Box<dyn Error>> {
    eprintln!(
        "distributed build: compiling Rust runtime {} ({})",
        project.runtime_binary, project.runtime_package
    );
    run_project_command(
        &project.cargo_root,
        "cargo",
        &[
            OsString::from("build"),
            OsString::from("--package"),
            OsString::from(&project.runtime_package),
            OsString::from("--bin"),
            OsString::from(&project.runtime_binary),
        ],
        "Rust runtime build",
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProjectGenerationPhase {
    ApplicationPreflight,
    LifecycleBuild,
}

fn contextualize_project_generation_error(
    project: &DiscoveredLifecycleProject,
    phase: ProjectGenerationPhase,
    error: impl std::fmt::Display,
) -> Box<dyn Error> {
    let error = error.to_string();
    if is_application_introspection_failure(phase, &error) {
        return format!(
            "typed application `{}` could not be introspected through `{}`; the selected application crate must publicly export a zero-argument function returning `distributed::ApplicationManifest` (or select another export with `[package.metadata.distributed.application] entrypoint = \"...\"`):\n{error}",
            project.application_package, project.application_entrypoint
        )
        .into();
    }
    format!(
        "coherent application generation failed for `{}`:\n{error}",
        project.name
    )
    .into()
}

fn is_application_introspection_failure(phase: ProjectGenerationPhase, error: &str) -> bool {
    match phase {
        ProjectGenerationPhase::ApplicationPreflight => {
            error.contains("manifest harness failed for package")
        }
        ProjectGenerationPhase::LifecycleBuild => error.contains("lifecycle node `application`"),
    }
}

fn prepare_project_wasm_pures(project: &DiscoveredLifecycleProject) -> Result<(), Box<dyn Error>> {
    if project.ui.is_none() {
        return Ok(());
    }
    let json = run_manifest_harness(
        &HarnessOptions {
            path: project.cargo_root.clone(),
            manifest_path: Some(project.cargo_root.join("Cargo.toml")),
            package: Some(project.application_package.clone()),
            features: Vec::new(),
            no_default_features: false,
            entrypoint: Some(project.application_entrypoint.clone()),
            distributed_path: Some(project.distributed_root.clone()),
        },
        HarnessMode::DescribeJson,
    )?;
    let manifest: serde_json::Value = serde_json::from_str(&json)?;
    validate_manifest_json(&manifest)?;
    let wasm_pack_launcher = project
        .javascript
        .as_ref()
        .zip(project.ui.as_ref())
        .map(|(javascript, ui)| javascript.wasm_pack_launcher(ui));
    build_declared_wasm_pures(
        &manifest,
        &project.cargo_root.join("Cargo.toml"),
        wasm_pack_launcher.as_deref(),
    )?;
    Ok(())
}

fn build_project_ui(
    project: &DiscoveredLifecycleProject,
    generation: &crate::lifecycle::LifecycleBuildReport,
) -> Result<(), Box<dyn Error>> {
    if let Some(ui) = &project.ui {
        let ui_root = ui;
        prepare_ui_dependencies(project, &ui_root)?;
        eprintln!(
            "distributed build: checking SvelteKit UI {}",
            ui_root.display()
        );
        run_project_ui_script(project, generation, ui_root, "check", true)?;
        eprintln!(
            "distributed build: compiling SvelteKit UI {}",
            ui_root.display()
        );
        run_project_ui_script(project, generation, ui_root, "build", false)?;
    }
    Ok(())
}

fn run_project_ui_script(
    project: &DiscoveredLifecycleProject,
    generation: &crate::lifecycle::LifecycleBuildReport,
    ui_root: &Path,
    script: &str,
    if_present: bool,
) -> Result<(), Box<dyn Error>> {
    let lifecycle_dir = if project.plan.out.is_absolute() {
        project.plan.out.clone()
    } else {
        project.plan.root.join(&project.plan.out)
    };
    let mut command = Command::new("npm");
    command.args(["run", script]);
    if if_present {
        command.arg("--if-present");
    }
    let status = command
        .current_dir(ui_root)
        .env("DISTRIBUTED_LIFECYCLE_OWNS_CLIENT_COMPILE", "1")
        .env("DISTRIBUTED_LIFECYCLE_PROJECT_ROOT", &project.plan.root)
        .env("DISTRIBUTED_LIFECYCLE_DIR", lifecycle_dir)
        .env(
            "DISTRIBUTED_LIFECYCLE_GENERATION_ID",
            &generation.generation_id,
        )
        .status()?;
    if !status.success() {
        return Err(format!("SvelteKit UI `{script}` failed with {status}").into());
    }
    Ok(())
}

fn prepare_ui_dependencies(
    project: &DiscoveredLifecycleProject,
    ui_root: &Path,
) -> Result<(), Box<dyn Error>> {
    ensure_ui_dependencies(ui_root)?;
    let Some(javascript) = &project.javascript else {
        return Ok(());
    };
    if let Err(error) = javascript.verify_installed(ui_root) {
        eprintln!("distributed: refreshing SvelteKit dependencies because {error}");
        install_ui_dependencies(ui_root)?;
        javascript.verify_installed(ui_root)?;
    }
    Ok(())
}

fn prepare_project_javascript(
    project: &DiscoveredLifecycleProject,
    check: bool,
) -> Result<(), Box<dyn Error>> {
    if let Some(javascript) = &project.javascript {
        javascript.prepare(&project.cargo_root, check)?;
        if check {
            let ui = project
                .ui
                .as_ref()
                .ok_or("Distributed JavaScript package resolved without a UI root")?;
            javascript.verify_installed(ui)?;
        }
    }
    Ok(())
}

fn ensure_ui_dependencies(ui_root: &Path) -> Result<(), Box<dyn Error>> {
    if ui_root.join("node_modules").is_dir() {
        return Ok(());
    }
    install_ui_dependencies(ui_root)
}

fn install_ui_dependencies(ui_root: &Path) -> Result<(), Box<dyn Error>> {
    let install = if ui_root.join("package-lock.json").is_file() {
        "ci"
    } else {
        "install"
    };
    eprintln!(
        "distributed: installing SvelteKit dependencies with `npm {install}` in {}",
        ui_root.display()
    );
    run_project_command(
        ui_root,
        "npm",
        &[OsString::from(install)],
        "SvelteKit dependency install",
    )
}

fn run_project_command(
    cwd: &Path,
    program: &str,
    args: &[OsString],
    label: &str,
) -> Result<(), Box<dyn Error>> {
    let status = Command::new(program).args(args).current_dir(cwd).status()?;
    if !status.success() {
        return Err(format!("{label} failed with {status}").into());
    }
    Ok(())
}

fn run_dev(args: &DevArgs, command_prefix: &[&str]) -> Result<(), Box<dyn Error>> {
    let resolved = resolve_lifecycle_project(
        args.project.as_deref(),
        args.root.as_deref(),
        args.catalog.as_deref(),
        args.config.as_deref(),
        args.out.as_deref(),
        command_prefix,
    )?;
    let stop = Arc::new(AtomicBool::new(false));
    let signal_stop = Arc::clone(&stop);
    ctrlc::set_handler(move || signal_stop.store(true, Ordering::SeqCst))?;
    let report = match resolved {
        ResolvedLifecycleProject::Discovered(project) => {
            prepare_project_javascript(&project, false)?;
            if let Some(ui) = &project.ui {
                prepare_ui_dependencies(&project, ui)?;
            }
            eprintln!(
                "distributed dev: project={} api={} ui={} (Ctrl-C to stop)",
                project.name,
                project.runtime_binary,
                project
                    .ui
                    .as_ref()
                    .map(|ui| ui.display().to_string())
                    .unwrap_or_else(|| "none".to_string())
            );
            run_lifecycle_project_dev(&LifecycleProjectDevOptions {
                project: project.plan,
                build: LifecycleBuildRequest {
                    check: false,
                    check_baseline: LifecycleCheckBaseline::Workspace,
                    lock_timeout: Duration::from_millis(args.lock_timeout_ms),
                    nodes: None,
                    activation_inputs: None,
                    cancel: Some(Arc::clone(&stop)),
                    activation: LifecycleActivation::Immediate,
                },
                stop: Arc::clone(&stop),
                progress: true,
            })?
        }
        ResolvedLifecycleProject::Files {
            project,
            workspace,
            catalog,
            config,
            out,
        } => {
            eprintln!(
                "lifecycle dev: starting project={} and waiting for the initial generation (Ctrl-C to stop)",
                project.display()
            );
            run_lifecycle_dev(&LifecycleDevOptions {
                build: LifecycleBuildOptions {
                    root: workspace,
                    catalog,
                    config,
                    out,
                    check: false,
                    lock_timeout: Duration::from_millis(args.lock_timeout_ms),
                    nodes: None,
                    activation_inputs: None,
                    cancel: Some(Arc::clone(&stop)),
                },
                stop: Arc::clone(&stop),
                progress: true,
            })?
        }
    };
    println!(
        "lifecycle dev: stopped initial={} final={} rebuilds={}",
        report.initial_generation, report.final_generation, report.rebuilds
    );
    for (process, count) in report.restarts {
        println!("process={process} restarts={count}");
    }
    Ok(())
}

fn run_contracts(args: &ContractsArgs) -> Result<(), Box<dyn Error>> {
    match &args.command {
        ContractsCommands::Check(check) => run_contracts_check(check),
        ContractsCommands::Accept(accept) => run_contracts_accept(accept),
    }
}

fn run_contracts_check(args: &ContractsCheckArgs) -> Result<(), Box<dyn Error>> {
    let root = absolute_path(&args.root)?;
    let catalog_path = if args.catalog.is_absolute() {
        args.catalog.clone()
    } else {
        root.join(&args.catalog)
    };
    let catalog = ContractCatalog::from_path(&catalog_path)?;
    let report = contracts_check(&catalog, &root, std::iter::empty());
    match args.output {
        ContractsOutput::Human => {
            if report.human.is_empty() {
                println!("contracts check: ok");
            } else {
                println!("{}", report.human);
            }
        }
        ContractsOutput::Json => {
            println!("{}", serde_json::to_string_pretty(&report.result)?);
        }
    }
    if report.ok {
        Ok(())
    } else {
        Err("contracts check failed".into())
    }
}

fn run_contracts_accept(args: &ContractsAcceptArgs) -> Result<(), Box<dyn Error>> {
    let Some(scope) = ContractAcceptScope::parse(&args.scope) else {
        let diagnostic = unknown_scope_diagnostic(&args.scope);
        return Err(diagnostic.human().into());
    };
    let root = absolute_path(&args.root)?;
    let staged_source = fs::read_to_string(&args.staged)?;
    let staged_json: serde_json::Value = serde_json::from_str(&staged_source)?;
    let object = staged_json
        .as_object()
        .ok_or("staged payload must be a JSON object of path -> string contents")?;
    let mut staged = BTreeMap::new();
    for (path, value) in object {
        let contents = value
            .as_str()
            .ok_or_else(|| format!("staged path `{path}` must map to a UTF-8 string"))?;
        staged.insert(path.clone(), contents.as_bytes().to_vec());
    }
    let report = contracts_accept(&root, scope, &staged)?;
    match args.output {
        ContractsOutput::Human => {
            if report.noop {
                println!("contracts accept: no-op ({})", report.scope);
            } else {
                println!(
                    "contracts accept: updated {} path(s) for scope {}",
                    report.changed_paths.len(),
                    report.scope
                );
                for path in &report.changed_paths {
                    println!("  {path}");
                }
            }
        }
        ContractsOutput::Json => {
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
    }
    if report.ok {
        Ok(())
    } else {
        Err("contracts accept failed".into())
    }
}

fn run_skills_list() -> Result<(), Box<dyn Error>> {
    let width = embedded_skills()
        .iter()
        .map(|skill| skill.name.len())
        .max()
        .unwrap_or(0);
    for skill in embedded_skills() {
        println!("{:width$}  {}", skill.name, skill.description);
    }
    Ok(())
}

fn run_skills_init(args: &SkillsInitArgs) -> Result<(), Box<dyn Error>> {
    let container = absolute_path(&args.path)?;
    if container.exists() && !container.is_dir() {
        return Err(format!("{} exists and is not a directory", container.display()).into());
    }
    let (Some(anchor), Some(container_name)) = (container.parent(), container.file_name()) else {
        return Err(format!("--path {} has no parent directory", container.display()).into());
    };
    let anchor = anchor.to_path_buf();

    let wiring = resolve_agent_wiring(&args.agents, &anchor)?;
    let agents_md = if wiring.agents {
        read_optional(&anchor.join(AGENTS_MD_FILE))?
    } else {
        None
    };

    let project = generate_skills(&SkillsInitSpec {
        container: container_name.to_string_lossy().into_owned(),
        wire_claude: wiring.claude,
        wire_agents: wiring.agents,
        agents_md,
    });

    for file in &project.files {
        let target = anchor.join(&file.path);
        let (action, skip_reason) = if file.mode == Some(FileMode::Symlink) {
            (
                decide_symlink_write(&target, Path::new(&file.contents), args.force)?,
                "existing path; --force to replace with a symlink",
            )
        } else if file.path == AGENTS_MD_FILE {
            // AGENTS.md contents are a merge of the on-disk file, so "differs"
            // means "update the managed block" — never skip, never need --force.
            (
                decide_managed_write(read_optional(&target)?.as_deref(), &file.contents),
                "",
            )
        } else {
            (
                decide_write(
                    read_optional(&target)?.as_deref(),
                    &file.contents,
                    args.force,
                ),
                "local edits; --force to overwrite",
            )
        };
        let shown = display_path(&target);
        match action {
            WriteAction::Created | WriteAction::Updated => {
                write_generated_file(&anchor, file)?;
                if file.mode == Some(FileMode::Symlink) {
                    println!("{} {shown} -> {}", action.verb(), file.contents);
                } else {
                    println!("{} {shown}", action.verb());
                }
            }
            WriteAction::Unchanged => println!("unchanged {shown}"),
            WriteAction::Skipped => {
                eprintln!("warning: skipped {shown} ({skip_reason})");
            }
        }
    }
    for warning in &project.warnings {
        eprintln!("warning: {warning}");
    }

    let wired = match (wiring.claude, wiring.agents) {
        (true, true) => "claude, agents",
        (true, false) => "claude",
        (false, true) => "agents",
        (false, false) => "none",
    };
    println!(
        "Initialized {} skills at {} (wired: {wired})",
        embedded_skills().len(),
        display_path(&container.join("skills")),
    );
    Ok(())
}

/// Which discovery adapters to wire, resolved from `--agents`.
struct AgentWiring {
    claude: bool,
    agents: bool,
}

fn resolve_agent_wiring(
    values: &[AgentHarness],
    anchor: &Path,
) -> Result<AgentWiring, Box<dyn Error>> {
    let auto = values.contains(&AgentHarness::Auto);
    let none = values.contains(&AgentHarness::None);
    if (auto || none) && values.len() > 1 {
        return Err("--agents auto and none cannot be combined with other values".into());
    }
    if none {
        return Ok(AgentWiring {
            claude: false,
            agents: false,
        });
    }
    if auto {
        let claude = anchor.join(".claude").is_dir();
        let agents = anchor.join(AGENTS_MD_FILE).is_file()
            || anchor.join(".agents").is_dir()
            || anchor.join(".gemini").is_dir()
            || anchor.join(".pi").is_dir();
        if !claude && !agents {
            // Fresh project: being discoverable by default beats being minimal.
            return Ok(AgentWiring {
                claude: true,
                agents: true,
            });
        }
        return Ok(AgentWiring { claude, agents });
    }

    let mut wiring = AgentWiring {
        claude: false,
        agents: false,
    };
    for value in values {
        match value {
            AgentHarness::Claude => wiring.claude = true,
            AgentHarness::Codex
            | AgentHarness::Grok
            | AgentHarness::Openai
            | AgentHarness::Gemini
            | AgentHarness::Pi
            | AgentHarness::Agents => wiring.agents = true,
            AgentHarness::Auto | AgentHarness::None => unreachable!("handled above"),
        }
    }
    Ok(wiring)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriteAction {
    Created,
    Unchanged,
    Skipped,
    Updated,
}

impl WriteAction {
    fn verb(self) -> &'static str {
        match self {
            WriteAction::Created => "created",
            WriteAction::Unchanged => "unchanged",
            WriteAction::Skipped => "skipped",
            WriteAction::Updated => "updated",
        }
    }
}

/// Per-file drift decision for skill files: never silently clobber local edits.
fn decide_write(on_disk: Option<&str>, contents: &str, force: bool) -> WriteAction {
    match on_disk {
        None => WriteAction::Created,
        Some(existing) if existing == contents => WriteAction::Unchanged,
        Some(_) if force => WriteAction::Updated,
        Some(_) => WriteAction::Skipped,
    }
}

/// Drift decision for symlink entries: a link already pointing at the right
/// target is unchanged; anything else at the path (a stale link, or a real
/// file/directory such as a user's own skill) is only replaced with --force.
fn decide_symlink_write(
    path: &Path,
    target: &Path,
    force: bool,
) -> Result<WriteAction, Box<dyn Error>> {
    match fs::symlink_metadata(path) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(WriteAction::Created),
        Err(err) => Err(format!("inspect {}: {err}", path.display()).into()),
        Ok(meta) if meta.file_type().is_symlink() => {
            if fs::read_link(path)? == target {
                Ok(WriteAction::Unchanged)
            } else if force {
                Ok(WriteAction::Updated)
            } else {
                Ok(WriteAction::Skipped)
            }
        }
        Ok(_) if force => Ok(WriteAction::Updated),
        Ok(_) => Ok(WriteAction::Skipped),
    }
}

/// Drift decision for merged files (AGENTS.md): a difference is the managed
/// block converging, so it is always written.
fn decide_managed_write(on_disk: Option<&str>, contents: &str) -> WriteAction {
    match on_disk {
        None => WriteAction::Created,
        Some(existing) if existing == contents => WriteAction::Unchanged,
        Some(_) => WriteAction::Updated,
    }
}

fn read_optional(path: &Path) -> Result<Option<String>, Box<dyn Error>> {
    match fs::read_to_string(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(format!("read {}: {err}", path.display()).into()),
    }
}

/// Prefer a cwd-relative path in status output; fall back to the full path.
fn display_path(path: &Path) -> String {
    std::env::current_dir()
        .ok()
        .and_then(|cwd| path.strip_prefix(&cwd).ok())
        .unwrap_or(path)
        .display()
        .to_string()
}

fn run_scaffold(args: &ScaffoldArgs) -> Result<(), Box<dyn Error>> {
    validate_scaffold_kind(args.framework, args.kind.as_deref())?;
    let transport = if args.http && args.knative {
        return Err("--http and --knative cannot be used together".into());
    } else if args.http {
        Transport::Http
    } else if args.knative {
        Transport::Knative
    } else {
        args.transport
    };

    let github = parse_optional_github_repo(args.github.as_deref(), "--github")?;
    let github_preview =
        parse_optional_github_repo(args.github_preview.as_deref(), "--github-preview")?;
    let github_promote =
        parse_optional_github_repo(args.github_promote.as_deref(), "--github-promote")?;

    // The default output directory uses the normalized package name, so derive it
    // (and fail fast on an invalid name) before creating any directory.
    let package_name = package_name(&args.name)?;
    let output_dir = args
        .path
        .clone()
        .unwrap_or_else(|| PathBuf::from(&package_name));
    let output_dir = absolute_path(&output_dir)?;
    ensure_output_dir(&output_dir, args.force)?;

    let distributed_path = resolve_distributed_path(args.distributed_path.as_deref(), &output_dir)?;
    let distributed_dependency_path = path_for_toml(&relative_path(&output_dir, &distributed_path));

    // GraphQL execution needs a SQL store feature; promote in-memory → sqlite.
    let store = {
        let store = args.store.into();
        if args.query_api && matches!(store, crate::StoreTarget::InMemory) {
            eprintln!(
                "warning: --query-api requires a SQL store; promoting --store in-memory to sqlite"
            );
            crate::StoreTarget::Sqlite
        } else {
            store
        }
    };

    let spec = ServiceScaffoldSpec {
        name: args.name.clone(),
        transport: transport.into(),
        store,
        bus: args.bus.map(Into::into),
        metrics: args.metrics.map(Into::into),
        models: args.model.clone(),
        read_models: args.read_models || args.query_api,
        query_api: args.query_api,
        tracing: args.tracing,
        commands: args.command.clone(),
        events: args.event.clone(),
        distributed_dependency_path,
        gitops: args.gitops,
        gitops_promote: args.gitops_promote.map(Into::into),
        github,
        github_preview,
        github_promote,
    };

    let project = generate_service_scaffold(spec)?;
    for file in &project.files {
        write_generated_file(&output_dir, file)?;
    }
    for warning in &project.warnings {
        eprintln!("warning: {warning}");
    }
    for action in &project.post_create_actions {
        match action {
            PostCreateAction::EnsureGithubRepository { repo } => ensure_github_repo(repo)?,
        }
    }

    println!("Scaffolded Distributed service at {}", output_dir.display());
    Ok(())
}

fn run_describe(args: &DescribeArgs) -> Result<(), Box<dyn Error>> {
    match args.format {
        ManifestFormat::Json => {
            let json = run_manifest_harness(
                &HarnessOptions {
                    path: args.path.clone(),
                    manifest_path: args.manifest_path.clone(),
                    package: args.package.clone(),
                    features: args.features.clone(),
                    no_default_features: args.no_default_features,
                    entrypoint: args.entrypoint.clone(),
                    distributed_path: args.distributed_path.clone(),
                },
                HarnessMode::DescribeJson,
            )?;
            let envelope: serde_json::Value = serde_json::from_str(&json)?;
            validate_manifest_json(&envelope)?;
            if std::env::var_os("DISTRIBUTED_LIFECYCLE_ROOT").is_some()
                && std::env::var_os("DISTRIBUTED_LIFECYCLE_CHECK").as_deref()
                    != Some(std::ffi::OsStr::new("1"))
            {
                let cargo_manifest =
                    resolve_target_manifest_path(&args.path, args.manifest_path.as_deref())?;
                build_declared_wasm_pures(
                    &envelope,
                    &cargo_manifest,
                    args.wasm_pack_launcher.as_deref(),
                )?;
            }
            println!("{}", serde_json::to_string_pretty(&envelope)?);
            Ok(())
        }
    }
}

const MAX_CLIENT_MANIFEST_BYTES: usize = 16 * 1024 * 1024;
const MAX_CLIENT_DOCUMENT_BYTES: usize = 1024 * 1024;
const MAX_GENERATED_CLIENT_ARTIFACT_BYTES: usize = 8 * 1024 * 1024;
const MAX_GENERATED_CLIENT_FILES: usize = 8_192;

fn run_client(args: &ClientArgs) -> Result<(), Box<dyn Error>> {
    let manifest_source =
        read_utf8_bounded(&args.manifest, MAX_CLIENT_MANIFEST_BYTES, "client manifest")?;
    let manifest: serde_json::Value =
        serde_json::from_str(&manifest_source).map_err(|error| -> Box<dyn Error> {
            format!("parse client manifest {}: {error}", args.manifest.display()).into()
        })?;
    let selector = match (&args.role, &args.surface) {
        (Some(role), None) => ClientSurfaceSelector::role(role.clone()),
        (None, Some(surface)) => {
            // Prefer explicit CLI roles when both lists are provided; otherwise
            // take eligible/schema roles from the application surface in the
            // manifest (one source of truth — no dual inventory config).
            let (eligible_roles, schema_roles) = if !args.eligible_role.is_empty()
                && !args.schema_role.is_empty()
            {
                (args.eligible_role.clone(), args.schema_role.clone())
            } else if !args.eligible_role.is_empty() || !args.schema_role.is_empty() {
                return Err(
                        "pass both --eligible-role and --schema-role, or neither (to use the manifest surface)"
                            .into(),
                    );
            } else {
                application_roles_from_manifest(&manifest, surface)?
            };
            ClientSurfaceSelector::application(surface.clone(), eligible_roles, schema_roles)
        }
        _ => {
            return Err("pass exactly one of --role <name> or --surface <application-name>".into());
        }
    };
    let documents = collect_client_documents(&args.documents)?;
    let project = compile_client(ClientCompileInput::new(manifest, selector, documents))?;

    if args.check {
        check_client_project(&args.out, &project)?;
        println!(
            "Distributed client artifacts are current ({} files)",
            project.files.len()
        );
    } else {
        write_client_project(&args.out, &project)?;
        println!(
            "Generated {} Distributed client artifacts at {}",
            project.files.len(),
            args.out.display()
        );
    }
    Ok(())
}

fn read_utf8_bounded(path: &Path, limit: usize, label: &str) -> Result<String, Box<dyn Error>> {
    let bytes =
        fs::read(path).map_err(|error| format!("read {label} {}: {error}", path.display()))?;
    if bytes.len() > limit {
        return Err(format!(
            "{label} {} is {} bytes; maximum supported size is {limit}",
            path.display(),
            bytes.len()
        )
        .into());
    }
    String::from_utf8(bytes)
        .map_err(|error| format!("{label} {} is not UTF-8: {error}", path.display()).into())
}

/// Read application eligible/schema roles from the client manifest surface.
///
/// Application surfaces already declare these on the exported manifest; the
/// client compiler should not require them again on the CLI when `--surface`
/// is used.
///
/// Kind/name mismatches deliberately return empty role lists so
/// `compile_client` can emit the shared `client.manifest.surface_mismatch`
/// diagnostic (one validation path for CLI and library callers).
fn application_roles_from_manifest(
    manifest: &serde_json::Value,
    surface_name: &str,
) -> Result<(Vec<String>, Vec<String>), Box<dyn Error>> {
    let Some(surface) = manifest.get("surface") else {
        return Ok((Vec::new(), Vec::new()));
    };
    let kind = surface
        .get("kind")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    let name = surface
        .get("name")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    if kind != "application" || name != surface_name {
        return Ok((Vec::new(), Vec::new()));
    }
    let roles = |field: &str| -> Result<Vec<String>, Box<dyn Error>> {
        let values = surface
            .get(field)
            .and_then(|value| value.as_array())
            .ok_or_else(|| format!("client manifest surface is missing non-empty `{field}`"))?;
        let roles = values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| format!("client manifest surface `{field}` must be strings"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if roles.is_empty() {
            return Err(format!("client manifest surface `{field}` must not be empty").into());
        }
        Ok(roles)
    };
    Ok((roles("eligible_roles")?, roles("schema_roles")?))
}

fn collect_client_documents(patterns: &[String]) -> Result<Vec<ClientDocument>, Box<dyn Error>> {
    let project_root = fs::canonicalize(std::env::current_dir()?)?;
    let mut matched = BTreeMap::<PathBuf, String>::new();
    for pattern in patterns {
        if pattern.trim().is_empty() {
            return Err("--documents glob must not be empty".into());
        }

        // Exact existing files must not go through glob expansion. Paths can
        // contain SvelteKit route params such as `[[gameId]]`, which are valid
        // path characters but glob metacharacters.
        let direct = PathBuf::from(pattern);
        if direct.is_file() {
            insert_client_document_path(&project_root, &mut matched, &direct)?;
            continue;
        }

        let mut pattern_matches = 0usize;
        let entries = glob::glob(pattern)
            .map_err(|error| format!("invalid --documents glob `{pattern}`: {error}"))?;
        for entry in entries {
            let path =
                entry.map_err(|error| format!("expand --documents glob `{pattern}`: {error}"))?;
            insert_client_document_path(&project_root, &mut matched, &path)?;
            pattern_matches += 1;
        }
        if pattern_matches == 0 {
            return Err(format!("--documents glob `{pattern}` matched no files").into());
        }
    }

    matched
        .into_iter()
        .map(|(path, source_path)| {
            Ok(ClientDocument::new(
                source_path,
                read_utf8_bounded(&path, MAX_CLIENT_DOCUMENT_BYTES, "GraphQL document")?,
            ))
        })
        .collect()
}

fn insert_client_document_path(
    project_root: &Path,
    matched: &mut BTreeMap<PathBuf, String>,
    path: &Path,
) -> Result<(), Box<dyn Error>> {
    let canonical = fs::canonicalize(path)
        .map_err(|error| format!("resolve GraphQL document {}: {error}", path.display()))?;
    if !canonical.is_file() {
        return Err(format!("GraphQL document {} is not a file", path.display()).into());
    }
    let relative = canonical.strip_prefix(project_root).map_err(|_| {
        format!(
            "GraphQL document {} resolves outside project root {}",
            path.display(),
            project_root.display()
        )
    })?;
    let source_path = portable_relative_path(relative)?;
    matched.entry(canonical).or_insert(source_path);
    Ok(())
}

fn portable_relative_path(path: &Path) -> Result<String, Box<dyn Error>> {
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => parts.push(value.to_string_lossy().into_owned()),
            Component::CurDir => {}
            _ => {
                return Err(
                    format!("path {} is not a portable relative path", path.display()).into(),
                )
            }
        }
    }
    if parts.is_empty() {
        return Err(format!("path {} has no file name", path.display()).into());
    }
    Ok(parts.join("/"))
}

fn generated_client_path(
    root: &Path,
    file: &GeneratedClientFile,
) -> Result<PathBuf, Box<dyn Error>> {
    let relative = Path::new(&file.path);
    if relative.is_absolute()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!("compiler produced unsafe generated path `{}`", file.path).into());
    }
    Ok(root.join(relative))
}

fn write_client_project(
    root: &Path,
    project: &GeneratedClientProject,
) -> Result<(), Box<dyn Error>> {
    validate_generated_client_files(&project.files)?;
    validate_client_write_targets(root, &project.files)?;
    let stale_files = stale_generated_client_files(root, project)?;
    validate_client_output_convergence(root, project, &stale_files)?;
    fs::create_dir_all(root)?;

    for path in stale_files {
        fs::remove_file(&path).map_err(|error| {
            format!(
                "remove stale generated client artifact {}: {error}",
                path.display()
            )
        })?;
    }

    // Provenance is written last so an interrupted run never advertises a new
    // complete generation before its operation modules exist.
    let mut files = project.files.iter().collect::<Vec<_>>();
    files.sort_by_key(|file| (file.path == "manifest.json", file.path.as_str()));
    for file in files {
        let path = generated_client_path(root, file)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, &file.contents)
            .map_err(|error| format!("write generated artifact {}: {error}", path.display()))?;
    }
    Ok(())
}

fn validate_client_output_convergence(
    root: &Path,
    project: &GeneratedClientProject,
    stale_files: &[PathBuf],
) -> Result<(), Box<dyn Error>> {
    let expected = project
        .files
        .iter()
        .map(|file| file.path.clone())
        .collect::<BTreeSet<_>>();
    let stale = stale_files
        .iter()
        .map(|path| {
            path.strip_prefix(root)
                .map_err(|_| {
                    format!(
                        "stale generated client path {} escaped output {}",
                        path.display(),
                        root.display()
                    )
                })
                .and_then(|relative| {
                    portable_relative_path(relative).map_err(|error| error.to_string())
                })
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    let unexpected = collect_generated_client_paths(root)?
        .into_iter()
        .filter(|path| !expected.contains(path) && !stale.contains(path))
        .collect::<Vec<_>>();
    if unexpected.is_empty() {
        return Ok(());
    }
    Err(format!(
        "generated output contains files without current or previous compiler ownership:\n  {}; refusing to write until they are moved or removed",
        unexpected.join("\n  ")
    )
    .into())
}

fn validate_client_write_targets(
    root: &Path,
    files: &[GeneratedClientFile],
) -> Result<(), Box<dyn Error>> {
    match fs::symlink_metadata(root) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(format!(
                "generated output {} must be a real directory, not a symlink or other entry",
                root.display()
            )
            .into())
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(format!("inspect generated output {}: {error}", root.display()).into())
        }
    }

    for file in files {
        let relative = Path::new(&file.path);
        let mut current = root.to_path_buf();
        let component_count = relative.components().count();
        for (index, component) in relative.components().enumerate() {
            let Component::Normal(component) = component else {
                unreachable!("generated paths were validated before write-target inspection")
            };
            current.push(component);
            let metadata = match fs::symlink_metadata(&current) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                Err(error) => {
                    return Err(format!(
                        "inspect generated client path {}: {error}",
                        current.display()
                    )
                    .into())
                }
            };
            let is_target = index + 1 == component_count;
            let valid = if is_target {
                metadata.is_file() && !metadata.file_type().is_symlink()
            } else {
                metadata.is_dir() && !metadata.file_type().is_symlink()
            };
            if !valid {
                return Err(format!(
                    "generated client path {} contains a symlink or incompatible entry",
                    current.display()
                )
                .into());
            }
        }
    }
    Ok(())
}

fn stale_generated_client_files(
    root: &Path,
    project: &GeneratedClientProject,
) -> Result<Vec<PathBuf>, Box<dyn Error>> {
    let provenance_path = root.join("manifest.json");
    let metadata = match fs::symlink_metadata(&provenance_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(format!(
                "inspect previous generated client manifest {}: {error}",
                provenance_path.display()
            )
            .into())
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!(
            "previous generated client manifest {} must be a regular file",
            provenance_path.display()
        )
        .into());
    }

    let source = read_utf8_bounded(
        &provenance_path,
        MAX_CLIENT_MANIFEST_BYTES,
        "previous generated client manifest",
    )?;
    let provenance: serde_json::Value = serde_json::from_str(&source).map_err(|error| {
        format!(
            "parse previous generated client manifest {}: {error}; refusing to guess stale-file ownership",
            provenance_path.display()
        )
    })?;
    let compiler_manifest_version = provenance
        .get("compiler_manifest_version")
        .and_then(serde_json::Value::as_u64);
    if !matches!(compiler_manifest_version, Some(1 | 2)) {
        return Err(format!(
            "previous generated client manifest {} has an unsupported compiler_manifest_version; refusing to guess stale-file ownership",
            provenance_path.display()
        )
        .into());
    }
    let operations = provenance
        .get("operations")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            format!(
                "previous generated client manifest {} is missing operations; refusing to guess stale-file ownership",
                provenance_path.display()
            )
        })?;
    let expected = project
        .files
        .iter()
        .map(|file| file.path.as_str())
        .collect::<BTreeSet<_>>();
    let mut stale = BTreeSet::new();
    for (index, operation) in operations.iter().enumerate() {
        let module_path = operation
            .get("module_path")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                format!(
                    "previous generated client manifest {} operations[{index}] is missing module_path; refusing to guess stale-file ownership",
                    provenance_path.display()
                )
            })?;
        validate_previous_client_module_path(module_path)?;
        if !expected.contains(module_path) {
            stale.insert(module_path.to_string());
        }
    }

    if compiler_manifest_version == Some(1) && !expected.contains("routes.ts") {
        stale.insert("routes.ts".to_string());
    }

    let mut paths = Vec::with_capacity(stale.len());
    for relative in stale {
        let path = root.join(&relative);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(format!(
                    "inspect stale generated client artifact {}: {error}",
                    path.display()
                )
                .into())
            }
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(format!(
                "stale generated client artifact {} must be a regular file",
                path.display()
            )
            .into());
        }
        let contents = read_utf8_bounded(
            &path,
            MAX_GENERATED_CLIENT_ARTIFACT_BYTES,
            "stale generated client artifact",
        )?;
        let has_compatible_marker = contents
            .starts_with("/** GENERATED by distributed client. Do not edit. */")
            || (compiler_manifest_version == Some(1)
                && relative == "routes.ts"
                && contents
                    .starts_with("/** GENERATED framework-neutral `@load` ownership plan. */"));
        if !has_compatible_marker {
            return Err(format!(
                "refusing to remove {} because its compiler ownership marker is missing",
                path.display()
            )
            .into());
        }
        paths.push(path);
    }
    Ok(paths)
}

fn validate_previous_client_module_path(path: &str) -> Result<(), Box<dyn Error>> {
    let mut components = Path::new(path).components();
    let valid = matches!(components.next(), Some(Component::Normal(value)) if value == "operations")
        && matches!(components.next(), Some(Component::Normal(value)) if Path::new(value).extension().is_some_and(|extension| extension == "ts"))
        && components.next().is_none();
    if !valid {
        return Err(format!(
            "previous compiler provenance contains unsafe operation module path `{path}`; refusing to remove it"
        )
        .into());
    }
    Ok(())
}

fn check_client_project(
    root: &Path,
    project: &GeneratedClientProject,
) -> Result<(), Box<dyn Error>> {
    validate_generated_client_files(&project.files)?;
    let expected = project
        .files
        .iter()
        .map(|file| (file.path.clone(), file.contents.as_str()))
        .collect::<BTreeMap<_, _>>();
    let actual_paths = collect_generated_client_paths(root)?;
    let mut drift = Vec::new();

    for (path, contents) in &expected {
        let full_path = root.join(path);
        match fs::read_to_string(&full_path) {
            Ok(actual) if actual == *contents => {}
            Ok(_) => drift.push(format!("changed {path}")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                drift.push(format!("missing {path}"));
            }
            Err(error) => {
                return Err(
                    format!("read generated artifact {}: {error}", full_path.display()).into(),
                )
            }
        }
    }
    for path in actual_paths {
        if !expected.contains_key(&path) {
            drift.push(format!("unexpected {path}"));
        }
    }
    if drift.is_empty() {
        return Ok(());
    }
    drift.sort();
    Err(format!(
        "generated Distributed client artifacts are stale:\n  {}\nrun `distributed client` without --check to regenerate",
        drift.join("\n  ")
    )
    .into())
}

fn validate_generated_client_files(files: &[GeneratedClientFile]) -> Result<(), Box<dyn Error>> {
    if files.is_empty() || files.len() > MAX_GENERATED_CLIENT_FILES {
        return Err(format!(
            "compiler produced {} files; expected 1..={MAX_GENERATED_CLIENT_FILES}",
            files.len()
        )
        .into());
    }
    let mut paths = BTreeSet::new();
    for file in files {
        let _ = generated_client_path(Path::new("."), file)?;
        if !paths.insert(file.path.as_str()) {
            return Err(format!("compiler produced duplicate path `{}`", file.path).into());
        }
    }
    Ok(())
}

fn collect_generated_client_paths(root: &Path) -> Result<BTreeSet<String>, Box<dyn Error>> {
    let mut files = BTreeSet::new();
    match fs::symlink_metadata(root) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(format!(
                "generated output {} must be a real directory, not a symlink or other entry",
                root.display()
            )
            .into())
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(files),
        Err(error) => {
            return Err(format!("inspect generated output {}: {error}", root.display()).into())
        }
    }
    let mut pending = vec![(root.to_path_buf(), PathBuf::new())];
    while let Some((directory, relative)) = pending.pop() {
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let next_relative = relative.join(entry.file_name());
            if file_type.is_symlink() {
                return Err(format!(
                    "generated output contains unsupported symlink {}",
                    entry.path().display()
                )
                .into());
            }
            if file_type.is_dir() {
                pending.push((entry.path(), next_relative));
            } else if file_type.is_file() {
                files.insert(portable_relative_path(&next_relative)?);
                if files.len() > MAX_GENERATED_CLIENT_FILES {
                    return Err(format!(
                        "generated output {} exceeds {MAX_GENERATED_CLIENT_FILES} files",
                        root.display()
                    )
                    .into());
                }
            } else {
                return Err(format!(
                    "generated output contains unsupported entry {}",
                    entry.path().display()
                )
                .into());
            }
        }
    }
    Ok(files)
}

fn run_client_manifest(args: &ClientManifestArgs) -> Result<(), Box<dyn Error>> {
    let json = run_manifest_harness(
        &HarnessOptions {
            path: args.path.clone(),
            manifest_path: args.manifest_path.clone(),
            package: args.package.clone(),
            features: args.features.clone(),
            no_default_features: args.no_default_features,
            entrypoint: args.entrypoint.clone(),
            distributed_path: args.distributed_path.clone(),
        },
        HarnessMode::ClientManifest,
    )?;
    let manifest: serde_json::Value = serde_json::from_str(&json)?;
    validate_client_manifest_json(&manifest)?;
    println!("{}", serde_json::to_string_pretty(&manifest)?);
    Ok(())
}

fn run_schema(args: &SchemaArgs) -> Result<(), Box<dyn Error>> {
    let mode = match args.format {
        SchemaFormat::Graphql => HarnessMode::SchemaGraphql,
        _ => HarnessMode::SchemaSql(args.dialect),
    };
    let rendered = run_manifest_harness(
        &HarnessOptions {
            path: args.path.clone(),
            manifest_path: args.manifest_path.clone(),
            package: args.package.clone(),
            features: args.features.clone(),
            no_default_features: args.no_default_features,
            entrypoint: args.entrypoint.clone(),
            distributed_path: args.distributed_path.clone(),
        },
        mode,
    ).map_err(|err| -> Box<dyn Error> {
        let msg = err.to_string();
        if msg.contains("graphql_sdl") || msg.contains("no method named `graphql_sdl`") {
            format!(
                "target service's distributed version predates read-model GraphQL schema support — upgrade distributed to a version that provides graphql_sdl_for_tables(): {msg}"
            ).into()
        } else {
            err
        }
    })?;

    let content = match args.format {
        SchemaFormat::Sql => rendered,
        SchemaFormat::Atlas => render_atlas_schema(&atlas_spec_from_flags(args, rendered)?)?,
        SchemaFormat::Graphql => rendered,
    };

    if let Some(out) = &args.out {
        if let Some(parent) = out.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        fs::write(out, content)?;
    } else {
        print!("{content}");
    }
    Ok(())
}

/// Build an [`AtlasSchemaSpec`] from `--format atlas` flags plus the rendered
/// desired-state SQL. The database reference must be given explicitly (a Secret
/// reference for GitOps, or an inline URL for dev).
fn atlas_spec_from_flags(
    args: &SchemaArgs,
    sql: String,
) -> Result<AtlasSchemaSpec, Box<dyn Error>> {
    let name = args
        .name
        .clone()
        .ok_or("--name is required for --format atlas")?;

    let database = match (&args.db_url, &args.db_secret) {
        (Some(_), Some(_)) => {
            return Err("pass either --db-url or --db-secret, not both".into());
        }
        (Some(url), None) => AtlasDatabaseUrl::Inline(url.clone()),
        (None, Some(secret)) => AtlasDatabaseUrl::SecretKeyRef {
            name: secret.clone(),
            key: args.db_secret_key.clone(),
        },
        (None, None) => {
            return Err(
                "--format atlas needs a database: pass --db-secret <name> (GitOps) or --db-url <url> (dev)"
                    .into(),
            );
        }
    };

    Ok(AtlasSchemaSpec {
        name,
        namespace: args.namespace.clone(),
        database,
        dev_url: args.dev_url.clone(),
        sql,
    })
}

fn validate_scaffold_kind(framework: Framework, kind: Option<&str>) -> Result<(), Box<dyn Error>> {
    if framework != Framework::Distributed {
        return Err("only --framework distributed is supported".into());
    }

    if let Some(kind) = kind {
        match kind {
            "distributed-microsvc" | "distributed" => {}
            _ => {
                return Err(format!(
                    "unsupported service kind `{kind}`; expected distributed-microsvc"
                )
                .into());
            }
        }
    }

    Ok(())
}

fn ensure_output_dir(path: &Path, force: bool) -> Result<(), Box<dyn Error>> {
    if path.exists() {
        if !path.is_dir() {
            return Err(format!("{} exists and is not a directory", path.display()).into());
        }
        if !force && fs::read_dir(path)?.next().is_some() {
            return Err(format!(
                "{} already exists and is not empty; pass --force to overwrite generated files",
                path.display()
            )
            .into());
        }
    }
    fs::create_dir_all(path)?;
    Ok(())
}

/// Write one generated file under `output_dir`, creating parent directories and
/// honoring the optional mode hint (executable bit, or a symlink whose
/// `contents` is the relative target).
fn write_generated_file(output_dir: &Path, file: &GeneratedFile) -> Result<(), Box<dyn Error>> {
    let path = output_dir.join(&file.path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    if file.mode == Some(FileMode::Symlink) {
        return replace_with_symlink(&path, Path::new(&file.contents));
    }
    fs::write(&path, &file.contents)?;
    if file.mode == Some(FileMode::Executable) {
        set_executable(&path)?;
    }
    Ok(())
}

/// Point `path` at `target`, replacing whatever is there. Only reached after a
/// write decision said to write, so removal of an existing entry is deliberate
/// (a stale link, or an old copy being converted with --force).
#[cfg(unix)]
fn replace_with_symlink(path: &Path, target: &Path) -> Result<(), Box<dyn Error>> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.is_dir() => fs::remove_dir_all(path)?,
        Ok(_) => fs::remove_file(path)?,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(format!("inspect {}: {err}", path.display()).into()),
    }
    std::os::unix::fs::symlink(target, path)?;
    Ok(())
}

#[cfg(not(unix))]
fn replace_with_symlink(path: &Path, _target: &Path) -> Result<(), Box<dyn Error>> {
    // generate_skills emits copies instead of symlinks off unix; reaching this
    // means a generator bug, not a user error.
    Err(format!(
        "symlink generation is unsupported on this platform: {}",
        path.display()
    )
    .into())
}

#[cfg(unix)]
fn set_executable(path: &Path) -> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = fs::metadata(path)?.permissions();
    perms.set_mode(perms.mode() | 0o111);
    fs::set_permissions(path, perms)?;
    Ok(())
}

#[cfg(not(unix))]
fn set_executable(_path: &Path) -> Result<(), Box<dyn Error>> {
    Ok(())
}

fn parse_optional_github_repo(
    raw: Option<&str>,
    flag: &str,
) -> Result<Option<GithubRepo>, Box<dyn Error>> {
    raw.map(|value| {
        GithubRepo::parse(value)
            .map_err(|err| -> Box<dyn Error> { format!("{flag}: {err}").into() })
    })
    .transpose()
}

fn ensure_github_repo(repo: &GithubRepo) -> Result<(), Box<dyn Error>> {
    let slug = repo.slug();
    let view_output = Command::new("gh")
        .args(["repo", "view", &slug, "--json", "nameWithOwner"])
        .output();

    match view_output {
        Ok(output) if output.status.success() => {
            println!("GitHub repository {slug} already exists");
            return Ok(());
        }
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(
                "GitHub CLI (`gh`) is not installed or not in PATH. Install it before using --github."
                    .into(),
            );
        }
        Err(err) => return Err(Box::new(err)),
    }

    let output = Command::new("gh")
        .args(github_repo_create_args(&slug))
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("gh repo create failed: {stderr}").into());
    }
    println!("Created GitHub repository {slug}");
    Ok(())
}

fn github_repo_create_args(slug: &str) -> Vec<&str> {
    vec!["repo", "create", slug, "--private"]
}

fn validate_manifest_json(envelope: &serde_json::Value) -> Result<(), Box<dyn Error>> {
    let Some(schema_version) = envelope
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
    else {
        return Err("manifest JSON is missing numeric schema_version".into());
    };
    if schema_version != DISTRIBUTED_MANIFEST_SCHEMA_VERSION {
        return Err(format!(
            "unsupported Distributed manifest schema version {schema_version}; expected {DISTRIBUTED_MANIFEST_SCHEMA_VERSION}"
        )
        .into());
    }
    // `describe` emits ApplicationManifest JSON (logical composition artifact),
    // not the retired DistributedManifestEnvelope { project: ... } shape.
    if envelope
        .get("name")
        .and_then(serde_json::Value::as_str)
        .map(str::is_empty)
        .unwrap_or(true)
    {
        return Err("application manifest JSON is missing non-empty string name".into());
    }
    for field in [
        "modules",
        "commands",
        "events",
        "projections",
        "models",
        "surfaces",
    ] {
        if envelope
            .get(field)
            .and_then(serde_json::Value::as_array)
            .is_none()
        {
            return Err(format!("application manifest JSON is missing array {field}").into());
        }
    }
    let framework = envelope
        .get("extensions")
        .and_then(serde_json::Value::as_array)
        .and_then(|extensions| {
            extensions.iter().find(|extension| {
                extension.get("id").and_then(serde_json::Value::as_str)
                    == Some("distributed.framework")
            })
        })
        .ok_or("application manifest JSON is missing framework compatibility")?;
    if framework.get("version").and_then(serde_json::Value::as_u64) != Some(1) {
        return Err("unsupported application framework compatibility record version".into());
    }
    let release = framework
        .get("value")
        .and_then(|value| value.get("release"))
        .and_then(serde_json::Value::as_str)
        .filter(|release| !release.is_empty())
        .ok_or("application framework compatibility record is missing release")?;
    if release != env!("CARGO_PKG_VERSION") {
        return Err(format!(
            "application manifest framework release `{release}` does not match CLI release `{}`",
            env!("CARGO_PKG_VERSION")
        )
        .into());
    }
    Ok(())
}

fn validate_client_manifest_json(manifest: &serde_json::Value) -> Result<(), Box<dyn Error>> {
    let Some(version) = manifest
        .get("manifest_version")
        .and_then(serde_json::Value::as_u64)
    else {
        return Err("client manifest JSON is missing numeric manifest_version".into());
    };
    if version != DISTRIBUTED_CLIENT_MANIFEST_VERSION {
        return Err(format!(
            "unsupported Distributed client manifest version {version}; expected {DISTRIBUTED_CLIENT_MANIFEST_VERSION}"
        )
        .into());
    }
    if manifest
        .get("protocol_version")
        .and_then(serde_json::Value::as_u64)
        .is_none()
    {
        return Err("client manifest JSON is missing numeric protocol_version".into());
    }
    for field in ["service_id", "schema_fingerprint", "protocol_fingerprint"] {
        if manifest
            .get(field)
            .and_then(serde_json::Value::as_str)
            .is_none()
        {
            return Err(format!("client manifest JSON is missing string {field}").into());
        }
    }
    for field in ["surface", "capabilities"] {
        if manifest
            .get(field)
            .and_then(serde_json::Value::as_object)
            .is_none()
        {
            return Err(format!("client manifest JSON is missing object {field}").into());
        }
    }
    for field in ["scalar_codecs", "models", "roots", "commands", "projectors"] {
        if manifest
            .get(field)
            .and_then(serde_json::Value::as_array)
            .is_none()
        {
            return Err(format!("client manifest JSON is missing array {field}").into());
        }
    }
    Ok(())
}

pub(crate) fn resolve_distributed_path(
    provided: Option<&Path>,
    anchor: &Path,
) -> Result<PathBuf, Box<dyn Error>> {
    if let Some(path) = provided {
        return validate_distributed_path(path);
    }
    if let Ok(path) = std::env::var("DISTRIBUTED_PATH") {
        return validate_distributed_path(Path::new(&path));
    }

    let mut roots = Vec::new();
    roots.extend(anchor.ancestors().map(Path::to_path_buf));
    roots.extend(std::env::current_dir()?.ancestors().map(Path::to_path_buf));

    for root in roots {
        for candidate in [root.clone(), root.join("distributed")] {
            if candidate.join("Cargo.toml").exists()
                && cargo_toml_package_name(&candidate.join("Cargo.toml")).as_deref()
                    == Some("distributed")
            {
                return Ok(candidate.canonicalize()?);
            }
        }
    }

    Err("unable to find local Distributed crate; pass --distributed-path".into())
}

fn validate_distributed_path(path: &Path) -> Result<PathBuf, Box<dyn Error>> {
    let path = path.canonicalize()?;
    let manifest = path.join("Cargo.toml");
    if !manifest.exists() {
        return Err(format!("{} does not contain Cargo.toml", path.display()).into());
    }
    if cargo_toml_package_name(&manifest).as_deref() != Some("distributed") {
        return Err(format!("{} is not the Distributed crate", path.display()).into());
    }
    Ok(path)
}

fn cargo_toml_package_name(path: &Path) -> Option<String> {
    let contents = fs::read_to_string(path).ok()?;
    let mut in_package = false;
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed == "[package]" {
            in_package = true;
            continue;
        }
        if trimmed.starts_with('[') {
            in_package = false;
        }
        if in_package {
            if let Some(value) = trimmed.strip_prefix("name") {
                let value = value.trim_start();
                if let Some(value) = value.strip_prefix('=') {
                    return value.trim().trim_matches('"').to_string().into();
                }
            }
        }
    }
    None
}

fn absolute_path(path: &Path) -> Result<PathBuf, Box<dyn Error>> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn relative_path(from_dir: &Path, to: &Path) -> PathBuf {
    let from = path_components(from_dir);
    let to = path_components(to);
    let common = from
        .iter()
        .zip(to.iter())
        .take_while(|(left, right)| left == right)
        .count();
    let mut relative = PathBuf::new();
    for _ in common..from.len() {
        relative.push("..");
    }
    for component in &to[common..] {
        relative.push(component);
    }
    if relative.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        relative
    }
}

fn path_components(path: &Path) -> Vec<OsString> {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(value) => Some(value.to_os_string()),
            _ => None,
        })
        .collect()
}

pub(crate) fn path_for_toml(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema_args() -> SchemaArgs {
        SchemaArgs {
            path: PathBuf::from("."),
            manifest_path: None,
            package: None,
            features: Vec::new(),
            no_default_features: false,
            entrypoint: None,
            dialect: SchemaDialect::Postgres,
            format: SchemaFormat::Atlas,
            name: Some("orders".to_string()),
            namespace: None,
            db_secret: Some("orders-db".to_string()),
            db_secret_key: "url".to_string(),
            db_url: None,
            dev_url: None,
            out: None,
            distributed_path: None,
        }
    }

    #[test]
    fn write_decisions_cover_absent_identical_and_drift() {
        for force in [false, true] {
            assert_eq!(decide_write(None, "new", force), WriteAction::Created);
            assert_eq!(
                decide_write(Some("same"), "same", force),
                WriteAction::Unchanged
            );
        }
        assert_eq!(
            decide_write(Some("edited"), "new", false),
            WriteAction::Skipped
        );
        assert_eq!(
            decide_write(Some("edited"), "new", true),
            WriteAction::Updated
        );

        // Merged files converge without --force.
        assert_eq!(decide_managed_write(None, "new"), WriteAction::Created);
        assert_eq!(
            decide_managed_write(Some("same"), "same"),
            WriteAction::Unchanged
        );
        assert_eq!(
            decide_managed_write(Some("old"), "new"),
            WriteAction::Updated
        );
    }

    #[test]
    fn agent_wiring_maps_aliases_and_rejects_mixed_auto() {
        let anchor = Path::new("/nonexistent-anchor-for-wiring-test");
        // Aliases collapse onto the agents adapter; claude stays separate.
        for alias in [
            AgentHarness::Codex,
            AgentHarness::Grok,
            AgentHarness::Openai,
            AgentHarness::Gemini,
            AgentHarness::Pi,
            AgentHarness::Agents,
        ] {
            let wiring = resolve_agent_wiring(&[alias], anchor).unwrap();
            assert!(!wiring.claude);
            assert!(wiring.agents);
        }
        let wiring =
            resolve_agent_wiring(&[AgentHarness::Claude, AgentHarness::Codex], anchor).unwrap();
        assert!(wiring.claude && wiring.agents);

        let wiring = resolve_agent_wiring(&[AgentHarness::None], anchor).unwrap();
        assert!(!wiring.claude && !wiring.agents);

        // Auto on a project with no harness evidence wires both.
        let wiring = resolve_agent_wiring(&[AgentHarness::Auto], anchor).unwrap();
        assert!(wiring.claude && wiring.agents);

        assert!(resolve_agent_wiring(&[AgentHarness::Auto, AgentHarness::Claude], anchor).is_err());
        assert!(resolve_agent_wiring(&[AgentHarness::None, AgentHarness::Agents], anchor).is_err());
    }

    #[test]
    fn github_repo_create_args_are_private() {
        assert_eq!(
            github_repo_create_args("hops-ops/test-domain"),
            vec!["repo", "create", "hops-ops/test-domain", "--private"]
        );
    }

    #[test]
    fn client_manifest_validator_accepts_only_current_version() {
        let manifest = serde_json::json!({
            "manifest_version": DISTRIBUTED_CLIENT_MANIFEST_VERSION,
            "protocol_version": 1,
            "service_id": "orders",
            "schema_fingerprint": "sha256:schema",
            "protocol_fingerprint": "sha256:protocol",
            "surface": {},
            "capabilities": {},
            "scalar_codecs": [],
            "models": [],
            "roots": [],
            "commands": [],
            "projectors": [],
            "projection_programs": [],
            "projection_bindings": []
        });
        validate_client_manifest_json(&manifest).expect("current manifest version");

        let mut stale = manifest;
        stale["manifest_version"] = serde_json::json!(3);
        let error = validate_client_manifest_json(&stale)
            .expect_err("pre-release v3 manifests must be rejected");
        assert!(error.to_string().contains(&format!(
            "version 3; expected {DISTRIBUTED_CLIENT_MANIFEST_VERSION}"
        )));
    }

    #[test]
    fn project_generation_errors_preserve_the_failing_phase() {
        let harness_error = "manifest harness failed for package `client-surface`";
        assert!(is_application_introspection_failure(
            ProjectGenerationPhase::ApplicationPreflight,
            harness_error
        ));
        assert!(!is_application_introspection_failure(
            ProjectGenerationPhase::LifecycleBuild,
            harness_error
        ));
        assert!(is_application_introspection_failure(
            ProjectGenerationPhase::LifecycleBuild,
            "lifecycle node `application` executor failed"
        ));
    }

    #[test]
    fn client_generation_removes_owned_version_one_routes_artifact() {
        let fixture = tempfile::tempdir().expect("create generated client fixture");
        let root = fixture.path();
        fs::write(
            root.join("manifest.json"),
            r#"{"compiler_manifest_version":1,"operations":[]}"#,
        )
        .expect("write legacy manifest");
        fs::write(
            root.join("routes.ts"),
            "/** GENERATED framework-neutral `@load` ownership plan. */\nexport const DISTRIBUTED_ROUTES = [];\n",
        )
        .expect("write legacy routes");
        let current_manifest = "{\"compiler_manifest_version\":2,\"operations\":[]}\n".to_string();
        let project = GeneratedClientProject {
            files: vec![GeneratedClientFile {
                path: "manifest.json".to_string(),
                contents: current_manifest.clone(),
            }],
            operations: Vec::new(),
            islands: Vec::new(),
            schema_fingerprint: "sha256:schema".to_string(),
            protocol_fingerprint: "sha256:protocol".to_string(),
        };

        write_client_project(root, &project).expect("upgrade legacy generated client");

        assert!(!root.join("routes.ts").exists());
        assert_eq!(
            fs::read_to_string(root.join("manifest.json")).expect("read current manifest"),
            current_manifest
        );
    }

    #[test]
    fn optional_github_repo_reports_the_flag_on_error() {
        let err = parse_optional_github_repo(Some("missing-repo"), "--github")
            .expect_err("invalid repo should error");
        assert!(err.to_string().contains("--github"));
        assert!(parse_optional_github_repo(None, "--github")
            .unwrap()
            .is_none());
        let ok = parse_optional_github_repo(Some("hops-ops/test-domain"), "--github")
            .unwrap()
            .unwrap();
        assert_eq!(ok.slug(), "hops-ops/test-domain");
    }

    #[test]
    fn scaffold_help_lists_otel_alias() {
        let mut command = ScaffoldArgs::augment_args(clap::Command::new("scaffold"));
        let mut help = Vec::new();
        command.write_help(&mut help).unwrap();
        let help = String::from_utf8(help).unwrap();

        assert!(help.contains("--tracing"));
        assert!(help.contains("--otel"));
    }

    #[test]
    fn atlas_spec_uses_secret_ref_by_default() {
        let spec = atlas_spec_from_flags(&schema_args(), "CREATE TABLE orders (id text);".into())
            .expect("secret ref spec");
        assert_eq!(
            spec.database,
            AtlasDatabaseUrl::SecretKeyRef {
                name: "orders-db".to_string(),
                key: "url".to_string(),
            }
        );
        assert_eq!(spec.name, "orders");
    }

    #[test]
    fn atlas_inline_url_when_db_url_given() {
        let mut args = schema_args();
        args.db_secret = None;
        args.db_url = Some("postgres://localhost/orders".to_string());
        let spec = atlas_spec_from_flags(&args, "CREATE TABLE orders (id text);".into()).unwrap();
        assert_eq!(
            spec.database,
            AtlasDatabaseUrl::Inline("postgres://localhost/orders".to_string())
        );
    }

    #[test]
    fn atlas_requires_name_and_a_database() {
        let mut no_name = schema_args();
        no_name.name = None;
        assert!(atlas_spec_from_flags(&no_name, "sql".into()).is_err());

        let mut no_db = schema_args();
        no_db.db_secret = None;
        assert!(atlas_spec_from_flags(&no_db, "sql".into()).is_err());

        let mut both = schema_args();
        both.db_url = Some("postgres://localhost/orders".to_string());
        assert!(atlas_spec_from_flags(&both, "sql".into()).is_err());
    }
}
