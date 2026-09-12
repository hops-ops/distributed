//! The `distributed` CLI for Distributed applications — both a binary and a library.
//!
//! It bundles pure generation (`generate` / `atlas`), the contract lifecycle
//! surface (`contracts`), and the clap command surface (`cli`). The standalone
//! binary parses [`DistributedArgs`] and dispatches with [`run_distributed`].
//! Host CLIs such as `hops` may mount [`ServiceArgs`] and dispatch with [`run`].

mod atlas;
mod cli;
mod client_compiler;
pub mod contracts;
mod generate;
mod js_framework;
pub mod lifecycle;
mod manifest_harness;
mod skills;
mod wasm_pures;

pub use atlas::{render_atlas_schema, AtlasDatabaseUrl, AtlasSchemaSpec};
pub use cli::{
    run, run_distributed, AgentHarness, BuildArgs, Bus, ClientArgs, ClientManifestArgs,
    ContractsAcceptArgs, ContractsArgs, ContractsCheckArgs, ContractsCommands, ContractsOutput,
    DescribeArgs, DevArgs, DistributedArgs, DistributedCommands, Framework, GitopsPromote,
    JavascriptWatchArgs, LifecycleOutput, ManifestFormat, Metrics, ProbeArgs, ScaffoldArgs,
    SchemaArgs, SchemaDialect, SchemaFormat, ServiceArgs, ServiceCommands, SkillsArgs,
    SkillsCommands, SkillsInitArgs, Store, Transport,
};
pub use client_compiler::{
    compile_client, ClientCompileError, ClientCompileInput, ClientDocument, ClientSourceLocation,
    ClientSurfaceSelector, GeneratedClientFile, GeneratedClientProject, GeneratedIslandDirectives,
    GeneratedIslandLiveCoverage, GeneratedIslandPlan, GeneratedIslandSource,
    GeneratedIslandVariable, GeneratedIslandVariableSchema, GeneratedOperationSummary,
};
pub use contracts::{
    check_migration_history, check_migration_inventory, check_predecessor_chain,
    classify_release_programs, classify_snapshot_diff, close_local_contract_chain,
    contracts_accept, contracts_check, diff_snapshots, snapshot_from_json, ArtifactIdentity,
    ArtifactPredecessor, ArtifactProvenance, BaselineAvailability, ClassifiedChange,
    ClientDeclaration, ClientInventory, ClientProgramArtifact, ClientProgramAsset,
    ClientProgramDescriptor, ClientProgramSurface, ContractAcceptScope, ContractArtifactKind,
    ContractCatalog, ContractCheckResult, ContractDiagnostic, ContractDiagnosticCode,
    ContractEntry, ContractError, ContractScope, ContractsAcceptReport, ContractsCheckReport,
    EnvironmentPolicyReference, LifecycleDecision, MigrationDialect, MigrationEntry, MigrationFile,
    MigrationHistoryCheck, MigrationInventory, ObservedPredecessor, ProgramCompatibility,
    SafeDiagnosticValue, SemanticSnapshot, SnapshotChange, SnapshotDiff, SnapshotEntry,
    MAX_CATALOG_BYTES, MAX_CATALOG_DIRECTORIES, MAX_CATALOG_DIRECTORY_DEPTH,
    MAX_CATALOG_DIRECTORY_ENTRIES, MAX_CATALOG_ENTRIES, MAX_CATALOG_FILES,
    MAX_CATALOG_GLOB_MATCHES, MAX_CATALOG_JSON_DEPTH, MAX_MIGRATIONS,
    MAX_MIGRATION_INVENTORY_BYTES, MAX_MIGRATION_SQL_BYTES, MAX_SNAPSHOT_DEPTH, MAX_SNAPSHOT_PATHS,
    MAX_SNAPSHOT_VALUE_BYTES, MIGRATION_INVENTORY_PATH, MIGRATION_INVENTORY_SCHEMA_VERSION,
    MIGRATION_OWNER, MIGRATION_SCOPE,
};
pub use generate::{generate_service_scaffold, package_name};
pub use lifecycle::{
    run_lifecycle_build, run_lifecycle_dev, ArtifactNodeReceipt, BuildDrift,
    DistributedSourceIdentity, GenerationManifest, LifecycleBuildConfig, LifecycleBuildOptions,
    LifecycleBuildReport, LifecycleConfig, LifecycleDevConfig, LifecycleDevOptions,
    LifecycleDevProbe, LifecycleDevProcess, LifecycleDevReport, LifecycleError, LifecycleExecutor,
    LifecycleGraph, LifecycleNode, ReleaseManifest, ReleaseMember,
    GENERATION_MANIFEST_SCHEMA_VERSION, LIFECYCLE_BUILD_CONFIG_SCHEMA_VERSION,
    LIFECYCLE_CONFIG_SCHEMA_VERSION, LIFECYCLE_GRAPH_SCHEMA_VERSION, NODE_RECEIPT_SCHEMA_VERSION,
    RELEASE_MANIFEST_SCHEMA_VERSION,
};
pub use skills::{embedded_skills, generate_skills, EmbeddedFile, EmbeddedSkill, SkillsInitSpec};

/// What to scaffold. The pure input to [`generate_service_scaffold`].
///
/// `name` and the raw `models`/`commands`/`events` strings are normalized by the
/// generator (kebab/pascal/ident casing, validation, dedup) — that normalization
/// is part of the rules this crate owns.
#[derive(Clone, Debug)]
pub struct ServiceScaffoldSpec {
    /// Service / package name (free-form; normalized to a kebab package name).
    pub name: String,
    /// Runtime transport to scaffold.
    pub transport: ServiceTransport,
    /// Read-model / schema storage target.
    pub store: StoreTarget,
    /// Optional message bus backend.
    pub bus: Option<BusTarget>,
    /// Optional generated metrics integration.
    pub metrics: Option<MetricsTarget>,
    /// Aggregate model names to scaffold (raw; may be empty).
    pub models: Vec<String>,
    /// Generate placeholder read-model modules and register them in the manifest.
    pub read_models: bool,
    /// Generate `src/query/` GraphQL exposure skeleton + `graphql` feature wiring.
    pub query_api: bool,
    /// Enable Distributed's optional tracing span feature and GitOps OTLP env metadata.
    pub tracing: bool,
    /// Command handler message names (raw; empty → a default command is derived).
    pub commands: Vec<String>,
    /// Event handler message names (raw; may be empty).
    pub events: Vec<String>,
    /// Relative path (from the generated project dir) to the local `distributed`
    /// crate, used in the generated `Cargo.toml` dependency.
    pub distributed_dependency_path: String,
    /// Generate independent local and cloud workload charts under
    /// `.gitops/local` and `.gitops/deploy`.
    pub gitops: bool,
    /// Generate a GitOps promotion chart for Argo CD or Flux.
    pub gitops_promote: Option<GitopsPromoteTarget>,
    /// The service's own GitHub repository: emits the version/release workflows
    /// and an `EnsureGithubRepository` post-create action.
    pub github: Option<GithubRepo>,
    /// Preview-environment GitOps repository: emits the preview workflow and the
    /// `.gitops/preview/helm` promotion chart. Independent of `github`.
    pub github_preview: Option<GithubRepo>,
    /// Permanent-environment GitOps repository: emits the promote workflow and the
    /// `.gitops/promote/helm` promotion chart. Independent of `github`.
    pub github_promote: Option<GithubRepo>,
}

/// Runtime transport for the scaffolded service.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServiceTransport {
    /// Axum HTTP transport (`microsvc::serve`).
    Http,
    /// Knative / CloudEvents HTTP ingress (`cloud_events_router`).
    Knative,
}

/// Read-model / schema storage target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreTarget {
    /// Postgres-backed persistence (`postgres` feature).
    Postgres,
    /// SQLite-backed persistence (`sqlite` feature).
    Sqlite,
    /// In-memory only (no persistence feature).
    InMemory,
}

/// Message bus backend to scaffold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BusTarget {
    /// RabbitMQ.
    Rabbitmq,
    /// Kafka.
    Kafka,
    /// Postgres-backed bus.
    Psql,
    /// NATS JetStream.
    Nats,
}

impl BusTarget {
    /// The lowercase kind string used in generated env/manifest values.
    pub fn kind(self) -> &'static str {
        match self {
            BusTarget::Rabbitmq => "rabbitmq",
            BusTarget::Kafka => "kafka",
            BusTarget::Psql => "psql",
            BusTarget::Nats => "nats",
        }
    }
}

/// Metrics integration to scaffold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricsTarget {
    /// Prometheus text exposition and optional Prometheus Operator resources.
    Prometheus,
}

/// GitOps promotion flavor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GitopsPromoteTarget {
    /// Argo CD `Application`.
    Argo,
    /// Flux `HelmRelease`.
    Flux,
}

/// An `owner/repo` GitHub identifier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GithubRepo {
    /// Repository owner (user or org).
    pub owner: String,
    /// Repository name.
    pub repo: String,
}

impl GithubRepo {
    /// Parse an `owner/repo` string, validating both halves.
    pub fn parse(raw: &str) -> Result<Self, ScaffoldError> {
        generate::parse_github_repo(raw)
    }

    /// `owner/repo`.
    pub fn slug(&self) -> String {
        format!("{}/{}", self.owner, self.repo)
    }
}

/// The result of generating a scaffold: the files to write, advisory warnings,
/// and side effects for the caller to perform. Filesystem-agnostic.
#[derive(Clone, Debug, Default)]
pub struct GeneratedProject {
    /// Files to write, with paths relative to the project directory.
    pub files: Vec<GeneratedFile>,
    /// Non-fatal advisory messages (e.g. a requested feature not yet generated).
    pub warnings: Vec<String>,
    /// Side effects the caller should perform after writing files.
    pub post_create_actions: Vec<PostCreateAction>,
}

/// A single generated file: a relative path and its contents.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GeneratedFile {
    /// Path relative to the project directory (forward slashes).
    pub path: String,
    /// File contents. For [`FileMode::Symlink`] entries this is the link
    /// target (a relative path), not file data.
    pub contents: String,
    /// Optional file mode hint (e.g. executable). `None` = default text file.
    pub mode: Option<FileMode>,
}

/// File mode hint for a [`GeneratedFile`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileMode {
    /// The file should be marked executable.
    Executable,
    /// The entry is a symbolic link; `contents` holds the relative target.
    Symlink,
}

/// A side effect the caller should perform after writing the generated files.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PostCreateAction {
    /// Ensure the GitHub repository exists (e.g. `gh repo view` / `gh repo create`).
    EnsureGithubRepository {
        /// The repository to ensure.
        repo: GithubRepo,
    },
}

/// A scaffold generation error (bad spec value).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScaffoldError(pub String);

impl std::fmt::Display for ScaffoldError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ScaffoldError {}

impl ScaffoldError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}
