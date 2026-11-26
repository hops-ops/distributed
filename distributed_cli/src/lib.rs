//! The `distributed` CLI for Distributed applications — both a binary and a library.
//!
//! It bundles pure generation (`atlas`, `skills`), the build/dev lifecycle
//! (`lifecycle`, over the catalog records in `contracts`), and the clap
//! command surface (`cli`). The standalone binary parses [`DistributedArgs`]
//! and dispatches with [`run`]. A host CLI such as `hops` mounts the same
//! arguments under one of its commands and passes that command to [`run`].

mod atlas;
mod cli;
mod client_compiler;
pub mod contracts;
mod game_client;
mod game_wasm;
mod generated_dir;
#[cfg(test)]
#[path = "../tests/support/golden.rs"]
mod golden;
mod js_framework;
pub mod lifecycle;
mod manifest_harness;
mod process;
mod skills;
mod wasm_pures;

pub use atlas::{render_atlas_schema, AtlasDatabaseUrl, AtlasSchemaError, AtlasSchemaSpec};
pub use cli::{
    run, AgentHarness, BuildArgs, ClientArgs, ClientManifestArgs, DescribeArgs, DevArgs,
    DistributedArgs, DistributedCommands, GameClientArgs, GameManifestArgs, GameWasmArgs,
    JavascriptWatchArgs, LifecycleOutput, ManifestFormat, ProbeArgs, SchemaArgs, SchemaDialect,
    SchemaFormat, SkillsArgs, SkillsCommands, SkillsInitArgs,
};
pub use client_compiler::{
    compile_client, ClientCompileError, ClientCompileInput, ClientDocument, ClientSourceLocation,
    ClientSurfaceIdentity, DistributedClientManifest, GeneratedClientFile, GeneratedClientProject,
    GeneratedIslandDirectives, GeneratedIslandLiveCoverage, GeneratedIslandPlan,
    GeneratedIslandSource, GeneratedIslandVariable, GeneratedIslandVariableSchema,
    GeneratedOperationSummary,
};
pub use game_client::{generate_game_client, validate_game_surface, ImportExtension};
pub use lifecycle::{
    activate_lifecycle_project_generation, discover_lifecycle_project, run_lifecycle_project_build,
    run_lifecycle_project_dev, ArtifactNodeReceipt, BuildDrift, DiscoveredLifecycleProject,
    DistributedSourceIdentity, GenerationManifest, LifecycleActivation, LifecycleBuildConfig,
    LifecycleBuildReport, LifecycleBuildRequest, LifecycleConfig, LifecycleDevConfig,
    LifecycleDevProbe, LifecycleDevProcess, LifecycleDevReport, LifecycleError, LifecycleExecutor,
    LifecycleGraph, LifecycleNode, LifecycleProjectDevOptions, LifecycleProjectPlan,
    ReleaseManifest, ReleaseMember, UiAdapter, GENERATION_MANIFEST_SCHEMA_VERSION,
    LIFECYCLE_BUILD_CONFIG_SCHEMA_VERSION, LIFECYCLE_CONFIG_SCHEMA_VERSION,
    LIFECYCLE_GRAPH_SCHEMA_VERSION, NODE_RECEIPT_SCHEMA_VERSION, RELEASE_MANIFEST_SCHEMA_VERSION,
};
pub use skills::{embedded_skills, generate_skills, EmbeddedFile, EmbeddedSkill, SkillsInitSpec};

/// Generated files to write, plus advisory warnings. Filesystem-agnostic.
#[derive(Clone, Debug, Default)]
pub struct GeneratedProject {
    /// Files to write, with paths relative to the project directory.
    pub files: Vec<GeneratedFile>,
    /// Non-fatal advisory messages.
    pub warnings: Vec<String>,
}

/// A single generated file: a relative path and its contents.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GeneratedFile {
    /// Path relative to the project directory (forward slashes).
    pub path: String,
    /// File contents. For [`FileMode::Symlink`] entries this is the link
    /// target (a relative path), not file data.
    pub contents: String,
    /// Optional file mode hint. `None` = default text file.
    pub mode: Option<FileMode>,
}

/// File mode hint for a [`GeneratedFile`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileMode {
    /// The entry is a symbolic link; `contents` holds the relative target.
    Symlink,
}
