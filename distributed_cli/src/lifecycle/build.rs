use fs2::FileExt;
use glob::{glob_with, MatchOptions};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::contracts::ContractCatalog;

use super::graph::source_uses_output;
use super::{
    digest_bytes, validate_content_identity, validate_portable_path, validate_stable_value,
    ArtifactNodeReceipt, DistributedSourceIdentity, GenerationManifest, LifecycleConfig,
    LifecycleDevConfig, LifecycleError, LifecycleGraph, ReleaseManifest,
    LIFECYCLE_CONFIG_SCHEMA_VERSION,
};

pub const LIFECYCLE_BUILD_CONFIG_SCHEMA_VERSION: u32 = 1;
const MAX_EXECUTORS: usize = 256;
const MAX_EXECUTOR_ARGS: usize = 256;
const MAX_EXECUTOR_ARG_BYTES: usize = 16 * 1024;
const MAX_HASHED_FILES: usize = 8192;
const MAX_HASHED_FILE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_HASHED_TOTAL_BYTES: u64 = 256 * 1024 * 1024;
const MAX_HASH_DEPTH: usize = 64;
const MAX_EXECUTOR_STDERR_BYTES: usize = 64 * 1024;
const MAX_LOCK_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleExecutor {
    /// Stable tool identity (for example a compiler version plus config digest).
    pub identity: String,
    /// Executable name or path. Commands are invoked directly, never by a shell.
    pub program: String,
    /// Argument vector. `{root}`, `{stage}`, and `{node}` are expanded per invocation.
    #[serde(default)]
    pub args: Vec<String>,
    /// Optional declared node output that receives the executor's stdout bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout: Option<String>,
}

impl LifecycleExecutor {
    fn validate(&self, name: &str) -> Result<(), LifecycleError> {
        validate_stable_value(name, "lifecycle executor name")?;
        validate_content_identity(&self.identity, "lifecycle executor identity")?;
        validate_stable_value(&self.program, "lifecycle executor program")?;
        if self.args.len() > MAX_EXECUTOR_ARGS
            || self.args.iter().map(String::len).sum::<usize>() > MAX_EXECUTOR_ARG_BYTES
        {
            return Err(LifecycleError::new(format!(
                "executor `{name}` exceeds argument bounds"
            )));
        }
        if self.args.iter().any(|arg| arg.contains('\0')) {
            return Err(LifecycleError::new(format!(
                "executor `{name}` contains a NUL argument"
            )));
        }
        if let Some(stdout) = &self.stdout {
            validate_portable_path(stdout, "lifecycle executor stdout")?;
        }
        Ok(())
    }
}

/// Path/target-only lifecycle selection plus explicit native command adapters.
/// Artifact membership remains owned by the contract catalog.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleBuildConfig {
    pub schema_version: u32,
    pub application: String,
    pub source: DistributedSourceIdentity,
    pub roots: BTreeSet<String>,
    pub executors: BTreeMap<String, LifecycleExecutor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dev: Option<LifecycleDevConfig>,
}

impl LifecycleBuildConfig {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, LifecycleError> {
        let metadata = fs::symlink_metadata(path.as_ref()).map_err(|error| {
            LifecycleError::new(format!(
                "failed to inspect lifecycle config `{}`: {error}",
                path.as_ref().display()
            ))
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(LifecycleError::new(
                "lifecycle config must be a regular non-symlink file",
            ));
        }
        if metadata.len() > 1024 * 1024 {
            return Err(LifecycleError::new("lifecycle config exceeds 1 MiB"));
        }
        let bytes = fs::read(path.as_ref()).map_err(|error| {
            LifecycleError::new(format!(
                "failed to read lifecycle config `{}`: {error}",
                path.as_ref().display()
            ))
        })?;
        let config: Self = serde_json::from_slice(&bytes).map_err(|error| {
            LifecycleError::new(format!(
                "failed to parse lifecycle config `{}`: {error}",
                path.as_ref().display()
            ))
        })?;
        config.validate()?;
        Ok(config)
    }

    pub fn lifecycle(&self) -> LifecycleConfig {
        LifecycleConfig {
            schema_version: LIFECYCLE_CONFIG_SCHEMA_VERSION,
            application: self.application.clone(),
            source: self.source.clone(),
            roots: self.roots.clone(),
        }
    }

    pub fn validate(&self) -> Result<(), LifecycleError> {
        if self.schema_version != LIFECYCLE_BUILD_CONFIG_SCHEMA_VERSION {
            return Err(LifecycleError::new(format!(
                "unsupported lifecycle build config schema version {}; expected {}",
                self.schema_version, LIFECYCLE_BUILD_CONFIG_SCHEMA_VERSION
            )));
        }
        self.lifecycle().validate()?;
        if self.executors.is_empty() || self.executors.len() > MAX_EXECUTORS {
            return Err(LifecycleError::new(format!(
                "lifecycle executors must contain 1..={MAX_EXECUTORS} entries"
            )));
        }
        for (name, executor) in &self.executors {
            executor.validate(name)?;
        }
        if let Some(dev) = &self.dev {
            dev.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct LifecycleBuildOptions {
    pub root: PathBuf,
    pub catalog: PathBuf,
    pub config: PathBuf,
    pub out: PathBuf,
    pub check: bool,
    pub lock_timeout: Duration,
    /// Optional downstream-closed node subset; other receipts are verified and reused.
    pub nodes: Option<BTreeSet<String>>,
    /// Optional content snapshot that must still match immediately before activation.
    pub activation_inputs: Option<BTreeMap<String, String>>,
    /// Optional cooperative cancellation shared with a supervising watcher.
    pub cancel: Option<Arc<AtomicBool>>,
}

/// A lifecycle graph resolved from project metadata rather than user-authored
/// lifecycle files.
#[derive(Clone, Debug)]
pub struct LifecycleProjectPlan {
    /// Canonical lifecycle root shared by every owned application component.
    pub root: PathBuf,
    /// Tool-derived ownership catalog for generated application artifacts.
    pub catalog: ContractCatalog,
    /// Tool-derived executors, roots, source identity, and dev processes.
    pub config: LifecycleBuildConfig,
    /// Project-relative or absolute content-addressed lifecycle state directory.
    pub out: PathBuf,
}

/// Per-invocation behavior for an already resolved lifecycle project.
#[derive(Clone, Debug)]
pub struct LifecycleBuildRequest {
    /// Build in an isolated stage and report drift without activation.
    pub check: bool,
    /// Existing output set against which check-mode outputs are compared.
    pub check_baseline: LifecycleCheckBaseline,
    /// Maximum time to wait for a concurrent lifecycle build.
    pub lock_timeout: Duration,
    /// Optional downstream-closed node subset for incremental rebuilds.
    pub nodes: Option<BTreeSet<String>>,
    /// Optional source snapshot that must still match before activation.
    pub activation_inputs: Option<BTreeMap<String, String>>,
    /// Optional cooperative cancellation shared with a supervisor.
    pub cancel: Option<Arc<AtomicBool>>,
    /// Whether this invocation may move the active generation pointer.
    pub activation: LifecycleActivation,
}

/// Activation policy for one coherent build.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleActivation {
    /// Install the immutable generation and atomically make it active.
    Immediate,
    /// Install the immutable generation but leave the current pointer untouched.
    /// The dev supervisor uses this while it proves replacement processes ready.
    Deferred,
}

/// Output set used as the baseline for a read-only lifecycle check.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleCheckBaseline {
    /// Compare staged outputs with catalog-owned files in the source workspace.
    Workspace,
    /// Compare staged outputs with the currently active immutable generation.
    ActiveGeneration,
}

impl From<&LifecycleBuildOptions> for LifecycleBuildRequest {
    fn from(options: &LifecycleBuildOptions) -> Self {
        Self {
            check: options.check,
            check_baseline: LifecycleCheckBaseline::Workspace,
            lock_timeout: options.lock_timeout,
            nodes: options.nodes.clone(),
            activation_inputs: options.activation_inputs.clone(),
            cancel: options.cancel.clone(),
            activation: LifecycleActivation::Immediate,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BuildDrift {
    pub node_id: String,
    pub output: String,
    pub built_identity: String,
    pub workspace_identity: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleBuildReport {
    pub ok: bool,
    pub check: bool,
    pub graph_id: String,
    pub generation_id: String,
    pub release_id: String,
    /// Identity of generated public artifacts, independent of source-only rebuilds.
    pub compatibility_id: String,
    pub order: Vec<String>,
    pub executed: Vec<String>,
    pub drift: Vec<BuildDrift>,
    pub active_generation: Option<String>,
}

pub fn run_lifecycle_build(
    options: &LifecycleBuildOptions,
) -> Result<LifecycleBuildReport, LifecycleError> {
    let root = options.root.canonicalize().map_err(|error| {
        LifecycleError::new(format!(
            "failed to resolve lifecycle root `{}`: {error}",
            options.root.display()
        ))
    })?;
    if !root.is_dir() {
        return Err(LifecycleError::new("lifecycle root must be a directory"));
    }
    let catalog_path = resolve_under_root(&root, &options.catalog, "contract catalog")?;
    let config_path = resolve_under_root(&root, &options.config, "lifecycle config")?;
    let config = LifecycleBuildConfig::from_path(config_path)?;
    let catalog = ContractCatalog::from_path_at_root(&catalog_path, &root)
        .map_err(|error| LifecycleError::new(error.to_string()))?;
    run_lifecycle_project_build(
        &LifecycleProjectPlan {
            root,
            catalog,
            config,
            out: options.out.clone(),
        },
        &LifecycleBuildRequest::from(options),
    )
}

/// Build a lifecycle project whose catalog and execution plan were derived by
/// the CLI from Cargo metadata and project conventions.
pub fn run_lifecycle_project_build(
    project: &LifecycleProjectPlan,
    request: &LifecycleBuildRequest,
) -> Result<LifecycleBuildReport, LifecycleError> {
    if request.lock_timeout.is_zero() || request.lock_timeout > MAX_LOCK_TIMEOUT {
        return Err(LifecycleError::new(
            "lifecycle lock timeout must be within 1ms..=60s",
        ));
    }
    let root = project.root.canonicalize().map_err(|error| {
        LifecycleError::new(format!(
            "failed to resolve lifecycle root `{}`: {error}",
            project.root.display()
        ))
    })?;
    if !root.is_dir() {
        return Err(LifecycleError::new("lifecycle root must be a directory"));
    }
    let config = &project.config;
    config.validate()?;
    let catalog = &project.catalog;
    let graph = LifecycleGraph::from_catalog(catalog, &config.lifecycle())?;
    validate_executor_coverage(&graph, config)?;

    let out = resolve_output(&root, &project.out)?;
    let out_parent = out
        .parent()
        .ok_or_else(|| LifecycleError::new("lifecycle output must have a parent directory"))?;
    if !request.check {
        fs::create_dir_all(out_parent).map_err(io_error("create lifecycle output parent"))?;
    }
    let _lock = if out_parent.exists() {
        Some(BuildLock::acquire(&root, out_parent, request.lock_timeout)?)
    } else {
        None
    };
    let selected = selected_nodes(request.nodes.as_ref(), &graph)?;
    let previous = if selected.len() == graph.nodes.len() {
        None
    } else {
        Some(load_active_generation(&out, &graph)?)
    };
    let stage_parent = if request.check {
        None
    } else {
        Some(out_parent)
    };
    let stage = match stage_parent {
        Some(parent) => tempfile::Builder::new()
            .prefix(".distributed-stage-")
            .tempdir_in(parent),
        None => tempfile::Builder::new()
            .prefix("distributed-check-")
            .tempdir(),
    }
    .map_err(io_error("create isolated lifecycle stage"))?;

    let order = graph.topological_order()?;
    let mut executed = Vec::new();
    let mut receipts = BTreeMap::new();
    for node_id in &order {
        if request
            .cancel
            .as_ref()
            .is_some_and(|cancel| cancel.load(Ordering::SeqCst))
        {
            return Err(LifecycleError::canceled("lifecycle build was canceled"));
        }
        let node = &graph.nodes[node_id];
        if !selected.contains(node_id) {
            let (previous_generation, previous_root) = previous.as_ref().unwrap();
            let receipt = previous_generation.receipts[node_id].clone();
            receipt.validate_against(node)?;
            for (output, expected_identity) in &receipt.output_identities {
                let source = previous_root.join(output);
                let source_identity = hash_path(&source, previous_root)?;
                if &source_identity != expected_identity {
                    return Err(LifecycleError::new(format!(
                        "active generation output `{output}` for node `{node_id}` disagrees with its receipt"
                    )));
                }
                copy_path(&source, &stage.path().join(output))?;
                let observed = hash_path(&stage.path().join(output), stage.path())?;
                if &observed != expected_identity {
                    return Err(LifecycleError::new(format!(
                        "active generation output `{output}` for node `{node_id}` disagrees with its receipt"
                    )));
                }
            }
            receipts.insert(node_id.clone(), receipt);
            continue;
        }
        let executor = &config.executors[&node.executor];
        let input_identities = collect_node_inputs(&root, stage.path(), catalog, &graph, node_id)?;
        let dependency_receipts = node
            .dependencies
            .iter()
            .map(|dependency| {
                let receipt: &ArtifactNodeReceipt = &receipts[dependency];
                (dependency.clone(), receipt.receipt_id.clone())
            })
            .collect();
        execute_node(
            &root,
            stage.path(),
            node_id,
            &node.outputs,
            executor,
            request.check,
            request.cancel.as_deref(),
        )?;
        let output_identities = collect_node_outputs(stage.path(), node_id, &graph)?;
        let receipt = ArtifactNodeReceipt::new(
            node,
            executor.identity.clone(),
            input_identities,
            dependency_receipts,
            output_identities,
            true,
        )?;
        receipts.insert(node_id.clone(), receipt);
        executed.push(node_id.clone());
    }
    reject_unowned_stage_files(stage.path(), &graph)?;
    let generation = GenerationManifest::new(&graph, receipts.into_values())?;
    let release = ReleaseManifest::new(&graph, &generation)?;
    let compatibility_bytes = serde_json::to_vec(
        &generation
            .receipts
            .values()
            .map(|receipt| (&receipt.node_id, &receipt.output_identities))
            .collect::<Vec<_>>(),
    )
    .map_err(|error| LifecycleError::new(format!("serialize compatibility identity: {error}")))?;
    let compatibility_id = digest_bytes(&compatibility_bytes);

    let drift = if request.check {
        match request.check_baseline {
            LifecycleCheckBaseline::Workspace => compare_workspace_outputs(&root, &generation)?,
            LifecycleCheckBaseline::ActiveGeneration => compare_active_outputs(&out, &generation)?,
        }
    } else {
        if request
            .cancel
            .as_ref()
            .is_some_and(|cancel| cancel.load(Ordering::SeqCst))
        {
            return Err(LifecycleError::canceled("lifecycle build was canceled"));
        }
        Vec::new()
    };
    let active_generation = if request.check {
        read_active_generation(&out)?
    } else {
        if let Some(expected) = &request.activation_inputs {
            let observed = lifecycle_input_snapshot(&root, catalog, &graph)?;
            if &observed != expected {
                return Err(LifecycleError::superseded(
                    "lifecycle build was superseded by newer input content before activation",
                ));
            }
        }
        write_manifest(
            stage.path(),
            "generation.json",
            &generation.canonical_bytes(&graph)?,
        )?;
        write_manifest(
            stage.path(),
            "release.json",
            &release.canonical_bytes(&graph, &generation)?,
        )?;
        install_generation(stage, &out, &generation.generation_id)?;
        if request.activation == LifecycleActivation::Immediate {
            write_active_generation(&out, &generation.generation_id, &release.release_id)?;
            Some(generation.generation_id.clone())
        } else {
            read_active_generation(&out)?
        }
    };

    Ok(LifecycleBuildReport {
        ok: !request.check || drift.is_empty(),
        check: request.check,
        graph_id: graph.graph_id,
        generation_id: generation.generation_id,
        release_id: release.release_id,
        compatibility_id,
        order,
        executed,
        drift,
        active_generation,
    })
}

fn compare_active_outputs(
    out: &Path,
    generation: &GenerationManifest,
) -> Result<Vec<BuildDrift>, LifecycleError> {
    let active = read_active_generation(out)?;
    let active_root = active
        .as_ref()
        .map(|identity| out.join("generations").join(identity));
    let mut drift = Vec::new();
    for receipt in generation.receipts.values() {
        for (output, built_identity) in &receipt.output_identities {
            let active_identity = match &active_root {
                Some(root) => match fs::symlink_metadata(root.join(output)) {
                    Ok(_) => Some(hash_path(&root.join(output), root)?),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                    Err(error) => return Err(io_error("inspect active generation output")(error)),
                },
                None => None,
            };
            if active_identity.as_ref() != Some(built_identity) {
                drift.push(BuildDrift {
                    node_id: receipt.node_id.clone(),
                    output: output.clone(),
                    built_identity: built_identity.clone(),
                    workspace_identity: active_identity,
                });
            }
        }
    }
    Ok(drift)
}

fn selected_nodes(
    requested: Option<&BTreeSet<String>>,
    graph: &LifecycleGraph,
) -> Result<BTreeSet<String>, LifecycleError> {
    let selected = requested
        .cloned()
        .unwrap_or_else(|| graph.nodes.keys().cloned().collect());
    if selected.is_empty() {
        return Err(LifecycleError::new(
            "lifecycle build node selection must not be empty",
        ));
    }
    let unknown = selected
        .iter()
        .filter(|node| !graph.nodes.contains_key(*node))
        .cloned()
        .collect::<Vec<_>>();
    if !unknown.is_empty() {
        return Err(LifecycleError::new(format!(
            "lifecycle build selects unknown nodes: {}",
            unknown.join(", ")
        )));
    }
    for (node_id, node) in &graph.nodes {
        if !selected.contains(node_id)
            && node
                .dependencies
                .iter()
                .any(|dependency| selected.contains(dependency))
        {
            return Err(LifecycleError::new(format!(
                "lifecycle build selection is not downstream-closed: `{node_id}` depends on a selected node"
            )));
        }
    }
    Ok(selected)
}

fn load_active_generation(
    out: &Path,
    graph: &LifecycleGraph,
) -> Result<(GenerationManifest, PathBuf), LifecycleError> {
    let generation_id = read_active_generation(out)?.ok_or_else(|| {
        LifecycleError::new("partial lifecycle build requires an active complete generation")
    })?;
    validate_content_identity(&generation_id, "active generation identity")?;
    let root = out.join("generations").join(&generation_id);
    reject_symlink_components(out, &root, "active generation")?;
    let manifest_path = root.join("generation.json");
    let metadata = fs::symlink_metadata(&manifest_path)
        .map_err(io_error("inspect active generation manifest"))?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > 16 * 1024 * 1024
    {
        return Err(LifecycleError::new(
            "active generation manifest is missing, linked, or oversized",
        ));
    }
    let bytes = fs::read(&manifest_path).map_err(io_error("read active generation manifest"))?;
    let manifest: GenerationManifest = serde_json::from_slice(&bytes).map_err(|error| {
        LifecycleError::new(format!("invalid active generation manifest: {error}"))
    })?;
    manifest.validate_against(graph)?;
    if manifest.generation_id != generation_id {
        return Err(LifecycleError::new(
            "active generation pointer and manifest identity disagree",
        ));
    }
    Ok((manifest, root))
}

fn copy_path(source: &Path, target: &Path) -> Result<(), LifecycleError> {
    let metadata = fs::symlink_metadata(source).map_err(io_error("inspect reusable output"))?;
    if metadata.file_type().is_symlink() {
        return Err(LifecycleError::new(format!(
            "reusable output `{}` must not be a symlink",
            source.display()
        )));
    }
    if metadata.is_file() {
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(io_error("create reusable output parent"))?;
        }
        fs::copy(source, target).map_err(io_error("copy reusable output"))?;
        return Ok(());
    }
    if !metadata.is_dir() {
        return Err(LifecycleError::new("reusable output is not a regular path"));
    }
    fs::create_dir_all(target).map_err(io_error("create reusable output directory"))?;
    let mut entries = fs::read_dir(source)
        .map_err(io_error("read reusable output directory"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(io_error("read reusable output entry"))?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        copy_path(&entry.path(), &target.join(entry.file_name()))?;
    }
    Ok(())
}

fn validate_executor_coverage(
    graph: &LifecycleGraph,
    config: &LifecycleBuildConfig,
) -> Result<(), LifecycleError> {
    let required = graph
        .nodes
        .values()
        .map(|node| node.executor.clone())
        .collect::<BTreeSet<_>>();
    let missing = required
        .difference(&config.executors.keys().cloned().collect())
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(LifecycleError::new(format!(
            "lifecycle config is missing executors: {}",
            missing.join(", ")
        )));
    }
    Ok(())
}

fn execute_node(
    root: &Path,
    stage: &Path,
    node_id: &str,
    declared_outputs: &BTreeSet<String>,
    executor: &LifecycleExecutor,
    check: bool,
    cancel: Option<&AtomicBool>,
) -> Result<(), LifecycleError> {
    let root_value = root.to_string_lossy();
    let stage_value = stage.to_string_lossy();
    let args = executor
        .args
        .iter()
        .map(|arg| {
            arg.replace("{root}", &root_value)
                .replace("{stage}", &stage_value)
                .replace("{node}", node_id)
        })
        .collect::<Vec<_>>();
    let stdout = if let Some(stdout) = &executor.stdout {
        if !declared_outputs.contains(stdout) {
            return Err(LifecycleError::new(format!(
                "lifecycle node `{node_id}` executor stdout `{stdout}` is not a declared node output"
            )));
        }
        let path = stage.join(stdout);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(io_error("create executor stdout parent"))?;
        }
        Stdio::from(File::create(path).map_err(io_error("create executor stdout output"))?)
    } else {
        Stdio::null()
    };
    let mut command = Command::new(&executor.program);
    command
        .args(args)
        .current_dir(stage)
        .env("DISTRIBUTED_LIFECYCLE_ROOT", root)
        .env("DISTRIBUTED_LIFECYCLE_STAGE", stage)
        .env("DISTRIBUTED_LIFECYCLE_NODE", node_id)
        .env("DISTRIBUTED_LIFECYCLE_CHECK", if check { "1" } else { "0" })
        .stdout(stdout)
        .stderr(Stdio::piped());
    let mut child = command.spawn().map_err(|error| {
        LifecycleError::new(format!(
            "lifecycle node `{node_id}` failed to start executor `{}`: {error}",
            executor.program
        ))
    })?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| LifecycleError::new("lifecycle executor stderr pipe is unavailable"))?;
    let stderr_reader = match std::thread::Builder::new()
        .name(format!("lifecycle-stderr-{node_id}"))
        .spawn(move || forward_executor_stderr(stderr))
    {
        Ok(reader) => reader,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(io_error("start lifecycle executor stderr reader")(error));
        }
    };
    let status = loop {
        if cancel.is_some_and(|cancel| cancel.load(Ordering::SeqCst)) {
            child
                .kill()
                .map_err(io_error("cancel lifecycle executor"))?;
            child
                .wait()
                .map_err(io_error("wait for canceled lifecycle executor"))?;
            let _ = finish_executor_stderr(stderr_reader);
            return Err(LifecycleError::canceled(format!(
                "lifecycle build was canceled while node `{node_id}` was running"
            )));
        }
        if let Some(status) = child
            .try_wait()
            .map_err(io_error("inspect lifecycle executor"))?
        {
            break status;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let stderr_tail = finish_executor_stderr(stderr_reader)?;
    if !status.success() {
        let detail = String::from_utf8_lossy(&stderr_tail);
        let detail = detail.trim();
        return Err(LifecycleError::new(if detail.is_empty() {
            format!("lifecycle node `{node_id}` executor failed with {status}")
        } else {
            format!(
                "lifecycle node `{node_id}` executor failed with {status}; stderr tail:\n{detail}"
            )
        }));
    }
    Ok(())
}

fn collect_node_inputs(
    root: &Path,
    stage: &Path,
    catalog: &ContractCatalog,
    graph: &LifecycleGraph,
    node_id: &str,
) -> Result<BTreeMap<String, String>, LifecycleError> {
    let node = &graph.nodes[node_id];
    let entry = &catalog.entries[node_id];
    let dependency_outputs = node
        .dependencies
        .iter()
        .flat_map(|dependency| graph.nodes[dependency].outputs.iter())
        .collect::<Vec<_>>();
    let mut identities = BTreeMap::new();
    for source in &node.inputs {
        let base = if dependency_outputs
            .iter()
            .any(|output| source_uses_output(source, output))
        {
            stage
        } else {
            root
        };
        for (path, identity) in hash_source(base, source, entry.provenance.glob_limit)? {
            if identities.insert(path.clone(), identity).is_some() {
                return Err(LifecycleError::new(format!(
                    "lifecycle node `{node_id}` resolves duplicate input `{path}`"
                )));
            }
        }
    }
    if identities.is_empty() {
        return Err(LifecycleError::new(format!(
            "lifecycle node `{node_id}` resolved no inputs"
        )));
    }
    Ok(identities)
}

fn collect_node_outputs(
    stage: &Path,
    node_id: &str,
    graph: &LifecycleGraph,
) -> Result<BTreeMap<String, String>, LifecycleError> {
    graph.nodes[node_id]
        .outputs
        .iter()
        .map(|output| {
            let identity = hash_path(&stage.join(output), stage)?;
            Ok((output.clone(), identity))
        })
        .collect()
}

fn hash_source(
    base: &Path,
    source: &str,
    glob_limit: Option<usize>,
) -> Result<BTreeMap<String, String>, LifecycleError> {
    validate_portable_path(source, "lifecycle source")?;
    if !source.contains(['*', '?', '[']) {
        let path = base.join(source);
        return Ok(BTreeMap::from([(
            source.to_string(),
            hash_path(&path, base)?,
        )]));
    }
    let limit = glob_limit.ok_or_else(|| {
        LifecycleError::new(format!(
            "lifecycle source glob `{source}` has no match limit"
        ))
    })?;
    let pattern = base.join(source).to_string_lossy().into_owned();
    let matches = glob_with(
        &pattern,
        MatchOptions {
            case_sensitive: true,
            require_literal_separator: true,
            require_literal_leading_dot: true,
        },
    )
    .map_err(|error| LifecycleError::new(format!("invalid lifecycle glob: {error}")))?;
    let mut identities = BTreeMap::new();
    for result in matches {
        let path = result.map_err(|error| LifecycleError::new(error.to_string()))?;
        if identities.len() >= limit || identities.len() >= MAX_HASHED_FILES {
            return Err(LifecycleError::new(format!(
                "lifecycle source glob `{source}` exceeds its match limit"
            )));
        }
        let relative = portable_relative(base, &path)?;
        identities.insert(relative, hash_path(&path, base)?);
    }
    if identities.is_empty() {
        return Err(LifecycleError::new(format!(
            "lifecycle source glob `{source}` matched no files"
        )));
    }
    Ok(identities)
}

pub(super) fn lifecycle_input_snapshot(
    root: &Path,
    catalog: &ContractCatalog,
    graph: &LifecycleGraph,
) -> Result<BTreeMap<String, String>, LifecycleError> {
    let generated = graph
        .nodes
        .values()
        .flat_map(|node| node.outputs.iter())
        .collect::<Vec<_>>();
    let mut snapshot = BTreeMap::new();
    for (node_id, node) in &graph.nodes {
        let limit = catalog.entries[node_id].provenance.glob_limit;
        for source in &node.inputs {
            if generated
                .iter()
                .any(|output| source_uses_output(source, output))
            {
                continue;
            }
            for (path, identity) in hash_source(root, source, limit)? {
                snapshot.insert(path, identity);
            }
        }
    }
    Ok(snapshot)
}

fn hash_path(path: &Path, base: &Path) -> Result<String, LifecycleError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        LifecycleError::new(format!(
            "required lifecycle path `{}` is unavailable: {error}",
            path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() {
        return Err(LifecycleError::new(format!(
            "lifecycle path `{}` must not be a symlink",
            path.display()
        )));
    }
    if metadata.is_file() {
        if metadata.len() > MAX_HASHED_FILE_BYTES {
            return Err(LifecycleError::new(format!(
                "lifecycle file `{}` exceeds the 64 MiB hashing bound",
                path.display()
            )));
        }
        let mut file = File::open(path).map_err(io_error("open lifecycle file"))?;
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.read_to_end(&mut bytes)
            .map_err(io_error("read lifecycle file"))?;
        return Ok(digest_bytes(&bytes));
    }
    if !metadata.is_dir() {
        return Err(LifecycleError::new(format!(
            "lifecycle path `{}` is not a regular file or directory",
            path.display()
        )));
    }
    let mut files = Vec::new();
    let mut total_bytes = 0;
    collect_regular_files(path, &mut files, &mut total_bytes, 0)?;
    if files.len() > MAX_HASHED_FILES {
        return Err(LifecycleError::new(format!(
            "lifecycle directory `{}` exceeds the file bound",
            path.display()
        )));
    }
    let mut identities = BTreeMap::new();
    for file in files {
        let relative = portable_relative(base, &file)?;
        identities.insert(relative, hash_path(&file, base)?);
    }
    let bytes =
        serde_json::to_vec(&identities).map_err(|error| LifecycleError::new(error.to_string()))?;
    Ok(digest_bytes(&bytes))
}

fn collect_regular_files(
    path: &Path,
    files: &mut Vec<PathBuf>,
    total_bytes: &mut u64,
    depth: usize,
) -> Result<(), LifecycleError> {
    if depth > MAX_HASH_DEPTH {
        return Err(LifecycleError::new(format!(
            "lifecycle directory `{}` exceeds the depth bound",
            path.display()
        )));
    }
    let mut entries = fs::read_dir(path)
        .map_err(io_error("read lifecycle directory"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(io_error("read lifecycle directory entry"))?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let file_type = entry
            .file_type()
            .map_err(io_error("inspect lifecycle entry"))?;
        if file_type.is_symlink() {
            return Err(LifecycleError::new(format!(
                "lifecycle path `{}` must not contain symlinks",
                entry.path().display()
            )));
        }
        if file_type.is_dir() {
            collect_regular_files(&entry.path(), files, total_bytes, depth + 1)?;
        } else if file_type.is_file() {
            *total_bytes = total_bytes.saturating_add(
                entry
                    .metadata()
                    .map_err(io_error("inspect lifecycle file"))?
                    .len(),
            );
            if *total_bytes > MAX_HASHED_TOTAL_BYTES {
                return Err(LifecycleError::new(format!(
                    "lifecycle directory `{}` exceeds the total byte bound",
                    path.display()
                )));
            }
            files.push(entry.path());
        } else {
            return Err(LifecycleError::new(format!(
                "lifecycle path `{}` contains a special file",
                entry.path().display()
            )));
        }
        if files.len() > MAX_HASHED_FILES {
            return Err(LifecycleError::new(format!(
                "lifecycle directory `{}` exceeds the file bound",
                path.display()
            )));
        }
    }
    Ok(())
}

fn compare_workspace_outputs(
    root: &Path,
    generation: &GenerationManifest,
) -> Result<Vec<BuildDrift>, LifecycleError> {
    let mut drift = Vec::new();
    for (node_id, receipt) in &generation.receipts {
        for (output, built_identity) in &receipt.output_identities {
            let workspace_identity = match fs::symlink_metadata(root.join(output)) {
                Ok(_) => Some(hash_path(&root.join(output), root)?),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(io_error("inspect workspace output")(error)),
            };
            if workspace_identity.as_ref() != Some(built_identity) {
                drift.push(BuildDrift {
                    node_id: node_id.clone(),
                    output: output.clone(),
                    built_identity: built_identity.clone(),
                    workspace_identity,
                });
            }
        }
    }
    Ok(drift)
}

fn reject_unowned_stage_files(stage: &Path, graph: &LifecycleGraph) -> Result<(), LifecycleError> {
    let outputs = graph
        .nodes
        .values()
        .flat_map(|node| node.outputs.iter())
        .map(Path::new)
        .collect::<Vec<_>>();
    let mut files = Vec::new();
    let mut total_bytes = 0;
    collect_regular_files(stage, &mut files, &mut total_bytes, 0)?;
    for file in files {
        let relative = portable_relative(stage, &file)?;
        let relative_path = Path::new(&relative);
        if !outputs
            .iter()
            .any(|output| relative_path == *output || relative_path.starts_with(output))
        {
            return Err(LifecycleError::new(format!(
                "executor produced unowned staged output `{relative}`"
            )));
        }
    }
    Ok(())
}

fn install_generation(
    stage: tempfile::TempDir,
    out: &Path,
    generation_id: &str,
) -> Result<(), LifecycleError> {
    let generations = out.join("generations");
    fs::create_dir_all(&generations).map_err(io_error("create generations directory"))?;
    let target = generations.join(generation_id);
    if target.exists() {
        let staged_identity = hash_path(stage.path(), stage.path())?;
        let active_identity = hash_path(&target, &target)?;
        if staged_identity != active_identity {
            return Err(LifecycleError::new(
                "existing generation directory disagrees with its content identity",
            ));
        }
    } else {
        fs::rename(stage.path(), &target).map_err(|error| {
            LifecycleError::new(format!(
                "failed to atomically install generation `{generation_id}`: {error}"
            ))
        })?;
    }
    Ok(())
}

fn write_active_generation(
    out: &Path,
    generation_id: &str,
    release_id: &str,
) -> Result<(), LifecycleError> {
    let active = serde_json::to_vec(&serde_json::json!({
        "schema_version": 1,
        "generation_id": generation_id,
        "release_id": release_id,
        "path": format!("generations/{generation_id}")
    }))
    .map_err(|error| LifecycleError::new(error.to_string()))?;
    fs::create_dir_all(out).map_err(io_error("create lifecycle output"))?;
    let mut pointer = tempfile::NamedTempFile::new_in(out)
        .map_err(io_error("create active generation pointer"))?;
    pointer
        .write_all(&active)
        .map_err(io_error("write active generation pointer"))?;
    pointer
        .as_file()
        .sync_all()
        .map_err(io_error("sync active generation pointer"))?;
    pointer
        .persist(out.join("active.json"))
        .map_err(|error| io_error("activate generation pointer")(error.error))?;
    Ok(())
}

/// Atomically activate an already-installed immutable generation.
///
/// The IDs are checked against both manifests before the pointer can move, so
/// callers cannot relabel an arbitrary directory as a coherent generation.
pub fn activate_lifecycle_project_generation(
    project: &LifecycleProjectPlan,
    report: &LifecycleBuildReport,
    lock_timeout: Duration,
) -> Result<(), LifecycleError> {
    if lock_timeout.is_zero() || lock_timeout > MAX_LOCK_TIMEOUT {
        return Err(LifecycleError::new(
            "lifecycle lock timeout must be within 1ms..=60s",
        ));
    }
    validate_content_identity(&report.generation_id, "generation identity")?;
    validate_content_identity(&report.release_id, "release identity")?;
    let root = project.root.canonicalize().map_err(|error| {
        LifecycleError::new(format!("failed to resolve lifecycle root: {error}"))
    })?;
    let out = resolve_output(&root, &project.out)?;
    let generation_root = out.join("generations").join(&report.generation_id);
    let metadata = fs::symlink_metadata(&generation_root).map_err(|error| {
        LifecycleError::new(format!(
            "failed to inspect installed generation `{}`: {error}",
            report.generation_id
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(LifecycleError::new(
            "installed generation must be a non-symlink directory",
        ));
    }
    let generation: GenerationManifest = read_bounded_manifest(
        &generation_root.join("generation.json"),
        "installed generation manifest",
    )?;
    let release: ReleaseManifest = read_bounded_manifest(
        &generation_root.join("release.json"),
        "installed release manifest",
    )?;
    if generation.generation_id != report.generation_id
        || release.generation_id != report.generation_id
        || release.release_id != report.release_id
    {
        return Err(LifecycleError::new(
            "installed lifecycle manifests disagree with the requested activation",
        ));
    }
    fs::create_dir_all(&out).map_err(io_error("create lifecycle output"))?;
    let out_parent = out
        .parent()
        .ok_or_else(|| LifecycleError::new("lifecycle output must have a parent directory"))?;
    let _lock = BuildLock::acquire(&root, out_parent, lock_timeout)?;
    ensure_activation_predecessor(
        &out,
        report.active_generation.as_deref(),
        &report.generation_id,
    )?;
    write_active_generation(&out, &report.generation_id, &report.release_id)
}

fn ensure_activation_predecessor(
    out: &Path,
    expected: Option<&str>,
    generation_id: &str,
) -> Result<(), LifecycleError> {
    let observed = read_active_generation(out)?;
    if observed.as_deref() == expected || observed.as_deref() == Some(generation_id) {
        return Ok(());
    }
    Err(LifecycleError::superseded(format!(
        "lifecycle activation for generation `{generation_id}` was superseded: expected active generation `{}`, found `{}`",
        expected.unwrap_or("none"),
        observed.as_deref().unwrap_or("none")
    )))
}

fn read_bounded_manifest<T: for<'de> Deserialize<'de>>(
    path: &Path,
    label: &str,
) -> Result<T, LifecycleError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| LifecycleError::new(format!("failed to inspect {label}: {error}")))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > 1024 * 1024 {
        return Err(LifecycleError::new(format!(
            "{label} must be a regular non-symlink file no larger than 1 MiB"
        )));
    }
    let bytes = fs::read(path)
        .map_err(|error| LifecycleError::new(format!("failed to read {label}: {error}")))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| LifecycleError::new(format!("failed to parse {label}: {error}")))
}

fn write_manifest(stage: &Path, name: &str, bytes: &[u8]) -> Result<(), LifecycleError> {
    fs::write(stage.join(name), bytes).map_err(io_error("write lifecycle manifest"))
}

fn read_active_generation(out: &Path) -> Result<Option<String>, LifecycleError> {
    let path = out.join("active.json");
    if path
        .metadata()
        .is_ok_and(|metadata| metadata.len() > 1024 * 1024)
    {
        return Err(LifecycleError::new(
            "active generation pointer exceeds 1 MiB",
        ));
    }
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error("read active generation pointer")(error)),
    };
    let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(|error| {
        LifecycleError::new(format!("invalid active generation pointer: {error}"))
    })?;
    Ok(value
        .get("generation_id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string))
}

#[derive(Debug)]
struct BuildLock {
    file: File,
}

impl BuildLock {
    fn acquire(root: &Path, directory: &Path, timeout: Duration) -> Result<Self, LifecycleError> {
        let identity = digest_bytes(root.to_string_lossy().as_bytes());
        let name = format!("distributed-lifecycle-{}.lock", &identity[7..23]);
        let path = directory.join(name);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .map_err(io_error("open lifecycle process lock"))?;
        let started = Instant::now();
        loop {
            match file.try_lock_exclusive() {
                Ok(()) => return Ok(Self { file }),
                Err(error) if lock_is_contended(&error) => {
                    if started.elapsed() >= timeout {
                        return Err(LifecycleError::new(format!(
                            "timed out after {}ms waiting for lifecycle process lock",
                            timeout.as_millis()
                        )));
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => return Err(io_error("acquire lifecycle process lock")(error)),
            }
        }
    }
}

fn lock_is_contended(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::WouldBlock
        || error.raw_os_error() == fs2::lock_contended_error().raw_os_error()
}

impl Drop for BuildLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

fn resolve_under_root(root: &Path, path: &Path, label: &str) -> Result<PathBuf, LifecycleError> {
    let candidate = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };
    reject_symlink_components(root, &candidate, label)?;
    let resolved = candidate.canonicalize().map_err(|error| {
        LifecycleError::new(format!(
            "failed to resolve {label} `{}`: {error}",
            candidate.display()
        ))
    })?;
    if !resolved.starts_with(root) {
        return Err(LifecycleError::new(format!(
            "{label} escapes lifecycle root"
        )));
    }
    Ok(resolved)
}

pub(super) fn resolve_output(root: &Path, path: &Path) -> Result<PathBuf, LifecycleError> {
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(LifecycleError::new(
            "lifecycle output must not contain parent-directory components",
        ));
    }
    let output = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };
    if output == root || !output.starts_with(root) {
        return Err(LifecycleError::new(
            "lifecycle output must be a child of the lifecycle root",
        ));
    }
    reject_symlink_components(root, &output, "lifecycle output")?;
    let mut ancestor = output.as_path();
    while !ancestor.exists() {
        ancestor = ancestor
            .parent()
            .ok_or_else(|| LifecycleError::new("lifecycle output has no existing ancestor"))?;
    }
    let resolved_ancestor = ancestor
        .canonicalize()
        .map_err(io_error("resolve lifecycle output ancestor"))?;
    if !resolved_ancestor.starts_with(root) {
        return Err(LifecycleError::new(
            "lifecycle output escapes lifecycle root",
        ));
    }
    Ok(output)
}

fn reject_symlink_components(root: &Path, path: &Path, label: &str) -> Result<(), LifecycleError> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| LifecycleError::new(format!("{label} escapes lifecycle root")))?;
    let mut candidate = root.to_path_buf();
    for component in relative.components() {
        candidate.push(component);
        match fs::symlink_metadata(&candidate) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(LifecycleError::new(format!(
                    "{label} `{}` crosses a symlink",
                    candidate.display()
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(io_error("inspect lifecycle path component")(error)),
        }
    }
    Ok(())
}

fn portable_relative(base: &Path, path: &Path) -> Result<String, LifecycleError> {
    let relative = path.strip_prefix(base).map_err(|_| {
        LifecycleError::new(format!("path `{}` escapes lifecycle root", path.display()))
    })?;
    let value = relative.to_str().ok_or_else(|| {
        LifecycleError::new(format!("path `{}` is not valid UTF-8", relative.display()))
    })?;
    validate_portable_path(value, "lifecycle path")?;
    Ok(value.to_string())
}

fn forward_executor_stderr(mut stderr: impl Read) -> std::io::Result<Vec<u8>> {
    let mut tail = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        let count = stderr.read(&mut chunk)?;
        if count == 0 {
            return Ok(tail);
        }
        let _ = std::io::stderr().write_all(&chunk[..count]);
        tail.extend_from_slice(&chunk[..count]);
        if tail.len() > MAX_EXECUTOR_STDERR_BYTES {
            tail.drain(..tail.len() - MAX_EXECUTOR_STDERR_BYTES);
        }
    }
}

fn finish_executor_stderr(
    reader: std::thread::JoinHandle<std::io::Result<Vec<u8>>>,
) -> Result<Vec<u8>, LifecycleError> {
    reader
        .join()
        .map_err(|_| LifecycleError::new("lifecycle executor stderr reader panicked"))?
        .map_err(io_error("read lifecycle executor stderr"))
}

fn io_error(label: &'static str) -> impl FnOnce(std::io::Error) -> LifecycleError {
    move |error| LifecycleError::new(format!("{label}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::graph::LifecycleErrorReason;

    #[test]
    fn activation_rejects_an_advanced_generation() {
        let fixture = tempfile::tempdir().expect("create activation fixture");
        let out = fixture.path();
        write_active_generation(out, "generation-b", "release-b")
            .expect("write advanced generation");

        let error = ensure_activation_predecessor(out, Some("generation-a"), "generation-c")
            .expect_err("a stale deferred activation must be rejected");
        assert_eq!(error.reason(), LifecycleErrorReason::Superseded);
        assert!(error.message().contains("generation-a"));
        assert!(error.message().contains("generation-b"));

        ensure_activation_predecessor(out, Some("generation-b"), "generation-c")
            .expect("the observed predecessor may advance");
        ensure_activation_predecessor(out, Some("generation-a"), "generation-b")
            .expect("activating the already-active generation is idempotent");
    }

    #[test]
    fn concurrent_build_lock_wait_is_bounded() {
        let root = tempfile::tempdir().expect("create lock fixture root");
        let _first = BuildLock::acquire(root.path(), root.path(), Duration::from_millis(100))
            .expect("acquire first lifecycle lock");
        assert!(fs::read_dir(root.path()).unwrap().any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".lock")));
        let error = BuildLock::acquire(root.path(), root.path(), Duration::from_millis(20))
            .expect_err("second lifecycle lock should time out");
        assert!(error.message().contains("timed out"));
    }

    #[test]
    fn directory_file_bound_fails_closed_for_every_caller() {
        let root = tempfile::tempdir().expect("create file-bound fixture root");
        fs::write(root.path().join("overflow.txt"), b"overflow").unwrap();
        let mut files = vec![PathBuf::new(); MAX_HASHED_FILES];
        let mut total_bytes = 0;

        let error = collect_regular_files(root.path(), &mut files, &mut total_bytes, 0)
            .expect_err("file bound must reject rather than truncate");
        assert!(error.message().contains("exceeds the file bound"));
    }
}
