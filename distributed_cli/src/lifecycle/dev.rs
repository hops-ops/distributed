use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::contracts::ContractCatalog;

use super::build::{lifecycle_input_snapshot, resolve_output};
use super::graph::LifecycleErrorReason;
use super::{
    activate_lifecycle_project_generation, run_lifecycle_project_build, validate_portable_path,
    validate_stable_value, LifecycleActivation, LifecycleBuildConfig, LifecycleBuildOptions,
    LifecycleBuildReport, LifecycleBuildRequest, LifecycleError, LifecycleGraph,
    LifecycleProjectPlan,
};

const MAX_DEV_PROCESSES: usize = 64;
const MAX_DEV_INTERVAL: Duration = Duration::from_secs(30);
const MAX_DEV_PROBE_TIMEOUT: Duration = Duration::from_secs(300);
const MAX_DEV_READINESS_TOTAL: Duration = Duration::from_secs(361);

fn default_poll_ms() -> u64 {
    200
}

fn default_debounce_ms() -> u64 {
    100
}

fn default_ready_after_ms() -> u64 {
    100
}

fn default_shutdown_ms() -> u64 {
    5_000
}

fn default_prepare_ms() -> u64 {
    30_000
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleDevProcess {
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
    /// Working directory relative to the lifecycle root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// Permit an absolute working directory selected by trusted project
    /// discovery. Ordinary lifecycle files remain root-confined by default.
    #[serde(skip)]
    pub external_cwd: bool,
    /// Process-specific environment. Values may use lifecycle placeholders.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    /// Operator-facing URL printed after readiness succeeds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Restart after a successful generation when any named node invalidates.
    /// An empty set leaves native HMR/process watching entirely in charge.
    #[serde(default)]
    pub restart_on: BTreeSet<String>,
    #[serde(default = "default_ready_after_ms")]
    pub ready_after_ms: u64,
    /// Optional command probe; exit status 0 is readiness evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready: Option<LifecycleDevProbe>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleDevProbe {
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default = "default_poll_ms")]
    pub interval_ms: u64,
    #[serde(default = "default_shutdown_ms")]
    pub timeout_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleDevConfig {
    #[serde(default = "default_poll_ms")]
    pub poll_ms: u64,
    #[serde(default = "default_debounce_ms")]
    pub debounce_ms: u64,
    #[serde(default = "default_shutdown_ms")]
    pub shutdown_ms: u64,
    /// Maximum browser capsule preparation window for coherent reloads.
    #[serde(default = "default_prepare_ms")]
    pub prepare_ms: u64,
    pub processes: BTreeMap<String, LifecycleDevProcess>,
}

impl LifecycleDevConfig {
    pub fn validate(&self) -> Result<(), LifecycleError> {
        for (label, value) in [
            ("poll", self.poll_ms),
            ("debounce", self.debounce_ms),
            ("shutdown", self.shutdown_ms),
            ("prepare", self.prepare_ms),
        ] {
            if value == 0 || Duration::from_millis(value) > MAX_DEV_INTERVAL {
                return Err(LifecycleError::new(format!(
                    "lifecycle dev {label} interval must be within 1ms..=30s"
                )));
            }
        }
        if self.processes.is_empty() || self.processes.len() > MAX_DEV_PROCESSES {
            return Err(LifecycleError::new(format!(
                "lifecycle dev processes must contain 1..={MAX_DEV_PROCESSES} entries"
            )));
        }
        if self
            .processes
            .values()
            .map(readiness_budget_ms)
            .sum::<u64>()
            > MAX_DEV_READINESS_TOTAL.as_millis() as u64
        {
            return Err(LifecycleError::new(
                "total lifecycle dev readiness delay must not exceed 361s",
            ));
        }
        for (name, process) in &self.processes {
            validate_stable_value(name, "lifecycle dev process name")?;
            validate_stable_value(&process.program, "lifecycle dev process program")?;
            if let Some(cwd) = &process.cwd {
                if process.external_cwd {
                    let cwd = Path::new(cwd);
                    if !cwd.is_absolute() || cwd.as_os_str().as_encoded_bytes().contains(&0) {
                        return Err(LifecycleError::new(
                            "external lifecycle dev process cwd must be an absolute path",
                        ));
                    }
                } else {
                    validate_portable_path(cwd, "lifecycle dev process cwd")?;
                }
            } else if process.external_cwd {
                return Err(LifecycleError::new(
                    "external lifecycle dev process cwd requires cwd",
                ));
            }
            validate_process_env(name, &process.env)?;
            if let Some(url) = &process.url {
                validate_stable_value(url, "lifecycle dev process URL")?;
            }
            if process.args.len() > 256
                || process.args.iter().map(String::len).sum::<usize>() > 16 * 1024
                || process.args.iter().any(|arg| arg.contains('\0'))
            {
                return Err(LifecycleError::new(format!(
                    "lifecycle dev process `{name}` exceeds argument bounds"
                )));
            }
            if process.ready_after_ms == 0
                || Duration::from_millis(process.ready_after_ms) > MAX_DEV_INTERVAL
            {
                return Err(LifecycleError::new(format!(
                    "lifecycle dev process `{name}` readiness interval is out of bounds"
                )));
            }
            if let Some(probe) = &process.ready {
                validate_stable_value(&probe.program, "lifecycle readiness probe program")?;
                if probe.args.len() > 256
                    || probe.args.iter().map(String::len).sum::<usize>() > 16 * 1024
                    || probe.args.iter().any(|arg| arg.contains('\0'))
                    || probe.interval_ms == 0
                    || probe.timeout_ms == 0
                    || probe.interval_ms > probe.timeout_ms
                    || Duration::from_millis(probe.timeout_ms) > MAX_DEV_PROBE_TIMEOUT
                {
                    return Err(LifecycleError::new(format!(
                        "lifecycle dev process `{name}` readiness probe is out of bounds"
                    )));
                }
            }
            for node in &process.restart_on {
                validate_stable_value(node, "lifecycle dev restart node")?;
            }
        }
        Ok(())
    }
}

fn readiness_budget_ms(process: &LifecycleDevProcess) -> u64 {
    process
        .ready
        .as_ref()
        .map_or(process.ready_after_ms, |probe| probe.timeout_ms)
}

#[derive(Clone, Debug)]
pub struct LifecycleDevOptions {
    pub build: LifecycleBuildOptions,
    pub stop: Arc<AtomicBool>,
    /// Emit operator-facing readiness and rebuild progress to stderr.
    pub progress: bool,
}

#[derive(Clone, Debug)]
pub struct LifecycleProjectDevOptions {
    /// In-memory lifecycle plan derived from the project.
    pub project: LifecycleProjectPlan,
    /// Build behavior shared by the initial and incremental generations.
    pub build: LifecycleBuildRequest,
    /// Cooperative stop flag set by Ctrl-C or the embedding host.
    pub stop: Arc<AtomicBool>,
    /// Emit operator-facing readiness and rebuild progress to stderr.
    pub progress: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleDevReport {
    pub initial_generation: String,
    pub final_generation: String,
    pub rebuilds: usize,
    pub restarts: BTreeMap<String, usize>,
}

pub fn run_lifecycle_dev(
    options: &LifecycleDevOptions,
) -> Result<LifecycleDevReport, LifecycleError> {
    if options.build.check {
        return Err(LifecycleError::new(
            "lifecycle dev requires an activating build configuration",
        ));
    }
    let root = options.build.root.canonicalize().map_err(|error| {
        LifecycleError::new(format!("failed to resolve lifecycle dev root: {error}"))
    })?;
    let config_path = resolve_file(&root, &options.build.config)?;
    let catalog_path = resolve_file(&root, &options.build.catalog)?;
    let config = LifecycleBuildConfig::from_path(config_path)?;
    let dev = config.dev.clone().ok_or_else(|| {
        LifecycleError::new("lifecycle config has no `dev` process configuration")
    })?;
    dev.validate()?;
    let catalog = ContractCatalog::from_path_at_root(&catalog_path, &root)
        .map_err(|error| LifecycleError::new(error.to_string()))?;
    run_lifecycle_project_dev(&LifecycleProjectDevOptions {
        project: LifecycleProjectPlan {
            root,
            catalog,
            config,
            out: options.build.out.clone(),
        },
        build: LifecycleBuildRequest::from(&options.build),
        stop: Arc::clone(&options.stop),
        progress: options.progress,
    })
}

/// Run development processes for a project resolved from Cargo metadata and
/// filesystem conventions.
pub fn run_lifecycle_project_dev(
    options: &LifecycleProjectDevOptions,
) -> Result<LifecycleDevReport, LifecycleError> {
    if options.build.check {
        return Err(LifecycleError::new(
            "lifecycle dev requires an activating build configuration",
        ));
    }
    let root = options.project.root.canonicalize().map_err(|error| {
        LifecycleError::new(format!("failed to resolve lifecycle dev root: {error}"))
    })?;
    let config = &options.project.config;
    let mut dev = config.dev.clone().ok_or_else(|| {
        LifecycleError::new("lifecycle project has no development process configuration")
    })?;
    dev.validate()?;
    let catalog = &options.project.catalog;
    let graph = LifecycleGraph::from_catalog(catalog, &config.lifecycle())?;
    validate_restart_nodes(&dev, &graph)?;

    // Initial coherent generation is an absolute serving barrier.
    let mut initial_options = options.build.clone();
    initial_options.nodes = None;
    initial_options.activation_inputs = None;
    initial_options.cancel = Some(Arc::clone(&options.stop));
    let initial = run_lifecycle_project_build(&options.project, &initial_options)?;
    let state = DevStateStore::new(&root, &options.project.out)?;
    state.write_active(&initial, &BTreeMap::new())?;
    for process in dev.processes.values_mut() {
        process.env.insert(
            "DISTRIBUTED_LIFECYCLE_DIR".to_string(),
            state.root.to_string_lossy().into_owned(),
        );
    }
    let mut children = ChildSet::start(&root, &dev, &initial, &options.stop)?;
    if let Err(error) = state.write_active(&initial, &children.members) {
        let _ = children.shutdown(Duration::from_millis(dev.shutdown_ms));
        return Err(error);
    }
    if options.progress {
        for (name, process) in &dev.processes {
            if let Some(url) = &process.url {
                eprintln!("lifecycle dev: process {name} ready {url}");
            }
        }
        eprintln!(
            "lifecycle dev: ready generation={} processes={} (Ctrl-C to stop)",
            initial.generation_id,
            dev.processes.keys().cloned().collect::<Vec<_>>().join(",")
        );
    }
    let mut snapshot = lifecycle_input_snapshot(&root, catalog, &graph)?;
    let mut final_generation = initial.generation_id.clone();
    let mut active = initial.clone();
    let mut rebuilds = 0;
    let mut restarts = dev
        .processes
        .keys()
        .map(|name| (name.clone(), 0))
        .collect::<BTreeMap<_, _>>();

    let result = (|| {
        while !options.stop.load(Ordering::SeqCst) {
            children.ensure_running()?;
            std::thread::sleep(Duration::from_millis(dev.poll_ms));
            let next = lifecycle_input_snapshot(&root, catalog, &graph)?;
            let mut changed = changed_paths(&snapshot, &next);
            if changed.is_empty() {
                continue;
            }

            // Quiet-period debounce; each new content identity extends the window.
            let mut quiet_since = Instant::now();
            let mut latest = next;
            while quiet_since.elapsed() < Duration::from_millis(dev.debounce_ms) {
                if options.stop.load(Ordering::SeqCst) {
                    return Ok(());
                }
                std::thread::sleep(Duration::from_millis(dev.poll_ms.min(dev.debounce_ms)));
                let observed = lifecycle_input_snapshot(&root, catalog, &graph)?;
                let delta = changed_paths(&latest, &observed);
                if !delta.is_empty() {
                    changed.extend(delta);
                    latest = observed;
                    quiet_since = Instant::now();
                }
            }

            let invalidated = graph.invalidated_by_paths(&changed)?;
            let mut rebuild_options = options.build.clone();
            rebuild_options.nodes = Some(invalidated.clone());
            let submitted_inputs = latest.clone();
            rebuild_options.activation_inputs = Some(submitted_inputs.clone());
            rebuild_options.activation = LifecycleActivation::Deferred;
            let cancel = Arc::new(AtomicBool::new(false));
            rebuild_options.cancel = Some(Arc::clone(&cancel));
            let build_project = options.project.clone();
            let build = std::thread::spawn(move || {
                run_lifecycle_project_build(&build_project, &rebuild_options)
            });
            let mut superseded = false;
            while !build.is_finished() {
                if options.stop.load(Ordering::SeqCst) {
                    cancel.store(true, Ordering::SeqCst);
                }
                if let Err(error) = children.ensure_running() {
                    cancel.store(true, Ordering::SeqCst);
                    let _ = build.join();
                    return Err(error);
                }
                std::thread::sleep(Duration::from_millis(dev.poll_ms));
                let observed = match lifecycle_input_snapshot(&root, catalog, &graph) {
                    Ok(observed) => observed,
                    Err(error) => {
                        cancel.store(true, Ordering::SeqCst);
                        let _ = build.join();
                        return Err(error);
                    }
                };
                if !changed_paths(&latest, &observed).is_empty() {
                    latest = observed;
                    superseded = true;
                    cancel.store(true, Ordering::SeqCst);
                }
            }
            let build_result = build
                .join()
                .map_err(|_| LifecycleError::new("lifecycle build worker panicked"))?;
            let generation = match build_result {
                Ok(generation) => generation,
                Err(error)
                    if options.stop.load(Ordering::SeqCst)
                        && error.reason() == LifecycleErrorReason::Canceled =>
                {
                    return Ok(())
                }
                Err(error)
                    if superseded
                        && matches!(
                            error.reason(),
                            LifecycleErrorReason::Canceled | LifecycleErrorReason::Superseded
                        ) =>
                {
                    continue
                }
                Err(error) if error.reason() == LifecycleErrorReason::Superseded => continue,
                Err(error) => return Err(error),
            };
            rebuilds += 1;
            if options.stop.load(Ordering::SeqCst) {
                return Ok(());
            }
            // A generation may be attempted more than once. Scope browser
            // acknowledgements to this attempt so a persisted response from a
            // rejected/superseded attempt can never authorize the next one.
            let transition = state.begin_transition()?;
            state.write_preparing(
                &active,
                &generation,
                transition.id(),
                dev.prepare_ms,
                &children.members,
            )?;
            if let Err(error) = state.wait_for_prepare(
                transition.id(),
                Duration::from_millis(dev.prepare_ms),
                &options.stop,
            ) {
                if options.stop.load(Ordering::SeqCst)
                    && error.reason() == LifecycleErrorReason::Canceled
                {
                    return Ok(());
                }
                children.ensure_running().map_err(|serving| {
					LifecycleError::new(format!(
						"pending generation preparation failed: {error}; prior generation is no longer serving: {serving}"
					))
				})?;
                state.write_active(&active, &children.members)?;
                if options.progress {
                    eprintln!(
						"lifecycle dev: rejected generation={} during preparation; prior generation remains active: {}",
						generation.generation_id, error
					);
                }
                snapshot = submitted_inputs;
                continue;
            }
            let restarting = dev
                .processes
                .iter()
                .filter(|(_, process)| !process.restart_on.is_disjoint(&invalidated))
                .map(|(name, _)| name.clone())
                .collect::<Vec<_>>();
            if let Err(error) = children.restart_transactional(
                &root,
                &dev,
                &active,
                &generation,
                &invalidated,
                &mut restarts,
                &options.stop,
            ) {
                if options.stop.load(Ordering::SeqCst)
                    && error.reason() == LifecycleErrorReason::Canceled
                {
                    return Ok(());
                }
                children.ensure_running().map_err(|serving| {
					LifecycleError::new(format!(
						"pending generation readiness failed: {error}; prior generation rollback is not serving: {serving}"
					))
                })?;
                state.write_active(&active, &children.members)?;
                if options.progress {
                    eprintln!(
						"lifecycle dev: rejected generation={} during readiness; prior generation remains active: {}",
						generation.generation_id, error
					);
                }
                snapshot = submitted_inputs;
                continue;
            }
            if let Err(error) = activate_lifecycle_project_generation(
                &options.project,
                &generation,
                options.build.lock_timeout,
            ) {
                let rollback = children.replace_generation(
                    &root,
                    &dev,
                    &generation,
                    &active,
                    &invalidated,
                    &options.stop,
                );
                return match rollback {
					Ok(()) => {
						children.ensure_running()?;
						state.write_active(&active, &children.members)?;
						if options.progress {
							eprintln!(
								"lifecycle dev: rejected generation={} during activation; prior generation remains active: {}",
								generation.generation_id, error
							);
						}
						snapshot = submitted_inputs;
						continue;
					}
                    Err(rollback) => Err(LifecycleError::new(format!(
                        "failed to activate pending generation: {error}; process rollback also failed: {rollback}"
                    ))),
                };
            }
            final_generation = generation.generation_id.clone();
            active = generation.clone();
            children.ensure_running()?;
            state.write_active(&active, &children.members)?;
            if options.progress {
                eprintln!(
                    "lifecycle dev: activated generation={} invalidated={} restarted={}",
                    generation.generation_id,
                    invalidated.iter().cloned().collect::<Vec<_>>().join(","),
                    if restarting.is_empty() {
                        "none".to_string()
                    } else {
                        restarting.join(",")
                    }
                );
            }
            snapshot = submitted_inputs;
        }
        Ok(())
    })();

    let retired = state.write_active(&active, &BTreeMap::new());
    let shutdown = children.shutdown(Duration::from_millis(dev.shutdown_ms));
    result?;
    retired?;
    shutdown?;
    Ok(LifecycleDevReport {
        initial_generation: initial.generation_id,
        final_generation,
        rebuilds,
        restarts,
    })
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DevGenerationState<'a> {
    generation_id: &'a str,
    release_id: &'a str,
    topology_id: &'a str,
    compatibility_id: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DevLifecycleState<'a> {
    schema_version: u32,
    phase: &'static str,
    active: DevGenerationState<'a>,
    members: &'a BTreeMap<String, DevMemberState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pending: Option<DevGenerationState<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transition_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deadline_unix_ms: Option<u64>,
}

struct DevStateStore {
    root: PathBuf,
    state: PathBuf,
}

impl DevStateStore {
    fn new(project_root: &Path, output: &Path) -> Result<Self, LifecycleError> {
        let root = resolve_output(project_root, output)?;
        fs::create_dir_all(root.join("dev-control/participants"))
            .map_err(dev_io("create lifecycle participant directory"))?;
        fs::create_dir_all(root.join("dev-control/acks"))
            .map_err(dev_io("create lifecycle acknowledgement directory"))?;
        Ok(Self {
            state: root.join("dev.json"),
            root,
        })
    }

    fn write_active(
        &self,
        active: &LifecycleBuildReport,
        members: &BTreeMap<String, DevMemberState>,
    ) -> Result<(), LifecycleError> {
        self.write(&DevLifecycleState {
            schema_version: 1,
            phase: "active",
            active: generation_state(active),
            members,
            pending: None,
            transition_id: None,
            deadline_unix_ms: None,
        })
    }

    fn begin_transition(&self) -> Result<DevTransition, LifecycleError> {
        let directory = tempfile::Builder::new()
            .prefix("transition-")
            .tempdir_in(self.root.join("dev-control/acks"))
            .map_err(dev_io(
                "create lifecycle transition acknowledgement directory",
            ))?;
        let id = directory
            .path()
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| safe_control_id(name))
            .ok_or_else(|| LifecycleError::new("temporary lifecycle transition ID is invalid"))?
            .to_string();
        Ok(DevTransition {
            id,
            _directory: directory,
        })
    }

    fn write_preparing(
        &self,
        active: &LifecycleBuildReport,
        pending: &LifecycleBuildReport,
        transition_id: &str,
        prepare_ms: u64,
        members: &BTreeMap<String, DevMemberState>,
    ) -> Result<(), LifecycleError> {
        let deadline = unix_ms()?.saturating_add(prepare_ms);
        self.write(&DevLifecycleState {
            schema_version: 1,
            phase: "preparing",
            active: generation_state(active),
            members,
            pending: Some(generation_state(pending)),
            transition_id: Some(transition_id),
            deadline_unix_ms: Some(deadline),
        })
    }

    fn write(&self, state: &DevLifecycleState<'_>) -> Result<(), LifecycleError> {
        let bytes = serde_json::to_vec(state).map_err(|error| {
            LifecycleError::new(format!("serialize lifecycle dev state: {error}"))
        })?;
        let mut file = tempfile::NamedTempFile::new_in(&self.root)
            .map_err(dev_io("create lifecycle dev state"))?;
        use std::io::Write as _;
        file.write_all(&bytes)
            .map_err(dev_io("write lifecycle dev state"))?;
        file.as_file()
            .sync_all()
            .map_err(dev_io("sync lifecycle dev state"))?;
        file.persist(&self.state)
            .map_err(|error| dev_io("activate lifecycle dev state")(error.error))?;
        Ok(())
    }

    fn wait_for_prepare(
        &self,
        transition_id: &str,
        timeout: Duration,
        stop: &AtomicBool,
    ) -> Result<(), LifecycleError> {
        let started = Instant::now();
        let discovery = Duration::from_millis(500).min(timeout);
        let acknowledgement_settle = Duration::from_millis(1_000).min(timeout);
        let mut complete_since = None;
        loop {
            if stop.load(Ordering::SeqCst) {
                return Err(LifecycleError::canceled(
                    "browser reload preparation was canceled",
                ));
            }
            let participants = self.fresh_participants(Duration::from_secs(5))?;
            if !participants.is_empty() {
                let acknowledgements = self.acknowledgements(transition_id)?;
                if participants
                    .iter()
                    .any(|id| acknowledgements.get(id) == Some(&false))
                {
                    return Err(LifecycleError::new(
                        "a browser rejected coherent reload capsule preparation",
                    ));
                }
                if participants
                    .iter()
                    .all(|id| acknowledgements.get(id) == Some(&true))
                {
                    let complete = complete_since.get_or_insert_with(Instant::now);
                    if complete.elapsed() >= acknowledgement_settle {
                        return Ok(());
                    }
                } else {
                    complete_since = None;
                }
            } else if started.elapsed() >= discovery {
                return Ok(());
            } else {
                complete_since = None;
            }
            if started.elapsed() >= timeout {
                return Err(LifecycleError::new(format!(
                    "browser reload preparation timed out after {}ms",
                    timeout.as_millis()
                )));
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    fn fresh_participants(
        &self,
        maximum_age: Duration,
    ) -> Result<BTreeSet<String>, LifecycleError> {
        let now = SystemTime::now();
        let mut result = BTreeSet::new();
        for entry in fs::read_dir(self.root.join("dev-control/participants"))
            .map_err(dev_io("read lifecycle participants"))?
        {
            let entry = entry.map_err(dev_io("read lifecycle participant"))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if !safe_control_id(name.strip_suffix(".json").unwrap_or("")) {
                continue;
            }
            if !entry
                .file_type()
                .map_err(dev_io("inspect lifecycle participant type"))?
                .is_file()
            {
                continue;
            }
            let metadata = entry
                .metadata()
                .map_err(dev_io("inspect lifecycle participant"))?;
            if metadata.is_file()
                && now
                    .duration_since(
                        metadata
                            .modified()
                            .map_err(dev_io("read participant time"))?,
                    )
                    .unwrap_or_default()
                    <= maximum_age
            {
                result.insert(name.trim_end_matches(".json").to_string());
            }
        }
        Ok(result)
    }

    fn acknowledgements(
        &self,
        transition_id: &str,
    ) -> Result<BTreeMap<String, bool>, LifecycleError> {
        let mut result = BTreeMap::new();
        let directory = self.root.join("dev-control/acks").join(transition_id);
        let entries = match fs::read_dir(directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(result),
            Err(error) => return Err(dev_io("read lifecycle acknowledgements")(error)),
        };
        for entry in entries {
            let entry = entry.map_err(dev_io("read lifecycle acknowledgement"))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let id = name.strip_suffix(".json").unwrap_or("");
            if !safe_control_id(id)
                || !entry
                    .file_type()
                    .map_err(dev_io("inspect lifecycle acknowledgement type"))?
                    .is_file()
                || entry
                    .metadata()
                    .map_err(dev_io("inspect lifecycle acknowledgement"))?
                    .len()
                    > 1024
            {
                continue;
            }
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Ack {
                ok: bool,
            }
            let ack: Ack = serde_json::from_slice(
                &fs::read(entry.path()).map_err(dev_io("read lifecycle acknowledgement"))?,
            )
            .map_err(|error| {
                LifecycleError::new(format!("parse lifecycle acknowledgement: {error}"))
            })?;
            result.insert(id.to_string(), ack.ok);
        }
        Ok(result)
    }
}

struct DevTransition {
    id: String,
    // TempDir removes this attempt's acknowledgement directory after the
    // preparation barrier completes or unwinds on an error.
    _directory: tempfile::TempDir,
}

impl DevTransition {
    fn id(&self) -> &str {
        &self.id
    }
}

impl Drop for DevStateStore {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.state);
    }
}

fn generation_state(report: &LifecycleBuildReport) -> DevGenerationState<'_> {
    DevGenerationState {
        generation_id: &report.generation_id,
        release_id: &report.release_id,
        topology_id: &report.graph_id,
        compatibility_id: &report.compatibility_id,
    }
}

fn unix_ms() -> Result<u64, LifecycleError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| LifecycleError::new(format!("system clock precedes Unix epoch: {error}")))?
        .as_millis()
        .try_into()
        .map_err(|_| LifecycleError::new("system clock exceeds lifecycle timestamp bounds"))
}

fn safe_control_id(value: &str) -> bool {
    (16..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b':'))
}

fn dev_io(label: &'static str) -> impl FnOnce(std::io::Error) -> LifecycleError {
    move |error| LifecycleError::new(format!("{label}: {error}"))
}

fn validate_restart_nodes(
    dev: &LifecycleDevConfig,
    graph: &LifecycleGraph,
) -> Result<(), LifecycleError> {
    for (name, process) in &dev.processes {
        let missing = process
            .restart_on
            .iter()
            .filter(|node| !graph.nodes.contains_key(*node))
            .cloned()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(LifecycleError::new(format!(
                "lifecycle dev process `{name}` references unknown restart nodes: {}",
                missing.join(", ")
            )));
        }
    }
    Ok(())
}

fn changed_paths(
    before: &BTreeMap<String, String>,
    after: &BTreeMap<String, String>,
) -> BTreeSet<String> {
    before
        .keys()
        .chain(after.keys())
        .filter(|path| before.get(*path) != after.get(*path))
        .cloned()
        .collect()
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DevMemberState {
    instance_id: String,
    generation_id: String,
}

impl DevMemberState {
    fn new(generation: &LifecycleBuildReport) -> Result<Self, LifecycleError> {
        let identity = tempfile::Builder::new()
            .prefix("member-")
            .rand_bytes(16)
            .tempfile()
            .map_err(dev_io("allocate lifecycle process identity"))?;
        let instance_id = identity
            .path()
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| LifecycleError::new("invalid lifecycle process identity"))?
            .to_owned();
        Ok(Self {
            instance_id,
            generation_id: generation.generation_id.clone(),
        })
    }
}

struct ChildSet {
    children: BTreeMap<String, Child>,
    members: BTreeMap<String, DevMemberState>,
}

impl ChildSet {
    fn start(
        root: &Path,
        config: &LifecycleDevConfig,
        generation: &LifecycleBuildReport,
        stop: &AtomicBool,
    ) -> Result<Self, LifecycleError> {
        let mut set = Self {
            children: BTreeMap::new(),
            members: BTreeMap::new(),
        };
        for (name, process) in &config.processes {
            let spawned = DevMemberState::new(generation).and_then(|member| {
                spawn_process(root, name, process, generation, &member.instance_id)
                    .map(|child| (child, member))
            });
            match spawned {
                Ok((child, member)) => {
                    set.children.insert(name.clone(), child);
                    set.members.insert(name.clone(), member);
                }
                Err(error) => {
                    let _ = set.shutdown(Duration::from_millis(config.shutdown_ms));
                    return Err(error);
                }
            }
        }
        for (name, process) in &config.processes {
            if let Err(error) = wait_ready(
                root,
                name,
                process,
                generation,
                set.children.get_mut(name).unwrap(),
                stop,
            ) {
                let _ = set.shutdown(Duration::from_millis(config.shutdown_ms));
                return Err(error);
            }
        }
        Ok(set)
    }

    fn ensure_running(&mut self) -> Result<(), LifecycleError> {
        if self.children.len() != self.members.len() || self.children.keys().ne(self.members.keys())
        {
            return Err(LifecycleError::new(
                "lifecycle process cohort is incomplete",
            ));
        }
        for (name, child) in &mut self.children {
            if let Some(status) = child.try_wait().map_err(|error| {
                LifecycleError::new(format!("failed to inspect dev process `{name}`: {error}"))
            })? {
                return Err(LifecycleError::new(format!(
                    "lifecycle dev process `{name}` exited with {status}"
                )));
            }
        }
        Ok(())
    }

    fn restart_transactional(
        &mut self,
        root: &Path,
        config: &LifecycleDevConfig,
        previous: &LifecycleBuildReport,
        generation: &LifecycleBuildReport,
        invalidated: &BTreeSet<String>,
        restarts: &mut BTreeMap<String, usize>,
        stop: &AtomicBool,
    ) -> Result<(), LifecycleError> {
        let mut replaced = BTreeSet::new();
        for (name, process) in &config.processes {
            if process.restart_on.is_disjoint(invalidated) {
                continue;
            }
            let mut previous_child = self
                .children
                .remove(name)
                .expect("configured lifecycle process must be running");
            if let Err(error) = stop_child(
                name,
                &mut previous_child,
                Duration::from_millis(config.shutdown_ms),
            ) {
                self.children.insert(name.clone(), previous_child);
                return Err(error);
            }
            let replacement = DevMemberState::new(generation).and_then(|member| {
                spawn_process(root, name, process, generation, &member.instance_id).and_then(
                    |mut child| {
                        if let Err(error) =
                            wait_ready(root, name, process, generation, &mut child, stop)
                        {
                            let _ = stop_child(
                                name,
                                &mut child,
                                Duration::from_millis(config.shutdown_ms),
                            );
                            return Err(error);
                        }
                        Ok((child, member))
                    },
                )
            });
            match replacement {
                Ok((child, member)) => {
                    self.children.insert(name.clone(), child);
                    self.members.insert(name.clone(), member);
                    replaced.insert(name.clone());
                }
                Err(error) => {
                    replaced.insert(name.clone());
                    let rollback =
                        self.replace_named_generation(root, config, previous, &replaced, stop);
                    return match rollback {
                        Ok(()) => Err(error),
                        Err(rollback) => Err(LifecycleError::new(format!(
                            "pending generation readiness failed: {error}; process rollback also failed: {rollback}"
                        ))),
                    };
                }
            }
            *restarts.get_mut(name).unwrap() += 1;
        }
        Ok(())
    }

    fn replace_generation(
        &mut self,
        root: &Path,
        config: &LifecycleDevConfig,
        _current: &LifecycleBuildReport,
        replacement: &LifecycleBuildReport,
        invalidated: &BTreeSet<String>,
        stop: &AtomicBool,
    ) -> Result<(), LifecycleError> {
        let names = config
            .processes
            .iter()
            .filter(|(_, process)| !process.restart_on.is_disjoint(invalidated))
            .map(|(name, _)| name.clone())
            .collect();
        self.replace_named_generation(root, config, replacement, &names, stop)
    }

    fn replace_named_generation(
        &mut self,
        root: &Path,
        config: &LifecycleDevConfig,
        replacement: &LifecycleBuildReport,
        names: &BTreeSet<String>,
        stop: &AtomicBool,
    ) -> Result<(), LifecycleError> {
        for name in names {
            let process = &config.processes[name];
            if let Some(mut child) = self.children.remove(name) {
                if let Err(error) =
                    stop_child(name, &mut child, Duration::from_millis(config.shutdown_ms))
                {
                    self.children.insert(name.clone(), child);
                    return Err(error);
                }
            }
            let member = DevMemberState::new(replacement)?;
            let mut child = spawn_process(root, name, process, replacement, &member.instance_id)?;
            if let Err(error) = wait_ready(root, name, process, replacement, &mut child, stop) {
                let _ = stop_child(name, &mut child, Duration::from_millis(config.shutdown_ms));
                return Err(error);
            }
            self.children.insert(name.clone(), child);
            self.members.insert(name.clone(), member);
        }
        Ok(())
    }

    fn shutdown(&mut self, timeout: Duration) -> Result<(), LifecycleError> {
        for (name, child) in &mut self.children {
            terminate_process_group(name, child)?;
        }
        let started = Instant::now();
        loop {
            let mut running = Vec::new();
            for (name, child) in &mut self.children {
                if process_group_is_running(child)? {
                    running.push(name.clone());
                }
            }
            if running.is_empty() {
                return Ok(());
            }
            if started.elapsed() >= timeout {
                for name in &running {
                    let child = self.children.get_mut(name).unwrap();
                    kill_process_group(name, child)?;
                }
                return wait_for_stopped_groups(&mut self.children, Duration::from_secs(1));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

fn wait_ready(
    root: &Path,
    name: &str,
    process: &LifecycleDevProcess,
    generation: &LifecycleBuildReport,
    child: &mut Child,
    stop: &AtomicBool,
) -> Result<(), LifecycleError> {
    let started = Instant::now();
    let timeout = process
        .ready
        .as_ref()
        .map(|probe| Duration::from_millis(probe.timeout_ms));
    loop {
        if stop.load(Ordering::SeqCst) {
            return Err(LifecycleError::canceled(format!(
                "readiness for lifecycle dev process `{name}` was canceled"
            )));
        }
        if let Some(status) = child.try_wait().map_err(|error| {
            LifecycleError::new(format!("failed to inspect dev process `{name}`: {error}"))
        })? {
            return Err(LifecycleError::new(format!(
                "lifecycle dev process `{name}` exited before readiness with {status}"
            )));
        }
        let Some(probe) = &process.ready else {
            sleep_interruptibly(Duration::from_millis(process.ready_after_ms), stop, name)?;
            return Ok(());
        };
        if started.elapsed() >= timeout.unwrap() {
            return Err(readiness_timeout(name, probe.timeout_ms));
        }
        let cwd = resolve_working_dir(root, process.cwd.as_deref(), process.external_cwd)?;
        let environment = expand_environment(root, name, process, generation);
        let args = probe
            .args
            .iter()
            .map(|arg| expand_process_value(arg, root, name, generation))
            .collect::<Vec<_>>();
        let program = expand_process_value(&probe.program, root, name, generation);
        let mut command = Command::new(&program);
        command
            .args(args)
            .current_dir(cwd)
            .envs(environment)
            .env("DISTRIBUTED_GENERATION_ID", &generation.generation_id)
            .env("DISTRIBUTED_RELEASE_ID", &generation.release_id)
            .env("DISTRIBUTED_TOPOLOGY_ID", &generation.graph_id)
            .env("DISTRIBUTED_COMPATIBILITY_ID", &generation.compatibility_id)
            .env("DISTRIBUTED_MEMBER_ID", name)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(unix)]
        command.process_group(0);
        let mut probe_child = command.spawn().map_err(|error| {
            LifecycleError::new(format!(
                "failed to run readiness probe for lifecycle dev process `{name}` with `{program}`: {error}"
            ))
        })?;
        let status = loop {
            match probe_child.try_wait() {
                Ok(Some(status)) => {
                    if started.elapsed() >= timeout.unwrap() {
                        return Err(readiness_timeout(name, probe.timeout_ms));
                    }
                    break status;
                }
                Ok(None) => {}
                Err(error) => {
                    let _ = force_stop_child(name, &mut probe_child);
                    return Err(LifecycleError::new(format!(
                        "failed to inspect readiness probe for lifecycle dev process `{name}`: {error}"
                    )));
                }
            }
            if stop.load(Ordering::SeqCst) {
                force_stop_child(name, &mut probe_child)?;
                return Err(LifecycleError::canceled(format!(
                    "readiness for lifecycle dev process `{name}` was canceled"
                )));
            }
            if started.elapsed() >= timeout.unwrap() {
                force_stop_child(name, &mut probe_child)?;
                return Err(readiness_timeout(name, probe.timeout_ms));
            }
            std::thread::sleep(
                Duration::from_millis(10).min(timeout.unwrap().saturating_sub(started.elapsed())),
            );
        };
        if status.success() {
            return Ok(());
        }
        let remaining = timeout.unwrap().saturating_sub(started.elapsed());
        sleep_interruptibly(
            Duration::from_millis(probe.interval_ms).min(remaining),
            stop,
            name,
        )?;
    }
}

fn readiness_timeout(name: &str, timeout_ms: u64) -> LifecycleError {
    LifecycleError::new(format!(
        "readiness probe for lifecycle dev process `{name}` timed out after {timeout_ms}ms"
    ))
}

fn sleep_interruptibly(
    duration: Duration,
    stop: &AtomicBool,
    name: &str,
) -> Result<(), LifecycleError> {
    let started = Instant::now();
    while started.elapsed() < duration {
        if stop.load(Ordering::SeqCst) {
            return Err(LifecycleError::canceled(format!(
                "readiness for lifecycle dev process `{name}` was canceled"
            )));
        }
        std::thread::sleep(
            Duration::from_millis(10).min(duration.saturating_sub(started.elapsed())),
        );
    }
    Ok(())
}

fn spawn_process(
    root: &Path,
    name: &str,
    process: &LifecycleDevProcess,
    generation: &LifecycleBuildReport,
    instance_id: &str,
) -> Result<Child, LifecycleError> {
    let cwd = resolve_working_dir(root, process.cwd.as_deref(), process.external_cwd)?;
    let args = process
        .args
        .iter()
        .map(|arg| expand_process_value(arg, root, name, generation))
        .collect::<Vec<_>>();
    let program = expand_process_value(&process.program, root, name, generation);
    let mut command = Command::new(&program);
    command
        .args(args)
        .current_dir(cwd)
        .envs(expand_environment(root, name, process, generation))
        .env("DISTRIBUTED_GENERATION_ID", &generation.generation_id)
        .env("DISTRIBUTED_RELEASE_ID", &generation.release_id)
        .env("DISTRIBUTED_TOPOLOGY_ID", &generation.graph_id)
        .env("DISTRIBUTED_COMPATIBILITY_ID", &generation.compatibility_id)
        .env("DISTRIBUTED_MEMBER_ID", name)
        .env("DISTRIBUTED_PROCESS_INSTANCE_ID", instance_id)
        .stdin(Stdio::null());
    #[cfg(unix)]
    command.process_group(0);
    command.spawn().map_err(|error| {
        LifecycleError::new(format!(
            "failed to start lifecycle dev process `{name}` with `{program}`: {error}"
        ))
    })
}

fn stop_child(name: &str, child: &mut Child, timeout: Duration) -> Result<(), LifecycleError> {
    terminate_process_group(name, child)?;
    if wait_for_stopped_child(child, timeout)? {
        return Ok(());
    }
    kill_process_group(name, child)?;
    if wait_for_stopped_child(child, Duration::from_secs(1))? {
        Ok(())
    } else {
        Err(LifecycleError::new(format!(
            "lifecycle dev process group `{name}` remained alive after SIGKILL"
        )))
    }
}

fn expand_process_value(
    value: &str,
    root: &Path,
    name: &str,
    generation: &LifecycleBuildReport,
) -> String {
    value
        .replace("{root}", &root.to_string_lossy())
        .replace("{process}", name)
        .replace("{generation}", &generation.generation_id)
}

fn expand_environment(
    root: &Path,
    name: &str,
    process: &LifecycleDevProcess,
    generation: &LifecycleBuildReport,
) -> BTreeMap<String, String> {
    process
        .env
        .iter()
        .map(|(key, value)| {
            (
                key.clone(),
                expand_process_value(value, root, name, generation),
            )
        })
        .collect()
}

fn resolve_working_dir(
    root: &Path,
    cwd: Option<&str>,
    external_cwd: bool,
) -> Result<PathBuf, LifecycleError> {
    let path = cwd.map_or_else(
        || root.to_path_buf(),
        |cwd| {
            let cwd = Path::new(cwd);
            if external_cwd {
                cwd.to_path_buf()
            } else {
                root.join(cwd)
            }
        },
    );
    let resolved = path.canonicalize().map_err(|error| {
        LifecycleError::new(format!(
            "failed to resolve lifecycle dev working directory `{}`: {error}",
            path.display()
        ))
    })?;
    if (!external_cwd && !resolved.starts_with(root))
        || !fs::metadata(&resolved).is_ok_and(|metadata| metadata.is_dir())
    {
        return Err(LifecycleError::new(
            "lifecycle dev working directory must be a permitted directory",
        ));
    }
    Ok(resolved)
}

fn validate_process_env(
    process_name: &str,
    environment: &BTreeMap<String, String>,
) -> Result<(), LifecycleError> {
    let valid_key = |key: &str| {
        let mut bytes = key.bytes();
        bytes
            .next()
            .is_some_and(|byte| byte == b'_' || byte.is_ascii_alphabetic())
            && bytes.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
    };
    if environment.len() > 64
        || environment
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum::<usize>()
            > 16 * 1024
        || environment
            .iter()
            .any(|(key, value)| !valid_key(key) || value.contains('\0'))
    {
        return Err(LifecycleError::new(format!(
            "lifecycle dev process `{process_name}` has invalid or oversized environment"
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn signal_process_group(child: &Child, signal: libc::c_int) -> std::io::Result<()> {
    let result = unsafe { libc::kill(-(child.id() as libc::pid_t), signal) };
    if result == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(error)
    }
}

fn process_group_is_running(child: &mut Child) -> Result<bool, LifecycleError> {
    let leader_running = child.try_wait().map_err(io_error)?.is_none();
    #[cfg(unix)]
    {
        let result = unsafe { libc::kill(-(child.id() as libc::pid_t), 0) };
        if result == 0 {
            return Ok(true);
        }
        let error = std::io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::ESRCH) => Ok(leader_running),
            Some(libc::EPERM) => Ok(true),
            _ => Err(LifecycleError::new(format!(
                "failed to inspect lifecycle dev process group: {error}"
            ))),
        }
    }
    #[cfg(not(unix))]
    {
        Ok(leader_running)
    }
}

fn wait_for_stopped_child(child: &mut Child, timeout: Duration) -> Result<bool, LifecycleError> {
    let started = Instant::now();
    loop {
        if !process_group_is_running(child)? {
            return Ok(true);
        }
        if started.elapsed() >= timeout {
            return Ok(false);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_stopped_groups(
    children: &mut BTreeMap<String, Child>,
    timeout: Duration,
) -> Result<(), LifecycleError> {
    let started = Instant::now();
    loop {
        let mut running = Vec::new();
        for (name, child) in children.iter_mut() {
            if process_group_is_running(child)? {
                running.push(name.clone());
            }
        }
        if running.is_empty() {
            return Ok(());
        }
        if started.elapsed() >= timeout {
            return Err(LifecycleError::new(format!(
                "lifecycle dev process groups remained alive after SIGKILL: {}",
                running.join(", ")
            )));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn force_stop_child(name: &str, child: &mut Child) -> Result<(), LifecycleError> {
    kill_process_group(name, child)?;
    if wait_for_stopped_child(child, Duration::from_secs(1))? {
        Ok(())
    } else {
        Err(LifecycleError::new(format!(
            "readiness probe for lifecycle dev process `{name}` remained alive after SIGKILL"
        )))
    }
}

fn terminate_process_group(name: &str, child: &mut Child) -> Result<(), LifecycleError> {
    #[cfg(unix)]
    {
        signal_process_group(child, libc::SIGTERM).map_err(|error| {
            LifecycleError::new(format!(
                "failed to stop lifecycle dev process group `{name}`: {error}"
            ))
        })
    }
    #[cfg(not(unix))]
    {
        if child.try_wait().map_err(io_error)?.is_none() {
            child.kill().map_err(|error| {
                LifecycleError::new(format!(
                    "failed to stop lifecycle dev process `{name}`: {error}"
                ))
            })?;
        }
        Ok(())
    }
}

fn kill_process_group(name: &str, child: &mut Child) -> Result<(), LifecycleError> {
    #[cfg(unix)]
    {
        signal_process_group(child, libc::SIGKILL).map_err(|error| {
            LifecycleError::new(format!(
                "failed to kill lifecycle dev process group `{name}`: {error}"
            ))
        })
    }
    #[cfg(not(unix))]
    {
        if child.try_wait().map_err(io_error)?.is_none() {
            child.kill().map_err(|error| {
                LifecycleError::new(format!(
                    "failed to kill lifecycle dev process `{name}`: {error}"
                ))
            })?;
        }
        Ok(())
    }
}

fn resolve_file(root: &Path, path: &PathBuf) -> Result<PathBuf, LifecycleError> {
    let path = if path.is_absolute() {
        path.clone()
    } else {
        root.join(path)
    };
    let resolved = path.canonicalize().map_err(|error| {
        LifecycleError::new(format!("failed to resolve `{}`: {error}", path.display()))
    })?;
    if !resolved.starts_with(root)
        || !fs::metadata(&resolved).is_ok_and(|metadata| metadata.is_file())
    {
        return Err(LifecycleError::new(
            "lifecycle dev input must be a file under the root",
        ));
    }
    Ok(resolved)
}

fn io_error(error: std::io::Error) -> LifecycleError {
    LifecycleError::new(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probed_process(timeout_ms: u64) -> LifecycleDevProcess {
        LifecycleDevProcess {
            program: "process".to_string(),
            args: Vec::new(),
            cwd: None,
            external_cwd: false,
            env: BTreeMap::new(),
            url: None,
            restart_on: BTreeSet::new(),
            ready_after_ms: 100,
            ready: Some(LifecycleDevProbe {
                program: "probe".to_string(),
                args: Vec::new(),
                interval_ms: 250,
                timeout_ms,
            }),
        }
    }

    fn config(processes: BTreeMap<String, LifecycleDevProcess>) -> LifecycleDevConfig {
        LifecycleDevConfig {
            poll_ms: 500,
            debounce_ms: 250,
            shutdown_ms: 5_000,
            prepare_ms: 30_000,
            processes,
        }
    }

    #[test]
    fn readiness_allows_cold_runtime_and_ui_probes_within_a_bounded_total() {
        config(BTreeMap::from([
            ("api".to_string(), probed_process(300_000)),
            ("ui".to_string(), probed_process(60_000)),
        ]))
        .validate()
        .unwrap();
    }

    #[test]
    fn readiness_rejects_an_unbounded_probe_or_aggregate_budget() {
        let probe_error = config(BTreeMap::from([(
            "ui".to_string(),
            probed_process(300_001),
        )]))
        .validate()
        .unwrap_err();
        assert!(probe_error.message().contains("probe is out of bounds"));

        let total_error = config(BTreeMap::from([
            ("api".to_string(), probed_process(300_000)),
            ("ui".to_string(), probed_process(61_001)),
        ]))
        .validate()
        .unwrap_err();
        assert!(total_error.message().contains("must not exceed 361s"));
    }

    #[test]
    fn external_working_directory_authority_is_discovery_only() {
        let mut discovered = probed_process(60_000);
        discovered.cwd = Some("/repository/clients/web".to_string());
        discovered.external_cwd = true;

        let serialized = serde_json::to_value(&discovered).unwrap();
        assert!(serialized.get("external_cwd").is_none());

        let mut authored = serialized;
        authored["external_cwd"] = serde_json::Value::Bool(true);
        let error = serde_json::from_value::<LifecycleDevProcess>(authored).unwrap_err();
        assert!(error.to_string().contains("unknown field `external_cwd`"));
    }

    #[test]
    fn transition_attempts_have_fresh_ids_and_isolated_acknowledgements() {
        let project = tempfile::tempdir().unwrap();
        let root = project.path().canonicalize().unwrap();
        let state = DevStateStore::new(&root, Path::new(".distributed/lifecycle")).unwrap();
        let first = state.begin_transition().unwrap();
        let second = state.begin_transition().unwrap();

        assert_ne!(first.id(), second.id());
        assert!(safe_control_id(first.id()));
        assert!(safe_control_id(second.id()));
        let first_path = first._directory.path().to_path_buf();
        let second_path = second._directory.path().to_path_buf();
        fs::write(
            first_path.join("browser_participant.json"),
            br#"{"ok":false}"#,
        )
        .unwrap();
        assert!(second_path.read_dir().unwrap().next().is_none());

        drop(first);
        assert!(!first_path.exists());
        assert!(second_path.exists());
    }
}
