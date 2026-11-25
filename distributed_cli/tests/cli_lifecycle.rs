//! Lifecycle `build` and `dev` behaviour.
//!
//! Most tests drive the lifecycle engine on a project whose two artifact nodes
//! are built by shell scripts, so every step is deterministic and fast. The
//! tests at the end run the `distributed` binary on a real Cargo project.

use distributed_cli::{
    run_lifecycle_project_build, run_lifecycle_project_dev, LifecycleActivation,
    LifecycleBuildReport, LifecycleBuildRequest, LifecycleDevReport, LifecycleError,
    LifecycleProjectDevOptions, LifecycleProjectPlan,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use tempfile::TempDir;

/// A directory of this test's own. The process ID keeps concurrent test
/// binaries apart and the counter keeps the tests of this one apart.
fn temporary_directory(label: &str) -> TempDir {
    static CREATED: AtomicUsize = AtomicUsize::new(0);
    tempfile::Builder::new()
        .prefix(&format!(
            "distributed-lifecycle-{label}-{}-{}-",
            std::process::id(),
            CREATED.fetch_add(1, Ordering::Relaxed)
        ))
        .tempdir()
        .expect("create exclusive lifecycle fixture root")
}

/// A lifecycle project: its workspace on disk, and the catalog and
/// configuration that project discovery would otherwise derive.
struct Fixture {
    _directory: TempDir,
    root: PathBuf,
    catalog: Value,
    config: Value,
}

impl Fixture {
    fn new(label: &str) -> Self {
        let directory = temporary_directory(label);
        let root = directory
            .path()
            .canonicalize()
            .expect("resolve lifecycle fixture root");
        fs::create_dir_all(root.join("src")).expect("create lifecycle fixture source");
        let (catalog, config) = write_fixture(&root);
        Self {
            _directory: directory,
            root,
            catalog,
            config,
        }
    }

    fn plan(&self) -> LifecycleProjectPlan {
        LifecycleProjectPlan {
            root: self.root.clone(),
            catalog: serde_json::from_value(self.catalog.clone()).expect("parse lifecycle catalog"),
            config: serde_json::from_value(self.config.clone()).expect("parse lifecycle config"),
            out: "dist/distributed".into(),
            cli_executable: env!("CARGO_BIN_EXE_distributed").into(),
        }
    }

    /// What `distributed build [--check]` does with the plan.
    fn build(&self, check: bool) -> Result<LifecycleBuildReport, LifecycleError> {
        run_lifecycle_project_build(&self.plan(), &build_request(check))
    }
}

fn build_request(check: bool) -> LifecycleBuildRequest {
    LifecycleBuildRequest {
        check,
        lock_timeout: Duration::from_secs(1),
        nodes: None,
        activation_inputs: None,
        cancel: None,
        activation: LifecycleActivation::Immediate,
    }
}

/// Writes the workspace and returns its catalog and lifecycle configuration.
fn write_fixture(root: &Path) -> (Value, Value) {
    fs::write(root.join("src/input.txt"), "first\n").expect("write lifecycle input");
    fs::create_dir_all(root.join("plan")).expect("create plan input directory");
    fs::write(root.join("plan/input.txt"), "local\n").expect("write plan input");
    fs::write(
        root.join("build-app.sh"),
        r#"#!/bin/sh
set -eu
root="$1"
stage="$2"
sed 's/^/application:/' "$root/src/input.txt"
if test -f "$root/produce-unowned"; then
  printf 'unowned\n' > "$stage/unowned.txt"
fi
"#,
    )
    .expect("write application executor");
    fs::write(
        root.join("build-plan.sh"),
        r#"#!/bin/sh
set -eu
root="$1"
stage="$2"
if test -f "$root/fail-plan"; then
  printf 'injected downstream failure\n' >&2
  exit 17
fi
if test -f "$root/slow-plan"; then
  /bin/sh -c 'trap "" TERM; while :; do sleep 1; done' &
  printf '%s %s\n' "$$" "$!" >> "$root/build-plan-family.log"
  printf 'started\n' >> "$root/build-plan-starts.log"
  while test -f "$root/slow-plan"; do :; done
fi
printf 'plan:' > "$stage/generated/plan.json"
tr -d '\n' < "$root/plan/input.txt" >> "$stage/generated/plan.json"
printf ':' >> "$stage/generated/plan.json"
cat "$stage/generated/application.json" >> "$stage/generated/plan.json"
"#,
    )
    .expect("write plan executor");
    fs::create_dir_all(root.join("generated")).expect("create tracked output directory");
    fs::write(
        root.join("generated/application.json"),
        "application:first\n",
    )
    .expect("write accepted application output");
    fs::write(
        root.join("generated/plan.json"),
        "plan:local:application:first\n",
    )
    .expect("write accepted plan output");

    let catalog = serde_json::json!({
        "schema_version": 1,
        "entries": {
            "application": {
                "id": "application",
                "kind": "application_manifest",
                "scope": { "id": "application/fixture" },
                "owner": "application/fixture",
                "identity": { "kind": "application_manifest", "value": "ref:application" },
                "provenance": {
                    "sources": ["src/input.txt"],
                    "generator": "fixture.application"
                },
                "outputs": { "manifest": "generated/application.json" },
                "lifecycle": ["build", "check", "dev"]
            },
            "plan": {
                "id": "plan",
                "kind": "deployment_plan",
                "scope": { "id": "deployment/fixture" },
                "owner": "deployment/fixture",
                "identity": { "kind": "deployment_plan", "value": "ref:plan" },
                "provenance": {
                    "sources": ["generated/application.json", "plan/input.txt"],
                    "generator": "fixture.plan"
                },
                "predecessor": {
                    "entry_id": "application",
                    "identity": { "kind": "application_manifest", "value": "ref:application" }
                },
                "outputs": { "plan": "generated/plan.json" },
                "lifecycle": ["build", "check", "dev"]
            }
        }
    });
    let identity = format!("sha256:{}", "1".repeat(64));
    let source_identity = format!("sha256:{}", "a".repeat(64));
    let config = serde_json::json!({
        "schema_version": 1,
        "application": "fixture",
        "source": {
            "rust": source_identity,
            "cli": format!("sha256:{}", "a".repeat(64)),
            "javascript": format!("sha256:{}", "a".repeat(64))
        },
        "roots": ["plan"],
        "executors": {
            "fixture.application": {
                "identity": identity,
                "program": "/bin/sh",
                "args": ["{root}/build-app.sh", "{root}", "{stage}"],
                "stdout": "generated/application.json"
            },
            "fixture.plan": {
                "identity": format!("sha256:{}", "2".repeat(64)),
                "program": "/bin/sh",
                "args": ["{root}/build-plan.sh", "{root}", "{stage}"]
            }
        }
    });
    (catalog, config)
}

/// The report as `distributed build --output json` prints it.
fn report(report: &LifecycleBuildReport) -> Value {
    serde_json::to_value(report).expect("serialize lifecycle report")
}

fn enable_dev(fixture: &mut Fixture) {
    let root = &fixture.root;
    fs::write(
        root.join("dev-child.sh"),
        r#"#!/bin/sh
set -eu
root="$1"
name="$2"
if test -f "$root/dist/distributed/active.json"; then
  barrier=present
else
  barrier=missing
fi
printf '%s:%s:%s\n' "$name" "$barrier" "$DISTRIBUTED_GENERATION_ID" >> "$root/dev-process.log"
printf '%s:%s:%s\n' "$name" "$DEV_FIXTURE_NAME" "$PWD" >> "$root/dev-environment.log"
printf '%s:%s\n' "$name" "$DISTRIBUTED_PROCESS_INSTANCE_ID" >> "$root/dev-instances.log"
trap 'exit 0' TERM
/bin/sh -c 'trap "" TERM; while :; do sleep 1; done' &
descendant=$!
printf '%s %s\n' "$$" "$descendant" >> "$root/dev-family.log"
printf '%s\n' "$descendant" >> "$root/dev-descendants.log"
wait "$descendant"
"#,
    )
    .expect("write dev child fixture");
    fixture.config["dev"] = serde_json::json!({
        "poll_ms": 20,
        "debounce_ms": 30,
        "shutdown_ms": 100,
        "processes": {
            "api": {
                "program": "/bin/sh",
                "args": ["{root}/dev-child.sh", "{root}", "{process}"],
                "cwd": "src",
                "env": { "DEV_FIXTURE_NAME": "{process}" },
                "url": "http://localhost:8000",
                "restart_on": ["application"],
                "ready_after_ms": 20,
                "ready": {
                    "program": "/bin/test",
                    "args": ["-f", "{root}/dist/distributed/active.json"],
                    "interval_ms": 10,
                    "timeout_ms": 200
                }
            },
            "ui": {
                "program": "/bin/sh",
                "args": ["{root}/dev-child.sh", "{root}", "{process}"],
                "cwd": "src",
                "env": { "DEV_FIXTURE_NAME": "{process}" },
                "restart_on": [],
                "ready_after_ms": 20
            }
        }
    });
}

fn configure_blocking_readiness_probe(fixture: &mut Fixture, timeout_ms: u64) {
    let root = &fixture.root;
    fs::write(
        root.join("readiness-probe.sh"),
        r#"#!/bin/sh
set -eu
root="$1"
printf '%s\n' "$$" > "$root/readiness-probe-leader.pid"
/bin/sh -c 'trap "" TERM; sleep 10' &
descendant=$!
printf '%s\n' "$descendant" > "$root/readiness-probe-descendant.pid"
wait "$descendant"
exit 1
"#,
    )
    .expect("write blocking readiness probe");
    fixture.config["dev"]["processes"]["api"]["ready"] = serde_json::json!({
        "program": "/bin/sh",
        "args": ["{root}/readiness-probe.sh", "{root}"],
        "interval_ms": 10,
        "timeout_ms": timeout_ms
    });
}

fn configure_generation_rejecting_probe(fixture: &mut Fixture) {
    let root = &fixture.root;
    fs::write(
        root.join("generation-probe.sh"),
        r#"#!/bin/sh
set -eu
root="$1"
baseline="$root/accepted-generation.txt"
if test ! -f "$baseline"; then
  printf '%s\n' "$DISTRIBUTED_GENERATION_ID" > "$baseline"
fi
if test "$(cat "$baseline")" = "$DISTRIBUTED_GENERATION_ID"; then
  printf '%s\n' "$DISTRIBUTED_GENERATION_ID" >> "$root/accepted-readiness.log"
  exit 0
fi
exit 1
"#,
    )
    .expect("write generation readiness probe");
    let config = &mut fixture.config;
    config["dev"]["prepare_ms"] = serde_json::json!(100);
    // The probe rejects a replacement by never passing, so this budget is
    // also how long a rejection takes. It must still leave headroom for the
    // initial generation's probe (a `/bin/sh` spawn) under a loaded
    // `cargo test --workspace`: at 100 ms that probe timed out and the
    // supervisor failed before the test's first assertion.
    config["dev"]["processes"]["api"]["ready"] = serde_json::json!({
        "program": "/bin/sh",
        "args": ["{root}/generation-probe.sh", "{root}"],
        "interval_ms": 10,
        "timeout_ms": 1500
    });
}

#[track_caller]
fn wait_until(timeout: Duration, predicate: impl Fn() -> bool) {
    let started = std::time::Instant::now();
    while !predicate() {
        assert!(
            started.elapsed() < timeout,
            "timed out waiting for fixture state"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

struct DevSupervisor {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<Result<LifecycleDevReport, LifecycleError>>>,
}

impl DevSupervisor {
    fn start(fixture: &Fixture) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let supervisor_stop = Arc::clone(&stop);
        let project = fixture.plan();
        let handle = thread::spawn(move || {
            run_lifecycle_project_dev(&LifecycleProjectDevOptions {
                project,
                build: build_request(false),
                stop: supervisor_stop,
                progress: false,
            })
        });
        Self {
            stop,
            handle: Some(handle),
        }
    }

    fn join_result(mut self) -> Result<LifecycleDevReport, LifecycleError> {
        self.handle
            .take()
            .expect("lifecycle supervisor handle")
            .join()
            .expect("join lifecycle supervisor")
    }

    fn stop_and_join_result(self) -> Result<LifecycleDevReport, LifecycleError> {
        self.stop.store(true, Ordering::SeqCst);
        self.join_result()
    }

    fn stop_and_join(self) -> LifecycleDevReport {
        self.stop_and_join_result()
            .expect("lifecycle supervisor succeeds")
    }
}

impl Drop for DevSupervisor {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn wait_for_stable_file(path: &Path, timeout: Duration) -> Vec<u8> {
    let started = std::time::Instant::now();
    let mut previous = None;
    let mut stable_observations = 0;
    loop {
        if let Ok(bytes) = fs::read(path) {
            if previous.as_ref() == Some(&bytes) {
                stable_observations += 1;
                if stable_observations >= 2 {
                    return bytes;
                }
            } else {
                previous = Some(bytes);
                stable_observations = 0;
            }
        }
        assert!(
            started.elapsed() < timeout,
            "timed out waiting for stable fixture file `{}`",
            path.display()
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn file_snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, path: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        let mut entries = fs::read_dir(path)
            .expect("read snapshot directory")
            .collect::<Result<Vec<_>, _>>()
            .expect("read snapshot entries");
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let kind = entry.file_type().expect("inspect snapshot entry");
            if kind.is_dir() {
                visit(root, &entry.path(), files);
            } else if kind.is_file() {
                files.insert(
                    entry.path().strip_prefix(root).unwrap().to_path_buf(),
                    fs::read(entry.path()).expect("read snapshot file"),
                );
            }
        }
    }
    let mut files = BTreeMap::new();
    visit(root, root, &mut files);
    files
}

#[test]
fn build_is_deterministic_and_check_is_read_only_and_content_based() {
    let fixture = Fixture::new("deterministic");
    let root = fixture.root.clone();

    let first = report(&fixture.build(false).expect("first build"));
    let active_before = fs::read(root.join("dist/distributed/active.json"))
        .expect("read active generation pointer");
    let generation = first["generation_id"].as_str().expect("generation ID");
    assert!(root
        .join("dist/distributed/generations")
        .join(generation)
        .join("generation.json")
        .is_file());
    assert!(root
        .join("dist/distributed/generations")
        .join(generation)
        .join("release.json")
        .is_file());

    fs::write(root.join("src/input.txt"), "first\n").expect("rewrite identical content");
    let second = report(&fixture.build(false).expect("second build"));
    assert_eq!(first["generation_id"], second["generation_id"]);
    assert_eq!(first["release_id"], second["release_id"]);

    let snapshot_before_check = file_snapshot(&root);
    let check = fixture.build(true).expect("check");
    assert!(check.ok, "`distributed build --check` exits 0");
    assert!(report(&check)["drift"].as_array().unwrap().is_empty());
    assert_eq!(
        active_before,
        fs::read(root.join("dist/distributed/active.json")).unwrap()
    );
    assert_eq!(snapshot_before_check, file_snapshot(&root));
}

#[test]
fn partial_build_reuses_verified_upstream_receipts() {
    let fixture = Fixture::new("partial");
    let root = fixture.root.clone();
    let project = fixture.plan();
    let first = run_lifecycle_project_build(&project, &build_request(false)).unwrap();
    let first_manifest: Value = serde_json::from_slice(
        &fs::read(
            root.join("dist/distributed/generations")
                .join(&first.generation_id)
                .join("generation.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let old_inputs = BTreeMap::from([
        (
            "src/input.txt".to_string(),
            first_manifest["receipts"]["application"]["input_identities"]["src/input.txt"]
                .as_str()
                .unwrap()
                .to_string(),
        ),
        (
            "plan/input.txt".to_string(),
            first_manifest["receipts"]["plan"]["input_identities"]["plan/input.txt"]
                .as_str()
                .unwrap()
                .to_string(),
        ),
    ]);
    fs::write(root.join("plan/input.txt"), "split\n").unwrap();
    let mut partial = build_request(false);
    partial.nodes = Some(["plan".to_string()].into_iter().collect());
    partial.activation_inputs = Some(old_inputs);
    let superseded = run_lifecycle_project_build(&project, &partial).unwrap_err();
    assert!(superseded.message().contains("superseded"));
    assert!(
        fs::read_to_string(root.join("dist/distributed/active.json"))
            .unwrap()
            .contains(&first.generation_id)
    );
    partial.activation_inputs = None;
    let second = run_lifecycle_project_build(&project, &partial).unwrap();
    assert_eq!(second.executed, ["plan"]);
    assert_ne!(first.generation_id, second.generation_id);

    // Activating the second generation removed the first; its manifest was
    // read while it was active.
    let second_manifest: Value = serde_json::from_slice(
        &fs::read(
            root.join("dist/distributed/generations")
                .join(&second.generation_id)
                .join("generation.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        first_manifest["receipts"]["application"],
        second_manifest["receipts"]["application"]
    );
    assert!(!root
        .join("dist/distributed/generations")
        .join(&first.generation_id)
        .exists());
}

#[test]
fn mixed_source_and_unowned_outputs_fail_before_activation() {
    let mut mixed = Fixture::new("mixed-source");
    mixed.config["source"]["javascript"] = Value::String(format!("sha256:{}", "b".repeat(64)));
    let mixed_root = mixed.root.clone();
    let diagnostic = mixed
        .build(false)
        .expect_err("mixed framework sources are refused")
        .to_string();
    assert!(diagnostic.contains(&format!("sha256:{}", "a".repeat(64))));
    assert!(diagnostic.contains(&format!("sha256:{}", "b".repeat(64))));
    assert!(!mixed_root.join("dist/distributed/active.json").exists());

    let unowned = Fixture::new("unowned-output");
    let unowned_root = unowned.root.clone();
    fs::write(unowned_root.join("produce-unowned"), "yes\n").unwrap();
    let diagnostic = unowned
        .build(false)
        .expect_err("an unowned output is refused")
        .to_string();
    assert!(diagnostic.contains("unowned staged output `unowned.txt`"));
    assert!(!unowned_root.join("dist/distributed/active.json").exists());
}

#[test]
fn dev_waits_for_initial_generation_and_restarts_only_invalidated_processes() {
    let mut fixture = Fixture::new("dev-supervisor");
    enable_dev(&mut fixture);
    let root = fixture.root.clone();
    let supervisor = DevSupervisor::start(&fixture);

    wait_until(Duration::from_secs(5), || {
        fs::read_to_string(root.join("dev-process.log")).is_ok_and(|log| log.lines().count() == 2)
            && fs::read_to_string(root.join("dev-environment.log"))
                .is_ok_and(|log| log.lines().count() == 2)
    });
    let initial_log = fs::read_to_string(root.join("dev-process.log")).unwrap();
    assert!(initial_log
        .lines()
        .all(|line| line.contains(":present:sha256:")));
    let environment_log = fs::read_to_string(root.join("dev-environment.log")).unwrap();
    let expected_cwd = root.join("src").canonicalize().unwrap();
    assert!(environment_log
        .lines()
        .any(|line| line == format!("api:api:{}", expected_cwd.display())));
    assert!(environment_log
        .lines()
        .any(|line| line == format!("ui:ui:{}", expected_cwd.display())));
    wait_for_stable_file(
        &root.join("dist/distributed/active.json"),
        Duration::from_secs(5),
    );
    wait_until(Duration::from_secs(5), || {
        dev_state(&root)["members"]["api"].is_object()
    });
    let initial_state = dev_state(&root);
    let instances = fs::read_to_string(root.join("dev-instances.log")).unwrap();
    for name in ["api", "ui"] {
        assert!(instances.lines().any(|line| line
            == format!(
                "{name}:{}",
                initial_state["members"][name]["instanceId"]
                    .as_str()
                    .unwrap()
            )));
    }
    let mut previous_generation = initial_state["active"]["generationId"].clone();
    // Changes outside either process's restart inventory retain both instances
    // while their authorized application generation advances repeatedly.
    for input in ["client-one", "client-two"] {
        fs::write(root.join("plan/input.txt"), input).unwrap();
        wait_until(Duration::from_secs(5), || {
            let state = dev_state(&root);
            state["phase"] == "active" && state["active"]["generationId"] != previous_generation
        });
        let state = dev_state(&root);
        assert_eq!(state["members"], initial_state["members"]);
        previous_generation = state["active"]["generationId"].clone();
    }
    fs::write(root.join("src/input.txt"), "second\n").unwrap();
    wait_until(Duration::from_secs(5), || {
        fs::read_to_string(root.join("dev-process.log")).is_ok_and(|log| log.lines().count() == 3)
    });
    wait_until(Duration::from_secs(5), || {
        let state = dev_state(&root);
        state["phase"] == "active" && state["active"]["generationId"] != previous_generation
    });
    let replaced = dev_state(&root);
    assert_ne!(
        replaced["members"]["api"]["instanceId"],
        initial_state["members"]["api"]["instanceId"]
    );
    assert_eq!(
        replaced["members"]["api"]["generationId"],
        replaced["active"]["generationId"]
    );
    assert_eq!(replaced["members"]["ui"], initial_state["members"]["ui"]);
    // Four generations were activated. The one in between is unused and gone:
    // what stays is the active one, the one it replaced, and the first, which
    // the `ui` process was started with and still resolves.
    let installed = fs::read_dir(root.join("dist/distributed/generations"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    let generation = |state: &Value| {
        state["active"]["generationId"]
            .as_str()
            .unwrap()
            .to_string()
    };
    assert_eq!(
        installed,
        [
            generation(&initial_state),
            previous_generation.as_str().unwrap().to_string(),
            generation(&replaced),
        ]
        .into_iter()
        .collect()
    );
    let report = supervisor.stop_and_join();
    assert_eq!(report.rebuilds, 3);
    assert!(!root.join("dist/distributed/dev.json").exists());
    assert_eq!(report.restarts["api"], 1);
    assert_eq!(report.restarts["ui"], 0);
    let log = fs::read_to_string(root.join("dev-process.log")).unwrap();
    assert_eq!(
        log.lines().filter(|line| line.starts_with("api:")).count(),
        2
    );
    assert_eq!(
        log.lines().filter(|line| line.starts_with("ui:")).count(),
        1
    );
    #[cfg(unix)]
    for pid in fs::read_to_string(root.join("dev-descendants.log"))
        .unwrap()
        .lines()
    {
        let status = Command::new("/bin/kill")
            .args(["-0", pid])
            .stderr(Stdio::null())
            .status()
            .expect("inspect lifecycle descendant");
        assert!(!status.success(), "descendant process {pid} leaked");
    }
}

fn dev_state(root: &Path) -> Value {
    fs::read(root.join("dist/distributed/dev.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or(Value::Null)
}

#[test]
fn dev_readiness_failure_restores_processes_and_preserves_active_pointer() {
    let mut fixture = Fixture::new("dev-rollback");
    enable_dev(&mut fixture);
    let root = fixture.root.clone();
    configure_generation_rejecting_probe(&mut fixture);
    let supervisor = DevSupervisor::start(&fixture);
    wait_until(Duration::from_secs(5), || {
        fs::read_to_string(root.join("dev-process.log")).is_ok_and(|log| log.lines().count() == 2)
    });
    let active_before = wait_for_stable_file(
        &root.join("dist/distributed/active.json"),
        Duration::from_secs(5),
    );
    wait_until(Duration::from_secs(5), || {
        dev_state(&root)["members"]["api"].is_object()
    });
    let initial_members = dev_state(&root)["members"].clone();

    fs::write(root.join("src/input.txt"), "replacement\n").unwrap();
    wait_until(Duration::from_secs(5), || {
        fs::read_to_string(root.join("dev-process.log"))
            .is_ok_and(|log| log.lines().filter(|line| line.starts_with("api:")).count() == 3)
    });
    wait_until(Duration::from_secs(5), || {
        fs::read_to_string(root.join("accepted-readiness.log"))
            .is_ok_and(|log| log.lines().count() == 2)
    });
    wait_until(Duration::from_secs(5), || {
        let state = dev_state(&root);
        state["phase"] == "active"
            && state["members"]["api"]["instanceId"] != initial_members["api"]["instanceId"]
    });
    let rollback = dev_state(&root);
    assert_eq!(
        rollback["members"]["api"]["generationId"],
        initial_members["api"]["generationId"]
    );
    assert_eq!(rollback["members"]["ui"], initial_members["ui"]);
    let report = supervisor.stop_and_join();
    assert_eq!(report.initial_generation, report.final_generation);
    assert_eq!(
        active_before,
        fs::read(root.join("dist/distributed/active.json")).unwrap()
    );
    let log = fs::read_to_string(root.join("dev-process.log")).unwrap();
    assert_eq!(
        log.lines().filter(|line| line.starts_with("api:")).count(),
        3,
        "initial, rejected replacement, and restored prior process must all start"
    );
    assert_eq!(
        log.lines().filter(|line| line.starts_with("ui:")).count(),
        1
    );
}

#[test]
fn dev_browser_prepare_timeout_preserves_active_pointer_and_processes() {
    let mut fixture = Fixture::new("dev-prepare-timeout");
    enable_dev(&mut fixture);
    let root = fixture.root.clone();
    fixture.config["dev"]["prepare_ms"] = serde_json::json!(100);
    let supervisor = DevSupervisor::start(&fixture);
    wait_until(Duration::from_secs(5), || {
        fs::read_to_string(root.join("dev-process.log")).is_ok_and(|log| log.lines().count() == 2)
    });
    let active_before = wait_for_stable_file(
        &root.join("dist/distributed/active.json"),
        Duration::from_secs(5),
    );
    let participants = root.join("dist/distributed/dev-control/participants");
    fs::create_dir_all(&participants).unwrap();
    fs::write(
        participants.join("browserparticipant123.json"),
        br#"{"seenAtUnixMs":1}"#,
    )
    .unwrap();

    fs::write(root.join("src/input.txt"), "replacement\n").unwrap();
    wait_until(Duration::from_secs(5), || {
        fs::read_dir(root.join("dist/distributed/generations"))
            .is_ok_and(|entries| entries.count() > 1)
    });
    thread::sleep(Duration::from_millis(250));
    let report = supervisor.stop_and_join();
    assert_eq!(report.initial_generation, report.final_generation);
    assert_eq!(
        active_before,
        fs::read(root.join("dist/distributed/active.json")).unwrap()
    );
    let log = fs::read_to_string(root.join("dev-process.log")).unwrap();
    assert_eq!(
        log.lines().filter(|line| line.starts_with("api:")).count(),
        1
    );
    assert_eq!(
        log.lines().filter(|line| line.starts_with("ui:")).count(),
        1
    );
}

#[test]
fn dev_rebuilds_source_edited_while_initial_process_readiness_is_held() {
    let mut fixture = Fixture::new("dev-startup-edit");
    enable_dev(&mut fixture);
    let root = fixture.root.clone();
    fixture.config["dev"]["processes"]["api"]["ready"] = serde_json::json!({
        "program": "/bin/test",
        "args": ["-f", "{root}/release-readiness"],
        "interval_ms": 10,
        "timeout_ms": 5000
    });
    let supervisor = DevSupervisor::start(&fixture);
    wait_until(Duration::from_secs(5), || {
        root.join("dev-process.log").exists()
    });
    let before = wait_for_stable_file(
        &root.join("dist/distributed/active.json"),
        Duration::from_secs(5),
    );
    // The initial build is complete but the supervisor cannot finish startup.
    fs::write(root.join("src/input.txt"), "edited-during-startup\n").unwrap();
    fs::write(root.join("release-readiness"), "release\n").unwrap();
    wait_until(Duration::from_secs(5), || {
        fs::read(root.join("dist/distributed/active.json")).is_ok_and(|active| active != before)
    });
    let report = supervisor.stop_and_join();
    assert_ne!(report.initial_generation, report.final_generation);
    assert_eq!(report.rebuilds, 1);
    assert_eq!(report.restarts["api"], 1);
    assert_eq!(report.restarts["ui"], 0);
    let active: Value =
        serde_json::from_slice(&fs::read(root.join("dist/distributed/active.json")).unwrap())
            .unwrap();
    let artifact = root
        .join("dist/distributed")
        .join(active["path"].as_str().unwrap())
        .join("generated/application.json");
    assert_eq!(
        fs::read_to_string(artifact).unwrap(),
        "application:edited-during-startup\n"
    );
}

#[test]
fn dev_cancels_a_superseded_executor_before_activation() {
    let mut fixture = Fixture::new("dev-cancel");
    enable_dev(&mut fixture);
    let root = fixture.root.clone();
    let supervisor = DevSupervisor::start(&fixture);
    wait_until(Duration::from_secs(5), || {
        fs::read_to_string(root.join("dev-process.log")).is_ok_and(|log| log.lines().count() == 2)
    });
    let initial_active = wait_for_stable_file(
        &root.join("dist/distributed/active.json"),
        Duration::from_secs(5),
    );

    fs::write(root.join("slow-plan"), "yes\n").unwrap();
    fs::write(root.join("plan/input.txt"), "obsolete\n").unwrap();
    wait_until(Duration::from_secs(5), || {
        root.join("build-plan-starts.log").exists()
    });
    let canceled = recorded_pids(&root.join("build-plan-family.log"));
    assert_eq!(canceled.len(), 2, "the executor and its descendant");
    fs::write(root.join("plan/input.txt"), "latest\n").unwrap();
    wait_until(Duration::from_secs(5), || {
        fs::read_to_string(root.join("build-plan-starts.log"))
            .is_ok_and(|log| log.lines().count() >= 2)
    });
    assert_no_survivors(&canceled);
    assert_eq!(
        initial_active,
        fs::read(root.join("dist/distributed/active.json")).unwrap()
    );
    fs::remove_file(root.join("slow-plan")).unwrap();
    wait_until(Duration::from_secs(5), || {
        fs::read(root.join("dist/distributed/active.json"))
            .is_ok_and(|active| active != initial_active)
    });
    let report = supervisor.stop_and_join();
    assert_eq!(report.rebuilds, 1);
    assert_eq!(report.restarts["api"], 0);
    assert_eq!(report.restarts["ui"], 0);
    // The executor that completed left a descendant behind; it is gone too.
    assert_no_survivors(&recorded_pids(&root.join("build-plan-family.log")));
    assert_no_survivors(&recorded_pids(&root.join("dev-family.log")));
    let active: Value =
        serde_json::from_slice(&fs::read(root.join("dist/distributed/active.json")).unwrap())
            .unwrap();
    let plan = fs::read_to_string(
        root.join("dist/distributed")
            .join(active["path"].as_str().unwrap())
            .join("generated/plan.json"),
    )
    .unwrap();
    assert!(plan.contains("plan:latest:"));
}

#[test]
fn dev_stop_cancels_the_initial_build_before_process_startup() {
    let mut fixture = Fixture::new("dev-initial-cancel");
    enable_dev(&mut fixture);
    let root = fixture.root.clone();
    fs::write(root.join("slow-plan"), "yes\n").unwrap();
    let supervisor = DevSupervisor::start(&fixture);
    wait_until(Duration::from_secs(5), || {
        root.join("build-plan-starts.log").exists()
    });

    let started = std::time::Instant::now();
    let error = supervisor
        .stop_and_join_result()
        .expect_err("initial build should report cancellation");
    // The plan never finishes by itself; the bound only has to be finite.
    assert!(started.elapsed() < Duration::from_secs(10));
    assert!(error.message().contains("was canceled"));
    assert!(!root.join("dev-process.log").exists());
}

#[test]
fn dev_enforces_timeout_while_readiness_probe_is_running() {
    let mut fixture = Fixture::new("dev-readiness-timeout");
    enable_dev(&mut fixture);
    let root = fixture.root.clone();
    configure_blocking_readiness_probe(&mut fixture, 100);

    let started = std::time::Instant::now();
    let error = DevSupervisor::start(&fixture)
        .join_result()
        .expect_err("blocked readiness probe should time out");
    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(error.message().contains("timed out after 100ms"));
    assert_fixture_processes_gone(&root, "readiness-probe");
}

#[test]
fn dev_stop_cancels_a_running_readiness_probe() {
    let mut fixture = Fixture::new("dev-readiness-cancel");
    enable_dev(&mut fixture);
    let root = fixture.root.clone();
    configure_blocking_readiness_probe(&mut fixture, 2_000);
    let supervisor = DevSupervisor::start(&fixture);
    wait_until(Duration::from_secs(5), || {
        root.join("readiness-probe-descendant.pid").exists()
    });

    let started = std::time::Instant::now();
    let error = supervisor
        .stop_and_join_result()
        .expect_err("readiness cancellation should be reported");
    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(error.message().contains("readiness") && error.message().contains("canceled"));
    assert_fixture_processes_gone(&root, "readiness-probe");
}

#[cfg(unix)]
fn assert_fixture_processes_gone(root: &Path, prefix: &str) {
    let mut alive = Vec::new();
    for label in ["leader", "descendant"] {
        let path = root.join(format!("{prefix}-{label}.pid"));
        let Ok(pid) = fs::read_to_string(path) else {
            continue;
        };
        let pid = pid.trim().parse::<libc::pid_t>().expect("fixture PID");
        if unsafe { libc::kill(pid, 0) } == 0 {
            alive.push(pid);
        }
    }
    for pid in &alive {
        unsafe {
            libc::kill(*pid, libc::SIGKILL);
        }
    }
    assert!(alive.is_empty(), "fixture processes leaked: {alive:?}");
}

#[cfg(not(unix))]
fn assert_fixture_processes_gone(_root: &Path, _prefix: &str) {}

#[test]
fn stale_check_names_owner_and_failed_downstream_build_preserves_active_generation() {
    let fixture = Fixture::new("failure");
    let root = fixture.root.clone();
    fixture.build(false).expect("baseline build");
    let active_before = fs::read(root.join("dist/distributed/active.json")).unwrap();

    // The plan no longer builds what the active generation holds.
    fs::write(root.join("plan/input.txt"), "stale\n").expect("change the plan input");
    let check = fixture.build(true).expect("check");
    assert!(!check.ok, "`distributed build --check` exits 1");
    let check = report(&check);
    assert_eq!(check["drift"].as_array().unwrap().len(), 1);
    assert_eq!(check["drift"][0]["node_id"], "plan");
    assert_eq!(check["drift"][0]["output"], "generated/plan.json");
    assert!(check["drift"][0]["built_identity"]
        .as_str()
        .unwrap()
        .starts_with("sha256:"));
    assert!(check["drift"][0]["workspace_identity"]
        .as_str()
        .unwrap()
        .starts_with("sha256:"));

    fs::write(root.join("src/input.txt"), "second\n").expect("change lifecycle source");
    fs::write(root.join("fail-plan"), "fail\n").expect("inject downstream failure");
    let failed_diagnostic = fixture
        .build(false)
        .expect_err("the plan executor fails")
        .to_string();
    assert!(failed_diagnostic.contains("lifecycle node `plan`"));
    assert!(failed_diagnostic.contains("injected downstream failure"));
    assert_eq!(
        active_before,
        fs::read(root.join("dist/distributed/active.json")).unwrap()
    );
}

/// Every process ID a fixture script appended to `path`.
#[cfg(unix)]
fn recorded_pids(path: &Path) -> Vec<libc::pid_t> {
    fs::read_to_string(path)
        .unwrap_or_default()
        .split_whitespace()
        .map(|pid| pid.parse().expect("fixture process ID"))
        .collect()
}

/// Polls until none of `pids` can be signalled. None of them is a child of
/// this test process once its owner has reaped or exited, so a process that
/// still answers is alive, not a zombie waiting for this test.
#[cfg(unix)]
fn assert_no_survivors(pids: &[libc::pid_t]) {
    let started = std::time::Instant::now();
    loop {
        let alive = pids
            .iter()
            .copied()
            .filter(|pid| unsafe { libc::kill(*pid, 0) } == 0)
            .collect::<Vec<_>>();
        if alive.is_empty() {
            return;
        }
        if started.elapsed() > Duration::from_secs(10) {
            for pid in &alive {
                unsafe { libc::kill(*pid, libc::SIGKILL) };
            }
            panic!("processes outlived their session: {alive:?}");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

/// The lifecycle test project (`tests/fixtures/lifecycle-project`) in a
/// directory of its own: a Cargo workspace that `distributed dev <project>`
/// discovers as any project. Its API and UI listen on ports of their own.
#[cfg(unix)]
struct Project {
    _directory: TempDir,
    root: PathBuf,
    /// Projects share one Cargo target directory, so they run one at a time.
    _turn: std::sync::MutexGuard<'static, ()>,
}

#[cfg(unix)]
impl Project {
    fn new(label: &str) -> Self {
        static TURN: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let turn = TURN.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let directory = temporary_directory(label);
        let root = directory
            .path()
            .canonicalize()
            .expect("resolve lifecycle project root");
        let listeners = [(); 2]
            .map(|()| std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a loopback port"));
        let [api_port, ui_port] =
            listeners.map(|listener| listener.local_addr().expect("reserved port").port());
        let framework = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("the CLI is a member of the framework workspace");
        let fixture =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/lifecycle-project");
        for file in [
            "Cargo.toml",
            "service/Cargo.toml",
            "service/src/lib.rs",
            "service/src/name.txt",
            "runner/Cargo.toml",
            "runner/src/main.rs",
            "ui/Cargo.toml",
            "ui/src/main.rs",
            "ui/package.json",
        ] {
            let text = fs::read_to_string(fixture.join(file))
                .expect("read lifecycle project file")
                .replace("@DISTRIBUTED@", framework.to_str().expect("UTF-8 path"))
                .replace("@API_PORT@", &api_port.to_string())
                .replace("@UI_PORT@", &ui_port.to_string());
            let target = root.join(file);
            fs::create_dir_all(target.parent().unwrap()).expect("create lifecycle project");
            fs::write(target, text).expect("write lifecycle project file");
        }
        // The framework's lock file resolves the project without the network.
        fs::copy(framework.join("Cargo.lock"), root.join("Cargo.lock"))
            .expect("copy the framework lock file");
        Self {
            _directory: directory,
            root,
            _turn: turn,
        }
    }

    /// `distributed dev <project>`, with its output in `dev.log`.
    fn dev(&self) -> DevProcess {
        let log = self.root.join("dev.log");
        let output = fs::File::create(&log).expect("create dev log");
        let mut command = Command::new(env!("CARGO_BIN_EXE_distributed"));
        command
            .arg("dev")
            .arg(&self.root)
            .env(
                "CARGO_TARGET_DIR",
                Path::new(env!("CARGO_TARGET_TMPDIR")).join("lifecycle-project"),
            )
            .stdin(Stdio::null())
            .stdout(output.try_clone().expect("share dev log"))
            .stderr(output);
        // The project's own metadata decides where its processes listen.
        for name in [
            "BIND",
            "UI_HOST",
            "UI_BIND",
            "UI_PORT",
            "UI_URL",
            "PUBLIC_ORIGIN",
        ] {
            command.env_remove(name);
        }
        DevProcess {
            child: command.spawn().expect("start distributed dev"),
            log,
        }
    }
}

/// How long a step that may compile the project gets.
#[cfg(unix)]
const COMPILES: Duration = Duration::from_secs(300);

#[cfg(unix)]
struct DevProcess {
    child: std::process::Child,
    log: PathBuf,
}

#[cfg(unix)]
impl DevProcess {
    fn log(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Polls until the session has printed `text`; fails once it has exited.
    #[track_caller]
    fn wait_for(&mut self, text: &str) {
        let started = std::time::Instant::now();
        while !self.log().contains(text) {
            let exited = self.child.try_wait().expect("inspect distributed dev");
            assert!(
                exited.is_none() && started.elapsed() < COMPILES,
                "distributed dev never printed `{text}` (exit: {exited:?}):\n{}",
                self.log()
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn signal(&self, signal: libc::c_int) {
        assert_eq!(
            unsafe { libc::kill(self.child.id() as libc::pid_t, signal) },
            0
        );
    }

    fn wait(&mut self) -> std::process::ExitStatus {
        let started = std::time::Instant::now();
        loop {
            if let Some(status) = self.child.try_wait().expect("inspect distributed dev") {
                return status;
            }
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "distributed dev did not exit:\n{}",
                self.log()
            );
            thread::sleep(Duration::from_millis(10));
        }
    }
}

#[cfg(unix)]
impl Drop for DevProcess {
    /// A failed test still stops its session: killing `distributed dev`
    /// outright would orphan what it started.
    fn drop(&mut self) {
        if self.child.try_wait().is_ok_and(|status| status.is_none()) {
            self.signal(libc::SIGTERM);
            let started = std::time::Instant::now();
            while self.child.try_wait().is_ok_and(|status| status.is_none())
                && started.elapsed() < Duration::from_secs(30)
            {
                thread::sleep(Duration::from_millis(10));
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A session with two processes that each started a descendant ignoring
/// SIGTERM, and a build executor (with its own descendant) in flight.
#[cfg(unix)]
fn signaled_dev_session_leaves_no_descendant(label: &str, signal: libc::c_int) {
    let project = Project::new(label);
    let root = &project.root;
    let mut dev = project.dev();
    dev.wait_for("lifecycle dev: ready");
    fs::write(root.join("slow-manifest"), "yes\n").unwrap();
    fs::write(root.join("service/src/name.txt"), "changed\n").unwrap();
    wait_until(COMPILES, || {
        !recorded_pids(&root.join("build-family.log")).is_empty()
    });
    let mut session = recorded_pids(&root.join("dev-family.log"));
    session.extend(recorded_pids(&root.join("build-family.log")));
    assert_eq!(
        session.len(),
        6,
        "two processes, one executor, a descendant each"
    );
    for pid in &session {
        assert_eq!(
            unsafe { libc::kill(*pid, 0) },
            0,
            "{pid} runs before the signal"
        );
    }

    dev.signal(signal);
    let status = dev.wait();
    assert!(status.success(), "{status}:\n{}", dev.log());
    assert!(
        dev.log().contains("lifecycle dev: stopped"),
        "{}",
        dev.log()
    );
    assert_no_survivors(&session);
}

#[cfg(unix)]
#[test]
#[ignore = "compiles the lifecycle test project; run in the integration job"]
fn dev_interrupted_leaves_no_descendant() {
    signaled_dev_session_leaves_no_descendant("dev-sigint", libc::SIGINT);
}

#[cfg(unix)]
#[test]
#[ignore = "compiles the lifecycle test project; run in the integration job"]
fn dev_terminated_leaves_no_descendant() {
    signaled_dev_session_leaves_no_descendant("dev-sigterm", libc::SIGTERM);
}

#[cfg(unix)]
#[test]
#[ignore = "compiles the lifecycle test project; run in the integration job"]
fn dev_hung_up_leaves_no_descendant() {
    signaled_dev_session_leaves_no_descendant("dev-sighup", libc::SIGHUP);
}

/// `distributed build` runs the manifest harness itself, before the lifecycle
/// graph. A lone SIGTERM stops that child and what it started.
#[cfg(unix)]
#[test]
#[ignore = "compiles the lifecycle test project; run in the integration job"]
fn build_terminated_leaves_no_descendant() {
    let project = Project::new("build-sigterm");
    let root = &project.root;
    fs::write(root.join("slow-manifest"), "yes\n").unwrap();
    let mut build = Command::new(env!("CARGO_BIN_EXE_distributed"))
        .arg("build")
        .arg(root)
        .env(
            "CARGO_TARGET_DIR",
            Path::new(env!("CARGO_TARGET_TMPDIR")).join("lifecycle-project"),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start distributed build");
    let started = std::time::Instant::now();
    while recorded_pids(&root.join("build-family.log")).is_empty() {
        let exited = build.try_wait().expect("inspect distributed build");
        if exited.is_some() || started.elapsed() > COMPILES {
            let _ = build.kill();
            panic!("the manifest harness never ran (exit: {exited:?})");
        }
        thread::sleep(Duration::from_millis(20));
    }
    let harness = recorded_pids(&root.join("build-family.log"));
    assert_eq!(harness.len(), 2, "the harness and its descendant");

    assert_eq!(
        unsafe { libc::kill(build.id() as libc::pid_t, libc::SIGTERM) },
        0
    );
    let status = build.wait().expect("wait for distributed build");
    assert!(!status.success(), "a stopped build fails: {status}");
    assert_no_survivors(&harness);
    assert!(!root.join(".distributed/lifecycle/active.json").exists());
}

#[cfg(unix)]
#[test]
fn dev_that_fails_leaves_no_descendant() {
    let mut fixture = Fixture::new("dev-failed");
    enable_dev(&mut fixture);
    let root = fixture.root.clone();
    let supervisor = DevSupervisor::start(&fixture);
    // Both processes are ready: a process that dies while its readiness is
    // awaited is a different failure.
    wait_until(Duration::from_secs(5), || {
        dev_state(&root)["members"]["ui"].is_object()
            && recorded_pids(&root.join("dev-family.log")).len() == 4
    });
    let session = recorded_pids(&root.join("dev-family.log"));

    // One process dies under the supervisor; the session ends with an error.
    assert_eq!(unsafe { libc::kill(session[0], libc::SIGKILL) }, 0);
    let error = supervisor
        .join_result()
        .expect_err("a dead process ends the session");
    assert!(error.message().contains("exited with"), "{error}");
    assert_no_survivors(&session);
}

#[cfg(unix)]
#[test]
#[ignore = "compiles the lifecycle test project; run in the integration job"]
fn dev_keeps_running_while_a_watched_input_is_missing_and_says_so_once() {
    let project = Project::new("dev-missing-input");
    let root = &project.root;
    let mut dev = project.dev();
    dev.wait_for("lifecycle dev: ready");

    // An editor or a checkout removes the file before writing its replacement.
    let manifest = root.join("service/Cargo.toml");
    let replacement = fs::read_to_string(&manifest).unwrap() + "\n# replaced\n";
    fs::remove_file(&manifest).unwrap();
    dev.wait_for("cannot read watched inputs");
    // Several polls (500ms each) see the same failure.
    thread::sleep(Duration::from_secs(2));
    fs::write(&manifest, replacement).unwrap();
    dev.wait_for("lifecycle dev: activated generation");

    dev.signal(libc::SIGINT);
    let status = dev.wait();
    let log = dev.log();
    assert!(status.success(), "{status}:\n{log}");
    assert_eq!(
        log.matches("cannot read watched inputs").count(),
        1,
        "{log}"
    );
    assert_eq!(
        log.matches("watched inputs are readable again").count(),
        1,
        "{log}"
    );
    assert!(log.contains("rebuilds=1"), "{log}");
}
