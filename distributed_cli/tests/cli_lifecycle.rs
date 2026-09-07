//! Process-level coherent lifecycle build checks.

use distributed_cli::{
    run_lifecycle_build, run_lifecycle_dev, LifecycleBuildOptions, LifecycleDevOptions,
    LifecycleDevReport, LifecycleError,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use tempfile::TempDir;

fn distributed(root: &Path, extra: &[&str]) -> Output {
    let mut args = vec![
        "build",
        "--root",
        root.to_str().expect("UTF-8 fixture root"),
        "--output",
        "json",
    ];
    args.extend_from_slice(extra);
    Command::new(env!("CARGO_BIN_EXE_distributed"))
        .args(args)
        .output()
        .expect("distributed build should run")
}

fn temporary_root(label: &str) -> TempDir {
    let root = tempfile::Builder::new()
        .prefix(&format!("distributed-lifecycle-{label}-"))
        .tempdir()
        .expect("create exclusive lifecycle fixture root");
    fs::create_dir_all(root.path().join("src")).expect("create lifecycle fixture source");
    root
}

fn write_fixture(root: &Path) {
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
    fs::write(
        root.join("distributed.contracts.json"),
        serde_json::to_vec_pretty(&catalog).expect("serialize lifecycle catalog"),
    )
    .expect("write lifecycle catalog");
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
    fs::write(
        root.join("distributed.lifecycle.json"),
        serde_json::to_vec_pretty(&config).expect("serialize lifecycle config"),
    )
    .expect("write lifecycle config");
}

fn report(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "parse lifecycle report: {error}; stdout={}; stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

fn enable_dev(root: &Path) {
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
printf '%s\n' "$descendant" >> "$root/dev-descendants.log"
wait "$descendant"
"#,
    )
    .expect("write dev child fixture");
    let path = root.join("distributed.lifecycle.json");
    let mut config: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    config["dev"] = serde_json::json!({
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
    fs::write(path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
}

fn configure_blocking_readiness_probe(root: &Path, timeout_ms: u64) {
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
    let path = root.join("distributed.lifecycle.json");
    let mut config: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    config["dev"]["processes"]["api"]["ready"] = serde_json::json!({
        "program": "/bin/sh",
        "args": ["{root}/readiness-probe.sh", "{root}"],
        "interval_ms": 10,
        "timeout_ms": timeout_ms
    });
    fs::write(path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
}

fn configure_generation_rejecting_probe(root: &Path) {
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
    let path = root.join("distributed.lifecycle.json");
    let mut config: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    config["dev"]["prepare_ms"] = serde_json::json!(100);
    config["dev"]["processes"]["api"]["ready"] = serde_json::json!({
        "program": "/bin/sh",
        "args": ["{root}/generation-probe.sh", "{root}"],
        "interval_ms": 10,
        "timeout_ms": 100
    });
    fs::write(path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
}

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
    fn start(root: PathBuf) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let supervisor_stop = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            run_lifecycle_dev(&LifecycleDevOptions {
                build: LifecycleBuildOptions {
                    root,
                    catalog: "distributed.contracts.json".into(),
                    config: "distributed.lifecycle.json".into(),
                    out: "dist/distributed".into(),
                    check: false,
                    lock_timeout: Duration::from_secs(1),
                    nodes: None,
                    activation_inputs: None,
                    cancel: None,
                },
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
    let fixture = temporary_root("deterministic");
    let root = fixture.path().to_path_buf();
    write_fixture(&root);

    let first = distributed(&root, &[]);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first = report(&first);
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
    let second = distributed(&root, &[]);
    assert!(second.status.success());
    let second = report(&second);
    assert_eq!(first["generation_id"], second["generation_id"]);
    assert_eq!(first["release_id"], second["release_id"]);

    let snapshot_before_check = file_snapshot(&root);
    let check = distributed(&root, &["--check"]);
    assert!(
        check.status.success(),
        "{}",
        String::from_utf8_lossy(&check.stderr)
    );
    assert!(report(&check)["drift"].as_array().unwrap().is_empty());
    assert_eq!(
        active_before,
        fs::read(root.join("dist/distributed/active.json")).unwrap()
    );
    assert_eq!(snapshot_before_check, file_snapshot(&root));
}

#[test]
fn partial_build_reuses_verified_upstream_receipts() {
    let fixture = temporary_root("partial");
    let root = fixture.path().to_path_buf();
    write_fixture(&root);
    let options = LifecycleBuildOptions {
        root: root.clone(),
        catalog: "distributed.contracts.json".into(),
        config: "distributed.lifecycle.json".into(),
        out: "dist/distributed".into(),
        check: false,
        lock_timeout: Duration::from_secs(1),
        nodes: None,
        activation_inputs: None,
        cancel: None,
    };
    let first = run_lifecycle_build(&options).unwrap();
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
    let mut partial = options.clone();
    partial.nodes = Some(["plan".to_string()].into_iter().collect());
    partial.activation_inputs = Some(old_inputs);
    let superseded = run_lifecycle_build(&partial).unwrap_err();
    assert!(superseded.message().contains("superseded"));
    assert!(
        fs::read_to_string(root.join("dist/distributed/active.json"))
            .unwrap()
            .contains(&first.generation_id)
    );
    partial.activation_inputs = None;
    let second = run_lifecycle_build(&partial).unwrap();
    assert_eq!(second.executed, ["plan"]);
    assert_ne!(first.generation_id, second.generation_id);

    let manifest = |generation: &str| -> Value {
        serde_json::from_slice(
            &fs::read(
                root.join("dist/distributed/generations")
                    .join(generation)
                    .join("generation.json"),
            )
            .unwrap(),
        )
        .unwrap()
    };
    assert_eq!(
        manifest(&first.generation_id)["receipts"]["application"],
        manifest(&second.generation_id)["receipts"]["application"]
    );
}

#[test]
fn mixed_source_and_unowned_outputs_fail_before_activation() {
    let mixed_fixture = temporary_root("mixed-source");
    let mixed_root = mixed_fixture.path().to_path_buf();
    write_fixture(&mixed_root);
    let config_path = mixed_root.join("distributed.lifecycle.json");
    let mut config: Value = serde_json::from_slice(&fs::read(&config_path).unwrap()).unwrap();
    config["source"]["javascript"] = Value::String(format!("sha256:{}", "b".repeat(64)));
    fs::write(&config_path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
    let mixed = distributed(&mixed_root, &[]);
    assert!(!mixed.status.success());
    let diagnostic = String::from_utf8_lossy(&mixed.stderr);
    assert!(diagnostic.contains(&format!("sha256:{}", "a".repeat(64))));
    assert!(diagnostic.contains(&format!("sha256:{}", "b".repeat(64))));
    assert!(!mixed_root.join("dist/distributed/active.json").exists());

    let unowned_fixture = temporary_root("unowned-output");
    let unowned_root = unowned_fixture.path().to_path_buf();
    write_fixture(&unowned_root);
    fs::write(unowned_root.join("produce-unowned"), "yes\n").unwrap();
    let unowned = distributed(&unowned_root, &[]);
    assert!(!unowned.status.success());
    assert!(
        String::from_utf8_lossy(&unowned.stderr).contains("unowned staged output `unowned.txt`")
    );
    assert!(!unowned_root.join("dist/distributed/active.json").exists());
}

#[test]
fn dev_waits_for_initial_generation_and_restarts_only_invalidated_processes() {
    let fixture = temporary_root("dev-supervisor");
    let root = fixture.path().to_path_buf();
    write_fixture(&root);
    enable_dev(&root);
    let supervisor = DevSupervisor::start(root.clone());

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
    let fixture = temporary_root("dev-rollback");
    let root = fixture.path().to_path_buf();
    write_fixture(&root);
    enable_dev(&root);
    configure_generation_rejecting_probe(&root);
    let supervisor = DevSupervisor::start(root.clone());
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
    let fixture = temporary_root("dev-prepare-timeout");
    let root = fixture.path().to_path_buf();
    write_fixture(&root);
    enable_dev(&root);
    let config_path = root.join("distributed.lifecycle.json");
    let mut config: Value = serde_json::from_slice(&fs::read(&config_path).unwrap()).unwrap();
    config["dev"]["prepare_ms"] = serde_json::json!(100);
    fs::write(&config_path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
    let supervisor = DevSupervisor::start(root.clone());
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
fn dev_cancels_a_superseded_executor_before_activation() {
    let fixture = temporary_root("dev-cancel");
    let root = fixture.path().to_path_buf();
    write_fixture(&root);
    enable_dev(&root);
    let supervisor = DevSupervisor::start(root.clone());
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
    fs::write(root.join("plan/input.txt"), "latest\n").unwrap();
    wait_until(Duration::from_secs(5), || {
        fs::read_to_string(root.join("build-plan-starts.log"))
            .is_ok_and(|log| log.lines().count() >= 2)
    });
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
    let fixture = temporary_root("dev-initial-cancel");
    let root = fixture.path().to_path_buf();
    write_fixture(&root);
    enable_dev(&root);
    fs::write(root.join("slow-plan"), "yes\n").unwrap();
    let supervisor = DevSupervisor::start(root.clone());
    wait_until(Duration::from_secs(5), || {
        root.join("build-plan-starts.log").exists()
    });

    let started = std::time::Instant::now();
    let error = supervisor
        .stop_and_join_result()
        .expect_err("initial build should report cancellation");
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(error.message().contains("was canceled"));
    assert!(!root.join("dev-process.log").exists());
}

#[test]
fn dev_enforces_timeout_while_readiness_probe_is_running() {
    let fixture = temporary_root("dev-readiness-timeout");
    let root = fixture.path().to_path_buf();
    write_fixture(&root);
    enable_dev(&root);
    configure_blocking_readiness_probe(&root, 100);

    let started = std::time::Instant::now();
    let error = DevSupervisor::start(root.clone())
        .join_result()
        .expect_err("blocked readiness probe should time out");
    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(error.message().contains("timed out after 100ms"));
    assert_fixture_processes_gone(&root, "readiness-probe");
}

#[test]
fn dev_stop_cancels_a_running_readiness_probe() {
    let fixture = temporary_root("dev-readiness-cancel");
    let root = fixture.path().to_path_buf();
    write_fixture(&root);
    enable_dev(&root);
    configure_blocking_readiness_probe(&root, 2_000);
    let supervisor = DevSupervisor::start(root.clone());
    wait_until(Duration::from_secs(1), || {
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
    let fixture = temporary_root("failure");
    let root = fixture.path().to_path_buf();
    write_fixture(&root);
    let baseline = distributed(&root, &[]);
    assert!(baseline.status.success());
    let active_before = fs::read(root.join("dist/distributed/active.json")).unwrap();

    fs::write(root.join("generated/plan.json"), "stale\n").expect("make tracked output stale");
    let check = distributed(&root, &["--check"]);
    assert_eq!(check.status.code(), Some(1));
    let check = report(&check);
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
    let failed = distributed(&root, &[]);
    assert!(!failed.status.success());
    let failed_diagnostic = String::from_utf8_lossy(&failed.stderr);
    assert!(failed_diagnostic.contains("lifecycle node `plan`"));
    assert!(failed_diagnostic.contains("injected downstream failure"));
    assert_eq!(
        active_before,
        fs::read(root.join("dist/distributed/active.json")).unwrap()
    );
}
