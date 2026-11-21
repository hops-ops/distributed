//! Integration tests for `distributed scaffold`: drive the real binary and assert the
//! generated project tree. Pure generation + filesystem, so these are fast and
//! need no nested compilation — `cli_scaffold_compile.rs` owns the (ignored)
//! end-to-end compile checks.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// Repo root (the `distributed` crate) — `distributed_cli`'s parent.
fn distributed_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("distributed_cli has a parent directory")
        .to_path_buf()
}

/// Run `distributed scaffold orders --path <tmp>/<dir_name> <extra_args...>` against a
/// fresh directory, returning the raw process output and the output directory.
fn run_scaffold(dir_name: &str, extra_args: &[&str]) -> (Output, PathBuf) {
    let out_dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(dir_name);
    let _ = fs::remove_dir_all(&out_dir);
    let output = scaffold_into(&out_dir, extra_args);
    (output, out_dir)
}

/// Run `distributed scaffold orders --path <out_dir> <extra_args...>` without touching
/// the directory first (error-path tests pre-populate it).
fn scaffold_into(out_dir: &Path, extra_args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_distributed"))
        .args([
            "scaffold",
            "orders",
            "--path",
            out_dir.to_str().unwrap(),
            "--distributed-path",
            distributed_root().to_str().unwrap(),
        ])
        .args(extra_args)
        .output()
        .expect("distributed should run")
}

/// Scaffold and assert success, returning the output directory.
fn scaffold(dir_name: &str, extra_args: &[&str]) -> PathBuf {
    let (output, out_dir) = run_scaffold(dir_name, extra_args);
    assert!(
        output.status.success(),
        "distributed scaffold {extra_args:?} failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    out_dir
}

fn read(dir: &Path, rel: &str) -> String {
    fs::read_to_string(dir.join(rel))
        .unwrap_or_else(|err| panic!("read {rel} in {}: {err}", dir.display()))
}

#[test]
fn scaffold_generates_a_service_tree() {
    let out_dir = scaffold("scaffold-orders", &["--store", "postgres", "--gitops"]);

    for expected in [
        "Cargo.toml",
        "src/lib.rs",
        "src/main.rs",
        "src/service.rs",
        ".gitops/local/Chart.yaml",
        ".gitops/local/values.yaml",
        ".gitops/deploy/Chart.yaml",
        ".gitops/deploy/values.yaml",
    ] {
        assert!(
            out_dir.join(expected).exists(),
            "missing generated file: {expected}"
        );
    }

    let local_values = read(&out_dir, ".gitops/local/values.yaml");
    assert!(local_values.starts_with("local: true\npreview: false\n"));
    assert!(local_values.contains("tag: \"latest\""));
    let deploy_values = read(&out_dir, ".gitops/deploy/values.yaml");
    assert!(deploy_values.starts_with("local: false\npreview: false\n"));
    assert!(deploy_values.contains("tag: \"\""));
    assert!(!read(&out_dir, ".gitops/local/templates/deployment.yaml").contains(".Values.local"));
    assert!(read(&out_dir, ".gitops/deploy/templates/deployment.yaml")
        .contains("image.tag must be an immutable build tag"));

    let cargo = read(&out_dir, "Cargo.toml");
    assert!(cargo.contains("\"postgres\""), "Cargo.toml: {cargo}");
    assert!(
        !out_dir
            .join(".gitops/deploy/templates/servicemonitor.yaml")
            .exists(),
        "plain --gitops should not emit Prometheus Operator CRDs"
    );
}

#[test]
fn scaffold_metrics_prometheus_emits_operator_resources() {
    let out_dir = scaffold(
        "scaffold-orders-metrics",
        &["--store", "postgres", "--gitops", "--metrics", "prometheus"],
    );

    for expected in [
        ".gitops/deploy/templates/servicemonitor.yaml",
        ".gitops/deploy/templates/prometheusrule.yaml",
    ] {
        assert!(
            out_dir.join(expected).exists(),
            "missing generated file: {expected}"
        );
    }

    let cargo = read(&out_dir, "Cargo.toml");
    assert!(cargo.contains("\"metrics\""), "Cargo.toml: {cargo}");
}

#[test]
fn store_variants_toggle_persistence_features() {
    // (flag value, distributed feature it must add — None for in-memory)
    for (store, feature) in [
        ("postgres", Some("\"postgres\"")),
        ("sqlite", Some("\"sqlite\"")),
        ("in-memory", None),
    ] {
        let out_dir = scaffold(&format!("scaffold-store-{store}"), &["--store", store]);
        let cargo = read(&out_dir, "Cargo.toml");
        assert!(cargo.contains("\"http\""), "--store {store}: {cargo}");
        match feature {
            Some(feature) => {
                assert!(cargo.contains(feature), "--store {store}: {cargo}");
            }
            None => {
                assert!(
                    !cargo.contains("\"postgres\"") && !cargo.contains("\"sqlite\""),
                    "--store {store} should add no persistence feature: {cargo}"
                );
            }
        }
    }
}

#[test]
fn http_transport_serves_the_command_router() {
    let out_dir = scaffold("scaffold-transport-http", &["--transport", "http"]);
    let main = read(&out_dir, "src/main.rs");
    assert!(main.contains("distributed::microsvc::serve"), "{main}");
    assert!(!main.contains("cloud_events_router"), "{main}");
    let cargo = read(&out_dir, "Cargo.toml");
    assert!(
        !cargo.contains("axum"),
        "http scaffold needs no direct axum dep: {cargo}"
    );
    let service = read(&out_dir, "src/service.rs");
    assert!(service.contains(".transport(\"http\")"), "{service}");
}

#[test]
fn knative_transport_serves_the_cloudevents_router() {
    let out_dir = scaffold("scaffold-transport-knative", &["--transport", "knative"]);
    let main = read(&out_dir, "src/main.rs");
    assert!(main.contains("cloud_events_router"), "{main}");
    assert!(main.contains("axum::serve"), "{main}");
    let cargo = read(&out_dir, "Cargo.toml");
    // The scaffolded axum must match the major the `distributed` crate exposes
    // its `Router` from, or `axum::serve(listener, app)` cannot type-check.
    assert!(cargo.contains("axum = \"0.8\""), "{cargo}");
    let service = read(&out_dir, "src/service.rs");
    assert!(service.contains(".transport(\"knative\")"), "{service}");
}

#[test]
fn bus_variants_render_gitops_bus_config() {
    for bus in ["rabbitmq", "kafka", "psql", "nats"] {
        let out_dir = scaffold(&format!("scaffold-bus-{bus}"), &["--bus", bus, "--gitops"]);
        let values = read(&out_dir, ".gitops/deploy/values.yaml");
        assert!(
            values.contains(&format!("kind: {bus}")),
            "--bus {bus}: {values}"
        );
        let deployment = read(&out_dir, ".gitops/deploy/templates/deployment.yaml");
        assert!(deployment.contains("HOPS_BUS"), "--bus {bus}: {deployment}");
        assert!(
            deployment.contains(&format!("value: {bus}")),
            "--bus {bus}: {deployment}"
        );
    }

    let out_dir = scaffold("scaffold-bus-none", &["--gitops"]);
    let values = read(&out_dir, ".gitops/deploy/values.yaml");
    assert!(!values.contains("bus:"), "no --bus: {values}");
    let deployment = read(&out_dir, ".gitops/deploy/templates/deployment.yaml");
    assert!(!deployment.contains("HOPS_BUS"), "no --bus: {deployment}");
}

#[test]
fn gitops_promote_variants_render_their_resource() {
    for (mode, path, kind) in [
        (
            "argo",
            ".gitops/promote/templates/application.yaml",
            "kind: Application",
        ),
        (
            "flux",
            ".gitops/promote/templates/helmrelease.yaml",
            "kind: HelmRelease",
        ),
    ] {
        let out_dir = scaffold(
            &format!("scaffold-promote-{mode}"),
            &["--gitops-promote", mode],
        );
        let resource = read(&out_dir, path);
        assert!(
            resource.contains(kind),
            "--gitops-promote {mode}: {resource}"
        );
        assert!(resource.contains(".gitops/deploy"));
        assert!(resource.contains(".Values.deploy.values"));
        assert!(!resource.contains(".Values.source.repoURL | toYaml"));
    }
}

#[test]
fn scaffold_refuses_a_non_empty_directory() {
    let out_dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("scaffold-non-empty");
    let _ = fs::remove_dir_all(&out_dir);
    fs::create_dir_all(&out_dir).unwrap();
    fs::write(out_dir.join("keep.txt"), "precious").unwrap();

    let output = scaffold_into(&out_dir, &[]);
    assert!(
        !output.status.success(),
        "scaffold should refuse to overwrite"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("not empty"), "stderr: {stderr}");
    assert!(stderr.contains("--force"), "stderr: {stderr}");
    // Nothing was written and the existing file is intact.
    assert_eq!(read(&out_dir, "keep.txt"), "precious");
    assert!(!out_dir.join("Cargo.toml").exists());

    // --force overwrites in place.
    let output = scaffold_into(&out_dir, &["--force"]);
    assert!(
        output.status.success(),
        "--force should scaffold into a non-empty dir:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(out_dir.join("Cargo.toml").exists());
    assert_eq!(read(&out_dir, "keep.txt"), "precious");
}

#[test]
fn describe_reports_a_missing_manifest() {
    let missing = Path::new(env!("CARGO_TARGET_TMPDIR")).join("no-such-dir/Cargo.toml");
    let output = Command::new(env!("CARGO_BIN_EXE_distributed"))
        .args(["describe", "--manifest-path", missing.to_str().unwrap()])
        .output()
        .expect("distributed should run");
    assert!(!output.status.success(), "describe should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("target manifest not found"),
        "stderr: {stderr}"
    );
}
