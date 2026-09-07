//! End-to-end compile tests for `distributed scaffold`: scaffold a project, point its
//! `distributed` dependency at this workspace, and `cargo check` the output.
//!
//! The fast generation tests only assert rendered text, so template drift
//! against the current `distributed` API is invisible to them — only compiling
//! the output catches it. Like the manifest-harness tests these nest a `cargo`
//! build, so they are `#[ignore]`d by default and run in the integration CI job
//! (`cargo test -p distributed_cli -- --include-ignored`).

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Repo root (the `distributed` crate) — `distributed_cli`'s parent.
fn distributed_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("distributed_cli has a parent directory")
        .to_path_buf()
}

/// Scaffold `orders` into a fresh `CARGO_TARGET_TMPDIR/<dir_name>`.
fn scaffold(dir_name: &str, extra_args: &[&str]) -> PathBuf {
    let out_dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(dir_name);
    let _ = fs::remove_dir_all(&out_dir);
    let output = Command::new(env!("CARGO_BIN_EXE_distributed"))
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
        .expect("distributed should run");
    assert!(
        output.status.success(),
        "distributed scaffold {extra_args:?} failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    out_dir
}

/// `cargo check` a scaffolded project. All compile tests share one target dir
/// so the `distributed` dependency graph builds once (cargo file-locks it, so
/// parallel tests serialize instead of clobbering each other).
fn cargo_check(project_dir: &Path) {
    let target_dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("scaffold-compile-target");
    let output = Command::new("cargo")
        .args(["check", "--quiet", "--manifest-path"])
        .arg(project_dir.join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", &target_dir)
        .output()
        .expect("cargo should run");
    assert!(
        output.status.success(),
        "cargo check failed for {}:\n{}",
        project_dir.display(),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
#[ignore = "compiles the scaffolded project via a nested cargo build; run in the integration job"]
fn scaffolded_http_service_compiles() {
    // The richest template surface: an aggregate model, read models, a
    // model-backed command handler, and an event handler, on the default
    // postgres store.
    let out_dir = scaffold(
        "compile-http-postgres",
        &[
            "--store",
            "postgres",
            "--model",
            "order",
            "--read-models",
            "--command",
            "orders.place",
            "--event",
            "orders.shipped",
        ],
    );
    cargo_check(&out_dir);
}

#[test]
#[ignore = "compiles the scaffolded project via a nested cargo build; run in the integration job"]
fn scaffolded_http_tracing_service_compiles() {
    let out_dir = scaffold(
        "compile-http-tracing",
        &["--transport", "http", "--store", "in-memory", "--tracing"],
    );
    cargo_check(&out_dir);
}

#[test]
#[ignore = "compiles the scaffolded project via a nested cargo build; run in the integration job"]
fn scaffolded_query_api_service_compiles() {
    let out_dir = scaffold(
        "compile-query-api-sqlite",
        &[
            "--query-api",
            "--store",
            "sqlite",
            "--model",
            "order",
            "--command",
            "orders.place",
        ],
    );
    cargo_check(&out_dir);
}

#[test]
#[ignore = "compiles the scaffolded project via a nested cargo build; run in the integration job"]
fn scaffolded_knative_service_compiles() {
    // The other main.rs branch: CloudEvents ingress served through the
    // scaffold's own axum dependency, with the in-memory store and the
    // derived default command handler.
    let out_dir = scaffold(
        "compile-knative-in-memory",
        &["--transport", "knative", "--store", "in-memory"],
    );
    cargo_check(&out_dir);
}

#[test]
#[ignore = "compiles the scaffolded project via a nested cargo build; run in the integration job"]
fn scaffolded_knative_tracing_service_compiles() {
    let out_dir = scaffold(
        "compile-knative-tracing",
        &[
            "--transport",
            "knative",
            "--store",
            "in-memory",
            "--tracing",
        ],
    );
    cargo_check(&out_dir);
}
