//! End-to-end tests for the manifest-harness commands (`describe` / `schema`).
//! These compile the `orders-service` fixture via a nested `cargo` build, so they
//! are `#[ignore]`d by default and run explicitly in the integration CI job
//! (`cargo test -p distributed_cli --include-ignored`).

use std::path::{Path, PathBuf};
use std::process::Command;

fn distributed_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("distributed_cli has a parent directory")
        .to_path_buf()
}

fn fixture_manifest() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/orders-service/Cargo.toml")
}

/// Run `distributed <args...>` against the fixture, returning stdout. Always passes
/// `--manifest-path` and `--distributed-path` so resolution is deterministic.
fn distributed(args: &[&str]) -> String {
    let root = distributed_root();
    let manifest = fixture_manifest();
    let output = Command::new(env!("CARGO_BIN_EXE_distributed"))
        .args(args)
        .args(["--manifest-path", manifest.to_str().unwrap()])
        .args(["--distributed-path", root.to_str().unwrap()])
        .output()
        .expect("distributed should run");
    assert!(
        output.status.success(),
        "distributed {args:?} failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

#[test]
#[ignore = "compiles the fixture via the manifest harness; run in the integration job"]
fn describe_emits_manifest_json() {
    let json = distributed(&["describe"]);
    let manifest: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(manifest["schema_version"], 2);
    for inventory in ["commands", "events", "models", "projections"] {
        assert!(manifest.get(inventory).is_none(), "redundant {inventory}");
    }
    // This read-model-only fixture owns models through its Surface, not modules.
    assert!(!manifest["surfaces"][0]["models"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(json.contains("\"schema_version\""), "json: {json}");
    assert!(json.contains("\"orders\""), "json: {json}");
    let framework = manifest["extensions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|extension| extension["id"] == "distributed.framework")
        .unwrap();
    assert_eq!(framework["version"], 1);
    assert_eq!(framework["value"]["release"], env!("CARGO_PKG_VERSION"));
}

#[test]
#[ignore = "compiles the fixture via the manifest harness; run in the integration job"]
fn client_manifest_uses_service_surface_export() {
    let json = distributed(&["client-manifest"]);
    let manifest: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(manifest["manifest_version"], 2);
    assert_eq!(manifest["protocol_version"], 1);
    assert_eq!(manifest["service_id"], "orders");
    assert_eq!(manifest["surface"]["kind"], "role");
    assert_eq!(manifest["surface"]["name"], "user");
    assert_eq!(
        manifest["schema_fingerprint"],
        "sha256:64007d9696cdd73fdbd3b12133155c53b709970ef2411631d9f66189c3c8fc90"
    );
    assert_eq!(
        manifest["protocol_fingerprint"],
        "sha256:00fb342f3acb4dc1c1716a43cc3001c748d5f6c500ff831690d820e9e43e2782"
    );
    assert_eq!(manifest["models"][0]["id"], "OrderView");
    assert_eq!(manifest["models"][0]["record_revisions"], true);
    assert_eq!(manifest["models"][0]["tombstones"], true);
    assert_eq!(manifest["capabilities"]["record_revisions"], true);
    assert_eq!(manifest["capabilities"]["tombstones"], true);
    assert_eq!(manifest["capabilities"]["live_resume"], true);
    assert_eq!(manifest["capabilities"]["query_fallback"], "revalidate");
    assert_eq!(
        manifest["commands"][0]["extensions"]["consistency"]["kind"],
        "atomic"
    );
    assert_eq!(
        manifest["commands"][0]["extensions"]["direct_projection"]["topology"]["version"],
        1
    );
    assert_eq!(
        manifest["commands"][0]["extensions"]["direct_projection"]["topology"]["name"],
        "project_orders"
    );
    assert_eq!(
        manifest["commands"][0]["extensions"]["direct_projection"]["topology"]["digest"],
        "sha256:32e51a5f5c3b7a83d27366f8dc889b87045e65f027f7b598421a6db765efe8a4"
    );
    assert_eq!(
        manifest["commands"][0]["extensions"]["direct_projection"]["model"],
        "OrderView"
    );
    assert_eq!(
        manifest["commands"][0]["extensions"]["direct_projection"]["change_epoch"],
        "orders-v1"
    );
    assert!(manifest["commands"][0]["extensions"]["direct_projection"]
        .get("partition")
        .is_none());
    assert!(manifest["protocol_operations"]["command_status"].is_object());
    assert!(manifest["projection_programs"].is_array());
    assert!(manifest["projection_bindings"].is_array());
}

#[test]
#[ignore = "compiles the fixture via the manifest harness; run in the integration job"]
fn emitted_client_manifest_is_accepted_by_client_compiler() {
    let json = distributed(&["client-manifest"]);
    let manifest: serde_json::Value = serde_json::from_str(&json).unwrap();
    let project = distributed_cli::compile_client(distributed_cli::ClientCompileInput::new(
        manifest,
        distributed_cli::ClientSurfaceSelector::role("user"),
        vec![distributed_cli::ClientDocument::new(
            "src/routes/orders/+page.graphql",
            "query Orders { orders { order_id status } }",
        )],
    ))
    .expect("the compiler must consume the exact manifest emitted by the server crate");

    assert_eq!(project.operations.len(), 1);
    assert_eq!(project.operations[0].name, "Orders");
    assert_eq!(project.schema_fingerprint.len(), 71);
}

#[test]
#[ignore = "compiles the fixture via the manifest harness; run in the integration job"]
fn schema_renders_postgres_sql() {
    let sql = distributed(&["schema", "--dialect", "postgres"]);
    assert!(sql.contains("CREATE TABLE"), "sql: {sql}");
    assert!(sql.contains("orders"), "sql: {sql}");
}

#[test]
#[ignore = "compiles the fixture via the manifest harness; run in the integration job"]
fn schema_renders_sqlite_sql() {
    let sql = distributed(&["schema", "--dialect", "sqlite"]);
    assert!(sql.contains("CREATE TABLE"), "sql: {sql}");
    assert!(sql.contains("orders"), "sql: {sql}");
    // SQLite renders upper-case storage classes; postgres uses lower-case
    // `text`, so this distinguishes the dialects.
    assert!(sql.contains("TEXT"), "sql: {sql}");
}

#[test]
#[ignore = "compiles the fixture via the manifest harness; run in the integration job"]
fn schema_renders_atlas_resource() {
    let yaml = distributed(&[
        "schema",
        "--format",
        "atlas",
        "--name",
        "orders",
        "--db-secret",
        "orders-db",
    ]);
    assert!(yaml.contains("kind: AtlasSchema"), "yaml: {yaml}");
    assert!(yaml.contains("secretKeyRef"), "yaml: {yaml}");
    assert!(yaml.contains("CREATE TABLE"), "yaml: {yaml}");
}
