use super::*;
use serde_json::{json, Value};
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

#[cfg(unix)]
use std::os::unix::fs::symlink;
#[cfg(unix)]
use std::os::unix::net::UnixListener;

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("distributed_cli has a repository parent")
        .to_path_buf()
}

fn fixture(name: &str) -> &'static str {
    match name {
        "valid" => include_str!("../../tests/fixtures/contracts/catalog/catalog-valid.json"),
        "duplicate" => {
            include_str!("../../tests/fixtures/contracts/catalog/catalog-duplicate-scope.json")
        }
        "escaping" => {
            include_str!("../../tests/fixtures/contracts/catalog/catalog-escaping-path.json")
        }
        "cycle" => include_str!("../../tests/fixtures/contracts/catalog/catalog-chain-cycle.json"),
        "environment" => {
            include_str!("../../tests/fixtures/contracts/catalog/catalog-environment-value.json")
        }
        _ => panic!("unknown catalog fixture {name}"),
    }
}

fn path_catalog(source: &str) -> ContractCatalog {
    let input = serde_json::json!({
        "schema_version": 1,
        "entries": {
            "path-test": {
                "id": "path-test",
                "kind": "migration_inventory",
                "scope": { "id": "path/test" },
                "owner": "test/path",
                "identity": {
                    "kind": "migration_inventory",
                    "value": "sha256:path-test"
                },
                "provenance": {
                    "sources": [source],
                    "generator": "test.path"
                },
                "outputs": { "output": "inside.txt" }
            }
        }
    });
    ContractCatalog::from_json_str(&input.to_string()).expect("path catalog JSON")
}

fn migration_checksum(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn write_repository_file(root: &Path, relative: &str, bytes: &[u8]) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().expect("migration file parent"))
        .expect("migration file parent directory");
    fs::write(path, bytes).expect("migration fixture file");
}

fn migration_entry(
    version: u64,
    sqlite_path: &str,
    sqlite_sql: &[u8],
    postgres_path: &str,
    postgres_sql: &[u8],
) -> Value {
    json!({
        "version": version,
        "description": if version == 1 { "initial" } else { "next" },
        "sqlite": {
            "path": sqlite_path,
            "sha256": migration_checksum(sqlite_sql)
        },
        "postgres": {
            "path": postgres_path,
            "sha256": migration_checksum(postgres_sql)
        }
    })
}

fn write_migration_inventory(root: &Path, entries: &[Value]) {
    let path = root.join(MIGRATION_INVENTORY_PATH);
    fs::create_dir_all(path.parent().expect("inventory parent directory"))
        .expect("inventory parent directory");
    fs::write(
        path,
        serde_json::to_vec_pretty(&json!({
            "schema_version": MIGRATION_INVENTORY_SCHEMA_VERSION,
            "migrations": entries
        }))
        .expect("migration inventory JSON"),
    )
    .expect("migration inventory file");
}

fn create_migration_fixture(label: &str) -> (TemporaryDirectory, Vec<u8>, Vec<u8>) {
    let root = TemporaryDirectory::new_short(label);
    let sqlite_sql = b"CREATE TABLE one (id INTEGER PRIMARY KEY);\n".to_vec();
    let postgres_sql = b"CREATE TABLE one (id BIGINT PRIMARY KEY);\n".to_vec();
    let sqlite_path = "migrations/sqlite/0001_initial.sql";
    let postgres_path = "migrations/postgres/0001_initial.sql";
    write_repository_file(root.path(), sqlite_path, &sqlite_sql);
    write_repository_file(root.path(), postgres_path, &postgres_sql);
    write_migration_inventory(
        root.path(),
        &[migration_entry(
            1,
            sqlite_path,
            &sqlite_sql,
            postgres_path,
            &postgres_sql,
        )],
    );
    (root, sqlite_sql, postgres_sql)
}

fn git_fixture_command(root: &Path, args: &[&str]) -> std::process::Output {
    Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .expect("invoke git fixture command")
}

fn commit_migration_fixture(root: &Path) -> String {
    for args in [
        ["init", "-q"].as_slice(),
        ["config", "user.email", "migration-tests@example.invalid"].as_slice(),
        ["config", "user.name", "Migration Tests"].as_slice(),
        ["add", "--all"].as_slice(),
        ["commit", "-qm", "baseline"].as_slice(),
    ] {
        let output = git_fixture_command(root, args);
        assert!(
            output.status.success(),
            "git fixture command {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let output = git_fixture_command(root, &["rev-parse", "HEAD"]);
    assert!(output.status.success(), "read fixture revision");
    String::from_utf8(output.stdout)
        .expect("fixture revision UTF-8")
        .trim()
        .to_string()
}

fn glob_catalog(source: &str, glob_limit: usize) -> ContractCatalog {
    let input = serde_json::json!({
        "schema_version": 1,
        "entries": {
            "glob-test": {
                "id": "glob-test",
                "kind": "migration_inventory",
                "scope": { "id": "path/glob" },
                "owner": "test/glob",
                "identity": {
                    "kind": "migration_inventory",
                    "value": "sha256:glob-test"
                },
                "provenance": {
                    "sources": [source],
                    "generator": "test.glob",
                    "glob_limit": glob_limit
                },
                "outputs": { "output": "inside.txt" }
            }
        }
    });
    ContractCatalog::from_json_str(&input.to_string()).expect("glob catalog JSON")
}

fn create_sparse_file(path: &Path, length: usize) {
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .expect("sparse file");
    file.set_len(length as u64).expect("sparse file length");
}

struct TemporaryDirectory(PathBuf);

impl TemporaryDirectory {
    fn new(label: &str) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after Unix epoch")
            .as_nanos();
        let base = std::env::temp_dir();
        for attempt in 0..100 {
            let path = base.join(format!(
                "distributed-contracts-{label}-{}-{timestamp}-{attempt}",
                std::process::id()
            ));
            if fs::create_dir(&path).is_ok() {
                return Self(path);
            }
        }
        panic!("could not create temporary directory for {label}");
    }

    fn path(&self) -> &Path {
        &self.0
    }

    #[cfg(unix)]
    fn new_short(label: &str) -> Self {
        let base = std::env::temp_dir();
        for attempt in 0..100 {
            let path = base.join(format!("dct-{label}-{}-{attempt}", std::process::id()));
            if fs::create_dir(&path).is_ok() {
                return Self(path);
            }
        }
        panic!("could not create short temporary directory for {label}");
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn catalog_and_declarative_client_inventory_validate_independently() {
    let root = repository_root();
    let catalog = ContractCatalog::from_path(root.join("distributed.contracts.json"))
        .expect("repository catalog should resolve without writing");
    let inventory =
        ClientInventory::from_path(root.join("tests/e2e-ui/ui/distributed.clients.json"))
            .expect("client inventory should use the shared schema");

    assert_eq!(inventory.clients.len(), 3);
    assert!(inventory
        .clients
        .iter()
        .any(|client| client.surface == "e2e-ui-admin"));
    assert_eq!(catalog.entries.len(), 1);
    assert!(catalog.entries.contains_key("migration-inventory"));
    assert_eq!(
        catalog.canonical_bytes().expect("canonical catalog"),
        ContractCatalog::from_json_str(
            std::str::from_utf8(&catalog.canonical_bytes().expect("canonical catalog"))
                .expect("canonical catalog is UTF-8")
        )
        .expect("canonical catalog parses")
        .canonical_bytes()
        .expect("second canonical catalog")
    );
}

#[test]
fn catalog_from_path_accepts_repository_relative_catalog_name() {
    if std::env::var_os("DISTRIBUTED_CATALOG_RELATIVE_PATH_CHILD").is_some() {
        ContractCatalog::from_path("distributed.contracts.json")
            .expect("repository-relative catalog path should resolve from the repository root");
        return;
    }

    let status = Command::new(std::env::current_exe().expect("contract test executable"))
        .current_dir(repository_root())
        .env("DISTRIBUTED_CATALOG_RELATIVE_PATH_CHILD", "1")
        .args([
            "--exact",
            "contracts::tests::catalog_from_path_accepts_repository_relative_catalog_name",
            "--quiet",
        ])
        .status()
        .expect("run repository-relative catalog test from the repository root");
    assert!(status.success());
}

#[test]
fn catalog_and_inventory_reject_sparse_oversized_files_before_reading() {
    let root = TemporaryDirectory::new("sparse-input");
    let catalog_path = root.path().join("distributed.contracts.json");
    create_sparse_file(&catalog_path, MAX_CATALOG_BYTES + 1);
    let catalog_error = ContractCatalog::from_path(&catalog_path)
        .expect_err("sparse oversized catalog must fail before allocation");
    assert_eq!(
        catalog_error.code(),
        ContractDiagnosticCode::CatalogInputLimit
    );

    let inventory_path = root.path().join("distributed.clients.json");
    create_sparse_file(&inventory_path, MAX_CATALOG_BYTES + 1);
    let inventory_error = ClientInventory::from_path(&inventory_path)
        .expect_err("sparse oversized inventory must fail before allocation");
    assert_eq!(
        inventory_error.code(),
        ContractDiagnosticCode::CatalogInputLimit
    );
}

#[test]
fn catalog_rejects_duplicate_scope_and_escaping_paths() {
    let duplicate = ContractCatalog::from_json_str(fixture("duplicate"))
        .expect_err("duplicate scopes must fail");
    assert_eq!(
        duplicate.code(),
        ContractDiagnosticCode::CatalogDuplicateScope
    );

    let escaping = ContractCatalog::from_json_str(fixture("escaping"))
        .expect_err("parent traversal must fail");
    assert_eq!(escaping.code(), ContractDiagnosticCode::CatalogPath);
}

#[test]
fn catalog_rejects_unknown_kinds_duplicate_owners_outputs_and_unbounded_globs() {
    let mut unknown_kind: Value =
        serde_json::from_str(fixture("valid")).expect("valid fixture JSON");
    unknown_kind["entries"]["application-manifest"]["kind"] =
        Value::String("future_artifact".to_string());
    let unknown_kind = ContractCatalog::from_json_str(
        &serde_json::to_string(&unknown_kind).expect("unknown kind JSON"),
    )
    .expect_err("unknown kinds must fail");
    assert_eq!(
        unknown_kind.code(),
        ContractDiagnosticCode::CatalogUnknownKind
    );

    let mut duplicate_owner: Value =
        serde_json::from_str(fixture("valid")).expect("valid fixture JSON");
    duplicate_owner["entries"]["deployment-plan"]["owner"] =
        duplicate_owner["entries"]["application-manifest"]["owner"].clone();
    let duplicate_owner = ContractCatalog::from_json_str(
        &serde_json::to_string(&duplicate_owner).expect("duplicate owner JSON"),
    )
    .expect_err("duplicate owners must fail");
    assert_eq!(
        duplicate_owner.code(),
        ContractDiagnosticCode::CatalogDuplicateOwner
    );

    let mut duplicate_output: Value =
        serde_json::from_str(fixture("valid")).expect("valid fixture JSON");
    duplicate_output["entries"]["deployment-plan"]["outputs"]["deployment"] =
        duplicate_output["entries"]["application-manifest"]["outputs"]["application"].clone();
    let duplicate_output = ContractCatalog::from_json_str(
        &serde_json::to_string(&duplicate_output).expect("duplicate output JSON"),
    )
    .expect_err("duplicate outputs must fail");
    assert_eq!(
        duplicate_output.code(),
        ContractDiagnosticCode::CatalogDuplicateOutput
    );

    let mut unbounded_glob: Value =
        serde_json::from_str(fixture("valid")).expect("valid fixture JSON");
    unbounded_glob["entries"]["application-manifest"]["provenance"]["sources"][0] =
        Value::String("migrations/**/*.sql".to_string());
    unbounded_glob["entries"]["application-manifest"]["provenance"]
        .as_object_mut()
        .expect("provenance object")
        .remove("glob_limit");
    let unbounded_glob = ContractCatalog::from_json_str(
        &serde_json::to_string(&unbounded_glob).expect("unbounded glob JSON"),
    )
    .expect_err("unbounded globs must fail");
    assert_eq!(
        unbounded_glob.code(),
        ContractDiagnosticCode::CatalogUnboundedGlob
    );
}

#[test]
fn catalog_rejects_recursive_globs_even_with_a_match_limit() {
    let mut recursive_glob: Value =
        serde_json::from_str(fixture("valid")).expect("valid fixture JSON");
    recursive_glob["entries"]["application-manifest"]["provenance"]["sources"][0] =
        Value::String("migrations/**/*.sql".to_string());
    recursive_glob["entries"]["application-manifest"]["provenance"]["glob_limit"] = Value::from(1);
    let recursive_glob = ContractCatalog::from_json_str(
        &serde_json::to_string(&recursive_glob).expect("recursive glob JSON"),
    )
    .expect_err("recursive globs must fail despite a match limit");
    assert_eq!(
        recursive_glob.code(),
        ContractDiagnosticCode::CatalogUnboundedGlob
    );
}

#[test]
fn catalog_glob_discovery_bounds_candidate_directory_entries() {
    let root = TemporaryDirectory::new("glob-candidates");
    fs::write(root.path().join("inside.txt"), b"output").expect("output fixture");
    let candidates = root.path().join("candidates");
    fs::create_dir(&candidates).expect("candidate directory");
    for index in 0..=MAX_CATALOG_DIRECTORY_ENTRIES {
        fs::write(
            candidates.join(format!("candidate-{index:04}.txt")),
            b"candidate",
        )
        .expect("candidate fixture");
    }

    let error = glob_catalog("candidates/*.sql", 1)
        .validate_paths(root.path())
        .expect_err("glob candidate traversal must be bounded independently of matches");
    assert_eq!(error.code(), ContractDiagnosticCode::CatalogInputLimit);
}

#[cfg(unix)]
#[test]
fn catalog_glob_uses_canonical_symlinked_parent_for_candidate_limits() {
    let root = TemporaryDirectory::new_short("glob-symlink-parent");
    fs::write(root.path().join("inside.txt"), b"output").expect("output fixture");
    let target = root.path().join("candidate-target");
    fs::create_dir(&target).expect("candidate target directory");
    for index in 0..=MAX_CATALOG_DIRECTORY_ENTRIES {
        fs::write(
            target.join(format!("candidate-{index:04}.txt")),
            b"candidate",
        )
        .expect("candidate fixture");
    }
    symlink(&target, root.path().join("candidates")).expect("symlinked candidate directory");

    let error = glob_catalog("candidates/*.sql", 1)
        .validate_paths(root.path())
        .expect_err("symlinked glob candidate traversal must be bounded");
    assert_eq!(error.code(), ContractDiagnosticCode::CatalogInputLimit);
}

#[test]
fn catalog_accepts_a_glob_within_a_bounded_candidate_directory() {
    let root = TemporaryDirectory::new("bounded-glob");
    fs::write(root.path().join("inside.txt"), b"output").expect("output fixture");
    let candidates = root.path().join("candidates");
    fs::create_dir(&candidates).expect("candidate directory");
    fs::write(candidates.join("selected.sql"), b"candidate").expect("candidate fixture");

    glob_catalog("candidates/*.sql", 1)
        .validate_paths(root.path())
        .expect("bounded glob candidate traversal");
}

#[test]
fn catalog_rejects_absolute_paths_and_exhausting_filesystem_traversal() {
    let absolute = ContractCatalog::from_json_str(
        &serde_json::json!({
            "schema_version": 1,
            "entries": {
                "absolute": {
                    "id": "absolute",
                    "kind": "migration_inventory",
                    "scope": { "id": "path/absolute" },
                    "owner": "test/absolute",
                    "identity": {
                        "kind": "migration_inventory",
                        "value": "sha256:absolute"
                    },
                    "provenance": {
                        "sources": ["/etc/passwd"],
                        "generator": "test.absolute"
                    },
                    "outputs": { "output": "inside.txt" }
                }
            }
        })
        .to_string(),
    )
    .expect_err("absolute paths must fail");
    assert_eq!(absolute.code(), ContractDiagnosticCode::CatalogPath);

    let root = TemporaryDirectory::new("traversal");
    fs::write(root.path().join("inside.txt"), b"output").expect("output fixture");
    let empty_tree = root.path().join("empty-tree");
    fs::create_dir(&empty_tree).expect("empty tree");
    for index in 0..=MAX_CATALOG_DIRECTORIES {
        fs::create_dir(empty_tree.join(format!("directory-{index:04}")))
            .expect("empty child directory");
    }
    let traversal = path_catalog("empty-tree")
        .validate_paths(root.path())
        .expect_err("empty directories must be bounded");
    assert_eq!(traversal.code(), ContractDiagnosticCode::CatalogInputLimit);

    let deep_tree = root.path().join("deep-tree");
    fs::create_dir(&deep_tree).expect("deep tree");
    let mut current = deep_tree;
    for depth in 0..=MAX_CATALOG_DIRECTORY_DEPTH {
        current = current.join(format!("level-{depth:02}"));
        fs::create_dir(&current).expect("deep child directory");
    }
    let depth_error = path_catalog("deep-tree")
        .validate_paths(root.path())
        .expect_err("directory depth must be bounded");
    assert_eq!(
        depth_error.code(),
        ContractDiagnosticCode::CatalogInputLimit
    );

    let many_entries = root.path().join("many-entries");
    fs::create_dir(&many_entries).expect("many-entry directory");
    for index in 0..=MAX_CATALOG_DIRECTORY_ENTRIES {
        fs::write(many_entries.join(format!("file-{index:04}")), b"entry")
            .expect("many-entry file");
    }
    let entry_error = path_catalog("many-entries")
        .validate_paths(root.path())
        .expect_err("directory entries must be bounded");
    assert_eq!(
        entry_error.code(),
        ContractDiagnosticCode::CatalogInputLimit
    );
}

#[test]
fn catalog_rejects_excessive_json_bytes_and_depth() {
    let oversized = format!(
        "{{\"schema_version\":1,\"entries\":{{}},\"padding\":\"{}\"}}",
        "x".repeat(MAX_CATALOG_BYTES)
    );
    let byte_error = ContractCatalog::from_json_str(&oversized)
        .expect_err("oversized JSON must fail before parsing");
    assert_eq!(byte_error.code(), ContractDiagnosticCode::CatalogInputLimit);

    let mut deeply_nested = String::from("{\"padding\":");
    for _ in 0..=MAX_CATALOG_JSON_DEPTH {
        deeply_nested.push('[');
    }
    deeply_nested.push_str("null");
    for _ in 0..=MAX_CATALOG_JSON_DEPTH {
        deeply_nested.push(']');
    }
    deeply_nested.push('}');
    let depth_error =
        ContractCatalog::from_json_str(&deeply_nested).expect_err("deeply nested JSON must fail");
    assert_eq!(
        depth_error.code(),
        ContractDiagnosticCode::CatalogInputLimit
    );
}

#[cfg(unix)]
#[test]
fn catalog_rejects_symlink_and_special_file_paths() {
    let root = TemporaryDirectory::new_short("filesystem");
    let outside = TemporaryDirectory::new("outside");
    fs::write(root.path().join("inside.txt"), b"output").expect("output fixture");
    let outside_file = outside.path().join("outside.txt");
    fs::write(&outside_file, b"outside").expect("outside fixture");

    symlink(&outside_file, root.path().join("escaped")).expect("symlink fixture");
    let symlink_error = path_catalog("escaped")
        .validate_paths(root.path())
        .expect_err("symlink escapes must fail");
    assert_eq!(
        symlink_error.code(),
        ContractDiagnosticCode::CatalogSymlinkEscape
    );

    let socket_path = root.path().join("special.sock");
    let _listener = UnixListener::bind(&socket_path).expect("socket fixture");
    let special_error = path_catalog("special.sock")
        .validate_paths(root.path())
        .expect_err("special files must fail");
    assert_eq!(
        special_error.code(),
        ContractDiagnosticCode::CatalogSpecialFile
    );
}

#[cfg(unix)]
#[test]
fn client_inventory_loader_rejects_symlink_inputs() {
    let root = TemporaryDirectory::new_short("inventory-symlink");
    let inventory = root.path().join("distributed.clients.json");
    fs::write(
        &inventory,
        include_str!("../../../tests/e2e-ui/ui/distributed.clients.json"),
    )
    .expect("inventory fixture");
    let link = root.path().join("inventory-link.json");
    symlink(&inventory, &link).expect("inventory symlink fixture");

    let error = ClientInventory::from_path(&link)
        .expect_err("inventory symlink inputs must be rejected before following them");
    assert_eq!(error.code(), ContractDiagnosticCode::CatalogSymlinkEscape);
    assert_eq!(error.message(), "client inventory must not be a symlink");
}

#[test]
fn artifact_chain_rejects_cycles_kind_mismatch_and_environment_values() {
    let cycle =
        ContractCatalog::from_json_str(fixture("cycle")).expect_err("predecessor cycles must fail");
    assert_eq!(cycle.code(), ContractDiagnosticCode::ChainCycle);

    let mut missing_predecessor: Value =
        serde_json::from_str(fixture("valid")).expect("valid fixture JSON");
    missing_predecessor["entries"]["deployment-plan"]["predecessor"]["entry_id"] =
        Value::String("missing-predecessor".to_string());
    let missing_predecessor = ContractCatalog::from_json_str(
        &serde_json::to_string(&missing_predecessor).expect("missing predecessor JSON"),
    )
    .expect_err("missing predecessor must fail");
    assert_eq!(
        missing_predecessor.code(),
        ContractDiagnosticCode::ChainMissingPredecessor
    );

    let mut kind_mismatch: Value =
        serde_json::from_str(fixture("valid")).expect("valid fixture JSON");
    kind_mismatch["entries"]["deployment-plan"]["predecessor"]["identity"]["kind"] =
        Value::String("deployment_plan".to_string());
    let kind_mismatch = ContractCatalog::from_json_str(
        &serde_json::to_string(&kind_mismatch).expect("kind mismatch JSON"),
    )
    .expect_err("predecessor kind mismatch must fail");
    assert_eq!(
        kind_mismatch.code(),
        ContractDiagnosticCode::ChainKindMismatch
    );

    let environment = ContractCatalog::from_json_str(fixture("environment"))
        .expect_err("raw environment values must fail");
    assert_eq!(environment.code(), ContractDiagnosticCode::EnvironmentValue);
}

#[test]
fn human_and_json_diagnostics_preserve_same_facts_and_redact_values() {
    let migration = ContractDiagnostic::new(
        ContractDiagnosticCode::MigrationInventory,
        Some(ContractArtifactKind::MigrationInventory),
        Some("repository/migrations"),
        "distributed::migrations",
        ["migrations/postgres/0005_new.sql"],
        ["distributed.contracts.json"],
        None::<&str>,
        Some("sha256:before"),
        Some("sha256:after"),
        Some("add migration 5"),
        Some(true),
        "distributed contracts check --base <revision>",
    )
    .with_detail("migration inventory is missing a registered file");
    let schema = ContractDiagnostic::new(
        ContractDiagnosticCode::SchemaDrift,
        Some(ContractArtifactKind::SurfaceClientManifest),
        Some("e2e-ui"),
        "query-layer::surface",
        ["tests/e2e-ui/ui/distributed.clients.json"],
        ["tests/e2e-ui/ui/src/lib/generated/user/manifest.json"],
        Some("models.Todo.owner.nullable"),
        Some("postgres://user:password@example.invalid/db"),
        Some("Bearer hidden-token"),
        Some("surface schema drift"),
        None,
        "distributed contracts accept --scope client:e2e-ui",
    );
    let mut result = ContractCheckResult::default();
    result.push(migration.clone());
    result.push(schema.clone());

    let human = result.human();
    let json: Value = serde_json::from_str(&result.to_json().expect("diagnostic JSON"))
        .expect("diagnostic JSON object");
    assert!(human.contains("CTL-MIG-INVENTORY"));
    assert!(human.contains("CTL-SCHEMA-DRIFT"));
    assert!(human.contains("[REDACTED]"));
    assert!(!human.contains("postgres://"));
    assert_eq!(
        json["diagnostics"]
            .as_array()
            .expect("diagnostic list")
            .len(),
        2
    );
    assert_eq!(json["diagnostics"][1]["code"], "CTL-SCHEMA-DRIFT");
    assert_eq!(json["diagnostics"][1]["expected"], "[REDACTED]");
    assert_eq!(json["diagnostics"][1]["observed"], "[REDACTED]");
    assert_eq!(
        result,
        serde_json::from_str(&result.to_json().expect("diagnostic JSON"))
            .expect("diagnostic JSON round-trip")
    );
    assert!(!result
        .to_json()
        .expect("diagnostic JSON")
        .contains("postgres://"));
    assert_eq!(
        result.canonical_bytes().expect("canonical result"),
        result.canonical_bytes().expect("second canonical result")
    );

    let decoded: ContractDiagnostic = serde_json::from_value(serde_json::json!({
        "code": "CTL-SCHEMA-DRIFT",
        "scope": "secret=scope",
        "owner": "postgres://user:password@example.invalid/db",
        "source_paths": ["token=source"],
        "derived_paths": ["secret=derived"],
        "semantic_path": "password=semantic",
        "expected": "secret=expected",
        "observed": "Bearer observed-token",
        "required_classification": "token=classification",
        "repair_command": "secret=repair",
        "detail": "password=detail"
    }))
    .expect("diagnostic JSON round-trip");
    assert!(!decoded.human().contains("postgres://"));
    assert!(!decoded.human().contains("password="));
    assert!(!decoded.human().contains("token="));

    let mut directly_mutated = migration;
    directly_mutated.scope = Some("secret=scope".to_string());
    directly_mutated.owner = "postgres://user:password@example.invalid/db".to_string();
    directly_mutated
        .source_paths
        .insert("token=source".to_string());
    directly_mutated
        .derived_paths
        .insert("secret=derived".to_string());
    directly_mutated.semantic_path = Some("password=semantic".to_string());
    directly_mutated.required_classification = Some("token=classification".to_string());
    directly_mutated.repair_command = "secret=repair".to_string();
    directly_mutated.detail = "password=detail".to_string();

    let direct_human = directly_mutated.human();
    let direct_json = serde_json::to_string(&directly_mutated).expect("direct diagnostic JSON");
    let direct_debug = format!("{directly_mutated:?}");
    assert!(!direct_human.contains("postgres://"));
    assert!(!direct_human.contains("password="));
    assert!(!direct_human.contains("token="));
    assert!(!direct_json.contains("postgres://"));
    assert!(!direct_json.contains("password="));
    assert!(!direct_json.contains("token="));
    assert!(!direct_debug.contains("postgres://"));
    assert!(!direct_debug.contains("password="));
    assert!(!direct_debug.contains("token="));
}

#[test]
fn client_inventory_canonicalizes_client_and_document_order() {
    let inventory = ClientInventory::from_path(
        repository_root().join("tests/e2e-ui/ui/distributed.clients.json"),
    )
    .expect("client inventory");
    let mut reversed = inventory.clone();
    reversed.clients.reverse();
    assert_eq!(
        inventory.canonical_bytes().expect("canonical inventory"),
        reversed
            .canonical_bytes()
            .expect("reversed canonical inventory")
    );
}

#[test]
fn canonical_bytes_reject_direct_public_secret_mutation() {
    let mut catalog = ContractCatalog::from_json_str(fixture("valid")).expect("valid catalog");
    catalog
        .entries
        .get_mut("application-manifest")
        .expect("application entry")
        .identity
        .value = "postgres://user:password@example.invalid/db".to_string();
    let catalog_error = catalog
        .canonical_bytes()
        .expect_err("catalog serialization must validate public mutations");
    assert_eq!(
        catalog_error.code(),
        ContractDiagnosticCode::EnvironmentValue
    );

    let mut inventory = ClientInventory::from_json_str(include_str!(
        "../../../tests/e2e-ui/ui/distributed.clients.json"
    ))
    .expect("valid client inventory");
    inventory.clients[0]
        .documents
        .insert("src/routes/token=secret.graphql".to_string());
    let inventory_error = inventory
        .canonical_bytes()
        .expect_err("client serialization must validate public mutations");
    assert_eq!(
        inventory_error.code(),
        ContractDiagnosticCode::EnvironmentValue
    );
}

#[test]
fn check_and_aggregate_renderers_do_not_leak_mutated_artifact_identity() {
    let mut catalog = ContractCatalog::from_json_str(fixture("valid")).expect("valid catalog");
    catalog
        .entries
        .get_mut("application-manifest")
        .expect("application entry")
        .identity
        .value = "postgres://user:password@example.invalid/db".to_string();

    let checked = catalog.check(repository_root());
    assert!(checked.artifacts.is_empty());
    let checked_json = checked.to_json().expect("checked JSON");
    let checked_canonical =
        String::from_utf8(checked.canonical_bytes().expect("checked canonical JSON"))
            .expect("checked JSON is UTF-8");
    let checked_debug = format!("{checked:?}");
    assert!(!checked_json.contains("postgres://"));
    assert!(!checked_canonical.contains("postgres://"));
    assert!(!checked_debug.contains("postgres://"));

    let mut directly_mutated = ContractCheckResult {
        catalog_identity: Some("token=identity".to_string()),
        ..Default::default()
    };
    directly_mutated.artifacts.insert(
        "secret=entry".to_string(),
        ArtifactIdentity::new(
            ContractArtifactKind::ApplicationManifest,
            "password=artifact",
        ),
    );
    let direct_json = directly_mutated.to_json().expect("aggregate JSON");
    let direct_canonical = String::from_utf8(
        directly_mutated
            .canonical_bytes()
            .expect("aggregate canonical JSON"),
    )
    .expect("aggregate JSON is UTF-8");
    let direct_debug = format!("{directly_mutated:?}");
    assert!(!direct_json.contains("token=identity"));
    assert!(!direct_json.contains("secret=entry"));
    assert!(!direct_json.contains("password=artifact"));
    assert!(!direct_canonical.contains("token=identity"));
    assert!(!direct_canonical.contains("secret=entry"));
    assert!(!direct_canonical.contains("password=artifact"));
    assert!(!direct_debug.contains("token=identity"));
    assert!(!direct_debug.contains("secret=entry"));
    assert!(!direct_debug.contains("password=artifact"));
}

#[test]
fn client_inventory_rust_schema_matches_shared_parity_vectors() {
    let vectors: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/contracts/client-inventory-parity.json"
    ))
    .expect("client inventory parity JSON");
    for vector in vectors["vectors"].as_array().expect("parity vectors") {
        let name = vector["name"].as_str().expect("parity vector name");
        let expected = vector["valid"].as_bool().expect("parity vector result");
        let input = serde_json::to_string(&vector["inventory"]).expect("parity inventory JSON");
        assert_eq!(
            ClientInventory::from_json_str(&input).is_ok(),
            expected,
            "Rust client inventory parity vector `{name}`"
        );
    }
}

#[test]
fn migration_inventory_rejects_missing_extra_and_dialect_drift() {
    let (root, sqlite_sql, postgres_sql) = create_migration_fixture("inventory-vectors");
    let inventory = MigrationInventory::from_repository_root(root.path())
        .expect("valid migration fixture inventory");
    assert_eq!(inventory.migrations[0].version, 1);

    let inventory_path = root.path().join(MIGRATION_INVENTORY_PATH);
    let mut missing_dialect: Value =
        serde_json::from_slice(&fs::read(&inventory_path).expect("read inventory"))
            .expect("inventory value");
    missing_dialect["migrations"][0]
        .as_object_mut()
        .expect("migration object")
        .remove("sqlite");
    let missing_dialect_error = MigrationInventory::from_json_str(
        &serde_json::to_string(&missing_dialect).expect("missing dialect JSON"),
    )
    .expect_err("missing dialect must fail");
    assert_eq!(
        missing_dialect_error.code(),
        ContractDiagnosticCode::MigrationInventory
    );

    fs::remove_file(root.path().join("migrations/sqlite/0001_initial.sql"))
        .expect("remove declared migration");
    let missing_file = check_migration_inventory(root.path());
    assert!(!missing_file.is_ok());
    assert!(missing_file.human().contains("CTL-MIG-INVENTORY"));
    assert!(missing_file
        .human()
        .contains("migrations/sqlite/0001_initial.sql"));
    write_repository_file(
        root.path(),
        "migrations/sqlite/0001_initial.sql",
        &sqlite_sql,
    );

    write_repository_file(
        root.path(),
        "migrations/sqlite/0002_extra.sql",
        b"CREATE TABLE extra (id INTEGER);\n",
    );
    let extra_file = check_migration_inventory(root.path());
    assert!(!extra_file.is_ok());
    assert!(extra_file
        .human()
        .contains("migrations/sqlite/0002_extra.sql"));
    fs::remove_file(root.path().join("migrations/sqlite/0002_extra.sql"))
        .expect("remove extra migration");

    let mut dialect_drift: Value =
        serde_json::from_slice(&fs::read(&inventory_path).expect("read inventory"))
            .expect("inventory value");
    dialect_drift["migrations"][0]["postgres"]["path"] =
        Value::String("migrations/mysql/0001_initial.sql".to_string());
    let dialect_drift_error = MigrationInventory::from_json_str(
        &serde_json::to_string(&dialect_drift).expect("dialect drift JSON"),
    )
    .expect_err("dialect drift must fail");
    assert_eq!(
        dialect_drift_error.code(),
        ContractDiagnosticCode::MigrationInventory
    );
    assert!(dialect_drift_error
        .message()
        .contains("migrations/mysql/0001_initial.sql"));
    assert_eq!(
        migration_checksum(&postgres_sql),
        inventory.migrations[0].postgres.sha256
    );
}

#[test]
fn migration_inventory_rejects_duplicate_and_non_consecutive_versions() {
    let (root, sqlite_sql, postgres_sql) = create_migration_fixture("inventory-order");
    let inventory_path = root.path().join(MIGRATION_INVENTORY_PATH);
    let mut non_consecutive: Value =
        serde_json::from_slice(&fs::read(&inventory_path).expect("read inventory"))
            .expect("inventory value");
    non_consecutive["migrations"][0]["version"] = Value::from(2_u64);
    let error = MigrationInventory::from_json_str(
        &serde_json::to_string(&non_consecutive).expect("non-consecutive JSON"),
    )
    .expect_err("non-consecutive version must fail");
    assert_eq!(error.code(), ContractDiagnosticCode::MigrationInventory);

    let mut duplicate =
        serde_json::from_slice::<Value>(&fs::read(&inventory_path).expect("read inventory"))
            .expect("inventory value");
    duplicate["migrations"] = json!([
        migration_entry(
            1,
            "migrations/sqlite/0001_initial.sql",
            &sqlite_sql,
            "migrations/postgres/0001_initial.sql",
            &postgres_sql,
        ),
        migration_entry(
            1,
            "migrations/sqlite/0002_duplicate.sql",
            &sqlite_sql,
            "migrations/postgres/0002_duplicate.sql",
            &postgres_sql,
        )
    ]);
    let error = MigrationInventory::from_json_str(
        &serde_json::to_string(&duplicate).expect("duplicate JSON"),
    )
    .expect_err("duplicate version must fail");
    assert_eq!(error.code(), ContractDiagnosticCode::MigrationInventory);
}

#[test]
fn migration_inventory_rejects_checksum_path_and_symlink_mutations() {
    let (root, sqlite_sql, postgres_sql) = create_migration_fixture("inventory-security");
    let inventory_path = root.path().join(MIGRATION_INVENTORY_PATH);
    let mut bad_checksum: Value =
        serde_json::from_slice(&fs::read(&inventory_path).expect("read inventory"))
            .expect("inventory value");
    bad_checksum["migrations"][0]["sqlite"]["sha256"] = Value::String("0".repeat(64));
    write_json_value(&inventory_path, &bad_checksum);
    let error = MigrationInventory::from_repository_root(root.path())
        .expect_err("checksum mutation must fail");
    assert_eq!(error.code(), ContractDiagnosticCode::MigrationInventory);
    assert!(error.message().contains("0001_initial.sql"));

    let valid = migration_entry(
        1,
        "migrations/sqlite/0001_initial.sql",
        &sqlite_sql,
        "migrations/postgres/0001_initial.sql",
        &postgres_sql,
    );
    write_migration_inventory(root.path(), std::slice::from_ref(&valid));
    let mut bad_path: Value =
        serde_json::from_slice(&fs::read(&inventory_path).expect("read inventory"))
            .expect("inventory value");
    bad_path["migrations"][0]["sqlite"]["path"] =
        Value::String("migrations/sqlite/../outside.sql".to_string());
    let error = MigrationInventory::from_json_str(
        &serde_json::to_string(&bad_path).expect("path mutation JSON"),
    )
    .expect_err("path traversal must fail");
    assert_eq!(error.code(), ContractDiagnosticCode::MigrationInventory);

    bad_path["migrations"][0]["sqlite"]["path"] = Value::String(
        root.path()
            .join("outside.sql")
            .to_string_lossy()
            .into_owned(),
    );
    let error = MigrationInventory::from_json_str(
        &serde_json::to_string(&bad_path).expect("absolute path JSON"),
    )
    .expect_err("absolute path must fail without exposing the machine path");
    assert_eq!(error.code(), ContractDiagnosticCode::MigrationInventory);
    assert!(!error
        .message()
        .contains(&root.path().to_string_lossy().to_string()));

    #[cfg(unix)]
    {
        write_migration_inventory(root.path(), std::slice::from_ref(&valid));
        fs::remove_file(root.path().join("migrations/sqlite/0001_initial.sql"))
            .expect("remove migration for symlink fixture");
        let outside = root.path().join("outside.sql");
        fs::write(&outside, &sqlite_sql).expect("outside SQL");
        symlink(
            &outside,
            root.path().join("migrations/sqlite/0001_initial.sql"),
        )
        .expect("migration symlink");
        let error = MigrationInventory::from_repository_root(root.path())
            .expect_err("symlink migration must fail");
        assert_eq!(error.code(), ContractDiagnosticCode::CatalogSymlinkEscape);
    }

    let declared_path = root.path().join("migrations/sqlite/0001_initial.sql");
    if declared_path.is_symlink() {
        fs::remove_file(&declared_path).expect("remove migration symlink");
    } else if declared_path.exists() {
        fs::remove_file(&declared_path).expect("remove migration file for special-file fixture");
    }
    fs::create_dir(&declared_path).expect("migration special-file fixture");
    let error = MigrationInventory::from_repository_root(root.path())
        .expect_err("directory at declared SQL path must fail");
    assert_eq!(error.code(), ContractDiagnosticCode::CatalogSpecialFile);
    assert!(!error
        .message()
        .contains(&root.path().to_string_lossy().to_string()));
}

#[test]
fn migration_inventory_redacts_sensitive_and_non_normal_paths() {
    let (root, sqlite_sql, postgres_sql) = create_migration_fixture("inventory-path-redaction");
    let inventory_path = root.path().join(MIGRATION_INVENTORY_PATH);
    let unique_secret = "migration-path-secret-7f5e1c9b";
    let mut sensitive: Value =
        serde_json::from_slice(&fs::read(&inventory_path).expect("read inventory"))
            .expect("inventory value");
    sensitive["migrations"][0]["sqlite"]["path"] =
        Value::String(format!("migrations/sqlite/password={unique_secret}.sql"));
    let sensitive_json = serde_json::to_string(&sensitive).expect("sensitive path JSON");
    let error = MigrationInventory::from_json_str(&sensitive_json)
        .expect_err("credential-like path must fail safely");
    for rendered in [
        error.message().to_string(),
        error.to_string(),
        format!("{error:?}"),
    ] {
        assert!(
            !rendered.contains(unique_secret),
            "sensitive path leaked from error rendering: {rendered}"
        );
    }
    write_json_value(&inventory_path, &sensitive);
    let checked = check_migration_inventory(root.path());
    for rendered in [
        checked.human(),
        checked.to_json().expect("sensitive diagnostic JSON"),
        format!("{checked:?}"),
    ] {
        assert!(
            !rendered.contains(unique_secret),
            "sensitive path leaked from diagnostic rendering: {rendered}"
        );
    }

    let traversal_sentinel = "migration-traversal-sentinel-3c2a8e1d";
    let mut traversal = sensitive;
    traversal["migrations"][0]["sqlite"]["path"] =
        Value::String(format!("migrations/sqlite/../{traversal_sentinel}.sql"));
    let traversal_error = MigrationInventory::from_json_str(
        &serde_json::to_string(&traversal).expect("traversal path JSON"),
    )
    .expect_err("non-normal path must fail safely");
    assert!(!traversal_error.message().contains(traversal_sentinel));
    assert!(!format!("{traversal_error:?}").contains(traversal_sentinel));

    let safe_relative = "migrations/sqlite/safe-relative-diagnostic.sql";
    traversal["migrations"][0]["sqlite"]["path"] = Value::String(safe_relative.to_string());
    MigrationInventory::from_json_str(
        &serde_json::to_string(&traversal).expect("safe relative path JSON"),
    )
    .expect("safe relative path should remain structurally valid");
    write_json_value(&inventory_path, &traversal);
    let safe_error = MigrationInventory::from_repository_root(root.path())
        .expect_err("missing safe relative path must be reported");
    assert!(safe_error.message().contains(safe_relative));
    let safe_checked = check_migration_inventory(root.path());
    assert!(safe_checked.human().contains(safe_relative));
    assert!(safe_checked
        .to_json()
        .expect("safe relative diagnostic JSON")
        .contains(safe_relative));

    write_migration_inventory(
        root.path(),
        &[migration_entry(
            1,
            "migrations/sqlite/0001_initial.sql",
            &sqlite_sql,
            "migrations/postgres/0001_initial.sql",
            &postgres_sql,
        )],
    );
    write_repository_file(
        root.path(),
        &format!("migrations/sqlite/password={unique_secret}.sql"),
        b"CREATE TABLE secret_path (id INTEGER);\n",
    );
    let extra_checked = check_migration_inventory(root.path());
    for rendered in [
        extra_checked.human(),
        extra_checked.to_json().expect("extra diagnostic JSON"),
        format!("{extra_checked:?}"),
    ] {
        assert!(
            !rendered.contains(unique_secret),
            "sensitive extra path leaked from diagnostic rendering: {rendered}"
        );
    }
}

#[test]
fn migration_inventory_rejects_json_nesting_before_parse_and_ignores_strings() {
    let nested_json = |count: usize| format!("{}0{}", "[".repeat(count), "]".repeat(count));
    let at_limit = format!(
        r#"{{"schema_version":1,"migrations":[],"padding":{}}}"#,
        nested_json(MAX_MIGRATION_JSON_DEPTH - 1)
    );
    let at_limit_error = MigrationInventory::from_json_str(&at_limit)
        .expect_err("the configured JSON nesting limit should reach serde");
    assert!(!at_limit_error
        .message()
        .contains("maximum JSON nesting depth"));

    let over_limit = format!(
        r#"{{"schema_version":1,"migrations":[],"padding":{}}}"#,
        nested_json(MAX_MIGRATION_JSON_DEPTH)
    );
    let over_limit_error = MigrationInventory::from_json_str(&over_limit)
        .expect_err("JSON beyond the configured nesting limit must fail");
    assert!(over_limit_error
        .message()
        .contains("maximum JSON nesting depth"));

    let string_delimiters = r#"{"schema_version":1,"migrations":[],"padding":"braces {[ ]} and an escaped quote: \" plus [nested]"}"#;
    let string_error = MigrationInventory::from_json_str(string_delimiters)
        .expect_err("unknown string field should fail structural parsing");
    assert!(!string_error
        .message()
        .contains("maximum JSON nesting depth"));
}

#[test]
fn migration_inventory_rejects_total_and_top_level_entry_floods() {
    let (root, _, _) = create_migration_fixture("entry-flood");
    for index in 0..=MAX_MIGRATION_TOTAL_ENTRIES {
        write_repository_file(
            root.path(),
            &format!("migrations/sqlite/noise-{index:04}.txt"),
            b"not SQL",
        );
    }
    let error = MigrationInventory::from_repository_root(root.path())
        .expect_err("non-SQL entries must count toward the dialect traversal bound");
    assert_eq!(error.code(), ContractDiagnosticCode::MigrationInventory);
    assert!(error.message().contains("entries"));
    assert!(!error
        .message()
        .contains(&root.path().to_string_lossy().to_string()));

    let (root, _, _) = create_migration_fixture("top-level-entry-flood");
    for index in 0..=MAX_MIGRATION_TOP_LEVEL_ENTRIES {
        write_repository_file(
            root.path(),
            &format!("migrations/top-level-noise-{index:03}.txt"),
            b"ignored top-level file",
        );
    }
    let error = MigrationInventory::from_repository_root(root.path())
        .expect_err("top-level migrations entries must be bounded");
    assert_eq!(error.code(), ContractDiagnosticCode::MigrationInventory);
    assert!(error.message().contains("migrations directory"));
    assert!(!error
        .message()
        .contains(&root.path().to_string_lossy().to_string()));
}

#[test]
fn migration_inventory_directory_diagnostics_are_deterministic() {
    let (root, _, _) = create_migration_fixture("directory-order");
    fs::create_dir(root.path().join("migrations/zzz-invalid"))
        .expect("late invalid dialect directory");
    fs::create_dir(root.path().join("migrations/aaa-invalid"))
        .expect("early invalid dialect directory");

    let first = check_migration_inventory(root.path());
    let second = check_migration_inventory(root.path());
    assert_eq!(first.human(), second.human());
    assert_eq!(
        first.to_json().expect("first diagnostic JSON"),
        second.to_json().expect("second diagnostic JSON")
    );
    assert!(first.human().contains("migrations/aaa-invalid"));
    assert!(!first.human().contains("migrations/zzz-invalid"));
}

fn write_json_value(path: &Path, value: &Value) {
    fs::write(
        path,
        serde_json::to_vec_pretty(value).expect("JSON value serialization"),
    )
    .expect("JSON value file");
}

#[test]
fn migration_inventory_is_deterministic_and_preserves_runtime_order() {
    let inventory = MigrationInventory::from_repository_root(repository_root())
        .expect("repository migration inventory");
    let versions = inventory
        .migrations
        .iter()
        .map(|migration| migration.version)
        .collect::<Vec<_>>();
    assert_eq!(versions, vec![1, 2, 3, 4, 5]);
    assert_eq!(
        inventory.canonical_bytes().expect("canonical inventory"),
        inventory
            .canonical_bytes()
            .expect("second canonical inventory")
    );
}

#[test]
fn released_v3_3_4_migration_fixture_matches_provenance() {
    let baseline: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/migrations/v3.3.4/baseline.json"
    ))
    .expect("released migration baseline JSON");
    assert_eq!(baseline["release_tag"], "v3.3.4");
    assert_eq!(
        baseline["revision"],
        "f57543ddeb9e293cf366dba1a2330b34ce6509f0"
    );
    assert_eq!(baseline["migration_count"], 1);
    assert_eq!(
        baseline["migrations"][0]["sqlite"]["sha256"],
        migration_checksum(include_bytes!(
            "../../../tests/fixtures/migrations/v3.3.4/sqlite/0001_initial.sql"
        ))
    );
    assert_eq!(
        baseline["migrations"][0]["postgres"]["sha256"],
        migration_checksum(include_bytes!(
            "../../../tests/fixtures/migrations/v3.3.4/postgres/0001_initial.sql"
        ))
    );
}

#[test]
fn baseline_history_rejects_coordinated_sql_and_checksum_edits() {
    let (root, base_sqlite, base_postgres) = create_migration_fixture("history-edit");
    let base_revision = commit_migration_fixture(root.path());

    let mutated_sqlite = b"CREATE TABLE one (id INTEGER PRIMARY KEY, changed INTEGER);\n";
    write_repository_file(
        root.path(),
        "migrations/sqlite/0001_initial.sql",
        mutated_sqlite,
    );
    write_migration_inventory(
        root.path(),
        &[migration_entry(
            1,
            "migrations/sqlite/0001_initial.sql",
            mutated_sqlite,
            "migrations/postgres/0001_initial.sql",
            &base_postgres,
        )],
    );
    let current = MigrationInventory::from_repository_root(root.path())
        .expect("coordinated local checksum edit remains current-tree valid");
    let refused = current.check_history(root.path(), &base_revision);
    assert!(!refused.is_verified());
    assert!(refused.human().contains("CTL-MIG-HISTORY"));
    assert!(refused
        .human()
        .contains("migrations/sqlite/0001_initial.sql"));
    assert!(refused.human().contains("add migration 2"));

    let migration_two_sqlite = b"ALTER TABLE one ADD COLUMN added INTEGER;\n";
    let migration_two_postgres = b"ALTER TABLE one ADD COLUMN added BIGINT;\n";
    write_repository_file(
        root.path(),
        "migrations/sqlite/0001_initial.sql",
        &base_sqlite,
    );
    write_repository_file(
        root.path(),
        "migrations/sqlite/0002_next.sql",
        migration_two_sqlite,
    );
    write_repository_file(
        root.path(),
        "migrations/postgres/0002_next.sql",
        migration_two_postgres,
    );
    write_migration_inventory(
        root.path(),
        &[
            migration_entry(
                1,
                "migrations/sqlite/0001_initial.sql",
                &base_sqlite,
                "migrations/postgres/0001_initial.sql",
                &base_postgres,
            ),
            migration_entry(
                2,
                "migrations/sqlite/0002_next.sql",
                migration_two_sqlite,
                "migrations/postgres/0002_next.sql",
                migration_two_postgres,
            ),
        ],
    );
    let repaired = MigrationInventory::from_repository_root(root.path())
        .expect("restored baseline plus new migration inventory");
    let accepted = repaired.check_history(root.path(), &base_revision);
    assert!(accepted.is_verified(), "{}", accepted.human());
}

#[test]
fn baseline_history_rejects_description_delete_renumber_and_reorder() {
    let (root, first_sqlite, first_postgres) = create_migration_fixture("history-vectors");
    let second_sqlite = b"ALTER TABLE one ADD COLUMN added INTEGER;\n";
    let second_postgres = b"ALTER TABLE one ADD COLUMN added BIGINT;\n";
    write_repository_file(
        root.path(),
        "migrations/sqlite/0002_next.sql",
        second_sqlite,
    );
    write_repository_file(
        root.path(),
        "migrations/postgres/0002_next.sql",
        second_postgres,
    );
    let baseline_entries = vec![
        migration_entry(
            1,
            "migrations/sqlite/0001_initial.sql",
            &first_sqlite,
            "migrations/postgres/0001_initial.sql",
            &first_postgres,
        ),
        migration_entry(
            2,
            "migrations/sqlite/0002_next.sql",
            second_sqlite,
            "migrations/postgres/0002_next.sql",
            second_postgres,
        ),
    ];
    write_migration_inventory(root.path(), &baseline_entries);
    let base_revision = commit_migration_fixture(root.path());
    let baseline = MigrationInventory::from_repository_root(root.path())
        .expect("two-migration baseline inventory");

    let mut description = baseline.clone();
    description.migrations[0].description = "edited".to_string();
    let refused = description.check_history(root.path(), &base_revision);
    assert!(!refused.is_verified());
    assert!(refused.human().contains("description changed"));

    fs::remove_file(root.path().join("migrations/sqlite/0002_next.sql"))
        .expect("remove deleted sqlite migration");
    fs::remove_file(root.path().join("migrations/postgres/0002_next.sql"))
        .expect("remove deleted postgres migration");
    let mut deleted = baseline.clone();
    deleted.migrations.pop();
    let refused = deleted.check_history(root.path(), &base_revision);
    assert!(!refused.is_verified());
    assert!(refused.human().contains("baseline migration 2 was deleted"));
    assert!(refused.human().contains("add migration 3"));

    write_repository_file(
        root.path(),
        "migrations/sqlite/0002_next.sql",
        second_sqlite,
    );
    write_repository_file(
        root.path(),
        "migrations/postgres/0002_next.sql",
        second_postgres,
    );
    let mut renumbered = baseline.clone();
    renumbered.migrations[1].version = 3;
    let refused = renumbered.check_history(root.path(), &base_revision);
    assert!(!refused.is_verified());
    assert!(refused.human().contains("CTL-MIG-HISTORY"));
    assert!(refused.human().contains("order or numbering changed"));

    let mut reordered = baseline;
    reordered.migrations.swap(0, 1);
    let refused = reordered.check_history(root.path(), &base_revision);
    assert!(!refused.is_verified());
    assert!(refused.human().contains("CTL-MIG-HISTORY"));
    assert!(refused.human().contains("order or numbering changed"));
}

#[test]
fn baseline_newer_deletion_repair_never_reuses_immutable_version() {
    let (root, first_sqlite, first_postgres) = create_migration_fixture("history-newer");
    let second_sqlite = b"ALTER TABLE one ADD COLUMN second INTEGER;\n";
    let second_postgres = b"ALTER TABLE one ADD COLUMN second BIGINT;\n";
    let third_sqlite = b"ALTER TABLE one ADD COLUMN third INTEGER;\n";
    let third_postgres = b"ALTER TABLE one ADD COLUMN third BIGINT;\n";
    write_repository_file(
        root.path(),
        "migrations/sqlite/0002_second.sql",
        second_sqlite,
    );
    write_repository_file(
        root.path(),
        "migrations/postgres/0002_second.sql",
        second_postgres,
    );
    write_repository_file(
        root.path(),
        "migrations/sqlite/0003_third.sql",
        third_sqlite,
    );
    write_repository_file(
        root.path(),
        "migrations/postgres/0003_third.sql",
        third_postgres,
    );
    let baseline_entries = vec![
        migration_entry(
            1,
            "migrations/sqlite/0001_initial.sql",
            &first_sqlite,
            "migrations/postgres/0001_initial.sql",
            &first_postgres,
        ),
        migration_entry(
            2,
            "migrations/sqlite/0002_second.sql",
            second_sqlite,
            "migrations/postgres/0002_second.sql",
            second_postgres,
        ),
        migration_entry(
            3,
            "migrations/sqlite/0003_third.sql",
            third_sqlite,
            "migrations/postgres/0003_third.sql",
            third_postgres,
        ),
    ];
    write_migration_inventory(root.path(), &baseline_entries);
    let base_revision = commit_migration_fixture(root.path());
    let baseline =
        MigrationInventory::from_repository_root(root.path()).expect("newer baseline inventory");

    fs::remove_file(root.path().join("migrations/sqlite/0003_third.sql"))
        .expect("remove deleted third sqlite migration");
    fs::remove_file(root.path().join("migrations/postgres/0003_third.sql"))
        .expect("remove deleted third postgres migration");
    let mut deleted = baseline.clone();
    deleted.migrations.pop();
    let refused = deleted.check_history(root.path(), &base_revision);
    assert!(!refused.is_verified());
    assert!(refused.human().contains("baseline migration 3 was deleted"));
    assert!(refused.human().contains("add migration 4"));
    assert!(!refused.human().contains("add migration 3"));
}

#[test]
fn oversized_git_baseline_object_is_unavailable_without_leaking_contents() {
    let (root, base_sqlite, base_postgres) = create_migration_fixture("history-oversized");
    let oversized_sql = vec![b'x'; MAX_MIGRATION_SQL_BYTES + 1];
    write_repository_file(
        root.path(),
        "migrations/sqlite/0001_initial.sql",
        &oversized_sql,
    );
    write_migration_inventory(
        root.path(),
        &[migration_entry(
            1,
            "migrations/sqlite/0001_initial.sql",
            &oversized_sql,
            "migrations/postgres/0001_initial.sql",
            &base_postgres,
        )],
    );
    let base_revision = commit_migration_fixture(root.path());

    write_repository_file(
        root.path(),
        "migrations/sqlite/0001_initial.sql",
        &base_sqlite,
    );
    write_migration_inventory(
        root.path(),
        &[migration_entry(
            1,
            "migrations/sqlite/0001_initial.sql",
            &base_sqlite,
            "migrations/postgres/0001_initial.sql",
            &base_postgres,
        )],
    );
    let current = MigrationInventory::from_repository_root(root.path())
        .expect("current tree remains valid after oversized baseline");
    let refused = current.check_history(root.path(), &base_revision);
    assert!(refused.is_unavailable());
    assert!(!refused.is_verified());
    assert!(refused
        .baseline
        .reason()
        .expect("oversized baseline reason")
        .contains("exceeds"));
    let human = refused.human();
    assert!(human.contains("history evidence unavailable"));
    assert!(!human.contains("xxxxxxxx"));

    let (root, current_sqlite, current_postgres) =
        create_migration_fixture("history-inventory-limit");
    let inventory_path = root.path().join(MIGRATION_INVENTORY_PATH);
    let mut oversized_inventory: Value =
        serde_json::from_slice(&fs::read(&inventory_path).expect("read inventory baseline"))
            .expect("inventory baseline JSON");
    oversized_inventory["padding"] = Value::String("y".repeat(MAX_MIGRATION_INVENTORY_BYTES));
    write_json_value(&inventory_path, &oversized_inventory);
    let inventory_base_revision = commit_migration_fixture(root.path());
    write_repository_file(
        root.path(),
        "migrations/sqlite/0001_initial.sql",
        &current_sqlite,
    );
    write_repository_file(
        root.path(),
        "migrations/postgres/0001_initial.sql",
        &current_postgres,
    );
    write_migration_inventory(
        root.path(),
        &[migration_entry(
            1,
            "migrations/sqlite/0001_initial.sql",
            &current_sqlite,
            "migrations/postgres/0001_initial.sql",
            &current_postgres,
        )],
    );
    let current = MigrationInventory::from_repository_root(root.path())
        .expect("current tree remains valid after oversized inventory baseline");
    let refused = current.check_history(root.path(), &inventory_base_revision);
    assert!(refused.is_unavailable());
    assert!(refused
        .baseline
        .reason()
        .expect("oversized inventory baseline reason")
        .contains("exceeds"));
}

#[test]
fn unavailable_merge_base_is_reported_not_verified() {
    let root = repository_root();
    let inventory =
        MigrationInventory::from_repository_root(&root).expect("repository migration inventory");
    let result = inventory.check_history(&root, "missing-merge-base-for-test");
    assert!(result.is_unavailable());
    assert!(!result.is_verified());
    assert!(matches!(
        result.baseline,
        BaselineAvailability::Unavailable { .. }
    ));
    assert!(result.human().contains("history evidence unavailable"));
    assert!(result.human().contains("merge_base_available=false"));
}
