use super::*;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::symlink;

const E2E_UI_CLIENTS: &str = include_str!("../../../tests/e2e-ui/ui/distributed.clients.json");

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("distributed_cli has a repository parent")
        .to_path_buf()
}

fn entry(id: &str, kind: ContractArtifactKind, source: &str, output: &str) -> ContractEntry {
    ContractEntry {
        id: id.into(),
        kind,
        scope: ContractScope {
            id: format!("scope/{id}"),
        },
        owner: format!("owner/{id}"),
        identity: ArtifactIdentity::new(kind, format!("ref:{id}")),
        provenance: ArtifactProvenance {
            sources: [source.to_string()].into_iter().collect(),
            generator: format!("generator.{id}"),
            glob_limit: source.contains('*').then_some(32),
        },
        predecessor: None,
        outputs: BTreeMap::from([(format!("{id}-artifact"), output.into())]),
        lifecycle: ["build".into()].into_iter().collect(),
    }
}

fn link(entry: &mut ContractEntry, predecessor: &ContractEntry) {
    entry.predecessor = Some(ArtifactPredecessor {
        entry_id: predecessor.id.clone(),
        identity: predecessor.identity.clone(),
    });
}

/// `application` -> `client`, both valid.
fn valid_catalog() -> ContractCatalog {
    let application = entry(
        "application",
        ContractArtifactKind::ApplicationManifest,
        "src/*.rs",
        "generated/application.json",
    );
    let mut client = entry(
        "client",
        ContractArtifactKind::GeneratedClientTree,
        "generated/application.json",
        "ui/src/generated",
    );
    link(&mut client, &application);
    catalog([application, client])
}

fn catalog(entries: impl IntoIterator<Item = ContractEntry>) -> ContractCatalog {
    ContractCatalog {
        schema_version: CONTRACT_CATALOG_SCHEMA_VERSION,
        entries: entries
            .into_iter()
            .map(|entry| (entry.id.clone(), entry))
            .collect(),
    }
}

fn rejection(catalog: &ContractCatalog) -> ContractDiagnosticCode {
    catalog
        .canonical_bytes()
        .expect_err("catalog must be rejected")
        .code()
}

fn mutate(edit: impl FnOnce(&mut BTreeMap<String, ContractEntry>)) -> ContractCatalog {
    let mut catalog = valid_catalog();
    edit(&mut catalog.entries);
    catalog
}

fn application(entries: &mut BTreeMap<String, ContractEntry>) -> &mut ContractEntry {
    entries.get_mut("application").expect("application entry")
}

fn client(entries: &mut BTreeMap<String, ContractEntry>) -> &mut ContractEntry {
    entries.get_mut("client").expect("client entry")
}

struct TemporaryDirectory(tempfile::TempDir);

impl TemporaryDirectory {
    fn new() -> Self {
        Self(tempfile::tempdir().expect("temporary directory"))
    }

    fn path(&self) -> &Path {
        self.0.path()
    }
}

#[test]
fn valid_catalog_serializes_canonically() {
    let catalog = valid_catalog();
    let bytes = catalog.canonical_bytes().expect("valid catalog");
    assert_eq!(bytes, catalog.clone().canonical_bytes().expect("again"));
}

#[test]
fn catalog_rejects_duplicate_scopes_owners_and_outputs() {
    let duplicate_scope = mutate(|entries| {
        client(entries).scope = application(entries).scope.clone();
    });
    assert_eq!(
        rejection(&duplicate_scope),
        ContractDiagnosticCode::CatalogDuplicateScope
    );

    let duplicate_owner = mutate(|entries| {
        client(entries).owner = application(entries).owner.clone();
    });
    assert_eq!(
        rejection(&duplicate_owner),
        ContractDiagnosticCode::CatalogDuplicateOwner
    );

    let duplicate_output = mutate(|entries| {
        let output = application(entries).outputs.clone();
        client(entries).outputs = output;
    });
    assert_eq!(
        rejection(&duplicate_output),
        ContractDiagnosticCode::CatalogDuplicateOutput
    );
}

#[test]
fn catalog_rejects_escaping_absolute_and_unbounded_paths() {
    for (source, code) in [
        ("../outside.json", ContractDiagnosticCode::CatalogPath),
        ("/etc/passwd", ContractDiagnosticCode::CatalogPath),
        (
            "migrations/**/*.sql",
            ContractDiagnosticCode::CatalogUnboundedGlob,
        ),
        ("src/*/lib.rs", ContractDiagnosticCode::CatalogUnboundedGlob),
    ] {
        let catalog = mutate(|entries| {
            application(entries).provenance.sources = [source.to_string()].into_iter().collect();
        });
        assert_eq!(rejection(&catalog), code, "source `{source}`");
    }

    let unlimited_glob = mutate(|entries| {
        application(entries).provenance.glob_limit = None;
    });
    assert_eq!(
        rejection(&unlimited_glob),
        ContractDiagnosticCode::CatalogUnboundedGlob
    );

    let glob_output = mutate(|entries| {
        client(entries).outputs = BTreeMap::from([("client-artifact".into(), "ui/*".into())]);
    });
    assert_eq!(
        rejection(&glob_output),
        ContractDiagnosticCode::CatalogUnboundedGlob
    );
}

#[test]
fn catalog_rejects_broken_predecessor_chains() {
    let missing = mutate(|entries| {
        client(entries)
            .predecessor
            .as_mut()
            .expect("client predecessor")
            .entry_id = "missing".into();
    });
    assert_eq!(
        rejection(&missing),
        ContractDiagnosticCode::ChainMissingPredecessor
    );

    let kind_mismatch = mutate(|entries| {
        client(entries)
            .predecessor
            .as_mut()
            .expect("client predecessor")
            .identity
            .kind = ContractArtifactKind::DeploymentPlan;
    });
    assert_eq!(
        rejection(&kind_mismatch),
        ContractDiagnosticCode::ChainKindMismatch
    );

    let stale = mutate(|entries| {
        client(entries)
            .predecessor
            .as_mut()
            .expect("client predecessor")
            .identity
            .value = "ref:stale".into();
    });
    assert_eq!(
        rejection(&stale),
        ContractDiagnosticCode::ChainIdentityMismatch
    );

    let cycle = mutate(|entries| {
        let client_entry = client(entries).clone();
        link(application(entries), &client_entry);
    });
    assert_eq!(rejection(&cycle), ContractDiagnosticCode::ChainCycle);
}

#[test]
fn catalog_rejects_secret_like_and_machine_specific_identities() {
    for value in [
        "postgres://user:password@example.invalid/db",
        "/Users/someone/project",
        "2026-01-01T00:00:00.000Z",
    ] {
        let catalog = mutate(|entries| {
            application(entries).identity.value = value.into();
        });
        assert_eq!(
            rejection(&catalog),
            ContractDiagnosticCode::EnvironmentValue,
            "identity `{value}`"
        );
    }
}

#[test]
fn client_inventory_loads_the_e2e_ui_declarations() {
    let inventory = ClientInventory::from_path(
        repository_root().join("tests/e2e-ui/ui/distributed.clients.json"),
    )
    .expect("client inventory");
    assert_eq!(inventory.clients.len(), 3);
    assert!(inventory
        .clients
        .iter()
        .any(|client| client.surface == "e2e-ui-admin"));
}

#[test]
fn client_inventory_rejects_sparse_oversized_files_before_reading() {
    let root = TemporaryDirectory::new();
    let path = root.path().join("distributed.clients.json");
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&path)
        .expect("sparse file");
    file.set_len(1024 * 1024 + 1).expect("sparse file length");
    let error = ClientInventory::from_path(&path)
        .expect_err("sparse oversized inventory must fail before allocation");
    assert_eq!(error.code(), ContractDiagnosticCode::CatalogInputLimit);
}

#[test]
fn client_inventory_rejects_excessive_json_bytes_and_depth() {
    let oversized = format!("{{\"padding\":\"{}\"}}", "x".repeat(1024 * 1024));
    let error = ClientInventory::from_json_str(&oversized)
        .expect_err("oversized JSON must fail before parsing");
    assert_eq!(error.code(), ContractDiagnosticCode::CatalogInputLimit);

    let depth = 25;
    let nested = format!(
        "{{\"padding\":{}null{}}}",
        "[".repeat(depth),
        "]".repeat(depth)
    );
    let error = ClientInventory::from_json_str(&nested).expect_err("deeply nested JSON must fail");
    assert_eq!(error.code(), ContractDiagnosticCode::CatalogInputLimit);
}

#[cfg(unix)]
#[test]
fn client_inventory_loader_rejects_symlink_inputs() {
    let root = TemporaryDirectory::new();
    let inventory = root.path().join("distributed.clients.json");
    fs::write(&inventory, E2E_UI_CLIENTS).expect("inventory fixture");
    let link = root.path().join("inventory-link.json");
    symlink(&inventory, &link).expect("inventory symlink fixture");

    let error = ClientInventory::from_path(&link)
        .expect_err("inventory symlink inputs must be rejected before following them");
    assert_eq!(error.code(), ContractDiagnosticCode::CatalogSymlinkEscape);
    assert_eq!(error.message(), "client inventory must not be a symlink");
}

#[test]
fn client_inventory_validate_rejects_direct_secret_mutation() {
    let mut inventory = ClientInventory::from_json_str(E2E_UI_CLIENTS).expect("client inventory");
    inventory.clients[0]
        .documents
        .insert("src/routes/token=secret.graphql".to_string());
    let error = inventory
        .validate()
        .expect_err("validation must cover public mutations");
    assert_eq!(error.code(), ContractDiagnosticCode::EnvironmentValue);
}

#[test]
fn client_inventory_matches_schema_vectors() {
    let vectors: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/contracts/client-inventory-vectors.json"
    ))
    .expect("client inventory vectors JSON");
    for vector in vectors["vectors"].as_array().expect("vectors") {
        let name = vector["name"].as_str().expect("vector name");
        let expected = vector["valid"].as_bool().expect("vector result");
        let input = serde_json::to_string(&vector["inventory"]).expect("vector inventory JSON");
        assert_eq!(
            ClientInventory::from_json_str(&input).is_ok(),
            expected,
            "client inventory vector `{name}`"
        );
    }
}
