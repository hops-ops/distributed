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

    // The JSON loader refuses credential-like strings before `validate`
    // runs; a value built in code reaches only `validate`.
    for (field, edit) in [
        (
            "module",
            (|client: &mut ClientDeclaration| client.module = "$distributed/token=abc".into())
                as fn(&mut ClientDeclaration),
        ),
        ("manifest entrypoint", |client| {
            client.manifest_entrypoint = Some("bearer abc".into())
        }),
    ] {
        let mut inventory =
            ClientInventory::from_json_str(E2E_UI_CLIENTS).expect("client inventory");
        edit(&mut inventory.clients[0]);
        let error = inventory
            .validate()
            .expect_err("credential-like value must be refused");
        assert_eq!(
            error.code(),
            ContractDiagnosticCode::EnvironmentValue,
            "{field}"
        );
    }
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

type CatalogEdit = fn(&mut ContractCatalog);

/// Each structural rule of a catalog, one row per rule, with the code it is
/// refused with.
#[test]
fn catalog_rejects_each_structural_rule_with_its_code() {
    let rows: [(&str, CatalogEdit, ContractDiagnosticCode); 20] = [
        (
            "unsupported schema version",
            |catalog| catalog.schema_version = CONTRACT_CATALOG_SCHEMA_VERSION + 1,
            ContractDiagnosticCode::CatalogInvalid,
        ),
        (
            "no entries",
            |catalog| catalog.entries.clear(),
            ContractDiagnosticCode::CatalogInvalid,
        ),
        (
            "more entries than the limit",
            |catalog| {
                *catalog = self::catalog((0..257).map(|index| {
                    let id = format!("entry-{index}");
                    entry(
                        &id,
                        ContractArtifactKind::ApplicationManifest,
                        "src/lib.rs",
                        &format!("generated/{id}.json"),
                    )
                }));
            },
            ContractDiagnosticCode::CatalogInputLimit,
        ),
        (
            "entry key that is not the entry's ID",
            |catalog| client(&mut catalog.entries).id = "other".into(),
            ContractDiagnosticCode::CatalogInvalid,
        ),
        (
            "scope ID with surrounding whitespace",
            |catalog| client(&mut catalog.entries).scope.id = " scope/client".into(),
            ContractDiagnosticCode::CatalogInvalid,
        ),
        (
            "owner ID carrying a credential",
            |catalog| client(&mut catalog.entries).owner = "owner/token=abc".into(),
            ContractDiagnosticCode::EnvironmentValue,
        ),
        (
            "owner ID with parent traversal",
            |catalog| client(&mut catalog.entries).owner = "owner/../client".into(),
            ContractDiagnosticCode::CatalogInputLimit,
        ),
        (
            "owner ID with unsupported characters",
            |catalog| client(&mut catalog.entries).owner = "owner client".into(),
            ContractDiagnosticCode::CatalogInvalid,
        ),
        (
            "identity of another kind than the entry",
            |catalog| {
                client(&mut catalog.entries).identity.kind = ContractArtifactKind::DeploymentPlan
            },
            ContractDiagnosticCode::ChainKindMismatch,
        ),
        (
            "identity with surrounding whitespace",
            |catalog| client(&mut catalog.entries).identity.value = "ref:client ".into(),
            ContractDiagnosticCode::CatalogInvalid,
        ),
        (
            "entry without sources",
            |catalog| client(&mut catalog.entries).provenance.sources.clear(),
            ContractDiagnosticCode::CatalogInvalid,
        ),
        (
            "generator that is a home-relative path",
            |catalog| client(&mut catalog.entries).provenance.generator = "~/bin/gen".into(),
            ContractDiagnosticCode::EnvironmentValue,
        ),
        (
            "glob limit of zero",
            |catalog| application(&mut catalog.entries).provenance.glob_limit = Some(0),
            ContractDiagnosticCode::CatalogInputLimit,
        ),
        (
            "glob limit above the maximum",
            |catalog| {
                application(&mut catalog.entries).provenance.glob_limit =
                    Some(MAX_CATALOG_GLOB_MATCHES + 1)
            },
            ContractDiagnosticCode::CatalogInputLimit,
        ),
        (
            "entry without outputs",
            |catalog| client(&mut catalog.entries).outputs.clear(),
            ContractDiagnosticCode::CatalogInvalid,
        ),
        (
            "more outputs than the limit",
            |catalog| {
                client(&mut catalog.entries).outputs = (0..257)
                    .map(|index| (format!("output-{index}"), format!("ui/out-{index}")))
                    .collect();
            },
            ContractDiagnosticCode::CatalogInputLimit,
        ),
        (
            "output path declared by two output IDs",
            |catalog| {
                let path =
                    application(&mut catalog.entries).outputs["application-artifact"].clone();
                client(&mut catalog.entries).outputs = BTreeMap::from([("other".into(), path)]);
            },
            ContractDiagnosticCode::CatalogDuplicateOutput,
        ),
        (
            "source path with a drive letter",
            |catalog| {
                client(&mut catalog.entries).provenance.sources =
                    ["C:/generated/application.json".to_string()].into();
            },
            ContractDiagnosticCode::CatalogPath,
        ),
        (
            "lifecycle policy with unsupported characters",
            |catalog| client(&mut catalog.entries).lifecycle = ["build now".to_string()].into(),
            ContractDiagnosticCode::CatalogInvalid,
        ),
        (
            "predecessor entry ID with surrounding whitespace",
            |catalog| {
                client(&mut catalog.entries)
                    .predecessor
                    .as_mut()
                    .expect("client predecessor")
                    .entry_id = "application ".into();
            },
            ContractDiagnosticCode::CatalogInvalid,
        ),
    ];
    for (name, edit, code) in rows {
        let mut catalog = valid_catalog();
        edit(&mut catalog);
        assert_eq!(rejection(&catalog), code, "{name}");
    }
}

/// A valid one-client inventory as JSON, for rows that edit the raw document.
fn inventory_json() -> Value {
    serde_json::json!({
        "schema_version": 1,
        "clients": [{
            "module": "$distributed/client",
            "surface": "e2e-ui",
            "documents": ["src/routes/example/+page.graphql"],
            "output": "src/lib/generated/example",
            "manifest_entrypoint": "crate::surface::client"
        }]
    })
}

type InventoryEdit = fn(&mut Value);

/// Each rule of the client inventory, one row per rule, with its code.
#[test]
fn client_inventory_rejects_each_rule_with_its_code() {
    let rows: [(&str, InventoryEdit, ContractDiagnosticCode); 18] = [
        (
            "unsupported schema version",
            |value| value["schema_version"] = 2.into(),
            ContractDiagnosticCode::CatalogInvalid,
        ),
        (
            "no clients",
            |value| value["clients"] = serde_json::json!([]),
            ContractDiagnosticCode::CatalogInputLimit,
        ),
        (
            "module outside $distributed",
            |value| value["clients"][0]["module"] = "virtual/client".into(),
            ContractDiagnosticCode::CatalogInvalid,
        ),
        (
            "module carrying a credential",
            |value| value["clients"][0]["module"] = "$distributed/token=abc".into(),
            ContractDiagnosticCode::EnvironmentValue,
        ),
        (
            "surface with unsupported characters",
            |value| value["clients"][0]["surface"] = "e2e ui".into(),
            ContractDiagnosticCode::CatalogInvalid,
        ),
        (
            "output with a glob",
            |value| value["clients"][0]["output"] = "src/lib/*".into(),
            ContractDiagnosticCode::CatalogUnboundedGlob,
        ),
        (
            "client without documents",
            |value| value["clients"][0]["documents"] = serde_json::json!([]),
            ContractDiagnosticCode::CatalogInputLimit,
        ),
        (
            "document that is not GraphQL",
            |value| value["clients"][0]["documents"][0] = "src/routes/page.ts".into(),
            ContractDiagnosticCode::CatalogInvalid,
        ),
        (
            "recursive document glob",
            |value| value["clients"][0]["documents"][0] = "src/**/page.graphql".into(),
            ContractDiagnosticCode::CatalogUnboundedGlob,
        ),
        (
            "document glob at the root",
            |value| value["clients"][0]["documents"][0] = "*.graphql".into(),
            ContractDiagnosticCode::CatalogUnboundedGlob,
        ),
        (
            "entrypoint that is not a Rust path",
            |value| value["clients"][0]["manifest_entrypoint"] = "crate::".into(),
            ContractDiagnosticCode::CatalogInvalid,
        ),
        (
            "entrypoint carrying a credential",
            |value| value["clients"][0]["manifest_entrypoint"] = "bearer abc".into(),
            ContractDiagnosticCode::EnvironmentValue,
        ),
        (
            "module declared twice",
            |value| {
                let mut second = value["clients"][0].clone();
                second["surface"] = "other".into();
                second["output"] = "src/lib/generated/other".into();
                value["clients"]
                    .as_array_mut()
                    .expect("clients")
                    .push(second);
            },
            ContractDiagnosticCode::CatalogDuplicateScope,
        ),
        (
            "surface declared twice",
            |value| {
                let mut second = value["clients"][0].clone();
                second["module"] = "$distributed/other".into();
                second["output"] = "src/lib/generated/other".into();
                value["clients"]
                    .as_array_mut()
                    .expect("clients")
                    .push(second);
            },
            ContractDiagnosticCode::CatalogDuplicateScope,
        ),
        (
            "output declared twice",
            |value| {
                let mut second = value["clients"][0].clone();
                second["module"] = "$distributed/other".into();
                second["surface"] = "other".into();
                value["clients"]
                    .as_array_mut()
                    .expect("clients")
                    .push(second);
            },
            ContractDiagnosticCode::CatalogDuplicateOutput,
        ),
        (
            "forbidden metadata field",
            |value| value["clients"][0]["env"] = "production".into(),
            ContractDiagnosticCode::EnvironmentValue,
        ),
        (
            "string longer than the limit",
            |value| value["clients"][0]["surface"] = "s".repeat(4097).into(),
            ContractDiagnosticCode::CatalogInputLimit,
        ),
        (
            "array longer than the limit",
            |value| value["clients"][0]["documents"] = serde_json::json!(vec![0; 8193]),
            ContractDiagnosticCode::CatalogInputLimit,
        ),
    ];
    let valid = serde_json::to_string(&inventory_json()).expect("inventory JSON");
    ClientInventory::from_json_str(&valid).expect("the unmodified inventory is valid");
    for (name, edit, code) in rows {
        let mut value = inventory_json();
        edit(&mut value);
        let input = serde_json::to_string(&value).expect("inventory JSON");
        let error = ClientInventory::from_json_str(&input)
            .err()
            .unwrap_or_else(|| panic!("{name}: inventory was accepted"));
        assert_eq!(error.code(), code, "{name}: {}", error.message());
    }

    let error = ClientInventory::from_json_str("{").expect_err("malformed JSON");
    assert_eq!(error.code(), ContractDiagnosticCode::CatalogInvalid);
}

/// What the file loader refuses before parsing: a missing path, a directory
/// and bytes that are not UTF-8.
#[test]
fn client_inventory_loader_refuses_missing_special_and_non_utf8_files() {
    let root = TemporaryDirectory::new();
    let missing =
        ClientInventory::from_path(root.path().join("absent.json")).expect_err("missing inventory");
    assert_eq!(missing.code(), ContractDiagnosticCode::CatalogPath);

    let directory = ClientInventory::from_path(root.path()).expect_err("directory inventory");
    assert_eq!(directory.code(), ContractDiagnosticCode::CatalogSpecialFile);

    let binary = root.path().join("binary.json");
    fs::write(&binary, [0xff, 0xfe, 0x00]).expect("binary fixture");
    let error = ClientInventory::from_path(&binary).expect_err("non-UTF-8 inventory");
    assert_eq!(error.code(), ContractDiagnosticCode::CatalogInvalid);
}

/// The code strings are a stable vocabulary: tools match on them.
#[test]
fn diagnostic_codes_render_their_stable_strings() {
    use ContractDiagnosticCode::*;
    for (code, text) in [
        (CatalogInvalid, "CTL-CATALOG-INVALID"),
        (CatalogInputLimit, "CTL-CATALOG-LIMIT"),
        (CatalogPath, "CTL-CATALOG-PATH"),
        (CatalogSymlinkEscape, "CTL-CATALOG-SYMLINK"),
        (CatalogSpecialFile, "CTL-CATALOG-FILE"),
        (CatalogDuplicateScope, "CTL-CATALOG-DUPLICATE-SCOPE"),
        (CatalogDuplicateOwner, "CTL-CATALOG-DUPLICATE-OWNER"),
        (CatalogDuplicateOutput, "CTL-CATALOG-DUPLICATE-OUTPUT"),
        (CatalogUnboundedGlob, "CTL-CATALOG-GLOB"),
        (EnvironmentValue, "CTL-ENV-VALUE"),
        (ChainMissingPredecessor, "CTL-CHAIN-MISSING"),
        (ChainKindMismatch, "CTL-CHAIN-KIND"),
        (ChainIdentityMismatch, "CTL-CHAIN-IDENTITY"),
        (ChainCycle, "CTL-CHAIN-CYCLE"),
    ] {
        assert_eq!(code.to_string(), text);
    }
    let error = ClientInventory::from_json_str("{").expect_err("malformed JSON");
    assert!(
        error
            .to_string()
            .starts_with("CTL-CATALOG-INVALID: parse client inventory JSON"),
        "{error}"
    );
}
