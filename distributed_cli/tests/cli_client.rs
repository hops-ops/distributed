//! Integration tests for `distributed client`: drive the real binary against a small
//! manifest-v2 project and verify generation, read-only drift checking,
//! authorization-surface selection, document discovery, and explicit `@load`
//! route registration.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const ROLE_MANIFEST: &str = r#"{
  "manifest_version": 2,
  "protocol_version": 1,
  "service_id": "todos",
  "surface": {
    "kind": "role",
    "name": "user"
  },
  "schema_fingerprint": "sha256:758a97e4f7e1e538e8be86d24abd3d50a8da2d5813d29abd7a04bfa092d05189",
  "protocol_fingerprint": "sha256:00fb342f3acb4dc1c1716a43cc3001c748d5f6c500ff831690d820e9e43e2782",
  "execution": {
    "max_depth": 8,
    "max_complexity": 500,
    "max_bool_width": 256,
    "max_in_list": 1000,
    "complexity": {
      "version": 1,
      "scalar": 1,
      "belongs_to": 2,
      "has_many": 10,
      "m2m": 12,
      "aggregate": 8,
      "list_root": 3,
      "by_pk": 1,
      "list_fanout": 5
    }
  },
  "capabilities": {
    "live_queries": false,
    "record_revisions": false,
    "tombstones": false,
    "causal_receipts": false,
    "live_resume": false,
    "query_fallback": "revalidate",
    "cache_scope": true,
    "confirmed_persistence": false
  },
  "scalar_codecs": [
    {
      "scalar": "BigInt",
      "codec": "json_number_precision_limited"
    },
    {
      "scalar": "Boolean",
      "codec": "boolean"
    },
    {
      "scalar": "Bytea",
      "codec": "base64"
    },
    {
      "scalar": "Float",
      "codec": "float64"
    },
    {
      "scalar": "ID",
      "codec": "string"
    },
    {
      "scalar": "Int",
      "codec": "int32"
    },
    {
      "scalar": "JSON",
      "codec": "json"
    },
    {
      "scalar": "String",
      "codec": "string"
    },
    {
      "scalar": "Timestamptz",
      "codec": "string_unvalidated_timestamp"
    }
  ],
  "models": [
    {
      "id": "Todo",
      "typename": "Todo",
      "source_table": "todos",
      "dependencies": [
        "todos"
      ],
      "normalization": {
        "kind": "normalized",
        "fields": [
          {
            "name": "id",
            "codec": "string"
          }
        ],
        "encoding": "canonical_json_tuple_v1"
      },
      "fields": [
        {
          "name": "id",
          "scalar": "ID",
          "codec": "string",
          "nullable": false
        },
        {
          "name": "title",
          "scalar": "String",
          "codec": "string",
          "nullable": false
        }
      ],
      "relationships": [],
      "filter_input": {
        "type_name": "todos_bool_exp",
        "fields": [
          {
            "name": "id",
            "operators": [
              "_eq",
              "_neq",
              "_gt",
              "_gte",
              "_lt",
              "_lte",
              "_in",
              "_nin",
              "_is_null"
            ]
          },
          {
            "name": "title",
            "operators": [
              "_eq",
              "_neq",
              "_gt",
              "_gte",
              "_lt",
              "_lte",
              "_in",
              "_nin",
              "_is_null",
              "_like",
              "_ilike"
            ]
          }
        ],
        "relationships": []
      },
      "row_policy": {
        "kind": "unrestricted"
      },
      "record_revisions": false,
      "tombstones": false
    }
  ],
  "roots": [
    {
      "id": "query:todos",
      "operation": "query",
      "name": "todos",
      "kind": "list",
      "model": "Todo",
      "arguments": [
        {
          "name": "where",
          "kind": "filter",
          "type_name": "todos_bool_exp",
          "nullable": true,
          "list": false
        },
        {
          "name": "order_by",
          "kind": "order",
          "type_name": "todos_order_by",
          "nullable": true,
          "list": true
        },
        {
          "name": "limit",
          "kind": "limit",
          "type_name": "Int",
          "nullable": true,
          "list": false,
          "codec": "int32"
        },
        {
          "name": "offset",
          "kind": "offset",
          "type_name": "Int",
          "nullable": true,
          "list": false,
          "codec": "int32"
        }
      ],
      "filter": {
        "fields": [
          {
            "name": "id",
            "operators": [
              "_eq",
              "_neq",
              "_gt",
              "_gte",
              "_lt",
              "_lte",
              "_in",
              "_nin",
              "_is_null"
            ]
          },
          {
            "name": "title",
            "operators": [
              "_eq",
              "_neq",
              "_gt",
              "_gte",
              "_lt",
              "_lte",
              "_in",
              "_nin",
              "_is_null",
              "_like",
              "_ilike"
            ]
          }
        ],
        "relationships": [],
        "row_policy": {
          "kind": "unrestricted"
        }
      },
      "order": {
        "fields": [
          "id",
          "title"
        ],
        "values": [
          "asc",
          "asc_nulls_first",
          "asc_nulls_last",
          "desc",
          "desc_nulls_first",
          "desc_nulls_last"
        ]
      },
      "pagination": {
        "kind": "offset",
        "default_limit": 100,
        "max_limit": 1000,
        "coverage": "window"
      },
      "aggregate": null,
      "dependencies": [
        "todos"
      ],
      "live": false
    }
  ],
  "commands": [],
  "protocol_operations": {
    "version": 1,
    "command_status": null
  },
  "projectors": [],
  "projection_programs": [],
  "projection_bindings": []
}"#;

const TODOS_QUERY: &str = r#"query Todos {
  todos {
    id
    title
  }
}
"#;

const LOAD_TODOS_QUERY: &str = r#"query Todos @load {
  todos {
    id
    title
  }
}
"#;

const SECOND_TODOS_QUERY: &str = r#"query SecondTodos {
  todos {
    title
  }
}
"#;

fn project_dir(name: &str) -> PathBuf {
    let project = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = fs::remove_dir_all(&project);
    fs::create_dir_all(&project).expect("create disposable client project");
    fs::write(project.join("client-manifest.json"), ROLE_MANIFEST).expect("write client manifest");
    project
}

fn write_document(project: &Path, relative: &str, source: &str) {
    let path = project.join(relative);
    fs::create_dir_all(path.parent().expect("document has a parent"))
        .expect("create GraphQL document parent");
    fs::write(path, source).expect("write GraphQL document");
}

fn distributed_client(project: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_distributed"))
        .arg("client")
        .args(args)
        .current_dir(project)
        .output()
        .expect("distributed should run")
}

fn generate(project: &Path, documents: &str, extra: &[&str]) -> Output {
    let mut args = vec![
        "--manifest",
        "client-manifest.json",
        "--role",
        "user",
        "--documents",
        documents,
        "--out",
        "generated",
    ];
    args.extend_from_slice(extra);
    distributed_client(project, &args)
}

fn assert_success(output: &Output, context: &str) {
    assert!(
        output.status.success(),
        "{context} failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_failure_contains(output: &Output, expected: &str, context: &str) {
    assert!(!output.status.success(), "{context} unexpectedly succeeded");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(expected),
        "{context} stderr did not contain {expected:?}:\n{stderr}"
    );
}

fn snapshot_tree(root: &Path) -> BTreeMap<String, Vec<u8>> {
    fn visit(root: &Path, directory: &Path, snapshot: &mut BTreeMap<String, Vec<u8>>) {
        let mut entries = fs::read_dir(directory)
            .unwrap_or_else(|error| panic!("read {}: {error}", directory.display()))
            .map(|entry| entry.expect("read directory entry"))
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let path = entry.path();
            let file_type = entry.file_type().expect("read entry type");
            if file_type.is_dir() {
                visit(root, &path, snapshot);
            } else if file_type.is_file() {
                let relative = path
                    .strip_prefix(root)
                    .expect("snapshot path remains below root")
                    .to_string_lossy()
                    .replace('\\', "/");
                snapshot.insert(relative, fs::read(&path).expect("read snapshot file"));
            } else {
                panic!("unexpected entry in test snapshot: {}", path.display());
            }
        }
    }

    let mut snapshot = BTreeMap::new();
    visit(root, root, &mut snapshot);
    snapshot
}

#[test]
fn literal_text_search_generates_a_typed_load_island() {
    let project = project_dir("client-literal-text");
    fs::write(
        project.join("client-manifest.json"),
        ROLE_MANIFEST
            .replace("\"_ilike\"", "\"_ilike\", \"_icontains\"")
            // Adding a filter changes the selected schema contract.
            .replace(
                "sha256:758a97e4f7e1e538e8be86d24abd3d50a8da2d5813d29abd7a04bfa092d05189",
                "sha256:9143345e62737d38ed28e997fd76f434b18ae6b9c794b2c06477d1879fcfc136",
            ),
    )
    .unwrap();
    write_document(
        &project,
        "queries/search.graphql",
        r#"
        query SearchTodos($q: String! = "") @load {
            todos(where: {title: {_icontains: $q}}, limit: 20) { id title }
        }
    "#,
    );
    assert_success(
        &generate(&project, "queries/*.graphql", &[]),
        "literal search generation",
    );
    assert_success(
        &generate(&project, "queries/*.graphql", &["--check"]),
        "literal search drift check",
    );
    let inventory = fs::read_to_string(project.join("generated/islands.json")).unwrap();
    assert!(inventory.contains("SearchTodos"));
    assert!(inventory.contains("\"load\": true"));
    assert!(inventory.contains("String!"));
    write_document(
        &project,
        "queries/search.graphql",
        r#"
        query SearchTodos { todos(where: {title: {_icontains: 1}}) { id } }
    "#,
    );
    assert!(!generate(&project, "queries/*.graphql", &[])
        .status
        .success());
}

#[test]
fn generate_then_check_accepts_the_exact_artifact_tree() {
    let project = project_dir("client-generate-check");
    write_document(&project, "queries/todos.graphql", TODOS_QUERY);

    let generated = generate(&project, "queries/*.graphql", &[]);
    assert_success(&generated, "initial client generation");
    let generated_stdout = String::from_utf8_lossy(&generated.stdout);
    assert!(
        generated_stdout.contains("Generated") && generated_stdout.contains("generated"),
        "stdout: {generated_stdout}"
    );
    for expected in [
        "commands.ts",
        "index.ts",
        "islands.json",
        "islands.ts",
        "manifest.json",
        "operations/todos.ts",
        "protocol.ts",
        "sveltekit.ts",
    ] {
        assert!(
            project.join("generated").join(expected).is_file(),
            "missing generated artifact {expected}"
        );
    }
    let operation = fs::read_to_string(project.join("generated/operations/todos.ts"))
        .expect("read generated operation");
    assert!(
        operation.contains("\"order\": {")
            && operation.contains("\"tieBreakers\": [")
            && operation.contains("\"field\": \"id\"")
            && operation.contains("\"filter\": {")
            && operation.contains("\"pagination\": {")
            && operation.contains("\"kind\": \"offset\"")
            && operation.contains("\"defaultLimit\": 100")
            && operation.contains("\"maxLimit\": 1000"),
        "list operations must encode filter, stable identity ordering, and bounded offset-window maintenance"
    );
    let commands =
        fs::read_to_string(project.join("generated/commands.ts")).expect("read command bindings");
    assert!(
        commands.contains("export const COMMAND_ARTIFACTS = [] as const;")
            && commands.contains("export const COMMANDS = {\n\n} as const;"),
        "a query-only surface must retain inspectable empty inventories"
    );
    assert!(
        !commands.contains("@hops-ops/distributed/replica")
            && !commands.contains("createCommands")
            && !commands.contains("COMMAND_STATUS"),
        "a query-only surface must not emit unusable command runtime imports or factories"
    );
    let sveltekit = fs::read_to_string(project.join("generated/sveltekit.ts"))
        .expect("read SvelteKit bindings");
    assert!(
        sveltekit.contains(
            "export const Todos = defineDistributedSvelteKitOperation(DistributedOperation_0);"
        ) && sveltekit.contains(
            "export type GeneratedCommands = Readonly<Record<never, never>>;"
        ),
        "the virtual-module target must expose an SSR-safe generated operation wrapper: {sveltekit}"
    );
    assert!(
        !sveltekit.contains("createGeneratedCommands"),
        "query-only surfaces must not invent a command runtime: {sveltekit}"
    );

    let checked = generate(&project, "queries/*.graphql", &["--check"]);
    assert_success(&checked, "client artifact check");
    assert!(
        String::from_utf8_lossy(&checked.stdout)
            .contains("Distributed client artifacts are current"),
        "stdout: {}",
        String::from_utf8_lossy(&checked.stdout)
    );
}

#[test]
fn exact_document_path_with_glob_metacharacters_generates_and_checks_portably() {
    let project = project_dir("client-exact-glob-metacharacters");
    let document_path = "src/routes/blob/[[gameId]]/+page.graphql";
    write_document(&project, document_path, TODOS_QUERY);

    let generated = generate(&project, document_path, &[]);
    assert_success(&generated, "exact metacharacter-path generation");
    let manifest: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(project.join("generated/manifest.json"))
            .expect("read generated manifest"),
    )
    .expect("parse generated manifest");
    let source_paths = manifest["operations"]
        .as_array()
        .expect("manifest operations")
        .iter()
        .map(|operation| {
            operation["source_path"]
                .as_str()
                .expect("operation source path")
        })
        .collect::<Vec<_>>();
    assert_eq!(source_paths, vec![document_path]);

    let checked = generate(&project, document_path, &["--check"]);
    assert_success(&checked, "exact metacharacter-path check");
    assert!(
        String::from_utf8_lossy(&checked.stdout)
            .contains("Distributed client artifacts are current"),
        "stdout: {}",
        String::from_utf8_lossy(&checked.stdout)
    );
}

#[test]
fn check_reports_tampering_without_rewriting_any_artifact() {
    let project = project_dir("client-check-tampered");
    write_document(&project, "queries/todos.graphql", TODOS_QUERY);
    assert_success(
        &generate(&project, "queries/*.graphql", &[]),
        "initial client generation",
    );

    let generated = project.join("generated");
    fs::write(generated.join("index.ts"), "user-owned tamper\n")
        .expect("tamper with generated artifact");
    let before = snapshot_tree(&generated);

    let checked = generate(&project, "queries/*.graphql", &["--check"]);
    assert_failure_contains(&checked, "changed index.ts", "tampered client check");
    assert_eq!(
        snapshot_tree(&generated),
        before,
        "--check must not repair or otherwise write the generated tree"
    );
    assert_eq!(
        fs::read_to_string(generated.join("index.ts")).unwrap(),
        "user-owned tamper\n"
    );
}

#[test]
fn selected_role_manifest_rejects_role_name_and_application_kind_mismatches() {
    let project = project_dir("client-surface-mismatch");
    write_document(&project, "queries/todos.graphql", TODOS_QUERY);

    let wrong_role = distributed_client(
        &project,
        &[
            "--manifest",
            "client-manifest.json",
            "--role",
            "admin",
            "--documents",
            "queries/*.graphql",
            "--out",
            "generated",
        ],
    );
    assert_failure_contains(
        &wrong_role,
        "client.manifest.surface_mismatch",
        "role-name mismatch",
    );

    let wrong_kind = distributed_client(
        &project,
        &[
            "--manifest",
            "client-manifest.json",
            "--surface",
            "web",
            "--documents",
            "queries/*.graphql",
            "--out",
            "generated",
        ],
    );
    assert_failure_contains(
        &wrong_kind,
        "client.manifest.surface_mismatch",
        "role/application selector mismatch",
    );
    assert!(
        !project.join("generated").exists(),
        "surface mismatch must fail before writing output"
    );
}

#[test]
fn unmatched_document_glob_fails_before_creating_output() {
    let project = project_dir("client-unmatched-glob");

    let output = generate(&project, "queries/**/*.graphql", &[]);
    assert_failure_contains(&output, "matched no files", "unmatched document glob");
    assert!(
        !project.join("generated").exists(),
        "an unmatched source glob must not create output"
    );
}

#[test]
fn component_load_intent_remains_framework_neutral_outside_route_conventions() {
    let project = project_dir("client-component-load");
    write_document(&project, "queries/todos.graphql", LOAD_TODOS_QUERY);

    let unplaced = generate(&project, "queries/*.graphql", &[]);
    assert_success(&unplaced, "component-owned load island generation");
    assert!(
        fs::read_to_string(project.join("generated/islands.json"))
            .expect("read generated island inventory")
            .contains("\"load\": true"),
        "unplaced component island must retain its load intent for the adapter"
    );

    assert!(
        !project.join("generated/routes.ts").exists(),
        "the framework-neutral compiler must not invent adapter route ownership"
    );
}

#[test]
fn check_rejects_an_unexpected_file_without_touching_the_tree() {
    let project = project_dir("client-check-unexpected");
    write_document(&project, "queries/todos.graphql", TODOS_QUERY);
    assert_success(
        &generate(&project, "queries/*.graphql", &[]),
        "initial client generation",
    );

    let generated = project.join("generated");
    fs::write(generated.join("stale.ts"), "keep me\n").expect("write unexpected artifact");
    let before = snapshot_tree(&generated);

    let checked = generate(&project, "queries/*.graphql", &["--check"]);
    assert_failure_contains(&checked, "unexpected stale.ts", "unexpected-file check");
    assert_eq!(
        snapshot_tree(&generated),
        before,
        "--check must remain read-only when the file set drifts"
    );

    let regenerated = generate(&project, "queries/*.graphql", &[]);
    assert_failure_contains(
        &regenerated,
        "files without current or previous compiler ownership",
        "unexpected-file regeneration",
    );
    assert_eq!(
        snapshot_tree(&generated),
        before,
        "normal generation must also fail before mutating an unowned file set"
    );
}

#[test]
fn regeneration_removes_only_modules_owned_by_previous_provenance() {
    let project = project_dir("client-regenerate-converges");
    write_document(&project, "queries/todos.graphql", TODOS_QUERY);
    write_document(&project, "queries/second.graphql", SECOND_TODOS_QUERY);
    assert_success(
        &generate(&project, "queries/*.graphql", &[]),
        "initial two-operation generation",
    );

    let generated = project.join("generated");
    let stale_module = generated.join("operations/second-todos.ts");
    assert!(
        stale_module.is_file(),
        "second operation should be generated"
    );
    fs::remove_file(project.join("queries/second.graphql")).expect("remove obsolete source");

    assert_success(
        &generate(&project, "queries/*.graphql", &[]),
        "regeneration after deleting an operation",
    );
    assert!(
        !stale_module.exists(),
        "a module proven by previous provenance must be removed when its operation disappears"
    );
    assert_success(
        &generate(&project, "queries/*.graphql", &["--check"]),
        "converged generation check",
    );
}

#[test]
fn regeneration_refuses_to_delete_a_stale_module_without_ownership_marker() {
    let project = project_dir("client-regenerate-marker");
    write_document(&project, "queries/todos.graphql", TODOS_QUERY);
    write_document(&project, "queries/second.graphql", SECOND_TODOS_QUERY);
    assert_success(
        &generate(&project, "queries/*.graphql", &[]),
        "initial two-operation generation",
    );

    let stale_module = project.join("generated/operations/second-todos.ts");
    fs::write(&stale_module, "user-owned contents\n").expect("replace ownership marker");
    fs::remove_file(project.join("queries/second.graphql")).expect("remove obsolete source");
    let before = snapshot_tree(&project.join("generated"));

    let regenerated = generate(&project, "queries/*.graphql", &[]);
    assert_failure_contains(
        &regenerated,
        "compiler ownership marker is missing",
        "unowned stale-module regeneration",
    );
    assert_eq!(
        snapshot_tree(&project.join("generated")),
        before,
        "failed ownership proof must not mutate generated output"
    );
}

#[cfg(unix)]
#[test]
fn generation_rejects_symlinked_output_components_without_writing_through_them() {
    use std::os::unix::fs::symlink;

    let project = project_dir("client-generate-symlink");
    write_document(&project, "queries/todos.graphql", TODOS_QUERY);
    let outside = project.join("outside");
    fs::create_dir_all(&outside).expect("create outside target");
    fs::create_dir_all(project.join("generated")).expect("create generated root");
    symlink(&outside, project.join("generated/operations")).expect("link operation directory");

    let generated = generate(&project, "queries/*.graphql", &[]);
    assert_failure_contains(
        &generated,
        "contains a symlink or incompatible entry",
        "symlinked output generation",
    );
    assert_eq!(
        snapshot_tree(&outside),
        BTreeMap::new(),
        "generation must not follow an output-directory symlink"
    );
}

#[test]
fn generation_rejects_an_unproven_old_surface_chunk_without_provenance() {
    let project = project_dir("client-generate-unproven-surface");
    write_document(&project, "queries/todos.graphql", TODOS_QUERY);
    let generated = project.join("generated");
    fs::create_dir_all(generated.join("operations")).expect("create old output tree");
    fs::write(
        generated.join("operations/admin-only.ts"),
        "/** GENERATED by distributed client. Do not edit. */\nexport const secret = true;\n",
    )
    .expect("write unproven elevated chunk");
    let before = snapshot_tree(&generated);

    let output = generate(&project, "queries/*.graphql", &[]);
    assert_failure_contains(
        &output,
        "files without current or previous compiler ownership",
        "unproven prior-surface output",
    );
    assert_eq!(
        snapshot_tree(&generated),
        before,
        "missing provenance must never authorize deletion or partial overwrite"
    );
}
