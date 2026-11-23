use super::graphql::{compile_document, CompiledOperation};
use super::manifest::ClientManifest;
use super::render::{render_operation_artifact_json, render_operation_module};
use super::tests::{manifest, parse_manifest, refresh_schema_fingerprint};
use super::{ClientDocument, ClientSurfaceIdentity};
use crate::golden::assert_golden;

#[test]
fn unique_key_runtime_bridge_artifact_is_byte_exact() {
    let mut value = manifest();
    value["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({
            "name": "ownerTitle", "scalar": "String", "codec": "string", "nullable": true
        }));
    value["models"][0]["filter_input"]["fields"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({
            "name": "ownerTitle", "operators": ["_eq"]
        }));
    for root in value["roots"].as_array_mut().unwrap() {
        if let Some(fields) = root
            .get_mut("filter")
            .and_then(|filter| filter.get_mut("fields"))
            .and_then(|fields| fields.as_array_mut())
        {
            fields.push(serde_json::json!({"name": "ownerTitle", "operators": ["_eq"]}));
        }
        if let Some(fields) = root
            .get_mut("order")
            .and_then(|order| order.get_mut("fields"))
            .and_then(|fields| fields.as_array_mut())
        {
            fields.push(serde_json::json!("ownerTitle"));
        }
    }
    value["models"][0]["relationships"][0]["key_mapping"] = serde_json::json!({
        "kind": "direct", "local": ["tenantId", "ownerTitle"], "remote": ["tenantId", "title"]
    });
    value["models"][0]["relationships"][0]["maintenance"] = serde_json::json!("local");
    refresh_schema_fingerprint(&mut value);
    let manifest = parse_manifest(value, &ClientSurfaceIdentity::role("user")).unwrap();
    let document = ClientDocument::new(
        "src/routes/unique-key/+page.graphql",
        "query UniqueKeyBridge @load @live { todos(limit: 25) { id title owner { id title } } }",
    );
    let operation = compile_document(&document, &manifest).unwrap();
    let artifact = format!(
        "{}\n",
        render_operation_artifact_json(&operation, &manifest).unwrap()
    );
    assert_golden("unique-key-bridge-operation.json", &artifact);
}

const RUNTIME_BRIDGE_QUERY: &str = r#"
  query RustRuntimeBridge($id: ID!, $tenantId: ID!) {
    todo(id: $id, tenantId: $tenantId) {
      id
      title
    }
  }
"#;

fn compile_runtime_bridge_operation() -> (ClientManifest, CompiledOperation) {
    let manifest = parse_manifest(manifest(), &ClientSurfaceIdentity::role("user"))
        .expect("runtime bridge manifest");
    let document = ClientDocument::new(
        "src/routes/runtime-bridge/+page.graphql",
        RUNTIME_BRIDGE_QUERY,
    );
    let operation =
        compile_document(&document, &manifest).expect("compile runtime bridge operation");
    (manifest, operation)
}

fn rust_runtime_bridge_artifact() -> String {
    let (manifest, operation) = compile_runtime_bridge_operation();
    let artifact = render_operation_artifact_json(&operation, &manifest)
        .expect("serialize runtime bridge operation artifact");
    format!("{artifact}\n")
}

#[test]
fn rust_emitted_runtime_bridge_artifact_is_byte_exact() {
    // The JavaScript runtime bridge reads this fixture.
    assert_golden(
        "runtime-bridge-operation.json",
        &rust_runtime_bridge_artifact(),
    );
}

#[test]
fn scalar_only_operation_module_is_byte_exact_and_needs_no_replica_value_import() {
    let (manifest, operation) = compile_runtime_bridge_operation();
    let module = render_operation_module(&operation, &manifest)
        .expect("render scalar-only operation module");
    // The strict TypeScript consumer check compiles this fixture.
    assert_golden("generated-scalar-operation.ts", &module);
    assert!(
        module.contains(
            "import type { ReplicaOperationArtifact } from '@hops-ops/distributed/replica';"
        ),
        "{module}"
    );
    assert!(!module.contains("ReplicaValue"), "{module}");
}
