use super::graphql::{compile_document, CompiledOperation};
use super::manifest::ClientManifest;
use super::render::{render_operation_artifact_json, render_operation_module};
use super::tests::manifest;
use super::{ClientDocument, ClientSurfaceSelector};

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
    super::manifest::refresh_schema_fingerprint(&mut value);
    let manifest = ClientManifest::parse(value, &ClientSurfaceSelector::role("user")).unwrap();
    let document = ClientDocument::new(
        "src/routes/unique-key/+page.graphql",
        "query UniqueKeyBridge @load @live { todos(limit: 25) { id title owner { id title } } }",
    );
    let operation = compile_document(&document, &manifest).unwrap();
    let artifact = format!(
        "{}\n",
        render_operation_artifact_json(&operation, &manifest).unwrap()
    );
    assert_eq!(
        artifact,
        include_str!("../../tests/fixtures/unique-key-bridge-operation.json")
    );
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
    let manifest = ClientManifest::parse(manifest(), &ClientSurfaceSelector::role("user"))
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
    assert_eq!(
        rust_runtime_bridge_artifact(),
        include_str!("../../tests/fixtures/runtime-bridge-operation.json"),
        "the JavaScript runtime bridge fixture must remain exact Rust compiler output"
    );
}

#[test]
fn scalar_only_operation_module_is_byte_exact_and_needs_no_replica_value_import() {
    let (manifest, operation) = compile_runtime_bridge_operation();
    let module = render_operation_module(&operation, &manifest)
        .expect("render scalar-only operation module");
    assert_eq!(
        module,
        include_str!("../../tests/fixtures/generated-scalar-operation.ts"),
        "the strict TypeScript scalar-only fixture must remain exact Rust compiler output"
    );
    assert!(
        module.contains(
            "import type { ReplicaOperationArtifact } from '@hops-ops/distributed/replica';"
        ),
        "{module}"
    );
    assert!(!module.contains("ReplicaValue"), "{module}");
}
