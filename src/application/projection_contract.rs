//! Lossless sharing in application artifacts, independent of runtime program IR.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

use super::{ApplicationError, ApplicationResult, MAX_MANIFEST_JSON_BYTES};

const ENCODING: &str = "shared_projection_program_v1";

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SharedProgram {
    encoding: String,
    program: Value,
    operation_sets: Vec<Value>,
    body_schemas: Vec<String>,
}

fn invalid(reason: &str) -> ApplicationError {
    ApplicationError::InvalidSpec(format!("shared projection program: {reason}"))
}

fn json_len(value: &impl Serialize) -> ApplicationResult<usize> {
    // Stop counting before allocating an oversized encoding or expanding refs.
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.saturating_add(bytes.len());
            if self.0 > MAX_MANIFEST_JSON_BYTES {
                return Err(std::io::Error::other(
                    "projection program exceeds JSON byte budget",
                ));
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value).map_err(|error| invalid(&error.to_string()))?;
    Ok(counter.0)
}

pub(crate) fn compact_projection_program_contract(value: Value) -> ApplicationResult<Value> {
    let expanded_len = json_len(&value)?;
    let mut program = value.clone();
    let arms = program
        .get_mut("arms")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| invalid("missing arms"))?;
    let mut operations = BTreeMap::new();
    let mut schemas = BTreeMap::new();
    for arm in arms.iter() {
        let operation = arm
            .get("operations")
            .filter(|value| value.is_array())
            .ok_or_else(|| invalid("missing operations"))?;
        operations.insert(
            serde_json::to_string(&super::canonical_json(operation))?,
            operation.clone(),
        );
        let schema = arm
            .pointer("/selector/body_schema")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("missing selector body schema"))?;
        schemas.insert(schema.to_owned(), ());
    }
    let operation_sets: Vec<_> = operations.values().cloned().collect();
    let operation_indexes: BTreeMap<_, _> = operations
        .keys()
        .enumerate()
        .map(|(i, key)| (key.clone(), i))
        .collect();
    let body_schemas: Vec<_> = schemas.into_keys().collect();
    for arm in arms {
        let fields = arm
            .as_object_mut()
            .ok_or_else(|| invalid("arm must be an object"))?;
        let operation = fields.remove("operations").unwrap();
        let key = serde_json::to_string(&super::canonical_json(&operation))?;
        fields.insert(
            "operations_ref".into(),
            Value::from(operation_indexes[&key]),
        );
        let selector = fields
            .get_mut("selector")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| invalid("selector must be an object"))?;
        let schema = selector.remove("body_schema").unwrap();
        let index = body_schemas
            .binary_search(&schema.as_str().unwrap().to_owned())
            .unwrap();
        selector.insert("body_schema_ref".into(), Value::from(index));
    }
    let shared = serde_json::to_value(SharedProgram {
        encoding: ENCODING.into(),
        program,
        operation_sets,
        body_schemas,
    })?;
    // Single-arm/small programs keep their exact historical representation.
    if json_len(&shared).is_ok_and(|len| len < expanded_len) {
        Ok(shared)
    } else {
        Ok(value)
    }
}

/// Expand a modeled application-manifest program into its original program JSON.
///
/// Accepts existing expanded values and `shared_projection_program_v1`. Sharing
/// changes artifact storage only: selectors, arm IDs, operations, runtime IR and
/// program digests are unchanged. References and table order are canonical, and
/// expansion is bounded by the existing 1 MiB opaque-contract budget before any
/// repeated operation list or schema is cloned.
pub fn expand_projection_program_contract(value: &Value) -> ApplicationResult<Value> {
    json_len(value)?;
    if value.get("encoding").is_none() {
        return Ok(value.clone());
    }
    let shared: SharedProgram =
        serde_json::from_value(value.clone()).map_err(|error| invalid(&error.to_string()))?;
    if shared.encoding != ENCODING {
        return Err(invalid("unsupported encoding"));
    }
    if shared.operation_sets.iter().any(|value| !value.is_array()) {
        return Err(invalid("operation sets must be arrays"));
    }
    let arms = shared
        .program
        .get("arms")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("missing arms"))?;
    let mut expanded_len = json_len(&shared.program)? as i128;
    let mut refs = Vec::with_capacity(arms.len());
    for arm in arms {
        let operation_ref = arm
            .get("operations_ref")
            .and_then(Value::as_u64)
            .and_then(|index| usize::try_from(index).ok())
            .ok_or_else(|| invalid("invalid operations reference"))?;
        let schema_ref = arm
            .pointer("/selector/body_schema_ref")
            .and_then(Value::as_u64)
            .and_then(|index| usize::try_from(index).ok())
            .ok_or_else(|| invalid("invalid body schema reference"))?;
        let operation = shared
            .operation_sets
            .get(operation_ref)
            .ok_or_else(|| invalid("operations reference out of bounds"))?;
        let schema = shared
            .body_schemas
            .get(schema_ref)
            .ok_or_else(|| invalid("body schema reference out of bounds"))?;
        if arm.get("operations").is_some() || arm.pointer("/selector/body_schema").is_some() {
            return Err(invalid("reference and inline material cannot coexist"));
        }
        // Replacing each `*_ref` key removes four key bytes and its index.
        expanded_len += json_len(operation)? as i128 + json_len(schema)? as i128
            - json_len(&operation_ref)? as i128
            - json_len(&schema_ref)? as i128
            - 8;
        if expanded_len > MAX_MANIFEST_JSON_BYTES as i128 {
            return Err(invalid("expanded program exceeds JSON byte budget"));
        }
        refs.push((operation_ref, schema_ref));
    }
    let mut expanded = shared.program;
    for (arm, (operation_ref, schema_ref)) in expanded["arms"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .zip(refs)
    {
        let fields = arm
            .as_object_mut()
            .ok_or_else(|| invalid("arm must be an object"))?;
        fields.remove("operations_ref");
        fields.insert(
            "operations".into(),
            shared.operation_sets[operation_ref].clone(),
        );
        let selector = fields
            .get_mut("selector")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| invalid("selector must be an object"))?;
        selector.remove("body_schema_ref");
        selector.insert(
            "body_schema".into(),
            Value::from(shared.body_schemas[schema_ref].clone()),
        );
    }
    if compact_projection_program_contract(expanded.clone())? != *value {
        return Err(invalid("noncanonical tables or references"));
    }
    Ok(expanded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn history_program() -> Value {
        let operations = json!([{
            "operation_id": "save", "staging_ordinal": 0, "kind": "upsert",
            "model": "Repository", "storage": "repositories",
            "fields": (0..40).map(|index| json!({
                "name": format!("field_{index}"),
                "expression": {"kind": "body_path", "value_type": "string", "path": [format!("field_{index}")]}
            })).collect::<Vec<_>>()
        }]);
        json!({
            "ir_version": 1, "name": "repository", "version": 2,
            "source_snapshots": true,
            "arms": (0..32).map(|index| json!({
                "arm_id": format!("fact-{}", index % 16),
                "selector": {
                    "occurrence_version": 1, "event_name": format!("repository.fact_{}", index % 16),
                    "event_version": index / 16 + 1, "body_kind": "state",
                    "body_type_name": "RepositoryState", "body_version": index / 16 + 1,
                    "body_schema": format!("state:v{}:{}", index / 16 + 1, "fields;".repeat(100)),
                    "body_fingerprint": format!("sha256:{:064x}", index / 16 + 1),
                    "body_codec": "json", "body_codec_version": 1,
                },
                "operations": operations,
            })).collect::<Vec<_>>()
        })
    }

    #[test]
    fn shared_program_round_trip_preserves_all_current_and_retained_selectors() {
        let original = history_program();
        let compact = compact_projection_program_contract(original.clone()).unwrap();
        assert_eq!(compact["encoding"], ENCODING);
        assert_eq!(compact["operation_sets"].as_array().unwrap().len(), 1);
        assert_eq!(compact["body_schemas"].as_array().unwrap().len(), 2);
        assert_eq!(compact["program"]["arms"].as_array().unwrap().len(), 32);
        assert!(json_len(&compact).unwrap() * 4 < json_len(&original).unwrap());
        assert_eq!(
            expand_projection_program_contract(&compact).unwrap(),
            original
        );
        assert_eq!(
            compact_projection_program_contract(original.clone()).unwrap(),
            compact
        );
        assert_eq!(
            expand_projection_program_contract(&original).unwrap(),
            original
        );
    }

    #[test]
    fn single_arm_keeps_existing_expanded_encoding() {
        let mut original = history_program();
        original["arms"].as_array_mut().unwrap().truncate(1);
        assert_eq!(
            compact_projection_program_contract(original.clone()).unwrap(),
            original
        );
    }

    #[test]
    fn shared_programs_fit_manifest_cap_and_legacy_artifacts_still_round_trip() {
        use crate::application::{Application, ApplicationManifest, Module, ProjectionSpec};

        let original = history_program();
        let compact = compact_projection_program_contract(original.clone()).unwrap();
        let module = |index: usize, program: &Value| {
            let id = format!("history-{index:02}");
            let mut projection =
                ProjectionSpec::try_new(&id, Vec::<String>::new(), Vec::<String>::new()).unwrap();
            projection.modeled_programs = vec!["program-history".into()];
            projection.modeled = vec![
                json!({"program_id": "program-history", "output_models": [], "program": program}),
            ];
            // Recompute the ordinary ProjectionSpec fingerprint after setting
            // its opaque behavior material, without altering that material.
            let projection = projection.with_direct(false).unwrap();
            Module::new(id).projection(projection).build().unwrap()
        };
        let expanded_modules = (0..32)
            .map(|index| module(index, &original))
            .collect::<Vec<_>>();
        let error = Application::try_new("history-app", expanded_modules, []).unwrap_err();
        assert!(error
            .to_string()
            .contains("application manifest exceeds 4194304 bytes"));
        let modules = (0..32)
            .map(|index| module(index, &compact))
            .collect::<Vec<_>>();
        let application = Application::try_new("history-app", modules.clone(), []).unwrap();
        let bytes = application.manifest().canonical_bytes().unwrap();
        assert!(bytes.len() < crate::application::MAX_APPLICATION_MANIFEST_BYTES);
        assert_eq!(
            ApplicationManifest::from_canonical_bytes(&bytes).unwrap(),
            *application.manifest()
        );
        assert_eq!(
            Application::try_new("history-app", modules.into_iter().rev(), [])
                .unwrap()
                .manifest()
                .canonical_bytes()
                .unwrap(),
            bytes
        );
        for projection in &application.manifest().projections {
            assert_eq!(
                expand_projection_program_contract(&projection.modeled[0]["program"]).unwrap(),
                original
            );
        }

        let legacy = Application::try_new("legacy", [module(0, &original)], []).unwrap();
        let legacy_bytes = legacy.manifest().canonical_bytes().unwrap();
        assert_eq!(
            ApplicationManifest::from_canonical_bytes(&legacy_bytes)
                .unwrap()
                .canonical_bytes()
                .unwrap(),
            legacy_bytes
        );
    }

    #[test]
    fn shared_program_rejects_invalid_ambiguous_and_noncanonical_references() {
        let compact = compact_projection_program_contract(history_program()).unwrap();
        let mut variants = Vec::new();
        for bad_ref in [
            json!(-1),
            json!(0.5),
            json!("0"),
            json!(null),
            json!(999999),
        ] {
            let mut invalid = compact.clone();
            invalid["program"]["arms"][0]["operations_ref"] = bad_ref;
            variants.push(invalid);
        }
        let mut invalid = compact.clone();
        invalid["program"]["arms"][0]["selector"]["body_schema_ref"] = json!(999);
        variants.push(invalid);
        let mut invalid = compact.clone();
        invalid["program"]["arms"][0]["operations"] = json!([]);
        variants.push(invalid);
        let mut invalid = compact.clone();
        invalid["operation_sets"]
            .as_array_mut()
            .unwrap()
            .push(json!([]));
        variants.push(invalid);
        let mut invalid = compact.clone();
        invalid["body_schemas"].as_array_mut().unwrap().reverse();
        for arm in invalid["program"]["arms"].as_array_mut().unwrap() {
            arm["selector"]["body_schema_ref"] =
                json!(1 - arm["selector"]["body_schema_ref"].as_u64().unwrap());
        }
        variants.push(invalid);
        let mut invalid = compact.clone();
        invalid["encoding"] = json!("future_encoding");
        variants.push(invalid);
        let mut invalid = compact;
        invalid["extra"] = json!(true);
        variants.push(invalid);
        for invalid in variants {
            assert!(expand_projection_program_contract(&invalid).is_err());
        }
    }

    #[test]
    fn shared_program_rejects_expansion_beyond_existing_opaque_budget() {
        let mut compact = compact_projection_program_contract(history_program()).unwrap();
        compact["operation_sets"][0] = json!([{"fields": "x".repeat(40_000)}]);
        assert!(json_len(&compact).unwrap() < MAX_MANIFEST_JSON_BYTES);
        assert!(expand_projection_program_contract(&compact)
            .unwrap_err()
            .to_string()
            .contains("expanded program exceeds"));
    }
}
