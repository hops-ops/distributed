use std::collections::BTreeMap;

use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use super::*;

pub(crate) fn validate_exact_operation_hash(
    operation: &str,
    expected: &str,
    label: &str,
) -> Result<(), ClientCompileError> {
    validate_hash(expected, &format!("{label} operation hash"))?;
    let actual = hash_bytes(operation.as_bytes());
    if actual != expected {
        return Err(ClientCompileError::manifest(
            "client.manifest.operation_hash",
            format!("{label} operation hash mismatch: expected `{expected}`, computed `{actual}`"),
        ));
    }
    Ok(())
}

pub(crate) fn hash_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("sha256:{digest:x}")
}

pub(crate) fn validate_hash(value: &str, label: &str) -> Result<(), ClientCompileError> {
    if value.len() != 71
        || !value.starts_with("sha256:")
        || !value[7..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ClientCompileError::manifest(
            "client.manifest.hash",
            format!("{label} must be a lowercase sha256 fingerprint"),
        ));
    }
    Ok(())
}

pub(crate) fn validate_nonempty(value: &str, label: &str) -> Result<(), ClientCompileError> {
    if value.trim().is_empty() {
        return Err(ClientCompileError::manifest(
            "client.manifest.empty",
            format!("{label} must not be empty"),
        ));
    }
    Ok(())
}

pub(crate) fn canonical_json_value(value: JsonValue) -> JsonValue {
    match value {
        JsonValue::Array(values) => {
            JsonValue::Array(values.into_iter().map(canonical_json_value).collect())
        }
        JsonValue::Object(values) => {
            let sorted = values
                .into_iter()
                .map(|(key, value)| (key, canonical_json_value(value)))
                .collect::<BTreeMap<_, _>>();
            JsonValue::Object(sorted.into_iter().collect())
        }
        scalar => scalar,
    }
}

#[cfg(test)]
mod canonical_wire_tests {
    use super::super::{FilterExpr, LitValue, Operand};

    #[test]
    fn filter_expr_serialization_is_canonical_across_map_backends() {
        let mut value = serde_json::Map::new();
        value.insert("z".into(), serde_json::json!(2));
        value.insert("a".into(), serde_json::json!(1));
        let expression = FilterExpr::In {
            column: "metadata".into(),
            values: vec![Operand::Lit(LitValue::Json(serde_json::Value::Object(
                value,
            )))],
            negated: false,
        };

        assert_eq!(
            serde_json::to_string(&expression).unwrap(),
            r#"{"kind":"in","value":{"column":"metadata","negated":false,"values":[{"kind":"lit","value":{"kind":"json","value":{"a":1,"z":2}}}]}}"#
        );
    }
}
