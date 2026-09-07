//! Canonical typed command-input validation.
//!
//! Every authenticated command host passes through this validator before
//! ledger reservation. Transport adapters must retain these normalization rules.
//! The retained wire value is the source of hashing and declaration-expression
//! resolution, so decoding the Rust input never becomes a second, potentially
//! differently named serialization path.

use std::collections::{BTreeMap, BTreeSet};

use base64::Engine as _;
use serde::de::DeserializeOwned;
use serde_json::{Number, Value};
use sha2::{Digest, Sha256};

use super::{CommandTypeDef, CommandTypeField};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CommandInputError {
    message: String,
}

impl CommandInputError {
    fn at(path: &str, message: impl Into<String>) -> Self {
        Self {
            message: format!("command input `{path}` {}", message.into()),
        }
    }

    fn configuration(message: impl Into<String>) -> Self {
        Self {
            message: format!("command input definition {}", message.into()),
        }
    }
}

impl std::fmt::Display for CommandInputError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for CommandInputError {}

/// Validated, recursively key-sorted command wire input and its stable digest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CanonicalCommandInput {
    wire: Value,
    digest: [u8; 32],
}

impl CanonicalCommandInput {
    #[cfg(test)]
    pub(crate) fn wire(&self) -> &Value {
        &self.wire
    }

    #[cfg(test)]
    pub(crate) fn hash(&self) -> String {
        format!("sha256:{}", hex_digest(&self.digest))
    }

    /// Decode the retained wire value exactly once into the route's registered
    /// Rust input type while keeping that same wire/hash for effects and ledger
    /// completion. This intentionally never serializes `I` back to JSON.
    pub(crate) fn decode<I: DeserializeOwned>(
        self,
    ) -> Result<CanonicalTypedCommandInput<I>, CommandInputError> {
        let decoded = serde_json::from_value(self.wire.clone()).map_err(|error| {
            CommandInputError::at(
                "$",
                format!("does not decode as the registered type: {error}"),
            )
        })?;
        Ok(CanonicalTypedCommandInput {
            decoded,
            wire: self.wire,
            digest: self.digest,
        })
    }
}

/// Private input envelope consumed by one exact typed route.
pub(crate) struct CanonicalTypedCommandInput<I> {
    decoded: I,
    wire: Value,
    digest: [u8; 32],
}

impl<I> CanonicalTypedCommandInput<I> {
    #[cfg(test)]
    pub(crate) fn decoded(&self) -> &I {
        &self.decoded
    }

    #[cfg(test)]
    pub(crate) fn wire(&self) -> &Value {
        &self.wire
    }

    #[cfg(test)]
    pub(crate) fn hash(&self) -> String {
        format!("sha256:{}", hex_digest(&self.digest))
    }

    pub(crate) fn into_parts(self) -> (I, Value, [u8; 32]) {
        (self.decoded, self.wire, self.digest)
    }
}

pub(crate) fn canonicalize_command_input(
    definition: &CommandTypeDef,
    input: Value,
) -> Result<CanonicalCommandInput, CommandInputError> {
    let wire = canonicalize_object(definition, input, "$")?;
    let bytes = serde_json::to_vec(&wire).map_err(|error| {
        CommandInputError::at("$", format!("cannot be canonically encoded: {error}"))
    })?;
    let digest: [u8; 32] = Sha256::digest(bytes).into();
    Ok(CanonicalCommandInput { wire, digest })
}

#[cfg(test)]
fn hex_digest(digest: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn canonicalize_object(
    definition: &CommandTypeDef,
    value: Value,
    path: &str,
) -> Result<Value, CommandInputError> {
    let Value::Object(mut object) = value else {
        return Err(CommandInputError::at(path, "must be an object"));
    };

    let mut fields = BTreeMap::new();
    for field in &definition.fields {
        if fields.insert(field.name.as_str(), field).is_some() {
            return Err(CommandInputError::configuration(format!(
                "`{}` repeats field `{}`",
                definition.name, field.name
            )));
        }
    }
    let declared = fields.keys().copied().collect::<BTreeSet<_>>();
    let mut unknown = object
        .keys()
        .filter(|name| !declared.contains(name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    unknown.sort();
    if let Some(name) = unknown.first() {
        return Err(CommandInputError::at(
            &field_path(path, name),
            "is not declared",
        ));
    }

    let mut canonical = serde_json::Map::new();
    for (name, field) in fields {
        let Some(value) = object.remove(name) else {
            if field.nullable {
                continue;
            }
            return Err(CommandInputError::at(
                &field_path(path, name),
                "is required",
            ));
        };
        canonical.insert(
            name.to_string(),
            canonicalize_field(field, value, &field_path(path, name))?,
        );
    }
    Ok(Value::Object(canonical))
}

fn canonicalize_field(
    field: &CommandTypeField,
    value: Value,
    path: &str,
) -> Result<Value, CommandInputError> {
    if value.is_null() {
        return if field.nullable {
            Ok(Value::Null)
        } else {
            Err(CommandInputError::at(path, "cannot be null"))
        };
    }

    if field.list {
        let Value::Array(values) = value else {
            return Err(CommandInputError::at(path, "must be a list"));
        };
        let mut canonical = Vec::with_capacity(values.len());
        for (index, value) in values.into_iter().enumerate() {
            let item_path = format!("{path}[{index}]");
            if value.is_null() {
                if !field.item_nullable {
                    return Err(CommandInputError::at(&item_path, "cannot be null"));
                }
                canonical.push(Value::Null);
            } else {
                canonical.push(canonicalize_leaf(field, value, &item_path)?);
            }
        }
        return Ok(Value::Array(canonical));
    }

    canonicalize_leaf(field, value, path)
}

fn canonicalize_leaf(
    field: &CommandTypeField,
    value: Value,
    path: &str,
) -> Result<Value, CommandInputError> {
    if let Some(nested) = field.nested.as_deref() {
        return canonicalize_object(nested, value, path);
    }

    match field.type_name.as_str() {
        "String" | "Timestamptz" => match value {
            Value::String(value) => {
                if field.type_name == "Timestamptz" && !is_rfc3339_timestamp(&value) {
                    Err(CommandInputError::at(path, "must be an RFC 3339 timestamp"))
                } else {
                    Ok(Value::String(value))
                }
            }
            _ => Err(CommandInputError::at(path, "must be a string")),
        },
        "ID" => match value {
            Value::String(value) => Ok(Value::String(value)),
            Value::Number(value) if value.is_i64() || value.is_u64() => {
                Ok(Value::String(value.to_string()))
            }
            _ => Err(CommandInputError::at(
                path,
                "must be a string or integer ID",
            )),
        },
        "Boolean" => match value {
            Value::Bool(value) => Ok(Value::Bool(value)),
            _ => Err(CommandInputError::at(path, "must be a boolean")),
        },
        "BigInt" => match value {
            Value::Number(value) if value.is_i64() || value.is_u64() => Ok(Value::Number(value)),
            _ => Err(CommandInputError::at(path, "must be an integer")),
        },
        "Int" => match value {
            Value::Number(value)
                if value
                    .as_i64()
                    .is_some_and(|value| i32::try_from(value).is_ok())
                    || value
                        .as_u64()
                        .is_some_and(|value| i32::try_from(value).is_ok()) =>
            {
                Ok(Value::Number(value))
            }
            _ => Err(CommandInputError::at(path, "must be a 32-bit integer")),
        },
        "Float" => match value {
            Value::Number(value) => {
                let finite = value
                    .as_f64()
                    .and_then(Number::from_f64)
                    .ok_or_else(|| CommandInputError::at(path, "must be a finite number"))?;
                Ok(Value::Number(finite))
            }
            _ => Err(CommandInputError::at(path, "must be a number")),
        },
        "Bytea" => match value {
            Value::String(value) => {
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(value)
                    .map_err(|_| CommandInputError::at(path, "must be canonical base64"))?;
                Ok(Value::String(
                    base64::engine::general_purpose::STANDARD.encode(decoded),
                ))
            }
            _ => Err(CommandInputError::at(path, "must be a base64 string")),
        },
        "JSON" => Ok(canonical_json(value)),
        scalar => Err(CommandInputError::configuration(format!(
            "`{}` field `{}` uses unsupported scalar `{scalar}`",
            field.type_name, field.name
        ))),
    }
}

fn canonical_json(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(canonical_json).collect()),
        Value::Object(values) => {
            let mut values = values.into_iter().collect::<Vec<_>>();
            values.sort_by(|(left, _), (right, _)| left.cmp(right));
            Value::Object(
                values
                    .into_iter()
                    .map(|(key, value)| (key, canonical_json(value)))
                    .collect(),
            )
        }
        scalar => scalar,
    }
}

fn field_path(parent: &str, field: &str) -> String {
    if parent == "$" {
        format!("$.{field}")
    } else {
        format!("{parent}.{field}")
    }
}

fn is_rfc3339_timestamp(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() < 20
        || bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || !matches!(bytes.get(10), Some(b'T' | b't'))
        || bytes.get(13) != Some(&b':')
        || bytes.get(16) != Some(&b':')
    {
        return false;
    }
    let digits = |range: std::ops::Range<usize>| -> Option<u32> {
        std::str::from_utf8(bytes.get(range)?).ok()?.parse().ok()
    };
    let (Some(year), Some(month), Some(day), Some(hour), Some(minute), Some(second)) = (
        digits(0..4),
        digits(5..7),
        digits(8..10),
        digits(11..13),
        digits(14..16),
        digits(17..19),
    ) else {
        return false;
    };
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return false,
    };
    if day == 0 || day > max_day || hour > 23 || minute > 59 || second > 60 {
        return false;
    }

    let mut cursor = 19;
    if bytes.get(cursor) == Some(&b'.') {
        cursor += 1;
        let fraction_start = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
        }
        if cursor == fraction_start {
            return false;
        }
    }
    match bytes.get(cursor) {
        Some(b'Z' | b'z') => cursor + 1 == bytes.len(),
        Some(b'+' | b'-') => {
            if cursor + 6 != bytes.len() || bytes.get(cursor + 3) != Some(&b':') {
                return false;
            }
            let hour = std::str::from_utf8(&bytes[cursor + 1..cursor + 3])
                .ok()
                .and_then(|value| value.parse::<u32>().ok());
            let minute = std::str::from_utf8(&bytes[cursor + 4..cursor + 6])
                .ok()
                .and_then(|value| value.parse::<u32>().ok());
            matches!((hour, minute), (Some(0..=23), Some(0..=59)))
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde_json::json;

    fn field(
        name: &str,
        type_name: &str,
        nullable: bool,
        list: bool,
        item_nullable: bool,
        nested: Option<CommandTypeDef>,
    ) -> CommandTypeField {
        CommandTypeField {
            name: name.into(),
            type_name: type_name.into(),
            nullable,
            list,
            item_nullable,
            nested: nested.map(Box::new),
        }
    }

    fn definition() -> CommandTypeDef {
        CommandTypeDef::new(
            "Input",
            vec![
                field("id", "String", false, false, false, None),
                field("note", "String", true, false, false, None),
                field("tags", "String", false, true, false, None),
                field(
                    "nested",
                    "NestedInput",
                    false,
                    false,
                    false,
                    Some(CommandTypeDef::new(
                        "NestedInput",
                        vec![field("count", "BigInt", false, false, false, None)],
                    )),
                ),
                field("document", "JSON", true, false, false, None),
            ],
        )
    }

    #[test]
    fn key_order_is_canonical_but_lists_are_preserved() {
        let left = canonicalize_command_input(
            &definition(),
            json!({
                "tags": ["b", "a"],
                "nested": { "count": 2 },
                "id": "one",
                "document": { "z": 1, "a": { "y": 2, "x": 1 } }
            }),
        )
        .unwrap();
        let right = canonicalize_command_input(
            &definition(),
            json!({
                "document": { "a": { "x": 1, "y": 2 }, "z": 1 },
                "id": "one",
                "nested": { "count": 2 },
                "tags": ["b", "a"]
            }),
        )
        .unwrap();
        assert_eq!(left, right);
        assert_eq!(left.wire()["tags"], json!(["b", "a"]));
    }

    #[test]
    fn absent_nullable_and_explicit_null_remain_distinct() {
        let absent = canonicalize_command_input(
            &definition(),
            json!({ "id": "one", "tags": [], "nested": { "count": 1 } }),
        )
        .unwrap();
        let null = canonicalize_command_input(
            &definition(),
            json!({ "id": "one", "note": null, "tags": [], "nested": { "count": 1 } }),
        )
        .unwrap();
        assert_ne!(absent.hash(), null.hash());
        assert!(absent.wire().get("note").is_none());
        assert!(null.wire()["note"].is_null());
    }

    #[test]
    fn rejects_unknown_missing_null_list_and_scalar_violations() {
        for (input, needle) in [
            (
                json!({ "id": "one", "extra": 1, "tags": [], "nested": { "count": 1 } }),
                "$.extra` is not declared",
            ),
            (
                json!({ "tags": [], "nested": { "count": 1 } }),
                "$.id` is required",
            ),
            (
                json!({ "id": null, "tags": [], "nested": { "count": 1 } }),
                "$.id` cannot be null",
            ),
            (
                json!({ "id": "one", "tags": [null], "nested": { "count": 1 } }),
                "$.tags[0]` cannot be null",
            ),
            (
                json!({ "id": "one", "tags": [], "nested": { "count": 1.5 } }),
                "$.nested.count` must be an integer",
            ),
        ] {
            let error = canonicalize_command_input(&definition(), input).unwrap_err();
            assert!(error.to_string().contains(needle), "{error}");
        }
    }

    #[test]
    fn graphql_int_is_limited_to_the_signed_32_bit_range() {
        let definition = CommandTypeDef::new(
            "IntInput",
            vec![field("value", "Int", false, false, false, None)],
        );
        canonicalize_command_input(&definition, json!({ "value": i32::MAX })).unwrap();
        let error =
            canonicalize_command_input(&definition, json!({ "value": i64::from(i32::MAX) + 1 }))
                .unwrap_err();
        assert!(error.to_string().contains("must be a 32-bit integer"));
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct RenamedInput {
        #[serde(rename = "wireId")]
        id: String,
    }

    #[test]
    fn decoding_retains_the_original_wire_instead_of_reserializing_rust() {
        let definition = CommandTypeDef::new(
            "RenamedInput",
            vec![field("wireId", "String", false, false, false, None)],
        );
        let typed = canonicalize_command_input(&definition, json!({ "wireId": "one" }))
            .unwrap()
            .decode::<RenamedInput>()
            .unwrap();
        assert_eq!(typed.decoded(), &RenamedInput { id: "one".into() });
        assert_eq!(typed.wire(), &json!({ "wireId": "one" }));
        assert_eq!(typed.hash().len(), "sha256:".len() + 64);
    }
}
