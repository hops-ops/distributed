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
    /// The offending value; `None` for a definition error.
    path: Option<String>,
    detail: String,
}

impl CommandInputError {
    fn at(path: &str, detail: impl Into<String>) -> Self {
        Self {
            path: Some(path.to_string()),
            detail: detail.into(),
        }
    }

    fn configuration(detail: impl Into<String>) -> Self {
        Self {
            path: None,
            detail: detail.into(),
        }
    }

    /// The error about a value of `subject` (`command input`, `command output`).
    fn describe(&self, subject: &str) -> String {
        match &self.path {
            Some(path) => format!("{subject} `{path}` {}", self.detail),
            None => format!("{subject} definition {}", self.detail),
        }
    }
}

impl std::fmt::Display for CommandInputError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.describe("command input"))
    }
}

impl std::error::Error for CommandInputError {}

/// Validated, recursively key-sorted command wire input and its stable digest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CanonicalCommandInput {
    wire: Value,
    /// The canonical encoding the digest was taken over.
    bytes: Vec<u8>,
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

    /// Overwrite every `now()` input default with the host clock, as canonical
    /// unix milliseconds.
    ///
    /// The digest keeps identifying the input the caller submitted: a retry of
    /// the same command id carries the same client preview value and must not
    /// look like a different command because the server clock moved. Every
    /// other consumer (decode, effects, the command message) sees the stamped
    /// value, so the caller's clock never reaches stored state.
    pub(crate) fn stamp_now(
        mut self,
        defaults: &[super::CommandInputDefault],
        now: Option<std::time::SystemTime>,
    ) -> Result<Self, CommandInputError> {
        let mut stamped = false;
        for default in defaults {
            if default.generator != super::InputDefaultGenerator::Now {
                continue;
            }
            let [field] = default.path.as_slice() else {
                return Err(CommandInputError::configuration(
                    "now() default must target one top-level field",
                ));
            };
            let now = now.ok_or_else(|| {
                CommandInputError::configuration("now() default requires the host clock")
            })?;
            let millis = now
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| CommandInputError::configuration("host clock is before 1970"))?
                .as_millis();
            let Value::Object(object) = &mut self.wire else {
                return Err(CommandInputError::at("$", "must be an object"));
            };
            object.insert(field.clone(), Value::String(millis.to_string()));
            stamped = true;
        }
        if stamped {
            self.bytes = serde_json::to_vec(&self.wire).map_err(|error| {
                CommandInputError::at("$", format!("cannot be canonically encoded: {error}"))
            })?;
        }
        Ok(self)
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
            bytes: self.bytes,
            digest: self.digest,
        })
    }
}

/// Private input envelope consumed by one exact typed route.
pub(crate) struct CanonicalTypedCommandInput<I> {
    decoded: I,
    wire: Value,
    bytes: Vec<u8>,
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

    /// The decoded input, its canonical wire value, that value's canonical
    /// encoding, and the digest of the encoding.
    pub(crate) fn into_parts(self) -> (I, Value, Vec<u8>, [u8; 32]) {
        (self.decoded, self.wire, self.bytes, self.digest)
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
    let digest: [u8; 32] = Sha256::digest(&bytes).into();
    Ok(CanonicalCommandInput {
        wire,
        bytes,
        digest,
    })
}

#[cfg(feature = "http")]
/// Check a command payload produced elsewhere (an aggregate cell) against
/// the command's declared output type, by the input rules: every required
/// field present, no undeclared field, every value of its declared type.
pub(crate) fn validate_command_output(
    definition: &CommandTypeDef,
    output: &Value,
) -> Result<(), String> {
    canonicalize_object(definition, output.clone(), "$")
        .map(|_| ())
        .map_err(|error| error.describe("command output"))
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
    if let Some(unsigned) = field.unsigned_integer {
        if field.type_name != "BigInt" || field.nested.is_some() {
            return Err(CommandInputError::at(
                path,
                "unsigned refinement requires BigInt",
            ));
        }
        return match value {
            Value::Number(value) if value.as_u64().is_some_and(|n| n <= unsigned.max_value()) => {
                Ok(Value::Number(value))
            }
            _ => Err(CommandInputError::at(
                path,
                "must be an unsigned integer in range",
            )),
        };
    }
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
            unsigned_integer: None,
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
    fn unsigned_inputs_preserve_full_rust_range_and_reject_invalid_values() {
        use crate::command::CommandUnsignedInteger;
        for unsigned in [
            CommandUnsignedInteger::U8,
            CommandUnsignedInteger::U16,
            CommandUnsignedInteger::U32,
            CommandUnsignedInteger::U64,
        ] {
            let mut value_field = field("revision", "BigInt", false, false, false, None);
            value_field.unsigned_integer = Some(unsigned);
            let definition = CommandTypeDef::new("RevisionInput", vec![value_field]);
            for value in [0, 1, unsigned.max_value()] {
                assert_eq!(
                    canonicalize_command_input(&definition, json!({"revision": value}))
                        .unwrap()
                        .wire()["revision"],
                    json!(value)
                );
            }
            let mut invalid = vec![
                json!(-1),
                json!(-0.0),
                json!(1.5),
                json!("1"),
                serde_json::from_str("18446744073709551616").unwrap(),
            ];
            if let Some(overflow) = unsigned.max_value().checked_add(1) {
                invalid.push(json!(overflow));
            }
            for value in invalid {
                assert!(
                    canonicalize_command_input(&definition, json!({"revision": value})).is_err()
                );
            }
        }
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

    fn scalar(type_name: &str) -> CommandTypeDef {
        CommandTypeDef::new(
            "ScalarInput",
            vec![field("value", type_name, false, false, false, None)],
        )
    }

    #[test]
    fn scalar_leaves_are_normalized_or_rejected_at_their_path() {
        struct Case {
            name: &'static str,
            type_name: &'static str,
            value: Value,
            expected: Result<Value, &'static str>,
        }
        let cases = [
            Case {
                name: "integer id becomes a string",
                type_name: "ID",
                value: json!(7),
                expected: Ok(json!("7")),
            },
            Case {
                name: "string id",
                type_name: "ID",
                value: json!("a"),
                expected: Ok(json!("a")),
            },
            Case {
                name: "fractional id",
                type_name: "ID",
                value: json!(1.5),
                expected: Err("must be a string or integer ID"),
            },
            Case {
                name: "string",
                type_name: "String",
                value: json!(1),
                expected: Err("must be a string"),
            },
            Case {
                name: "timestamp",
                type_name: "Timestamptz",
                value: json!("2024-02-29T23:59:60.5+05:30"),
                expected: Ok(json!("2024-02-29T23:59:60.5+05:30")),
            },
            Case {
                name: "timestamp, not rfc 3339",
                type_name: "Timestamptz",
                value: json!("2023-02-29T00:00:00Z"),
                expected: Err("must be an RFC 3339 timestamp"),
            },
            Case {
                name: "boolean",
                type_name: "Boolean",
                value: json!("true"),
                expected: Err("must be a boolean"),
            },
            Case {
                name: "big int",
                type_name: "BigInt",
                value: json!(u64::MAX),
                expected: Ok(json!(u64::MAX)),
            },
            Case {
                name: "big int from string",
                type_name: "BigInt",
                value: json!("1"),
                expected: Err("must be an integer"),
            },
            Case {
                name: "int from string",
                type_name: "Int",
                value: json!("1"),
                expected: Err("must be a 32-bit integer"),
            },
            Case {
                name: "int at the lower bound",
                type_name: "Int",
                value: json!(i32::MIN),
                expected: Ok(json!(i32::MIN)),
            },
            Case {
                name: "float",
                type_name: "Float",
                value: json!(2),
                expected: Ok(json!(2.0)),
            },
            Case {
                name: "float from string",
                type_name: "Float",
                value: json!("2"),
                expected: Err("must be a number"),
            },
            Case {
                name: "base64",
                type_name: "Bytea",
                value: json!("aGk="),
                expected: Ok(json!("aGk=")),
            },
            Case {
                name: "base64, not canonical",
                type_name: "Bytea",
                value: json!("aGk"),
                expected: Err("must be canonical base64"),
            },
            Case {
                name: "base64 from number",
                type_name: "Bytea",
                value: json!(1),
                expected: Err("must be a base64 string"),
            },
            Case {
                name: "json keeps any value",
                type_name: "JSON",
                value: json!([{"b": 1, "a": 2}]),
                expected: Ok(json!([{"a": 2, "b": 1}])),
            },
        ];
        for case in cases {
            let result =
                canonicalize_command_input(&scalar(case.type_name), json!({ "value": case.value }));
            match (result, case.expected) {
                (Ok(input), Ok(expected)) => {
                    assert_eq!(input.wire()["value"], expected, "{}", case.name);
                }
                (Err(error), Err(detail)) => assert_eq!(
                    error,
                    CommandInputError::at("$.value", detail),
                    "{}",
                    case.name
                ),
                (result, expected) => panic!("{}: got {result:?}, want {expected:?}", case.name),
            }
        }
    }

    #[test]
    fn malformed_definitions_and_shapes_are_rejected() {
        let mut unsigned_string = field("value", "String", false, false, false, None);
        unsigned_string.unsigned_integer = Some(crate::command::CommandUnsignedInteger::U8);
        let cases = [
            (
                "input is not an object",
                scalar("String"),
                json!([1]),
                CommandInputError::at("$", "must be an object"),
            ),
            (
                "nested input is not an object",
                definition(),
                json!({ "id": "one", "tags": [], "nested": 1 }),
                CommandInputError::at("$.nested", "must be an object"),
            ),
            (
                "list field given a scalar",
                definition(),
                json!({ "id": "one", "tags": "a", "nested": { "count": 1 } }),
                CommandInputError::at("$.tags", "must be a list"),
            ),
            (
                "repeated field",
                CommandTypeDef::new(
                    "Twice",
                    vec![
                        field("value", "String", false, false, false, None),
                        field("value", "String", false, false, false, None),
                    ],
                ),
                json!({ "value": "a" }),
                CommandInputError::configuration("`Twice` repeats field `value`"),
            ),
            (
                "unsupported scalar",
                scalar("Vector"),
                json!({ "value": 1 }),
                CommandInputError::configuration(
                    "`Vector` field `value` uses unsupported scalar `Vector`",
                ),
            ),
            (
                "unsigned refinement on a string",
                CommandTypeDef::new("Unsigned", vec![unsigned_string]),
                json!({ "value": 1 }),
                CommandInputError::at("$.value", "unsigned refinement requires BigInt"),
            ),
        ];
        for (name, definition, input, expected) in cases {
            assert_eq!(
                canonicalize_command_input(&definition, input).unwrap_err(),
                expected,
                "{name}"
            );
        }
    }

    #[test]
    fn nullable_list_items_are_kept_as_null() {
        let definition = CommandTypeDef::new(
            "Items",
            vec![field("items", "BigInt", false, true, true, None)],
        );
        let input = canonicalize_command_input(&definition, json!({ "items": [1, null] })).unwrap();
        assert_eq!(input.wire()["items"], json!([1, null]));
    }

    #[test]
    fn errors_name_the_value_path_or_the_definition() {
        assert_eq!(
            CommandInputError::at("$.id", "is required").to_string(),
            "command input `$.id` is required"
        );
        assert_eq!(
            CommandInputError::configuration("is broken").describe("command output"),
            "command output definition is broken"
        );
    }

    #[test]
    fn rfc3339_timestamps_are_checked_field_by_field() {
        let cases = [
            ("utc", "2026-01-31T12:30:45Z", true),
            ("lower-case separators", "2026-01-31t12:30:45z", true),
            ("fraction and offset", "2026-01-31T12:30:45.123-08:00", true),
            ("leap day in a leap year", "2000-02-29T00:00:00Z", true),
            ("leap second", "2026-06-30T23:59:60Z", true),
            ("leap day in a century year", "1900-02-29T00:00:00Z", false),
            ("day 31 of a 30-day month", "2026-04-31T00:00:00Z", false),
            ("day zero", "2026-01-00T00:00:00Z", false),
            ("month 13", "2026-13-01T00:00:00Z", false),
            ("hour 24", "2026-01-01T24:00:00Z", false),
            ("minute 60", "2026-01-01T00:60:00Z", false),
            ("second 61", "2026-01-01T00:00:61Z", false),
            ("space separator", "2026-01-01 00:00:00Z", false),
            ("no zone", "2026-01-01T00:00:00", false),
            ("empty fraction", "2026-01-01T00:00:00.Z", false),
            ("trailing text", "2026-01-01T00:00:00Zx", false),
            ("offset without colon", "2026-01-01T00:00:00+0000", false),
            ("offset hour 24", "2026-01-01T00:00:00+24:00", false),
            ("offset minute 60", "2026-01-01T00:00:00+00:60", false),
            ("non-digit year", "2O26-01-01T00:00:00Z", false),
        ];
        for (name, value, expected) in cases {
            assert_eq!(is_rfc3339_timestamp(value), expected, "{name}: {value}");
        }
    }

    fn default(
        generator: super::super::InputDefaultGenerator,
        path: &[&str],
    ) -> super::super::CommandInputDefault {
        super::super::CommandInputDefault {
            generator,
            path: path.iter().map(|segment| segment.to_string()).collect(),
        }
    }

    #[test]
    fn stamp_now_overwrites_the_client_value_and_keeps_the_submitted_digest() {
        use super::super::InputDefaultGenerator::{Now, Ulid};
        let definition = CommandTypeDef::new(
            "Stamped",
            vec![
                field("at", "String", false, false, false, None),
                field("id", "String", false, false, false, None),
            ],
        );
        let submitted =
            canonicalize_command_input(&definition, json!({ "at": "1", "id": "client" })).unwrap();
        let submitted_hash = submitted.hash();
        let clock = std::time::UNIX_EPOCH + std::time::Duration::from_millis(1_700_000_000_123);
        let stamped = submitted
            .stamp_now(
                &[default(Ulid, &["id"]), default(Now, &["at"])],
                Some(clock),
            )
            .unwrap();
        assert_eq!(
            stamped.wire(),
            &json!({ "at": "1700000000123", "id": "client" })
        );
        assert_eq!(
            stamped.hash(),
            submitted_hash,
            "the digest names the submitted input"
        );
        assert_eq!(stamped.bytes, serde_json::to_vec(stamped.wire()).unwrap());

        let unstamped =
            canonicalize_command_input(&definition, json!({ "at": "1", "id": "client" })).unwrap();
        let untouched = unstamped
            .clone()
            .stamp_now(&[default(Ulid, &["id"])], None)
            .unwrap();
        assert_eq!(untouched, unstamped, "no now() default needs no clock");
    }

    #[test]
    fn stamp_now_rejects_nested_targets_and_a_missing_clock() {
        use super::super::InputDefaultGenerator::Now;
        let input =
            || canonicalize_command_input(&scalar("String"), json!({ "value": "1" })).unwrap();
        let before_epoch = std::time::UNIX_EPOCH - std::time::Duration::from_secs(1);
        let cases = [
            (
                "nested path",
                default(Now, &["a", "b"]),
                Some(std::time::SystemTime::now()),
                "now() default must target one top-level field",
            ),
            (
                "no host clock",
                default(Now, &["value"]),
                None,
                "now() default requires the host clock",
            ),
            (
                "clock before 1970",
                default(Now, &["value"]),
                Some(before_epoch),
                "host clock is before 1970",
            ),
        ];
        for (name, default, clock, detail) in cases {
            assert_eq!(
                input().stamp_now(&[default], clock).unwrap_err(),
                CommandInputError::configuration(detail),
                "{name}"
            );
        }
    }
}
