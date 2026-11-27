use std::collections::BTreeSet;

#[cfg(any(feature = "graphql", test))]
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
#[cfg(any(feature = "graphql", test))]
use base64::Engine as _;

use super::ProjectionScopeCodecError;
use crate::projection_protocol::MAX_PROJECTION_MODEL_NAME_BYTES;
use crate::table::{ColumnType, RowValue, TableColumn, TableKind, TableSchema};

pub(super) fn validate_registration_name(model: &str) -> Result<(), ProjectionScopeCodecError> {
    if model.trim().is_empty() {
        return Err(ProjectionScopeCodecError::BlankModelRegistration);
    }
    if model.len() > MAX_PROJECTION_MODEL_NAME_BYTES {
        return Err(ProjectionScopeCodecError::InvalidModelRegistration {
            model: model.to_string(),
            reason: format!(
                "name is {} bytes, exceeding the maximum of {}",
                model.len(),
                MAX_PROJECTION_MODEL_NAME_BYTES
            ),
        });
    }
    if model
        .chars()
        .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(ProjectionScopeCodecError::InvalidModelRegistration {
            model: model.to_string(),
            reason: "name contains whitespace or a control character".into(),
        });
    }
    Ok(())
}

pub(super) fn validate_model_schema(schema: &TableSchema) -> Result<(), ProjectionScopeCodecError> {
    schema.validate().map_err(
        |error| ProjectionScopeCodecError::InvalidModelRegistration {
            model: schema.model_name.clone(),
            reason: error.to_string(),
        },
    )?;
    if !matches!(schema.kind, TableKind::ReadModel) {
        return Err(ProjectionScopeCodecError::InvalidModelRegistration {
            model: schema.model_name.clone(),
            reason: "projection models must be read models".into(),
        });
    }

    let mut primary_key_columns = BTreeSet::new();
    let mut primary_key_fields = BTreeSet::new();
    for column_name in &schema.primary_key.columns {
        if !primary_key_columns.insert(column_name.as_str()) {
            return Err(ProjectionScopeCodecError::InvalidModelRegistration {
                model: schema.model_name.clone(),
                reason: format!("primary key repeats column `{column_name}`"),
            });
        }
        let column = key_column(schema, column_name).ok_or_else(|| {
            ProjectionScopeCodecError::InvalidModelRegistration {
                model: schema.model_name.clone(),
                reason: format!("primary key references missing column `{column_name}`"),
            }
        })?;
        if !column.primary_key {
            return Err(ProjectionScopeCodecError::InvalidModelRegistration {
                model: schema.model_name.clone(),
                reason: format!(
                    "primary-key list contains column `{column_name}` but the column is not marked primary-key"
                ),
            });
        }
        if column.field_name.trim().is_empty() {
            return Err(ProjectionScopeCodecError::InvalidModelRegistration {
                model: schema.model_name.clone(),
                reason: format!("primary-key column `{column_name}` has a blank field name"),
            });
        }
        if !primary_key_fields.insert(column.field_name.as_str()) {
            return Err(ProjectionScopeCodecError::InvalidModelRegistration {
                model: schema.model_name.clone(),
                reason: format!(
                    "primary key maps multiple columns to field `{}`",
                    column.field_name
                ),
            });
        }
    }
    if let Some(column) = schema.columns.iter().find(|column| {
        column.primary_key && !primary_key_columns.contains(column.column_name.as_str())
    }) {
        return Err(ProjectionScopeCodecError::InvalidModelRegistration {
            model: schema.model_name.clone(),
            reason: format!(
                "column `{}` is marked primary-key but absent from the primary-key list",
                column.column_name
            ),
        });
    }
    Ok(())
}

pub(super) fn key_column<'a>(
    schema: &'a TableSchema,
    column_name: &str,
) -> Option<&'a TableColumn> {
    schema
        .columns
        .iter()
        .find(|column| column.column_name == column_name)
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum TypedKeyValue {
    Text(String),
    Boolean(bool),
    Integer(i64),
    UnsignedInteger(u64),
    Float(f64),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
    Timestamp(String),
}

#[cfg(any(feature = "graphql", test))]
pub(super) fn typed_value_from_json(
    schema: &TableSchema,
    column: &TableColumn,
    value: &serde_json::Value,
) -> Result<TypedKeyValue, ProjectionScopeCodecError> {
    if value.is_null() {
        return Err(ProjectionScopeCodecError::NullPrimaryKey {
            model: schema.model_name.clone(),
            field: column.field_name.clone(),
        });
    }

    let wrong_shape = |expected| ProjectionScopeCodecError::WrongJsonShape {
        model: schema.model_name.clone(),
        field: column.field_name.clone(),
        expected,
        actual: json_shape(value),
    };
    match &column.column_type {
        ColumnType::Text => value
            .as_str()
            .map(|value| TypedKeyValue::Text(value.to_string()))
            .ok_or_else(|| wrong_shape("a string")),
        ColumnType::Boolean => value
            .as_bool()
            .map(TypedKeyValue::Boolean)
            .ok_or_else(|| wrong_shape("a boolean")),
        ColumnType::Integer => value.as_i64().map(TypedKeyValue::Integer).ok_or_else(|| {
            ProjectionScopeCodecError::IntegerOutOfRange {
                model: schema.model_name.clone(),
                field: column.field_name.clone(),
                expected: "signed 64-bit integer",
            }
        }),
        ColumnType::UnsignedInteger => value
            .as_u64()
            .map(TypedKeyValue::UnsignedInteger)
            .ok_or_else(|| ProjectionScopeCodecError::IntegerOutOfRange {
                model: schema.model_name.clone(),
                field: column.field_name.clone(),
                expected: "unsigned 64-bit integer",
            }),
        ColumnType::Float => value
            .as_f64()
            .filter(|value| value.is_finite())
            .map(TypedKeyValue::Float)
            .ok_or_else(|| wrong_shape("a finite number")),
        ColumnType::Bytes => {
            let encoded = value
                .as_str()
                .ok_or_else(|| wrong_shape("a base64 string"))?;
            let decoded = BASE64_STANDARD.decode(encoded).map_err(|_| {
                ProjectionScopeCodecError::InvalidBytes {
                    model: schema.model_name.clone(),
                    field: column.field_name.clone(),
                }
            })?;
            if BASE64_STANDARD.encode(&decoded) != encoded {
                return Err(ProjectionScopeCodecError::InvalidBytes {
                    model: schema.model_name.clone(),
                    field: column.field_name.clone(),
                });
            }
            Ok(TypedKeyValue::Bytes(decoded))
        }
        ColumnType::Json => Ok(TypedKeyValue::Json(value.clone())),
        ColumnType::Timestamp => value
            .as_str()
            .map(|value| TypedKeyValue::Timestamp(value.to_string()))
            .ok_or_else(|| wrong_shape("a timestamp string")),
        ColumnType::Unsupported(type_name) => {
            Err(ProjectionScopeCodecError::InvalidModelRegistration {
                model: schema.model_name.clone(),
                reason: format!(
                    "primary-key field `{}` has unsupported type `{type_name}`",
                    column.field_name
                ),
            })
        }
    }
}

#[cfg(any(feature = "graphql", test))]
pub(super) fn row_value_from_graphql_json(
    schema: &TableSchema,
    column: &TableColumn,
    value: &serde_json::Value,
) -> Result<RowValue, ProjectionScopeCodecError> {
    if value.is_null() {
        return Err(ProjectionScopeCodecError::NullPrimaryKey {
            model: schema.model_name.clone(),
            field: column.field_name.clone(),
        });
    }

    let integer_out_of_range = |expected| ProjectionScopeCodecError::IntegerOutOfRange {
        model: schema.model_name.clone(),
        field: column.field_name.clone(),
        expected,
    };
    let typed = match &column.column_type {
        ColumnType::Integer => match value {
            serde_json::Value::String(value) => {
                let parsed = value
                    .parse::<i64>()
                    .map_err(|_| integer_out_of_range("signed 64-bit integer"))?;
                if parsed.to_string() != *value {
                    return Err(integer_out_of_range(
                        "canonical signed 64-bit integer string",
                    ));
                }
                TypedKeyValue::Integer(parsed)
            }
            _ => typed_value_from_json(schema, column, value)?,
        },
        ColumnType::UnsignedInteger => match value {
            serde_json::Value::String(value) => {
                let parsed = value
                    .parse::<u64>()
                    .map_err(|_| integer_out_of_range("unsigned 64-bit integer"))?;
                if parsed.to_string() != *value {
                    return Err(integer_out_of_range(
                        "canonical unsigned 64-bit integer string",
                    ));
                }
                TypedKeyValue::UnsignedInteger(parsed)
            }
            _ => typed_value_from_json(schema, column, value)?,
        },
        // SQLite's JSON1 extension exposes BOOLEAN-affinity columns as the
        // lossless integer values 0/1. Accept exactly those private evidence
        // representations in addition to native JSON booleans.
        ColumnType::Boolean => match value.as_i64() {
            Some(0) => TypedKeyValue::Boolean(false),
            Some(1) => TypedKeyValue::Boolean(true),
            _ => typed_value_from_json(schema, column, value)?,
        },
        _ => typed_value_from_json(schema, column, value)?,
    };

    Ok(match typed {
        TypedKeyValue::Text(value) | TypedKeyValue::Timestamp(value) => RowValue::String(value),
        TypedKeyValue::Boolean(value) => RowValue::Bool(value),
        TypedKeyValue::Integer(value) => RowValue::I64(value),
        TypedKeyValue::UnsignedInteger(value) => RowValue::U64(value),
        TypedKeyValue::Float(value) => RowValue::F64(value),
        TypedKeyValue::Bytes(value) => RowValue::Bytes(value),
        TypedKeyValue::Json(value) => RowValue::Json(value),
    })
}

pub(super) fn typed_value_from_row(
    schema: &TableSchema,
    column: &TableColumn,
    value: &RowValue,
) -> Result<TypedKeyValue, ProjectionScopeCodecError> {
    if matches!(
        value,
        RowValue::Null | RowValue::Json(serde_json::Value::Null)
    ) {
        return Err(ProjectionScopeCodecError::NullPrimaryKey {
            model: schema.model_name.clone(),
            field: column.field_name.clone(),
        });
    }

    let wrong_shape = |expected| ProjectionScopeCodecError::WrongRowValueShape {
        model: schema.model_name.clone(),
        column: column.column_name.clone(),
        expected,
        actual: row_value_shape(value),
    };
    match (&column.column_type, value) {
        (ColumnType::Text, RowValue::String(value)) => Ok(TypedKeyValue::Text(value.clone())),
        (ColumnType::Boolean, RowValue::Bool(value)) => Ok(TypedKeyValue::Boolean(*value)),
        (ColumnType::Integer, RowValue::I64(value)) => Ok(TypedKeyValue::Integer(*value)),
        (ColumnType::UnsignedInteger, RowValue::U64(value)) => {
            Ok(TypedKeyValue::UnsignedInteger(*value))
        }
        (ColumnType::Float, RowValue::F64(value)) if value.is_finite() => {
            Ok(TypedKeyValue::Float(*value))
        }
        (ColumnType::Float, RowValue::F64(_)) => Err(ProjectionScopeCodecError::NonFiniteFloat {
            model: schema.model_name.clone(),
            field: column.field_name.clone(),
        }),
        (ColumnType::Bytes, RowValue::Bytes(value)) => Ok(TypedKeyValue::Bytes(value.clone())),
        (ColumnType::Json, RowValue::Json(value)) => Ok(TypedKeyValue::Json(value.clone())),
        (ColumnType::Timestamp, RowValue::String(value)) => {
            Ok(TypedKeyValue::Timestamp(value.clone()))
        }
        (ColumnType::Unsupported(type_name), _) => {
            Err(ProjectionScopeCodecError::InvalidModelRegistration {
                model: schema.model_name.clone(),
                reason: format!(
                    "primary-key column `{}` has unsupported type `{type_name}`",
                    column.column_name
                ),
            })
        }
        (column_type, _) => Err(wrong_shape(row_value_expectation(column_type))),
    }
}

pub(super) fn encode_typed_key_value(
    encoder: &mut CanonicalEncoder,
    value: TypedKeyValue,
) -> Result<(), ProjectionScopeCodecError> {
    match value {
        TypedKeyValue::Text(value) => {
            encoder.push_tag(0)?;
            encoder.push_bytes(value.as_bytes())
        }
        TypedKeyValue::Boolean(value) => {
            encoder.push_tag(1)?;
            encoder.push_tag(u8::from(value))
        }
        TypedKeyValue::Integer(value) => {
            encoder.push_tag(2)?;
            encoder.push_raw(&value.to_be_bytes())
        }
        TypedKeyValue::UnsignedInteger(value) => {
            encoder.push_tag(3)?;
            encoder.push_raw(&value.to_be_bytes())
        }
        TypedKeyValue::Float(value) => {
            encoder.push_tag(4)?;
            let canonical = if value == 0.0 { 0.0 } else { value };
            encoder.push_raw(&canonical.to_bits().to_be_bytes())
        }
        TypedKeyValue::Bytes(value) => {
            encoder.push_tag(5)?;
            encoder.push_bytes(&value)
        }
        TypedKeyValue::Json(value) => {
            encoder.push_tag(6)?;
            encode_json(encoder, &value)
        }
        TypedKeyValue::Timestamp(value) => {
            encoder.push_tag(7)?;
            encoder.push_bytes(value.as_bytes())
        }
    }
}

pub(super) fn encode_json(
    encoder: &mut CanonicalEncoder,
    value: &serde_json::Value,
) -> Result<(), ProjectionScopeCodecError> {
    match value {
        serde_json::Value::Null => encoder.push_tag(0),
        serde_json::Value::Bool(false) => encoder.push_tag(1),
        serde_json::Value::Bool(true) => encoder.push_tag(2),
        serde_json::Value::Number(value) => {
            encoder.push_tag(3)?;
            encoder.push_bytes(value.to_string().as_bytes())
        }
        serde_json::Value::String(value) => {
            encoder.push_tag(4)?;
            encoder.push_bytes(value.as_bytes())
        }
        serde_json::Value::Array(values) => {
            encoder.push_tag(5)?;
            encoder.push_len(values.len())?;
            for value in values {
                encode_json(encoder, value)?;
            }
            Ok(())
        }
        serde_json::Value::Object(values) => {
            encoder.push_tag(6)?;
            encoder.push_len(values.len())?;
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            for (key, value) in entries {
                encoder.push_bytes(key.as_bytes())?;
                encode_json(encoder, value)?;
            }
            Ok(())
        }
    }
}

pub(super) struct CanonicalEncoder {
    target: &'static str,
    max: usize,
    bytes: Vec<u8>,
}

impl CanonicalEncoder {
    pub(super) fn new(
        target: &'static str,
        domain: &[u8],
        max: usize,
    ) -> Result<Self, ProjectionScopeCodecError> {
        let mut encoder = Self {
            target,
            max,
            bytes: Vec::with_capacity(domain.len() + 32),
        };
        encoder.push_raw(domain)?;
        Ok(encoder)
    }

    pub(super) fn push_tag(&mut self, tag: u8) -> Result<(), ProjectionScopeCodecError> {
        self.push_raw(&[tag])
    }

    pub(super) fn push_len(&mut self, len: usize) -> Result<(), ProjectionScopeCodecError> {
        self.push_raw(&(len as u64).to_be_bytes())
    }

    pub(super) fn push_bytes(&mut self, value: &[u8]) -> Result<(), ProjectionScopeCodecError> {
        self.push_len(value.len())?;
        self.push_raw(value)
    }

    pub(super) fn push_raw(&mut self, value: &[u8]) -> Result<(), ProjectionScopeCodecError> {
        if self.bytes.len().saturating_add(value.len()) > self.max {
            return Err(ProjectionScopeCodecError::CanonicalEncodingTooLong {
                target: self.target,
                max: self.max,
            });
        }
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    pub(super) fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

#[cfg(any(feature = "graphql", test))]
fn json_shape(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

fn row_value_shape(value: &RowValue) -> &'static str {
    match value {
        RowValue::Null => "null",
        RowValue::Bool(_) => "boolean",
        RowValue::I64(_) => "signed integer",
        RowValue::U64(_) => "unsigned integer",
        RowValue::F64(_) => "float",
        RowValue::String(_) => "string",
        RowValue::Bytes(_) => "bytes",
        RowValue::Json(_) => "json",
    }
}

fn row_value_expectation(column_type: &ColumnType) -> &'static str {
    match column_type {
        ColumnType::Text | ColumnType::Timestamp => "a string RowValue",
        ColumnType::Boolean => "a boolean RowValue",
        ColumnType::Integer => "a signed-integer RowValue",
        ColumnType::UnsignedInteger => "an unsigned-integer RowValue",
        ColumnType::Float => "a finite-float RowValue",
        ColumnType::Bytes => "a bytes RowValue",
        ColumnType::Json => "a JSON RowValue",
        ColumnType::Unsupported(_) => "a supported RowValue",
    }
}
