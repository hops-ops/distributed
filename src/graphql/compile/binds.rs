use async_graphql::Value;
use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::graphql::filter::{LitValue, Operand};
use crate::microsvc::Session;
use crate::table::ColumnType;

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum BindValue {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Text(String),
    Bytes(Vec<u8>),
    Json(JsonValue),
}

/// Two binds are equal exactly when the driver receives the same value: an
/// `F64` by its bits, a `Json` by the text it is bound as. Live sharing keys
/// on this, so equal binds must never be told apart by SQL.
impl PartialEq for BindValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::I64(a), Self::I64(b)) => a == b,
            (Self::F64(a), Self::F64(b)) => a.to_bits() == b.to_bits(),
            (Self::Text(a), Self::Text(b)) => a == b,
            (Self::Bytes(a), Self::Bytes(b)) => a == b,
            (Self::Json(a), Self::Json(b)) => bound_text(a) == bound_text(b),
            _ => false,
        }
    }
}

impl Eq for BindValue {}

/// The text a `Json` bind is sent as (see `apply_binds!`).
fn bound_text(value: &JsonValue) -> String {
    value.to_string()
}

impl std::hash::Hash for BindValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Null => {}
            Self::Bool(value) => value.hash(state),
            Self::I64(value) => value.hash(state),
            Self::F64(value) => value.to_bits().hash(state),
            Self::Text(value) => value.hash(state),
            Self::Bytes(value) => value.hash(state),
            Self::Json(value) => bound_text(value).hash(state),
        }
    }
}

/// Give a numeric column one bind type.
///
/// The Postgres driver prepares a statement once per connection and reuses
/// its parameter types for every later bind of the same SQL. Bound as an
/// integer where the statement was first prepared with a float (or the other
/// way round), a value's bytes are read as the other type and the comparison
/// silently goes wrong: `2.0 <> 2` comes back true.
fn for_column(bind: BindValue, column_type: &ColumnType) -> Result<BindValue, String> {
    match (column_type, bind) {
        (ColumnType::Float, BindValue::I64(value)) => Ok(BindValue::F64(value as f64)),
        (ColumnType::Integer | ColumnType::UnsignedInteger, BindValue::F64(value)) => {
            Err(format!("expected integer, got `{value}`"))
        }
        (_, bind) => Ok(bind),
    }
}

pub(super) fn operand_to_bind(
    op: &Operand,
    session: &Session,
    column_type: &ColumnType,
) -> Result<BindValue, String> {
    match op {
        Operand::Lit(lit) => for_column(lit_to_bind(lit)?, column_type),
        Operand::Claim(c) => {
            let raw = session
                .get(&c.header)
                .or_else(|| session.get(&c.header.to_ascii_lowercase()))
                .ok_or_else(|| format!("missing claim `{}`", c.header))?;
            parse_claim(raw, column_type)
        }
    }
}

fn lit_to_bind(lit: &LitValue) -> Result<BindValue, String> {
    Ok(match lit {
        LitValue::String(s) => BindValue::Text(s.clone()),
        LitValue::I64(i) => BindValue::I64(*i),
        LitValue::F64(f) => BindValue::F64(*f),
        LitValue::Bool(b) => BindValue::Bool(*b),
        LitValue::Json(j) => BindValue::Json(j.clone()),
        LitValue::Null => BindValue::Null,
    })
}

fn parse_claim(raw: &str, column_type: &ColumnType) -> Result<BindValue, String> {
    match column_type {
        ColumnType::Integer | ColumnType::UnsignedInteger => raw
            .parse::<i64>()
            .map(BindValue::I64)
            .map_err(|_| format!("claim value `{raw}` is not an integer")),
        ColumnType::Float => raw
            .parse::<f64>()
            .map(BindValue::F64)
            .map_err(|_| format!("claim value `{raw}` is not a float")),
        ColumnType::Boolean => match raw {
            "true" | "TRUE" | "1" => Ok(BindValue::Bool(true)),
            "false" | "FALSE" | "0" => Ok(BindValue::Bool(false)),
            _ => Err(format!("claim value `{raw}` is not a boolean")),
        },
        ColumnType::Json => Err("claims cannot compare to Json columns".into()),
        _ => Ok(BindValue::Text(raw.to_string())),
    }
}

pub(super) fn value_to_bind(v: &Value, column_type: &ColumnType) -> Result<BindValue, String> {
    match v {
        Value::Null => Ok(BindValue::Null),
        Value::Boolean(b) => Ok(BindValue::Bool(*b)),
        Value::Number(n) => {
            let bind = if let Some(i) = n.as_i64() {
                BindValue::I64(i)
            } else if let Some(f) = n.as_f64() {
                BindValue::F64(f)
            } else {
                return Err("number out of range".into());
            };
            for_column(bind, column_type)
        }
        Value::String(s) => match column_type {
            ColumnType::Bytes => {
                use base64::Engine as _;
                base64::engine::general_purpose::STANDARD
                    .decode(s.as_bytes())
                    .map(BindValue::Bytes)
                    .map_err(|e| format!("invalid base64: {e}"))
            }
            ColumnType::Integer | ColumnType::UnsignedInteger => s
                .parse::<i64>()
                .map(BindValue::I64)
                .map_err(|_| format!("expected integer, got `{s}`")),
            _ => Ok(BindValue::Text(s.clone())),
        },
        Value::List(_) | Value::Object(_) | Value::Enum(_) => {
            let json = value_to_json(v)?;
            Ok(BindValue::Json(json))
        }
        _ => Err("unsupported GraphQL value for bind".into()),
    }
}

fn value_to_json(v: &Value) -> Result<JsonValue, String> {
    serde_json::to_value(v).map_err(|e| e.to_string())
}

#[cfg(test)]
mod parse_claim_tests {
    use super::*;

    #[test]
    fn a_numeric_column_always_binds_one_type() {
        let session = Session::new();
        let literal = |value: LitValue, column: ColumnType| {
            operand_to_bind(&Operand::Lit(value), &session, &column)
        };
        assert!(matches!(
            literal(LitValue::I64(2), ColumnType::Float),
            Ok(BindValue::F64(value)) if value == 2.0
        ));
        assert!(matches!(
            value_to_bind(&Value::from(2), &ColumnType::Float),
            Ok(BindValue::F64(value)) if value == 2.0
        ));
        assert!(matches!(
            value_to_bind(&Value::from(2), &ColumnType::Integer),
            Ok(BindValue::I64(2))
        ));
        assert!(literal(LitValue::F64(2.5), ColumnType::Integer).is_err());
        assert!(value_to_bind(&Value::from(2.5), &ColumnType::UnsignedInteger).is_err());
    }

    #[test]
    fn integer_claim_ok_and_fail() {
        assert!(matches!(
            parse_claim("42", &ColumnType::Integer).unwrap(),
            BindValue::I64(42)
        ));
        assert!(parse_claim("nope", &ColumnType::Integer).is_err());
    }

    #[test]
    fn bool_claim_variants() {
        assert!(matches!(
            parse_claim("true", &ColumnType::Boolean).unwrap(),
            BindValue::Bool(true)
        ));
        assert!(matches!(
            parse_claim("0", &ColumnType::Boolean).unwrap(),
            BindValue::Bool(false)
        ));
        assert!(parse_claim("maybe", &ColumnType::Boolean).is_err());
    }

    #[test]
    fn json_claim_rejected() {
        assert!(parse_claim("{}", &ColumnType::Json).is_err());
    }
}
