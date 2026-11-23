//! Filter expression AST + column/claim/literal DSL for permission row filters
//! and (via the compiler) client `where` arguments.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use std::cmp::Ordering;

use crate::table::{ColumnType, TableSchema};

/// Right-hand side of a comparison: literal, claim header, or nested operand.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Operand {
    Lit(LitValue),
    Claim(ClaimRef),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum LitValue {
    String(String),
    I64(i64),
    F64(f64),
    Bool(bool),
    Json(JsonValue),
    Null,
}

pub(crate) fn validate_row_policy_operand_literal(
    column: &str,
    column_type: &ColumnType,
    op: Option<CmpOp>,
    operand: &Operand,
) -> Result<(), String> {
    if let Some(op) = op {
        let supported = match op {
            CmpOp::Like | CmpOp::Ilike => matches!(column_type, ColumnType::Text),
            CmpOp::Contains | CmpOp::ContainedIn | CmpOp::HasKey => {
                matches!(column_type, ColumnType::Json)
            }
            CmpOp::Eq | CmpOp::Neq | CmpOp::Gt | CmpOp::Gte | CmpOp::Lt | CmpOp::Lte => true,
        };
        if !supported {
            return Err(format!(
                "row-policy operator `{op:?}` cannot target column `{column}` of type `{column_type:?}`"
            ));
        }
    }
    let Operand::Lit(literal) = operand else {
        return Ok(());
    };
    if matches!(literal, LitValue::Null) {
        return Ok(());
    }
    if let LitValue::F64(value) = literal {
        if !value.is_finite() {
            return Err(format!(
                "row-policy floating-point literal `{value}` for column `{column}` must be finite"
            ));
        }
    }
    let compatible = match (column_type, literal) {
        (ColumnType::Text | ColumnType::Timestamp, LitValue::String(_))
        | (ColumnType::Boolean, LitValue::Bool(_))
        | (ColumnType::Integer, LitValue::I64(_))
        | (ColumnType::UnsignedInteger, LitValue::I64(0..))
        | (ColumnType::Float, LitValue::F64(_) | LitValue::I64(_)) => true,
        (ColumnType::Json, LitValue::String(_)) if matches!(op, Some(CmpOp::HasKey)) => true,
        (ColumnType::Json, LitValue::Json(_)) if !matches!(op, Some(CmpOp::HasKey)) => true,
        _ => false,
    };
    if compatible {
        return Ok(());
    }

    let literal_kind = match literal {
        LitValue::String(_) => "string",
        LitValue::I64(_) => "i64",
        LitValue::F64(_) => "f64",
        LitValue::Bool(_) => "bool",
        LitValue::Json(_) => "json",
        LitValue::Null => "null",
    };
    let expected = match column_type {
        ColumnType::Text | ColumnType::Timestamp => "a string literal",
        ColumnType::Boolean => "a boolean literal",
        ColumnType::Integer => "an i64 literal",
        ColumnType::UnsignedInteger => "a non-negative i64 literal",
        ColumnType::Float => "a finite f64 or i64 literal",
        ColumnType::Json if matches!(op, Some(CmpOp::HasKey)) => "a string key literal",
        ColumnType::Json => "an explicitly tagged JSON literal",
        ColumnType::Bytes => "no non-null row-policy literal encoding is defined",
        ColumnType::Unsupported(_) => "no row-policy literal encoding is supported",
    };
    Err(format!(
        "row-policy literal kind `{literal_kind}` cannot target column `{column}` of type `{column_type:?}`; expected {expected}"
    ))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimRef {
    pub header: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColRef {
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum FilterExpr {
    And(Vec<FilterExpr>),
    Or(Vec<FilterExpr>),
    Not(Box<FilterExpr>),
    Cmp {
        column: String,
        op: CmpOp,
        rhs: Operand,
    },
    In {
        column: String,
        values: Vec<Operand>,
        negated: bool,
    },
    IsNull {
        column: String,
        is_null: bool,
    },
    Rel {
        field: String,
        predicate: Box<FilterExpr>,
    },
}

// A handwritten serializer keeps the recursive AST out of serde's nested
// generic ContentSerializer expansion (which otherwise overflows rustc's trait
// solver when the AST is embedded in the versioned client manifest).
impl Serialize for FilterExpr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde_json::{json, Value};
        fn value(expression: &FilterExpr) -> Value {
            match expression {
                FilterExpr::And(items) => {
                    json!({ "kind": "and", "value": items.iter().map(value).collect::<Vec<_>>() })
                }
                FilterExpr::Or(items) => {
                    json!({ "kind": "or", "value": items.iter().map(value).collect::<Vec<_>>() })
                }
                FilterExpr::Not(item) => json!({ "kind": "not", "value": value(item) }),
                FilterExpr::Cmp { column, op, rhs } => json!({
                    "kind": "cmp",
                    "value": { "column": column, "op": op, "rhs": rhs }
                }),
                FilterExpr::In {
                    column,
                    values,
                    negated,
                } => json!({
                    "kind": "in",
                    "value": { "column": column, "values": values, "negated": negated }
                }),
                FilterExpr::IsNull { column, is_null } => json!({
                    "kind": "is_null",
                    "value": { "column": column, "is_null": is_null }
                }),
                FilterExpr::Rel { field, predicate } => json!({
                    "kind": "rel",
                    "value": { "field": field, "predicate": value(predicate) }
                }),
            }
        }
        canonical_json_value(value(self)).serialize(serializer)
    }
}

fn canonical_json_value(value: JsonValue) -> JsonValue {
    match value {
        JsonValue::Array(values) => {
            JsonValue::Array(values.into_iter().map(canonical_json_value).collect())
        }
        JsonValue::Object(values) => {
            let mut entries = values.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            JsonValue::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonical_json_value(value)))
                    .collect(),
            )
        }
        scalar => scalar,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CmpOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    Like,
    Ilike,
    Contains,
    ContainedIn,
    HasKey,
}

pub fn col(name: &str) -> ColRef {
    ColRef {
        name: name.to_string(),
    }
}

pub fn claim(header: &str) -> ClaimRef {
    ClaimRef {
        header: header.to_string(),
    }
}

pub fn lit(v: impl Into<LitValue>) -> LitValue {
    v.into()
}

pub fn rel(field: &str, f: FilterExpr) -> FilterExpr {
    FilterExpr::Rel {
        field: field.to_string(),
        predicate: Box::new(f),
    }
}

impl From<&str> for LitValue {
    fn from(s: &str) -> Self {
        LitValue::String(s.to_string())
    }
}
impl From<String> for LitValue {
    fn from(s: String) -> Self {
        LitValue::String(s)
    }
}
impl From<i64> for LitValue {
    fn from(v: i64) -> Self {
        LitValue::I64(v)
    }
}
impl From<i32> for LitValue {
    fn from(v: i32) -> Self {
        LitValue::I64(v as i64)
    }
}
impl From<f64> for LitValue {
    fn from(v: f64) -> Self {
        LitValue::F64(v)
    }
}
impl From<bool> for LitValue {
    fn from(v: bool) -> Self {
        LitValue::Bool(v)
    }
}
impl From<JsonValue> for LitValue {
    fn from(v: JsonValue) -> Self {
        LitValue::Json(v)
    }
}

impl From<LitValue> for Operand {
    fn from(v: LitValue) -> Self {
        Operand::Lit(v)
    }
}
impl From<ClaimRef> for Operand {
    fn from(c: ClaimRef) -> Self {
        Operand::Claim(c)
    }
}
impl From<&str> for Operand {
    fn from(s: &str) -> Self {
        Operand::Lit(LitValue::from(s))
    }
}
impl From<String> for Operand {
    fn from(s: String) -> Self {
        Operand::Lit(LitValue::from(s))
    }
}
impl From<i64> for Operand {
    fn from(v: i64) -> Self {
        Operand::Lit(LitValue::from(v))
    }
}
impl From<f64> for Operand {
    fn from(v: f64) -> Self {
        Operand::Lit(LitValue::from(v))
    }
}
impl From<f32> for Operand {
    fn from(v: f32) -> Self {
        Operand::Lit(LitValue::from(v as f64))
    }
}
impl From<bool> for Operand {
    fn from(v: bool) -> Self {
        Operand::Lit(LitValue::from(v))
    }
}

impl ColRef {
    pub fn eq(self, rhs: impl Into<Operand>) -> FilterExpr {
        FilterExpr::Cmp {
            column: self.name,
            op: CmpOp::Eq,
            rhs: rhs.into(),
        }
    }
    pub fn neq(self, rhs: impl Into<Operand>) -> FilterExpr {
        FilterExpr::Cmp {
            column: self.name,
            op: CmpOp::Neq,
            rhs: rhs.into(),
        }
    }
    pub fn gt(self, rhs: impl Into<Operand>) -> FilterExpr {
        FilterExpr::Cmp {
            column: self.name,
            op: CmpOp::Gt,
            rhs: rhs.into(),
        }
    }
    pub fn gte(self, rhs: impl Into<Operand>) -> FilterExpr {
        FilterExpr::Cmp {
            column: self.name,
            op: CmpOp::Gte,
            rhs: rhs.into(),
        }
    }
    pub fn lt(self, rhs: impl Into<Operand>) -> FilterExpr {
        FilterExpr::Cmp {
            column: self.name,
            op: CmpOp::Lt,
            rhs: rhs.into(),
        }
    }
    pub fn lte(self, rhs: impl Into<Operand>) -> FilterExpr {
        FilterExpr::Cmp {
            column: self.name,
            op: CmpOp::Lte,
            rhs: rhs.into(),
        }
    }
    pub fn like(self, rhs: impl Into<Operand>) -> FilterExpr {
        FilterExpr::Cmp {
            column: self.name,
            op: CmpOp::Like,
            rhs: rhs.into(),
        }
    }
    pub fn ilike(self, rhs: impl Into<Operand>) -> FilterExpr {
        FilterExpr::Cmp {
            column: self.name,
            op: CmpOp::Ilike,
            rhs: rhs.into(),
        }
    }
    pub fn is_null(self, yes: bool) -> FilterExpr {
        FilterExpr::IsNull {
            column: self.name,
            is_null: yes,
        }
    }
    pub fn is_in(self, values: impl IntoIterator<Item = impl Into<Operand>>) -> FilterExpr {
        FilterExpr::In {
            column: self.name,
            values: values.into_iter().map(Into::into).collect(),
            negated: false,
        }
    }
    pub fn not_in(self, values: impl IntoIterator<Item = impl Into<Operand>>) -> FilterExpr {
        FilterExpr::In {
            column: self.name,
            values: values.into_iter().map(Into::into).collect(),
            negated: true,
        }
    }
}

impl FilterExpr {
    /// Validate row-policy literals before they reach either SQL execution or
    /// a serialized client Surface. JSON cannot represent NaN or infinities;
    /// accepting them would let distinct runtime policies collapse to the same
    /// manifest/fingerprint.
    pub fn validate_row_policy_literals(&self) -> Result<(), String> {
        fn validate_operand(operand: &Operand) -> Result<(), String> {
            if let Operand::Lit(LitValue::F64(value)) = operand {
                if !value.is_finite() {
                    return Err(format!(
                        "row-policy floating-point literal `{value}` must be finite"
                    ));
                }
            }
            Ok(())
        }

        match self {
            FilterExpr::And(expressions) | FilterExpr::Or(expressions) => {
                for expression in expressions {
                    expression.validate_row_policy_literals()?;
                }
            }
            FilterExpr::Not(expression)
            | FilterExpr::Rel {
                predicate: expression,
                ..
            } => {
                expression.validate_row_policy_literals()?;
            }
            FilterExpr::Cmp { rhs, .. } => validate_operand(rhs)?,
            FilterExpr::In { values, .. } => {
                for value in values {
                    validate_operand(value)?;
                }
            }
            FilterExpr::IsNull { .. } => {}
        }
        Ok(())
    }

    /// Whether this predicate can be evaluated by a JavaScript client without
    /// changing integer identity/ordering semantics.
    ///
    /// Runtime SQL can safely retain all i64 values. Values outside the JS
    /// safe-integer interval are therefore kept server-only in client
    /// manifests until the wire protocol provides canonical decimal strings.
    pub fn is_client_portable(&self) -> bool {
        const JS_MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;

        fn json_is_portable(value: &JsonValue) -> bool {
            match value {
                JsonValue::Null | JsonValue::Bool(_) | JsonValue::String(_) => true,
                JsonValue::Number(number) => {
                    if let Some(value) = number.as_i64() {
                        value.unsigned_abs() <= JS_MAX_SAFE_INTEGER as u64
                    } else if let Some(value) = number.as_u64() {
                        value <= JS_MAX_SAFE_INTEGER as u64
                    } else {
                        // serde_json accepts only finite JSON floats. Requiring a
                        // representable f64 also fails closed if a future number
                        // backend retains a value outside the JavaScript wire.
                        number.as_f64().is_some_and(f64::is_finite)
                    }
                }
                JsonValue::Array(values) => values.iter().all(json_is_portable),
                JsonValue::Object(values) => values.values().all(json_is_portable),
            }
        }

        fn operand_is_portable(operand: &Operand) -> bool {
            match operand {
                // Claim values are locally usable only after the selected
                // Surface binds their exact typed descriptor/value inventory to
                // the server-issued cache scope. Field visibility and codec
                // compatibility are validated while that inventory is derived.
                Operand::Claim(_) => true,
                Operand::Lit(LitValue::I64(value)) => {
                    value.unsigned_abs() <= JS_MAX_SAFE_INTEGER as u64
                }
                Operand::Lit(LitValue::Json(value)) => json_is_portable(value),
                _ => true,
            }
        }

        match self {
            FilterExpr::And(expressions) | FilterExpr::Or(expressions) => {
                expressions.iter().all(FilterExpr::is_client_portable)
            }
            FilterExpr::Not(expression)
            | FilterExpr::Rel {
                predicate: expression,
                ..
            } => expression.is_client_portable(),
            FilterExpr::Cmp { rhs, .. } => operand_is_portable(rhs),
            FilterExpr::In { values, .. } => values.iter().all(operand_is_portable),
            FilterExpr::IsNull { .. } => true,
        }
    }

    pub fn and(self, other: FilterExpr) -> FilterExpr {
        match self {
            FilterExpr::And(mut xs) => {
                xs.push(other);
                FilterExpr::And(xs)
            }
            other_self => FilterExpr::And(vec![other_self, other]),
        }
    }
    pub fn or(self, other: FilterExpr) -> FilterExpr {
        match self {
            FilterExpr::Or(mut xs) => {
                xs.push(other);
                FilterExpr::Or(xs)
            }
            other_self => FilterExpr::Or(vec![other_self, other]),
        }
    }
    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> FilterExpr {
        FilterExpr::Not(Box::new(self))
    }

    /// Walk columns / claims / rel fields for builder validation.
    pub fn visit_columns(&self, mut f: impl FnMut(&str)) {
        self.visit_columns_inner(&mut f);
    }
    fn visit_columns_inner(&self, f: &mut impl FnMut(&str)) {
        match self {
            FilterExpr::And(xs) | FilterExpr::Or(xs) => {
                for x in xs {
                    x.visit_columns_inner(f);
                }
            }
            FilterExpr::Not(x) => x.visit_columns_inner(f),
            FilterExpr::Cmp { column, .. }
            | FilterExpr::In { column, .. }
            | FilterExpr::IsNull { column, .. } => f(column),
            FilterExpr::Rel { predicate, .. } => predicate.visit_columns_inner(f),
        }
    }

    pub fn visit_claims(&self, mut f: impl FnMut(&str)) {
        self.visit_claims_inner(&mut f);
    }
    fn visit_claims_inner(&self, f: &mut impl FnMut(&str)) {
        match self {
            FilterExpr::And(xs) | FilterExpr::Or(xs) => {
                for x in xs {
                    x.visit_claims_inner(f);
                }
            }
            FilterExpr::Not(x) => x.visit_claims_inner(f),
            FilterExpr::Cmp { rhs, .. } => {
                if let Operand::Claim(c) = rhs {
                    f(&c.header);
                }
            }
            FilterExpr::In { values, .. } => {
                for v in values {
                    if let Operand::Claim(c) = v {
                        f(&c.header);
                    }
                }
            }
            FilterExpr::IsNull { .. } => {}
            FilterExpr::Rel { predicate, .. } => predicate.visit_claims_inner(f),
        }
    }

    pub fn visit_rels(&self, mut f: impl FnMut(&str, &FilterExpr)) {
        self.visit_rels_inner(&mut f);
    }
    fn visit_rels_inner(&self, f: &mut impl FnMut(&str, &FilterExpr)) {
        match self {
            FilterExpr::And(xs) | FilterExpr::Or(xs) => {
                for x in xs {
                    x.visit_rels_inner(f);
                }
            }
            FilterExpr::Not(x) => x.visit_rels_inner(f),
            FilterExpr::Rel { field, predicate } => {
                f(field, predicate);
                predicate.visit_rels_inner(f);
            }
            _ => {}
        }
    }
}

impl std::ops::Not for FilterExpr {
    type Output = FilterExpr;

    fn not(self) -> Self::Output {
        FilterExpr::Not(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        claim, col, validate_row_policy_operand_literal, CmpOp, FilterExpr, LitValue, Operand,
    };
    use crate::table::ColumnType;

    #[test]
    fn comparison_operands_accept_float_literals() {
        let expr = col("price").gt(9.99);
        let FilterExpr::Cmp {
            rhs: Operand::Lit(LitValue::F64(value)),
            ..
        } = expr
        else {
            panic!("expected f64 literal operand");
        };
        assert!((value - 9.99).abs() < f64::EPSILON);

        let expr = col("ratio").lt(0.5_f32);
        let FilterExpr::Cmp {
            rhs: Operand::Lit(LitValue::F64(value)),
            ..
        } = expr
        else {
            panic!("expected f32 literal operand to promote to f64");
        };
        assert!((value - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn row_policy_wire_json_is_canonical_across_map_backends() {
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

    #[test]
    fn row_policy_rejects_non_finite_floats_recursively_in_cmp_and_in() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let cmp = FilterExpr::Not(Box::new(FilterExpr::And(vec![col("price").gt(value)])));
            assert!(cmp
                .validate_row_policy_literals()
                .unwrap_err()
                .contains("must be finite"));

            let in_list = FilterExpr::Or(vec![FilterExpr::In {
                column: "price".into(),
                values: vec![Operand::from(1.0), Operand::from(value)],
                negated: false,
            }]);
            assert!(in_list
                .validate_row_policy_literals()
                .unwrap_err()
                .contains("must be finite"));
        }
    }

    #[test]
    fn row_policy_literal_kinds_must_match_the_authoritative_column_type() {
        let valid = [
            (ColumnType::Text, LitValue::String("text".into())),
            (
                ColumnType::Timestamp,
                LitValue::String("2026-01-01T00:00:00Z".into()),
            ),
            (ColumnType::Boolean, LitValue::Bool(true)),
            (ColumnType::Integer, LitValue::I64(-1)),
            (ColumnType::UnsignedInteger, LitValue::I64(1)),
            (ColumnType::Float, LitValue::F64(1.5)),
            (
                ColumnType::Json,
                LitValue::Json(serde_json::json!({"nested": true})),
            ),
            (ColumnType::Float, LitValue::I64(1)),
        ];
        for (column_type, literal) in valid {
            validate_row_policy_operand_literal(
                "value",
                &column_type,
                Some(CmpOp::Eq),
                &Operand::Lit(literal),
            )
            .unwrap();
            validate_row_policy_operand_literal(
                "value",
                &column_type,
                Some(CmpOp::Eq),
                &Operand::Lit(LitValue::Null),
            )
            .unwrap();
        }

        for (column_type, literal, expected) in [
            (
                ColumnType::Text,
                LitValue::Json(serde_json::json!("text")),
                "literal kind `json`",
            ),
            (
                ColumnType::Json,
                LitValue::String("text".into()),
                "explicitly tagged JSON",
            ),
            (
                ColumnType::UnsignedInteger,
                LitValue::I64(-1),
                "non-negative i64",
            ),
            (ColumnType::Boolean, LitValue::I64(1), "boolean literal"),
            (
                ColumnType::Bytes,
                LitValue::String("YQ==".into()),
                "no non-null",
            ),
            (
                ColumnType::Unsupported("custom".into()),
                LitValue::String("value".into()),
                "no row-policy literal encoding",
            ),
        ] {
            let error = validate_row_policy_operand_literal(
                "value",
                &column_type,
                Some(CmpOp::Eq),
                &Operand::Lit(literal),
            )
            .unwrap_err();
            assert!(error.contains("column `value`"), "{error}");
            assert!(error.contains(expected), "{error}");
        }

        validate_row_policy_operand_literal(
            "metadata",
            &ColumnType::Json,
            Some(CmpOp::HasKey),
            &Operand::Lit(LitValue::String("tenant".into())),
        )
        .unwrap();
        let has_key_error = validate_row_policy_operand_literal(
            "metadata",
            &ColumnType::Json,
            Some(CmpOp::HasKey),
            &Operand::Lit(LitValue::Json(serde_json::json!("tenant"))),
        )
        .unwrap_err();
        assert!(
            has_key_error.contains("string key literal"),
            "{has_key_error}"
        );

        for (column_type, op) in [
            (ColumnType::Text, CmpOp::HasKey),
            (ColumnType::Boolean, CmpOp::Like),
            (ColumnType::Json, CmpOp::Ilike),
        ] {
            let error = validate_row_policy_operand_literal(
                "value",
                &column_type,
                Some(op),
                &Operand::Lit(LitValue::String("value".into())),
            )
            .unwrap_err();
            assert!(error.contains("operator"), "{error}");
            assert!(error.contains("column `value`"), "{error}");
        }
    }

    #[test]
    fn finite_floats_pass_and_js_unsafe_i64_is_not_client_portable() {
        let finite = col("price")
            .gte(-123.5)
            .and(col("price").is_in([0.0, f64::MAX]));
        finite.validate_row_policy_literals().unwrap();
        assert!(finite.is_client_portable());

        assert!(col("id").eq(9_007_199_254_740_991_i64).is_client_portable());
        assert!(!col("id").eq(9_007_199_254_740_992_i64).is_client_portable());
        assert!(!col("id").eq(i64::MIN).is_client_portable());

        let safe_json = FilterExpr::Cmp {
            column: "metadata".into(),
            op: CmpOp::Contains,
            rhs: Operand::Lit(LitValue::Json(serde_json::json!({
                "nested": [9_007_199_254_740_991_u64, -9_007_199_254_740_991_i64]
            }))),
        };
        assert!(safe_json.is_client_portable());

        let unsafe_json = FilterExpr::Cmp {
            column: "metadata".into(),
            op: CmpOp::Contains,
            rhs: Operand::Lit(LitValue::Json(serde_json::json!({
                "nested": [9_007_199_254_740_992_u64]
            }))),
        };
        assert!(!unsafe_json.is_client_portable());

        assert!(
            col("owner_id").eq(claim("x-user-id")).is_client_portable(),
            "typed claim operands are portable once the selected surface binds their values"
        );
    }
}

/// SQL three-valued truth: a comparison with NULL is neither true nor false.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Truth {
    True,
    False,
    Unknown,
}

impl Truth {
    fn not(self) -> Self {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Unknown => Self::Unknown,
        }
    }

    fn of(decided: Option<bool>) -> Self {
        match decided {
            Some(true) => Self::True,
            Some(false) => Self::False,
            None => Self::Unknown,
        }
    }
}

impl FilterExpr {
    /// Decide this filter for one row without asking the database.
    ///
    /// The answer is the one the compiled `WHERE` gives on SQLite and on
    /// Postgres, or [`Truth::Unknown`]. `Unknown` covers SQL's own unknown (a
    /// comparison with NULL) and everything that cannot be decided here: a
    /// column missing from `row`, a claim `claims` does not supply, a
    /// relationship, patterns, JSON operators, and two different spellings of
    /// a timestamp. Callers that authorize on the
    /// result must require `True`.
    ///
    /// `row` holds JSON values keyed by field name or column name. `claims`
    /// resolves a claim to a literal of the compared column's type.
    pub fn evaluate(
        &self,
        schema: &TableSchema,
        row: &serde_json::Map<String, JsonValue>,
        claims: &dyn Fn(&ClaimRef, &ColumnType) -> Option<LitValue>,
    ) -> Truth {
        let cell = |column: &str| {
            let column = schema
                .columns
                .iter()
                .find(|candidate| candidate.column_name == column)?;
            let value = row
                .get(&column.field_name)
                .or_else(|| row.get(&column.column_name))?;
            Some((&column.column_type, value))
        };
        let literal = |operand: &Operand, column_type: &ColumnType| match operand {
            Operand::Lit(literal) => Some(literal.clone()),
            Operand::Claim(claim) => claims(claim, column_type),
        };
        match self {
            FilterExpr::And(items) => {
                let mut result = Truth::True;
                for item in items {
                    match item.evaluate(schema, row, claims) {
                        Truth::False => return Truth::False,
                        Truth::Unknown => result = Truth::Unknown,
                        Truth::True => {}
                    }
                }
                result
            }
            FilterExpr::Or(items) => {
                let mut result = Truth::False;
                for item in items {
                    match item.evaluate(schema, row, claims) {
                        Truth::True => return Truth::True,
                        Truth::Unknown => result = Truth::Unknown,
                        Truth::False => {}
                    }
                }
                result
            }
            FilterExpr::Not(item) => item.evaluate(schema, row, claims).not(),
            FilterExpr::Cmp { column, op, rhs } => {
                let Some((column_type, left)) = cell(column) else {
                    return Truth::Unknown;
                };
                let Some(right) = literal(rhs, column_type) else {
                    return Truth::Unknown;
                };
                Truth::of(match op {
                    CmpOp::Eq => values_equal(column_type, left, &right),
                    CmpOp::Neq => values_equal(column_type, left, &right).map(|equal| !equal),
                    CmpOp::Gt => values_order(column_type, left, &right).map(Ordering::is_gt),
                    CmpOp::Gte => values_order(column_type, left, &right).map(Ordering::is_ge),
                    CmpOp::Lt => values_order(column_type, left, &right).map(Ordering::is_lt),
                    CmpOp::Lte => values_order(column_type, left, &right).map(Ordering::is_le),
                    CmpOp::Like
                    | CmpOp::Ilike
                    | CmpOp::Contains
                    | CmpOp::ContainedIn
                    | CmpOp::HasKey => None,
                })
            }
            FilterExpr::In {
                column,
                values,
                negated,
            } => {
                // The compiler answers an empty list before reading the column.
                if values.is_empty() {
                    return Truth::of(Some(*negated));
                }
                let Some((column_type, left)) = cell(column) else {
                    return Truth::Unknown;
                };
                let mut unknown = false;
                for value in values {
                    let equal = literal(value, column_type)
                        .and_then(|right| values_equal(column_type, left, &right));
                    match equal {
                        Some(true) => return Truth::of(Some(!*negated)),
                        Some(false) => {}
                        None => unknown = true,
                    }
                }
                if unknown {
                    Truth::Unknown
                } else {
                    Truth::of(Some(*negated))
                }
            }
            FilterExpr::IsNull { column, is_null } => match cell(column) {
                Some((_, value)) => Truth::of(Some(value.is_null() == *is_null)),
                None => Truth::Unknown,
            },
            FilterExpr::Rel { .. } => Truth::Unknown,
        }
    }
}

/// `left = right` as SQL decides it, or `None` when SQL says NULL or the
/// pair has no portable equality.
///
/// Text is compared byte for byte; the compiler pins the same on SQLite and
/// Postgres text equality is bytewise under every deterministic collation.
fn values_equal(column_type: &ColumnType, left: &JsonValue, right: &LitValue) -> Option<bool> {
    if left.is_null() || matches!(right, LitValue::Null) {
        return None;
    }
    Some(match (column_type, right) {
        (ColumnType::Text, LitValue::String(right)) => left.as_str()? == right,
        // Postgres compares instants, so two spellings may still be equal.
        (ColumnType::Timestamp, LitValue::String(right)) => {
            if left.as_str()? == right {
                true
            } else {
                return None;
            }
        }
        (ColumnType::Boolean, LitValue::Bool(right)) => left.as_bool()? == *right,
        (ColumnType::Integer, LitValue::I64(right)) => left.as_i64()? == *right,
        (ColumnType::UnsignedInteger, LitValue::I64(right)) if *right >= 0 => {
            left.as_u64()? == *right as u64
        }
        (ColumnType::Float, LitValue::F64(right)) => left.as_f64()? == *right,
        (ColumnType::Float, LitValue::I64(right)) => left.as_f64()? == *right as f64,
        _ => return None,
    })
}

/// `left` against `right` as SQL orders them, or `None` when SQL says NULL or
/// the pair has no portable order. Text orders by its UTF-8 bytes, the
/// collation the compiler pins.
fn values_order(column_type: &ColumnType, left: &JsonValue, right: &LitValue) -> Option<Ordering> {
    match (column_type, right) {
        (ColumnType::Text, LitValue::String(right)) => Some(left.as_str()?.cmp(right.as_str())),
        (ColumnType::Integer, LitValue::I64(right)) => left.as_i64()?.partial_cmp(right),
        (ColumnType::UnsignedInteger, LitValue::I64(right)) if *right >= 0 => {
            left.as_u64()?.partial_cmp(&(*right as u64))
        }
        (ColumnType::Float, LitValue::F64(right)) => left.as_f64()?.partial_cmp(right),
        (ColumnType::Float, LitValue::I64(right)) => left.as_f64()?.partial_cmp(&(*right as f64)),
        _ => None,
    }
}
