use std::cmp::Ordering;
use std::collections::BTreeMap;

use async_graphql::Value;
use serde_json::Value as JsonValue;

use crate::microsvc::Session;
use crate::table::{ColumnType, TableSchema};

use super::super::engine::EngineInner;
use super::super::filter::{CmpOp, FilterExpr, LitValue, Operand};
use super::super::naming::{is_valid_graphql_name, scalar_type_name};
use super::super::permissions::ReadPermission;
use super::binds::{operand_to_bind, value_to_bind, BindValue};
use super::dialect::{placeholder, SqlDialect};
use super::evidence::{
    ExtractedQueryEvidence, QueryEvidenceFieldPlan, QueryEvidenceKeyPlan, QueryEvidenceNode,
    QueryEvidenceObjectPlan, QueryEvidencePlan, QueryEvidenceRecordPlan,
    QUERY_EVIDENCE_HIDDEN_PREFIX,
};
use super::filter::compile_where;
use super::relationship::{compile_relationship_aggregate_subquery, compile_relationship_subquery};

#[derive(Clone, Debug)]
pub struct SqlPlan {
    pub sql: String,
    pub binds: Vec<BindValue>,
    /// JSON paths (dot-separated response keys) that need hex→base64 rewrite (SQLite Bytes).
    pub bytes_hex_paths: Vec<String>,
    pub tables_touched: Vec<String>,
    /// Compiler-owned shape for recovering every causal row identity. The
    /// hidden SQL aliases it describes are stripped before GraphQL sees data.
    pub(crate) evidence: QueryEvidencePlan,
}

impl SqlPlan {
    /// Recover complete physical keys and remove all compiler-only aliases.
    ///
    /// Call this after dialect JSON normalization (including SQLite's
    /// hex-to-base64 rewrite) and before converting to an async-graphql value.
    /// Shape errors still perform every safe, plan-guided removal so internal
    /// identity fields are never disclosed through an error path.
    pub(crate) fn extract_evidence_and_strip(
        &self,
        value: &mut JsonValue,
    ) -> Result<ExtractedQueryEvidence, String> {
        self.evidence.extract_and_strip(value)
    }
}

#[derive(Clone, Debug)]
pub struct SelectionNode {
    pub response_key: String,
    pub field_name: String,
    pub args: BTreeMap<String, Value>,
    pub children: Vec<SelectionNode>,
}

type RecordEvidenceProjection = (Vec<(String, String)>, Option<QueryEvidenceRecordPlan>);

/// Compiled GraphQL read: SQL scan or cell GET-by-pk.
#[derive(Clone, Debug)]
pub enum QueryPlan {
    Sql(SqlPlan),
    CellByKey {
        model: String,
        pk: BTreeMap<String, String>,
        /// Role row policy with every claim resolved from the trusted session.
        row_filter: Option<FilterExpr>,
    },
}

/// Compile a root field against the model's [`crate::graphql::ReadStore`].
pub fn compile_query(
    inner: &EngineInner,
    session: &Session,
    role: &str,
    model_name: &str,
    kind: RootKind,
    selection: &SelectionNode,
) -> Result<QueryPlan, String> {
    let store = inner
        .read_stores
        .get(model_name)
        .copied()
        .unwrap_or(crate::graphql::read_store::ReadStoreKind::SqlScan);
    match store {
        crate::graphql::read_store::ReadStoreKind::SqlScan => Ok(QueryPlan::Sql(compile_root(
            inner, session, role, model_name, kind, selection,
        )?)),
        crate::graphql::read_store::ReadStoreKind::CellByKey => {
            compile_cell_by_key(inner, session, role, model_name, kind, selection)
        }
    }
}

fn compile_cell_by_key(
    inner: &EngineInner,
    session: &Session,
    role: &str,
    model_name: &str,
    kind: RootKind,
    selection: &SelectionNode,
) -> Result<QueryPlan, String> {
    let entry = inner
        .catalog
        .get(model_name)
        .ok_or_else(|| format!("unknown model `{model_name}`"))?;
    let permission = inner
        .permissions
        .get(&(model_name.to_string(), role.to_string()))
        .map(|entry| &entry.permission)
        .ok_or_else(|| format!("role `{role}` has no permission on `{model_name}`"))?;
    match kind {
        RootKind::List => {
            return Err(
                "cell-by-key store does not support list queries (would fan out to N cells); declare a SQL index read model"
                    .into(),
            );
        }
        RootKind::Aggregate => {
            return Err(
                "cell-by-key store does not support aggregate queries; declare a SQL index read model"
                    .into(),
            );
        }
        RootKind::ByPk => {}
    }
    if selection.args.contains_key("where") {
        return Err("cell-by-key store does not support filter".into());
    }
    if selection.args.contains_key("order_by") {
        return Err("cell-by-key store does not support sort".into());
    }
    for child in &selection.children {
        let is_join = entry.schema.relationships.iter().any(|rel| {
            rel.field_name == child.field_name
                || child.field_name == format!("{}_aggregate", rel.field_name)
        });
        if is_join {
            return Err(
                "cell-by-key store does not support SQL joins; declare a SQL index read model"
                    .into(),
            );
        }
    }
    let row_filter = permission
        .row_filter
        .as_ref()
        .map(|filter| resolve_cell_row_filter(&entry.schema, session, filter))
        .transpose()?;
    let mut pk = BTreeMap::new();
    for column in &entry.schema.primary_key.columns {
        let value = selection
            .args
            .get(column)
            .ok_or_else(|| format!("missing primary key argument `{column}`"))?;
        let key = match value {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            other => {
                return Err(format!(
                    "cell-by-key primary key `{column}` must be a scalar, got {other:?}"
                ));
            }
        };
        pk.insert(column.clone(), key);
    }
    Ok(QueryPlan::CellByKey {
        model: model_name.to_string(),
        pk,
        row_filter,
    })
}

/// Resolve a cell row policy before the remote GET so missing or malformed
/// claims cannot turn row existence into an authorization side channel.
fn resolve_cell_row_filter(
    schema: &TableSchema,
    session: &Session,
    filter: &FilterExpr,
) -> Result<FilterExpr, String> {
    Ok(match filter {
        FilterExpr::And(items) => FilterExpr::And(
            items
                .iter()
                .map(|item| resolve_cell_row_filter(schema, session, item))
                .collect::<Result<_, _>>()?,
        ),
        FilterExpr::Or(items) => FilterExpr::Or(
            items
                .iter()
                .map(|item| resolve_cell_row_filter(schema, session, item))
                .collect::<Result<_, _>>()?,
        ),
        FilterExpr::Not(item) => {
            FilterExpr::Not(Box::new(resolve_cell_row_filter(schema, session, item)?))
        }
        FilterExpr::Cmp { column, op, rhs } => {
            let column_schema = cell_policy_column(schema, column)?;
            match op {
                CmpOp::Eq | CmpOp::Neq
                    if matches!(
                        column_schema.column_type,
                        ColumnType::Text
                            | ColumnType::Timestamp
                            | ColumnType::Boolean
                            | ColumnType::Integer
                            | ColumnType::UnsignedInteger
                            | ColumnType::Float
                    ) => {}
                CmpOp::Gt | CmpOp::Gte | CmpOp::Lt | CmpOp::Lte
                    if matches!(
                        column_schema.column_type,
                        ColumnType::Integer | ColumnType::UnsignedInteger | ColumnType::Float
                    ) => {}
                _ => {
                    return Err(format!(
                        "cell-by-key row policy operator `{op:?}` is unsupported for column `{column}`"
                    ));
                }
            }
            FilterExpr::Cmp {
                column: column.clone(),
                op: *op,
                rhs: resolve_cell_operand(rhs, session, &column_schema.column_type)?,
            }
        }
        FilterExpr::In {
            column,
            values,
            negated,
        } => {
            let column_schema = cell_policy_column(schema, column)?;
            if !matches!(
                column_schema.column_type,
                ColumnType::Text
                    | ColumnType::Timestamp
                    | ColumnType::Boolean
                    | ColumnType::Integer
                    | ColumnType::UnsignedInteger
                    | ColumnType::Float
            ) {
                return Err(format!(
                    "cell-by-key row policy IN is unsupported for column `{column}`"
                ));
            }
            FilterExpr::In {
                column: column.clone(),
                values: values
                    .iter()
                    .map(|value| resolve_cell_operand(value, session, &column_schema.column_type))
                    .collect::<Result<_, _>>()?,
                negated: *negated,
            }
        }
        FilterExpr::IsNull { column, is_null } => {
            cell_policy_column(schema, column)?;
            FilterExpr::IsNull {
                column: column.clone(),
                is_null: *is_null,
            }
        }
        FilterExpr::Rel { field, .. } => {
            return Err(format!(
                "cell-by-key row policy cannot traverse relationship `{field}`"
            ));
        }
    })
}

fn cell_policy_column<'a>(
    schema: &'a TableSchema,
    column: &str,
) -> Result<&'a crate::table::TableColumn, String> {
    schema
        .columns
        .iter()
        .find(|candidate| candidate.column_name == column)
        .ok_or_else(|| format!("unknown cell row-policy column `{column}`"))
}

fn resolve_cell_operand(
    operand: &Operand,
    session: &Session,
    column_type: &ColumnType,
) -> Result<Operand, String> {
    let literal = match operand_to_bind(operand, session, column_type)? {
        BindValue::Null => LitValue::Null,
        BindValue::Bool(value) => LitValue::Bool(value),
        BindValue::I64(value) => LitValue::I64(value),
        BindValue::F64(value) if value.is_finite() => LitValue::F64(value),
        BindValue::F64(value) => {
            return Err(format!(
                "cell-by-key row-policy float `{value}` must be finite"
            ));
        }
        BindValue::Text(value) => LitValue::String(value),
        BindValue::Json(value) => LitValue::Json(value),
        BindValue::Bytes(_) => {
            return Err("cell-by-key row policies do not support byte operands".into());
        }
    };
    Ok(Operand::Lit(literal))
}

/// Apply the already-resolved scalar policy to a sealed cell row. Only an
/// exact SQL-style TRUE authorizes the row; FALSE, NULL/unknown, malformed
/// fields, and unsupported material all fail closed.
pub(crate) fn cell_row_matches(schema: &TableSchema, filter: &FilterExpr, row: &JsonValue) -> bool {
    let JsonValue::Object(row) = row else {
        return false;
    };
    matches!(evaluate_cell_filter(schema, filter, row), CellTruth::True)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CellTruth {
    True,
    False,
    Unknown,
}

impl CellTruth {
    fn not(self) -> Self {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Unknown => Self::Unknown,
        }
    }
}

fn evaluate_cell_filter(
    schema: &TableSchema,
    filter: &FilterExpr,
    row: &serde_json::Map<String, JsonValue>,
) -> CellTruth {
    match filter {
        FilterExpr::And(items) => {
            let mut result = CellTruth::True;
            for item in items {
                match evaluate_cell_filter(schema, item, row) {
                    CellTruth::False => return CellTruth::False,
                    CellTruth::Unknown => result = CellTruth::Unknown,
                    CellTruth::True => {}
                }
            }
            result
        }
        FilterExpr::Or(items) => {
            let mut result = CellTruth::False;
            for item in items {
                match evaluate_cell_filter(schema, item, row) {
                    CellTruth::True => return CellTruth::True,
                    CellTruth::Unknown => result = CellTruth::Unknown,
                    CellTruth::False => {}
                }
            }
            result
        }
        FilterExpr::Not(item) => evaluate_cell_filter(schema, item, row).not(),
        FilterExpr::Cmp { column, op, rhs } => {
            let Some((column_type, left)) = cell_row_value(schema, row, column) else {
                return CellTruth::Unknown;
            };
            let Operand::Lit(right) = rhs else {
                return CellTruth::Unknown;
            };
            evaluate_cell_comparison(&column_type, left, *op, right)
        }
        FilterExpr::In {
            column,
            values,
            negated,
        } => {
            if values.is_empty() {
                return if *negated {
                    CellTruth::True
                } else {
                    CellTruth::False
                };
            }
            let Some((column_type, left)) = cell_row_value(schema, row, column) else {
                return CellTruth::Unknown;
            };
            let mut unknown = false;
            for value in values {
                let Operand::Lit(right) = value else {
                    unknown = true;
                    continue;
                };
                match cell_values_equal(&column_type, left, right) {
                    Some(true) => {
                        return if *negated {
                            CellTruth::False
                        } else {
                            CellTruth::True
                        };
                    }
                    Some(false) => {}
                    None => unknown = true,
                }
            }
            if unknown {
                CellTruth::Unknown
            } else if *negated {
                CellTruth::True
            } else {
                CellTruth::False
            }
        }
        FilterExpr::IsNull { column, is_null } => {
            let Some((_, value)) = cell_row_value(schema, row, column) else {
                return CellTruth::Unknown;
            };
            if value.is_null() == *is_null {
                CellTruth::True
            } else {
                CellTruth::False
            }
        }
        FilterExpr::Rel { .. } => CellTruth::Unknown,
    }
}

fn cell_row_value<'a>(
    schema: &TableSchema,
    row: &'a serde_json::Map<String, JsonValue>,
    column: &str,
) -> Option<(ColumnType, &'a JsonValue)> {
    let column_schema = schema
        .columns
        .iter()
        .find(|candidate| candidate.column_name == column)?;
    let value = row
        .get(&column_schema.field_name)
        .or_else(|| row.get(&column_schema.column_name))?;
    Some((column_schema.column_type.clone(), value))
}

fn evaluate_cell_comparison(
    column_type: &ColumnType,
    left: &JsonValue,
    op: CmpOp,
    right: &LitValue,
) -> CellTruth {
    let matched = match op {
        CmpOp::Eq => cell_values_equal(column_type, left, right),
        CmpOp::Neq => cell_values_equal(column_type, left, right).map(|equal| !equal),
        CmpOp::Gt => cell_values_order(column_type, left, right).map(|order| order.is_gt()),
        CmpOp::Gte => cell_values_order(column_type, left, right).map(|order| order.is_ge()),
        CmpOp::Lt => cell_values_order(column_type, left, right).map(|order| order.is_lt()),
        CmpOp::Lte => cell_values_order(column_type, left, right).map(|order| order.is_le()),
        CmpOp::Like | CmpOp::Ilike | CmpOp::Contains | CmpOp::ContainedIn | CmpOp::HasKey => None,
    };
    match matched {
        Some(true) => CellTruth::True,
        Some(false) => CellTruth::False,
        None => CellTruth::Unknown,
    }
}

fn cell_values_equal(column_type: &ColumnType, left: &JsonValue, right: &LitValue) -> Option<bool> {
    if left.is_null() || matches!(right, LitValue::Null) {
        return None;
    }
    Some(match (column_type, right) {
        (ColumnType::Text | ColumnType::Timestamp, LitValue::String(right)) => {
            left.as_str()? == right
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

fn cell_values_order(
    column_type: &ColumnType,
    left: &JsonValue,
    right: &LitValue,
) -> Option<Ordering> {
    match (column_type, right) {
        (ColumnType::Integer, LitValue::I64(right)) => left.as_i64()?.partial_cmp(right),
        (ColumnType::UnsignedInteger, LitValue::I64(right)) if *right >= 0 => {
            left.as_u64()?.partial_cmp(&(*right as u64))
        }
        (ColumnType::Float, LitValue::F64(right)) => left.as_f64()?.partial_cmp(right),
        (ColumnType::Float, LitValue::I64(right)) => left.as_f64()?.partial_cmp(&(*right as f64)),
        _ => None,
    }
}

/// Compile a root field selection into one SQL statement.
pub fn compile_root(
    inner: &EngineInner,
    session: &Session,
    role: &str,
    model_name: &str,
    kind: RootKind,
    selection: &SelectionNode,
) -> Result<SqlPlan, String> {
    // Relationship-aware complexity before SQL (covers query + subscription paths).
    let cost =
        super::super::complexity::estimate_root_complexity(inner, model_name, kind, selection)?;
    if super::super::complexity::exceeds_budget(cost, inner.max_complexity) {
        return Err(format!(
            "query too complex (estimated {cost}, max {})",
            inner.max_complexity
        ));
    }

    let entry = inner
        .catalog
        .get(model_name)
        .ok_or_else(|| format!("unknown model `{model_name}`"))?;
    let perm = inner
        .permissions
        .get(&(model_name.to_string(), role.to_string()))
        .map(|p| &p.permission)
        .ok_or_else(|| format!("role `{role}` has no permission on `{model_name}`"))?;

    let mut binds = Vec::new();
    let mut bytes_paths = Vec::new();
    let mut tables = vec![entry.schema.table_name.clone()];
    let alias = "t0";

    let limit = resolve_limit(
        selection.args.get("limit"),
        perm.limit,
        inner.default_limit,
        inner.max_limit,
    );
    let offset = selection
        .args
        .get("offset")
        .and_then(value_as_u64)
        .unwrap_or(0);

    let order_sql = compile_order_by(
        &entry.schema,
        selection.args.get("order_by"),
        alias,
        perm,
        inner.strict_where,
        inner.dialect,
    )?;

    let ops = inner.dialect.ops();

    let (sql, evidence_root) = match kind {
        RootKind::List => {
            let (projection, object_evidence) = compile_object_projection(
                inner,
                session,
                role,
                &entry.schema,
                perm,
                selection,
                alias,
                &mut binds,
                &mut bytes_paths,
                &mut tables,
                "",
                0,
            )?;
            let where_sql = compile_where(
                inner,
                session,
                role,
                &entry.schema,
                perm,
                selection.args.get("where"),
                alias,
                &mut binds,
                &mut tables,
                0,
            )?;
            let json_agg = ops.json_agg;
            let coalesce_empty = ops.empty_array;
            let agg_arg = match ops.json_cast_fn {
                None => "root".to_string(),
                Some(f) => format!("{f}(root)"),
            };
            (
                format!(
                    "SELECT coalesce({json_agg}({agg_arg}), {coalesce_empty}) FROM (\n  SELECT {projection} AS root\n  FROM \"{}\" {alias}\n  WHERE {where_sql}\n  {order_sql}\n  LIMIT {} OFFSET {}\n) sub",
                    entry.schema.table_name,
                    {
                        binds.push(BindValue::I64(limit as i64));
                        placeholder(inner.dialect, binds.len())
                    },
                    {
                        binds.push(BindValue::I64(offset as i64));
                        placeholder(inner.dialect, binds.len())
                    }
                ),
                QueryEvidenceNode::List(Box::new(QueryEvidenceNode::Object(object_evidence))),
            )
        }
        RootKind::ByPk => {
            // Projection first: nested has_many/m2m subqueries emit LIMIT/OFFSET
            // `?` binds that appear in the SELECT text *before* the outer WHERE.
            // SQLite binds are positional, so PK + filter binds must be pushed
            // after projection binds (same order as `?` appearance in SQL).
            let (projection, object_evidence) = compile_object_projection(
                inner,
                session,
                role,
                &entry.schema,
                perm,
                selection,
                alias,
                &mut binds,
                &mut bytes_paths,
                &mut tables,
                "",
                0,
            )?;
            let mut pk_preds = Vec::new();
            for pk in &entry.schema.primary_key.columns {
                let v = selection
                    .args
                    .get(pk)
                    .ok_or_else(|| format!("missing primary key argument `{pk}`"))?;
                let col = entry
                    .schema
                    .columns
                    .iter()
                    .find(|c| c.column_name == *pk)
                    .ok_or_else(|| format!("pk column `{pk}` missing"))?;
                let bind = value_to_bind(v, &col.column_type)?;
                binds.push(bind);
                let ph = placeholder(inner.dialect, binds.len());
                pk_preds.push(format!("{alias}.\"{pk}\" = {ph}"));
            }
            let where_sql = compile_where(
                inner,
                session,
                role,
                &entry.schema,
                perm,
                selection.args.get("where"),
                alias,
                &mut binds,
                &mut tables,
                0,
            )?;
            let pk_where = pk_preds.join(" AND ");
            let full_where = if where_sql == "TRUE" || where_sql == "true" {
                pk_where
            } else {
                format!("({pk_where}) AND ({where_sql})")
            };
            (
                format!(
                    "SELECT {projection} FROM \"{}\" {alias} WHERE {full_where} LIMIT 1",
                    entry.schema.table_name
                ),
                QueryEvidenceNode::Object(object_evidence),
            )
        }
        RootKind::Aggregate => {
            let json_agg = ops.json_agg;
            let coalesce_empty = ops.empty_array;
            let table = entry.schema.table_name.as_str();
            let mut pairs = Vec::new();
            let mut evidence_fields = Vec::new();
            for aggregate_member in &selection.children {
                match aggregate_member.field_name.as_str() {
                    "__typename" => {}
                    "aggregate" => {
                        validate_response_key(&aggregate_member.response_key)?;
                        let mut aggregate_pairs = Vec::new();
                        for metric in &aggregate_member.children {
                            match metric.field_name.as_str() {
                                "__typename" => {}
                                "count" => {
                                    validate_response_key(&metric.response_key)?;
                                    let where_for_count = compile_where(
                                        inner,
                                        session,
                                        role,
                                        &entry.schema,
                                        perm,
                                        selection.args.get("where"),
                                        alias,
                                        &mut binds,
                                        &mut tables,
                                        0,
                                    )?;
                                    aggregate_pairs.push((
                                        metric.response_key.clone(),
                                        format!(
                                            "(SELECT count(*) FROM \"{table}\" {alias} WHERE {where_for_count})"
                                        ),
                                    ));
                                }
                                _ => {
                                    return Err(
                                        "aggregate fields selection contains an unsupported member"
                                            .into(),
                                    );
                                }
                            }
                        }
                        pairs.push((
                            aggregate_member.response_key.clone(),
                            chunked_json_object(inner.dialect, &aggregate_pairs),
                        ));
                    }
                    "nodes" => {
                        validate_response_key(&aggregate_member.response_key)?;
                        let nodes_path = aggregate_member.response_key.as_str();
                        let (nodes_proj, nodes_evidence) = compile_object_projection(
                            inner,
                            session,
                            role,
                            &entry.schema,
                            perm,
                            aggregate_member,
                            alias,
                            &mut binds,
                            &mut bytes_paths,
                            &mut tables,
                            nodes_path,
                            0,
                        )?;
                        let where_for_nodes = compile_where(
                            inner,
                            session,
                            role,
                            &entry.schema,
                            perm,
                            selection.args.get("where"),
                            alias,
                            &mut binds,
                            &mut tables,
                            0,
                        )?;
                        let lim = {
                            binds.push(BindValue::I64(limit as i64));
                            placeholder(inner.dialect, binds.len())
                        };
                        let off = {
                            binds.push(BindValue::I64(offset as i64));
                            placeholder(inner.dialect, binds.len())
                        };
                        pairs.push((
                            aggregate_member.response_key.clone(),
                            inner.dialect.ops().nested_json(format!(
                                "coalesce((SELECT {json_agg}(n) FROM (SELECT {nodes_proj} AS n FROM \"{table}\" {alias} WHERE {where_for_nodes} {order_sql} LIMIT {lim} OFFSET {off}) x), {coalesce_empty})"
                            )),
                        ));
                        evidence_fields.push(QueryEvidenceFieldPlan {
                            storage_key: aggregate_member.response_key.clone(),
                            response_key: aggregate_member.response_key.clone(),
                            node: Box::new(QueryEvidenceNode::List(Box::new(
                                QueryEvidenceNode::Object(nodes_evidence),
                            ))),
                        });
                    }
                    _ => {
                        return Err("aggregate selection contains an unsupported member".into());
                    }
                }
            }

            (
                format!("SELECT {}", chunked_json_object(inner.dialect, &pairs)),
                QueryEvidenceNode::Object(QueryEvidenceObjectPlan {
                    record: None,
                    fields: evidence_fields,
                }),
            )
        }
    };
    let evidence = QueryEvidencePlan::new(selection.response_key.clone(), evidence_root)?;

    Ok(SqlPlan {
        sql,
        binds,
        bytes_hex_paths: bytes_paths,
        tables_touched: tables,
        evidence,
    })
}

#[derive(Clone, Copy)]
pub enum RootKind {
    List,
    ByPk,
    Aggregate,
}

pub(super) fn validate_response_key(key: &str) -> Result<(), String> {
    if is_valid_graphql_name(key) {
        Ok(())
    } else {
        Err(format!("invalid GraphQL response key `{key}`"))
    }
}

pub(super) fn resolve_limit(
    client: Option<&Value>,
    role_limit: Option<u64>,
    default_limit: u64,
    max_limit: u64,
) -> u64 {
    let client = client.and_then(value_as_u64).unwrap_or(default_limit);
    let with_role = role_limit.map(|r| client.min(r)).unwrap_or(client);
    with_role.min(max_limit)
}

pub(super) fn value_as_u64(v: &Value) -> Option<u64> {
    match v {
        Value::Number(n) => n
            .as_u64()
            .or_else(|| n.as_i64().and_then(|i| u64::try_from(i).ok())),
        _ => None,
    }
}

pub(super) fn compile_object_projection(
    inner: &EngineInner,
    session: &Session,
    role: &str,
    schema: &TableSchema,
    perm: &ReadPermission,
    selection: &SelectionNode,
    alias: &str,
    binds: &mut Vec<BindValue>,
    bytes_paths: &mut Vec<String>,
    tables: &mut Vec<String>,
    path_prefix: &str,
    depth: usize,
) -> Result<(String, QueryEvidenceObjectPlan), String> {
    if depth > inner.max_depth {
        return Err("max depth exceeded".into());
    }
    let (mut pairs, record) = compile_record_evidence_projection(
        inner.dialect,
        schema,
        perm,
        alias,
        binds,
        bytes_paths,
        path_prefix,
    )?;
    let mut evidence_fields = Vec::new();

    // If no children, project all allowed columns.
    let fields: Vec<&SelectionNode> = if selection.children.is_empty() {
        Vec::new()
    } else {
        selection.children.iter().collect()
    };

    if fields.is_empty() {
        for col in schema.columns.iter().filter(|c| !c.skipped) {
            if !perm.allows_column(&col.column_name) {
                continue;
            }
            let expr = column_json_expr(inner.dialect, alias, col, binds)?;
            if matches!(col.column_type, ColumnType::Bytes)
                && matches!(inner.dialect, SqlDialect::Sqlite)
            {
                let p = if path_prefix.is_empty() {
                    col.column_name.clone()
                } else {
                    format!("{path_prefix}.{}", col.column_name)
                };
                bytes_paths.push(p);
            }
            validate_response_key(&col.column_name)?;
            pairs.push((col.column_name.clone(), expr));
        }
    } else {
        for child in fields {
            if let Some(rel_name) = child.field_name.strip_suffix("_aggregate") {
                if let Some(rel) = schema
                    .relationships
                    .iter()
                    .find(|r| r.field_name == rel_name)
                {
                    let target_entry = match inner.catalog.get(&rel.target_model) {
                        Some(e) => e,
                        None => continue,
                    };
                    let target_perm = match inner
                        .permissions
                        .get(&(rel.target_model.clone(), role.to_string()))
                    {
                        Some(p) if p.permission.aggregations => &p.permission,
                        _ => continue,
                    };
                    tables.push(target_entry.schema.table_name.clone());
                    let child_path = if path_prefix.is_empty() {
                        child.response_key.clone()
                    } else {
                        format!("{path_prefix}.{}", child.response_key)
                    };
                    let (sub, evidence_node) = compile_relationship_aggregate_subquery(
                        inner,
                        session,
                        role,
                        schema,
                        alias,
                        rel,
                        target_entry,
                        target_perm,
                        child,
                        binds,
                        bytes_paths,
                        tables,
                        &child_path,
                        depth + 1,
                    )?;
                    validate_response_key(&child.response_key)?;
                    pairs.push((
                        child.response_key.clone(),
                        inner.dialect.ops().nested_json(sub),
                    ));
                    evidence_fields.push(QueryEvidenceFieldPlan {
                        storage_key: child.response_key.clone(),
                        response_key: child.response_key.clone(),
                        node: Box::new(evidence_node),
                    });
                }
                continue;
            }
            if let Some(col) = schema
                .columns
                .iter()
                .find(|c| c.column_name == child.field_name && !c.skipped)
            {
                if !perm.allows_column(&col.column_name) {
                    continue;
                }
                let expr = column_json_expr(inner.dialect, alias, col, binds)?;
                if matches!(col.column_type, ColumnType::Bytes)
                    && matches!(inner.dialect, SqlDialect::Sqlite)
                {
                    let p = if path_prefix.is_empty() {
                        child.response_key.clone()
                    } else {
                        format!("{path_prefix}.{}", child.response_key)
                    };
                    bytes_paths.push(p);
                }
                validate_response_key(&child.response_key)?;
                pairs.push((child.response_key.clone(), expr));
                continue;
            }
            if let Some(rel) = schema
                .relationships
                .iter()
                .find(|r| r.field_name == child.field_name)
            {
                let target_entry = match inner.catalog.get(&rel.target_model) {
                    Some(e) => e,
                    None => continue,
                };
                let target_perm = match inner
                    .permissions
                    .get(&(rel.target_model.clone(), role.to_string()))
                {
                    Some(p) => &p.permission,
                    None => continue, // untracked for role
                };
                tables.push(target_entry.schema.table_name.clone());
                let child_path = if path_prefix.is_empty() {
                    child.response_key.clone()
                } else {
                    format!("{path_prefix}.{}", child.response_key)
                };
                let (sub, evidence_node) = compile_relationship_subquery(
                    inner,
                    session,
                    role,
                    schema,
                    alias,
                    rel,
                    target_entry,
                    target_perm,
                    child,
                    binds,
                    bytes_paths,
                    tables,
                    &child_path,
                    depth + 1,
                )?;
                validate_response_key(&child.response_key)?;
                pairs.push((
                    child.response_key.clone(),
                    inner.dialect.ops().nested_json(sub),
                ));
                evidence_fields.push(QueryEvidenceFieldPlan {
                    storage_key: child.response_key.clone(),
                    response_key: child.response_key.clone(),
                    node: Box::new(evidence_node),
                });
            }
        }
    }

    Ok((
        chunked_json_object(inner.dialect, &pairs),
        QueryEvidenceObjectPlan {
            record,
            fields: evidence_fields,
        },
    ))
}

pub(super) fn compile_record_evidence_projection(
    dialect: SqlDialect,
    schema: &TableSchema,
    perm: &ReadPermission,
    alias: &str,
    binds: &mut Vec<BindValue>,
    bytes_paths: &mut Vec<String>,
    path_prefix: &str,
) -> Result<RecordEvidenceProjection, String> {
    // Embedded client models deliberately have no stable normalized identity.
    // Do not manufacture per-record evidence that the client cannot address;
    // table/projector dependencies still flow through `tables_touched` and
    // produce conservative index evidence.
    if !has_client_normalized_identity(schema, perm) {
        return Ok((Vec::new(), None));
    }

    let mut pairs = Vec::with_capacity(schema.primary_key.columns.len());
    let mut key_fields = Vec::with_capacity(schema.primary_key.columns.len());

    for (ordinal, column_name) in schema.primary_key.columns.iter().enumerate() {
        let column = schema
            .columns
            .iter()
            .find(|column| column.column_name == *column_name)
            .ok_or_else(|| {
                format!(
                    "primary key column `{column_name}` missing from model `{}`",
                    schema.model_name
                )
            })?;
        let hidden_key = format!("{QUERY_EVIDENCE_HIDDEN_PREFIX}{ordinal}");
        debug_assert!(!is_valid_graphql_name(&hidden_key));

        // GraphQL BigInt uses decimal strings. Casting the private identity
        // copy avoids any JSON-number precision loss while leaving the visible
        // field's legacy representation unchanged.
        let expression = match (&column.column_type, dialect) {
            (ColumnType::Integer | ColumnType::UnsignedInteger, SqlDialect::Postgres) => {
                format!("{alias}.\"{}\"::text", column.column_name)
            }
            (ColumnType::Integer | ColumnType::UnsignedInteger, SqlDialect::Sqlite) => {
                format!("CAST({alias}.\"{}\" AS TEXT)", column.column_name)
            }
            // PostgreSQL's MIME-style base64 encoder inserts line breaks for
            // long values. Evidence uses canonical RFC 4648 text so the scope
            // codec can reject ambiguous spellings without rejecting valid
            // byte primary keys.
            (ColumnType::Bytes, SqlDialect::Postgres) => format!(
                "replace(encode({alias}.\"{}\", 'base64'), E'\\n', '')",
                column.column_name
            ),
            _ => column_json_expr(dialect, alias, column, binds)?,
        };
        if matches!(column.column_type, ColumnType::Bytes) && matches!(dialect, SqlDialect::Sqlite)
        {
            bytes_paths.push(if path_prefix.is_empty() {
                hidden_key.clone()
            } else {
                format!("{path_prefix}.{hidden_key}")
            });
        }
        pairs.push((hidden_key.clone(), expression));
        key_fields.push(QueryEvidenceKeyPlan {
            hidden_key,
            column: column.column_name.clone(),
        });
    }

    Ok((
        pairs,
        Some(QueryEvidenceRecordPlan {
            model: schema.model_name.clone(),
            key_fields,
        }),
    ))
}

fn has_client_normalized_identity(schema: &TableSchema, perm: &ReadPermission) -> bool {
    !schema.primary_key.columns.is_empty()
        && schema.primary_key.columns.iter().all(|key| {
            schema
                .columns
                .iter()
                .find(|column| column.column_name == *key)
                .is_some_and(|column| {
                    !column.skipped
                        && !column.nullable
                        && perm.allows_column(key)
                        && scalar_type_name(&column.column_type)
                            .is_some_and(|scalar| scalar != "BigInt")
                })
        })
}

pub(super) fn column_json_expr(
    dialect: SqlDialect,
    alias: &str,
    col: &crate::table::TableColumn,
    _binds: &mut Vec<BindValue>,
) -> Result<String, String> {
    let q = format!("{alias}.\"{}\"", col.column_name);
    Ok(match (&col.column_type, dialect) {
        (ColumnType::Timestamp, SqlDialect::Postgres) => format!("{q}::text"),
        (ColumnType::Bytes, SqlDialect::Postgres) => format!("encode({q}, 'base64')"),
        (ColumnType::Bytes, SqlDialect::Sqlite) => format!("hex({q})"),
        (ColumnType::Json, SqlDialect::Postgres) => q.to_string(),
        _ => q,
    })
}

pub(super) fn chunked_json_object(dialect: SqlDialect, pairs: &[(String, String)]) -> String {
    let build = dialect.ops().build_object;
    if pairs.is_empty() {
        return format!("{build}()");
    }
    let chunks: Vec<&[(String, String)]> = pairs.chunks(40).collect();
    if chunks.len() == 1 {
        return format!(
            "{build}({})",
            pairs
                .iter()
                .map(|(k, v)| format!("'{k}', {v}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    match dialect {
        SqlDialect::Postgres => {
            let parts: Vec<String> = chunks
                .iter()
                .map(|chunk| {
                    format!(
                        "{build}({})",
                        chunk
                            .iter()
                            .map(|(k, v)| format!("'{k}', {v}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                })
                .collect();
            parts.join(" || ")
        }
        SqlDialect::Sqlite => {
            // Nested json_insert
            let mut expr = format!(
                "{build}({})",
                chunks[0]
                    .iter()
                    .map(|(k, v)| format!("'{k}', {v}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            for chunk in chunks.iter().skip(1) {
                let inserts = chunk
                    .iter()
                    .map(|(k, v)| format!("'$.{k}', {v}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                expr = format!("json_insert({expr}, {inserts})");
            }
            expr
        }
    }
}

pub(super) fn compile_order_by(
    schema: &TableSchema,
    order_arg: Option<&Value>,
    alias: &str,
    perm: &ReadPermission,
    strict: bool,
    dialect: SqlDialect,
) -> Result<String, String> {
    let mut parts = Vec::new();
    if let Some(Value::List(items)) = order_arg {
        for item in items {
            if let Value::Object(map) = item {
                if map.len() > 1 {
                    return Err(
                        "ambiguous order_by entry: use one field per list entry to declare priority"
                            .into(),
                    );
                }
                for (col, dir) in map {
                    if !schema.columns.iter().any(|c| c.column_name == *col) {
                        if strict {
                            return Err(format!("unknown order_by column `{col}`"));
                        }
                        continue;
                    }
                    if !perm.allows_column(col) {
                        if strict {
                            return Err(format!("ungranted order_by column `{col}`"));
                        }
                        continue;
                    }
                    let dir_s = match dir {
                        Value::Enum(e) => e.as_str(),
                        Value::String(s) => s.as_str(),
                        _ => "asc",
                    };
                    let sql_dir = match dir_s {
                        "desc" | "desc_nulls_first" | "desc_nulls_last" => "DESC",
                        _ => "ASC",
                    };
                    let nulls = match dir_s {
                        "asc_nulls_first" | "desc_nulls_first" => " NULLS FIRST",
                        "asc_nulls_last" | "desc_nulls_last" => " NULLS LAST",
                        _ => "",
                    };
                    let collation = schema
                        .columns
                        .iter()
                        .find(|column| column.column_name == *col)
                        .filter(|column| column.column_type == ColumnType::Text)
                        .map(|_| format!(" COLLATE {}", dialect.ops().binary_collation))
                        .unwrap_or_default();
                    parts.push(format!("{alias}.\"{col}\"{collation} {sql_dir}{nulls}"));
                }
            }
        }
    }
    // Always append PK asc tiebreaker.
    for pk in &schema.primary_key.columns {
        let collation = schema
            .columns
            .iter()
            .find(|column| column.column_name == *pk)
            .filter(|column| column.column_type == ColumnType::Text)
            .map(|_| format!(" COLLATE {}", dialect.ops().binary_collation))
            .unwrap_or_default();
        parts.push(format!("{alias}.\"{pk}\"{collation} ASC"));
    }
    if parts.is_empty() {
        Ok(String::new())
    } else {
        Ok(format!("ORDER BY {}", parts.join(", ")))
    }
}

/// Walk async-graphql selection field into our SelectionNode tree.
pub fn selection_from_field(field: async_graphql::SelectionField<'_>) -> SelectionNode {
    let mut args = BTreeMap::new();
    if let Ok(arg_list) = field.arguments() {
        for (name, value) in arg_list {
            args.insert(name.to_string(), value);
        }
    }
    let mut children = Vec::new();
    for sel in field.selection_set() {
        children.push(selection_from_field(sel));
    }
    SelectionNode {
        response_key: field.alias().unwrap_or_else(|| field.name()).to_string(),
        field_name: field.name().to_string(),
        args,
        children,
    }
}

/// Helper for pure unit tests without an engine.
#[allow(dead_code)]
pub fn compile_list_sql_for_test(
    dialect: SqlDialect,
    schema: &TableSchema,
    where_sql: &str,
    limit: u64,
) -> String {
    let ops = dialect.ops();
    let json_agg = ops.json_agg;
    let coalesce_empty = ops.empty_array;
    let build = ops.build_object;
    let pairs: Vec<String> = schema
        .columns
        .iter()
        .filter(|c| !c.skipped)
        .map(|c| format!("'{}', t0.\"{}\"", c.column_name, c.column_name))
        .collect();
    format!(
        "SELECT coalesce({json_agg}(root), {coalesce_empty}) FROM (\n  SELECT {build}({}) AS root\n  FROM \"{}\" t0\n  WHERE {where_sql}\n  ORDER BY {}\n  LIMIT {limit} OFFSET 0\n) sub",
        pairs.join(", "),
        schema.table_name,
        schema
            .primary_key
            .columns
            .iter()
            .map(|c| format!("t0.\"{c}\" ASC"))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

#[cfg(test)]
mod security_tests {
    use super::*;
    use crate::graphql::naming::is_valid_graphql_name;

    #[test]
    fn response_key_validator_accepts_graphql_names() {
        assert!(validate_response_key("order_id").is_ok());
        assert!(validate_response_key("_x").is_ok());
        assert!(validate_response_key("a1").is_ok());
    }

    #[test]
    fn response_key_validator_rejects_injection_shaped_keys() {
        assert!(validate_response_key("a', (SELECT 1), '").is_err());
        assert!(validate_response_key("a b").is_err());
        assert!(validate_response_key("").is_err());
        assert!(validate_response_key("__proto__").is_err());
        assert!(!is_valid_graphql_name("1bad"));
    }

    #[test]
    fn resolve_limit_clamps_to_max() {
        assert_eq!(resolve_limit(None, None, 100, 1000), 100);
        assert_eq!(
            resolve_limit(Some(&Value::from(9_000_000u64)), None, 100, 1000),
            1000
        );
        assert_eq!(
            resolve_limit(Some(&Value::from(50u64)), Some(10), 100, 1000),
            10
        );
    }

    #[test]
    fn resolve_limit_ignores_negative_values() {
        assert_eq!(resolve_limit(Some(&Value::from(-1)), None, 100, 1000), 100);
        assert_eq!(value_as_u64(&Value::from(-1)), None);
    }
}

#[cfg(test)]
mod strict_order_by_tests {
    use super::*;
    use crate::graphql::permissions::read;
    use crate::table::{ColumnType, PrimaryKey, TableColumn, TableKind, TableSchema};
    use async_graphql::indexmap::IndexMap;
    use async_graphql::Value as GqlValue;

    fn item_schema() -> TableSchema {
        TableSchema {
            model_name: "Item".into(),
            table_name: "items".into(),
            columns: vec![
                TableColumn {
                    primary_key: true,
                    ..TableColumn::new("id", "id", ColumnType::Text)
                },
                TableColumn::new("name", "name", ColumnType::Text),
                TableColumn::new("secret", "secret", ColumnType::Text),
            ],
            primary_key: PrimaryKey::new(["id"]),
            version_column: None,
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: TableKind::ReadModel,
        }
    }

    fn order_list(entries: Vec<(&str, &str)>) -> GqlValue {
        let mut items = Vec::new();
        for (col, dir) in entries {
            let mut map = IndexMap::new();
            map.insert(
                async_graphql::Name::new(col),
                GqlValue::Enum(async_graphql::Name::new(dir)),
            );
            items.push(GqlValue::Object(map));
        }
        GqlValue::List(items)
    }

    #[test]
    fn strict_rejects_unknown_order_column() {
        let schema = item_schema();
        let perm = read().all_columns();
        let arg = order_list(vec![("nope", "asc")]);
        let err = compile_order_by(&schema, Some(&arg), "t0", &perm, true, SqlDialect::Sqlite)
            .unwrap_err();
        assert!(err.contains("unknown order_by"), "{err}");
    }

    #[test]
    fn strict_rejects_ungranted_order_column() {
        let schema = item_schema();
        let perm = read().columns(["id", "name"]);
        let arg = order_list(vec![("secret", "asc")]);
        let err = compile_order_by(&schema, Some(&arg), "t0", &perm, true, SqlDialect::Sqlite)
            .unwrap_err();
        assert!(err.contains("ungranted order_by"), "{err}");
    }

    #[test]
    fn soft_skip_ignores_unknown_and_ungranted_order() {
        let schema = item_schema();
        let perm = read().columns(["id", "name"]);
        let arg = order_list(vec![("secret", "asc"), ("nope", "desc"), ("name", "desc")]);
        let sql =
            compile_order_by(&schema, Some(&arg), "t0", &perm, false, SqlDialect::Sqlite).unwrap();
        assert!(sql.contains(r#"t0."name" COLLATE BINARY DESC"#), "{sql}");
        assert!(!sql.contains("secret"), "{sql}");
        assert!(!sql.contains("nope"), "{sql}");
        assert!(
            sql.contains(r#"t0."id" COLLATE BINARY ASC"#),
            "pk tiebreak: {sql}"
        );
    }

    #[test]
    fn strict_accepts_granted_order_with_pk_tiebreak() {
        let schema = item_schema();
        let perm = read().all_columns();
        let arg = order_list(vec![("name", "desc")]);
        let sql =
            compile_order_by(&schema, Some(&arg), "t0", &perm, true, SqlDialect::Sqlite).unwrap();
        assert!(sql.contains(r#"t0."name" COLLATE BINARY DESC"#), "{sql}");
        assert!(sql.contains(r#"t0."id" COLLATE BINARY ASC"#), "{sql}");
    }

    #[test]
    fn multi_field_order_object_is_rejected_even_in_soft_mode() {
        let schema = item_schema();
        let perm = read().all_columns();
        let mut entry = IndexMap::new();
        entry.insert(
            async_graphql::Name::new("name"),
            GqlValue::Enum(async_graphql::Name::new("desc")),
        );
        entry.insert(
            async_graphql::Name::new("id"),
            GqlValue::Enum(async_graphql::Name::new("asc")),
        );
        let arg = GqlValue::List(vec![GqlValue::Object(entry)]);

        let error = compile_order_by(&schema, Some(&arg), "t0", &perm, false, SqlDialect::Sqlite)
            .unwrap_err();
        assert!(error.contains("ambiguous order_by"), "{error}");
        assert!(error.contains("one field per list entry"), "{error}");
    }

    #[test]
    fn separate_order_entries_preserve_declared_priority() {
        let schema = item_schema();
        let perm = read().all_columns();
        let arg = order_list(vec![("name", "desc"), ("secret", "asc")]);

        let sql =
            compile_order_by(&schema, Some(&arg), "t0", &perm, true, SqlDialect::Postgres).unwrap();
        assert!(sql.contains(r#"t0."name" COLLATE "C" DESC"#), "{sql}");
        assert!(sql.contains(r#"t0."secret" COLLATE "C" ASC"#), "{sql}");
        let name_position = sql
            .find(r#"t0."name" COLLATE "C" DESC"#)
            .expect("name ordering");
        let secret_position = sql
            .find(r#"t0."secret" COLLATE "C" ASC"#)
            .expect("secret ordering");
        assert!(name_position < secret_position, "{sql}");
    }
}
