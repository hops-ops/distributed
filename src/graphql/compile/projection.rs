use std::collections::BTreeMap;
use std::sync::Arc;

use async_graphql::Value;
use serde_json::Value as JsonValue;

use crate::microsvc::Session;
use crate::table::{ColumnType, TableSchema};

use super::super::engine::EngineInner;
use super::super::filter::{CmpOp, FilterExpr, LitValue, Operand, Truth};
use super::super::naming::is_valid_graphql_name;
use super::binds::{operand_to_bind, value_to_bind, BindValue};
use super::dialect::{placeholder, SqlDialect};
use super::evidence::{
    ExtractedQueryEvidence, QueryEvidenceFieldPlan, QueryEvidenceKeyPlan, QueryEvidenceNode,
    QueryEvidenceObjectPlan, QueryEvidencePlan, QueryEvidenceRecordPlan,
    QUERY_EVIDENCE_HIDDEN_PREFIX,
};
use super::relationship::{compile_relationship_aggregate_subquery, compile_relationship_subquery};
use super::scan::{value_as_u64, ModelScan, ScanLink};
use super::CompileError;

#[derive(Clone, Debug)]
pub struct SqlPlan {
    /// Shared with every execution, so running the plan copies no SQL.
    pub sql: Arc<str>,
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

    /// What makes two executions comparable snapshots of one query: the same
    /// statement over the same binds and tables.
    pub(crate) fn snapshot_scope(&self) -> impl serde::Serialize + '_ {
        #[derive(serde::Serialize)]
        struct QueryPlanInstance<'a> {
            domain: &'static str,
            version: u32,
            sql: &'a str,
            binds: &'a [BindValue],
            tables: &'a [String],
        }
        QueryPlanInstance {
            domain: "distributed.graphql.query-plan-instance",
            version: 1,
            sql: &self.sql,
            binds: &self.binds,
            tables: &self.tables_touched,
        }
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
    /// Shared: a live query re-executes, and gateway delivery hands on, the
    /// same plan without copying its SQL and binds.
    Sql(Arc<SqlPlan>),
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
) -> Result<QueryPlan, CompileError> {
    let store = inner
        .read_stores
        .get(model_name)
        .copied()
        .unwrap_or(crate::graphql::read_store::ReadStoreKind::SqlScan);
    match store {
        crate::graphql::read_store::ReadStoreKind::SqlScan => Ok(QueryPlan::Sql(Arc::new(
            compile_root(inner, session, role, model_name, kind, selection)?,
        ))),
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
) -> Result<QueryPlan, CompileError> {
    let unsupported = |detail: &str| Err(CompileError::UnsupportedOnCellStore(detail.into()));
    let scan = ModelScan::open(inner, session, role, model_name)?;
    let schema = scan.schema;
    match kind {
        RootKind::List => {
            return unsupported(
                "cell-by-key store does not support list queries (would fan out to N cells); declare a SQL index read model",
            );
        }
        RootKind::Aggregate => {
            return unsupported(
                "cell-by-key store does not support aggregate queries; declare a SQL index read model",
            );
        }
        RootKind::ByPk => {}
    }
    if selection.args.contains_key("where") {
        return unsupported("cell-by-key store does not support filter");
    }
    if selection.args.contains_key("order_by") {
        return unsupported("cell-by-key store does not support sort");
    }
    for child in &selection.children {
        let is_join = schema.relationships.iter().any(|rel| {
            rel.field_name == child.field_name
                || child.field_name == format!("{}_aggregate", rel.field_name)
        });
        if is_join {
            return unsupported(
                "cell-by-key store does not support SQL joins; declare a SQL index read model",
            );
        }
        // The cell resolver returns stored fields by name: nothing else
        // stands between a selection and an ungranted column.
        if child.field_name != "__typename" && !scan.allows_column(&child.field_name) {
            return Err(CompileError::internal(format!(
                "selection reached ungranted field `{}` on `{model_name}`",
                child.field_name
            )));
        }
    }
    let row_filter = scan
        .grant
        .row_policy
        .filter()
        .map(|filter| resolve_cell_row_filter(schema, session, filter))
        .transpose()?;
    let mut pk = BTreeMap::new();
    for column in &schema.primary_key.columns {
        let value = selection
            .args
            .get(column)
            .ok_or_else(|| format!("missing primary key argument `{column}`"))?;
        let key = match value {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            other => {
                return Err(CompileError::UnsupportedOnCellStore(format!(
                    "cell-by-key primary key `{column}` must be a scalar, got {other:?}"
                )));
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
) -> Result<FilterExpr, CompileError> {
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
                    return Err(CompileError::UnsupportedOnCellStore(format!(
                        "cell-by-key row policy operator `{op:?}` is unsupported for column `{column}`"
                    )));
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
                return Err(CompileError::UnsupportedOnCellStore(format!(
                    "cell-by-key row policy IN is unsupported for column `{column}`"
                )));
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
            return Err(CompileError::UnsupportedOnCellStore(format!(
                "cell-by-key row policy cannot traverse relationship `{field}`"
            )));
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
) -> Result<Operand, CompileError> {
    let literal = match operand_to_bind(operand, session, column_type)? {
        BindValue::Null => LitValue::Null,
        BindValue::Bool(value) => LitValue::Bool(value),
        BindValue::I64(value) => LitValue::I64(value),
        BindValue::F64(value) if value.is_finite() => LitValue::F64(value),
        BindValue::F64(value) => {
            return Err(CompileError::UnsupportedOnCellStore(format!(
                "cell-by-key row-policy float `{value}` must be finite"
            )));
        }
        BindValue::Text(value) => LitValue::String(value),
        BindValue::Json(value) => LitValue::Json(value),
        BindValue::Bytes(_) => {
            return Err(CompileError::UnsupportedOnCellStore(
                "cell-by-key row policies do not support byte operands".into(),
            ));
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
    // `resolve_cell_row_filter` already replaced every claim with a literal.
    filter.evaluate(schema, row, &|_, _| None) == Truth::True
}

/// Compile a root field selection into one SQL statement.
///
/// Refuses a statement that could produce more than `max_rows` rows: see
/// [`Rows`] for how they are counted.
pub fn compile_root(
    inner: &EngineInner,
    session: &Session,
    role: &str,
    model_name: &str,
    kind: RootKind,
    selection: &SelectionNode,
) -> Result<SqlPlan, CompileError> {
    let scan = ModelScan::open(inner, session, role, model_name)?;
    let mut binds = Vec::new();
    let mut bytes_paths = Vec::new();
    let mut tables = vec![scan.schema.table_name.clone()];
    let alias = "t0";
    let root = ScanLink::default();

    let (sql, evidence_root, rows) = match kind {
        RootKind::List => {
            let (page, evidence, rows) = compile_page(
                scan,
                selection,
                &selection.args,
                alias,
                &root,
                &mut binds,
                &mut bytes_paths,
                &mut tables,
                "",
                0,
            )?;
            (
                page,
                QueryEvidenceNode::List(Box::new(QueryEvidenceNode::Object(evidence))),
                rows,
            )
        }
        RootKind::ByPk => {
            // Binds are positional: the projection's (nested LIMIT/OFFSET)
            // come first in the statement text, then the key, then the WHERE.
            let (projection, evidence, nested) = compile_object_projection(
                scan,
                selection,
                alias,
                &mut binds,
                &mut bytes_paths,
                &mut tables,
                "",
                0,
            )?;
            let mut key = Vec::new();
            for pk in &scan.schema.primary_key.columns {
                let value = selection
                    .args
                    .get(pk)
                    .ok_or_else(|| format!("missing primary key argument `{pk}`"))?;
                let column = scan
                    .schema
                    .columns
                    .iter()
                    .find(|c| c.column_name == *pk)
                    .ok_or_else(|| CompileError::internal(format!("pk column `{pk}` missing")))?;
                binds.push(value_to_bind(value, &column.column_type)?);
                let ph = placeholder(inner.dialect, binds.len());
                key.push(format!("{alias}.\"{pk}\" = {ph}"));
            }
            let one_row = ScanLink {
                join: String::new(),
                on: format!("({})", key.join(" AND ")),
            };
            let rows = scan.visible_rows(
                alias,
                &one_row,
                selection.args.get("where"),
                &mut binds,
                &mut tables,
                0,
            )?;
            (
                format!("SELECT {projection} {rows} LIMIT 1"),
                QueryEvidenceNode::Object(evidence),
                Rows::one(nested),
            )
        }
        RootKind::Aggregate => {
            let (object, evidence, rows) = compile_aggregate(
                scan,
                selection,
                alias,
                &root,
                &mut binds,
                &mut bytes_paths,
                &mut tables,
                "",
                0,
            )?;
            (format!("SELECT {object}"), evidence, rows)
        }
    };
    if rows.0 > inner.max_rows {
        return Err(CompileError::TooManyRows {
            rows: rows.0,
            max_rows: inner.max_rows,
        });
    }
    let evidence = QueryEvidencePlan::new(selection.response_key.clone(), evidence_root)
        .map_err(CompileError::Internal)?;

    Ok(SqlPlan {
        sql: sql.into(),
        binds,
        bytes_hex_paths: bytes_paths,
        tables_touched: tables,
        evidence,
    })
}

/// The most model rows one evaluation of a compiled expression can produce:
/// the query cost the engine bounds by `max_rows`, counted with
/// [`row_cost`](crate::graphql::row_cost) from each list's resolved limit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Rows(u64);

impl Rows {
    /// A page of up to `limit` rows, each carrying `nested`.
    fn page(limit: u64, nested: Rows) -> Rows {
        Rows(crate::graphql::row_cost::page(limit, nested.0))
    }

    /// One row carrying `nested`.
    pub(super) fn one(nested: Rows) -> Rows {
        Rows(crate::graphql::row_cost::one(nested.0))
    }

    fn add(self, other: Rows) -> Rows {
        Rows(self.0.saturating_add(other.0))
    }
}

#[derive(Clone, Copy)]
pub enum RootKind {
    List,
    ByPk,
    Aggregate,
}

pub(super) fn validate_response_key(key: &str) -> Result<(), CompileError> {
    if is_valid_graphql_name(key) {
        Ok(())
    } else {
        Err(CompileError::InvalidResponseKey(format!(
            "invalid GraphQL response key `{key}`"
        )))
    }
}

/// One page of a scan as a JSON array: `fields` projected from the rows the
/// privilege may read, filtered, ordered and limited by `args`.
pub(super) fn compile_page(
    scan: ModelScan<'_>,
    fields: &SelectionNode,
    args: &BTreeMap<String, Value>,
    alias: &str,
    link: &ScanLink,
    binds: &mut Vec<BindValue>,
    bytes_paths: &mut Vec<String>,
    tables: &mut Vec<String>,
    path_prefix: &str,
    depth: usize,
) -> Result<(String, QueryEvidenceObjectPlan, Rows), CompileError> {
    let dialect = scan.inner.dialect;
    let ops = dialect.ops();
    let order_sql = scan.order_by(args.get("order_by"), alias)?;
    let (projection, evidence, nested) = compile_object_projection(
        scan,
        fields,
        alias,
        binds,
        bytes_paths,
        tables,
        path_prefix,
        depth,
    )?;
    let rows = scan.visible_rows(alias, link, args.get("where"), binds, tables, depth)?;
    let limit = scan.limit(args.get("limit"));
    binds.push(BindValue::I64(i64::try_from(limit).unwrap_or(i64::MAX)));
    let limit_ph = placeholder(dialect, binds.len());
    binds.push(BindValue::I64(
        args.get("offset").and_then(value_as_u64).unwrap_or(0) as i64,
    ));
    let offset = placeholder(dialect, binds.len());
    let row = match ops.json_cast_fn {
        None => "r".to_string(),
        Some(cast) => format!("{cast}(r)"),
    };
    Ok((
        format!(
            "SELECT coalesce({}({row}), {}) FROM (\n  SELECT {projection} AS r\n  {rows}\n  {order_sql}\n  LIMIT {limit_ph} OFFSET {offset}\n) page",
            ops.json_agg, ops.empty_array
        ),
        evidence,
        Rows::page(limit, nested),
    ))
}

/// `{ aggregate { count } nodes { … } }` over a scan, as a JSON object.
pub(super) fn compile_aggregate(
    scan: ModelScan<'_>,
    selection: &SelectionNode,
    alias: &str,
    link: &ScanLink,
    binds: &mut Vec<BindValue>,
    bytes_paths: &mut Vec<String>,
    tables: &mut Vec<String>,
    path_prefix: &str,
    depth: usize,
) -> Result<(String, QueryEvidenceNode, Rows), CompileError> {
    if !scan.aggregations() {
        return Err(CompileError::internal(format!(
            "aggregate reached `{}` without an aggregations grant",
            scan.schema.model_name
        )));
    }
    let dialect = scan.inner.dialect;
    let mut pairs = Vec::new();
    let mut evidence_fields = Vec::new();
    let mut rows = Rows::default();
    for member in &selection.children {
        match member.field_name.as_str() {
            "__typename" => {}
            "aggregate" => {
                validate_response_key(&member.response_key)?;
                let mut metrics = Vec::new();
                for metric in &member.children {
                    match metric.field_name.as_str() {
                        "__typename" => {}
                        "count" => {
                            validate_response_key(&metric.response_key)?;
                            let rows = scan.visible_rows(
                                alias,
                                link,
                                selection.args.get("where"),
                                binds,
                                tables,
                                depth,
                            )?;
                            metrics.push((
                                metric.response_key.clone(),
                                format!("(SELECT count(*) {rows})"),
                            ));
                        }
                        _ => {
                            return Err(CompileError::BadRequest(
                                "aggregate fields selection contains an unsupported member".into(),
                            ));
                        }
                    }
                }
                pairs.push((
                    member.response_key.clone(),
                    chunked_json_object(dialect, &metrics),
                ));
            }
            "nodes" => {
                validate_response_key(&member.response_key)?;
                let nodes_path = if path_prefix.is_empty() {
                    member.response_key.clone()
                } else {
                    format!("{path_prefix}.{}", member.response_key)
                };
                let (page, evidence, page_rows) = compile_page(
                    scan,
                    member,
                    &selection.args,
                    alias,
                    link,
                    binds,
                    bytes_paths,
                    tables,
                    &nodes_path,
                    depth,
                )?;
                rows = rows.add(page_rows);
                pairs.push((
                    member.response_key.clone(),
                    dialect.ops().nested_json(format!("({page})")),
                ));
                evidence_fields.push(QueryEvidenceFieldPlan {
                    storage_key: member.response_key.clone(),
                    response_key: member.response_key.clone(),
                    node: Box::new(QueryEvidenceNode::List(Box::new(
                        QueryEvidenceNode::Object(evidence),
                    ))),
                });
            }
            _ => {
                return Err(CompileError::BadRequest(
                    "aggregate selection contains an unsupported member".into(),
                ));
            }
        }
    }
    Ok((
        chunked_json_object(dialect, &pairs),
        QueryEvidenceNode::Object(QueryEvidenceObjectPlan {
            record: None,
            fields: evidence_fields,
        }),
        rows,
    ))
}

/// The JSON object for one row of `scan`: its selected columns and nested
/// relationships, plus the hidden identity the causal evidence needs. Also
/// returns the rows nested under that one row.
pub(super) fn compile_object_projection(
    scan: ModelScan<'_>,
    selection: &SelectionNode,
    alias: &str,
    binds: &mut Vec<BindValue>,
    bytes_paths: &mut Vec<String>,
    tables: &mut Vec<String>,
    path_prefix: &str,
    depth: usize,
) -> Result<(String, QueryEvidenceObjectPlan, Rows), CompileError> {
    let (inner, schema) = (scan.inner, scan.schema);
    if depth > inner.max_depth {
        return Err(CompileError::TooDeep);
    }
    let (mut pairs, record) =
        compile_record_evidence_projection(scan, alias, bytes_paths, path_prefix)?;
    let mut evidence_fields = Vec::new();
    let mut nested = Rows::default();
    let path = |response_key: &str| {
        if path_prefix.is_empty() {
            response_key.to_string()
        } else {
            format!("{path_prefix}.{response_key}")
        }
    };
    let column_pair = |column: &crate::table::TableColumn,
                       response_key: &str,
                       bytes_paths: &mut Vec<String>|
     -> Result<(String, String), CompileError> {
        let expr = column_json_expr(inner.dialect, alias, column);
        if matches!(column.column_type, ColumnType::Bytes)
            && matches!(inner.dialect, SqlDialect::Sqlite)
        {
            bytes_paths.push(path(response_key));
        }
        validate_response_key(response_key)?;
        Ok((response_key.to_string(), expr))
    };

    // No selection set: every column the privilege may read.
    if selection.children.is_empty() {
        for column in schema.columns.iter().filter(|c| !c.skipped) {
            if scan.allows_column(&column.column_name) {
                pairs.push(column_pair(column, &column.column_name, bytes_paths)?);
            }
        }
    }
    for child in &selection.children {
        if child.field_name == "__typename" {
            continue;
        }
        if let Some(column) = schema
            .columns
            .iter()
            .find(|c| c.column_name == child.field_name && !c.skipped)
        {
            // The per-privilege schema never offers an ungranted column.
            if !scan.allows_column(&column.column_name) {
                return Err(CompileError::internal(format!(
                    "selection reached ungranted column `{}` on `{}`",
                    column.column_name, schema.model_name
                )));
            }
            pairs.push(column_pair(column, &child.response_key, bytes_paths)?);
            continue;
        }
        let relationship = |name: &str| schema.relationships.iter().find(|r| r.field_name == name);
        let (sub, evidence, rows) = if let Some(rel) = relationship(&child.field_name) {
            compile_relationship_subquery(
                scan,
                alias,
                rel,
                child,
                binds,
                bytes_paths,
                tables,
                &path(&child.response_key),
                depth + 1,
            )?
        } else if let Some(rel) = child
            .field_name
            .strip_suffix("_aggregate")
            .and_then(relationship)
        {
            compile_relationship_aggregate_subquery(
                scan,
                alias,
                rel,
                child,
                binds,
                bytes_paths,
                tables,
                &path(&child.response_key),
                depth + 1,
            )?
        } else {
            return Err(CompileError::internal(format!(
                "selection reached unknown field `{}` on `{}`",
                child.field_name, schema.model_name
            )));
        };
        validate_response_key(&child.response_key)?;
        nested = nested.add(rows);
        pairs.push((
            child.response_key.clone(),
            inner.dialect.ops().nested_json(sub),
        ));
        evidence_fields.push(QueryEvidenceFieldPlan {
            storage_key: child.response_key.clone(),
            response_key: child.response_key.clone(),
            node: Box::new(evidence),
        });
    }

    Ok((
        chunked_json_object(inner.dialect, &pairs),
        QueryEvidenceObjectPlan {
            record,
            fields: evidence_fields,
        },
        nested,
    ))
}

pub(super) fn compile_record_evidence_projection(
    scan: ModelScan<'_>,
    alias: &str,
    bytes_paths: &mut Vec<String>,
    path_prefix: &str,
) -> Result<RecordEvidenceProjection, CompileError> {
    // Evidence is read only by the causal protocol: without it, the hidden
    // identity columns would be selected only to be stripped again.
    // Embedded client models deliberately have no stable normalized identity.
    // Do not manufacture per-record evidence that the client cannot address;
    // table/projector dependencies still flow through `tables_touched` and
    // produce conservative index evidence.
    if scan.inner.protocol.is_none() || !scan.has_client_normalized_identity() {
        return Ok((Vec::new(), None));
    }
    record_identity_projection(
        scan.inner.dialect,
        scan.schema,
        alias,
        bytes_paths,
        path_prefix,
    )
    .map_err(CompileError::Internal)
}

/// The hidden primary-key copy that names one row in causal evidence.
pub(super) fn record_identity_projection(
    dialect: SqlDialect,
    schema: &TableSchema,
    alias: &str,
    bytes_paths: &mut Vec<String>,
    path_prefix: &str,
) -> Result<RecordEvidenceProjection, String> {
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
            _ => column_json_expr(dialect, alias, column),
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

pub(super) fn column_json_expr(
    dialect: SqlDialect,
    alias: &str,
    col: &crate::table::TableColumn,
) -> String {
    let q = format!("{alias}.\"{}\"", col.column_name);
    match (&col.column_type, dialect) {
        (ColumnType::Timestamp, SqlDialect::Postgres) => format!("{q}::text"),
        (ColumnType::Bytes, SqlDialect::Postgres) => format!("encode({q}, 'base64')"),
        (ColumnType::Bytes, SqlDialect::Sqlite) => format!("hex({q})"),
        // SQLite stores JSON as text; `json()` gives the value its JSON subtype
        // so the enclosing object embeds it as JSON, not as a string.
        (ColumnType::Json, SqlDialect::Sqlite) => format!("json({q})"),
        _ => q,
    }
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

#[cfg(test)]
mod security_tests {
    use super::super::scan::resolve_limit;
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
