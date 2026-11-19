use async_graphql::Value;

use crate::graphql::filter::{CmpOp, FilterExpr};
use crate::microsvc::Session;
use crate::table::{ColumnType, TableSchema};

use super::super::engine::EngineInner;
use super::binds::{operand_to_bind, value_to_bind, BindValue};
use super::dialect::{comparison_operand, placeholder, text_pattern, SqlDialect};
use super::scan::{relationship_link, ModelScan, ScanLink};

impl ModelScan<'_> {
    /// `FROM "table" alias [JOIN …] WHERE [link AND] (row policy AND where)`.
    ///
    /// Every `SELECT` over a model is built from this, so the row policy is
    /// applied on every access path. Binds are pushed here: call it where the
    /// clause appears in the statement text.
    pub(super) fn visible_rows(
        &self,
        alias: &str,
        link: &ScanLink,
        client_where: Option<&Value>,
        binds: &mut Vec<BindValue>,
        tables: &mut Vec<String>,
        depth: usize,
    ) -> Result<String, String> {
        if depth > self.inner.max_depth {
            return Err("max depth exceeded".into());
        }
        let mut preds = Vec::new();
        if let Some(policy) = self.grant.row_policy.filter() {
            preds.push(compile_filter_expr(
                self.inner,
                self.session,
                self.schema,
                policy,
                alias,
                binds,
                tables,
                depth,
            )?);
        }
        if let Some(client_where) = client_where {
            preds.push(compile_client_where(
                *self,
                client_where,
                alias,
                binds,
                tables,
                depth,
            )?);
        }
        let visible = if preds.is_empty() {
            "TRUE".to_string()
        } else {
            preds.join(" AND ")
        };
        let on = if link.on.is_empty() {
            String::new()
        } else {
            format!("{} AND ", link.on)
        };
        Ok(format!(
            "FROM \"{}\" {alias}{} WHERE {on}({visible})",
            self.schema.table_name, link.join
        ))
    }
}

fn compile_filter_expr(
    inner: &EngineInner,
    session: &Session,
    schema: &TableSchema,
    expr: &FilterExpr,
    alias: &str,
    binds: &mut Vec<BindValue>,
    tables: &mut Vec<String>,
    depth: usize,
) -> Result<String, String> {
    match expr {
        FilterExpr::And(xs) => {
            if xs.is_empty() {
                return Ok("TRUE".into());
            }
            let parts: Result<Vec<_>, _> = xs
                .iter()
                .map(|x| {
                    compile_filter_expr(inner, session, schema, x, alias, binds, tables, depth + 1)
                })
                .collect();
            Ok(format!("({})", parts?.join(" AND ")))
        }
        FilterExpr::Or(xs) => {
            if xs.is_empty() {
                return Ok("FALSE".into());
            }
            let parts: Result<Vec<_>, _> = xs
                .iter()
                .map(|x| {
                    compile_filter_expr(inner, session, schema, x, alias, binds, tables, depth + 1)
                })
                .collect();
            Ok(format!("({})", parts?.join(" OR ")))
        }
        FilterExpr::Not(x) => Ok(format!(
            "NOT ({})",
            compile_filter_expr(inner, session, schema, x, alias, binds, tables, depth + 1)?
        )),
        FilterExpr::Cmp { column, op, rhs } => {
            let col = schema
                .columns
                .iter()
                .find(|c| c.column_name == *column)
                .ok_or_else(|| format!("unknown column `{column}`"))?;
            let bind = operand_to_bind(rhs, session, &col.column_type)?;
            let col_ref = format!("{alias}.\"{column}\"");
            if matches!(op, CmpOp::Like | CmpOp::Ilike) {
                let BindValue::Text(pattern) = bind else {
                    return Err("a pattern comparison requires a string".into());
                };
                return compile_text_pattern(
                    inner.dialect,
                    &col_ref,
                    &pattern,
                    '\\',
                    *op == CmpOp::Like,
                    binds,
                );
            }
            let ph = comparand(inner.dialect, bind, binds);
            let sql_op = match op {
                CmpOp::Eq => "=",
                CmpOp::Neq => "<>",
                CmpOp::Gt => ">",
                CmpOp::Gte => ">=",
                CmpOp::Lt => "<",
                CmpOp::Lte => "<=",
                CmpOp::Like | CmpOp::Ilike => unreachable!("compiled above"),
                CmpOp::Contains => {
                    require_postgres_json_op(inner.dialect, "_contains")?;
                    "@>"
                }
                CmpOp::ContainedIn => {
                    require_postgres_json_op(inner.dialect, "_contained_in")?;
                    "<@"
                }
                CmpOp::HasKey => {
                    require_postgres_json_op(inner.dialect, "_has_key")?;
                    "?"
                }
            };
            let cast_ph = cast_placeholder(inner.dialect, &col.column_type, &ph);
            let col_ref = match op {
                CmpOp::Eq | CmpOp::Neq => {
                    comparison_operand(inner.dialect, &col.column_type, &col_ref, false)
                }
                CmpOp::Gt | CmpOp::Gte | CmpOp::Lt | CmpOp::Lte => {
                    comparison_operand(inner.dialect, &col.column_type, &col_ref, true)
                }
                _ => col_ref,
            };
            Ok(format!("{col_ref} {sql_op} {cast_ph}"))
        }
        FilterExpr::In {
            column,
            values,
            negated,
        } => {
            if values.is_empty() {
                return Ok(if *negated {
                    "TRUE".into()
                } else {
                    "FALSE".into()
                });
            }
            if values.len() > inner.max_in_list {
                return Err(format!(
                    "_in list length {} exceeds max_in_list {}",
                    values.len(),
                    inner.max_in_list
                ));
            }
            let col = schema
                .columns
                .iter()
                .find(|c| c.column_name == *column)
                .ok_or_else(|| format!("unknown column `{column}`"))?;
            let col_ref = format!("{alias}.\"{column}\"");
            let col_ref = comparison_operand(inner.dialect, &col.column_type, &col_ref, false);
            let mut phs = Vec::new();
            for v in values {
                let bind = operand_to_bind(v, session, &col.column_type)?;
                phs.push(comparand(inner.dialect, bind, binds));
            }
            let op = if *negated { "NOT IN" } else { "IN" };
            Ok(format!("{col_ref} {op} ({})", phs.join(", ")))
        }
        FilterExpr::IsNull { column, is_null } => {
            let col_ref = format!("{alias}.\"{column}\"");
            Ok(if *is_null {
                format!("{col_ref} IS NULL")
            } else {
                format!("{col_ref} IS NOT NULL")
            })
        }
        FilterExpr::Rel { field, predicate } => {
            let rel = schema
                .relationships
                .iter()
                .find(|r| r.field_name == *field)
                .ok_or_else(|| format!("unknown relationship `{field}`"))?;
            // A row policy is authored, trusted server logic: it reads the
            // related model directly, not as the requesting privilege does.
            let target = inner
                .catalog
                .get(&rel.target_model)
                .ok_or_else(|| format!("rel target `{}` not in catalog", rel.target_model))?;
            let child_alias = format!("r{depth}");
            let inner_pred = compile_filter_expr(
                inner,
                session,
                &target.schema,
                predicate,
                &child_alias,
                binds,
                tables,
                depth + 1,
            )?;
            let link = relationship_link(
                inner,
                schema,
                rel,
                &target.schema,
                alias,
                &child_alias,
                &format!("j{depth}"),
                tables,
            )?;
            Ok(format!(
                "EXISTS (SELECT 1 FROM \"{}\" {child_alias}{} WHERE {} AND ({inner_pred}))",
                target.schema.table_name, link.join, link.on
            ))
        }
    }
}

fn compile_client_where(
    scan: ModelScan<'_>,
    value: &Value,
    alias: &str,
    binds: &mut Vec<BindValue>,
    tables: &mut Vec<String>,
    depth: usize,
) -> Result<String, String> {
    let (inner, session, schema) = (scan.inner, scan.session, scan.schema);
    if depth > inner.max_depth {
        return Err("max depth exceeded".into());
    }
    let Value::Object(map) = value else {
        return Ok("TRUE".into());
    };
    let mut preds = Vec::new();
    for (key, val) in map {
        match key.as_str() {
            "_and" => {
                if let Value::List(items) = val {
                    if items.len() > inner.max_bool_width {
                        return Err(format!(
                            "_and list length {} exceeds max_bool_width {}",
                            items.len(),
                            inner.max_bool_width
                        ));
                    }
                    for item in items {
                        preds.push(compile_client_where(
                            scan,
                            item,
                            alias,
                            binds,
                            tables,
                            depth + 1,
                        )?);
                    }
                }
            }
            "_or" => {
                if let Value::List(items) = val {
                    if items.len() > inner.max_bool_width {
                        return Err(format!(
                            "_or list length {} exceeds max_bool_width {}",
                            items.len(),
                            inner.max_bool_width
                        ));
                    }
                    let mut parts = Vec::new();
                    for item in items {
                        parts.push(compile_client_where(
                            scan,
                            item,
                            alias,
                            binds,
                            tables,
                            depth + 1,
                        )?);
                    }
                    if !parts.is_empty() {
                        preds.push(format!("({})", parts.join(" OR ")));
                    }
                }
            }
            "_not" => {
                preds.push(format!(
                    "NOT ({})",
                    compile_client_where(scan, val, alias, binds, tables, depth + 1,)?
                ));
            }
            col_name => {
                if let Some(col) = schema.columns.iter().find(|c| c.column_name == *col_name) {
                    if !scan.allows_column(col_name) {
                        if inner.strict_where {
                            return Err(format!("ungranted where column `{col_name}`"));
                        }
                        continue;
                    }
                    if let Value::Object(ops) = val {
                        for (op, rhs) in ops {
                            preds.push(compile_client_op(
                                inner,
                                session,
                                col_name,
                                &col.column_type,
                                op,
                                rhs,
                                alias,
                                binds,
                            )?);
                        }
                    }
                } else if let Some(rel) = schema
                    .relationships
                    .iter()
                    .find(|r| r.field_name == *col_name)
                {
                    // Relationship predicate → EXISTS over the target as this
                    // privilege reads it, so the target's row policy cannot be
                    // bypassed by a source-model predicate.
                    let Some(target) = scan.target(rel) else {
                        if inner.strict_where {
                            return Err(format!("ungranted where relationship `{col_name}`"));
                        }
                        continue;
                    };
                    let child_alias = format!("cw{depth}");
                    let link = relationship_link(
                        inner,
                        schema,
                        rel,
                        target.schema,
                        alias,
                        &child_alias,
                        &format!("cwj{depth}"),
                        tables,
                    )?;
                    preds.push(format!(
                        "EXISTS (SELECT 1 {})",
                        target.visible_rows(
                            &child_alias,
                            &link,
                            Some(val),
                            binds,
                            tables,
                            depth + 1
                        )?
                    ));
                } else if inner.strict_where {
                    return Err(format!("unknown where field `{col_name}`"));
                }
                // soft-skip: ignore unknown keys when strict_where is false
            }
        }
    }
    if preds.is_empty() {
        Ok("TRUE".into())
    } else {
        Ok(format!("({})", preds.join(" AND ")))
    }
}

fn compile_client_op(
    inner: &EngineInner,
    _session: &Session,
    column: &str,
    column_type: &ColumnType,
    op: &str,
    rhs: &Value,
    alias: &str,
    binds: &mut Vec<BindValue>,
) -> Result<String, String> {
    let col_ref = format!("{alias}.\"{column}\"");
    match op {
        "_icontains" => {
            let Value::String(text) = rhs else {
                return Err("_icontains requires a string".into());
            };
            // Use an explicit escape character on both dialects. The operand
            // stays a bind parameter; user text never becomes SQL or a wildcard.
            let mut pattern = String::from("%");
            for ch in text.chars() {
                if matches!(ch, '!' | '%' | '_') {
                    pattern.push('!');
                }
                pattern.push(ch);
            }
            pattern.push('%');
            compile_text_pattern(inner.dialect, &col_ref, &pattern, '!', false, binds)
        }
        "_like" | "_ilike" => {
            let Value::String(pattern) = rhs else {
                return Err(format!("{op} requires a string"));
            };
            compile_text_pattern(inner.dialect, &col_ref, pattern, '\\', op == "_like", binds)
        }
        "_is_null" => {
            let yes = matches!(rhs, Value::Boolean(true));
            Ok(if yes {
                format!("{col_ref} IS NULL")
            } else {
                format!("{col_ref} IS NOT NULL")
            })
        }
        "_in" | "_nin" => {
            let Value::List(items) = rhs else {
                return Err(format!("{op} requires a list"));
            };
            if items.is_empty() {
                return Ok(if op == "_in" {
                    "FALSE".into()
                } else {
                    "TRUE".into()
                });
            }
            if items.len() > inner.max_in_list {
                return Err(format!(
                    "{op} list length {} exceeds max_in_list {}",
                    items.len(),
                    inner.max_in_list
                ));
            }
            let mut phs = Vec::new();
            for item in items {
                phs.push(comparand(
                    inner.dialect,
                    value_to_bind(item, column_type)?,
                    binds,
                ));
            }
            let sql_op = if op == "_in" { "IN" } else { "NOT IN" };
            let col_ref = comparison_operand(inner.dialect, column_type, &col_ref, false);
            Ok(format!("{col_ref} {sql_op} ({})", phs.join(", ")))
        }
        other => {
            let sql_op = match other {
                "_eq" => "=",
                "_neq" => "<>",
                "_gt" => ">",
                "_gte" => ">=",
                "_lt" => "<",
                "_lte" => "<=",
                "_contains" => {
                    require_postgres_json_op(inner.dialect, "_contains")?;
                    "@>"
                }
                "_contained_in" => {
                    require_postgres_json_op(inner.dialect, "_contained_in")?;
                    "<@"
                }
                "_has_key" => {
                    require_postgres_json_op(inner.dialect, "_has_key")?;
                    "?"
                }
                _ => return Err(format!("unknown comparison op `{other}`")),
            };
            let ph = comparand(inner.dialect, value_to_bind(rhs, column_type)?, binds);
            let cast_ph = cast_placeholder(inner.dialect, column_type, &ph);
            let col_ref = match other {
                "_eq" | "_neq" => comparison_operand(inner.dialect, column_type, &col_ref, false),
                "_gt" | "_gte" | "_lt" | "_lte" => {
                    comparison_operand(inner.dialect, column_type, &col_ref, true)
                }
                _ => col_ref,
            };
            Ok(format!("{col_ref} {sql_op} {cast_ph}"))
        }
    }
}

/// Postgres jsonb operators are not emitted on SQLite (would confuse the driver
/// and risk opaque execute errors). Message must stay free of SQL fragments so
/// [`super::schema::sanitize_compile_error`] maps to a stable client code.
fn require_postgres_json_op(dialect: SqlDialect, op: &str) -> Result<(), String> {
    match dialect {
        SqlDialect::Postgres => Ok(()),
        SqlDialect::Sqlite => Err(format!(
            "unknown comparison op `{op}` is not supported on sqlite"
        )),
    }
}

/// Push the pattern bind and return the predicate; see [`text_pattern`].
fn compile_text_pattern(
    dialect: SqlDialect,
    col_ref: &str,
    pattern: &str,
    escape: char,
    case_sensitive: bool,
    binds: &mut Vec<BindValue>,
) -> Result<String, String> {
    let ph = placeholder(dialect, binds.len() + 1);
    let (sql, bind) = text_pattern(dialect, col_ref, &ph, pattern, escape, case_sensitive)?;
    binds.push(BindValue::Text(bind));
    Ok(sql)
}

/// The right side of a comparison: a placeholder for `bind`, or `NULL`.
///
/// A NULL is written into the statement rather than bound. A bound NULL has
/// to be given some type, and Postgres then rejects it against a column of
/// any other type instead of answering unknown as SQLite does.
fn comparand(dialect: SqlDialect, bind: BindValue, binds: &mut Vec<BindValue>) -> String {
    if matches!(bind, BindValue::Null) {
        return "NULL".into();
    }
    binds.push(bind);
    placeholder(dialect, binds.len())
}

fn cast_placeholder(dialect: SqlDialect, column_type: &ColumnType, ph: &str) -> String {
    match (dialect, column_type) {
        (SqlDialect::Postgres, ColumnType::Timestamp) => format!("{ph}::timestamptz"),
        (SqlDialect::Postgres, ColumnType::Json) => format!("{ph}::jsonb"),
        _ => ph.to_string(),
    }
}
