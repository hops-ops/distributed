use async_graphql::Value;

use crate::graphql::filter::{CmpOp, FilterExpr};
use crate::microsvc::Session;
use crate::table::{resolve_m2m_join_keys, ColumnType, RelationshipKind, TableSchema};

use super::super::engine::EngineInner;
use super::super::permissions::ReadPermission;
use super::binds::{operand_to_bind, value_to_bind, BindValue};
use super::dialect::{join_predicate_m2m_pairs, placeholder, SqlDialect};
use super::relationship::{compile_belongs_to_join, compile_has_many_join};

pub(super) fn compile_where(
    inner: &EngineInner,
    session: &Session,
    role: &str,
    schema: &TableSchema,
    perm: &ReadPermission,
    client_where: Option<&Value>,
    alias: &str,
    binds: &mut Vec<BindValue>,
    tables: &mut Vec<String>,
    depth: usize,
) -> Result<String, String> {
    if depth > inner.max_depth {
        return Err("max depth exceeded".into());
    }
    let mut preds = Vec::new();
    if let Some(filter) = &perm.row_filter {
        preds.push(compile_filter_expr(
            inner, session, schema, filter, alias, binds, tables, depth,
        )?);
    }
    if let Some(w) = client_where {
        preds.push(compile_client_where(
            inner, session, role, schema, perm, w, alias, binds, tables, depth,
        )?);
    }
    if preds.is_empty() {
        Ok("TRUE".into())
    } else {
        Ok(preds.join(" AND "))
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
            binds.push(bind);
            let ph = placeholder(inner.dialect, binds.len());
            let col_ref = format!("{alias}.\"{column}\"");
            let sql_op = match op {
                CmpOp::Eq => "=",
                CmpOp::Neq => "<>",
                CmpOp::Gt => ">",
                CmpOp::Gte => ">=",
                CmpOp::Lt => "<",
                CmpOp::Lte => "<=",
                CmpOp::Like => "LIKE",
                CmpOp::Ilike => inner.dialect.ops().ilike_op,
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
            match inner.dialect {
                SqlDialect::Postgres => {
                    // Expand as IN for simplicity (array bind is dialect-specific).
                    let mut phs = Vec::new();
                    for v in values {
                        let bind = operand_to_bind(v, session, &col.column_type)?;
                        binds.push(bind);
                        phs.push(placeholder(inner.dialect, binds.len()));
                    }
                    let op = if *negated { "NOT IN" } else { "IN" };
                    Ok(format!("{col_ref} {op} ({})", phs.join(", ")))
                }
                SqlDialect::Sqlite => {
                    let mut phs = Vec::new();
                    for v in values {
                        let bind = operand_to_bind(v, session, &col.column_type)?;
                        binds.push(bind);
                        phs.push(placeholder(inner.dialect, binds.len()));
                    }
                    let op = if *negated { "NOT IN" } else { "IN" };
                    Ok(format!("{col_ref} {op} ({})", phs.join(", ")))
                }
            }
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
            let target = inner
                .catalog
                .get(&rel.target_model)
                .ok_or_else(|| format!("rel target `{}` not in catalog", rel.target_model))?;
            tables.push(target.schema.table_name.clone());
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
            match rel.kind {
                RelationshipKind::HasMany => {
                    let join_pred =
                        compile_has_many_join(schema, rel, &target.schema, alias, &child_alias)?;
                    Ok(format!(
                        "EXISTS (SELECT 1 FROM \"{}\" {child_alias} WHERE {join_pred} AND ({inner_pred}))",
                        target.schema.table_name
                    ))
                }
                RelationshipKind::BelongsTo => {
                    let join_pred =
                        compile_belongs_to_join(schema, rel, &target.schema, alias, &child_alias)?;
                    Ok(format!(
                        "EXISTS (SELECT 1 FROM \"{}\" {child_alias} WHERE {join_pred} AND ({inner_pred}))",
                        target.schema.table_name
                    ))
                }
                RelationshipKind::ManyToMany => {
                    let through = rel
                        .through
                        .as_deref()
                        .ok_or_else(|| "m2m rel missing through".to_string())?;
                    let through_entry = inner
                        .by_table
                        .get(through)
                        .and_then(|m| inner.catalog.get(m))
                        .ok_or_else(|| format!("through `{through}` missing"))?;
                    let join =
                        resolve_m2m_join_keys(schema, rel, &through_entry.schema, &target.schema)
                            .map_err(|error| error.to_string())?;
                    let j = format!("j{depth}");
                    let on_target = join_predicate_m2m_pairs(&j, &child_alias, &join.target)?;
                    let parent_pred = join_predicate_m2m_pairs(&j, alias, &join.parent)?;
                    Ok(format!(
                        "EXISTS (SELECT 1 FROM \"{through}\" {j} JOIN \"{}\" {child_alias} ON {on_target} WHERE {parent_pred} AND ({inner_pred}))",
                        target.schema.table_name
                    ))
                }
            }
        }
    }
}

fn compile_client_where(
    inner: &EngineInner,
    session: &Session,
    role: &str,
    schema: &TableSchema,
    perm: &ReadPermission,
    value: &Value,
    alias: &str,
    binds: &mut Vec<BindValue>,
    tables: &mut Vec<String>,
    depth: usize,
) -> Result<String, String> {
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
                            inner,
                            session,
                            role,
                            schema,
                            perm,
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
                            inner,
                            session,
                            role,
                            schema,
                            perm,
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
                    compile_client_where(
                        inner,
                        session,
                        role,
                        schema,
                        perm,
                        val,
                        alias,
                        binds,
                        tables,
                        depth + 1,
                    )?
                ));
            }
            col_name => {
                if let Some(col) = schema.columns.iter().find(|c| c.column_name == *col_name) {
                    if !perm.allows_column(col_name) {
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
                    // Relationship predicate → EXISTS
                    let target = match inner.catalog.get(&rel.target_model) {
                        Some(t) => t,
                        None => {
                            if inner.strict_where {
                                return Err(format!(
                                    "unknown where field `{col_name}` (relationship target missing)"
                                ));
                            }
                            continue;
                        }
                    };
                    tables.push(target.schema.table_name.clone());
                    let target_perm = match inner
                        .permissions
                        .get(&(rel.target_model.clone(), role.to_string()))
                    {
                        Some(p) => &p.permission,
                        None => {
                            if inner.strict_where {
                                return Err(format!("ungranted where relationship `{col_name}`"));
                            }
                            continue;
                        }
                    };
                    let child_alias = format!("cw{depth}");
                    // Entering a relationship is a new target-model access path.
                    // Compile the complete target WHERE so its row policy cannot
                    // be bypassed by a source-model client predicate.
                    let inner_pred = compile_where(
                        inner,
                        session,
                        role,
                        &target.schema,
                        target_perm,
                        Some(val),
                        &child_alias,
                        binds,
                        tables,
                        depth + 1,
                    )?;
                    match rel.kind {
                        RelationshipKind::HasMany => {
                            let join_pred = compile_has_many_join(
                                schema,
                                rel,
                                &target.schema,
                                alias,
                                &child_alias,
                            )?;
                            preds.push(format!(
                                "EXISTS (SELECT 1 FROM \"{}\" {child_alias} WHERE {join_pred} AND ({inner_pred}))",
                                target.schema.table_name
                            ));
                        }
                        RelationshipKind::BelongsTo => {
                            let join_pred = compile_belongs_to_join(
                                schema,
                                rel,
                                &target.schema,
                                alias,
                                &child_alias,
                            )?;
                            preds.push(format!(
                                "EXISTS (SELECT 1 FROM \"{}\" {child_alias} WHERE {join_pred} AND ({inner_pred}))",
                                target.schema.table_name
                            ));
                        }
                        RelationshipKind::ManyToMany => {
                            let through = rel
                                .through
                                .as_deref()
                                .ok_or_else(|| "m2m rel missing through".to_string())?;
                            let through_entry = inner
                                .by_table
                                .get(through)
                                .and_then(|m| inner.catalog.get(m))
                                .ok_or_else(|| format!("through `{through}` missing"))?;
                            let join = resolve_m2m_join_keys(
                                schema,
                                rel,
                                &through_entry.schema,
                                &target.schema,
                            )
                            .map_err(|error| error.to_string())?;
                            let join_alias = format!("cwj{depth}");
                            tables.push(through.to_string());
                            let on_target =
                                join_predicate_m2m_pairs(&join_alias, &child_alias, &join.target)?;
                            let parent_pred =
                                join_predicate_m2m_pairs(&join_alias, alias, &join.parent)?;
                            preds.push(format!(
                                "EXISTS (SELECT 1 FROM \"{through}\" {join_alias} JOIN \"{}\" {child_alias} ON {on_target} WHERE {parent_pred} AND ({inner_pred}))",
                                target.schema.table_name
                            ));
                        }
                    }
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
            binds.push(value_to_bind(&Value::String(pattern), column_type)?);
            let ph = placeholder(inner.dialect, binds.len());
            let sql_op = inner.dialect.ops().ilike_op;
            Ok(format!("{col_ref} {sql_op} {ph} ESCAPE '!'"))
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
                binds.push(value_to_bind(item, column_type)?);
                phs.push(placeholder(inner.dialect, binds.len()));
            }
            let sql_op = if op == "_in" { "IN" } else { "NOT IN" };
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
                "_like" => "LIKE",
                "_ilike" => inner.dialect.ops().ilike_op,
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
            binds.push(value_to_bind(rhs, column_type)?);
            let ph = placeholder(inner.dialect, binds.len());
            let cast_ph = cast_placeholder(inner.dialect, column_type, &ph);
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

fn cast_placeholder(dialect: SqlDialect, column_type: &ColumnType, ph: &str) -> String {
    match (dialect, column_type) {
        (SqlDialect::Postgres, ColumnType::Timestamp) => format!("{ph}::timestamptz"),
        (SqlDialect::Postgres, ColumnType::Json) => format!("{ph}::jsonb"),
        _ => ph.to_string(),
    }
}
