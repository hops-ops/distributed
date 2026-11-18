use crate::microsvc::Session;
use crate::table::{
    resolve_direct_join_keys, resolve_m2m_join_keys, M2mJoinKeys, RelationshipKind, TableSchema,
};

use super::super::engine::{CatalogEntry, EngineInner};
use super::super::permissions::ReadPermission;
use super::binds::BindValue;
use super::dialect::{join_predicate_direct, join_predicate_m2m_pairs, placeholder};
use super::evidence::{QueryEvidenceFieldPlan, QueryEvidenceNode, QueryEvidenceObjectPlan};
use super::filter::compile_where;
use super::projection::{
    chunked_json_object, compile_object_projection, compile_order_by, resolve_limit,
    validate_response_key, value_as_u64, SelectionNode,
};

pub(super) fn compile_relationship_aggregate_subquery(
    inner: &EngineInner,
    session: &Session,
    role: &str,
    source: &TableSchema,
    source_alias: &str,
    rel: &crate::table::RelationshipDef,
    target: &CatalogEntry,
    target_perm: &ReadPermission,
    selection: &SelectionNode,
    binds: &mut Vec<BindValue>,
    bytes_paths: &mut Vec<String>,
    tables: &mut Vec<String>,
    path_prefix: &str,
    depth: usize,
) -> Result<(String, QueryEvidenceNode), String> {
    let child_alias = format!("ta{depth}");

    let (from_sql, join_pred) = match rel.kind {
        RelationshipKind::HasMany => (
            format!("\"{}\" {child_alias}", target.schema.table_name),
            compile_has_many_join(source, rel, &target.schema, source_alias, &child_alias)?,
        ),
        RelationshipKind::ManyToMany => {
            let through_name = rel
                .through
                .as_deref()
                .ok_or_else(|| "m2m missing through".to_string())?;
            let through_model = inner
                .by_table
                .get(through_name)
                .and_then(|m| inner.catalog.get(m))
                .ok_or_else(|| format!("through table `{through_name}` not in catalog"))?;
            let join = resolve_m2m_join_keys(source, rel, &through_model.schema, &target.schema)
                .map_err(|error| error.to_string())?;
            let join_alias = format!("ja{depth}");
            tables.push(through_name.to_string());
            let on_target = join_predicate_m2m_pairs(&join_alias, &child_alias, &join.target)?;
            (
                format!(
                    "\"{}\" {child_alias} JOIN \"{through_name}\" {join_alias} ON {on_target}",
                    target.schema.table_name
                ),
                join_predicate_m2m_pairs(&join_alias, source_alias, &join.parent)?,
            )
        }
        RelationshipKind::BelongsTo => {
            return Err("belongs_to aggregate is not supported".into());
        }
    };

    let ops = inner.dialect.ops();
    let json_agg = ops.json_agg;
    let coalesce_empty = ops.empty_array;
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
                                &target.schema,
                                target_perm,
                                selection.args.get("where"),
                                &child_alias,
                                binds,
                                tables,
                                depth,
                            )?;
                            aggregate_pairs.push((
                                metric.response_key.clone(),
                                format!(
                                    "(SELECT count(*) FROM {from_sql} WHERE {join_pred} AND ({where_for_count}))"
                                ),
                            ));
                        }
                        _ => {
                            return Err(
                                "relationship aggregate fields selection contains an unsupported member"
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
                let nodes_path = if path_prefix.is_empty() {
                    aggregate_member.response_key.clone()
                } else {
                    format!("{path_prefix}.{}", aggregate_member.response_key)
                };
                let (nodes_proj, nodes_evidence) = compile_object_projection(
                    inner,
                    session,
                    role,
                    &target.schema,
                    target_perm,
                    aggregate_member,
                    &child_alias,
                    binds,
                    bytes_paths,
                    tables,
                    &nodes_path,
                    depth,
                )?;
                let where_for_nodes = compile_where(
                    inner,
                    session,
                    role,
                    &target.schema,
                    target_perm,
                    selection.args.get("where"),
                    &child_alias,
                    binds,
                    tables,
                    depth,
                )?;
                let order_sql = compile_order_by(
                    &target.schema,
                    selection.args.get("order_by"),
                    &child_alias,
                    target_perm,
                    inner.strict_where,
                    inner.dialect,
                )?;
                let limit = resolve_limit(
                    selection.args.get("limit"),
                    target_perm.limit,
                    inner.default_limit,
                    inner.max_limit,
                );
                let offset = selection
                    .args
                    .get("offset")
                    .and_then(value_as_u64)
                    .unwrap_or(0);
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
                        "coalesce((SELECT {json_agg}(n) FROM (SELECT {nodes_proj} AS n FROM {from_sql} WHERE {join_pred} AND ({where_for_nodes}) {order_sql} LIMIT {lim} OFFSET {off}) nested_agg_rows), {coalesce_empty})"
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
                return Err(
                    "relationship aggregate selection contains an unsupported member".into(),
                );
            }
        }
    }

    Ok((
        chunked_json_object(inner.dialect, &pairs),
        QueryEvidenceNode::Object(QueryEvidenceObjectPlan {
            record: None,
            fields: evidence_fields,
        }),
    ))
}

pub(super) fn compile_relationship_subquery(
    inner: &EngineInner,
    session: &Session,
    role: &str,
    source: &TableSchema,
    source_alias: &str,
    rel: &crate::table::RelationshipDef,
    target: &CatalogEntry,
    target_perm: &ReadPermission,
    selection: &SelectionNode,
    binds: &mut Vec<BindValue>,
    bytes_paths: &mut Vec<String>,
    tables: &mut Vec<String>,
    path_prefix: &str,
    depth: usize,
) -> Result<(String, QueryEvidenceNode), String> {
    let child_alias = format!("t{depth}");
    let limit = resolve_limit(
        selection.args.get("limit"),
        target_perm.limit,
        inner.default_limit,
        inner.max_limit,
    );
    let offset = selection
        .args
        .get("offset")
        .and_then(value_as_u64)
        .unwrap_or(0);

    let join_pred = match &rel.kind {
        RelationshipKind::HasMany => {
            compile_has_many_join(source, rel, &target.schema, source_alias, &child_alias)?
        }
        RelationshipKind::BelongsTo => {
            compile_belongs_to_join(source, rel, &target.schema, source_alias, &child_alias)?
        }
        RelationshipKind::ManyToMany => {
            let through_name = rel
                .through
                .as_deref()
                .ok_or_else(|| "m2m missing through".to_string())?;
            let through_model = inner
                .by_table
                .get(through_name)
                .and_then(|m| inner.catalog.get(m))
                .ok_or_else(|| format!("through table `{through_name}` not in catalog"))?;
            let join = resolve_m2m_join_keys(source, rel, &through_model.schema, &target.schema)
                .map_err(|error| error.to_string())?;
            tables.push(through_name.to_string());
            return compile_m2m_subquery(
                inner,
                session,
                role,
                source_alias,
                &join,
                through_name,
                &target.schema,
                target_perm,
                selection,
                binds,
                bytes_paths,
                tables,
                path_prefix,
                depth,
                limit,
                offset,
            );
        }
    };

    let order_sql = compile_order_by(
        &target.schema,
        selection.args.get("order_by"),
        &child_alias,
        target_perm,
        inner.strict_where,
        inner.dialect,
    )?;
    let (projection, object_evidence) = compile_object_projection(
        inner,
        session,
        role,
        &target.schema,
        target_perm,
        selection,
        &child_alias,
        binds,
        bytes_paths,
        tables,
        path_prefix,
        depth,
    )?;
    let where_extra = compile_where(
        inner,
        session,
        role,
        &target.schema,
        target_perm,
        selection.args.get("where"),
        &child_alias,
        binds,
        tables,
        depth,
    )?;

    let ops = inner.dialect.ops();
    let json_agg = ops.json_agg;
    let coalesce_empty = ops.empty_array;

    match rel.kind {
        RelationshipKind::BelongsTo => Ok((
            format!(
                "(SELECT {projection} FROM \"{}\" {child_alias} WHERE {join_pred} AND ({where_extra}) LIMIT 1)",
                target.schema.table_name
            ),
            QueryEvidenceNode::Object(object_evidence),
        )),
        _ => {
            binds.push(BindValue::I64(limit as i64));
            let lim = placeholder(inner.dialect, binds.len());
            binds.push(BindValue::I64(offset as i64));
            let off = placeholder(inner.dialect, binds.len());
            Ok((
                format!(
                    "(SELECT coalesce({json_agg}(obj), {coalesce_empty}) FROM (\n  SELECT {projection} AS obj\n  FROM \"{}\" {child_alias}\n  WHERE {join_pred} AND ({where_extra})\n  {order_sql}\n  LIMIT {lim} OFFSET {off}\n) inner_rows)",
                    target.schema.table_name
                ),
                QueryEvidenceNode::List(Box::new(QueryEvidenceNode::Object(object_evidence))),
            ))
        }
    }
}

fn compile_m2m_subquery(
    inner: &EngineInner,
    session: &Session,
    role: &str,
    source_alias: &str,
    join: &M2mJoinKeys,
    through_name: &str,
    target_schema: &TableSchema,
    target_perm: &ReadPermission,
    selection: &SelectionNode,
    binds: &mut Vec<BindValue>,
    bytes_paths: &mut Vec<String>,
    tables: &mut Vec<String>,
    path_prefix: &str,
    depth: usize,
    limit: u64,
    offset: u64,
) -> Result<(String, QueryEvidenceNode), String> {
    let child_alias = format!("t{depth}");
    let j_alias = format!("j{depth}");
    let order_sql = compile_order_by(
        target_schema,
        selection.args.get("order_by"),
        &child_alias,
        target_perm,
        inner.strict_where,
        inner.dialect,
    )?;
    let (projection, object_evidence) = compile_object_projection(
        inner,
        session,
        role,
        target_schema,
        target_perm,
        selection,
        &child_alias,
        binds,
        bytes_paths,
        tables,
        path_prefix,
        depth,
    )?;
    let where_extra = compile_where(
        inner,
        session,
        role,
        target_schema,
        target_perm,
        selection.args.get("where"),
        &child_alias,
        binds,
        tables,
        depth,
    )?;
    let ops = inner.dialect.ops();
    let json_agg = ops.json_agg;
    let coalesce_empty = ops.empty_array;
    binds.push(BindValue::I64(limit as i64));
    let lim = placeholder(inner.dialect, binds.len());
    binds.push(BindValue::I64(offset as i64));
    let off = placeholder(inner.dialect, binds.len());
    let on_target = join_predicate_m2m_pairs(&j_alias, &child_alias, &join.target)?;
    let parent_pred = join_predicate_m2m_pairs(&j_alias, source_alias, &join.parent)?;
    Ok((
        format!(
            "(SELECT coalesce({json_agg}(obj), {coalesce_empty}) FROM (\n  SELECT {projection} AS obj\n  FROM \"{target_table}\" {child_alias}\n  JOIN \"{through_name}\" {j_alias} ON {on_target}\n  WHERE {parent_pred}\n    AND ({where_extra})\n  {order_sql}\n  LIMIT {lim} OFFSET {off}\n) x)",
            target_table = target_schema.table_name,
        ),
        QueryEvidenceNode::List(Box::new(QueryEvidenceNode::Object(object_evidence))),
    ))
}

pub(super) fn compile_has_many_join(
    source: &TableSchema,
    rel: &crate::table::RelationshipDef,
    target: &TableSchema,
    source_alias: &str,
    child_alias: &str,
) -> Result<String, String> {
    compile_direct_join(
        RelationshipKind::HasMany,
        source,
        rel,
        target,
        source_alias,
        child_alias,
    )
}

pub(super) fn compile_belongs_to_join(
    source: &TableSchema,
    rel: &crate::table::RelationshipDef,
    target: &TableSchema,
    source_alias: &str,
    child_alias: &str,
) -> Result<String, String> {
    compile_direct_join(
        RelationshipKind::BelongsTo,
        source,
        rel,
        target,
        source_alias,
        child_alias,
    )
}

fn compile_direct_join(
    kind: RelationshipKind,
    source: &TableSchema,
    rel: &crate::table::RelationshipDef,
    target: &TableSchema,
    source_alias: &str,
    child_alias: &str,
) -> Result<String, String> {
    let pairs = resolve_direct_join_keys(source, rel, target).map_err(|error| error.to_string())?;
    join_predicate_direct(kind, source_alias, child_alias, &pairs)
}
