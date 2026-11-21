use crate::table::{RelationshipDef, RelationshipKind};

use super::binds::BindValue;
use super::evidence::QueryEvidenceNode;
use super::projection::{
    compile_aggregate, compile_object_projection, compile_page, SelectionNode,
};
use super::scan::ModelScan;

/// `<rel>_aggregate { aggregate { count } nodes { … } }` under `source`'s row.
pub(super) fn compile_relationship_aggregate_subquery(
    source: ModelScan<'_>,
    source_alias: &str,
    rel: &RelationshipDef,
    selection: &SelectionNode,
    binds: &mut Vec<BindValue>,
    bytes_paths: &mut Vec<String>,
    tables: &mut Vec<String>,
    path_prefix: &str,
    depth: usize,
) -> Result<(String, QueryEvidenceNode), String> {
    if matches!(rel.kind, RelationshipKind::BelongsTo) {
        return Err("belongs_to aggregate is not supported".into());
    }
    let alias = format!("ta{depth}");
    let (target, link) =
        source.related(rel, source_alias, &alias, &format!("ja{depth}"), tables)?;
    compile_aggregate(
        target,
        selection,
        &alias,
        &link,
        binds,
        bytes_paths,
        tables,
        path_prefix,
        depth,
    )
}

/// A nested relationship field under `source`'s row: one object for
/// belongs-to, a page for has-many and many-to-many.
pub(super) fn compile_relationship_subquery(
    source: ModelScan<'_>,
    source_alias: &str,
    rel: &RelationshipDef,
    selection: &SelectionNode,
    binds: &mut Vec<BindValue>,
    bytes_paths: &mut Vec<String>,
    tables: &mut Vec<String>,
    path_prefix: &str,
    depth: usize,
) -> Result<(String, QueryEvidenceNode), String> {
    let alias = format!("t{depth}");
    let (target, link) = source.related(rel, source_alias, &alias, &format!("j{depth}"), tables)?;
    if matches!(rel.kind, RelationshipKind::BelongsTo) {
        let (projection, evidence) = compile_object_projection(
            target,
            selection,
            &alias,
            binds,
            bytes_paths,
            tables,
            path_prefix,
            depth,
        )?;
        let rows = target.visible_rows(
            &alias,
            &link,
            selection.args.get("where"),
            binds,
            tables,
            depth,
        )?;
        return Ok((
            format!("(SELECT {projection} {rows} LIMIT 1)"),
            QueryEvidenceNode::Object(evidence),
        ));
    }
    let (page, evidence) = compile_page(
        target,
        selection,
        &selection.args,
        &alias,
        &link,
        binds,
        bytes_paths,
        tables,
        path_prefix,
        depth,
    )?;
    Ok((
        format!("({page})"),
        QueryEvidenceNode::List(Box::new(QueryEvidenceNode::Object(evidence))),
    ))
}
