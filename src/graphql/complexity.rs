//! Nested selection complexity estimation (ship-complexity-1).
//!
//! async-graphql's dynamic schema only supports flat `1 + children` complexity
//! (no per-field weights). We estimate a **relationship-aware** cost from the
//! selection tree before SQL compile so multi-level `has_many` fan-out is
//! bounded by `max_complexity`, not just `max_depth`.

use crate::table::{RelationshipKind, TableSchema};

use super::client_manifest::{ClientComplexityWeights, ClientRelationshipKind, ClientRootKind};
use super::compile::{RootKind, SelectionNode};
pub(crate) use super::complexity_contract::{DEFAULT_MAX_COMPLEXITY, DEFAULT_MAX_DEPTH};
use super::engine::EngineInner;

/// Estimate complexity for a root field selection before SQL compile, with
/// the weights and formula the client manifest exports.
pub fn estimate_root_complexity(
    inner: &EngineInner,
    model_name: &str,
    kind: RootKind,
    selection: &SelectionNode,
) -> Result<usize, String> {
    let entry = inner
        .catalog
        .get(model_name)
        .ok_or_else(|| format!("unknown model `{model_name}`"))?;
    let w = ClientComplexityWeights::current();
    let child = estimate_object_selection(inner, &entry.schema, selection, &w);
    let kind = match kind {
        RootKind::List => ClientRootKind::List,
        RootKind::ByPk => ClientRootKind::ByPk,
        RootKind::Aggregate => ClientRootKind::Aggregate,
    };
    Ok(usize::try_from(w.root(kind, child)).unwrap_or(usize::MAX))
}

fn estimate_object_selection(
    inner: &EngineInner,
    schema: &TableSchema,
    selection: &SelectionNode,
    w: &ClientComplexityWeights,
) -> u64 {
    w.object(
        selection
            .children
            .iter()
            .map(|child| estimate_field(inner, schema, child, w)),
    )
}

fn estimate_field(
    inner: &EngineInner,
    schema: &TableSchema,
    node: &SelectionNode,
    w: &ClientComplexityWeights,
) -> u64 {
    let name = node.field_name.as_str();

    // Nested aggregate field: `<rel>_aggregate`
    if let Some(rel_name) = name.strip_suffix("_aggregate") {
        if let Some(rel) = schema
            .relationships
            .iter()
            .find(|r| r.field_name == rel_name)
        {
            return match inner.catalog.get(&rel.target_model) {
                Some(target) => {
                    w.aggregate(estimate_object_selection(inner, &target.schema, node, w))
                }
                None => w.aggregate,
            };
        }
    }

    if let Some(rel) = schema.relationships.iter().find(|r| r.field_name == name) {
        let Some(target) = inner.catalog.get(&rel.target_model) else {
            return w.has_many;
        };
        let kind = match rel.kind {
            RelationshipKind::BelongsTo => ClientRelationshipKind::BelongsTo,
            RelationshipKind::HasMany => ClientRelationshipKind::HasMany,
            RelationshipKind::ManyToMany => ClientRelationshipKind::ManyToMany,
        };
        return w.relationship(
            kind,
            estimate_object_selection(inner, &target.schema, node, w),
        );
    }

    // `nodes` on an aggregate type is a list of the same model.
    if name == "nodes" {
        return w.aggregate_nodes(estimate_object_selection(inner, schema, node, w));
    }

    // `aggregate`, `count`, scalar or unknown field.
    w.scalar
}

/// True if estimated cost exceeds the engine budget.
pub fn exceeds_budget(cost: usize, max_complexity: usize) -> bool {
    cost > max_complexity
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exceeds_budget_helper() {
        assert!(exceeds_budget(501, 500));
        assert!(!exceeds_budget(500, 500));
    }
}
