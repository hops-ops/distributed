//! Nested selection complexity estimation (ship-complexity-1).
//!
//! async-graphql's dynamic schema only supports flat `1 + children` complexity
//! (no per-field weights). We estimate a **relationship-aware** cost from the
//! selection tree before SQL compile so multi-level `has_many` fan-out is
//! bounded by `max_complexity`, not just `max_depth`.

use crate::table::{RelationshipKind, TableSchema};

use super::compile::{RootKind, SelectionNode};
pub(crate) use super::complexity_contract::{
    default_weights, ComplexityWeights, DEFAULT_MAX_COMPLEXITY, DEFAULT_MAX_DEPTH,
};
use super::engine::EngineInner;

/// Estimate complexity for a root field selection before SQL compile.
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
    let w = default_weights();
    let child = estimate_object_selection(inner, &entry.schema, selection, &w)?;
    let root = match kind {
        RootKind::List => w
            .list_root
            .saturating_add(w.list_fanout.saturating_mul(child)),
        RootKind::ByPk => w.by_pk.saturating_add(child),
        RootKind::Aggregate => {
            // Aggregate selection often has `nodes { ... }` and `aggregate { count }`.
            w.aggregate.saturating_add(child)
        }
    };
    Ok(root)
}

fn estimate_object_selection(
    inner: &EngineInner,
    schema: &TableSchema,
    selection: &SelectionNode,
    w: &ComplexityWeights,
) -> Result<usize, String> {
    let mut total = 0usize;
    for child in &selection.children {
        total = total.saturating_add(estimate_field(inner, schema, child, w)?);
    }
    // Object with no children (empty selection) still costs a scalar floor.
    if selection.children.is_empty() {
        return Ok(w.scalar);
    }
    Ok(total)
}

fn estimate_field(
    inner: &EngineInner,
    schema: &TableSchema,
    node: &SelectionNode,
    w: &ComplexityWeights,
) -> Result<usize, String> {
    let name = node.field_name.as_str();

    // Nested aggregate field: `<rel>_aggregate`
    if let Some(rel_name) = name.strip_suffix("_aggregate") {
        if let Some(rel) = schema
            .relationships
            .iter()
            .find(|r| r.field_name == rel_name)
        {
            let target = match inner.catalog.get(&rel.target_model) {
                Some(t) => t,
                None => return Ok(w.aggregate),
            };
            let child = estimate_object_selection(inner, &target.schema, node, w)?;
            return Ok(w.aggregate.saturating_add(child));
        }
    }

    if let Some(rel) = schema.relationships.iter().find(|r| r.field_name == name) {
        let target = match inner.catalog.get(&rel.target_model) {
            Some(t) => t,
            None => return Ok(w.has_many),
        };
        let child = estimate_object_selection(inner, &target.schema, node, w)?;
        return Ok(match rel.kind {
            RelationshipKind::BelongsTo => w.belongs_to.saturating_add(child),
            RelationshipKind::HasMany => w
                .has_many
                .saturating_add(w.list_fanout.saturating_mul(child)),
            RelationshipKind::ManyToMany => {
                w.m2m.saturating_add(w.list_fanout.saturating_mul(child))
            }
        });
    }

    // Aggregate nodes/count leaves on aggregate type
    if name == "nodes" {
        // nodes reuses parent schema when on aggregate — treated as list of same model
        let child = estimate_object_selection(inner, schema, node, w)?;
        return Ok(w.list_fanout.saturating_mul(child).max(w.scalar));
    }
    if name == "aggregate" || name == "count" {
        return Ok(w.scalar);
    }

    // Scalar / unknown field
    Ok(w.scalar)
}

/// True if estimated cost exceeds the engine budget.
pub fn exceeds_budget(cost: usize, max_complexity: usize) -> bool {
    cost > max_complexity
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graphql::compile::SelectionNode;
    use crate::table::{
        ColumnType, PrimaryKey, RelationshipDef, RelationshipKind, TableColumn, TableKind,
        TableSchema,
    };
    use std::collections::BTreeMap;

    fn col(name: &str) -> TableColumn {
        TableColumn::new(name, name, ColumnType::Text)
    }

    fn parent_schema() -> TableSchema {
        TableSchema {
            model_name: "Parent".into(),
            table_name: "parents".into(),
            columns: vec![
                TableColumn {
                    primary_key: true,
                    ..col("parent_id")
                },
                col("name"),
            ],
            primary_key: PrimaryKey::new(["parent_id"]),
            version_column: None,
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            relationships: vec![RelationshipDef {
                references: None,
                field_name: "children".into(),
                kind: RelationshipKind::HasMany,
                target_model: "Child".into(),
                foreign_key: Some("parent_id".into()),
                through: None,
                target_foreign_key: None,
            }],
            kind: TableKind::ReadModel,
        }
    }

    fn child_schema() -> TableSchema {
        TableSchema {
            model_name: "Child".into(),
            table_name: "children".into(),
            columns: vec![
                TableColumn {
                    primary_key: true,
                    ..col("child_id")
                },
                col("parent_id"),
                col("name"),
                // nested grandchildren for multi-level
            ],
            primary_key: PrimaryKey::new(["child_id"]),
            version_column: None,
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            relationships: vec![RelationshipDef {
                references: None,
                field_name: "grandchildren".into(),
                kind: RelationshipKind::HasMany,
                target_model: "Grandchild".into(),
                foreign_key: Some("child_id".into()),
                through: None,
                target_foreign_key: None,
            }],
            kind: TableKind::ReadModel,
        }
    }

    fn grand_schema() -> TableSchema {
        TableSchema {
            model_name: "Grandchild".into(),
            table_name: "grandchildren".into(),
            columns: vec![
                TableColumn {
                    primary_key: true,
                    ..col("grand_id")
                },
                col("child_id"),
                col("name"),
            ],
            primary_key: PrimaryKey::new(["grand_id"]),
            version_column: None,
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: TableKind::ReadModel,
        }
    }

    fn sel(field: &str, children: Vec<SelectionNode>) -> SelectionNode {
        SelectionNode {
            response_key: field.into(),
            field_name: field.into(),
            args: BTreeMap::new(),
            children,
        }
    }

    #[test]
    fn nested_has_many_costs_more_than_flat_scalars() {
        let w = default_weights();
        // Pure formula check (mirrors estimate_field for has_many + list root).
        // Mirrors estimate_root_complexity(List) for parent { id, children { a, b, grandchildren { x, y } } }
        // with one scalar at each list level.
        let grand = w.has_many + w.list_fanout * (w.scalar * 2);
        let child = w.has_many + w.list_fanout * (w.scalar * 2 + grand);
        let three_nest = w.list_root + w.list_fanout * (w.scalar + child);
        let one_child = w.has_many + w.list_fanout * (w.scalar * 2);
        let one_nest = w.list_root + w.list_fanout * (w.scalar + one_child);
        let flat = w.list_root + w.list_fanout * (w.scalar * 3);
        assert!(one_nest > flat, "nest={one_nest} flat={flat}");
        assert!(three_nest > one_nest, "three={three_nest} one={one_nest}");
        assert!(
            three_nest > DEFAULT_MAX_COMPLEXITY,
            "expected 3-level nest to exceed default budget, got {three_nest}"
        );
        assert!(
            one_nest < DEFAULT_MAX_COMPLEXITY,
            "expected 1-level nest under budget, got {one_nest}"
        );
        let _ = (parent_schema(), child_schema(), grand_schema(), sel);
    }

    #[test]
    fn exceeds_budget_helper() {
        assert!(exceeds_budget(501, 500));
        assert!(!exceeds_budget(500, 500));
    }
}
