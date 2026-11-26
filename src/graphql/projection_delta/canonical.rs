//! The canonical merge of projection-delta operations and recoveries.
//!
//! Generic over the field value `V` and partition `P`: the server merges
//! concrete [`DeltaValue`](super::DeltaValue)s, the client compiler merges the
//! symbolic expressions of a command preview, and both get the same merge,
//! recovery and ordering semantics.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    ProjectionDeltaError, ProjectionDeltaMutation, ProjectionDeltaOperation,
    ProjectionDeltaRecovery, ProjectionDeltaRecoveryCondition, ProjectionDeltaRecoveryTarget,
};

/// Merge every operation on one scope into its final mutation, ordered by
/// scope.
///
/// Within one scope the later occurrence wins, except that a patch extends an
/// upsert (inside its replacement mask) and cannot revive a delete.
///
/// # Errors
///
/// [`ProjectionDeltaError::InvalidOperation`] when one occurrence contributes
/// two different mutations to a scope, or the mutations on a scope cannot
/// compose.
pub fn canonicalize_operations<V: Clone + Ord, P: Clone + Ord>(
    operations: Vec<ProjectionDeltaOperation<V, P>>,
) -> Result<Vec<ProjectionDeltaOperation<V, P>>, ProjectionDeltaError> {
    let mut by_scope = BTreeMap::new();
    for operation in operations {
        let scope = operation.canonical_scope();
        if let Some(existing) = by_scope.get_mut(&scope) {
            merge_operation(existing, operation)?;
        } else {
            by_scope.insert(scope, operation);
        }
    }
    let mut operations = by_scope.into_values().collect::<Vec<_>>();
    operations.sort_by_key(ProjectionDeltaOperation::canonical_order);
    Ok(operations)
}

/// Merge recoveries by target, ordered by target.
///
/// `Always` wins over `IfRecordMissing`, and an `IfRecordMissing` recovery is
/// kept only while a conditional patch of the same record remains in the
/// canonical `operations`.
pub fn canonicalize_recoveries<V: Clone + Ord, P: Clone + Ord>(
    recoveries: Vec<ProjectionDeltaRecovery<V, P>>,
    operations: &[ProjectionDeltaOperation<V, P>],
) -> Vec<ProjectionDeltaRecovery<V, P>> {
    let mut by_target = BTreeMap::new();
    for recovery in recoveries {
        by_target
            .entry(recovery.target.clone())
            .and_modify(|existing: &mut ProjectionDeltaRecovery<V, P>| {
                existing.occurrence_ordinal =
                    existing.occurrence_ordinal.max(recovery.occurrence_ordinal);
                if recovery.condition == ProjectionDeltaRecoveryCondition::Always {
                    existing.condition = ProjectionDeltaRecoveryCondition::Always;
                }
                merge_refs(
                    &mut existing.projection_refs,
                    recovery.projection_refs.clone(),
                );
            })
            .or_insert(recovery);
    }
    let mut recoveries = by_target.into_values().collect::<Vec<_>>();
    recoveries.retain(|recovery| {
        if recovery.condition == ProjectionDeltaRecoveryCondition::Always {
            return true;
        }
        let ProjectionDeltaRecoveryTarget::Record { scope } = &recovery.target else {
            return false;
        };
        operations.iter().any(|operation| {
            matches!(
                &operation.mutation,
                ProjectionDeltaMutation::Patch {
                    scope: patch_scope,
                    if_present: true,
                    ..
                } if patch_scope == scope
            )
        })
    });
    recoveries.sort_by_key(ProjectionDeltaRecovery::canonical_order);
    recoveries
}

fn merge_operation<V: Clone + Ord, P: Clone + Ord>(
    existing: &mut ProjectionDeltaOperation<V, P>,
    incoming: ProjectionDeltaOperation<V, P>,
) -> Result<(), ProjectionDeltaError> {
    if existing.occurrence_ordinal == incoming.occurrence_ordinal {
        if existing.mutation != incoming.mutation {
            return Err(ProjectionDeltaError::InvalidOperation(
                "same occurrence cannot contribute different mutations to one scope",
            ));
        }
        merge_refs(&mut existing.projection_refs, incoming.projection_refs);
        return Ok(());
    }
    let (earlier, later) = if existing.occurrence_ordinal < incoming.occurrence_ordinal {
        (existing.mutation.clone(), incoming.mutation)
    } else {
        (incoming.mutation, existing.mutation.clone())
    };
    let mutation = merge_mutation(earlier, later)?;
    existing.occurrence_ordinal = existing.occurrence_ordinal.max(incoming.occurrence_ordinal);
    merge_refs(&mut existing.projection_refs, incoming.projection_refs);
    existing.mutation = mutation;
    Ok(())
}

fn merge_mutation<V: Clone + Ord, P: Clone + Ord>(
    existing: ProjectionDeltaMutation<V, P>,
    incoming: ProjectionDeltaMutation<V, P>,
) -> Result<ProjectionDeltaMutation<V, P>, ProjectionDeltaError> {
    if !same_merge_scope(&existing, &incoming) {
        return Err(incompatible_final_mutations());
    }
    match (existing, incoming) {
        (
            ProjectionDeltaMutation::Patch {
                scope,
                set,
                unset,
                if_present,
            },
            ProjectionDeltaMutation::Patch {
                scope: _,
                set: incoming_set,
                unset: incoming_unset,
                if_present: incoming_present,
            },
        ) => {
            let mut values = set
                .into_iter()
                .map(|field| (field.field.clone(), Some(field)))
                .collect::<BTreeMap<_, _>>();
            for field in unset {
                values.insert(field, None);
            }
            for field in incoming_set {
                values.insert(field.field.clone(), Some(field));
            }
            for field in incoming_unset {
                values.insert(field, None);
            }
            let set = values.values().filter_map(Clone::clone).collect();
            let unset = values
                .into_iter()
                .filter_map(|(field, value)| value.is_none().then_some(field))
                .collect();
            Ok(ProjectionDeltaMutation::Patch {
                scope,
                set,
                unset,
                if_present: if_present && incoming_present,
            })
        }
        (
            ProjectionDeltaMutation::Upsert {
                scope,
                fields,
                replace,
            },
            ProjectionDeltaMutation::Patch {
                scope: _,
                set,
                unset,
                if_present: true,
            },
        ) => {
            let mut values = fields
                .into_iter()
                .map(|field| (field.field.clone(), field))
                .collect::<BTreeMap<_, _>>();
            let replace = replace.into_iter().collect::<BTreeSet<_>>();
            if set
                .iter()
                .map(|field| &field.field)
                .chain(unset.iter())
                .any(|field| !replace.contains(field))
            {
                return Err(ProjectionDeltaError::InvalidOperation(
                    "patch fields must belong to the upsert replacement mask",
                ));
            }
            for field in set {
                values.insert(field.field.clone(), field);
            }
            for field in unset {
                values.remove(&field);
            }
            Ok(ProjectionDeltaMutation::Upsert {
                scope,
                fields: values.into_values().collect(),
                replace: replace.into_iter().collect(),
            })
        }
        (
            ProjectionDeltaMutation::Patch { .. },
            incoming @ ProjectionDeltaMutation::Upsert { .. },
        )
        | (
            ProjectionDeltaMutation::Patch { .. },
            incoming @ ProjectionDeltaMutation::Delete { .. },
        )
        | (
            ProjectionDeltaMutation::Upsert { .. },
            incoming @ ProjectionDeltaMutation::Upsert { .. },
        )
        | (
            ProjectionDeltaMutation::Upsert { .. },
            incoming @ ProjectionDeltaMutation::Delete { .. },
        )
        | (
            ProjectionDeltaMutation::Delete { .. },
            incoming @ ProjectionDeltaMutation::Upsert { .. },
        )
        | (
            ProjectionDeltaMutation::Delete { .. },
            incoming @ ProjectionDeltaMutation::Delete { .. },
        ) => Ok(incoming),
        (
            existing @ ProjectionDeltaMutation::Delete { .. },
            ProjectionDeltaMutation::Patch { .. },
        ) => Ok(existing),
        (
            ProjectionDeltaMutation::Link { .. } | ProjectionDeltaMutation::Unlink { .. },
            incoming @ (ProjectionDeltaMutation::Link { .. }
            | ProjectionDeltaMutation::Unlink { .. }),
        ) => Ok(incoming),
        (
            ProjectionDeltaMutation::InvalidateModel { .. },
            incoming @ ProjectionDeltaMutation::InvalidateModel { .. },
        ) => Ok(incoming),
        (
            ProjectionDeltaMutation::InvalidateRelationship { .. },
            incoming @ ProjectionDeltaMutation::InvalidateRelationship { .. },
        ) => Ok(incoming),
        _ => Err(incompatible_final_mutations()),
    }
}

fn same_merge_scope<V: PartialEq, P: PartialEq>(
    existing: &ProjectionDeltaMutation<V, P>,
    incoming: &ProjectionDeltaMutation<V, P>,
) -> bool {
    match (existing, incoming) {
        (
            ProjectionDeltaMutation::Upsert {
                scope: existing, ..
            }
            | ProjectionDeltaMutation::Patch {
                scope: existing, ..
            }
            | ProjectionDeltaMutation::Delete { scope: existing },
            ProjectionDeltaMutation::Upsert {
                scope: incoming, ..
            }
            | ProjectionDeltaMutation::Patch {
                scope: incoming, ..
            }
            | ProjectionDeltaMutation::Delete { scope: incoming },
        ) => existing == incoming,
        (
            ProjectionDeltaMutation::Link {
                relationship: existing_relationship,
                source: existing_source,
                target: existing_target,
            }
            | ProjectionDeltaMutation::Unlink {
                relationship: existing_relationship,
                source: existing_source,
                target: existing_target,
            },
            ProjectionDeltaMutation::Link {
                relationship: incoming_relationship,
                source: incoming_source,
                target: incoming_target,
            }
            | ProjectionDeltaMutation::Unlink {
                relationship: incoming_relationship,
                source: incoming_source,
                target: incoming_target,
            },
        ) => {
            existing_relationship == incoming_relationship
                && existing_source == incoming_source
                && existing_target == incoming_target
        }
        (
            ProjectionDeltaMutation::InvalidateModel {
                partition: existing_partition,
                model: existing_model,
            },
            ProjectionDeltaMutation::InvalidateModel {
                partition: incoming_partition,
                model: incoming_model,
            },
        ) => existing_partition == incoming_partition && existing_model == incoming_model,
        (
            ProjectionDeltaMutation::InvalidateRelationship {
                relationship: existing_relationship,
                source: existing_source,
            },
            ProjectionDeltaMutation::InvalidateRelationship {
                relationship: incoming_relationship,
                source: incoming_source,
            },
        ) => existing_relationship == incoming_relationship && existing_source == incoming_source,
        _ => false,
    }
}

fn incompatible_final_mutations() -> ProjectionDeltaError {
    ProjectionDeltaError::InvalidOperation("same scope resolves to incompatible final mutations")
}

fn merge_refs(existing: &mut Vec<u32>, incoming: Vec<u32>) {
    let mut refs = existing.iter().copied().collect::<BTreeSet<_>>();
    refs.extend(incoming);
    *existing = refs.into_iter().collect();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graphql::projection_delta::{
        DeltaField, DeltaKeyField, DeltaValue, ProjectionDeltaPartition, ProjectionDeltaScope,
    };

    fn scope(model: &str) -> ProjectionDeltaScope {
        ProjectionDeltaScope {
            partition: ProjectionDeltaPartition::Unit,
            model: model.to_owned(),
            key: vec![DeltaKeyField {
                ordinal: 0,
                field: "id".to_owned(),
                value: DeltaValue::String("record-1".to_owned()),
            }],
        }
    }

    fn field(name: &str, value: &str) -> DeltaField {
        DeltaField {
            field: name.to_owned(),
            value: DeltaValue::String(value.to_owned()),
        }
    }

    fn patch(
        scope: &ProjectionDeltaScope,
        set: &[(&str, &str)],
        unset: &[&str],
    ) -> ProjectionDeltaMutation {
        ProjectionDeltaMutation::Patch {
            scope: scope.clone(),
            set: set.iter().map(|(name, value)| field(name, value)).collect(),
            unset: unset.iter().map(|name| (*name).to_owned()).collect(),
            if_present: true,
        }
    }

    fn upsert(
        scope: &ProjectionDeltaScope,
        fields: &[(&str, &str)],
        replace: &[&str],
    ) -> ProjectionDeltaMutation {
        ProjectionDeltaMutation::Upsert {
            scope: scope.clone(),
            fields: fields
                .iter()
                .map(|(name, value)| field(name, value))
                .collect(),
            replace: replace.iter().map(|name| (*name).to_owned()).collect(),
        }
    }

    fn delete(scope: &ProjectionDeltaScope) -> ProjectionDeltaMutation {
        ProjectionDeltaMutation::Delete {
            scope: scope.clone(),
        }
    }

    fn operation(
        occurrence_ordinal: u32,
        projection_refs: &[u32],
        mutation: ProjectionDeltaMutation,
    ) -> ProjectionDeltaOperation {
        ProjectionDeltaOperation {
            occurrence_ordinal,
            projection_refs: projection_refs.to_vec(),
            mutation,
        }
    }

    fn merge_pair(
        existing: ProjectionDeltaMutation,
        incoming: ProjectionDeltaMutation,
    ) -> ProjectionDeltaOperation {
        canonicalize_operations(vec![
            operation(0, &[0], existing),
            operation(1, &[1], incoming),
        ])
        .expect("compatible operations should canonicalize")
        .pop()
        .expect("one record scope should produce one operation")
    }

    #[test]
    fn patch_then_patch_merges_fields_with_last_write_winning() {
        let scope = scope("Todos");
        let actual = merge_pair(
            patch(
                &scope,
                &[("status", "open"), ("title", "old")],
                &["archived"],
            ),
            patch(
                &scope,
                &[("archived", "true"), ("status", "closed")],
                &["title"],
            ),
        );
        let expected = patch(
            &scope,
            &[("archived", "true"), ("status", "closed")],
            &["title"],
        );

        assert_eq!(actual.mutation, expected);
    }

    #[test]
    fn patch_then_upsert_keeps_the_later_upsert() {
        let scope = scope("Todos");
        let expected = upsert(
            &scope,
            &[("status", "closed"), ("title", "new")],
            &["status", "title"],
        );
        let actual = merge_pair(patch(&scope, &[("status", "open")], &[]), expected.clone());

        assert_eq!(actual.mutation, expected);
    }

    #[test]
    fn patch_then_delete_keeps_the_delete() {
        let scope = scope("Todos");
        let expected = delete(&scope);
        let actual = merge_pair(patch(&scope, &[("status", "open")], &[]), expected.clone());

        assert_eq!(actual.mutation, expected);
    }

    #[test]
    fn upsert_then_patch_applies_the_patch_to_the_upsert() {
        let scope = scope("Todos");
        let actual = merge_pair(
            upsert(
                &scope,
                &[("status", "open"), ("title", "old")],
                &["status", "title"],
            ),
            patch(&scope, &[("status", "closed")], &["title"]),
        );
        let expected = upsert(&scope, &[("status", "closed")], &["status", "title"]);

        assert_eq!(actual.mutation, expected);
    }

    #[test]
    fn upsert_then_upsert_keeps_the_later_upsert() {
        let scope = scope("Todos");
        let expected = upsert(
            &scope,
            &[("status", "closed"), ("title", "new")],
            &["status", "title"],
        );
        let actual = merge_pair(
            upsert(
                &scope,
                &[("status", "open"), ("title", "old")],
                &["status", "title"],
            ),
            expected.clone(),
        );

        assert_eq!(actual.mutation, expected);
    }

    #[test]
    fn upsert_then_delete_keeps_the_delete() {
        let scope = scope("Todos");
        let expected = delete(&scope);
        let actual = merge_pair(
            upsert(
                &scope,
                &[("status", "open"), ("title", "old")],
                &["status", "title"],
            ),
            expected.clone(),
        );

        assert_eq!(actual.mutation, expected);
    }

    #[test]
    fn delete_then_patch_keeps_the_delete() {
        let scope = scope("Todos");
        let expected = delete(&scope);
        let actual = merge_pair(
            expected.clone(),
            patch(&scope, &[("status", "closed")], &[]),
        );

        assert_eq!(actual.mutation, expected);
    }

    #[test]
    fn delete_then_upsert_keeps_the_recreating_upsert() {
        let scope = scope("Todos");
        let expected = upsert(
            &scope,
            &[("status", "closed"), ("title", "new")],
            &["status", "title"],
        );
        let actual = merge_pair(delete(&scope), expected.clone());

        assert_eq!(actual.mutation, expected);
    }

    #[test]
    fn delete_then_delete_keeps_the_later_delete() {
        let scope = scope("Todos");
        let expected = delete(&scope);
        let actual = merge_pair(delete(&scope), expected.clone());

        assert_eq!(actual.mutation, expected);
    }

    #[test]
    fn upsert_then_patch_rejects_set_outside_the_replacement_mask() {
        let scope = scope("Todos");
        let result = merge_mutation(
            upsert(&scope, &[("title", "old")], &["title"]),
            patch(&scope, &[("status", "closed")], &[]),
        );

        assert!(matches!(
            result,
            Err(ProjectionDeltaError::InvalidOperation(
                "patch fields must belong to the upsert replacement mask"
            ))
        ));
    }

    #[test]
    fn upsert_then_patch_rejects_unset_outside_the_replacement_mask() {
        let scope = scope("Todos");
        let result = merge_mutation(
            upsert(&scope, &[("title", "old")], &["title"]),
            patch(&scope, &[], &["status"]),
        );

        assert!(matches!(
            result,
            Err(ProjectionDeltaError::InvalidOperation(
                "patch fields must belong to the upsert replacement mask"
            ))
        ));
    }

    #[test]
    fn merged_operation_unions_projection_refs_and_uses_latest_ordinal() {
        let scope = scope("Todos");
        let actual = canonicalize_operations(vec![
            operation(2, &[3, 1], patch(&scope, &[("status", "open")], &[])),
            operation(7, &[3, 2], delete(&scope)),
        ])
        .expect("record lifecycle should canonicalize")
        .pop()
        .expect("one record scope should produce one operation");

        assert_eq!(
            (actual.occurrence_ordinal, actual.projection_refs),
            (7, vec![1, 2, 3])
        );
    }

    #[test]
    fn link_then_unlink_keeps_the_later_edge_state() {
        let source = scope("Todos");
        let target = scope("Tags");
        let expected = ProjectionDeltaMutation::Unlink {
            relationship: "tags".to_owned(),
            source: source.clone(),
            target: target.clone(),
        };
        let actual = merge_pair(
            ProjectionDeltaMutation::Link {
                relationship: "tags".to_owned(),
                source,
                target,
            },
            expected.clone(),
        );

        assert_eq!(actual.mutation, expected);
    }

    #[test]
    fn unlink_then_link_keeps_the_later_edge_state() {
        let source = scope("Todos");
        let target = scope("Tags");
        let expected = ProjectionDeltaMutation::Link {
            relationship: "tags".to_owned(),
            source: source.clone(),
            target: target.clone(),
        };
        let actual = merge_pair(
            ProjectionDeltaMutation::Unlink {
                relationship: "tags".to_owned(),
                source,
                target,
            },
            expected.clone(),
        );

        assert_eq!(actual.mutation, expected);
    }

    #[test]
    fn identical_model_invalidations_merge() {
        let expected = ProjectionDeltaMutation::InvalidateModel {
            partition: None,
            model: "Todos".to_owned(),
        };
        let actual = merge_pair(expected.clone(), expected.clone());

        assert_eq!(actual.mutation, expected);
    }

    #[test]
    fn identical_relationship_invalidations_merge() {
        let expected = ProjectionDeltaMutation::InvalidateRelationship {
            relationship: "tags".to_owned(),
            source: scope("Todos"),
        };
        let actual = merge_pair(expected.clone(), expected.clone());

        assert_eq!(actual.mutation, expected);
    }

    #[test]
    fn incompatible_cross_kind_mutations_are_rejected() {
        let source = scope("Todos");
        let result = merge_mutation(
            patch(&source, &[("status", "closed")], &[]),
            ProjectionDeltaMutation::Link {
                relationship: "tags".to_owned(),
                source,
                target: scope("Tags"),
            },
        );

        assert!(matches!(
            result,
            Err(ProjectionDeltaError::InvalidOperation(
                "same scope resolves to incompatible final mutations"
            ))
        ));
    }

    #[test]
    fn identical_same_occurrence_contributions_union_refs_independent_of_input_order() {
        let mutation = delete(&scope("Todos"));
        let left = operation(3, &[4], mutation.clone());
        let right = operation(3, &[1], mutation);

        let forward = canonicalize_operations(vec![left.clone(), right.clone()]).unwrap();
        let reversed = canonicalize_operations(vec![right, left]).unwrap();

        assert_eq!(forward, reversed);
        assert_eq!(forward[0].projection_refs, vec![1, 4]);
    }

    #[test]
    fn opposite_edge_contributions_in_one_occurrence_fail_closed_in_both_orders() {
        let source = scope("Todos");
        let target = scope("Tags");
        let link = operation(
            5,
            &[0],
            ProjectionDeltaMutation::Link {
                relationship: "tags".to_owned(),
                source: source.clone(),
                target: target.clone(),
            },
        );
        let unlink = operation(
            5,
            &[1],
            ProjectionDeltaMutation::Unlink {
                relationship: "tags".to_owned(),
                source,
                target,
            },
        );

        for operations in [
            vec![link.clone(), unlink.clone()],
            vec![unlink.clone(), link.clone()],
        ] {
            assert!(matches!(
                canonicalize_operations(operations),
                Err(ProjectionDeltaError::InvalidOperation(
                    "same occurrence cannot contribute different mutations to one scope"
                ))
            ));
        }
    }

    #[test]
    fn canonical_operations_are_strictly_scope_ordered() {
        let a = scope("ATodos");
        let z = scope("ZTodos");
        let actual = canonicalize_operations(vec![
            operation(0, &[0], delete(&z)),
            operation(0, &[0], delete(&a)),
        ])
        .expect("independent scopes should canonicalize")
        .into_iter()
        .map(|operation| match operation.mutation {
            ProjectionDeltaMutation::Delete { scope } => scope.model,
            mutation => panic!("expected delete mutation, got {mutation:?}"),
        })
        .collect::<Vec<_>>();

        assert_eq!(actual, vec!["ATodos", "ZTodos"]);
    }
}
