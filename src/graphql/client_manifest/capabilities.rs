use super::*;

pub(super) fn model_has_visible_causal_owner(surface: &Surface, model: &str) -> bool {
    surface
        .projectors
        .iter()
        .filter(|projector| projector.models.iter().any(|candidate| candidate == model))
        .take(2)
        .count()
        == 1
}

fn selected_query_models(surface: &Surface) -> BTreeSet<String> {
    let mut selected = surface
        .query_fields
        .iter()
        .map(|root| root.model_name.clone())
        .collect::<BTreeSet<_>>();
    let mut pending = selected.iter().cloned().collect::<Vec<_>>();
    while let Some(model_name) = pending.pop() {
        let Some(model) = surface.models.get(&model_name) else {
            continue;
        };
        for relationship in &model.relationships {
            if selected.insert(relationship.target_model.clone()) {
                pending.push(relationship.target_model.clone());
            }
        }
    }
    selected
}

fn selected_query_dependencies(surface: &Surface) -> BTreeSet<String> {
    let mut dependencies = surface
        .query_fields
        .iter()
        .flat_map(|root| root.dependencies.iter().cloned())
        .collect::<BTreeSet<_>>();
    for model_name in selected_query_models(surface) {
        let Some(model) = surface.models.get(&model_name) else {
            continue;
        };
        dependencies.insert(model.table_name.clone());
        for relationship in &model.relationships {
            dependencies.extend(relationship.dependencies.iter().cloned());
            if let Some(aggregate) = &relationship.aggregate {
                dependencies.extend(aggregate.dependencies.iter().cloned());
            }
        }
    }
    dependencies
}

pub(super) fn query_footprint_has_record_evidence(surface: &Surface) -> bool {
    let models = selected_query_models(surface);
    !models.is_empty()
        && models
            .iter()
            .all(|model| model_has_visible_causal_owner(surface, model))
}

fn projector_partition_matches_authorization(
    surface: &Surface,
    projector: &crate::graphql::surface::SurfaceProjectionOwner,
) -> bool {
    projector.models.iter().all(|model| {
        surface
            .models
            .get(model)
            .is_some_and(|model| matches!(model.row_policy, SurfaceRowPolicy::Unrestricted))
    })
}

pub(super) fn query_footprint_supports_live_resume(surface: &Surface) -> bool {
    if surface.subscription_fields.is_empty() {
        return false;
    }
    let dependencies = selected_query_dependencies(surface);
    if dependencies.is_empty() {
        return false;
    }
    dependencies.iter().all(|dependency| {
        let mut owners = surface.projectors.iter().filter(|projector| {
            projector
                .dependencies
                .iter()
                .any(|candidate| candidate == dependency)
        });
        let Some(owner) = owners.next() else {
            return false;
        };
        owners.next().is_none()
            && owner.partition.preserves_source_sequence()
            && projector_partition_matches_authorization(surface, owner)
    })
}
