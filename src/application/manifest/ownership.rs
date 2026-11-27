//! The application inventories equal what their modules and surfaces own,
//! and each surface exposes exactly the commands its selection authorizes.

use super::*;

pub(super) fn validate_manifest_ownership(manifest: &ApplicationManifest) -> ApplicationResult<()> {
    let mut module_commands = manifest
        .modules
        .iter()
        .flat_map(|module| module.commands.iter().cloned())
        .collect::<Vec<_>>();
    module_commands.sort_by(|left, right| left.id.cmp(&right.id));
    let module_commands = dedup_commands(module_commands)?;
    if module_commands != manifest.commands {
        return Err(ApplicationError::Collision {
            kind: "command",
            identity: manifest.name.clone(),
            reason: "application command inventory does not equal explicit module ownership".into(),
        });
    }

    let mut module_events = manifest
        .modules
        .iter()
        .flat_map(|module| module.events.iter().cloned())
        .collect::<Vec<_>>();
    let module_events = dedup_events(std::mem::take(&mut module_events))?;
    if module_events != manifest.events {
        return Err(ApplicationError::Collision {
            kind: "event",
            identity: manifest.name.clone(),
            reason: "application event inventory does not equal explicit module ownership".into(),
        });
    }

    let mut owned_projections = manifest
        .modules
        .iter()
        .flat_map(|module| module.projections.iter().cloned())
        .chain(
            manifest
                .surfaces
                .iter()
                .flat_map(|surface| surface.contract.projections.iter().cloned()),
        )
        .collect::<Vec<_>>();
    let owned_projections = dedup_projections(std::mem::take(&mut owned_projections))?;
    if owned_projections != manifest.projections {
        return Err(ApplicationError::Collision {
            kind: "projection",
            identity: manifest.name.clone(),
            reason: "application projection inventory is not closed over modules and surfaces"
                .into(),
        });
    }

    let mut owned_models = manifest
        .modules
        .iter()
        .flat_map(|module| module.models.iter().cloned())
        .chain(
            manifest
                .surfaces
                .iter()
                .flat_map(|surface| surface.contract.models.iter().cloned()),
        )
        .collect::<Vec<_>>();
    let owned_models = dedup_models(std::mem::take(&mut owned_models))?;
    if owned_models != manifest.models {
        return Err(ApplicationError::Collision {
            kind: "model",
            identity: manifest.name.clone(),
            reason: "application model inventory is not closed over modules and surfaces".into(),
        });
    }

    for module in &manifest.modules {
        for owned_surface in &module.surfaces {
            let Some(surface) = manifest
                .surfaces
                .iter()
                .find(|surface| surface.id == owned_surface.id)
            else {
                return Err(ApplicationError::Missing {
                    kind: "surface",
                    identity: owned_surface.id.clone(),
                });
            };
            if surface != owned_surface {
                return Err(ApplicationError::Collision {
                    kind: "surface",
                    identity: surface.id.clone(),
                    reason: "surface ownership has incompatible canonical material".into(),
                });
            }
        }
    }

    let commands = manifest
        .commands
        .iter()
        .map(|command| (command.id.as_str(), command))
        .collect::<std::collections::BTreeMap<_, _>>();
    let models = manifest
        .models
        .iter()
        .map(|model| (model.id.as_str(), model))
        .collect::<std::collections::BTreeMap<_, _>>();
    // Every surface model and projection is already inside the closed
    // inventories checked above: models dedup to one declaration per id or
    // collide, projection ids are unique. No per-surface comparison remains.
    for surface in &manifest.surfaces {
        let expected_command_ids = surface_command_closure(surface, &manifest.commands)?;
        let actual_command_ids = surface
            .contract
            .commands
            .iter()
            .map(|command| command.id.clone())
            .collect::<Vec<_>>();
        if actual_command_ids != expected_command_ids {
            return Err(ApplicationError::Collision {
                kind: "command",
                identity: surface.id.clone(),
                reason: "surface command inventory is not the exact authorized command closure"
                    .into(),
            });
        }
        for exposed in &surface.contract.commands {
            let Some(authoritative) = commands.get(exposed.id.as_str()) else {
                return Err(ApplicationError::Missing {
                    kind: "command",
                    identity: exposed.id.clone(),
                });
            };
            let expected_roles = surface_command_roles(surface, authoritative);
            validate_surface_command_ownership(exposed, authoritative, &expected_roles)?;
        }
        for root in &surface.contract.roots {
            if !models.contains_key(root.model.as_str()) {
                return Err(ApplicationError::Missing {
                    kind: "model",
                    identity: root.model.clone(),
                });
            }
        }
    }
    Ok(())
}

pub(super) fn validate_surface_command_ownership(
    exposed: &SurfaceCommandSpec,
    authoritative: &crate::application::command::CommandSpec,
    expected_roles: &[String],
) -> ApplicationResult<()> {
    if exposed.field_name != authoritative.field_name
        || exposed.roles != expected_roles
        || exposed.input.as_ref() != Some(&authoritative.input)
        || exposed.output.as_ref() != Some(&authoritative.output)
        || exposed.consistency != authoritative.consistency
        || canonical_json(&exposed.defaults) != canonical_json(&authoritative.defaults)
        || canonical_json(&exposed.applies()) != canonical_json(&authoritative.applies)
        || canonical_json(&exposed.projection_contract)
            != canonical_json(&authoritative.projection_contract)
        || exposed.direct_projection != authoritative.direct_projection
        || exposed.projected_model != authoritative.projected_model
    {
        return Err(ApplicationError::Collision {
            kind: "command",
            identity: authoritative.id.clone(),
            reason: "surface command is not compatible with its application declaration".into(),
        });
    }
    Ok(())
}

pub(super) fn surface_command_closure(
    surface: &SurfaceSpec,
    commands: &[crate::application::command::CommandSpec],
) -> ApplicationResult<Vec<String>> {
    let mut expected = commands
        .iter()
        .filter(|command| match &surface.contract.selection {
            SurfaceSelectionSpec::Catalog => true,
            SurfaceSelectionSpec::Role { name } => {
                command.roles.is_empty() || command.roles.iter().any(|allowed| allowed == name)
            }
            SurfaceSelectionSpec::Application { schema_roles, .. } => {
                command.roles.is_empty()
                    || schema_roles
                        .iter()
                        .all(|role| command.roles.iter().any(|allowed| allowed == role))
            }
        })
        .map(|command| command.id.clone())
        .collect::<Vec<_>>();
    expected.sort();
    expected.dedup();
    Ok(expected)
}

pub(super) fn surface_command_roles(
    surface: &SurfaceSpec,
    authoritative: &crate::application::command::CommandSpec,
) -> Vec<String> {
    match &surface.contract.selection {
        SurfaceSelectionSpec::Catalog => authoritative.roles.clone(),
        selection => selection.eligible_roles().to_vec(),
    }
}
