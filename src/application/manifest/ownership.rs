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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::application::command::{CommandTypeField, CommandTypeSpec, EventSpec};
    use crate::application::module::ModelFieldSpec;
    use crate::application::surface::SurfaceContract;
    use crate::application::{Application, CommandDefinition};
    use crate::command::CommandConsistency;
    use crate::graphql::read;
    use crate::graphql::surface::{build_surface, surface_for_role, Surface, SurfaceOptions};
    use crate::table::{ColumnType, PrimaryKey, TableColumn, TableKind, TableSchema};

    fn todos() -> Surface {
        let table = TableSchema {
            model_name: "TodoView".into(),
            table_name: "todos".into(),
            columns: vec![TableColumn {
                primary_key: true,
                ..TableColumn::new("id", "id", ColumnType::Text)
            }],
            primary_key: PrimaryKey::new(["id"]),
            version_column: None,
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: TableKind::ReadModel,
        };
        build_surface(&[table], &SurfaceOptions::sqlite()).unwrap()
    }

    fn reader_surface() -> SurfaceSpec {
        let grants = BTreeMap::from([("TodoView".to_string(), read().all_columns())]);
        let selected = surface_for_role(&todos(), "reader", &grants).unwrap();
        SurfaceSpec::from_surface("web", &selected).unwrap()
    }

    fn command(roles: &[&str]) -> CommandSpec {
        let field = |name: &str| CommandTypeField {
            unsigned_integer: None,
            name: name.into(),
            type_name: "String".into(),
            nullable: false,
            list: false,
            item_nullable: false,
            nested: None,
        };
        let mut spec = CommandSpec::try_new(
            "todo.create",
            "todo_create",
            CommandTypeSpec {
                name: "TodoCreateInput".into(),
                fields: vec![field("title")],
            },
            CommandTypeSpec {
                name: "TodoCreateOutput".into(),
                fields: vec![field("id")],
            },
            CommandConsistency::Eventual,
        )
        .unwrap();
        spec.roles = roles.iter().map(|role| role.to_string()).collect();
        spec.fingerprint = sha256_fingerprint(&spec.canonical_bytes().unwrap());
        spec
    }

    fn module(roles: &[&str]) -> Module {
        Module::new("todo")
            .command_definition(CommandDefinition::contract(command(roles)))
            .build()
            .unwrap()
    }

    /// A valid manifest: an admin-only command beside an explicit reader
    /// surface, which therefore exposes no command.
    fn manifest() -> ApplicationManifest {
        Application::try_new("app", [module(&["admin"])], [reader_surface()])
            .unwrap()
            .manifest()
            .clone()
    }

    /// The same surface owned by the module instead of passed explicitly.
    fn module_owned_surface_manifest() -> ApplicationManifest {
        let module = Module::new("todo")
            .surface(reader_surface())
            .build()
            .unwrap();
        Application::try_new("app", [module], [])
            .unwrap()
            .manifest()
            .clone()
    }

    fn id_field() -> ModelFieldSpec {
        ModelFieldSpec {
            name: "id".into(),
            scalar: "String".into(),
            nullable: false,
        }
    }

    fn refresh(surface: &mut SurfaceSpec) {
        surface.fingerprint = sha256_fingerprint(&surface.canonical_bytes().unwrap());
    }

    fn event(body_fingerprint: char) -> EventSpec {
        EventSpec {
            name: "todo.created".into(),
            version: 1,
            body_type: "TodoCreated".into(),
            body_version: 1,
            body_schema: "todo-created".into(),
            body_fingerprint: format!("sha256:{}", body_fingerprint.to_string().repeat(64)),
            body_codec: "json".into(),
            body_codec_version: 1,
        }
    }

    fn collision(kind: &'static str, identity: &str, reason: &str) -> ApplicationError {
        ApplicationError::Collision {
            kind,
            identity: identity.into(),
            reason: reason.into(),
        }
    }

    #[test]
    fn validation_rejects_each_inventory_that_disagrees_with_its_owners() {
        type Base = fn() -> ApplicationManifest;
        type Tamper = fn(&mut ApplicationManifest);
        let cases: [(&str, Base, Tamper, ApplicationError); 7] = [
            (
                "command without a module owner",
                manifest,
                |manifest| manifest.commands.clear(),
                collision(
                    "command",
                    "app",
                    "application command inventory does not equal explicit module ownership",
                ),
            ),
            (
                "event without a module owner",
                manifest,
                |manifest| manifest.events.push(event('a')),
                collision(
                    "event",
                    "app",
                    "application event inventory does not equal explicit module ownership",
                ),
            ),
            (
                "projection without an owner",
                manifest,
                |manifest| {
                    manifest.projections.push(
                        ProjectionSpec::try_new("orphan", Vec::<String>::new(), ["TodoView"])
                            .unwrap(),
                    )
                },
                collision(
                    "projection",
                    "app",
                    "application projection inventory is not closed over modules and surfaces",
                ),
            ),
            (
                "model without an owner",
                manifest,
                |manifest| {
                    manifest.models.push(
                        ModelSpec::try_new("Orphan", "orphans", [id_field()], ["id"]).unwrap(),
                    )
                },
                collision(
                    "model",
                    "app",
                    "application model inventory is not closed over modules and surfaces",
                ),
            ),
            (
                "module surface renamed in the application",
                module_owned_surface_manifest,
                |manifest| {
                    manifest.surfaces[0].id = "other".into();
                    refresh(&mut manifest.surfaces[0]);
                },
                ApplicationError::Missing {
                    kind: "surface",
                    identity: "web".into(),
                },
            ),
            (
                "module surface altered in the application",
                module_owned_surface_manifest,
                |manifest| {
                    manifest.surfaces[0].contract.default_limit += 1;
                    refresh(&mut manifest.surfaces[0]);
                },
                collision(
                    "surface",
                    "web",
                    "surface ownership has incompatible canonical material",
                ),
            ),
            (
                "root over an undeclared model",
                manifest,
                |manifest| {
                    manifest.surfaces[0].contract.roots[0].model = "Ghost".into();
                    refresh(&mut manifest.surfaces[0]);
                },
                ApplicationError::Missing {
                    kind: "model",
                    identity: "Ghost".into(),
                },
            ),
        ];
        for (name, base, tamper, expected) in cases {
            let mut manifest = base();
            manifest
                .validate()
                .unwrap_or_else(|error| panic!("{name}: base {error}"));
            tamper(&mut manifest);
            manifest.fingerprints = ManifestFingerprint::default();
            assert_eq!(manifest.validate().unwrap_err(), expected, "{name}");
        }
    }

    #[test]
    fn a_surface_must_expose_exactly_its_authorized_commands() {
        // An unrestricted command is authorized for the reader surface, which
        // was compiled without it.
        assert_eq!(
            Application::try_new("app", [module(&[])], [reader_surface()]).unwrap_err(),
            collision(
                "command",
                "web",
                "surface command inventory is not the exact authorized command closure",
            )
        );
        let catalog = SurfaceSpec {
            contract: SurfaceContract {
                selection: SurfaceSelectionSpec::Catalog,
                ..reader_surface().contract
            },
            ..reader_surface()
        };
        let cases = [
            ("catalog", &catalog, &[][..], vec!["todo.create"]),
            (
                "role without the grant",
                &reader_surface(),
                &["admin"][..],
                vec![],
            ),
            (
                "unrestricted",
                &reader_surface(),
                &[][..],
                vec!["todo.create"],
            ),
        ];
        for (name, surface, roles, expected) in cases {
            assert_eq!(
                surface_command_closure(surface, &[command(roles)]).unwrap(),
                expected,
                "{name}"
            );
        }
        let application = SurfaceSelectionSpec::Application {
            name: "web".into(),
            eligible_roles: vec!["admin".into(), "user".into()],
            schema_roles: vec!["admin".into(), "user".into()],
        };
        let application = SurfaceSpec {
            contract: SurfaceContract {
                selection: application,
                ..reader_surface().contract
            },
            ..reader_surface()
        };
        for (name, roles, expected) in [
            (
                "every schema role granted",
                &["admin", "user"][..],
                vec!["todo.create"],
            ),
            ("one schema role missing", &["admin"][..], vec![]),
        ] {
            assert_eq!(
                surface_command_closure(&application, &[command(roles)]).unwrap(),
                expected,
                "{name}"
            );
        }
        assert_eq!(
            surface_command_roles(&catalog, &command(&["admin"])),
            ["admin"]
        );
        assert_eq!(
            surface_command_roles(&application, &command(&["admin"])),
            ["admin", "user"]
        );
    }

    #[test]
    fn composition_rejects_incompatible_duplicates() {
        assert_eq!(
            dedup_events(vec![event('a'), event('a')]).unwrap(),
            vec![event('a')],
            "identical events collapse"
        );
        assert_eq!(
            dedup_events(vec![event('b'), event('a')]).unwrap_err(),
            collision(
                "event",
                "todo.created",
                "same identity has incompatible event schemas"
            )
        );
        let model =
            |table: &str| ModelSpec::try_new("TodoView", table, [id_field()], ["id"]).unwrap();
        assert_eq!(
            dedup_models(vec![model("todos"), model("todos")]).unwrap(),
            vec![model("todos")],
            "identical models collapse"
        );
        assert_eq!(
            dedup_models(vec![model("todos"), model("other")]).unwrap_err(),
            collision(
                "model",
                "TodoView",
                "same identity has incompatible model schemas"
            )
        );
        assert_eq!(
            Application::try_new("app", [module(&["admin"]), module(&["admin"])], []).unwrap_err(),
            ApplicationError::Duplicate {
                kind: "module",
                identity: "todo".into(),
            }
        );
    }

    #[test]
    fn framework_compatibility_rejects_each_malformed_record() {
        let base = manifest();
        assert_eq!(base.module_ids(), ["todo"]);
        assert_eq!(
            base.framework_compatibility().unwrap(),
            FrameworkCompatibility::current()
        );
        let with = |version: u32, value: serde_json::Value| {
            let mut manifest = base.clone();
            manifest
                .extensions
                .retain(|extension| extension.id != FRAMEWORK_COMPATIBILITY_EXTENSION_ID);
            manifest.with_extension(ApplicationExtension {
                id: FRAMEWORK_COMPATIBILITY_EXTENSION_ID.into(),
                version,
                value,
            })
        };
        let release = serde_json::json!({"release": env!("CARGO_PKG_VERSION")});
        let cases = [
            (
                "wrong version",
                with(2, release.clone()),
                "framework compatibility extension version must be 1",
            ),
            (
                "not a record",
                with(1, serde_json::json!("1.0")),
                "invalid framework compatibility record",
            ),
        ];
        for (name, manifest, expected) in cases {
            let Err(ApplicationError::InvalidSpec(reason)) = manifest.framework_compatibility()
            else {
                panic!("{name}: expected InvalidSpec");
            };
            assert!(reason.starts_with(expected), "{name}: {reason}");
        }
        assert_eq!(
            with(1, serde_json::json!({"release": ""}))
                .framework_compatibility()
                .unwrap_err(),
            ApplicationError::InvalidIdentity {
                kind: "framework release",
                value: String::new(),
                reason: "must be a bounded portable logical value",
            },
            "blank release"
        );
        assert!(with(1, release).framework_compatibility().is_ok());
    }
}
