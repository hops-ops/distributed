use super::*;
use crate::command::{typed_command, Eventual, PreparedCommand, Succeeded};
use crate::command::{CommandInputType, CommandOutputType, CommandTypeDef, CommandTypeField};
use crate::graphql::{
    build_surface, claim, col, rel, surface_for_application, surface_for_role, RoleGrant,
    SurfaceCommand, SurfaceOptions, SurfaceProjector, SurfaceTypeField,
};
use crate::microsvc::{CausalCommandContext, HandlerError, Routes, Service};
use crate::table::{
    ColumnType, PrimaryKey, RelationshipDef, RelationshipKind, TableColumn, TableKind, TableSchema,
};
use std::any::TypeId;

#[derive(Serialize, crate::DomainEvent)]
#[domain_event(name = "todo.projected", version = 1)]
struct ManifestTodoProjected {
    todo_id: String,
}

#[derive(Serialize, crate::DomainEvent)]
#[domain_event(name = "todo.archived", version = 1)]
struct ManifestTodoArchived {
    todo_id: String,
}

#[test]
fn generated_causal_command_operation_requires_framework_command_id() {
    let operation = command_operation(
        "todo_create",
        &ClientCommandShape::Object {
            definition: ClientTypeDef {
                name: "CreateTodoInput".into(),
                fields: Vec::new(),
            },
        },
        &ClientCommandShape::Object {
            definition: ClientTypeDef {
                name: "CreateTodoOutput".into(),
                fields: vec![ClientTypeField {
                    name: "id".into(),
                    type_name: "String".into(),
                    nullable: false,
                    list: false,
                    item_nullable: false,
                    codec: Some("string".into()),
                    nested: None,
                }],
            },
        },
    );
    assert_eq!(
            operation,
            "mutation Client_todo_create($commandId: ID!, $input: CreateTodoInput!) { todo_create(commandId: $commandId, input: $input) { id } }"
        );
}

fn column(name: &str, ty: ColumnType) -> TableColumn {
    TableColumn::new(name, name, ty)
}

fn users() -> TableSchema {
    TableSchema {
        model_name: "UserView".into(),
        table_name: "users".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..column("user_id", ColumnType::Text)
            },
            column("display_name", ColumnType::Text),
            column("secret", ColumnType::Text),
        ],
        primary_key: PrimaryKey::new(["user_id"]),
        version_column: Some("_sourced_version".into()),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    }
}

fn todos() -> TableSchema {
    TableSchema {
        model_name: "TodoView".into(),
        table_name: "todos".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..column("todo_id", ColumnType::Text)
            },
            column("owner_id", ColumnType::Text),
            column("title", ColumnType::Text),
            column("completed", ColumnType::Boolean),
        ],
        primary_key: PrimaryKey::new(["todo_id"]),
        version_column: Some("_sourced_version".into()),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: vec![RelationshipDef {
            field_name: "owner".into(),
            kind: RelationshipKind::BelongsTo,
            target_model: "UserView".into(),
            foreign_key: Some("owner_id".into()),
            through: None,
            target_foreign_key: None,
        }],
        kind: TableKind::ReadModel,
    }
}

fn memberships() -> TableSchema {
    TableSchema {
        model_name: "MembershipView".into(),
        table_name: "memberships".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..column("tenant_id", ColumnType::Text)
            },
            TableColumn {
                primary_key: true,
                ..column("user_id", ColumnType::Text)
            },
            column("role", ColumnType::Text),
        ],
        primary_key: PrimaryKey::new(["tenant_id", "user_id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    }
}

fn teams() -> TableSchema {
    TableSchema {
        model_name: "TeamView".into(),
        table_name: "teams".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..column("team_id", ColumnType::Text)
            },
            column("name", ColumnType::Text),
        ],
        primary_key: PrimaryKey::new(["team_id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: vec![RelationshipDef {
            field_name: "members".into(),
            kind: RelationshipKind::ManyToMany,
            target_model: "UserView".into(),
            foreign_key: Some("team_id".into()),
            through: Some("private_team_members".into()),
            target_foreign_key: Some("user_id".into()),
        }],
        kind: TableKind::ReadModel,
    }
}

fn team_members() -> TableSchema {
    TableSchema {
        model_name: "PrivateTeamMember".into(),
        table_name: "private_team_members".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..column("team_id", ColumnType::Text)
            },
            TableColumn {
                primary_key: true,
                ..column("user_id", ColumnType::Text)
            },
        ],
        primary_key: PrimaryKey::new(["team_id", "user_id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::Operational,
    }
}

#[derive(Deserialize)]
struct CompleteInput;
impl CommandInputType for CompleteInput {
    fn command_type() -> CommandTypeDef {
        CommandTypeDef::new(
            "CompleteTodoInput",
            vec![CommandTypeField {
                name: "todo_id".into(),
                type_name: "String".into(),
                nullable: false,
                list: false,
                item_nullable: false,
                nested: None,
            }],
        )
        .with_type_id(TypeId::of::<Self>())
    }
}

#[derive(Serialize)]
struct CompletePayload;
impl CommandOutputType for CompletePayload {
    fn command_type() -> CommandTypeDef {
        CommandTypeDef::new(
            "CompleteTodoPayload",
            vec![CommandTypeField {
                name: "todo_id".into(),
                type_name: "String".into(),
                nullable: false,
                list: false,
                item_nullable: false,
                nested: None,
            }],
        )
        .with_type_id(TypeId::of::<Self>())
    }
}

#[derive(Default)]
struct ManifestAggregate {
    entity: crate::Entity,
}

impl crate::Aggregate for ManifestAggregate {
    type ReplayError = std::convert::Infallible;

    fn entity(&self) -> &crate::Entity {
        &self.entity
    }

    fn entity_mut(&mut self) -> &mut crate::Entity {
        &mut self.entity
    }

    fn replay_event(&mut self, _event: &crate::EventRecord) -> Result<(), Self::ReplayError> {
        Ok(())
    }
}

async fn complete_handler(
    _context: &CausalCommandContext<'_, ManifestAggregate>,
    _input: CompleteInput,
) -> Result<PreparedCommand<Succeeded<CompletePayload>>, HandlerError> {
    Ok(
        PreparedCommand::<Succeeded<CompletePayload>>::prepare(CompletePayload)
            .expect("serializable command payload"),
    )
}

async fn complete_causal_handler(
    _context: &CausalCommandContext<'_, ManifestAggregate>,
    _input: CompleteInput,
) -> Result<PreparedCommand<Eventual<CompletePayload>>, HandlerError> {
    Ok(
        PreparedCommand::<Eventual<CompletePayload>>::prepare(CompletePayload)
            .expect("serializable command payload"),
    )
}

fn full_surface() -> Surface {
    let service = Service::new().named("todos-service").routes(
        Routes::new()
            .with_repo(crate::AggregateRepository::<_, ManifestAggregate>::new(
                crate::InMemoryRepository::new(),
            ))
            .typed_command(
                typed_command::<CompleteInput, Succeeded<CompletePayload>>("todo.complete")
                    .field_name("todos_complete")
                    .roles(["admin", "user"]),
            )
            .handle(complete_handler)
            .typed_command(
                typed_command::<CompleteInput, Succeeded<CompletePayload>>("todo.force_archive")
                    .field_name("todos_force_archive")
                    .roles(["admin"]),
            )
            .handle(complete_handler),
    );
    build_surface(
        &[todos(), users(), memberships()],
        &SurfaceOptions::sqlite(),
    )
    .expect("surface")
    .with_service(&service)
    .expect("typed service")
    .with_projectors([
        SurfaceProjector::new("project_todos")
            .facts(["todo.completed"])
            .models(["TodoView"]),
        SurfaceProjector::new("project_users")
            .facts(["user.changed"])
            .models(["UserView"]),
    ])
    .expect("projectors")
}

fn causal_full_surface() -> Surface {
    let service = Service::new().named("todos-service").routes(
        Routes::new()
            .with_repo(crate::AggregateRepository::<_, ManifestAggregate>::new(
                crate::InMemoryRepository::new(),
            ))
            .typed_command(
                typed_command::<CompleteInput, Eventual<CompletePayload>>("todo.complete")
                    .field_name("todos_complete")
                    .roles(["admin", "user"])
                    .emits(crate::events![ManifestTodoProjected])
                    .applies(crate::event_preview! {
                        ManifestTodoProjected => ManifestTodoProjected {
                            todo_id: input.todo_id,
                            ..unknown
                        }
                    }),
            )
            .handle(complete_causal_handler),
    );
    build_surface(
        &[todos(), users(), memberships()],
        &SurfaceOptions::sqlite(),
    )
    .expect("surface")
    .with_service(&service)
    .expect("typed service")
}

fn modeled_surface(
    state: crate::projection::placement::ProjectionBindingState,
    execution_class: crate::projection::placement::ProjectionExecutionClass,
) -> Surface {
    modeled_surface_with_base(
        full_surface(),
        state,
        execution_class,
        crate::ProjectionPartition::Unit,
    )
}

fn modeled_causal_surface(
    state: crate::projection::placement::ProjectionBindingState,
    execution_class: crate::projection::placement::ProjectionExecutionClass,
) -> Surface {
    modeled_surface_with_base(
        causal_full_surface(),
        state,
        execution_class,
        crate::ProjectionPartition::Unit,
    )
}

fn modeled_surface_with_partition(
    state: crate::projection::placement::ProjectionBindingState,
    execution_class: crate::projection::placement::ProjectionExecutionClass,
    partition: crate::ProjectionPartition,
) -> Surface {
    modeled_surface_with_base(full_surface(), state, execution_class, partition)
}

fn modeled_surface_with_base(
    mut surface: Surface,
    state: crate::projection::placement::ProjectionBindingState,
    execution_class: crate::projection::placement::ProjectionExecutionClass,
    partition: crate::ProjectionPartition,
) -> Surface {
    use crate::projection::catalog::ProjectionBindingActivation;
    use crate::projection::placement::{
        ProjectionBinding, ProjectionExecutorRoute, ProjectionOutput, ProjectionOwner,
        ProjectionPhysicalTopology, ProjectionSourceBinding, PROJECTION_PARTITION_CODEC_VERSION,
    };
    use crate::projection::{
        ProjectionArm, ProjectionField, ProjectionOperation, ProjectionTarget,
    };
    use crate::projection_protocol::{ProjectionEpoch, ProjectorTopologyId};
    use crate::{
        ProjectionAssignment, ProjectionExpression, ProjectionKeyField, ProjectionMutationKind,
        ProjectionProgram, ProjectionValueType,
    };

    let selector = crate::ProjectionEventSelector::try_from_descriptor(
        &<ManifestTodoProjected as crate::DomainEvent>::DESCRIPTOR,
    )
    .unwrap();
    let key = ProjectionKeyField::try_new(
        0,
        "todo_id",
        ProjectionExpression::body_path(ProjectionValueType::String, ["todo_id"]).unwrap(),
    )
    .unwrap();
    let field = ProjectionField::try_new(
        1,
        "title",
        ProjectionAssignment::Set(
            ProjectionExpression::body_path(ProjectionValueType::String, ["todo_id"]).unwrap(),
        ),
    )
    .unwrap();
    let key_field = ProjectionField::try_new(
        0,
        "todo_id",
        ProjectionAssignment::Set(
            ProjectionExpression::body_path(ProjectionValueType::String, ["todo_id"]).unwrap(),
        ),
    )
    .unwrap();
    let operation = ProjectionOperation::try_new(
        "project-todo",
        0,
        ProjectionMutationKind::Upsert,
        ProjectionTarget::try_new("TodoView", "todos").unwrap(),
        vec![key],
        vec![key_field, field],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let arm = ProjectionArm::try_new("todo-projected", selector, vec![operation]).unwrap();
    let program =
        ProjectionProgram::try_new("project-todos-modeled", 1, partition, vec![arm]).unwrap();
    let binding = ProjectionBinding::from_eventual_program(
        &program,
        ProjectionSourceBinding::try_new("todo-domain", "ordered-domain-events", 1).unwrap(),
        ProjectionOwner::try_new("project-todos-modeled").unwrap(),
        execution_class,
        "distributed-projection-partition",
        PROJECTION_PARTITION_CODEC_VERSION,
        vec![ProjectionOutput::try_new("TodoView", "todos", todos()).unwrap()],
        Vec::new(),
        Some(ProjectionPhysicalTopology::from_protocol(
            &ProjectorTopologyId::new(1, "project-todos-modeled", [0x44; 32]).unwrap(),
        )),
    )
    .unwrap();
    let catalog =
        crate::projection::catalog::ProjectionCatalog::try_new(vec![binding.clone()]).unwrap();
    let active = catalog
        .activate(
            vec![ProjectionBindingActivation::new(
                binding.id(),
                binding.program_id(),
                ProjectionEpoch::new("todos-modeled-v1").unwrap(),
                state,
                Some(ProjectionExecutorRoute::local("todos-service").unwrap()),
            )],
            None,
        )
        .unwrap();
    let modeled = crate::graphql::SurfaceModeledProjection::try_from_catalog(
        program,
        &catalog,
        &active,
        binding.id(),
    )
    .unwrap();

    surface.projectors.clear();
    surface.projectors_attached = false;
    let command = surface
        .commands
        .iter_mut()
        .find(|command| command.command_name == "todo.complete")
        .unwrap();
    if command.projections.selectors.is_empty() {
        command
            .projections
            .add_event_set(crate::events![ManifestTodoProjected]);
        command.projections.add_preview(crate::event_preview! {
            ManifestTodoProjected => ManifestTodoProjected {
                todo_id: input.todo_id,
                ..unknown
            }
        });
    }
    surface
        .with_projectors([SurfaceProjector::new("project-todos-modeled").modeled(modeled)])
        .unwrap()
}

fn projected_surface() -> Surface {
    use crate::command::{CommandEffects, CommandProjectedModel, EffectExpression};

    let todo_schema: &'static TableSchema = Box::leak(Box::new(todos()));
    let mut surface = build_surface(&[todo_schema.clone(), users()], &SurfaceOptions::sqlite())
        .expect("projected surface");
    let todo_model = surface.models["TodoView"].clone();
    let mut output_fields = todo_model
        .columns
        .iter()
        .map(|column| SurfaceTypeField {
            name: column.name.clone(),
            type_name: column.scalar.clone(),
            nullable: column.nullable,
            list: false,
            item_nullable: false,
            nested: None,
        })
        .collect::<Vec<_>>();
    output_fields.sort_by(|left, right| left.name.cmp(&right.name));
    surface.commands = vec![SurfaceCommand {
        command_name: "todo.project".into(),
        field_name: "todo_project".into(),
        roles: vec!["user".into()],
        input: SurfaceCommandShape::Typed(SurfaceTypeDef {
            name: "ProjectTodoInput".into(),
            fields: vec![SurfaceTypeField {
                name: "todo_id".into(),
                type_name: "String".into(),
                nullable: false,
                list: false,
                item_nullable: false,
                nested: None,
            }],
        }),
        output: SurfaceCommandShape::Typed(SurfaceTypeDef {
            name: todo_model.object_name,
            fields: output_fields,
        }),
        consistency: CommandConsistency::Atomic,
        input_defaults: Vec::new(),
        effects: Some(CommandEffects::revalidate()),
        confirmations: Vec::new(),
        projected_model: Some(CommandProjectedModel {
            output_type_id: TypeId::of::<()>(),
            model: "TodoView".into(),
            table: "todos".into(),
            schema: todo_schema,
            partition: Some(EffectExpression::Input {
                path: vec!["todo_id".into()],
            }),
        }),
        direct_projection: None,
        projections: Default::default(),
        confirmation_unavailable: false,
    }];
    surface.commands_attached = true;
    surface
        .with_projectors([SurfaceProjector::new("project_todo_domain")
            .facts(["todo.changed", "user.changed"])
            .models(["TodoView", "UserView"])
            .partition_by(["todo_id"])
            .change_epoch("todo-domain-v1")])
        .expect("projected topology")
}

#[test]
fn projected_command_exports_opaque_role_safe_direct_target() {
    let full = projected_surface();
    let selected = surface_for_role(
        &full,
        "user",
        &BTreeMap::from([("TodoView".into(), RoleGrant::all_columns())]),
    )
    .expect("role selection");
    assert!(
        selected.projectors.is_empty(),
        "the multi-model owner must be omitted when one owned model is denied"
    );

    let manifest = client_manifest_from_surface(
        "todos-service",
        ClientSurfaceIdentity::role("user"),
        &selected,
    )
    .expect("projected client manifest");
    let direct = manifest.commands[0]
        .extensions
        .direct_projection
        .as_ref()
        .expect("projected direct target");
    assert_eq!(direct.topology.version, 1);
    assert_eq!(direct.topology.name, "project_todo_domain");
    assert_eq!(direct.topology.digest.len(), 71);
    assert!(direct.topology.digest.starts_with("sha256:"));
    assert!(direct.topology.digest[7..]
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
    assert_eq!(direct.model, "TodoView");
    assert_eq!(
        direct.partition,
        Some(serde_json::json!({"kind": "input", "path": ["todo_id"]}))
    );
    assert_eq!(direct.change_epoch, "todo-domain-v1");

    let direct_wire = serde_json::to_value(direct).expect("direct target wire");
    assert_eq!(
        direct_wire
            .as_object()
            .expect("direct target object")
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["change_epoch", "model", "partition", "topology"])
    );
    let wire = serde_json::to_string(&manifest).expect("client manifest wire");
    assert!(!wire.contains("UserView"));
    assert!(!wire.contains("users"));
    assert!(!wire.contains("user.changed"));
}

#[test]
fn legacy_effect_presets_are_not_v2_client_authority() {
    use crate::command::{
        CommandEffect, CommandEffects, EffectExpression, EffectFieldValue, EffectKey,
    };

    let mut full = projected_surface();
    full.commands[0].effects = Some(CommandEffects::new([CommandEffect::Patch {
        model: "TodoView".into(),
        key: EffectKey {
            fields: vec![EffectFieldValue {
                field: "todo_id".into(),
                value: EffectExpression::Input {
                    path: vec!["todo_id".into()],
                },
            }],
        },
        fields: vec![EffectFieldValue {
            field: "owner_id".into(),
            value: EffectExpression::TrustedPreset {
                name: "x-user-id".into(),
            },
        }],
    }]));
    let selected = surface_for_role(
        &full,
        "user",
        &BTreeMap::from([
            ("TodoView".into(), RoleGrant::all_columns()),
            ("UserView".into(), RoleGrant::all_columns()),
        ]),
    )
    .expect("role selection");
    let manifest = client_manifest_from_surface(
        "todos-service",
        ClientSurfaceIdentity::role("user"),
        &selected,
    )
    .expect("trusted preset descriptor manifest");

    assert!(manifest.commands[0].extensions.trusted_presets.is_empty());
    let wire = serde_json::to_value(manifest).expect("manifest JSON");
    assert!(wire["commands"][0]["extensions"].get("effects").is_none());
    assert!(wire["commands"][0]["extensions"]
        .get("confirmations")
        .is_none());
    assert!(
        !wire.to_string().contains("preset-value"),
        "static artifacts must never freeze a runtime preset value"
    );
}

#[test]
fn projected_command_export_rejects_absent_or_wrong_direct_target() {
    let full = projected_surface();
    let mut selected = surface_for_role(
        &full,
        "user",
        &BTreeMap::from([
            ("TodoView".into(), RoleGrant::all_columns()),
            ("UserView".into(), RoleGrant::all_columns()),
        ]),
    )
    .expect("role selection");

    selected.commands[0].direct_projection = None;
    let error = client_manifest_from_surface(
        "todos-service",
        ClientSurfaceIdentity::role("user"),
        &selected,
    )
    .expect_err("projected command cannot omit direct target");
    assert!(error
        .0
        .contains("missing its bound direct projection target"));

    let mut selected = surface_for_role(
        &full,
        "user",
        &BTreeMap::from([
            ("TodoView".into(), RoleGrant::all_columns()),
            ("UserView".into(), RoleGrant::all_columns()),
        ]),
    )
    .expect("role selection");
    selected.commands[0]
        .direct_projection
        .as_mut()
        .expect("bound target")
        .model = "UserView".into();
    let error = client_manifest_from_surface(
        "todos-service",
        ClientSurfaceIdentity::role("user"),
        &selected,
    )
    .expect_err("direct target cannot name a different retained model");
    assert!(error.0.contains("differs from retained model"));
}

fn grants() -> BTreeMap<String, BTreeMap<String, RoleGrant>> {
    BTreeMap::from([
        (
            "admin".into(),
            BTreeMap::from([
                (
                    "TodoView".into(),
                    RoleGrant::all_columns().with_aggregations(),
                ),
                (
                    "UserView".into(),
                    RoleGrant::all_columns().with_aggregations(),
                ),
                ("MembershipView".into(), RoleGrant::all_columns()),
            ]),
        ),
        (
            "user".into(),
            BTreeMap::from([
                (
                    "TodoView".into(),
                    RoleGrant::all_columns()
                        .rows(col("owner_id").eq(claim("x-user-id")))
                        .limit(25),
                ),
                ("UserView".into(), RoleGrant::columns(["display_name"])),
                (
                    "MembershipView".into(),
                    RoleGrant::columns(["tenant_id", "role"]),
                ),
            ]),
        ),
    ])
}

fn manifest_for_all_models(
    service_id: &str,
    role: &str,
    full: &Surface,
) -> DistributedClientManifest {
    let grants = full
        .models
        .keys()
        .map(|model| (model.clone(), RoleGrant::all_columns().with_aggregations()))
        .collect();
    let selected = surface_for_role(full, role, &grants).expect("role surface");
    client_manifest_from_surface(service_id, ClientSurfaceIdentity::role(role), &selected)
        .expect("client manifest")
}

#[test]
fn role_manifest_is_deterministic_and_hides_denied_identity_and_commands() {
    let full = full_surface();
    let selected = surface_for_role(&full, "user", &grants()["user"]).unwrap();
    let export = DistributedClientSurfaceExport::from_selected("todos-service", selected)
        .expect("role-selected Surface");
    let first = export.manifest().unwrap();
    let second = export.manifest().unwrap();
    assert_eq!(first, second);
    assert_eq!(first.manifest_version, 2);
    assert_eq!(first.schema_fingerprint, second.schema_fingerprint);
    assert_eq!(
        first.schema_fingerprint,
        "sha256:d170cb2de47ed71c0127206a5a42970abff278cec2bcb494551da554053f3a83"
    );
    assert_eq!(
        first.protocol_fingerprint,
        "sha256:00fb342f3acb4dc1c1716a43cc3001c748d5f6c500ff831690d820e9e43e2782"
    );

    let user = first
        .models
        .iter()
        .find(|model| model.id == "UserView")
        .unwrap();
    assert_eq!(user.normalization, ModelNormalization::Embedded);
    assert!(user.record_revisions && user.tombstones);
    assert_eq!(
        user.fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        vec!["display_name"]
    );
    let todo = first
        .models
        .iter()
        .find(|model| model.id == "TodoView")
        .unwrap();
    assert!(todo.record_revisions && todo.tombstones);
    assert_eq!(
        todo.row_policy,
        ClientRowPolicy::Predicate {
            expression: col("owner_id").eq(claim("x-user-id")),
        }
    );
    assert_eq!(
        trusted_preset_descriptors(&first).unwrap(),
        vec![ClientTrustedPresetDescriptor {
            name: "x-user-id".into(),
            codec: "string".into(),
        }]
    );
    let owner = todo
        .relationships
        .iter()
        .find(|rel| rel.name == "owner")
        .unwrap();
    assert!(owner.nullable);
    assert_eq!(owner.key_mapping, RelationshipKeyMapping::Embedded);
    assert_eq!(owner.maintenance, ClientRelationshipMaintenance::Revalidate);
    assert_eq!(owner.dependencies, vec!["todos", "users"]);
    assert!(
        !owner.live,
        "singular relationships are not live list plans"
    );
    assert!(owner.arguments.is_empty());
    assert!(owner.filter.is_none());
    assert_eq!(todo.filter_input.type_name, "todos_bool_exp");
    assert_eq!(
        todo.filter_input
            .relationships
            .iter()
            .find(|relationship| relationship.field == "owner")
            .expect("singular relationship predicate")
            .target_type,
        user.filter_input.type_name
    );
    assert_eq!(user.filter_input.type_name, "users_bool_exp");
    assert!(first.capabilities.live_queries);
    assert!(!first.capabilities.record_revisions);
    assert!(!first.capabilities.tombstones);
    assert!(!first.capabilities.live_resume);
    assert_eq!(first.capabilities.query_fallback, "revalidate");
    assert!(first.capabilities.causal_receipts);
    assert!(first.capabilities.cache_scope);
    let membership = first
        .models
        .iter()
        .find(|model| model.id == "MembershipView")
        .unwrap();
    assert!(!membership.record_revisions && !membership.tombstones);
    let status = first
        .protocol_operations
        .command_status
        .as_ref()
        .expect("causal surfaces generate the framework status operation");
    assert_eq!(status.name, "Distributed_CommandStatus");
    assert_eq!(status.operation, command_status_operation());
    assert_eq!(
        status.operation_hash,
        hash_bytes(status.operation.as_bytes())
    );
    assert!(first
        .commands
        .iter()
        .any(|command| command.name == "todo.complete"));
    assert!(!first
        .commands
        .iter()
        .any(|command| command.name == "todo.force_archive"));
    assert_eq!(first.commands[0].grants, vec!["user"]);
    assert!(first.commands.iter().all(|command| {
        command.extensions.consistency.kind == "succeeded"
            && command.extensions.effects.is_none()
            && command.extensions.confirmations.is_none()
    }));

    let json = serde_json::to_string(&first).unwrap();
    assert!(!json.contains("secret"));
    assert!(!json.contains("user_id"));
    assert!(!json.contains("force_archive"));
    assert!(
        json.contains("x-user-id"),
        "portable row policies expose only the static claim name, never its value"
    );
}

#[test]
fn role_and_application_partition_manifests_hide_raw_paths_and_denied_values() {
    use crate::projection::placement::{ProjectionBindingState, ProjectionExecutionClass};

    let mut full = modeled_surface_with_partition(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
        crate::ProjectionPartition::Expression(
            crate::ProjectionExpression::body_path(
                crate::ProjectionValueType::String,
                ["private_partition_path"],
            )
            .unwrap(),
        ),
    );
    full.commands
        .iter_mut()
        .find(|command| command.command_name == "todo.complete")
        .unwrap()
        .projections
        .add_preview(
            crate::command::CommandProjectionPreview::new()
                .events(crate::events![ManifestTodoProjected])
                .field(
                    ["private_partition_path"],
                    crate::command::CommandProjectionPreviewSource::constant(
                        crate::ProjectionValue::string("denied-partition-value"),
                    ),
                ),
        );
    let all_grants = grants();
    let role = surface_for_role(&full, "user", &all_grants["user"]).unwrap();
    let application = surface_for_application(
        &full,
        "web",
        &["user".into()],
        &["user".into()],
        &all_grants,
    )
    .unwrap();

    for (identity, selected) in [
        (ClientSurfaceIdentity::role("user"), role),
        (
            ClientSurfaceIdentity::application("web", ["user"], ["user"]),
            application,
        ),
    ] {
        let manifest = client_manifest_from_surface("todos-service", identity, &selected).unwrap();
        assert!(matches!(
            manifest.projection_programs[0].arms[0].partition,
            ClientProjectionPartition::Expression {
                expression: ClientProjectionExpression::Slot { .. }
            }
        ));
        let wire = serde_json::to_string(&manifest).unwrap();
        assert!(!wire.contains("private_partition_path"));
        assert!(!wire.contains("denied-partition-value"));
        assert!(!wire.contains("\"kind\":\"body_path\""));
    }
}

#[test]
fn manifest_v2_decode_rejects_unknown_versions_fields_and_legacy_authority() {
    let full = full_surface();
    let selected = surface_for_role(&full, "user", &grants()["user"]).unwrap();
    let baseline = client_manifest_from_surface(
        "todos-service",
        ClientSurfaceIdentity::role("user"),
        &selected,
    )
    .expect("baseline manifest");
    let value = serde_json::to_value(&baseline).expect("manifest value");

    for version in [1, 3] {
        let mut hostile = value.clone();
        hostile["manifest_version"] = serde_json::json!(version);
        assert!(serde_json::from_value::<DistributedClientManifest>(hostile).is_err());
    }

    let mut unknown = value.clone();
    unknown["projection_secret"] = serde_json::json!("must-fail");
    assert!(serde_json::from_value::<DistributedClientManifest>(unknown).is_err());

    let mut slots = value.clone();
    slots["commands"][0]["extensions"]["version"] = serde_json::json!(1);
    assert!(serde_json::from_value::<DistributedClientManifest>(slots).is_err());

    for legacy in ["effects", "confirmations"] {
        let mut hostile = value.clone();
        hostile["commands"][0]["extensions"][legacy] = serde_json::json!(null);
        assert!(
            serde_json::from_value::<DistributedClientManifest>(hostile).is_err(),
            "v2 must reject even null legacy `{legacy}` authority"
        );
    }

    let mut mixed = value.clone();
    mixed["commands"][0]["extensions"]["projection"] = serde_json::json!({
        "version": 1,
        "event_set": [],
        "program_arms": [],
        "preview_occurrences": [],
        "fallback": "revalidate"
    });
    mixed["commands"][0]["extensions"]["effects"] = serde_json::json!({
        "version": 1,
        "operations": [],
        "fallback": "revalidate"
    });
    assert!(serde_json::from_value::<DistributedClientManifest>(mixed).is_err());

    for version in [1, 3] {
        let mut program = value.clone();
        program["projection_programs"] = serde_json::json!([{
        "version": version,
        "program_id": "pp1:sha256:bad",
        "name": "bad",
        "program_version": 1,
        "ir_version": 1,
        "operation_semantics_version": 1,
        "arms": []
        }]);
        assert!(serde_json::from_value::<DistributedClientManifest>(program).is_err());
    }
    for field in ["ir_version", "operation_semantics_version"] {
        let mut program = serde_json::to_value(&baseline).unwrap();
        program["projection_programs"] = serde_json::json!([{
            "version": 2,
            "program_id": "pp1:sha256:bad",
            "name": "bad",
            "program_version": 1,
            "ir_version": 1,
            "operation_semantics_version": 1,
            "arms": []
        }]);
        program["projection_programs"][0][field] = serde_json::json!(2);
        assert!(
            serde_json::from_value::<DistributedClientManifest>(program).is_err(),
            "unknown `{field}` must fail closed"
        );
    }
    let valid_program = serde_json::json!({
        "version": 2,
        "program_id": "pp1:sha256:bad",
        "name": "bad",
        "program_version": 1,
        "ir_version": 1,
        "operation_semantics_version": 1,
        "arms": [{
            "arm": "pa1:sha256:bad",
            "event": {
                "id": "pe1:sha256:bad",
                "name": "todo.partitioned",
                "version": 1
            },
            "partition": {"kind": "unit"},
            "operations": []
        }]
    });
    for field in ["version", "partition"] {
        let mut hostile = serde_json::to_value(&baseline).unwrap();
        hostile["projection_programs"] = serde_json::json!([valid_program.clone()]);
        if field == "version" {
            hostile["projection_programs"][0]
                .as_object_mut()
                .unwrap()
                .remove(field);
        } else {
            hostile["projection_programs"][0]["arms"][0]
                .as_object_mut()
                .unwrap()
                .remove(field);
        }
        assert!(
            serde_json::from_value::<DistributedClientManifest>(hostile).is_err(),
            "missing required arm/program `{field}` must fail closed"
        );
    }
    let mut unknown_partition = serde_json::to_value(&baseline).unwrap();
    unknown_partition["projection_programs"] = serde_json::json!([valid_program]);
    unknown_partition["projection_programs"][0]["arms"][0]["partition"] =
        serde_json::json!({"kind": "opaque", "value": "private"});
    assert!(
        serde_json::from_value::<DistributedClientManifest>(unknown_partition).is_err(),
        "unknown partition semantics must fail closed"
    );

    let mut binding = value.clone();
    binding["projection_bindings"] = serde_json::json!([{
        "version": 2,
        "binding_id": "pb1:sha256:bad",
        "program_id": "pp1:sha256:bad",
        "epoch": "bad-v1",
        "state": "active",
        "placement": "eventual",
        "execution_class": "causal"
    }]);
    assert!(serde_json::from_value::<DistributedClientManifest>(binding).is_err());

    let mut extension = value;
    extension["commands"][0]["extensions"]["projection"] = serde_json::json!({
        "version": 3,
        "event_set": [],
        "program_arms": [],
        "preview_occurrences": [],
        "fallback": "revalidate"
    });
    assert!(serde_json::from_value::<DistributedClientManifest>(extension).is_err());

    assert_ne!(
        baseline.protocol_fingerprint,
        "sha256:30f19c9f4d29280a02ddf67c4df62cdc92c4e8090792f43d6b1bdafea3e31273",
        "manifest v2 projection semantics must not share the v1 protocol fingerprint"
    );
}

#[test]
fn manifest_decode_rejects_hostile_preview_occurrence_topology() {
    use crate::projection::placement::{ProjectionBindingState, ProjectionExecutionClass};

    let selected = surface_for_role(
        &modeled_surface(
            ProjectionBindingState::Active,
            ProjectionExecutionClass::Causal,
        ),
        "user",
        &grants()["user"],
    )
    .unwrap();
    let manifest = client_manifest_from_surface(
        "todos-service",
        ClientSurfaceIdentity::role("user"),
        &selected,
    )
    .unwrap();
    let baseline = serde_json::to_value(manifest).unwrap();
    let command_index = baseline["commands"]
        .as_array()
        .unwrap()
        .iter()
        .position(|command| command["name"] == "todo.complete")
        .unwrap();

    let mut ordinal = baseline.clone();
    ordinal["commands"][command_index]["extensions"]["projection"]["preview_occurrences"][0]
        ["ordinal"] = serde_json::json!(2);
    assert!(
        serde_json::from_value::<DistributedClientManifest>(ordinal).is_err(),
        "synthetic preview ordinals must be dense and zero-based"
    );

    let mut foreign_event = baseline.clone();
    foreign_event["commands"][command_index]["extensions"]["projection"]["preview_occurrences"]
        [0]["event"] = serde_json::json!({
        "id": "pe1:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        "name": "private.denied-event",
        "version": 1
    });
    assert!(
        serde_json::from_value::<DistributedClientManifest>(foreign_event).is_err(),
        "a preview occurrence cannot inject an event without a selected arm"
    );

    let mut duplicate_slot = baseline;
    let duplicate = duplicate_slot["commands"][command_index]["extensions"]["projection"]
        ["preview_occurrences"][0]["values"][0]
        .clone();
    duplicate_slot["commands"][command_index]["extensions"]["projection"]["preview_occurrences"][0]
        ["values"]
        .as_array_mut()
        .unwrap()
        .push(duplicate);
    assert!(
        serde_json::from_value::<DistributedClientManifest>(duplicate_slot).is_err(),
        "preview slots must be sorted and unique"
    );
}

#[test]
fn active_causal_modeled_projection_exports_exact_command_plan() {
    use crate::projection::placement::{ProjectionBindingState, ProjectionExecutionClass};

    let full = modeled_surface(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let selected = surface_for_role(&full, "user", &grants()["user"]).unwrap();
    let manifest = client_manifest_from_surface(
        "todos-service",
        ClientSurfaceIdentity::role("user"),
        &selected,
    )
    .unwrap();

    assert_eq!(manifest.projection_programs.len(), 1);
    assert_eq!(manifest.projection_bindings.len(), 1);
    assert_eq!(
        manifest.projection_bindings[0].state,
        ClientProjectionBindingState::Active
    );
    let operation = &manifest.projection_programs[0].arms[0].operations[0];
    assert_eq!(operation.key.len(), 1);
    assert_eq!(operation.key[0].name, "todo_id");
    assert_eq!(
        operation
            .fields
            .iter()
            .map(|field| (field.ordinal, field.name.as_str()))
            .collect::<Vec<_>>(),
        vec![(0, "title")],
        "complete-row primary keys travel only in the canonical key"
    );
    let command = manifest
        .commands
        .iter()
        .find(|command| command.name == "todo.complete")
        .unwrap();
    let projection = command
        .extensions
        .projection
        .as_ref()
        .expect("active eventual causal binding can mint preview work");
    assert_eq!(projection.event_set.len(), 1);
    assert_eq!(projection.program_arms.len(), 1);
    assert_eq!(projection.preview_occurrences.len(), 1);
    assert_eq!(projection.preview_occurrences[0].ordinal, 0);
    assert_eq!(projection.preview_occurrences[0].values.len(), 2);
    assert!(projection.preview_occurrences[0]
        .values
        .iter()
        .all(|preview| matches!(preview.source, ClientProjectionPreviewSource::Input { .. })));
    let wire = serde_json::to_string(&manifest).unwrap();
    assert!(!wire.contains("domain_event"));
    assert!(!wire.contains("body_schema"));
    assert!(!wire.contains("\"kind\":\"body_path\""));
}

#[test]
fn client_projection_arms_are_canonical_across_declaration_order() {
    use super::projections::lower_program_for_test;
    use crate::graphql::surface::{
        SurfaceProjectionArm, SurfaceProjectionOperation, SurfaceSelectedProjectionProgram,
    };
    use crate::projection::{
        PROJECTION_OPERATION_SEMANTICS_VERSION, PROJECTION_PROGRAM_IR_VERSION,
    };
    use crate::{
        ProjectionAssignment, ProjectionExpression, ProjectionField, ProjectionKeyField,
        ProjectionMutationKind, ProjectionPartition, ProjectionValueType,
    };

    let surface = build_surface(&[todos(), users()], &SurfaceOptions::sqlite()).unwrap();
    let operation = || SurfaceProjectionOperation {
        operation_id: "project-todo".into(),
        staging_ordinal: 0,
        kind: ProjectionMutationKind::Patch,
        model: "TodoView".into(),
        storage: "todos".into(),
        key: vec![ProjectionKeyField::try_new(
            0,
            "todo_id",
            ProjectionExpression::body_path(ProjectionValueType::String, ["todo_id"]).unwrap(),
        )
        .unwrap()],
        fields: vec![ProjectionField::try_new(
            0,
            "title",
            ProjectionAssignment::Set(
                ProjectionExpression::body_path(ProjectionValueType::String, ["todo_id"]).unwrap(),
            ),
        )
        .unwrap()],
        relationship_effects: Vec::new(),
        invalidations: Vec::new(),
        force_revalidate: false,
    };
    let arm = |name: &str, descriptor: &crate::DomainEventDescriptor| SurfaceProjectionArm {
        arm_id: name.into(),
        selector: crate::ProjectionEventSelector::try_from_descriptor(descriptor).unwrap(),
        operations: vec![operation()],
    };
    let archived = arm(
        "archived",
        &<ManifestTodoArchived as crate::DomainEvent>::DESCRIPTOR,
    );
    let projected = arm(
        "projected",
        &<ManifestTodoProjected as crate::DomainEvent>::DESCRIPTOR,
    );
    let selected = |arms| SurfaceSelectedProjectionProgram {
        name: "canonical-client-arms".into(),
        version: 1,
        ir_version: PROJECTION_PROGRAM_IR_VERSION,
        operation_semantics_version: PROJECTION_OPERATION_SEMANTICS_VERSION,
        partition: ProjectionPartition::Unit,
        arms,
    };
    let program_id = crate::ProjectionProgramId::parse(
        "pp1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    .unwrap();
    let left = lower_program_for_test(
        program_id,
        &selected(vec![projected.clone(), archived.clone()]),
        &surface,
    )
    .unwrap();
    let right =
        lower_program_for_test(program_id, &selected(vec![archived, projected]), &surface).unwrap();

    assert_eq!(left.arms, right.arms);
    assert!(left
        .arms
        .windows(2)
        .all(|pair| (&pair[0].event, &pair[0].arm) < (&pair[1].event, &pair[1].arm)));
}

#[test]
fn draining_and_background_bindings_are_visible_but_cannot_mint_command_work() {
    use crate::projection::placement::{ProjectionBindingState, ProjectionExecutionClass};

    for (state, class, expected_state) in [
        (
            ProjectionBindingState::Draining,
            ProjectionExecutionClass::Causal,
            ClientProjectionBindingState::Draining,
        ),
        (
            ProjectionBindingState::Active,
            ProjectionExecutionClass::Background,
            ClientProjectionBindingState::Active,
        ),
    ] {
        let full = modeled_surface(state, class);
        let selected = surface_for_role(&full, "user", &grants()["user"]).unwrap();
        let manifest = client_manifest_from_surface(
            "todos-service",
            ClientSurfaceIdentity::role("user"),
            &selected,
        )
        .unwrap();
        assert_eq!(manifest.projection_programs.len(), 1);
        assert_eq!(manifest.projection_bindings[0].state, expected_state);
        assert!(manifest
            .commands
            .iter()
            .find(|command| command.name == "todo.complete")
            .unwrap()
            .extensions
            .projection
            .is_none());
    }
}

#[test]
fn draining_manifest_compiles_current_revalidation_without_projection_authority() {
    use crate::projection::placement::{ProjectionBindingState, ProjectionExecutionClass};
    use distributed_cli::{
        compile_client, ClientCompileInput, ClientDocument,
        ClientSurfaceSelector as CompilerSurfaceSelector,
    };

    let full = modeled_causal_surface(
        ProjectionBindingState::Draining,
        ProjectionExecutionClass::Causal,
    );
    let selected = surface_for_role(&full, "admin", &grants()["admin"]).unwrap();
    let manifest = client_manifest_from_surface(
        "todos-service",
        ClientSurfaceIdentity::role("admin"),
        &selected,
    )
    .unwrap();
    assert_eq!(
        manifest.projection_bindings[0].state,
        ClientProjectionBindingState::Draining
    );
    let project = compile_client(ClientCompileInput::new(
        serde_json::to_value(manifest).unwrap(),
        CompilerSurfaceSelector::role("admin"),
        vec![ClientDocument::new(
            "src/routes/todos/+page.graphql",
            "query Todos { todos { todo_id } }",
        )],
    ))
    .expect("compile the server-generated Draining manifest");
    let generated = project
        .files
        .iter()
        .find(|file| file.path == "commands.ts")
        .expect("generated commands module")
        .contents
        .as_str();
    let command = generated
        .split_once("export const Command_todos_complete:")
        .expect("generated todo.complete command")
        .1;
    let artifact_start = command.find(" = {").expect("generated command artifact") + 3;
    let artifact_end = command[artifact_start..]
        .find(";\n\nexport function prepareCommand_todos_complete")
        .expect("generated command artifact terminator")
        + artifact_start;
    let artifact_json = &command[artifact_start..artifact_end];
    let artifact: serde_json::Value =
        serde_json::from_str(artifact_json).expect("generated command artifact JSON");

    assert!(
        artifact.get("projection").is_none(),
        "Draining cannot mint or apply projection work"
    );
    assert_eq!(artifact["revalidation"]["required"], true);
    assert_eq!(
        artifact["revalidation"]["models"],
        serde_json::json!(["MembershipView", "TodoView", "UserView"])
    );
    assert_eq!(
        format!("{artifact_json}\n"),
        include_str!("../../../tests/fixtures/generated-draining-command-v2.json"),
        "the JS bridge fixture must remain byte-exact with the current server manifest and compiler"
    );
}

#[test]
fn denied_modeled_projection_exports_no_program_event_slot_or_preset_identity() {
    use crate::projection::placement::{ProjectionBindingState, ProjectionExecutionClass};

    let mut full = modeled_surface(
        ProjectionBindingState::Active,
        ProjectionExecutionClass::Causal,
    );
    let command = full
        .commands
        .iter_mut()
        .find(|command| command.command_name == "todo.complete")
        .unwrap();
    command.projections.previews[0].preview.fields[0].source =
        crate::command::CommandProjectionPreviewSource::trusted("denied-owner-secret", "string");
    let selected = surface_for_role(
        &full,
        "user",
        &BTreeMap::from([("UserView".into(), RoleGrant::all_columns())]),
    )
    .unwrap();
    let manifest = client_manifest_from_surface(
        "todos-service",
        ClientSurfaceIdentity::role("user"),
        &selected,
    )
    .unwrap();
    assert!(manifest.projection_programs.is_empty());
    assert!(manifest.projection_bindings.is_empty());
    let wire = serde_json::to_string(&manifest).unwrap();
    for hidden in [
        "todo.projected",
        "denied-owner-secret",
        "project-todos-modeled",
    ] {
        assert!(!wire.contains(hidden), "leaked `{hidden}` in {wire}");
    }
}

#[test]
fn filter_execution_limits_are_schema_fingerprinted_without_changing_protocol_epoch() {
    let full = full_surface();
    let selected = surface_for_role(&full, "user", &grants()["user"]).unwrap();
    let baseline = DistributedClientSurfaceExport::from_selected("todos-service", selected.clone())
        .unwrap()
        .manifest()
        .unwrap();

    let mut bool_limits = ClientExecutionLimits::default();
    bool_limits.max_bool_width += 1;
    let bool_manifest = DistributedClientSurfaceExport::from_selected_with_execution(
        "todos-service",
        selected.clone(),
        bool_limits,
    )
    .unwrap()
    .manifest()
    .unwrap();

    let mut in_limits = ClientExecutionLimits::default();
    in_limits.max_in_list += 1;
    let in_manifest = DistributedClientSurfaceExport::from_selected_with_execution(
        "todos-service",
        selected,
        in_limits,
    )
    .unwrap()
    .manifest()
    .unwrap();

    assert_ne!(
        baseline.schema_fingerprint,
        bool_manifest.schema_fingerprint
    );
    assert_ne!(baseline.schema_fingerprint, in_manifest.schema_fingerprint);
    assert_ne!(
        bool_manifest.schema_fingerprint,
        in_manifest.schema_fingerprint
    );
    assert_eq!(baseline.protocol_version, 1);
    assert_eq!(
        baseline.protocol_fingerprint,
        bool_manifest.protocol_fingerprint
    );
    assert_eq!(
        baseline.protocol_fingerprint,
        in_manifest.protocol_fingerprint
    );
}

#[test]
fn belongs_to_relationships_stay_nullable_independent_of_fk_storage() {
    let mut fingerprints = Vec::new();
    for nullable in [false, true] {
        let mut todo_schema = todos();
        todo_schema
            .columns
            .iter_mut()
            .find(|column| column.column_name == "owner_id")
            .expect("owner foreign key")
            .nullable = nullable;
        let full = build_surface(&[todo_schema, users()], &SurfaceOptions::sqlite()).unwrap();
        let selected = surface_for_role(
            &full,
            "user",
            &BTreeMap::from([
                ("TodoView".into(), RoleGrant::all_columns()),
                ("UserView".into(), RoleGrant::all_columns()),
            ]),
        )
        .unwrap();
        let surface_nullable = selected.models["TodoView"]
            .relationships
            .iter()
            .find(|relationship| relationship.name == "owner")
            .expect("surface relationship")
            .nullable;
        assert!(surface_nullable);

        let manifest = client_manifest_from_surface(
            "todos-service",
            ClientSurfaceIdentity::role("user"),
            &selected,
        )
        .unwrap();
        fingerprints.push(manifest.schema_fingerprint.clone());
        let owner = manifest
            .models
            .iter()
            .find(|model| model.id == "TodoView")
            .unwrap()
            .relationships
            .iter()
            .find(|relationship| relationship.name == "owner")
            .unwrap();
        assert_eq!(owner.nullable, surface_nullable);
        assert_eq!(serde_json::to_value(owner).unwrap()["nullable"], true);
    }
    assert_ne!(
        fingerprints[0], fingerprints[1],
        "foreign-key storage nullability remains part of the schema fingerprint"
    );
}

#[test]
fn query_protocol_capabilities_are_complete_or_explicitly_revalidate() {
    let fully_owned = build_surface(&[todos(), users()], &SurfaceOptions::sqlite())
        .unwrap()
        .with_projectors([
            SurfaceProjector::new("project_todos")
                .facts(["todo.changed"])
                .models(["TodoView"])
                .change_epoch("todos-v1"),
            SurfaceProjector::new("project_users")
                .facts(["user.changed"])
                .models(["UserView"])
                .partition_constant(serde_json::json!({"scope": "all"}))
                .change_epoch("users-v1"),
        ])
        .unwrap();
    let fully_owned = manifest_for_all_models("query-capabilities", "user", &fully_owned);
    assert!(fully_owned.capabilities.record_revisions);
    assert!(fully_owned.capabilities.tombstones);
    assert!(fully_owned.capabilities.live_resume);
    assert_eq!(fully_owned.capabilities.query_fallback, "revalidate");
    assert!(fully_owned
        .models
        .iter()
        .all(|model| model.record_revisions && model.tombstones));
    let wire = serde_json::to_value(&fully_owned).unwrap();
    assert_eq!(wire["capabilities"]["record_revisions"], true);
    assert_eq!(wire["capabilities"]["query_fallback"], "revalidate");
    assert!(wire["capabilities"].get("framework_revisions").is_none());
    assert!(wire["models"][0].get("framework_revision").is_none());

    let dynamic = build_surface(&[users()], &SurfaceOptions::sqlite())
        .unwrap()
        .with_projectors([SurfaceProjector::new("project_users")
            .facts(["user.changed"])
            .models(["UserView"])
            .partition_by(["tenant_id"])
            .change_epoch("users-v1")])
        .unwrap();
    let dynamic = manifest_for_all_models("query-capabilities", "user", &dynamic);
    assert!(dynamic.capabilities.record_revisions);
    assert!(dynamic.capabilities.tombstones);
    assert!(!dynamic.capabilities.live_resume);
    assert_eq!(dynamic.capabilities.query_fallback, "revalidate");

    let row_filtered = build_surface(&[users()], &SurfaceOptions::sqlite())
        .unwrap()
        .with_projectors([SurfaceProjector::new("project_users")
            .facts(["user.changed"])
            .models(["UserView"])
            .change_epoch("users-v1")])
        .unwrap();
    let row_filtered = surface_for_role(
        &row_filtered,
        "user",
        &BTreeMap::from([(
            "UserView".into(),
            RoleGrant::all_columns().rows(col("user_id").eq("visible-user")),
        )]),
    )
    .unwrap();
    let row_filtered = client_manifest_from_surface(
        "query-capabilities",
        ClientSurfaceIdentity::role("user"),
        &row_filtered,
    )
    .unwrap();
    assert!(row_filtered.capabilities.record_revisions);
    assert!(row_filtered.capabilities.tombstones);
    assert!(
        !row_filtered.capabilities.live_resume,
        "partition-wide positions and changes must not cross a row-authorization boundary"
    );

    let epochless = build_surface(&[users()], &SurfaceOptions::sqlite())
        .unwrap()
        .with_projectors([SurfaceProjector::new("project_users")
            .facts(["user.changed"])
            .models(["UserView"])])
        .unwrap();
    let epochless = manifest_for_all_models("query-capabilities", "user", &epochless);
    assert!(epochless.capabilities.record_revisions);
    assert!(epochless.capabilities.tombstones);
    assert!(!epochless.capabilities.live_resume);
    assert_eq!(epochless.capabilities.query_fallback, "revalidate");

    let unowned = build_surface(&[users()], &SurfaceOptions::sqlite()).unwrap();
    let unowned = manifest_for_all_models("query-capabilities", "user", &unowned);
    assert!(!unowned.capabilities.record_revisions);
    assert!(!unowned.capabilities.tombstones);
    assert!(!unowned.capabilities.live_resume);
    assert_eq!(unowned.capabilities.query_fallback, "revalidate");
    assert!(unowned
        .models
        .iter()
        .all(|model| !model.record_revisions && !model.tombstones));

    let mixed = build_surface(&[todos(), users()], &SurfaceOptions::sqlite())
        .unwrap()
        .with_projectors([SurfaceProjector::new("project_todos")
            .facts(["todo.changed"])
            .models(["TodoView"])
            .change_epoch("todos-v1")])
        .unwrap();
    let mixed = manifest_for_all_models("query-capabilities", "user", &mixed);
    assert!(!mixed.capabilities.record_revisions);
    assert!(!mixed.capabilities.tombstones);
    assert!(!mixed.capabilities.live_resume);
    assert_eq!(mixed.capabilities.query_fallback, "revalidate");
    assert!(mixed
        .models
        .iter()
        .find(|model| model.id == "TodoView")
        .is_some_and(|model| model.record_revisions && model.tombstones));
    assert!(mixed
        .models
        .iter()
        .find(|model| model.id == "UserView")
        .is_some_and(|model| !model.record_revisions && !model.tombstones));

    let uncovered_join = build_surface(
        &[teams(), users(), team_members()],
        &SurfaceOptions::sqlite(),
    )
    .unwrap()
    .with_projectors([
        SurfaceProjector::new("project_teams")
            .facts(["team.changed"])
            .models(["TeamView"])
            .change_epoch("teams-v1"),
        SurfaceProjector::new("project_users")
            .facts(["user.changed"])
            .models(["UserView"])
            .change_epoch("users-v1"),
    ])
    .unwrap();
    let uncovered_join = manifest_for_all_models("query-capabilities", "user", &uncovered_join);
    assert!(uncovered_join.capabilities.record_revisions);
    assert!(uncovered_join.capabilities.tombstones);
    assert!(!uncovered_join.capabilities.live_resume);
    assert_eq!(uncovered_join.capabilities.query_fallback, "revalidate");

    let mut query_only_options = SurfaceOptions::sqlite();
    query_only_options.subscriptions = false;
    let query_only = build_surface(&[users()], &query_only_options)
        .unwrap()
        .with_projectors([SurfaceProjector::new("project_users")
            .facts(["user.changed"])
            .models(["UserView"])
            .change_epoch("users-v1")])
        .unwrap();
    let query_only = manifest_for_all_models("query-capabilities", "user", &query_only);
    assert!(query_only.capabilities.record_revisions);
    assert!(query_only.capabilities.tombstones);
    assert!(!query_only.capabilities.live_queries);
    assert!(!query_only.capabilities.live_resume);
    assert_eq!(query_only.capabilities.query_fallback, "revalidate");

    let empty = build_surface(&[], &SurfaceOptions::sqlite()).unwrap();
    let empty = manifest_for_all_models("query-capabilities", "user", &empty);
    assert!(!empty.capabilities.record_revisions);
    assert!(!empty.capabilities.tombstones);
    assert!(!empty.capabilities.live_resume);
    assert_eq!(empty.capabilities.query_fallback, "revalidate");
}

#[test]
fn composite_keys_normalize_in_declared_order_and_hidden_keys_embed() {
    let full = full_surface();
    let admin = surface_for_role(&full, "admin", &grants()["admin"]).unwrap();
    let admin_manifest = client_manifest_from_surface(
        "todos-service",
        ClientSurfaceIdentity::role("admin"),
        &admin,
    )
    .unwrap();
    let membership = admin_manifest
        .models
        .iter()
        .find(|model| model.id == "MembershipView")
        .unwrap();
    let ModelNormalization::Normalized { fields, encoding } = &membership.normalization else {
        panic!("composite model should normalize")
    };
    assert_eq!(
        fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        vec!["tenant_id", "user_id"]
    );
    assert_eq!(encoding, KEY_ENCODING);

    let user = surface_for_role(&full, "user", &grants()["user"]).unwrap();
    let user_manifest =
        client_manifest_from_surface("todos-service", ClientSurfaceIdentity::role("user"), &user)
            .unwrap();
    let membership = user_manifest
        .models
        .iter()
        .find(|model| model.id == "MembershipView")
        .unwrap();
    assert_eq!(membership.normalization, ModelNormalization::Embedded);
    assert!(!serde_json::to_string(membership)
        .unwrap()
        .contains("user_id"));
}

#[test]
fn bigint_keys_embed_until_decimal_string_identity_is_available() {
    let accounts = TableSchema {
        model_name: "AccountView".into(),
        table_name: "accounts".into(),
        columns: vec![TableColumn {
            primary_key: true,
            ..column("account_id", ColumnType::Integer)
        }],
        primary_key: PrimaryKey::new(["account_id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    };
    let invoices = TableSchema {
        model_name: "InvoiceView".into(),
        table_name: "invoices".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..column("invoice_id", ColumnType::Text)
            },
            column("account_id", ColumnType::Integer),
        ],
        primary_key: PrimaryKey::new(["invoice_id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: vec![RelationshipDef {
            field_name: "account".into(),
            kind: RelationshipKind::BelongsTo,
            target_model: "AccountView".into(),
            foreign_key: Some("account_id".into()),
            through: None,
            target_foreign_key: None,
        }],
        kind: TableKind::ReadModel,
    };
    let full = build_surface(&[accounts, invoices], &SurfaceOptions::sqlite()).unwrap();
    let selected = surface_for_role(
        &full,
        "user",
        &BTreeMap::from([
            (
                "AccountView".into(),
                RoleGrant::all_columns().rows(col("account_id").eq(9_007_199_254_740_992_i64)),
            ),
            ("InvoiceView".into(), RoleGrant::all_columns()),
        ]),
    )
    .unwrap();
    let manifest = DistributedClientSurfaceExport::from_selected("billing", selected)
        .unwrap()
        .manifest()
        .unwrap();

    let account = manifest
        .models
        .iter()
        .find(|model| model.id == "AccountView")
        .unwrap();
    assert_eq!(account.normalization, ModelNormalization::Embedded);
    assert_eq!(account.row_policy, ClientRowPolicy::ServerOnly);
    assert!(!serde_json::to_string(account)
        .unwrap()
        .contains("9007199254740992"));
    let relationship = manifest
        .models
        .iter()
        .find(|model| model.id == "InvoiceView")
        .unwrap()
        .relationships
        .iter()
        .find(|relationship| relationship.name == "account")
        .unwrap();
    assert_eq!(relationship.key_mapping, RelationshipKeyMapping::Embedded);
    assert_eq!(
        relationship.maintenance,
        ClientRelationshipMaintenance::Revalidate
    );
}

#[test]
fn application_surface_is_common_contract_with_safe_role_limit_semantics() {
    let full = full_surface();
    let all_grants = grants();
    let selected = surface_for_application(
        &full,
        "web",
        &["user".into(), "admin".into()],
        &["user".into(), "admin".into()],
        &all_grants,
    )
    .unwrap();
    let manifest = client_manifest_from_surface(
        "todos-service",
        ClientSurfaceIdentity::application("web", ["admin", "user"], ["admin", "user"]),
        &selected,
    )
    .unwrap();
    assert!(!manifest
        .commands
        .iter()
        .any(|command| command.name == "todo.force_archive"));
    let todos = manifest
        .roots
        .iter()
        .find(|root| root.id == "query:todos")
        .unwrap();
    assert_eq!(todos.pagination.as_ref().unwrap().default_limit, 25);
    assert_eq!(todos.pagination.as_ref().unwrap().max_limit, 25);
    assert!(matches!(
        todos.filter.as_ref().unwrap().row_policy,
        ClientRowPolicy::ServerOnly
    ));

    let admin = surface_for_role(&full, "admin", &all_grants["admin"]).unwrap();
    let admin_manifest = client_manifest_from_surface(
        "todos-service",
        ClientSurfaceIdentity::role("admin"),
        &admin,
    )
    .unwrap();
    let admin_todos = admin_manifest
        .roots
        .iter()
        .find(|root| root.id == "query:todos")
        .unwrap();
    assert_eq!(admin_todos.pagination.as_ref().unwrap().default_limit, 100);
    assert_eq!(admin_todos.pagination.as_ref().unwrap().max_limit, 1000);
    assert_ne!(
        manifest.schema_fingerprint,
        admin_manifest.schema_fingerprint
    );
    assert_eq!(
        manifest.protocol_fingerprint,
        admin_manifest.protocol_fingerprint
    );
}

#[test]
fn mixed_target_projectors_are_omitted_for_role_and_application_surfaces() {
    let full = build_surface(&[todos(), users(), teams()], &SurfaceOptions::sqlite())
        .unwrap()
        .with_projectors([
            SurfaceProjector::new("project_todos")
                .facts(["todo.changed"])
                .models(["TodoView"]),
            SurfaceProjector::new("project_user_team")
                .facts(["private-user.changed"])
                .models(["UserView", "TeamView"]),
        ])
        .unwrap();
    let restricted = BTreeMap::from([
        ("TodoView".into(), RoleGrant::all_columns()),
        ("UserView".into(), RoleGrant::all_columns()),
    ]);
    let admin = BTreeMap::from([
        ("TodoView".into(), RoleGrant::all_columns()),
        ("UserView".into(), RoleGrant::all_columns()),
        ("TeamView".into(), RoleGrant::all_columns()),
    ]);

    let role = surface_for_role(&full, "restricted", &restricted).unwrap();
    let role_manifest = DistributedClientSurfaceExport::from_selected("todos-service", role)
        .unwrap()
        .manifest()
        .unwrap();
    assert_eq!(
        role_manifest
            .projectors
            .iter()
            .map(|projector| projector.name.as_str())
            .collect::<Vec<_>>(),
        vec!["project_todos"]
    );
    assert!(!role_manifest.capabilities.record_revisions);
    assert!(!role_manifest.capabilities.tombstones);
    assert!(!role_manifest.capabilities.live_resume);
    assert!(role_manifest
        .models
        .iter()
        .find(|model| model.id == "TodoView")
        .is_some_and(|model| model.record_revisions && model.tombstones));
    assert!(role_manifest
        .models
        .iter()
        .find(|model| model.id == "UserView")
        .is_some_and(|model| !model.record_revisions && !model.tombstones));
    let role_json = serde_json::to_string(&role_manifest).unwrap();
    assert!(!role_json.contains("project_user_team"));
    assert!(!role_json.contains("private-user.changed"));

    let application = surface_for_application(
        &full,
        "web",
        &["admin".into(), "restricted".into()],
        &["admin".into(), "restricted".into()],
        &BTreeMap::from([("admin".into(), admin), ("restricted".into(), restricted)]),
    )
    .unwrap();
    let application_manifest =
        DistributedClientSurfaceExport::from_selected("todos-service", application)
            .unwrap()
            .manifest()
            .unwrap();
    assert_eq!(
        application_manifest
            .projectors
            .iter()
            .map(|projector| projector.name.as_str())
            .collect::<Vec<_>>(),
        vec!["project_todos"]
    );
    assert!(!application_manifest.capabilities.record_revisions);
    assert!(!application_manifest.capabilities.tombstones);
    assert!(!application_manifest.capabilities.live_resume);
    let application_json = serde_json::to_string(&application_manifest).unwrap();
    assert!(!application_json.contains("project_user_team"));
    assert!(!application_json.contains("private-user.changed"));
}

#[test]
fn aggregate_nodes_export_exact_role_bounded_window_semantics() {
    let mut options = SurfaceOptions::sqlite();
    options.default_limit = 7;
    options.max_limit = 19;
    let full = build_surface(&[teams(), users(), team_members()], &options).unwrap();
    let selected = surface_for_role(
        &full,
        "admin",
        &BTreeMap::from([
            ("TeamView".into(), RoleGrant::all_columns()),
            (
                "UserView".into(),
                RoleGrant::all_columns().with_aggregations().limit(13),
            ),
        ]),
    )
    .unwrap();
    let manifest = client_manifest_from_surface(
        "teams-service",
        ClientSurfaceIdentity::role("admin"),
        &selected,
    )
    .unwrap();

    let users_aggregate = manifest
        .roots
        .iter()
        .find(|root| root.operation == ClientRootOperation::Query && root.name == "users_aggregate")
        .expect("aggregate root grant");
    assert!(
        users_aggregate.pagination.is_none(),
        "ordinary root pagination must not stand in for aggregate nodes"
    );
    let root_semantics = users_aggregate
        .aggregate
        .as_ref()
        .expect("aggregate root semantics");
    assert_eq!(root_semantics.wrapper_typename, "users_aggregate");
    assert_eq!(root_semantics.fields_typename, "users_aggregate_fields");
    assert_eq!(
        root_semantics.nodes_pagination,
        ClientPaginationSemantics {
            kind: "offset".into(),
            default_limit: 7,
            max_limit: 13,
            coverage: "window".into(),
        }
    );

    let members_aggregate = manifest
        .models
        .iter()
        .find(|model| model.id == "TeamView")
        .unwrap()
        .relationships
        .iter()
        .find(|relationship| relationship.name == "members")
        .unwrap()
        .aggregate
        .as_ref()
        .expect("relationship aggregate grant");
    assert_eq!(
        members_aggregate.semantics.wrapper_typename,
        "users_aggregate"
    );
    assert_eq!(
        members_aggregate.semantics.fields_typename,
        "users_aggregate_fields"
    );
    assert_eq!(
        members_aggregate.semantics.nodes_pagination,
        root_semantics.nodes_pagination
    );
    assert_eq!(
        serde_json::to_value(&members_aggregate.semantics).unwrap()["nodes_pagination"],
        serde_json::json!({
            "kind": "offset",
            "default_limit": 7,
            "max_limit": 13,
            "coverage": "window"
        })
    );
}

#[test]
fn opaque_m2m_plan_preserves_invalidation_without_leaking_join_internals() {
    let full = build_surface(
        &[teams(), users(), team_members()],
        &SurfaceOptions::sqlite(),
    )
    .unwrap();
    let admin = surface_for_role(
        &full,
        "admin",
        &BTreeMap::from([
            ("TeamView".into(), RoleGrant::all_columns()),
            (
                "UserView".into(),
                RoleGrant::all_columns().with_aggregations(),
            ),
        ]),
    )
    .unwrap();
    let manifest = client_manifest_from_surface(
        "teams-service",
        ClientSurfaceIdentity::role("admin"),
        &admin,
    )
    .unwrap();
    assert!(!manifest.capabilities.causal_receipts);
    assert!(manifest.capabilities.cache_scope);
    assert!(manifest.protocol_operations.command_status.is_none());
    let members = manifest
        .models
        .iter()
        .find(|model| model.id == "TeamView")
        .unwrap()
        .relationships
        .iter()
        .find(|relationship| relationship.name == "members")
        .unwrap();
    assert!(!members.nullable, "list relationships are non-null lists");
    let RelationshipKeyMapping::ThroughOpaque {
        local,
        remote,
        dependency,
    } = &members.key_mapping
    else {
        panic!("authorized source/target keys should retain an opaque m2m plan")
    };
    assert_eq!(local, &["team_id"]);
    assert_eq!(remote, &["user_id"]);
    assert!(dependency.starts_with("opaque:sha256:"));
    assert_eq!(
        members.maintenance,
        ClientRelationshipMaintenance::Revalidate
    );
    let aggregate = members.aggregate.as_ref().expect("aggregate grant");
    assert_eq!(aggregate.name, "members_aggregate");
    assert_eq!(aggregate.semantics.wrapper_typename, "users_aggregate");
    assert_eq!(
        aggregate.semantics.fields_typename,
        "users_aggregate_fields"
    );
    assert!(aggregate.semantics.count && aggregate.semantics.nodes);
    assert_eq!(aggregate.dependencies, members.dependencies);
    let users_aggregate = manifest
        .roots
        .iter()
        .find(|root| root.operation == ClientRootOperation::Query && root.name == "users_aggregate")
        .expect("aggregate root grant");
    let users_aggregate_semantics = users_aggregate
        .aggregate
        .as_ref()
        .expect("aggregate root semantics");
    assert_eq!(
        users_aggregate_semantics.wrapper_typename,
        "users_aggregate"
    );
    assert_eq!(
        users_aggregate_semantics.fields_typename,
        "users_aggregate_fields"
    );
    assert!(members.dependencies.contains(&"teams".into()));
    assert!(members.dependencies.contains(&"users".into()));
    assert!(members
        .dependencies
        .iter()
        .any(|dependency| dependency.starts_with("opaque:sha256:")));
    let json = serde_json::to_string(&manifest).unwrap();
    assert!(!json.contains("private_team_members"));

    let mut renamed_team = teams();
    renamed_team.relationships[0].through = Some("renamed_private_join".into());
    let mut renamed_join = team_members();
    renamed_join.table_name = "renamed_private_join".into();
    let renamed_full = build_surface(
        &[renamed_team, users(), renamed_join],
        &SurfaceOptions::sqlite(),
    )
    .unwrap();
    let renamed_admin = surface_for_role(
        &renamed_full,
        "admin",
        &BTreeMap::from([
            ("TeamView".into(), RoleGrant::all_columns()),
            (
                "UserView".into(),
                RoleGrant::all_columns().with_aggregations(),
            ),
        ]),
    )
    .unwrap();
    let renamed_manifest = client_manifest_from_surface(
        "teams-service",
        ClientSurfaceIdentity::role("admin"),
        &renamed_admin,
    )
    .unwrap();
    let renamed_members = renamed_manifest
        .models
        .iter()
        .find(|model| model.id == "TeamView")
        .unwrap()
        .relationships
        .iter()
        .find(|relationship| relationship.name == "members")
        .unwrap();
    let RelationshipKeyMapping::ThroughOpaque {
        dependency: renamed_dependency,
        ..
    } = &renamed_members.key_mapping
    else {
        panic!("renamed private join should remain opaque")
    };
    assert_eq!(renamed_dependency, dependency);

    let denied = surface_for_role(
        &full,
        "limited",
        &BTreeMap::from([
            ("TeamView".into(), RoleGrant::columns(["name"])),
            ("UserView".into(), RoleGrant::all_columns()),
        ]),
    )
    .unwrap();
    let denied = client_manifest_from_surface(
        "teams-service",
        ClientSurfaceIdentity::role("limited"),
        &denied,
    )
    .unwrap();
    let team = denied
        .models
        .iter()
        .find(|model| model.id == "TeamView")
        .unwrap();
    let members = team
        .relationships
        .iter()
        .find(|relationship| relationship.name == "members")
        .unwrap();
    assert_eq!(members.key_mapping, RelationshipKeyMapping::Embedded);
    assert_eq!(
        members.maintenance,
        ClientRelationshipMaintenance::Revalidate
    );
    assert!(!members.dependencies.is_empty());
    assert!(members.aggregate.is_none());
    let team_json = serde_json::to_string(team).unwrap();
    assert!(!team_json.contains("team_id"));
    assert!(!team_json.contains("private_team_members"));
}

#[test]
fn visible_read_model_join_emits_explicit_local_m2m_plan() {
    let mut join = team_members();
    join.model_name = "TeamMemberView".into();
    join.kind = TableKind::ReadModel;
    let full = build_surface(&[teams(), users(), join], &SurfaceOptions::sqlite()).unwrap();
    let selected = surface_for_role(
        &full,
        "admin",
        &BTreeMap::from([
            ("TeamView".into(), RoleGrant::all_columns()),
            ("UserView".into(), RoleGrant::all_columns()),
            ("TeamMemberView".into(), RoleGrant::all_columns()),
        ]),
    )
    .unwrap();
    let manifest = DistributedClientSurfaceExport::from_selected("teams-service", selected)
        .unwrap()
        .manifest()
        .unwrap();
    let members = manifest
        .models
        .iter()
        .find(|model| model.id == "TeamView")
        .unwrap()
        .relationships
        .iter()
        .find(|relationship| relationship.name == "members")
        .unwrap();
    assert_eq!(
        members.key_mapping,
        RelationshipKeyMapping::Through {
            local: vec!["team_id".into()],
            remote: vec!["user_id".into()],
            table: "private_team_members".into(),
            source_foreign_key: vec!["team_id".into()],
            target_foreign_key: vec!["user_id".into()],
        }
    );
    assert_eq!(members.maintenance, ClientRelationshipMaintenance::Local);
    assert_eq!(
        members.dependencies,
        vec!["private_team_members", "teams", "users"]
    );
}

#[test]
fn relational_row_policy_is_server_only_when_relationship_key_is_hidden() {
    let full = build_surface(&[todos(), users()], &SurfaceOptions::sqlite()).unwrap();
    let selected = surface_for_role(
        &full,
        "user",
        &BTreeMap::from([
            (
                "TodoView".into(),
                RoleGrant::columns(["todo_id", "title", "completed"])
                    .rows(rel("owner", col("display_name").eq("Patrick"))),
            ),
            ("UserView".into(), RoleGrant::columns(["display_name"])),
        ]),
    )
    .unwrap();
    let manifest = client_manifest_from_surface(
        "todos-service",
        ClientSurfaceIdentity::role("user"),
        &selected,
    )
    .unwrap();
    let todo = manifest
        .models
        .iter()
        .find(|model| model.id == "TodoView")
        .unwrap();
    assert_eq!(todo.row_policy, ClientRowPolicy::ServerOnly);
    let owner = todo
        .relationships
        .iter()
        .find(|relationship| relationship.name == "owner")
        .unwrap();
    assert_eq!(owner.key_mapping, RelationshipKeyMapping::Embedded);
    assert_eq!(owner.dependencies, vec!["todos", "users"]);
    assert_eq!(owner.maintenance, ClientRelationshipMaintenance::Revalidate);
    let json = serde_json::to_string(todo).unwrap();
    assert!(!json.contains("owner_id"));
    assert!(!json.contains("user_id"));
    assert!(!json.contains("Patrick"));
}

#[test]
fn application_role_sets_are_canonical_before_fingerprinting() {
    let full = full_surface();
    let selected = surface_for_application(
        &full,
        "web",
        &["admin".into(), "user".into()],
        &["admin".into(), "user".into()],
        &grants(),
    )
    .unwrap();
    let first = client_manifest_from_surface(
        "todos-service",
        ClientSurfaceIdentity::Application {
            name: "web".into(),
            eligible_roles: vec!["user".into(), "admin".into(), "user".into()],
            schema_roles: vec!["user".into(), "admin".into(), "user".into()],
        },
        &selected,
    )
    .unwrap();
    let second = client_manifest_from_surface(
        "todos-service",
        ClientSurfaceIdentity::Application {
            name: "web".into(),
            eligible_roles: vec!["admin".into(), "user".into()],
            schema_roles: vec!["admin".into(), "user".into()],
        },
        &selected,
    )
    .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.schema_fingerprint, second.schema_fingerprint);
    assert_eq!(
        first.surface,
        ClientSurfaceIdentity::application("web", ["admin", "user"], ["admin", "user"])
    );
}

#[test]
fn catalog_or_mismatched_surface_cannot_be_labeled_as_authorized() {
    let full = full_surface();
    let error =
        DistributedClientSurfaceExport::from_selected("todos-service", full.clone()).unwrap_err();
    assert!(error
        .to_string()
        .contains("explicitly role- or application-selected"));

    let selected = surface_for_role(&full, "user", &grants()["user"]).unwrap();
    let inventory_error =
        DistributedClientSurfaceExport::from_selected("wrong-service", selected.clone())
            .unwrap_err();
    assert!(inventory_error.to_string().contains("does not match"));

    let error = client_manifest_from_surface(
        "todos-service",
        ClientSurfaceIdentity::role("admin"),
        &selected,
    )
    .unwrap_err();
    assert!(error.to_string().contains("does not match"));
}
