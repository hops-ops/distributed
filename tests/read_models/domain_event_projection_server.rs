use distributed::projection::lower::{
    body_path, finish_lowering, lower_model_mutation, lower_related_mutation, model_operation,
    related_operation, state_selector, LoweredProjectionPlan, ProjectionAuthoringField,
    ProjectionLoweringError,
};
use distributed::projection::{
    MutationKind, ProjectionArm, ProjectionEventSet, ProjectionExpression, ProjectionPartition,
    ProjectionPlanTemplate, ProjectionProgram, ProjectionProgramError, ProjectionValue,
};
use distributed::read_model::ReadModelWorkspaceExt;
use distributed::read_model::ReadModelWritePlanBuilder;
#[cfg(feature = "sqlite")]
use distributed::table::TableSchemaRegistry;
use distributed::table::{
    ExpectedVersion, RowKey, RowValue, RowWriteMode, TableMutation, TableRowMutation,
    TableWritePlan,
};
use distributed::{Entity, InMemoryRepository, ReadModelWritePlanStore};
use serde::{Deserialize, Serialize};

#[derive(
    Clone, Debug, PartialEq, Serialize, Deserialize, distributed::domain_event::DomainState,
)]
#[domain_state(version = 1)]
struct PlayerState {
    player_id: String,
    display_name: String,
    weapon_id: String,
    acquired_at: String,
}

#[derive(Default)]
struct PlayerAggregate {
    entity: Entity,
    display_name: String,
    weapon_id: String,
    acquired_at: String,
}

impl From<&PlayerAggregate> for PlayerState {
    fn from(player: &PlayerAggregate) -> Self {
        Self {
            player_id: player.entity.id().to_owned(),
            display_name: player.display_name.clone(),
            weapon_id: player.weapon_id.clone(),
            acquired_at: player.acquired_at.clone(),
        }
    }
}

#[distributed::sourced(
    entity,
    events = "PlayerAggregateReplayEvent",
    aggregate_type = "projection.player",
    domain_state = PlayerState,
)]
impl PlayerAggregate {
    #[event("player.loaded", version = 1, domain)]
    fn load(
        &mut self,
        player_id: String,
        display_name: String,
        weapon_id: String,
        acquired_at: String,
    ) {
        self.entity.set_id(player_id);
        self.display_name = display_name;
        self.weapon_id = weapon_id;
        self.acquired_at = acquired_at;
    }
}

#[derive(
    Clone, Debug, Default, PartialEq, Serialize, Deserialize, distributed::read_model::ReadModel,
)]
#[readmodel(table = "projection_players", primary_key = ["player_id"])]
struct Players {
    player_id: String,
    display_name: String,
    #[readmodel(has_many = "PlayerWeapons", foreign_key = "player_id")]
    weapons: Vec<PlayerWeapons>,
}

#[derive(
    Clone, Debug, Default, PartialEq, Serialize, Deserialize, distributed::read_model::ReadModel,
)]
#[readmodel(table = "projection_player_weapons", primary_key = ["weapon_id"])]
struct PlayerWeapons {
    #[readmodel(
        foreign_key = "projection_players.player_id",
        delegated_from = "Players.player_id"
    )]
    player_id: String,
    weapon_id: String,
    acquired_at: String,
}

#[derive(
    Clone, Debug, Default, PartialEq, Serialize, Deserialize, distributed::read_model::ReadModel,
)]
#[readmodel(
    table = "projection_player_weapon_links",
    primary_key = ["player_id", "weapon_id"]
)]
struct PlayerWeaponLinks {
    #[readmodel(foreign_key = "projection_players.player_id")]
    player_id: String,
    #[readmodel(foreign_key = "projection_player_weapons.weapon_id")]
    weapon_id: String,
}

#[derive(
    Clone, Debug, Default, PartialEq, Serialize, Deserialize, distributed::read_model::ReadModel,
)]
#[readmodel(
    table = "projection_player_summaries",
    primary_key = ["player_id"]
)]
struct PlayerSummary {
    player_id: String,
    weapon_count: u64,
}

#[derive(
    Clone, Debug, Default, PartialEq, Serialize, Deserialize, distributed::read_model::ReadModel,
)]
#[readmodel(table = "projection_collision_parents", primary_key = ["id"])]
struct CollisionParent {
    id: String,
    parent_id: String,
    #[readmodel(has_many = "CollisionChild", foreign_key = "parent_id")]
    children: Vec<CollisionChild>,
}

#[derive(
    Clone, Debug, Default, PartialEq, Serialize, Deserialize, distributed::read_model::ReadModel,
)]
#[readmodel(table = "projection_collision_children", primary_key = ["child_id"])]
struct CollisionChild {
    child_id: String,
    #[readmodel(foreign_key = "projection_collision_parents.id")]
    parent_id: String,
}

struct PlayerEvents;

impl ProjectionEventSet for PlayerEvents {
    fn projection_event_selectors(
    ) -> Result<Vec<distributed::projection::ProjectionEventSelector>, ProjectionProgramError> {
        Ok(vec![state_selector::<PlayerState>("player.loaded", 1)?])
    }
}

fn projected(field: &'static str) -> Result<ProjectionAuthoringField, ProjectionProgramError> {
    Ok(ProjectionAuthoringField::set(
        field,
        body_path::<PlayerState>(field)?,
    ))
}

fn player_program() -> Result<ProjectionProgram, ProjectionProgramError> {
    let summary = model_operation::<PlayerSummary>(
        "upsert-summary",
        0,
        MutationKind::Upsert,
        vec![projected("player_id")?],
        vec![ProjectionAuthoringField::set(
            "weapon_count",
            ProjectionExpression::constant(ProjectionValue::unsigned(1)),
        )],
    )?;
    let player = model_operation::<Players>(
        "upsert-player",
        1,
        MutationKind::Upsert,
        vec![projected("player_id")?],
        vec![projected("display_name")?],
    )?;
    let weapon = related_operation::<__DistributedPlayersEffectRelationship_weapons>(
        "upsert-weapon",
        2,
        MutationKind::UpsertRelated,
        &player,
        vec![projected("weapon_id")?],
        vec![projected("acquired_at")?],
    )?;
    let link = model_operation::<PlayerWeaponLinks>(
        "insert-link",
        3,
        MutationKind::Insert,
        vec![projected("player_id")?, projected("weapon_id")?],
        Vec::new(),
    )?;
    ProjectionProgram::try_new(
        "domain-event-player-graph",
        1,
        ProjectionPartition::Expression(body_path::<PlayerState>("player_id")?),
        vec![ProjectionArm::try_new(
            "player-loaded",
            state_selector::<PlayerState>("player.loaded", 1)?,
            vec![summary, player, weapon, link],
        )?],
    )
}

fn lower_player_program(
    plan: &distributed::projection::ResolvedProjectionPlan,
) -> Result<LoweredProjectionPlan, ProjectionLoweringError> {
    let mut records = Vec::new();
    for mutation in plan.mutations() {
        let operation = mutation
            .provenance()
            .operation_ids()
            .first()
            .map(String::as_str)
            .expect("each resolved mutation retains an operation ID");
        records.push(match operation {
            "upsert-summary" => lower_model_mutation::<PlayerSummary>(mutation)?,
            "upsert-player" => lower_model_mutation::<Players>(mutation)?,
            "upsert-weapon" => lower_related_mutation::<
                __DistributedPlayersEffectRelationship_weapons,
            >(plan, mutation, "upsert-player")?,
            "insert-link" => lower_model_mutation::<PlayerWeaponLinks>(mutation)?,
            other => panic!("unexpected manual projection operation `{other}`"),
        });
    }
    finish_lowering(plan, records)
}

/// The physical plan a server-side builder would stage for these records:
/// inserts must not exist yet, upserts replace. The executor makes this
/// choice from each record's snapshot; these fixtures start empty.
fn fluent_shaped_plan(lowered: &LoweredProjectionPlan) -> TableWritePlan {
    TableWritePlan::new(
        lowered
            .records()
            .map(|(resolved, record)| {
                let (mode, expected_version) = match resolved.kind() {
                    MutationKind::Insert | MutationKind::InsertRelated => {
                        (RowWriteMode::Insert, ExpectedVersion::NotExists)
                    }
                    MutationKind::Upsert | MutationKind::UpsertRelated => {
                        (RowWriteMode::Upsert, ExpectedVersion::Any)
                    }
                    other => panic!("fixture lowers only full rows, not `{other:?}`"),
                };
                TableMutation::UpsertRow(TableRowMutation {
                    schema: record.schema(),
                    key: record.key().clone(),
                    values: record.values().clone(),
                    expected_version,
                    mode,
                })
            })
            .collect(),
    )
}

fn player_key() -> RowKey {
    RowKey::new([("player_id", RowValue::String("player-1".into()))])
}

fn weapon_key() -> RowKey {
    RowKey::new([
        ("player_id", RowValue::String("player-1".into())),
        ("weapon_id", RowValue::String("sword".into())),
    ])
}

fn occurrence() -> distributed::domain_event::DomainEventOccurrence {
    let mut aggregate = PlayerAggregate::default();
    aggregate
        .load(
            "player-1".into(),
            "Ada".into(),
            "sword".into(),
            "2026-07-28T00:00:00Z".into(),
        )
        .expect("capture state-bearing domain event");
    aggregate
        .entity
        .pending_domain_events()
        .last()
        .expect("outward occurrence")
        .clone()
}

fn collision_occurrence() -> distributed::domain_event::DomainEventOccurrence {
    let mut aggregate = PlayerAggregate::default();
    aggregate
        .load(
            "parent-1".into(),
            "Ada".into(),
            "child-1".into(),
            "2026-07-28T00:00:00Z".into(),
        )
        .expect("capture collision state-bearing domain event");
    aggregate
        .entity
        .pending_domain_events()
        .last()
        .expect("collision outward occurrence")
        .clone()
}

fn expected_models() -> (Players, PlayerWeapons, PlayerSummary, PlayerWeaponLinks) {
    (
        Players {
            player_id: "player-1".into(),
            display_name: "Ada".into(),
            weapons: Vec::new(),
        },
        PlayerWeapons {
            player_id: "player-1".into(),
            weapon_id: "sword".into(),
            acquired_at: "2026-07-28T00:00:00Z".into(),
        },
        PlayerSummary {
            player_id: "player-1".into(),
            weapon_count: 1,
        },
        PlayerWeaponLinks {
            player_id: "player-1".into(),
            weapon_id: "sword".into(),
        },
    )
}

fn portable_and_fluent_plans() -> (
    distributed::table::TableWritePlan,
    distributed::table::TableWritePlan,
) {
    let resolved = ProjectionPlanTemplate::<PlayerEvents>::try_new(
        player_program().expect("manual portable program"),
    )
    .expect("exact event set")
    .resolve(&occurrence())
    .expect("portable program resolves");
    let portable =
        fluent_shaped_plan(&lower_player_program(&resolved).expect("portable program lowers"));
    let (player, weapon, summary, link) = expected_models();
    let mut fluent = ReadModelWritePlanBuilder::new();
    fluent
        .upsert(&summary)
        .expect("summary")
        .upsert(&player)
        .expect("parent")
        .upsert_related(&player, "weapons", &weapon)
        .expect("related child")
        .insert(&link)
        .expect("join");
    (
        portable,
        fluent.into_write_plan().expect("fluent physical plan"),
    )
}

fn collision_projection_plan() -> TableWritePlan {
    let parent = model_operation::<CollisionParent>(
        "upsert-collision-parent",
        0,
        MutationKind::Upsert,
        vec![ProjectionAuthoringField::set(
            "id",
            body_path::<PlayerState>("player_id").expect("player ID path"),
        )],
        vec![ProjectionAuthoringField::set(
            "parent_id",
            ProjectionExpression::constant(ProjectionValue::string("wrong-parent")),
        )],
    )
    .expect("collision parent operation");
    let child = related_operation::<__DistributedCollisionParentEffectRelationship_children>(
        "upsert-collision-child",
        1,
        MutationKind::UpsertRelated,
        &parent,
        vec![ProjectionAuthoringField::set(
            "child_id",
            body_path::<PlayerState>("weapon_id").expect("child ID path"),
        )],
        Vec::new(),
    )
    .expect("collision child operation");
    let program = ProjectionProgram::try_new(
        "collision-relationship",
        1,
        ProjectionPartition::Unit,
        vec![ProjectionArm::try_new(
            "collision-loaded",
            state_selector::<PlayerState>("player.loaded", 1).expect("state selector"),
            vec![parent, child],
        )
        .expect("collision arm")],
    )
    .expect("collision program");
    let resolved = ProjectionPlanTemplate::<PlayerEvents>::try_new(program)
        .expect("collision event set")
        .resolve(&collision_occurrence())
        .expect("collision projection resolves");
    let mut records = Vec::new();
    for mutation in resolved.mutations() {
        let operation = mutation
            .provenance()
            .operation_ids()
            .first()
            .map(String::as_str)
            .expect("collision operation provenance");
        records.push(match operation {
            "upsert-collision-parent" => {
                lower_model_mutation::<CollisionParent>(mutation).expect("lower collision parent")
            }
            "upsert-collision-child" => lower_related_mutation::<
                __DistributedCollisionParentEffectRelationship_children,
            >(
                &resolved, mutation, "upsert-collision-parent"
            )
            .expect("lower collision child"),
            other => panic!("unexpected collision operation `{other}`"),
        });
    }
    fluent_shaped_plan(&finish_lowering(&resolved, records).expect("finish collision lowering"))
}

fn child_parent_id(plan: &TableWritePlan) -> Option<&RowValue> {
    plan.mutations.iter().find_map(|mutation| {
        let TableMutation::UpsertRow(row) = mutation else {
            return None;
        };
        (row.schema.table_name == "projection_collision_children")
            .then(|| row.values.get("parent_id"))
            .flatten()
    })
}

async fn assert_in_memory_rows(repository: &InMemoryRepository) {
    let (player, weapon, summary, link) = expected_models();
    let mut workspace = repository.workspace();
    let hydrated = workspace
        .load::<Players>(player_key())
        .include("weapons")
        .one()
        .await
        .expect("load player graph")
        .expect("player graph exists");
    assert_eq!(hydrated.data.weapons, vec![weapon.clone()]);
    assert_eq!(
        Players {
            weapons: Vec::new(),
            ..hydrated.data
        },
        player
    );
    assert_eq!(
        workspace
            .load::<PlayerSummary>(player_key())
            .one()
            .await
            .expect("load summary")
            .expect("summary exists")
            .data,
        summary
    );
    assert_eq!(
        workspace
            .load::<PlayerWeaponLinks>(weapon_key())
            .one()
            .await
            .expect("load join")
            .expect("join exists")
            .data,
        link
    );
}

#[tokio::test]
async fn explicit_child_fk_reference_beats_a_same_named_parent_column_everywhere() {
    let parent = CollisionParent {
        id: "parent-1".into(),
        parent_id: "wrong-parent".into(),
        children: Vec::new(),
    };
    let child = CollisionChild {
        child_id: "child-1".into(),
        parent_id: String::new(),
    };

    let mut fluent = ReadModelWritePlanBuilder::new();
    fluent
        .upsert_related(&parent, "children", &child)
        .expect("fluent relationship write");
    let fluent = fluent.into_write_plan().expect("fluent plan");
    assert_eq!(
        child_parent_id(&fluent),
        Some(&RowValue::String("parent-1".into()))
    );

    let portable = collision_projection_plan();
    assert_eq!(
        child_parent_id(&portable),
        Some(&RowValue::String("parent-1".into()))
    );

    let repository = InMemoryRepository::new();
    repository
        .model_store()
        .register_schema::<CollisionParent>()
        .expect("register collision parent");
    repository
        .model_store()
        .register_schema::<CollisionChild>()
        .expect("register collision child");
    let mut seed = ReadModelWritePlanBuilder::new();
    seed.upsert(&parent).expect("seed collision parent");
    repository
        .commit_write_plan(seed.into_write_plan().expect("seed plan"))
        .await
        .expect("seed parent");

    let mut workspace = repository.workspace();
    let mut loaded = workspace
        .load::<CollisionParent>(RowKey::new([("id", RowValue::String("parent-1".into()))]))
        .include("children")
        .one()
        .await
        .expect("load collision parent")
        .expect("collision parent exists")
        .data;
    loaded.children.push(child);
    workspace.sync(loaded).expect("sync collision child");
    let synced = workspace.into_write_plan().expect("sync plan");
    assert_eq!(
        child_parent_id(&synced),
        Some(&RowValue::String("parent-1".into()))
    );
}

#[tokio::test]
async fn portable_four_table_program_matches_fluent_plan_and_applies_in_memory() {
    let (portable, fluent) = portable_and_fluent_plans();
    assert_eq!(portable, fluent);
    assert_eq!(
        portable
            .mutations
            .iter()
            .map(|mutation| mutation.table_name())
            .collect::<Vec<_>>(),
        vec![
            "projection_player_summaries",
            "projection_players",
            "projection_player_weapons",
            "projection_player_weapon_links",
        ]
    );

    let repository = InMemoryRepository::new();
    repository
        .model_store()
        .register_schema::<Players>()
        .expect("register parent includes");
    repository
        .model_store()
        .register_schema::<PlayerWeapons>()
        .expect("register child includes");
    repository
        .commit_write_plan(portable)
        .await
        .expect("one atomic four-table commit");
    assert_in_memory_rows(&repository).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_applies_the_same_four_table_server_plan() {
    use distributed::SqliteRepository;

    let (portable, fluent) = portable_and_fluent_plans();
    assert_eq!(portable, fluent);
    let repository = SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .expect("SQLite repository");
    let mut registry = TableSchemaRegistry::new();
    registry.register::<Players>().expect("players schema");
    registry
        .register::<PlayerWeapons>()
        .expect("weapons schema");
    registry
        .register::<PlayerSummary>()
        .expect("summary schema");
    registry
        .register::<PlayerWeaponLinks>()
        .expect("join schema");
    repository
        .bootstrap_table_schema_for_dev(&registry)
        .await
        .expect("bootstrap four read models");
    repository
        .commit_write_plan(portable)
        .await
        .expect("one atomic four-table SQLite commit");

    let (player, weapon, summary, link) = expected_models();
    let mut workspace = repository.workspace();
    let hydrated = workspace
        .load::<Players>(player_key())
        .include("weapons")
        .one()
        .await
        .expect("load SQLite player graph")
        .expect("SQLite player exists");
    assert_eq!(hydrated.data.weapons, vec![weapon]);
    assert_eq!(
        Players {
            weapons: Vec::new(),
            ..hydrated.data
        },
        player
    );
    assert_eq!(
        workspace
            .load::<PlayerSummary>(player_key())
            .one()
            .await
            .expect("load SQLite summary")
            .expect("SQLite summary exists")
            .data,
        summary
    );
    assert_eq!(
        workspace
            .load::<PlayerWeaponLinks>(weapon_key())
            .one()
            .await
            .expect("load SQLite join")
            .expect("SQLite join exists")
            .data,
        link
    );
}
