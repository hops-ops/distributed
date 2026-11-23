use std::collections::HashMap;

use distributed::read_model::ReadModelWritePlanBuilder;
use distributed::{
    ExpectedVersion, InMemoryReadModelStore, PatchMode, ReadModel, ReadModelWorkspaceExt, RowKey,
    RowPatch, RowValue, RowWriteMode, TableMutation, TableStoreError, Versioned,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("account_summaries")]
struct AccountSummary {
    #[id("account_id")]
    account_id: String,
    #[index]
    owner: Option<String>,
    balance_cents: i64,
    #[readmodel(default = "0")]
    deposit_count: u32,
    #[readmodel(jsonb)]
    counters_by_game: HashMap<String, i64>,
    #[readmodel(skip_query)]
    projected_event_ids: Vec<String>,
}

impl AccountSummary {
    fn new(account_id: &str) -> Self {
        Self {
            account_id: account_id.into(),
            owner: Some("Ada".into()),
            balance_cents: 100,
            deposit_count: 1,
            counters_by_game: HashMap::new(),
            projected_event_ids: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("players")]
struct Player {
    #[id("player_id")]
    player_id: String,
    display_name: String,
    #[readmodel(has_many = "PlayerWeapon", foreign_key = "player_id")]
    weapons: Vec<PlayerWeapon>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("player_weapons")]
#[readmodel(primary_key = ["player_id", "weapon_id"])]
struct PlayerWeapon {
    #[readmodel(foreign_key = "players.player_id", delegated_from = "Player.player_id")]
    player_id: String,
    weapon_id: String,
    acquired_at: String,
}

fn account_key(account_id: &str) -> RowKey {
    RowKey::new([("account_id", RowValue::String(account_id.into()))])
}

#[test]
fn session_stages_multiple_read_model_types_in_deterministic_plan() {
    let mut session = ReadModelWritePlanBuilder::new();
    let weapon = PlayerWeapon {
        player_id: "player-1".into(),
        weapon_id: "sword".into(),
        acquired_at: "2026-05-23T00:00:00Z".into(),
    };
    let account = AccountSummary::new("acct-1");

    session.upsert(&weapon).unwrap().upsert(&account).unwrap();

    let plan = session.into_write_plan().unwrap();

    assert_eq!(plan.mutations.len(), 2);
    assert_eq!(plan.mutations[0].table_name(), "account_summaries");
    assert_eq!(plan.mutations[1].table_name(), "player_weapons");
}

#[test]
fn write_plan_orders_parent_rows_before_dependent_children() {
    let mut session = ReadModelWritePlanBuilder::new();
    let player = Player {
        player_id: "player-1".into(),
        display_name: "Ada".into(),
        weapons: Vec::new(),
    };
    let weapon = PlayerWeapon {
        player_id: "player-1".into(),
        weapon_id: "sword".into(),
        acquired_at: "2026-05-23T00:00:00Z".into(),
    };

    session.upsert(&weapon).unwrap().upsert(&player).unwrap();

    let plan = session.into_write_plan().unwrap();

    assert_eq!(plan.mutations[0].table_name(), "players");
    assert_eq!(plan.mutations[1].table_name(), "player_weapons");
}

#[test]
fn write_plan_contains_relational_rows_only() {
    let mut session = ReadModelWritePlanBuilder::new();
    let account = AccountSummary::new("acct-1");

    session
        .upsert(&account)
        .unwrap()
        .delete::<AccountSummary>(account_key("acct-2"))
        .unwrap();

    let plan = session.into_write_plan().unwrap();

    assert!(matches!(plan.mutations[0], TableMutation::UpsertRow(_)));
    assert!(matches!(plan.mutations[1], TableMutation::DeleteRow(_)));
}

#[test]
fn sparse_patches_and_full_replacements_are_distinct() {
    let mut session = ReadModelWritePlanBuilder::new();
    let account = AccountSummary::new("acct-1");
    let patch = RowPatch::new().set("owner", RowValue::Null);

    session.upsert(&account).unwrap();
    session
        .patch::<AccountSummary>(account_key("acct-1"), patch)
        .unwrap();

    let plan = session.into_write_plan().unwrap();

    let TableMutation::UpsertRow(full_row) = &plan.mutations[0] else {
        panic!("expected full-row mutation");
    };
    assert_eq!(full_row.mode, RowWriteMode::Upsert);
    assert!(full_row.values.contains_key("balance_cents"));
    assert!(full_row.values.contains_key("counters_by_game"));

    let TableMutation::PatchRow(patch_row) = &plan.mutations[1] else {
        panic!("expected patch-row mutation");
    };
    assert_eq!(patch_row.mode, PatchMode::UpdateExisting);
    assert_eq!(patch_row.patch.get("owner"), Some(&RowValue::Null));
    assert_eq!(patch_row.patch.iter().count(), 1);
}

#[test]
fn insert_and_upsert_patch_carry_explicit_missing_row_behavior() {
    let mut session = ReadModelWritePlanBuilder::new();
    let account = AccountSummary::new("acct-1");
    let patch = RowPatch::new().set("owner", RowValue::String("Grace".into()));

    session.insert(&account).unwrap();
    session
        .upsert_patch::<AccountSummary>(account_key("acct-2"), patch)
        .unwrap();

    let plan = session.into_write_plan().unwrap();

    let TableMutation::UpsertRow(insert_row) = &plan.mutations[0] else {
        panic!("expected insert row mutation");
    };
    assert_eq!(insert_row.mode, RowWriteMode::Insert);
    assert_eq!(insert_row.expected_version, ExpectedVersion::NotExists);

    let TableMutation::PatchRow(upsert_patch) = &plan.mutations[1] else {
        panic!("expected upsert patch mutation");
    };
    assert_eq!(upsert_patch.mode, PatchMode::InsertMissing);
}

#[tokio::test]
async fn insert_missing_patch_builds_full_row_from_key_before_insert() {
    let store = InMemoryReadModelStore::new();
    store.register_schema::<AccountSummary>().unwrap();
    let patch = RowPatch::new()
        .set("account_id", RowValue::String("acct-1".into()))
        .set("owner", RowValue::String("Grace".into()))
        .set("balance_cents", RowValue::I64(250))
        .set("deposit_count", RowValue::U64(2))
        .set(
            "counters_by_game",
            RowValue::Json(serde_json::json!({"deposits": 1})),
        );
    let mut session = ReadModelWritePlanBuilder::new();

    session
        .upsert_patch::<AccountSummary>(account_key("acct-1"), patch)
        .unwrap();
    session.commit(&store).await.unwrap();

    let mut read_models = store.workspace();
    let loaded = read_models
        .load::<AccountSummary>(account_key("acct-1"))
        .one()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.data.account_id, "acct-1");
    assert_eq!(loaded.data.owner, Some("Grace".into()));
    assert_eq!(loaded.data.balance_cents, 250);
    assert_eq!(loaded.data.deposit_count, 2);
}

#[tokio::test]
async fn insert_missing_patch_rejects_primary_key_mismatch() {
    let store = InMemoryReadModelStore::new();
    let patch = RowPatch::new()
        .set("account_id", RowValue::String("acct-2".into()))
        .set("owner", RowValue::String("Grace".into()))
        .set("balance_cents", RowValue::I64(250))
        .set("deposit_count", RowValue::U64(2))
        .set("counters_by_game", RowValue::Json(serde_json::json!({})));
    let mut session = ReadModelWritePlanBuilder::new();

    session
        .upsert_patch::<AccountSummary>(account_key("acct-1"), patch)
        .unwrap();
    let err = session.commit(&store).await.unwrap_err();

    assert!(
        matches!(err, TableStoreError::Metadata(message) if message.contains("primary-key column `account_id`"))
    );
}

#[tokio::test]
async fn insert_missing_patch_rejects_partial_new_row() {
    let store = InMemoryReadModelStore::new();
    store.register_schema::<AccountSummary>().unwrap();
    let patch = RowPatch::new().set("owner", RowValue::String("Grace".into()));
    let mut session = ReadModelWritePlanBuilder::new();

    session
        .upsert_patch::<AccountSummary>(account_key("acct-1"), patch)
        .unwrap();
    let err = session.commit(&store).await.unwrap_err();

    assert!(
        matches!(err, TableStoreError::Metadata(message) if message.contains("missing required column `balance_cents`"))
    );

    let mut read_models = store.workspace();
    let loaded = read_models
        .load::<AccountSummary>(account_key("acct-1"))
        .one()
        .await
        .unwrap();
    assert!(loaded.is_none());
}

#[tokio::test]
async fn existing_patch_rejects_primary_key_mismatch() {
    let store = InMemoryReadModelStore::new();
    let mut setup = ReadModelWritePlanBuilder::new();
    setup.upsert(&AccountSummary::new("acct-1")).unwrap();
    setup.commit(&store).await.unwrap();
    let patch = RowPatch::new()
        .set("account_id", RowValue::String("acct-2".into()))
        .set("owner", RowValue::String("Grace".into()));
    let mut session = ReadModelWritePlanBuilder::new();

    session
        .patch::<AccountSummary>(account_key("acct-1"), patch)
        .unwrap();
    let err = session.commit(&store).await.unwrap_err();

    assert!(
        matches!(err, TableStoreError::Metadata(message) if message.contains("primary-key column `account_id`"))
    );
}

#[test]
fn relationship_operation_populates_child_foreign_key_in_explicit_row_mutation() {
    let player = Player {
        player_id: "player-1".into(),
        display_name: "Ada".into(),
        weapons: Vec::new(),
    };
    let weapon = PlayerWeapon {
        player_id: String::new(),
        weapon_id: "sword".into(),
        acquired_at: "2026-05-23T00:00:00Z".into(),
    };
    let mut session = ReadModelWritePlanBuilder::new();

    session.upsert_related(&player, "weapons", &weapon).unwrap();

    let plan = session.into_write_plan().unwrap();

    let TableMutation::UpsertRow(child_row) = &plan.mutations[0] else {
        panic!("expected child row mutation");
    };
    assert_eq!(child_row.schema.table_name, "player_weapons");
    assert_eq!(
        child_row.values.get("player_id"),
        Some(&RowValue::String("player-1".into()))
    );
    assert_eq!(
        child_row.key.get("player_id"),
        Some(&RowValue::String("player-1".into()))
    );
}

#[test]
fn expected_versions_are_carried_into_plan() {
    let mut account = AccountSummary::new("acct-1");
    let loaded = Versioned {
        data: account.clone(),
        version: 7,
    };
    account.balance_cents = 250;
    let mut session = ReadModelWritePlanBuilder::new();

    session
        .track_loaded(&loaded)
        .unwrap()
        .upsert(&account)
        .unwrap();

    let plan = session.into_write_plan().unwrap();

    let TableMutation::UpsertRow(row) = &plan.mutations[0] else {
        panic!("expected upsert row");
    };
    assert_eq!(row.expected_version, ExpectedVersion::Exact(7));
}

#[test]
fn load_requests_validate_primary_keys_and_explicit_relationship_includes() {
    let session = ReadModelWritePlanBuilder::new();

    let request = session
        .load_with::<Player, _, _>(
            RowKey::new([("player_id", RowValue::String("player-1".into()))]),
            ["weapons"],
        )
        .unwrap();

    assert_eq!(request.schema.table_name, "players");
    assert_eq!(request.includes, vec!["weapons"]);

    let err = session
        .load_with::<Player, _, _>(
            RowKey::new([("player_id", RowValue::String("player-1".into()))]),
            ["missing"],
        )
        .unwrap_err();
    assert!(matches!(err, TableStoreError::Metadata(message) if message.contains("relationship")));
}

#[test]
fn validation_failures_happen_before_storage_writes() {
    let mut session = ReadModelWritePlanBuilder::new();
    let patch = RowPatch::new().set("balance_cents", RowValue::Null);

    session
        .patch::<AccountSummary>(account_key("acct-1"), patch)
        .unwrap();

    let err = session.into_write_plan().unwrap_err();

    assert!(matches!(err, TableStoreError::Metadata(message) if message.contains("not nullable")));
}
