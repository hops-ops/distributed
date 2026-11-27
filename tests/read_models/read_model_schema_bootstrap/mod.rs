use std::collections::HashMap;

use distributed::read_model::{ReadModel, RelationalReadModel};
use distributed::table::{
    ColumnType, TableSchemaRegistry, TableStoreError, DEFAULT_TABLE_VERSION_COLUMN,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("account_summaries")]
struct AccountSummary {
    #[id("account_id")]
    account_id: String,
    #[unique]
    owner_slug: String,
    balance_cents: i64,
    #[readmodel(default = "0")]
    deposit_count: u32,
    #[readmodel(jsonb)]
    counters_by_game: HashMap<String, i64>,
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
    #[index]
    acquired_at: String,
}

fn registry() -> TableSchemaRegistry {
    let mut registry = TableSchemaRegistry::new();
    registry
        .register::<AccountSummary>()
        .unwrap()
        .register::<Player>()
        .unwrap()
        .register::<PlayerWeapon>()
        .unwrap();
    registry
}

#[test]
fn registry_registers_relational_models_and_exposes_schema_metadata() {
    let registry = registry();

    registry.validate().unwrap();
    assert_eq!(registry.len(), 3);
    assert_eq!(
        registry.table_names().collect::<Vec<_>>(),
        vec!["account_summaries", "player_weapons", "players"]
    );

    let summary = registry.schema_for_model("AccountSummary").unwrap();
    assert_eq!(summary.table_name, "account_summaries");
    assert_eq!(
        summary.version_column.as_deref(),
        Some(DEFAULT_TABLE_VERSION_COLUMN)
    );
    assert!(summary
        .columns
        .iter()
        .any(|column| column.column_name == "counters_by_game"
            && column.column_type == ColumnType::Json));
    assert!(summary
        .indexes
        .iter()
        .any(|index| index.unique && index.columns == vec!["owner_slug"]));

    let weapon = registry.schema_for_table("player_weapons").unwrap();
    assert_eq!(weapon.primary_key.columns, vec!["player_id", "weapon_id"]);
    assert!(weapon.columns.iter().any(|column| {
        column.column_name == "player_id"
            && column.delegated_from.as_deref() == Some("Player.player_id")
            && column
                .foreign_key
                .as_ref()
                .is_some_and(|foreign_key| foreign_key.table == "players")
    }));
}

#[test]
fn registry_rejects_duplicate_tables_and_invalid_foreign_key_targets() {
    let mut duplicate_registry = TableSchemaRegistry::new();
    duplicate_registry.register::<AccountSummary>().unwrap();

    let err = duplicate_registry.register::<AccountSummary>().unwrap_err();

    assert!(
        matches!(err, TableStoreError::Metadata(message) if message.contains("already contains table"))
    );

    let mut invalid_registry = TableSchemaRegistry::new();
    invalid_registry
        .register_schema(PlayerWeapon::schema().clone())
        .unwrap();

    let err = invalid_registry.validate().unwrap_err();

    assert!(
        matches!(err, TableStoreError::Metadata(message) if message.contains("unregistered foreign-key table"))
    );
}

#[test]
fn registry_rejects_relationship_foreign_keys_missing_from_target_model() {
    let mut player_schema = Player::schema().clone();
    player_schema.relationships[0].foreign_key = Some("missing_player_id".into());
    let mut registry = TableSchemaRegistry::new();
    registry
        .register::<AccountSummary>()
        .unwrap()
        .register_schema(player_schema)
        .unwrap()
        .register::<PlayerWeapon>()
        .unwrap();

    let err = registry.validate().unwrap_err();

    assert!(matches!(err, TableStoreError::Metadata(message)
            if message.contains("foreign key `missing_player_id`")
                && message.contains("target model `PlayerWeapon`")));
}
