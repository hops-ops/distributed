//! Golden-file tests for the dep-free GraphQL SDL renderer.

use distributed::{
    graphql::{graphql_sdl_for_tables, graphql_sdl_for_tables_with_options, SdlOptions},
    ColumnType, ForeignKey, PrimaryKey, RelationshipDef, RelationshipKind, TableColumn, TableKind,
    TableSchema,
};

fn players() -> TableSchema {
    TableSchema {
        model_name: "PlayerView".into(),
        table_name: "players".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..TableColumn::new("player_id", "player_id", ColumnType::Text)
            },
            TableColumn::new("display_name", "display_name", ColumnType::Text),
            TableColumn {
                skipped: true,
                ..TableColumn::new("secret", "secret", ColumnType::Text)
            },
        ],
        primary_key: PrimaryKey::new(["player_id"]),
        version_column: Some("_sourced_version".into()),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: vec![RelationshipDef {
            references: None,
            field_name: "weapons".into(),
            kind: RelationshipKind::HasMany,
            target_model: "PlayerWeaponView".into(),
            foreign_key: Some("player_id".into()),
            through: None,
            target_foreign_key: None,
        }],
        kind: TableKind::ReadModel,
    }
}

fn weapons() -> TableSchema {
    TableSchema {
        model_name: "PlayerWeaponView".into(),
        table_name: "player_weapons".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                foreign_key: Some(ForeignKey::new("players", "player_id")),
                ..TableColumn::new("player_id", "player_id", ColumnType::Text)
            },
            TableColumn {
                primary_key: true,
                ..TableColumn::new("weapon_id", "weapon_id", ColumnType::Text)
            },
            TableColumn {
                jsonb: true,
                ..TableColumn::new("meta", "meta", ColumnType::Json)
            },
        ],
        primary_key: PrimaryKey::new(["player_id", "weapon_id"]),
        version_column: Some("_sourced_version".into()),
        foreign_keys: vec![ForeignKey::new("players", "player_id")],
        indexes: Vec::new(),
        relationships: vec![RelationshipDef {
            references: None,
            field_name: "player".into(),
            kind: RelationshipKind::BelongsTo,
            target_model: "PlayerView".into(),
            foreign_key: Some("player_id".into()),
            through: None,
            target_foreign_key: None,
        }],
        kind: TableKind::ReadModel,
    }
}

fn operational_outbox() -> TableSchema {
    TableSchema {
        model_name: "OutboxMessage".into(),
        table_name: "outbox_messages".into(),
        columns: vec![TableColumn {
            primary_key: true,
            ..TableColumn::new("message_id", "message_id", ColumnType::Text)
        }],
        primary_key: PrimaryKey::new(["message_id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::Operational,
    }
}

#[test]
fn renders_players_weapons_surface() {
    let sdl = graphql_sdl_for_tables_with_options(
        &[players(), weapons()],
        &SdlOptions {
            aggregates: true,
            jsonb_operators: false,
            subscriptions: true,
        },
    )
    .expect("sdl");
    assert!(sdl.contains("type PlayerView"));
    assert!(sdl.contains("type PlayerWeaponView"));
    assert!(sdl.contains("players("));
    assert!(sdl.contains("players_by_pk(player_id: String!): PlayerView"));
    assert!(sdl.contains(
        "player_weapons_by_pk(player_id: String!, weapon_id: String!): PlayerWeaponView"
    ));
    assert!(sdl.contains("weapons("));
    assert!(sdl.contains("player:"));
    // skipped column and version never appear
    assert!(!sdl.contains("secret"));
    assert!(!sdl.contains("_sourced_version"));
    // operational tables not in this input
    assert!(!sdl.contains("outbox"));
    // custom scalars
    assert!(sdl.contains("scalar BigInt"));
    assert!(sdl.contains("scalar JSON"));
    // Subscription root present
    assert!(sdl.contains("type Subscription"));
}

#[test]
fn operational_tables_filtered() {
    let sdl = graphql_sdl_for_tables(&[players(), operational_outbox()]).expect("sdl");
    assert!(sdl.contains("PlayerView"));
    assert!(!sdl.contains("OutboxMessage"));
    assert!(!sdl.contains("outbox_messages"));
}

#[test]
fn omits_relationship_when_target_absent() {
    let sdl = graphql_sdl_for_tables(&[players()]).expect("sdl");
    // weapons relationship target not present → omitted
    assert!(!sdl.contains("  weapons("));
}

#[test]
fn m2m_requires_through_error() {
    let posts = TableSchema {
        model_name: "Post".into(),
        table_name: "posts".into(),
        columns: vec![TableColumn {
            primary_key: true,
            ..TableColumn::new("id", "id", ColumnType::Text)
        }],
        primary_key: PrimaryKey::new(["id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: vec![RelationshipDef {
            references: None,
            field_name: "tags".into(),
            kind: RelationshipKind::ManyToMany,
            target_model: "Tag".into(),
            foreign_key: Some("post_id".into()),
            through: None,
            target_foreign_key: None,
        }],
        kind: TableKind::ReadModel,
    };
    let err = graphql_sdl_for_tables(&[posts]).unwrap_err();
    assert!(err.contains("through"), "{err}");
}

#[test]
fn invalid_name_errors() {
    let bad = TableSchema {
        model_name: "1Bad".into(),
        table_name: "1bad".into(),
        columns: vec![TableColumn {
            primary_key: true,
            ..TableColumn::new("id", "id", ColumnType::Text)
        }],
        primary_key: PrimaryKey::new(["id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    };
    let err = graphql_sdl_for_tables(&[bad]).unwrap_err();
    assert!(err.contains("valid GraphQL name"), "{err}");
}

#[test]
fn collision_errors() {
    let a = TableSchema {
        model_name: "A".into(),
        table_name: "items".into(),
        columns: vec![TableColumn {
            primary_key: true,
            ..TableColumn::new("id", "id", ColumnType::Text)
        }],
        primary_key: PrimaryKey::new(["id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    };
    let b = TableSchema {
        model_name: "B".into(),
        table_name: "items".into(), // same table_name → root field collision
        columns: vec![TableColumn {
            primary_key: true,
            ..TableColumn::new("id", "id", ColumnType::Text)
        }],
        primary_key: PrimaryKey::new(["id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    };
    let err = graphql_sdl_for_tables(&[a, b]).unwrap_err();
    assert!(err.contains("collides"), "{err}");
}

#[test]
fn manifest_graphql_sdl_method() {
    let manifest = distributed::ReadModelCatalog::new("demo")
        .table_schema(players())
        .table_schema(weapons())
        .table_schema(operational_outbox());
    let sdl = graphql_sdl_for_tables(&manifest.tables).expect("sdl");
    assert!(sdl.contains("PlayerView"));
    assert!(!sdl.contains("OutboxMessage"));
}

#[test]
fn capture_sdl_to_scratch() {
    let path = std::env::var("GROK_SCRATCH_SDL").unwrap_or_else(|_| "/dev/null".into());
    if path == "/dev/null" {
        return;
    }
    let players = players();
    let weapons = weapons();
    let sdl = graphql_sdl_for_tables(&[players, weapons]).unwrap();
    std::fs::write(&path, &sdl).unwrap();
    assert!(sdl.contains("type PlayerView"));
}

/// Default / SQLite SDL omits PG jsonb comparison operators (weapons has JSON meta).
#[test]
fn sqlite_sdl_omits_postgres_json_comparison_ops() {
    let sdl = graphql_sdl_for_tables_with_options(&[weapons()], &SdlOptions::sqlite()).unwrap();
    assert!(
        sdl.contains("JSON_comparison_exp") || sdl.contains("input JSON_comparison_exp"),
        "expected JSON comparison input: {sdl}"
    );
    assert!(
        !sdl.contains("_contains"),
        "SQLite SDL must not advertise _contains: {sdl}"
    );
    assert!(
        !sdl.contains("_contained_in"),
        "SQLite SDL must not advertise _contained_in: {sdl}"
    );
    assert!(
        !sdl.contains("_has_key"),
        "SQLite SDL must not advertise _has_key: {sdl}"
    );
    assert!(sdl.contains("_eq"), "portable _eq must remain: {sdl}");
}

/// Postgres-oriented SDL includes jsonb comparison operators.
#[test]
fn postgres_sdl_includes_json_comparison_ops() {
    let sdl = graphql_sdl_for_tables_with_options(&[weapons()], &SdlOptions::postgres()).unwrap();
    assert!(sdl.contains("_contains"), "{sdl}");
    assert!(sdl.contains("_contained_in"), "{sdl}");
    assert!(sdl.contains("_has_key"), "{sdl}");
}

/// Default SdlOptions matches SQLite policy.
#[test]
fn default_sdl_options_are_sqlite() {
    let def = SdlOptions::default();
    let sqlite = SdlOptions::sqlite();
    assert_eq!(def.jsonb_operators, sqlite.jsonb_operators);
    assert!(!def.jsonb_operators);
}
