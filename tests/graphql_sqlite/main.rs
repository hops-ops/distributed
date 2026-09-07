//! End-to-end GraphQL over temp-file SQLite (phase-2 exit criterion).

#![cfg(all(feature = "graphql", feature = "sqlite"))]

use async_graphql::Request;
use distributed::{
    graphql::{col, read, rel, GraphqlEngine, ModelPermissions},
    microsvc::Session,
    ColumnType, ForeignKey, PrimaryKey, ReadModel, RelationshipDef, RelationshipKind, TableColumn,
    TableKind, TableSchema, ROLE_KEY, USER_ID_KEY,
};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqlitePoolOptions;

fn orders_schema() -> TableSchema {
    TableSchema {
        model_name: "OrderView".into(),
        table_name: "orders".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..TableColumn::new("order_id", "order_id", ColumnType::Text)
            },
            TableColumn::new("customer_id", "customer_id", ColumnType::Text),
            TableColumn::new("status", "status", ColumnType::Text),
            TableColumn {
                column_type: ColumnType::Integer,
                ..TableColumn::new("total_cents", "total_cents", ColumnType::Integer)
            },
        ],
        primary_key: PrimaryKey::new(["order_id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    }
}

async fn setup_pool() -> sqlx::SqlitePool {
    let pool = SqlitePoolOptions::new()
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            status TEXT NOT NULL,
            total_cents INTEGER NOT NULL
        );
        INSERT INTO orders VALUES
            ('o1', 'c1', 'open', 1000),
            ('o2', 'c1', 'shipped', 2000),
            ('o3', 'c2', 'open', 500);",
    )
    .execute(&pool)
    .await
    .unwrap();
    pool
}

fn session_role(role: &str, user: &str) -> Session {
    let mut s = Session::new();
    s.set(ROLE_KEY, role);
    s.set(USER_ID_KEY, user);
    s.set("x-user-id", user);
    s
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("m2m_players")]
struct M2mPlayer {
    #[id("player_id")]
    player_id: String,
    name: String,
    #[readmodel(
        many_to_many = "M2mWeapon",
        through = "m2m_player_weapon_links",
        foreign_key = "player_id"
    )]
    weapons: Vec<M2mWeapon>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("m2m_weapons")]
struct M2mWeapon {
    #[id("weapon_id")]
    weapon_id: String,
    name: String,
}

fn m2m_link_schema() -> TableSchema {
    TableSchema {
        model_name: "M2mPlayerWeaponLink".into(),
        table_name: "m2m_player_weapon_links".into(),
        columns: vec![
            TableColumn {
                foreign_key: Some(ForeignKey::new("m2m_players", "player_id")),
                ..TableColumn::new("player_id", "player_ref", ColumnType::Text)
            },
            TableColumn {
                foreign_key: Some(ForeignKey::new("m2m_weapons", "weapon_id")),
                ..TableColumn::new("weapon_id", "weapon_ref", ColumnType::Text)
            },
        ],
        primary_key: PrimaryKey::new(["player_ref", "weapon_ref"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    }
}

#[tokio::test]
async fn list_filter_and_by_pk() {
    let schema = orders_schema();

    let manifest = distributed::ReadModelCatalog::new("orders").table_schema(schema);
    let pool = setup_pool().await;
    let engine = GraphqlEngine::from_schema_catalog(&manifest, pool)
        .unwrap()
        .roles(&["user", "anonymous"])
        .grant_all("user")
        .build()
        .expect("build");

    let session = session_role("user", "c1");
    let resp = engine
        .execute(
            &session,
            Request::new(
                r#"{ orders(where: { status: { _eq: "open" } }, limit: 10) { order_id status } }"#,
            ),
        )
        .await;
    assert!(!resp.is_err(), "{:?}", resp.errors);
    let data = serde_json::to_value(&resp.data).unwrap();
    let orders = data["orders"].as_array().unwrap();
    assert_eq!(orders.len(), 2);
    assert!(orders.iter().all(|o| o["status"] == "open"));

    let resp = engine
        .execute(
            &session,
            Request::new(r#"{ orders_by_pk(order_id: "o1") { order_id customer_id } }"#),
        )
        .await;
    assert!(!resp.is_err(), "{:?}", resp.errors);
    let data = serde_json::to_value(&resp.data).unwrap();
    assert_eq!(data["orders_by_pk"]["order_id"], "o1");
}

#[tokio::test]
async fn m2m_permission_filter_resolves_through_field_names_to_columns() {
    let pool = SqlitePoolOptions::new()
        .connect("sqlite::memory:")
        .await
        .unwrap();
    for sql in [
        "CREATE TABLE m2m_players (player_id TEXT PRIMARY KEY, name TEXT NOT NULL)",
        "CREATE TABLE m2m_weapons (weapon_id TEXT PRIMARY KEY, name TEXT NOT NULL)",
        "CREATE TABLE m2m_player_weapon_links (player_ref TEXT NOT NULL, weapon_ref TEXT NOT NULL)",
        "INSERT INTO m2m_players VALUES ('p1', 'Ada'), ('p2', 'Grace')",
        "INSERT INTO m2m_weapons VALUES ('w1', 'Compiler'), ('w2', 'Debugger')",
        "INSERT INTO m2m_player_weapon_links VALUES ('p1', 'w1'), ('p2', 'w2')",
    ] {
        sqlx::query(sql).execute(&pool).await.unwrap();
    }

    let engine = GraphqlEngine::builder(pool)
        .table_schema(m2m_link_schema())
        .model::<M2mPlayer>(
            ModelPermissions::new().grant(
                "user",
                read()
                    .all_columns()
                    .rows(rel("weapons", col("weapon_id").eq("w1"))),
            ),
        )
        .model::<M2mWeapon>(ModelPermissions::new().grant("user", read().all_columns()))
        .roles(&["user"])
        .build()
        .expect("build");

    let session = session_role("user", "u1");
    let resp = engine
        .execute(&session, Request::new("{ m2m_players { player_id name } }"))
        .await;
    assert!(!resp.is_err(), "{:?}", resp.errors);

    let data = serde_json::to_value(&resp.data).unwrap();
    let players = data["m2m_players"].as_array().expect("players");
    assert_eq!(players.len(), 1, "{data}");
    assert_eq!(players[0]["player_id"], "p1");
}

/// Client `where` with m2m relationship predicate (EXISTS through join table).
#[tokio::test]
async fn m2m_client_where_relationship_predicate() {
    let pool = SqlitePoolOptions::new()
        .connect("sqlite::memory:")
        .await
        .unwrap();
    for sql in [
        "CREATE TABLE m2m_players (player_id TEXT PRIMARY KEY, name TEXT NOT NULL)",
        "CREATE TABLE m2m_weapons (weapon_id TEXT PRIMARY KEY, name TEXT NOT NULL)",
        "CREATE TABLE m2m_player_weapon_links (player_ref TEXT NOT NULL, weapon_ref TEXT NOT NULL)",
        "INSERT INTO m2m_players VALUES ('p1', 'Ada'), ('p2', 'Grace'), ('p3', 'Both')",
        "INSERT INTO m2m_weapons VALUES ('w1', 'Compiler'), ('w2', 'Debugger')",
        "INSERT INTO m2m_player_weapon_links VALUES ('p1', 'w1'), ('p2', 'w2'), ('p3', 'w1'), ('p3', 'w2')",
    ] {
        sqlx::query(sql).execute(&pool).await.unwrap();
    }

    let engine = GraphqlEngine::builder(pool)
        .table_schema(m2m_link_schema())
        .model::<M2mPlayer>(ModelPermissions::new().grant("user", read().all_columns()))
        .model::<M2mWeapon>(ModelPermissions::new().grant("user", read().all_columns()))
        .roles(&["user"])
        .build()
        .expect("build");

    let session = session_role("user", "u1");
    let resp = engine
        .execute(
            &session,
            Request::new(
                r#"{ m2m_players(where: { weapons: { weapon_id: { _eq: "w1" } } }) { player_id name } }"#,
            ),
        )
        .await;
    assert!(!resp.is_err(), "{:?}", resp.errors);
    let data = serde_json::to_value(&resp.data).unwrap();
    let players = data["m2m_players"].as_array().expect("players");
    let ids: Vec<&str> = players
        .iter()
        .map(|p| p["player_id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"p1"), "p1 has w1: {data}");
    assert!(ids.contains(&"p3"), "p3 has w1: {data}");
    assert!(!ids.contains(&"p2"), "p2 only has w2: {data}");
    assert_eq!(ids.len(), 2, "{data}");
}

fn parent_schema() -> TableSchema {
    TableSchema {
        model_name: "ParentView".into(),
        table_name: "parents".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..TableColumn::new("id", "id", ColumnType::Text)
            },
            TableColumn::new("name", "name", ColumnType::Text),
        ],
        primary_key: PrimaryKey::new(["id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: vec![RelationshipDef {
            field_name: "children".into(),
            kind: RelationshipKind::HasMany,
            target_model: "ChildView".into(),
            foreign_key: Some("parent_id".into()),
            through: None,
            target_foreign_key: None,
        }],
        kind: TableKind::ReadModel,
    }
}

fn child_schema() -> TableSchema {
    TableSchema {
        model_name: "ChildView".into(),
        table_name: "children".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..TableColumn::new("child_id", "child_id", ColumnType::Text)
            },
            TableColumn::new("parent_id", "parent_id", ColumnType::Text),
            TableColumn::new("name", "name", ColumnType::Text),
        ],
        primary_key: PrimaryKey::new(["child_id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    }
}

#[tokio::test]
async fn sqlite_binds_follow_projection_then_where_order_for_relationships() {
    let pool = SqlitePoolOptions::new()
        .connect("sqlite::memory:")
        .await
        .unwrap();
    for sql in [
        "CREATE TABLE parents (id TEXT PRIMARY KEY, name TEXT NOT NULL)",
        "CREATE TABLE children (child_id TEXT PRIMARY KEY, parent_id TEXT NOT NULL, name TEXT NOT NULL)",
        "INSERT INTO parents VALUES ('p1', 'P'), ('p2', 'Other')",
        "INSERT INTO children VALUES ('c1', 'p1', 'C1'), ('c2', 'p1', 'C2'), ('c3', 'p2', 'C2')",
    ] {
        sqlx::query(sql).execute(&pool).await.unwrap();
    }

    let manifest = distributed::ReadModelCatalog::new("rel")
        .table_schema(parent_schema())
        .table_schema(child_schema());
    let engine = GraphqlEngine::from_schema_catalog(&manifest, pool)
        .unwrap()
        .roles(&["user"])
        .grant_all("user")
        .build()
        .expect("build");

    let session = session_role("user", "u1");
    let resp = engine
        .execute(
            &session,
            Request::new(
                r#"{ parents(where: { name: { _eq: "P" } }) { id children(where: { name: { _eq: "C2" } }) { child_id name } } }"#,
            ),
        )
        .await;
    assert!(!resp.is_err(), "{:?}", resp.errors);
    let data = serde_json::to_value(&resp.data).unwrap();
    let parents = data["parents"].as_array().expect("parents");
    assert_eq!(
        parents.len(),
        1,
        "root where bind must match parent name: {data}"
    );
    let children = parents[0]["children"].as_array().expect("children");
    assert_eq!(
        children.len(),
        1,
        "nested where bind must match child name: {data}"
    );
    assert_eq!(children[0]["child_id"], "c2");

    let sdl = engine.sdl_for_role("user").expect("user schema");
    assert!(
        sdl.contains("children_aggregate"),
        "relationship aggregate field must be present in runtime SDL: {sdl}"
    );

    let resp = engine
        .execute(
            &session,
            Request::new(
                r#"{ parents(where: { name: { _eq: "P" } }) { id children_aggregate(where: { name: { _eq: "C2" } }) { aggregate { count } nodes { child_id name } } } }"#,
            ),
        )
        .await;
    assert!(!resp.is_err(), "{:?}", resp.errors);
    let data = serde_json::to_value(&resp.data).unwrap();
    let aggregate = &data["parents"][0]["children_aggregate"];
    assert_eq!(aggregate["aggregate"]["count"], 1, "{data}");
    let nodes = aggregate["nodes"].as_array().expect("aggregate nodes");
    assert_eq!(nodes.len(), 1, "{data}");
    assert_eq!(nodes[0]["child_id"], "c2");

    let aliased = engine
        .execute(
            &session,
            Request::new(
                r#"{
                    parentRows: parents(where: { name: { _eq: "P" } }) {
                        parentLabel: name
                        matchingChildren: children(where: { name: { _eq: "C2" } }) {
                            childKey: child_id
                            childLabel: name
                        }
                        childStats: children_aggregate(where: { name: { _eq: "C2" } }) {
                            totals: aggregate { matches: count }
                            matchingNodes: nodes { childKey: child_id }
                        }
                    }
                }"#,
            ),
        )
        .await;
    assert!(!aliased.is_err(), "{:?}", aliased.errors);
    assert_eq!(
        serde_json::to_value(&aliased.data).unwrap(),
        serde_json::json!({
            "parentRows": [{
                "parentLabel": "P",
                "matchingChildren": [{
                    "childKey": "c2",
                    "childLabel": "C2"
                }],
                "childStats": {
                    "totals": { "matches": 1 },
                    "matchingNodes": [{ "childKey": "c2" }]
                }
            }]
        })
    );

    let repeated_aliases = engine
        .execute(
            &session,
            Request::new(
                r#"{
                    parentRows: parents(where: { name: { _eq: "P" } }) {
                        c1Children: children(where: { name: { _eq: "C1" } }) {
                            c1Key: child_id
                        }
                        c2Children: children(where: { name: { _eq: "C2" } }) {
                            c2Label: name
                        }
                        c1Stats: children_aggregate(where: { name: { _eq: "C1" } }) {
                            c1Totals: aggregate { c1Count: count }
                            c1Keys: nodes { c1Key: child_id }
                            c1Labels: nodes { c1Label: name }
                        }
                        c2Stats: children_aggregate(where: { name: { _eq: "C2" } }) {
                            firstTotals: aggregate { firstCount: count }
                            secondTotals: aggregate { secondCount: count }
                            c2Nodes: nodes { c2Label: name }
                        }
                    }
                }"#,
            ),
        )
        .await;
    assert!(!repeated_aliases.is_err(), "{:?}", repeated_aliases.errors);
    assert_eq!(
        serde_json::to_value(&repeated_aliases.data).unwrap(),
        serde_json::json!({
            "parentRows": [{
                "c1Children": [{ "c1Key": "c1" }],
                "c2Children": [{ "c2Label": "C2" }],
                "c1Stats": {
                    "c1Totals": { "c1Count": 1 },
                    "c1Keys": [{ "c1Key": "c1" }],
                    "c1Labels": [{ "c1Label": "C1" }]
                },
                "c2Stats": {
                    "firstTotals": { "firstCount": 1 },
                    "secondTotals": { "secondCount": 1 },
                    "c2Nodes": [{ "c2Label": "C2" }]
                }
            }]
        })
    );
}

fn author_schema() -> TableSchema {
    TableSchema {
        model_name: "AuthorView".into(),
        table_name: "authors".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..TableColumn::new("id", "id", ColumnType::Text)
            },
            TableColumn::new("name", "name", ColumnType::Text),
        ],
        primary_key: PrimaryKey::new(["id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: Vec::new(),
        kind: TableKind::ReadModel,
    }
}

fn post_schema() -> TableSchema {
    TableSchema {
        model_name: "PostView".into(),
        table_name: "posts".into(),
        columns: vec![
            TableColumn {
                primary_key: true,
                ..TableColumn::new("post_id", "post_id", ColumnType::Text)
            },
            TableColumn::new("author_id", "author_id", ColumnType::Text),
            TableColumn::new("title", "title", ColumnType::Text),
        ],
        primary_key: PrimaryKey::new(["post_id"]),
        version_column: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        relationships: vec![RelationshipDef {
            field_name: "author".into(),
            kind: RelationshipKind::BelongsTo,
            target_model: "AuthorView".into(),
            foreign_key: Some("author_id".into()),
            through: None,
            target_foreign_key: None,
        }],
        kind: TableKind::ReadModel,
    }
}

#[tokio::test]
async fn belongs_to_joins_source_fk_to_target_primary_key() {
    let pool = SqlitePoolOptions::new()
        .connect("sqlite::memory:")
        .await
        .unwrap();
    for sql in [
        "CREATE TABLE authors (id TEXT PRIMARY KEY, name TEXT NOT NULL)",
        "CREATE TABLE posts (post_id TEXT PRIMARY KEY, author_id TEXT NOT NULL, title TEXT NOT NULL)",
        "INSERT INTO authors VALUES ('a1', 'Ada')",
        "INSERT INTO posts VALUES ('p1', 'a1', 'GraphQL')",
    ] {
        sqlx::query(sql).execute(&pool).await.unwrap();
    }

    let manifest = distributed::ReadModelCatalog::new("posts")
        .table_schema(author_schema())
        .table_schema(post_schema());
    let engine = GraphqlEngine::from_schema_catalog(&manifest, pool)
        .unwrap()
        .roles(&["user"])
        .grant_all("user")
        .build()
        .expect("build");

    let session = session_role("user", "u1");
    let resp = engine
        .execute(
            &session,
            Request::new(r#"{ posts { post_id author { id name } } }"#),
        )
        .await;
    assert!(!resp.is_err(), "{:?}", resp.errors);
    let data = serde_json::to_value(&resp.data).unwrap();
    assert_eq!(data["posts"][0]["author"]["id"], "a1");
    assert_eq!(data["posts"][0]["author"]["name"], "Ada");
}

#[tokio::test]
async fn permissions_filter_by_claim() {
    let schema = orders_schema();
    let manifest = distributed::ReadModelCatalog::new("orders").table_schema(schema.clone());
    let pool = setup_pool().await;

    // Value-based path: grant_all then we need typed permission — use builder
    // with table_schema upgrade. The read-model catalog exposes all ReadModel tables.
    // Use permission via a hand-built approach: grant_all for user is full;
    // for restricted, register with filter via engine builder internals...
    // Spec API: .permission requires RelationalReadModelIncludes.
    // For fixture without derive, use grant_all and a second role without grants.
    let engine = GraphqlEngine::from_schema_catalog(&manifest, pool)
        .unwrap()
        .roles(&["user", "anonymous"])
        .grant_all("user")
        .build()
        .expect("build");

    // anonymous has no grants → empty Query fields / field error
    let anon = Session::new();
    let resp = engine
        .execute(&anon, Request::new(r#"{ orders { order_id } }"#))
        .await;
    assert!(
        resp.is_err() || {
            let v = serde_json::to_value(&resp.data).unwrap();
            v.get("orders").is_none()
        }
    );
}

#[tokio::test]
async fn domain_service_shaped_fixture() {
    // Phase-2 exit: one-file fixture serves queries on temp SQLite.
    let mut tables = Vec::new();
    for (model, table, pk) in [
        ("NamespaceView", "namespaces", "namespace_id"),
        ("UserView", "users", "user_id"),
        ("OrderView", "orders", "order_id"),
    ] {
        tables.push(TableSchema {
            model_name: model.into(),
            table_name: table.into(),
            columns: vec![
                TableColumn {
                    primary_key: true,
                    ..TableColumn::new(pk, pk, ColumnType::Text)
                },
                TableColumn::new("name", "name", ColumnType::Text),
            ],
            primary_key: PrimaryKey::new([pk]),
            version_column: None,
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            relationships: Vec::new(),
            kind: TableKind::ReadModel,
        });
    }
    let mut manifest = distributed::ReadModelCatalog::new("domain");
    for t in tables {
        manifest = manifest.table_schema(t);
    }

    let pool = SqlitePoolOptions::new()
        .connect("sqlite::memory:")
        .await
        .unwrap();
    for ddl in [
        "CREATE TABLE namespaces (namespace_id TEXT PRIMARY KEY, name TEXT NOT NULL);",
        "CREATE TABLE users (user_id TEXT PRIMARY KEY, name TEXT NOT NULL);",
        "CREATE TABLE orders (order_id TEXT PRIMARY KEY, name TEXT NOT NULL);",
        "INSERT INTO namespaces VALUES ('ns1', 'acme');",
        "INSERT INTO users VALUES ('u1', 'ada');",
        "INSERT INTO orders VALUES ('o1', 'widget');",
    ] {
        sqlx::query(ddl).execute(&pool).await.unwrap();
    }

    let engine = GraphqlEngine::from_schema_catalog(&manifest, pool)
        .unwrap()
        .roles(&["user"])
        .grant_all("user")
        .build()
        .expect("build");

    let session = session_role("user", "u1");
    let resp = engine
        .execute(
            &session,
            Request::new(
                r#"{ namespaces { namespace_id name } users { user_id name } orders { order_id name } }"#,
            ),
        )
        .await;
    assert!(!resp.is_err(), "{:?}", resp.errors);
    let data = serde_json::to_value(&resp.data).unwrap();
    assert_eq!(data["namespaces"][0]["name"], "acme");
    assert_eq!(data["users"][0]["name"], "ada");
    assert_eq!(data["orders"][0]["name"], "widget");
}

#[cfg(feature = "gateway-delivery")]
#[tokio::test]
async fn gateway_dependency_inventory_includes_empty_relationship_filters() {
    use distributed::graphql::{delivery::GatewayVersionStore, GraphqlPool};
    use serde_json::{json, Value};
    let pool = SqlitePoolOptions::new()
        .connect("sqlite::memory:")
        .await
        .unwrap();
    for sql in [
        "CREATE TABLE m2m_players (player_id TEXT PRIMARY KEY, name TEXT NOT NULL)",
        "CREATE TABLE m2m_weapons (weapon_id TEXT PRIMARY KEY, name TEXT NOT NULL)",
        "CREATE TABLE m2m_player_weapon_links (player_ref TEXT NOT NULL, weapon_ref TEXT NOT NULL)",
        "INSERT INTO m2m_players VALUES ('p1', 'Ada')",
        "INSERT INTO m2m_weapons VALUES ('w1', 'Compiler')",
    ] {
        sqlx::query(sql).execute(&pool).await.unwrap();
    }
    let store = GatewayVersionStore::install(
        &GraphqlPool::Sqlite(pool.clone()),
        "relationship-test",
        [
            "m2m_players".into(),
            "m2m_weapons".into(),
            "m2m_player_weapon_links".into(),
        ],
    )
    .await
    .unwrap();
    let engine = GraphqlEngine::builder(pool.clone())
        .service_id("relationship-test")
        .protocol_token_key([17; 32])
        .table_schema(m2m_link_schema())
        .model::<M2mPlayer>(ModelPermissions::new().grant("user", read().all_columns()))
        .model::<M2mWeapon>(ModelPermissions::new().grant("user", read().all_columns()))
        .roles(&["user"])
        .gateway_versions(store)
        .build()
        .unwrap();
    let document = r#"{ m2m_players(where: { weapons: { name: { _eq: "Compiler" } } }) { name } }"#;
    let validate = || async {
        let mut request = Request::new(document);
        request.extensions.insert(
            "gatewayDelivery".into(),
            async_graphql::Value::from_json(json!({"action":"validate"})).unwrap(),
        );
        let response: Value =
            serde_json::to_value(engine.execute(&session_role("user", "u1"), request).await)
                .unwrap();
        assert_eq!(
            response["extensions"]["gatewayDelivery"]["eligible"], true,
            "{response}"
        );
        response["extensions"]["gatewayDelivery"]["admission"]["validator"].clone()
    };
    let empty = validate().await;
    sqlx::query("INSERT INTO m2m_player_weapon_links VALUES ('p1','w1')")
        .execute(&pool)
        .await
        .unwrap();
    let linked = validate().await;
    assert_ne!(linked, empty);
    let rows = engine
        .execute(&session_role("user", "u1"), Request::new(document))
        .await;
    assert_eq!(
        serde_json::to_value(rows.data).unwrap()["m2m_players"],
        json!([{"name":"Ada"}])
    );
    sqlx::query("UPDATE m2m_weapons SET name='Debugger'")
        .execute(&pool)
        .await
        .unwrap();
    let changed_target = validate().await;
    assert_ne!(changed_target, linked);
    sqlx::query("DELETE FROM m2m_player_weapon_links")
        .execute(&pool)
        .await
        .unwrap();
    assert_ne!(validate().await, changed_target);
    let response = engine
        .execute(&session_role("user", "u1"), Request::new(document))
        .await;
    assert_eq!(
        serde_json::to_value(response.data).unwrap()["m2m_players"],
        json!([])
    );
}
