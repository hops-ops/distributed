//! A* Authorization red-team + related AuthZ e2e.
//! Drives shipped `GraphqlEngine::execute`.

use async_graphql::Request;
use distributed::graphql::IdentityConfig;
use distributed::graphql::{claim, col, read, GraphqlEngine, GraphqlPoolSource, ModelPermissions};
use distributed::{ReadModelCatalog, RelationalReadModel, RelationshipDef, RelationshipKind};

use common::graphql::{
    child, engine_all_columns, error_messages, exec_json, parent, seed_orders, session, ChildView,
    Db, OrderView, ParentView,
};

/// A1: claim row filter on list (existing harden case).
pub async fn claim_row_filter_isolates_tenants(db: Db) {
    let pool = seed_orders(&db).await;
    let engine = GraphqlEngine::builder(pool)
        .identity(IdentityConfig::dev_headers())
        .roles(&["user", "anonymous"])
        .model::<OrderView>(
            ModelPermissions::new().grant(
                "user",
                read()
                    .all_columns()
                    .rows(col("customer_id").eq(claim("x-user-id"))),
            ),
        )
        .build()
        .unwrap();

    let a = session("user", "tenant-a");
    let data = exec_json(&engine, &a, "{ orders { order_id customer_id } }").await;
    let orders = data["orders"].as_array().unwrap();
    assert_eq!(orders.len(), 2);
    assert!(orders.iter().all(|o| o["customer_id"] == "tenant-a"));

    let b = session("user", "tenant-b");
    let data = exec_json(&engine, &b, "{ orders { order_id } }").await;
    assert_eq!(data["orders"].as_array().unwrap().len(), 1);

    // A2: by_pk cross-tenant → null
    let resp = engine
        .execute(
            &b,
            Request::new(r#"{ orders_by_pk(order_id: "o1") { order_id } }"#),
        )
        .await;
    assert!(resp.errors.is_empty(), "{:?}", resp.errors);
    let data = serde_json::to_value(&resp.data).unwrap();
    assert!(
        data["orders_by_pk"].is_null() || data.get("orders_by_pk").is_none(),
        "cross-tenant by_pk must not leak: {data}"
    );
}

/// A2 focused: claim filter on by_pk of own vs other tenant.
pub async fn a2_claim_filter_on_by_pk_isolates_tenants(db: Db) {
    let pool = seed_orders(&db).await;
    let engine = GraphqlEngine::builder(pool)
        .identity(IdentityConfig::dev_headers())
        .roles(&["user"])
        .model::<OrderView>(
            ModelPermissions::new().grant(
                "user",
                read()
                    .all_columns()
                    .rows(col("customer_id").eq(claim("x-user-id"))),
            ),
        )
        .build()
        .unwrap();

    let a = session("user", "tenant-a");
    let data = exec_json(
        &engine,
        &a,
        r#"{ orders_by_pk(order_id: "o1") { order_id customer_id } }"#,
    )
    .await;
    assert_eq!(data["orders_by_pk"]["order_id"], "o1");
    assert_eq!(data["orders_by_pk"]["customer_id"], "tenant-a");

    let b = session("user", "tenant-b");
    let data = exec_json(
        &engine,
        &b,
        r#"{ orders_by_pk(order_id: "o1") { order_id } }"#,
    )
    .await;
    assert!(
        data["orders_by_pk"].is_null(),
        "tenant-b must not read tenant-a by_pk: {data}"
    );
}

/// A3: claim filter on aggregate count.
pub async fn a3_claim_filter_on_aggregate_count(db: Db) {
    let pool = seed_orders(&db).await;
    let engine = GraphqlEngine::builder(pool)
        .identity(IdentityConfig::dev_headers())
        .roles(&["user"])
        .model::<OrderView>(
            ModelPermissions::new().grant(
                "user",
                read()
                    .all_columns()
                    .aggregations()
                    .rows(col("customer_id").eq(claim("x-user-id"))),
            ),
        )
        .build()
        .unwrap();

    let a = session("user", "tenant-a");
    let data = exec_json(&engine, &a, "{ orders_aggregate { aggregate { count } } }").await;
    assert_eq!(
        data["orders_aggregate"]["aggregate"]["count"], 2,
        "tenant-a has 2 orders: {data}"
    );

    let b = session("user", "tenant-b");
    let data = exec_json(&engine, &b, "{ orders_aggregate { aggregate { count } } }").await;
    assert_eq!(
        data["orders_aggregate"]["aggregate"]["count"], 1,
        "tenant-b has 1 order: {data}"
    );
}

/// Column allowlist on root list (existing).
pub async fn column_allowlist_denies_ungranted_fields(db: Db) {
    let pool = seed_orders(&db).await;
    let engine = GraphqlEngine::builder(pool)
        .identity(IdentityConfig::dev_headers())
        .roles(&["restricted", "user"])
        .model::<OrderView>(
            ModelPermissions::new()
                .grant("restricted", read().columns(["order_id", "status"]))
                .grant("user", read().all_columns()),
        )
        .build()
        .unwrap();

    let restricted = session("restricted", "tenant-a");
    let resp = engine
        .execute(
            &restricted,
            Request::new("{ orders { order_id status customer_id } }"),
        )
        .await;
    assert!(
        !resp.errors.is_empty(),
        "customer_id must be unknown for restricted role: {:?}",
        resp.errors
    );
    let msgs = error_messages(&resp);
    assert!(
        msgs.contains("customer_id") || msgs.contains("unknown field"),
        "expected unknown field for denied column, got {msgs}"
    );

    let data = exec_json(&engine, &restricted, "{ orders { order_id status } }").await;
    let row = &data["orders"][0];
    assert!(row.get("order_id").is_some());
    assert!(row.get("customer_id").is_none());
    assert!(row.get("total_cents").is_none());
}

/// A5: nested relationship cannot project ungranted child columns.
pub async fn a5_nested_relationship_column_allowlist_denies(db: Db) {
    db.seed([parent("p1", "P")]).await;
    db.seed([child("c1", "p1", "secret-name")]).await;
    let pool = db.pool();

    let mut parent = ParentView::schema().clone();
    parent.relationships = vec![RelationshipDef {
        references: None,
        field_name: "children".into(),
        kind: RelationshipKind::HasMany,
        target_model: "ChildView".into(),
        foreign_key: Some("parent_id".into()),
        through: None,
        target_foreign_key: None,
    }];
    let child = ChildView::schema().clone();
    let manifest = ReadModelCatalog::new("rel-authz")
        .table_schema(parent)
        .table_schema(child);

    // Parent: all columns; child: only child_id (not name).
    let engine = GraphqlEngine::from_schema_catalog(&manifest, pool)
        .unwrap()
        .identity(IdentityConfig::dev_headers())
        .roles(&["user"])
        .permission::<ParentView>("user", read().all_columns())
        .permission::<ChildView>("user", read().columns(["child_id", "parent_id"]))
        .build()
        .expect("build");

    let s = session("user", "u");
    let resp = engine
        .execute(
            &s,
            Request::new("{ parents { parent_id children { child_id name } } }"),
        )
        .await;
    assert!(
        !resp.errors.is_empty(),
        "child.name must be denied for restricted child grants: {:?}",
        resp.errors
    );
    let msgs = error_messages(&resp);
    assert!(
        msgs.contains("name") || msgs.contains("unknown field"),
        "expected unknown field for nested denied column, got {msgs}"
    );

    // Allowed nested selection still works.
    let data = exec_json(
        &engine,
        &s,
        "{ parents { parent_id children { child_id } } }",
    )
    .await;
    let children = data["parents"][0]["children"].as_array().unwrap();
    assert_eq!(children.len(), 1);
    assert_eq!(children[0]["child_id"], "c1");
    assert!(children[0].get("name").is_none());
}

fn parent_child_engine(pool: GraphqlPoolSource) -> GraphqlEngine {
    let mut parent = ParentView::schema().clone();
    parent.relationships = vec![RelationshipDef {
        references: None,
        field_name: "children".into(),
        kind: RelationshipKind::HasMany,
        target_model: "ChildView".into(),
        foreign_key: Some("parent_id".into()),
        through: None,
        target_foreign_key: None,
    }];
    let child = ChildView::schema().clone();
    let manifest = ReadModelCatalog::new("rel")
        .table_schema(parent)
        .table_schema(child);

    GraphqlEngine::from_schema_catalog(&manifest, pool)
        .unwrap()
        .identity(IdentityConfig::dev_headers())
        .roles(&["user"])
        .grant_all("user")
        .build()
        .expect("build")
}

async fn seed_parents_children(db: &Db) -> GraphqlPoolSource {
    db.seed([parent("p1", "P")]).await;
    db.seed([child("c1", "p1", "C1"), child("c2", "p1", "C2")])
        .await;
    db.pool()
}

pub async fn nested_has_many_relationship_e2e(db: Db) {
    let pool = seed_parents_children(&db).await;
    let engine = parent_child_engine(pool);
    let s = session("user", "u");
    let data = exec_json(
        &engine,
        &s,
        "{ parents { parent_id children { child_id name } } }",
    )
    .await;
    let children = data["parents"][0]["children"].as_array().unwrap();
    assert_eq!(children.len(), 2);
}

/// Regression: by_pk with nested has_many must keep SQLite `?` binds aligned
/// (projection subquery LIMIT/OFFSET appear before outer WHERE in SQL text).
pub async fn by_pk_with_nested_children_returns_row(db: Db) {
    let pool = seed_parents_children(&db).await;
    let engine = parent_child_engine(pool);
    let s = session("user", "u");
    let data = exec_json(
        &engine,
        &s,
        r#"{ parents_by_pk(parent_id: "p1") { parent_id name children { child_id name } } }"#,
    )
    .await;
    let row = &data["parents_by_pk"];
    assert!(
        !row.is_null(),
        "parents_by_pk must return a row with nested children, got {data}"
    );
    assert_eq!(row["parent_id"], "p1");
    assert_eq!(row["name"], "P");
    let children = row["children"].as_array().expect("children array");
    assert_eq!(children.len(), 2, "{data}");
    let ids: Vec<_> = children
        .iter()
        .map(|c| c["child_id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"c1") && ids.contains(&"c2"), "{data}");
}

pub async fn json_looking_string_column_stays_string(db: Db) {
    let pool = seed_orders(&db).await;
    let engine = engine_all_columns(pool);
    let s = session("user", "tenant-a");
    let data = exec_json(&engine, &s, r#"{ orders_by_pk(order_id: "o1") { note } }"#).await;
    assert!(
        data["orders_by_pk"]["note"].is_string(),
        "note must remain string, got {:?}",
        data["orders_by_pk"]["note"]
    );
    assert_eq!(data["orders_by_pk"]["note"], "{\"looks\":\"json\"}");
}

/// A multi-role application surface executes with the intersection of its
/// schema roles' row policies: every opener is held to the most restrictive
/// combination, never to whichever role sorts first.
pub async fn application_surface_with_differing_row_policies_applies_all_of_them(db: Db) {
    let pool = seed_orders(&db).await;
    let roles = ["admin".to_string(), "user".to_string()];
    let engine = GraphqlEngine::builder(pool)
        .identity(IdentityConfig::dev_headers())
        .service_id("orders-service")
        .roles(&["admin", "user"])
        .model::<OrderView>(
            ModelPermissions::new()
                .grant("admin", read().all_columns().aggregations())
                .grant(
                    "user",
                    read()
                        .all_columns()
                        .aggregations()
                        .rows(col("customer_id").eq(claim("x-user-id"))),
                ),
        )
        .client_application_surface("console", roles.clone(), roles.clone())
        .protocol_token_key([7; 32])
        .build()
        .unwrap();
    let manifest = engine
        .client_manifest_for_application("console", &["admin", "user"], &["admin", "user"])
        .unwrap();
    let request = |query: &str| -> Request {
        serde_json::from_value(serde_json::json!({
            "query": query,
            "extensions": { "distributed": { "client": {
                "surface": {
                    "kind": "application",
                    "name": "console",
                    "eligible_roles": roles,
                    "schema_roles": roles,
                },
                "schemaHash": manifest.schema_fingerprint,
            }}}
        }))
        .unwrap()
    };

    for role in ["user", "admin"] {
        let principal = session(role, "tenant-b");
        for (query, expected) in [
            (
                "{ orders { order_id customer_id } }",
                serde_json::json!({ "orders": [{ "order_id": "o3", "customer_id": "tenant-b" }] }),
            ),
            (
                r#"{ orders_by_pk(order_id: "o1") { order_id } }"#,
                serde_json::json!({ "orders_by_pk": null }),
            ),
            (
                "{ orders_aggregate { aggregate { count } nodes { order_id } } }",
                serde_json::json!({ "orders_aggregate": {
                    "aggregate": { "count": 1 },
                    "nodes": [{ "order_id": "o3" }],
                }}),
            ),
        ] {
            let resp = engine.execute(&principal, request(query)).await;
            assert!(resp.errors.is_empty(), "{role} {query}: {:?}", resp.errors);
            assert_eq!(
                serde_json::to_value(&resp.data).unwrap(),
                expected,
                "{role} {query}"
            );
        }
    }
}
