//! D* Resource-exhaustion red-team suite.

use std::sync::Arc;
use std::time::Duration;

use async_graphql::Request;
use distributed::graphql::{read, GraphqlEngine, ModelPermissions};

use super::common::{
    assert_no_sql_leak, engine_all_columns, error_messages, seed_orders, session, OrderView,
};

#[tokio::test]
async fn where_max_depth_rejected() {
    let pool = seed_orders().await;
    let engine = GraphqlEngine::builder(pool)
        .roles(&["user"])
        .model::<OrderView>(ModelPermissions::new().grant("user", read().all_columns()))
        .max_depth(2)
        .build()
        .unwrap();
    let s = session("user", "tenant-a");
    let q = r#"{
      orders(where: { _and: [{ _and: [{ _and: [{ status: { _eq: "open" } }] }] }] }) {
        order_id
      }
    }"#;
    let resp = engine.execute(&s, Request::new(q)).await;
    assert!(!resp.errors.is_empty(), "expected max depth error");
    let msgs = error_messages(&resp);
    assert!(
        msgs.contains("depth") || msgs.contains("bad request"),
        "expected depth-related client error, got {msgs}"
    );
    assert_no_sql_leak(&resp);
}

#[tokio::test]
async fn graphql_depth_counts_root_and_leaf_fields() {
    let pool = seed_orders().await;
    let too_shallow = GraphqlEngine::builder(pool.clone())
        .roles(&["user"])
        .model::<OrderView>(ModelPermissions::new().grant("user", read().all_columns()))
        .max_depth(1)
        .build()
        .unwrap();
    let exact = GraphqlEngine::builder(pool)
        .roles(&["user"])
        .model::<OrderView>(ModelPermissions::new().grant("user", read().all_columns()))
        .max_depth(2)
        .build()
        .unwrap();
    let s = session("user", "tenant-a");
    let rejected = too_shallow
        .execute(&s, Request::new("{ orders { order_id } }"))
        .await;
    assert!(!rejected.errors.is_empty(), "root plus leaf has depth two");
    assert_no_sql_leak(&rejected);

    let accepted = exact
        .execute(&s, Request::new("{ orders { order_id } }"))
        .await;
    assert!(accepted.errors.is_empty(), "{accepted:?}");
}

#[tokio::test]
async fn max_in_list_rejected() {
    let pool = seed_orders().await;
    let engine = GraphqlEngine::builder(pool)
        .roles(&["user"])
        .model::<OrderView>(ModelPermissions::new().grant("user", read().all_columns()))
        .max_in_list(3)
        .build()
        .unwrap();
    let s = session("user", "x");
    let q = r#"{ orders(where: { order_id: { _in: ["a","b","c","d"] } }) { order_id } }"#;
    let resp = engine.execute(&s, Request::new(q)).await;
    assert!(
        !resp.errors.is_empty(),
        "expected list-too-long style error"
    );
    assert_no_sql_leak(&resp);
}

#[tokio::test]
async fn limit_clamped_by_max_limit() {
    let pool = seed_orders().await;
    let engine = GraphqlEngine::builder(pool)
        .roles(&["user"])
        .model::<OrderView>(ModelPermissions::new().grant("user", read().all_columns()))
        .max_limit(1)
        .default_limit(1)
        .build()
        .unwrap();
    let s = session("user", "x");
    let resp = engine
        .execute(&s, Request::new("{ orders(limit: 100) { order_id } }"))
        .await;
    assert!(resp.errors.is_empty(), "{:?}", resp.errors);
    let data = serde_json::to_value(&resp.data).unwrap();
    assert_eq!(data["orders"].as_array().unwrap().len(), 1);
}

/// D5: wide `_or` list rejected by `max_bool_width`.
#[tokio::test]
async fn d5_wide_or_list_rejected_by_max_bool_width() {
    let pool = seed_orders().await;
    let engine = GraphqlEngine::builder(pool)
        .roles(&["user"])
        .model::<OrderView>(ModelPermissions::new().grant("user", read().all_columns()))
        .max_bool_width(3)
        .build()
        .unwrap();
    let s = session("user", "x");
    // 4 disjuncts > max_bool_width 3
    let q = r#"{
      orders(where: { _or: [
        { status: { _eq: "open" } },
        { status: { _eq: "shipped" } },
        { status: { _eq: "closed" } },
        { status: { _eq: "x" } }
      ] }) { order_id }
    }"#;
    let resp = engine.execute(&s, Request::new(q)).await;
    assert!(
        !resp.errors.is_empty(),
        "expected breadth limit error for wide _or"
    );
    let msgs = error_messages(&resp);
    assert!(
        msgs.contains("list too long") || msgs.contains("bad request"),
        "expected list-too-long style message, got {msgs}"
    );
    assert_no_sql_leak(&resp);
}

/// D5 complementary: `_and` width bound.
#[tokio::test]
async fn d5_wide_and_list_rejected_by_max_bool_width() {
    let pool = seed_orders().await;
    let engine = GraphqlEngine::builder(pool)
        .roles(&["user"])
        .model::<OrderView>(ModelPermissions::new().grant("user", read().all_columns()))
        .max_bool_width(2)
        .build()
        .unwrap();
    let s = session("user", "x");
    let q = r#"{
      orders(where: { _and: [
        { status: { _eq: "open" } },
        { total_cents: { _gt: 0 } },
        { order_id: { _neq: "" } }
      ] }) { order_id }
    }"#;
    let resp = engine.execute(&s, Request::new(q)).await;
    assert!(!resp.errors.is_empty(), "expected _and breadth error");
    assert_no_sql_leak(&resp);
}

/// D7: concurrent executes complete without hang; short timeouts map cleanly.
#[tokio::test]
async fn d7_concurrent_queries_complete() {
    let pool = seed_orders().await;
    let engine = Arc::new(engine_all_columns(pool));
    let mut handles = Vec::new();
    for i in 0..8 {
        let engine = Arc::clone(&engine);
        handles.push(tokio::spawn(async move {
            let s = session("user", "x");
            let q = if i % 2 == 0 {
                "{ orders { order_id } }"
            } else {
                r#"{ orders(where: { status: { _eq: "open" } }) { order_id status } }"#
            };
            let resp = engine.execute(&s, Request::new(q)).await;
            assert_no_sql_leak(&resp);
            resp.errors.is_empty()
        }));
    }
    for h in handles {
        assert!(h.await.expect("join"), "concurrent query should succeed");
    }
}

/// D7b: concurrent queries under tight statement_timeout still terminate.
#[tokio::test]
async fn d7_concurrent_with_timeout_bound_terminates() {
    use std::path::PathBuf;

    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("graphql_harden_concurrent_to");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("orders.db");
    let url = format!("sqlite:{}?mode=rwc", db.display());

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            status TEXT NOT NULL,
            total_cents INTEGER NOT NULL,
            note TEXT NOT NULL
        );
        INSERT INTO orders VALUES ('o1', 't', 'open', 1, 'n');",
    )
    .execute(&pool)
    .await
    .unwrap();

    let engine = Arc::new(
        GraphqlEngine::builder(pool)
            .roles(&["user"])
            .model::<OrderView>(ModelPermissions::new().grant("user", read().all_columns()))
            .statement_timeout(Duration::from_secs(2))
            .build()
            .unwrap(),
    );

    let mut handles = Vec::new();
    for _ in 0..6 {
        let engine = Arc::clone(&engine);
        handles.push(tokio::spawn(async move {
            let s = session("user", "x");
            let resp = engine
                .execute(&s, Request::new("{ orders { order_id } }"))
                .await;
            assert_no_sql_leak(&resp);
            // Success or timeout — both terminate.
            true
        }));
    }
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        for h in handles {
            assert!(h.await.expect("join"));
        }
    })
    .await;
    assert!(result.is_ok(), "concurrent suite must not hang");
}

/// Nested has_many fan-out exceeds relationship-aware complexity budget while
/// staying under max_depth (proves weights, not only depth).
#[tokio::test]
async fn d8_nested_has_many_exceeds_complexity_budget() {
    use distributed::graphql::{read, GraphqlEngine};
    use distributed::ReadModel;
    use distributed::{ReadModelCatalog, RelationalReadModel, RelationshipDef, RelationshipKind};
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
    #[table("parents")]
    struct ParentView {
        #[id("parent_id")]
        parent_id: String,
        name: String,
    }
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
    #[table("children")]
    struct ChildView {
        #[id("child_id")]
        child_id: String,
        parent_id: String,
        name: String,
    }
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
    #[table("grandchildren")]
    struct GrandView {
        #[id("grand_id")]
        grand_id: String,
        child_id: String,
        name: String,
    }

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE parents (parent_id TEXT PRIMARY KEY, name TEXT NOT NULL);
         CREATE TABLE children (
            child_id TEXT PRIMARY KEY, parent_id TEXT NOT NULL, name TEXT NOT NULL
         );
         CREATE TABLE grandchildren (
            grand_id TEXT PRIMARY KEY, child_id TEXT NOT NULL, name TEXT NOT NULL
         );
         INSERT INTO parents VALUES ('p1', 'P');
         INSERT INTO children VALUES ('c1', 'p1', 'C');
         INSERT INTO grandchildren VALUES ('g1', 'c1', 'G');",
    )
    .execute(&pool)
    .await
    .unwrap();

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
    let mut child = ChildView::schema().clone();
    child.relationships = vec![RelationshipDef {
        references: None,
        field_name: "grandchildren".into(),
        kind: RelationshipKind::HasMany,
        target_model: "GrandView".into(),
        foreign_key: Some("child_id".into()),
        through: None,
        target_foreign_key: None,
    }];
    let grand = GrandView::schema().clone();
    let manifest = ReadModelCatalog::new("cx")
        .table_schema(parent)
        .table_schema(child)
        .table_schema(grand);

    // Default max_complexity (500) + max_depth high enough that depth is not the limit.
    let engine = GraphqlEngine::from_schema_catalog(&manifest, pool)
        .unwrap()
        .roles(&["user"])
        .max_depth(8)
        .permission::<ParentView>("user", read().all_columns())
        .permission::<ChildView>("user", read().all_columns())
        .permission::<GrandView>("user", read().all_columns())
        .build()
        .expect("build");

    let s = session("user", "u");
    // 3-level has_many: relationship weights push estimated cost above 500.
    let q = r#"{
      parents {
        parent_id
        children {
          child_id
          name
          grandchildren {
            grand_id
            name
          }
        }
      }
    }"#;
    let resp = engine.execute(&s, Request::new(q)).await;
    assert!(
        !resp.errors.is_empty(),
        "3-level nested has_many must exceed complexity budget"
    );
    assert_no_sql_leak(&resp);
    let msgs = error_messages(&resp);
    assert!(
        msgs.contains("complex") || msgs.contains("bad request"),
        "expected complexity client error, got {msgs}"
    );
}

/// Shallow nest stays under default complexity budget.
#[tokio::test]
async fn d8_shallow_nested_has_many_within_budget() {
    use distributed::graphql::{read, GraphqlEngine};
    use distributed::ReadModel;
    use distributed::{ReadModelCatalog, RelationalReadModel, RelationshipDef, RelationshipKind};
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
    #[table("parents")]
    struct ParentView {
        #[id("parent_id")]
        parent_id: String,
        name: String,
    }
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
    #[table("children")]
    struct ChildView {
        #[id("child_id")]
        child_id: String,
        parent_id: String,
        name: String,
    }

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE parents (parent_id TEXT PRIMARY KEY, name TEXT NOT NULL);
         CREATE TABLE children (
            child_id TEXT PRIMARY KEY, parent_id TEXT NOT NULL, name TEXT NOT NULL
         );
         INSERT INTO parents VALUES ('p1', 'P');
         INSERT INTO children VALUES ('c1', 'p1', 'C');",
    )
    .execute(&pool)
    .await
    .unwrap();

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
    let manifest = ReadModelCatalog::new("cx2")
        .table_schema(parent)
        .table_schema(child);
    let engine = GraphqlEngine::from_schema_catalog(&manifest, pool)
        .unwrap()
        .roles(&["user"])
        .permission::<ParentView>("user", read().all_columns())
        .permission::<ChildView>("user", read().all_columns())
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
        resp.errors.is_empty(),
        "1-level nest must succeed under default budget: {:?}",
        resp.errors
    );
}

/// Explicit low max_complexity rejects modest nests (budget knob works).
#[tokio::test]
async fn d8_low_max_complexity_rejects_single_nest() {
    use distributed::graphql::{read, GraphqlEngine};
    use distributed::ReadModel;
    use distributed::{ReadModelCatalog, RelationalReadModel, RelationshipDef, RelationshipKind};
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
    #[table("parents")]
    struct ParentView {
        #[id("parent_id")]
        parent_id: String,
        name: String,
    }
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
    #[table("children")]
    struct ChildView {
        #[id("child_id")]
        child_id: String,
        parent_id: String,
        name: String,
    }

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE parents (parent_id TEXT PRIMARY KEY, name TEXT NOT NULL);
         CREATE TABLE children (
            child_id TEXT PRIMARY KEY, parent_id TEXT NOT NULL, name TEXT NOT NULL
         );
         INSERT INTO parents VALUES ('p1', 'P');
         INSERT INTO children VALUES ('c1', 'p1', 'C');",
    )
    .execute(&pool)
    .await
    .unwrap();

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
    let manifest = ReadModelCatalog::new("cx3")
        .table_schema(parent)
        .table_schema(child);
    let engine = GraphqlEngine::from_schema_catalog(&manifest, pool)
        .unwrap()
        .roles(&["user"])
        .max_complexity(20)
        .max_depth(8)
        .permission::<ParentView>("user", read().all_columns())
        .permission::<ChildView>("user", read().all_columns())
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
        "low budget must reject 1-level nest"
    );
    assert_no_sql_leak(&resp);
    let msgs = error_messages(&resp);
    assert!(
        msgs.contains("complex") || msgs.contains("bad request"),
        "{msgs}"
    );
}
