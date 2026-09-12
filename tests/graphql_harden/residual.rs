//! Residual red-team cases from post-quality-1 review (A8/A12/S9/E4/E6).

use async_graphql::Request;
use distributed::graphql::{claim, col, read, GraphqlEngine, ModelPermissions};
use distributed::{ReadModelCatalog, RelationalReadModel, RelationshipDef, RelationshipKind};

use super::common::{
    assert_no_sql_leak, engine_all_columns, error_messages, extension_code, seed_orders, session,
    ChildView, OrderView, ParentView,
};

/// A8: nested has_many without grant on child model → relationship field absent.
#[tokio::test]
async fn a8_nested_has_many_without_child_grant_is_unknown_field() {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE parents (parent_id TEXT PRIMARY KEY, name TEXT NOT NULL);
         CREATE TABLE children (
            child_id TEXT PRIMARY KEY,
            parent_id TEXT NOT NULL,
            name TEXT NOT NULL
         );
         INSERT INTO parents VALUES ('p1', 'P');
         INSERT INTO children VALUES ('c1', 'p1', 'secret');",
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
    let manifest = ReadModelCatalog::new("a8")
        .table_schema(parent)
        .table_schema(child);

    // Parent granted; child model has **no** permission for this role.
    let engine = GraphqlEngine::from_schema_catalog(&manifest, pool)
        .unwrap()
        .roles(&["user"])
        .permission::<ParentView>("user", read().all_columns())
        .build()
        .expect("build");

    let s = session("user", "u");
    let resp = engine
        .execute(
            &s,
            Request::new("{ parents { parent_id children { child_id } } }"),
        )
        .await;
    assert!(
        !resp.errors.is_empty(),
        "children must be unknown without child grant: {:?}",
        resp.errors
    );
    let msgs = error_messages(&resp);
    assert!(
        msgs.contains("children") || msgs.contains("unknown field"),
        "expected unknown field for ungranted nested rel, got {msgs}"
    );
    assert_no_sql_leak(&resp);

    // Parent columns still queryable.
    let resp = engine
        .execute(&s, Request::new("{ parents { parent_id name } }"))
        .await;
    assert!(resp.errors.is_empty(), "{:?}", resp.errors);
}

/// A12: relationship where on ungranted target — schema omits rel key from bool_exp
/// when target has no permission, so GraphQL rejects unknown field (not silent SQL).
#[tokio::test]
async fn a12_rel_where_without_target_grant_is_unknown_field() {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE parents (parent_id TEXT PRIMARY KEY, name TEXT NOT NULL);
         CREATE TABLE children (
            child_id TEXT PRIMARY KEY,
            parent_id TEXT NOT NULL,
            name TEXT NOT NULL
         );
         INSERT INTO parents VALUES ('p1', 'P');
         INSERT INTO children VALUES ('c1', 'p1', 'n');",
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
    let manifest = ReadModelCatalog::new("a12")
        .table_schema(parent)
        .table_schema(child);

    let engine = GraphqlEngine::from_schema_catalog(&manifest, pool)
        .unwrap()
        .roles(&["user"])
        .permission::<ParentView>("user", read().all_columns())
        // no ChildView permission
        .build()
        .expect("build");

    let s = session("user", "u");
    let resp = engine
        .execute(
            &s,
            Request::new(
                r#"{ parents(where: { children: { name: { _eq: "n" } } }) { parent_id } }"#,
            ),
        )
        .await;
    assert!(
        !resp.errors.is_empty(),
        "rel where without grant must error"
    );
    assert_no_sql_leak(&resp);
    let msgs = error_messages(&resp);
    assert!(
        msgs.contains("children") || msgs.contains("unknown") || msgs.contains("field"),
        "expected unknown field for ungranted rel where, got {msgs}"
    );
}

/// S9 / E6: PG JSON ops on SQLite → BAD_REQUEST / invalid filter, no SQL leak.
#[tokio::test]
async fn s9_json_contains_on_sqlite_is_bad_request_without_sql_leak() {
    let pool = seed_orders().await;
    // note is a String column; _contains still hits client op path if typed as object
    // Use a Json column if available — OrderView note is Text. Build engine with all columns
    // and use _has_key which is only valid for jsonb ops on comparison exp for JSON scalars.
    // Our comparison_exp for String may still accept _contains in dynamic schema.
    let engine = engine_all_columns(pool);
    let s = session("user", "x");
    let resp = engine
        .execute(
            &s,
            Request::new(r#"{ orders(where: { note: { _contains: { a: 1 } } }) { order_id } }"#),
        )
        .await;
    assert!(!resp.errors.is_empty(), "sqlite must reject _contains");
    assert_no_sql_leak(&resp);
    let msgs = error_messages(&resp);
    // sanitize → invalid filter or bad request — never raw SQL operators in client message
    assert!(
        !msgs.contains("@>") && !msgs.contains("jsonb"),
        "must not leak PG operator text: {msgs}"
    );
    if let Some(err) = resp.errors.first() {
        if let Some(code) = extension_code(err) {
            assert!(
                code.contains("BAD_REQUEST") || code.contains("bad"),
                "expected BAD_REQUEST code, got {code}"
            );
        }
    }
}

/// E4: missing claim header on permission filter → stable client error, no leak.
#[tokio::test]
async fn e4_missing_claim_header_is_stable_without_sql_leak() {
    let pool = seed_orders().await;
    let engine = GraphqlEngine::builder(pool)
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

    // Role present but claim header absent.
    let mut s = distributed::microsvc::Session::new();
    s.set(distributed::microsvc::ROLE_KEY, "user");
    // no USER_ID_KEY

    let resp = engine
        .execute(&s, Request::new("{ orders { order_id } }"))
        .await;
    assert!(!resp.errors.is_empty(), "missing claim must fail closed");
    assert_no_sql_leak(&resp);
    let msgs = error_messages(&resp);
    assert!(
        !msgs.contains("select ") && !msgs.contains("customer_id ="),
        "must not leak SQL/claim internals: {msgs}"
    );
    // Prefer BAD_REQUEST from sanitize path
    if let Some(err) = resp.errors.first() {
        if let Some(code) = extension_code(err) {
            assert!(
                code.contains("BAD_REQUEST") || code.contains("INTERNAL"),
                "stable code expected, got {code}"
            );
        }
    }
}
