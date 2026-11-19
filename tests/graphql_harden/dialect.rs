//! Dialect-honest comparison surface (ship-dialect-1).
//!
//! SQLite engines must not expose Postgres jsonb operators on
//! `JSON_comparison_exp`. Failure is GraphQL unknown-field (type honesty),
//! with compile-time reject remaining as defense-in-depth.

use async_graphql::Request;
use distributed::graphql::IdentityConfig;
use distributed::graphql::{
    comparison_op_fields, include_postgres_json_comparison_ops, read, GraphqlEngine,
    ModelPermissions, POSTGRES_JSON_COMPARISON_OPS,
};
use distributed::ReadModel;
use serde::{Deserialize, Serialize};

use super::common::{assert_no_sql_leak, error_messages, session, Db};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("docs")]
struct DocView {
    #[id("doc_id")]
    doc_id: String,
    /// JSON column → GraphQL JSON scalar → JSON_comparison_exp.
    payload: serde_json::Value,
}

async fn engine() -> GraphqlEngine {
    let db = Db::sqlite().await;
    db.seed([DocView {
        doc_id: "d1".into(),
        payload: serde_json::json!({ "a": 1 }),
    }])
    .await;
    GraphqlEngine::builder(db.pool())
        .identity(IdentityConfig::dev_headers())
        .roles(&["user"])
        .model::<DocView>(ModelPermissions::new().grant("user", read().all_columns()))
        .build()
        .unwrap()
}

/// Matrix helper: shared naming policy says SQLite omits PG JSON ops.
#[test]
fn naming_matrix_sqlite_omits_pg_json_ops() {
    assert!(!include_postgres_json_comparison_ops(false));
    let fields = comparison_op_fields("JSON", false);
    for op in POSTGRES_JSON_COMPARISON_OPS {
        assert!(!fields.contains(op), "unexpected {op} in SQLite JSON ops");
    }
}

/// SQLite runtime: `_contains` is not a field on JSON_comparison_exp.
#[tokio::test]
async fn sqlite_json_contains_is_unknown_field() {
    let engine = engine().await;
    let s = session("user", "u");
    let resp = engine
        .execute(
            &s,
            Request::new(r#"{ docs(where: { payload: { _contains: { a: 1 } } }) { doc_id } }"#),
        )
        .await;
    assert!(
        !resp.errors.is_empty(),
        "SQLite must not accept _contains on JSON: {:?}",
        resp.data
    );
    assert_no_sql_leak(&resp);
    let msgs = error_messages(&resp);
    assert!(
        msgs.contains("_contains") || msgs.contains("unknown field") || msgs.contains("invalid"),
        "expected unknown/invalid field for _contains, got {msgs}"
    );
}

#[tokio::test]
async fn sqlite_json_has_key_is_unknown_field() {
    let engine = engine().await;
    let s = session("user", "u");
    let resp = engine
        .execute(
            &s,
            Request::new(r#"{ docs(where: { payload: { _has_key: "a" } }) { doc_id } }"#),
        )
        .await;
    assert!(!resp.errors.is_empty());
    assert_no_sql_leak(&resp);
    let msgs = error_messages(&resp);
    assert!(
        msgs.contains("_has_key") || msgs.contains("unknown field") || msgs.contains("invalid"),
        "{msgs}"
    );
}

#[tokio::test]
async fn sqlite_json_contained_in_is_unknown_field() {
    let engine = engine().await;
    let s = session("user", "u");
    let resp = engine
        .execute(
            &s,
            Request::new(
                r#"{ docs(where: { payload: { _contained_in: { a: 1, b: 2 } } }) { doc_id } }"#,
            ),
        )
        .await;
    assert!(!resp.errors.is_empty());
    assert_no_sql_leak(&resp);
}

/// Portable `_eq` is a known field on JSON_comparison_exp (SQLite).
#[tokio::test]
async fn sqlite_json_eq_is_known_field() {
    let engine = engine().await;
    let s = session("user", "u");
    let resp = engine
        .execute(
            &s,
            Request::new(r#"{ docs(where: { payload: { _eq: { a: 1 } } }) { doc_id } }"#),
        )
        .await;
    if !resp.errors.is_empty() {
        let msgs = error_messages(&resp);
        assert!(
            !msgs.contains("unknown field"),
            "portable _eq must be a known field on JSON_comparison_exp: {msgs}"
        );
        assert_no_sql_leak(&resp);
    } else {
        let data = serde_json::to_value(&resp.data).unwrap();
        assert!(data["docs"].as_array().is_some());
    }
}
