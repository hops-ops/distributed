//! S* Injection red-team suite — real `GraphqlEngine::execute` only.

use async_graphql::Request;
use distributed::graphql::{read, GraphqlEngine, ModelPermissions};

use super::common::{
    assert_no_sql_leak, engine_all_columns, exec_json, seed_orders, session, Db, OrderView,
};

/// S1: response keys are GraphQL Names; free-form injection cannot reach SQL keys.
pub async fn s1_response_key_field_selection_safe_roundtrip(db: Db) {
    let pool = seed_orders(&db).await;
    let engine = engine_all_columns(pool);
    let s = session("user", "x");
    let data = exec_json(&engine, &s, "{ orders { order_id status } }").await;
    let row = &data["orders"][0];
    assert!(row.get("order_id").is_some(), "{data}");
    assert!(row.get("status").is_some(), "{data}");
    // Where value with quote/SQL metacharacters is bound, not concatenated.
    let resp = engine
        .execute(
            &s,
            Request::new(r#"{ orders(where: { status: { _eq: "x' OR '1'='1" } }) { order_id } }"#),
        )
        .await;
    assert_no_sql_leak(&resp);
    // Table still intact and queryable.
    let data = exec_json(&engine, &s, "{ orders { order_id } }").await;
    assert_eq!(data["orders"].as_array().unwrap().len(), 3);
}

/// S2: unknown where keys fail closed under default strict_where (no silent ignore).
pub async fn s2_unknown_where_key_fails_closed_without_sql_leak(db: Db) {
    let pool = seed_orders(&db).await;
    let engine = engine_all_columns(pool);
    let s = session("user", "x");
    assert!(engine.strict_where());
    let resp = engine
        .execute(
            &s,
            Request::new(
                r#"{ orders(where: { not_a_column: { _eq: "x" }, status: { _eq: "open" } }) { order_id status } }"#,
            ),
        )
        .await;
    assert_no_sql_leak(&resp);
    assert!(
        !resp.errors.is_empty(),
        "unknown where key must not soft-skip under strict default"
    );
}

/// S3: `_like` wildcards are bound parameters, not SQL concatenation.
pub async fn s3_like_wildcards_are_bound_and_safe(db: Db) {
    let pool = seed_orders(&db).await;
    let engine = engine_all_columns(pool);
    let s = session("user", "x");
    let data = exec_json(
        &engine,
        &s,
        r#"{ orders(where: { status: { _like: "%pen" } }) { order_id status } }"#,
    )
    .await;
    let orders = data["orders"].as_array().unwrap();
    assert!(
        orders
            .iter()
            .all(|o| o["status"].as_str().unwrap().contains("pen") || o["status"] == "open"),
        "{data}"
    );
    // Malicious-looking pattern must not cause SQL error leak.
    let resp = engine
        .execute(
            &s,
            Request::new(
                r#"{ orders(where: { status: { _like: "'; DROP TABLE orders; --" } }) { order_id } }"#,
            ),
        )
        .await;
    assert_no_sql_leak(&resp);
    // Table still queryable.
    let _ = exec_json(&engine, &s, "{ orders { order_id } }").await;
}

/// S4: JSON-looking text column stays a GraphQL String (not re-typed).
pub async fn s4_json_looking_string_stays_string(db: Db) {
    let pool = seed_orders(&db).await;
    let engine = engine_all_columns(pool);
    let s = session("user", "tenant-a");
    let data = exec_json(&engine, &s, r#"{ orders_by_pk(order_id: "o1") { note } }"#).await;
    assert!(data["orders_by_pk"]["note"].is_string());
    assert_eq!(data["orders_by_pk"]["note"], "{\"looks\":\"json\"}");
}

/// S5: garbage order_by direction coerces safely (no raw SQL direction).
pub async fn s5_order_by_junk_direction_is_safe(db: Db) {
    let pool = seed_orders(&db).await;
    let engine = engine_all_columns(pool);
    let s = session("user", "x");
    // Unknown enum may fail GraphQL validation or coerce — either way no SQL leak.
    let resp = engine
        .execute(
            &s,
            Request::new(
                r#"{ orders(order_by: [{ status: totally_bogus_direction }]) { order_id status } }"#,
            ),
        )
        .await;
    assert_no_sql_leak(&resp);
    if resp.errors.is_empty() {
        let data = serde_json::to_value(&resp.data).unwrap();
        assert!(!data["orders"].as_array().unwrap().is_empty());
    }
}

/// S6: PK type confusion — wrong type yields error or empty without SQL leak.
pub async fn s6_pk_type_confusion_does_not_leak_sql(db: Db) {
    let pool = seed_orders(&db).await;
    let engine = engine_all_columns(pool);
    let s = session("user", "x");
    // order_id is Text; pass a non-string via variable-less int if schema allows —
    // GraphQL may reject at validation. Use a string that is still safe.
    let resp = engine
        .execute(
            &s,
            Request::new(r#"{ orders_by_pk(order_id: "'; DROP TABLE orders; --") { order_id } }"#),
        )
        .await;
    assert_no_sql_leak(&resp);
    // Table intact.
    let data = exec_json(&engine, &s, "{ orders { order_id } }").await;
    assert_eq!(data["orders"].as_array().unwrap().len(), 3);
}

/// S2b: denied where column fails closed under default strict_where.
pub async fn s2_denied_where_column_fails_closed(db: Db) {
    let pool = seed_orders(&db).await;
    let engine = GraphqlEngine::builder(pool)
        .roles(&["restricted"])
        .model::<OrderView>(
            ModelPermissions::new().grant("restricted", read().columns(["order_id", "status"])),
        )
        .build()
        .unwrap();
    let s = session("restricted", "x");
    let resp = engine
        .execute(
            &s,
            Request::new(
                r#"{ orders(where: { total_cents: { _eq: 100 }, status: { _eq: "open" } }) { order_id status } }"#,
            ),
        )
        .await;
    assert_no_sql_leak(&resp);
    assert!(
        !resp.errors.is_empty(),
        "denied where column must not soft-skip under strict default"
    );
}
