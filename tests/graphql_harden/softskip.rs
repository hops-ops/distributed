//! Fail-closed filter/order contracts (strict_where default true).
//!
//! **Default (strict):** unknown or ungranted client `where` / `order_by` keys
//! fail the operation. Failure may come from GraphQL schema validation and/or
//! compile-time checks — never silent success that ignores the key.
//!
//! **Opt-out:** `.strict_where(false)` restores compile-path soft-skip for keys
//! that reach the walker (not production-recommended). Typed GraphQL may still
//! reject keys absent from `*_bool_exp` / `*_order_by` before compile runs.

use async_graphql::Request;
use distributed::graphql::IdentityConfig;
use distributed::graphql::{read, GraphqlEngine, ModelPermissions};

use super::common::{
    assert_no_sql_leak, engine_all_columns, error_messages, extension_code, seed_orders, session,
    Db, OrderView,
};

/// Default builder is fail-closed.
pub async fn default_engine_has_strict_where_on(db: Db) {
    let pool = seed_orders(&db).await;
    let engine = engine_all_columns(pool);
    assert!(
        engine.strict_where(),
        "strict_where must default to true for ship API"
    );
}

/// Unknown where key → error under default settings (not silent success).
pub async fn strict_unknown_where_key_errors(db: Db) {
    let pool = seed_orders(&db).await;
    let engine = engine_all_columns(pool);
    let s = session("user", "x");
    let resp = engine
        .execute(
            &s,
            Request::new(
                r#"{ orders(where: { not_a_real_column: { _eq: "x" }, status: { _eq: "open" } }) { order_id status } }"#,
            ),
        )
        .await;
    assert!(
        !resp.errors.is_empty(),
        "unknown where key must fail under strict default, got data {:?}",
        resp.data
    );
    assert_no_sql_leak(&resp);
    let msgs = error_messages(&resp);
    assert!(
        msgs.contains("unknown") || msgs.contains("not_a_real_column") || msgs.contains("invalid"),
        "expected client-safe unknown/invalid error, got {msgs}"
    );
}

/// Denied where column → error under default settings.
pub async fn strict_denied_where_column_errors(db: Db) {
    let pool = seed_orders(&db).await;
    let engine = GraphqlEngine::builder(pool)
        .identity(IdentityConfig::dev_headers())
        .roles(&["restricted"])
        .model::<OrderView>(
            ModelPermissions::new().grant("restricted", read().columns(["order_id", "status"])),
        )
        .build()
        .unwrap();
    assert!(engine.strict_where());
    let s = session("restricted", "x");
    let resp = engine
        .execute(
            &s,
            Request::new(
                r#"{ orders(where: { total_cents: { _eq: 100 }, status: { _eq: "open" } }) { order_id status } }"#,
            ),
        )
        .await;
    assert!(
        !resp.errors.is_empty(),
        "denied where column must fail under strict default"
    );
    assert_no_sql_leak(&resp);
    let msgs = error_messages(&resp);
    assert!(
        msgs.contains("total_cents") || msgs.contains("unknown") || msgs.contains("invalid"),
        "expected unknown/denied field error, got {msgs}"
    );
}

/// Junk order_by column → error under default settings.
pub async fn strict_junk_order_by_column_errors(db: Db) {
    let pool = seed_orders(&db).await;
    let engine = engine_all_columns(pool);
    let s = session("user", "x");
    let resp = engine
        .execute(
            &s,
            Request::new(
                r#"{ orders(order_by: [{ totally_fake: asc }, { status: desc }]) { order_id status } }"#,
            ),
        )
        .await;
    assert!(
        !resp.errors.is_empty(),
        "junk order_by must fail under strict default"
    );
    assert_no_sql_leak(&resp);
    let msgs = error_messages(&resp);
    assert!(
        msgs.contains("totally_fake") || msgs.contains("unknown") || msgs.contains("invalid"),
        "expected unknown order field error, got {msgs}"
    );
}

/// Valid where + order_by still work under strict defaults.
pub async fn strict_valid_where_and_order_still_work(db: Db) {
    let pool = seed_orders(&db).await;
    let engine = engine_all_columns(pool);
    let s = session("user", "x");
    let resp = engine
        .execute(
            &s,
            Request::new(
                r#"{ orders(where: { status: { _eq: "open" } }, order_by: [{ status: asc }]) { order_id status } }"#,
            ),
        )
        .await;
    assert!(resp.errors.is_empty(), "{:?}", resp.errors);
    let data = serde_json::to_value(&resp.data).unwrap();
    let orders = data["orders"].as_array().unwrap();
    assert!(!orders.is_empty());
    assert!(orders.iter().all(|o| o["status"] == "open"), "{data}");
}

/// Escape hatch: strict_where(false) is opt-in and recorded on the engine.
pub async fn soft_skip_mode_is_opt_in_only(db: Db) {
    let pool = seed_orders(&db).await;
    let engine = GraphqlEngine::builder(pool)
        .identity(IdentityConfig::dev_headers())
        .roles(&["user"])
        .model::<OrderView>(ModelPermissions::new().grant("user", read().all_columns()))
        .strict_where(false)
        .build()
        .unwrap();
    assert!(
        !engine.strict_where(),
        "escape hatch must turn strict_where off"
    );
    // Typed GraphQL may still reject keys absent from the input type before
    // compile soft-skip runs; the flag is proven above + pure compile_order_by tests.
    let s = session("user", "x");
    let resp = engine
        .execute(
            &s,
            Request::new(r#"{ orders(where: { status: { _eq: "open" } }) { order_id } }"#),
        )
        .await;
    assert!(
        resp.errors.is_empty(),
        "valid query still works: {:?}",
        resp.errors
    );
    let _ = extension_code;
}
