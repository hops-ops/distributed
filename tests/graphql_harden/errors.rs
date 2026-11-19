//! E* Error-leakage red-team suite.

use std::path::PathBuf;
use std::time::Duration;

use async_graphql::Request;
use distributed::graphql::{read, GraphqlEngine, ModelPermissions};

use super::common::{
    order, Db, assert_no_sql_leak, engine_all_columns, error_messages, extension_code, seed_orders, session,
    OrderView,
};

/// Compile errors must not leak SQL.
pub async fn compile_errors_do_not_leak_sql(db: Db) {
    let pool = seed_orders(&db).await;
    let engine = GraphqlEngine::builder(pool)
        .roles(&["user"])
        .model::<OrderView>(ModelPermissions::new().grant("user", read().all_columns()))
        .max_in_list(1)
        .build()
        .unwrap();
    let s = session("user", "x");
    let resp = engine
        .execute(
            &s,
            Request::new(r#"{ orders(where: { order_id: { _in: ["a","b"] } }) { order_id } }"#),
        )
        .await;
    assert!(!resp.errors.is_empty());
    assert_no_sql_leak(&resp);
}

/// E3: statement timeout → stable TIMEOUT code (existing harden-13 path).
#[tokio::test]
async fn e3_sqlite_statement_timeout_returns_timeout_code() {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("graphql_harden_timeout_e3");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let url = format!("sqlite:{}?mode=rwc", dir.join("orders.db").display());

    let db = Db::sqlite_at(&url).await;
    db.seed([order("o1", "tenant-a", "open", 100, "n")]).await;
    let Db::Sqlite(repository) = &db else {
        unreachable!()
    };

    let mut hold = repository.pool().acquire().await.unwrap();
    sqlx::query("BEGIN EXCLUSIVE")
        .execute(&mut *hold)
        .await
        .unwrap();

    let engine = GraphqlEngine::builder(db.pool())
        .roles(&["user"])
        .model::<OrderView>(ModelPermissions::new().grant("user", read().all_columns()))
        .statement_timeout(Duration::from_millis(80))
        .build()
        .unwrap();

    let resp = engine
        .execute(
            &session("user", "x"),
            Request::new("{ orders { order_id } }"),
        )
        .await;

    let _ = sqlx::query("ROLLBACK").execute(&mut *hold).await;
    drop(hold);

    assert!(
        !resp.errors.is_empty(),
        "expected timeout; data={:?}",
        resp.data
    );
    let err = &resp.errors[0];
    assert!(
        err.message.to_ascii_lowercase().contains("timeout"),
        "message should mention timeout: {}",
        err.message
    );
    let code = extension_code(err);
    assert!(
        code.as_deref()
            .map(|c| c.contains("TIMEOUT"))
            .unwrap_or(false),
        "expected extensions.code=TIMEOUT, got {code:?}"
    );
    assert_no_sql_leak(&resp);
}

/// E2: execute failure (missing table) maps to INTERNAL without SQL/schema leak.
pub async fn e2_execute_failure_is_internal_without_sql_leak(db: Db) {
    let engine = engine_all_columns(seed_orders(&db).await);
    // Drop table so the compiler-produced SELECT fails at execute time.
    db.execute("DROP TABLE orders").await;

    let s = session("user", "x");
    let resp = engine
        .execute(&s, Request::new("{ orders { order_id } }"))
        .await;
    assert!(
        !resp.errors.is_empty(),
        "expected execute error after drop table"
    );
    assert_no_sql_leak(&resp);
    for e in &resp.errors {
        let m = e.message.to_ascii_lowercase();
        assert!(
            !m.contains("no such table") && !m.contains("does not exist") && !m.contains("orders"),
            "must not leak table/schema detail: {}",
            e.message
        );
        if let Some(code) = extension_code(e) {
            assert!(
                code.contains("INTERNAL") || code.contains("BAD_REQUEST"),
                "expected INTERNAL (or safe code), got {code}"
            );
        }
    }
    // Prefer INTERNAL when extensions present.
    let codes: Vec<_> = resp.errors.iter().filter_map(extension_code).collect();
    if !codes.is_empty() {
        assert!(
            codes.iter().any(|c| c.contains("INTERNAL")),
            "expected at least one INTERNAL code, got {codes:?}; msgs={}",
            error_messages(&resp)
        );
    }
}
