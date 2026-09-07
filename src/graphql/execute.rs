//! Dialect executors: run a SqlPlan and decode the single JSON column.
#![allow(clippy::items_after_test_module)]

use std::future::Future;
use std::time::Duration;

use async_graphql::Value;
use serde_json::Value as JsonValue;

use super::compile::{BindValue, ExtractedQueryEvidence, SqlDialect, SqlPlan};
use super::engine::{EngineInner, GraphqlPool};

pub async fn execute_sql(inner: &EngineInner, plan: &SqlPlan) -> Result<Value, String> {
    execute_sql_on_pool(inner, &inner.pool, true, plan).await
}

pub(crate) async fn execute_sql_on_pool(
    inner: &EngineInner,
    pool: &GraphqlPool,
    primary: bool,
    plan: &SqlPlan,
) -> Result<Value, String> {
    let _ = primary;
    match pool {
        #[cfg(feature = "sqlite")]
        GraphqlPool::Sqlite(pool) => execute_sqlite(pool, plan, inner.statement_timeout)
            .await
            .map(|executed| executed.value),
        #[cfg(feature = "postgres")]
        GraphqlPool::Postgres(pool) => {
            execute_postgres(pool, plan, inner.statement_timeout, primary)
                .await
                .map(|executed| executed.value)
        }
        #[allow(unreachable_patterns)]
        _ => Err("no database pool available for GraphQL execution".into()),
    }
}

pub(crate) struct ExecutedSql {
    pub(crate) value: Value,
    pub(crate) evidence: ExtractedQueryEvidence,
}

/// Wall-clock budget around a single statement future.
///
/// On elapse returns `Err("statement timeout")` — the stable string mapped to
/// client `TIMEOUT` by [`super::schema::client_error_for_execute_err`].
pub(crate) async fn apply_statement_timeout<T, F>(timeout: Duration, run: F) -> Result<T, String>
where
    F: Future<Output = Result<T, String>>,
{
    match tokio::time::timeout(timeout, run).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(e),
        Err(_) => Err("statement timeout".into()),
    }
}

/// Apply [`BindValue`]s to a sqlx query builder.
///
/// Shared by SQLite and Postgres executors so bind-order and type mapping stay
/// in one place (dedup-3). Macro because sqlx `Query` is database-generic.
macro_rules! apply_binds {
    ($qb:expr, $binds:expr) => {{
        let mut __qb = $qb;
        for __bind in $binds {
            __qb = match __bind {
                BindValue::Null => __qb.bind(None::<String>),
                BindValue::Bool(b) => __qb.bind(*b),
                BindValue::I64(i) => __qb.bind(*i),
                BindValue::F64(f) => __qb.bind(*f),
                BindValue::Text(s) => __qb.bind(s.clone()),
                BindValue::Bytes(b) => __qb.bind(b.clone()),
                // No sqlx `json` feature — bind as text; compiler adds casts.
                BindValue::Json(j) => __qb.bind(j.to_string()),
            };
        }
        __qb
    }};
}

#[cfg(feature = "sqlite")]
async fn execute_sqlite(
    pool: &sqlx::SqlitePool,
    plan: &SqlPlan,
    timeout: std::time::Duration,
) -> Result<ExecutedSql, String> {
    // SQL is compiler-produced from schema metadata + bound parameters only.
    let text = apply_statement_timeout(timeout, fetch_sqlite_json(pool, plan)).await?;
    decode_sqlite_value(text, plan)
}

/// Execute a compiled SQLite plan on a caller-owned read snapshot.
#[cfg(feature = "sqlite")]
pub(crate) async fn execute_sqlite_in_connection(
    connection: &mut sqlx::SqliteConnection,
    plan: &SqlPlan,
) -> Result<ExecutedSql, String> {
    let text = fetch_sqlite_json(&mut *connection, plan).await?;
    decode_sqlite_value(text, plan)
}

#[cfg(feature = "sqlite")]
async fn fetch_sqlite_json<'e, E>(executor: E, plan: &SqlPlan) -> Result<String, String>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    use sqlx::Row;

    let qb = apply_binds!(
        sqlx::query(sqlx::AssertSqlSafe(plan.sql.clone())),
        &plan.binds
    );
    // by_pk and filtered lookups may return zero rows → GraphQL null.
    let row = qb
        .fetch_optional(executor)
        .await
        .map_err(|e| format!("sqlite execute: {e}"))?;
    match row {
        Some(row) => row
            .try_get::<Option<String>, _>(0)
            .map_err(|e| format!("sqlite json column: {e}"))
            .map(|value| value.unwrap_or_else(|| "null".into())),
        None => Ok("null".into()),
    }
}

#[cfg(feature = "sqlite")]
fn decode_sqlite_value(text: String, plan: &SqlPlan) -> Result<ExecutedSql, String> {
    let mut json: JsonValue =
        serde_json::from_str(&text).map_err(|e| format!("json decode: {e}"))?;
    deep_parse_json_strings(&mut json);
    rewrite_hex_bytes(&mut json, &plan.bytes_hex_paths);
    finish_executed_value(json, plan)
}

#[cfg(test)]
mod statement_timeout_tests {
    use super::apply_statement_timeout;
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn elapses_to_statement_timeout_error() {
        let run = async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok::<String, String>("never".into())
        };
        let handle =
            tokio::spawn(
                async move { apply_statement_timeout(Duration::from_millis(1), run).await },
            );
        tokio::time::advance(Duration::from_millis(5)).await;
        let err = handle.await.expect("join").expect_err("budget must elapse");
        assert_eq!(err, "statement timeout");
    }

    #[tokio::test(start_paused = true)]
    async fn completes_when_under_budget() {
        let run = async { Ok::<String, String>("ok".into()) };
        let handle =
            tokio::spawn(async move { apply_statement_timeout(Duration::from_secs(5), run).await });
        // Allow the ready future to be polled under paused time.
        tokio::time::advance(Duration::from_millis(1)).await;
        let v: String = handle.await.expect("join").expect("under budget");
        assert_eq!(v, "ok");
    }

    #[tokio::test(start_paused = true)]
    async fn propagates_inner_error() {
        let run = async { Err::<String, String>("sqlite execute: boom".into()) };
        let handle =
            tokio::spawn(async move { apply_statement_timeout(Duration::from_secs(5), run).await });
        tokio::time::advance(Duration::from_millis(1)).await;
        let err = handle.await.expect("join").expect_err("inner err");
        assert!(err.contains("boom"), "{err}");
    }
}

/// Parse string-encoded JSON only for array elements (SQLite json_group_array
/// quirk). Object property strings stay GraphQL String scalars (never re-typed).
fn deep_parse_json_strings(value: &mut JsonValue) {
    match value {
        JsonValue::Array(items) => {
            for item in items {
                if let JsonValue::String(s) = item {
                    let trimmed = s.trim();
                    if (trimmed.starts_with('{') && trimmed.ends_with('}'))
                        || (trimmed.starts_with('[') && trimmed.ends_with(']'))
                    {
                        if let Ok(mut parsed) = serde_json::from_str::<JsonValue>(s) {
                            deep_parse_json_strings(&mut parsed);
                            *item = parsed;
                        }
                    }
                } else {
                    deep_parse_json_strings(item);
                }
            }
        }
        JsonValue::Object(map) => {
            for v in map.values_mut() {
                match v {
                    JsonValue::Object(_) | JsonValue::Array(_) => deep_parse_json_strings(v),
                    // Leave scalar strings alone (including JSON-looking text columns).
                    _ => {}
                }
            }
        }
        _ => {}
    }
}

#[cfg(feature = "postgres")]
async fn execute_postgres(
    pool: &sqlx::PgPool,
    plan: &SqlPlan,
    timeout: std::time::Duration,
    primary: bool,
) -> Result<ExecutedSql, String> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| format!("postgres begin: {e}"))?;
    let timeout_ms = timeout.as_millis() as i64;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SET LOCAL statement_timeout = '{timeout_ms}ms'"
    )))
    .execute(&mut *tx)
    .await
    .map_err(|e| format!("statement_timeout: {e}"))?;

    ensure_primary_backend(&mut tx, primary).await?;
    let value = fetch_postgres_value(&mut *tx, plan).await?;
    tx.commit()
        .await
        .map_err(|e| format!("postgres commit: {e}"))?;

    Ok(value)
}

/// Execute a compiled PostgreSQL plan on a caller-owned repeatable read
/// snapshot. The caller configures `SET LOCAL statement_timeout` once for the
/// complete physical-query/evidence transaction.
#[cfg(feature = "postgres")]
pub(crate) async fn execute_postgres_in_connection(
    connection: &mut sqlx::PgConnection,
    plan: &SqlPlan,
) -> Result<ExecutedSql, String> {
    fetch_postgres_value(&mut *connection, plan).await
}

#[cfg(feature = "postgres")]
async fn fetch_postgres_value<'e, E>(executor: E, plan: &SqlPlan) -> Result<ExecutedSql, String>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    use sqlx::Row;
    let qb = apply_binds!(
        sqlx::query(sqlx::AssertSqlSafe(plan.sql.clone())),
        &plan.binds
    );
    let row = qb
        .fetch_optional(executor)
        .await
        .map_err(|e| format!("postgres execute: {e}"))?;

    // Postgres returns `json`/`jsonb` which sqlx decodes as `Json<T>` when the
    // `json` feature is on; some plans also cast to text. Accept both.
    let json: JsonValue = match row {
        Some(r) => {
            if let Ok(j) = r.try_get::<sqlx::types::Json<JsonValue>, _>(0) {
                j.0
            } else if let Ok(s) = r.try_get::<String, _>(0) {
                serde_json::from_str(&s).map_err(|e| format!("json decode: {e}"))?
            } else if let Ok(Some(s)) = r.try_get::<Option<String>, _>(0) {
                serde_json::from_str(&s).map_err(|e| format!("json decode: {e}"))?
            } else if let Ok(None) = r.try_get::<Option<String>, _>(0) {
                JsonValue::Null
            } else {
                return Err("postgres json column: unsupported type".into());
            }
        }
        None => JsonValue::Null,
    };
    finish_executed_value(json, plan)
}

fn finish_executed_value(mut json: JsonValue, plan: &SqlPlan) -> Result<ExecutedSql, String> {
    let evidence = plan.extract_evidence_and_strip(&mut json)?;
    let value = Value::from_json(json).map_err(|e| format!("graphql value: {e}"))?;
    Ok(ExecutedSql { value, evidence })
}

/// Rewrite hex-encoded Bytes paths to base64 (SQLite executor path).
pub fn rewrite_hex_bytes(json: &mut JsonValue, paths: &[String]) {
    for path in paths {
        let parts: Vec<&str> = path.split('.').collect();
        rewrite_path(json, &parts);
    }
}

fn rewrite_path(json: &mut JsonValue, parts: &[&str]) {
    if parts.is_empty() {
        return;
    }
    match json {
        JsonValue::Array(items) => {
            for item in items {
                rewrite_path(item, parts);
            }
        }
        JsonValue::Object(map) => {
            if parts.len() == 1 {
                if let Some(JsonValue::String(hex)) = map.get_mut(parts[0]) {
                    if let Some(b64) = hex_to_base64(hex) {
                        *hex = b64;
                    }
                }
            } else if let Some(child) = map.get_mut(parts[0]) {
                rewrite_path(child, &parts[1..]);
            }
        }
        _ => {}
    }
}

fn hex_to_base64(hex: &str) -> Option<String> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for i in (0..hex.len()).step_by(2) {
        let byte = u8::from_str_radix(&hex[i..i + 2], 16).ok()?;
        bytes.push(byte);
    }
    use base64::Engine as _;
    Some(base64::engine::general_purpose::STANDARD.encode(bytes))
}

#[allow(dead_code)]
pub fn dialect_name(d: SqlDialect) -> &'static str {
    match d {
        SqlDialect::Postgres => "postgres",
        SqlDialect::Sqlite => "sqlite",
    }
}

/// Check the actual transaction connection, not another pool checkout. This
/// detects a physical standby misbound/switched into the primary slot; it does
/// not claim a general timeline or WAL replay routing implementation.
#[cfg(feature = "postgres")]
pub(crate) async fn ensure_primary_backend(
    connection: &mut sqlx::PgConnection,
    primary: bool,
) -> Result<(), String> {
    #[cfg(feature = "gateway-delivery")]
    if primary
        && sqlx::query_scalar::<_, bool>("SELECT pg_is_in_recovery()")
            .fetch_one(&mut *connection)
            .await
            .map_err(|e| format!("primary validation: {e}"))?
    {
        return Err("authoritative primary is unavailable".into());
    }
    let _ = (connection, primary);
    Ok(())
}
