//! The one executor for compiled plans.
//!
//! [`execute_plan`] runs a [`SqlPlan`] on either SQL backend: one read
//! snapshot, the plan's single statement, then — only when the causal protocol
//! asks for it — the evidence reads inside the same snapshot. A plain read is
//! the protocol read with no evidence consumer.
//!
//! Round trips on PostgreSQL: the snapshot `BEGIN` and its `SET LOCAL
//! statement_timeout` go in one simple-query message; the authoritative-primary
//! check (`gateway`) rides in the plan's own statement; then `COMMIT`. So a
//! plain root field costs three round trips, where it used to cost five.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_graphql::Value;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
use serde_json::Value as JsonValue;

use super::compile::SqlPlan;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
use super::compile::{BindValue, ExtractedQueryEvidence};
#[cfg(all(feature = "gateway", any(feature = "sqlite", feature = "postgres")))]
use super::delivery::{GatewayVersionStore, VersionError, VersionVector};
use super::engine::{EngineInner, GraphqlPool};
use super::protocol::{
    DistributedLiveMetadata, DistributedQuerySnapshot, ProtocolResponseAccumulator,
    RequestedLiveResume,
};
use super::surface::Surface;
use crate::projection_protocol::ProjectionProtocolError;

/// Why an execution failed, as far as the client may know.
#[derive(Debug)]
pub(crate) enum ExecuteError {
    /// The statement ran past `statement_timeout`: the client may retry.
    Timeout,
    /// Anything else. The detail is for logs, never for the client.
    Failed(String),
}

impl ExecuteError {
    /// The GraphQL error the client sees.
    pub(crate) fn client_error(&self) -> async_graphql::Error {
        match self {
            Self::Timeout => super::schema::client_error("TIMEOUT", "statement timeout"),
            Self::Failed(_) => super::schema::client_error("INTERNAL", "internal error"),
        }
    }

    fn failed(detail: impl Into<String>) -> Self {
        Self::Failed(detail.into())
    }
}

impl std::fmt::Display for ExecuteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout => f.write_str("statement timeout"),
            Self::Failed(detail) => f.write_str(detail),
        }
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
impl From<sqlx::Error> for ExecuteError {
    fn from(error: sqlx::Error) -> Self {
        if is_statement_timeout(&error) {
            Self::Timeout
        } else {
            Self::Failed(error.to_string())
        }
    }
}

impl From<ProjectionProtocolError> for ExecuteError {
    fn from(error: ProjectionProtocolError) -> Self {
        #[cfg(any(feature = "sqlite", feature = "postgres"))]
        if let ProjectionProtocolError::Repository(crate::RepositoryError::Storage {
            source: Some(source),
            ..
        }) = &error
        {
            if source
                .downcast_ref::<sqlx::Error>()
                .is_some_and(is_statement_timeout)
            {
                return Self::Timeout;
            }
        }
        Self::Failed(error.to_string())
    }
}

/// PostgreSQL cancelled the statement (SQLSTATE 57014, `query_canceled`):
/// the only cancellation the engine causes is its `statement_timeout`.
#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn is_statement_timeout(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(db) if db.code().as_deref() == Some("57014"))
}

/// The causal protocol's evidence consumer for one execution.
pub(crate) struct ProtocolRead {
    pub(crate) role_surface: Arc<Surface>,
    pub(crate) accumulator: ProtocolResponseAccumulator,
    /// `Some` for a live query: also read the resume metadata.
    pub(crate) live_resume: Option<RequestedLiveResume>,
}

pub(crate) struct Executed {
    pub(crate) value: Value,
    /// Present exactly when a [`ProtocolRead`] was given.
    pub(crate) protocol: Option<ProtocolEvidence>,
}

pub(crate) struct ProtocolEvidence {
    pub(crate) snapshot: DistributedQuerySnapshot,
    pub(crate) live: Option<DistributedLiveMetadata>,
}

/// Run `plan` on `pool`, within `statement_timeout`.
///
/// `primary` asks PostgreSQL to prove the connection is not a standby
/// (`gateway` builds only). `protocol` adds the evidence reads; without it the
/// result carries no evidence.
pub(crate) async fn execute_plan(
    inner: &EngineInner,
    pool: &GraphqlPool,
    primary: bool,
    plan: &SqlPlan,
    protocol: Option<ProtocolRead>,
) -> Result<Executed, ExecuteError> {
    let timeout = inner.statement_timeout;
    let run = async move {
        match pool {
            #[cfg(feature = "sqlite")]
            GraphqlPool::Sqlite(pool) => {
                run_plan(inner, pool, timeout, primary, plan, protocol).await
            }
            #[cfg(feature = "postgres")]
            GraphqlPool::Postgres(pool) => {
                run_plan(inner, pool, timeout, primary, plan, protocol).await
            }
            #[allow(unreachable_patterns)]
            _ => {
                let _ = (inner, primary, plan, protocol);
                Err(ExecuteError::failed(
                    "no database pool available for GraphQL execution",
                ))
            }
        }
    };
    apply_statement_timeout(timeout, run).await
}

/// Wall-clock budget around one execution; the backstop for a backend with
/// no server-side statement timeout (SQLite).
pub(crate) async fn apply_statement_timeout<T, F>(
    timeout: Duration,
    run: F,
) -> Result<T, ExecuteError>
where
    F: Future<Output = Result<T, ExecuteError>>,
{
    tokio::time::timeout(timeout, run)
        .await
        .unwrap_or(Err(ExecuteError::Timeout))
}

/// What differs between the SQL backends when executing a plan.
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) trait GraphqlBackend: crate::sqlx_repo::repo::SqlxRepoBackend {
    /// The one statement that opens a read snapshot whose statements are
    /// bounded by `timeout`.
    fn begin_read(timeout: Duration) -> String;

    /// Run the plan's statement and decode its JSON column (`null` for no
    /// row). With `primary`, fail on a standby.
    fn fetch<'c>(
        connection: &'c mut Self::Connection,
        plan: &'c SqlPlan,
        primary: bool,
    ) -> impl Future<Output = Result<JsonValue, ExecuteError>> + Send + 'c;

    /// The dependency versions a gateway validator binds, read in the same
    /// snapshot as the result.
    #[cfg(feature = "gateway")]
    fn gateway_versions<'c>(
        store: &'c GatewayVersionStore,
        connection: &'c mut Self::Connection,
        tables: &'c [String],
    ) -> impl Future<Output = Result<VersionVector, VersionError>> + Send + 'c;
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
async fn run_plan<DB>(
    inner: &EngineInner,
    pool: &sqlx::Pool<DB>,
    timeout: Duration,
    primary: bool,
    plan: &SqlPlan,
    protocol: Option<ProtocolRead>,
) -> Result<Executed, ExecuteError>
where
    DB: GraphqlBackend,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    DB::Arguments: sqlx::IntoArguments<DB>,
    for<'q> i64: sqlx::Encode<'q, DB> + sqlx::Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: sqlx::Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: sqlx::Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: sqlx::Encode<'q, DB> + sqlx::Type<DB>,
    for<'q> &'q [u8]: sqlx::Encode<'q, DB> + sqlx::Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    if let Some(read) = &protocol {
        // Matching compiler plans share one comparable snapshot scope, so a
        // lagging refresh is never authoritative over a newer live frame
        // merely because the transport operation differs.
        read.accumulator
            .bind_query_snapshot_scope(&plan.snapshot_scope())
            .map_err(|error| {
                ExecuteError::failed(format!("query snapshot scope failed: {error}"))
            })?;
    }
    let mut tx = pool
        .begin_with(sqlx::AssertSqlSafe(DB::begin_read(timeout)))
        .await?;
    #[cfg(feature = "gateway")]
    if protocol.is_some() {
        if let Some(store) = &inner.gateway_versions {
            store.record_result();
        }
    }
    let mut json = DB::fetch(&mut tx, plan, primary).await?;
    let evidence = plan
        .extract_evidence_and_strip(&mut json)
        .map_err(ExecuteError::Failed)?;
    let value = Value::from_json(json)
        .map_err(|error| ExecuteError::failed(format!("graphql value: {error}")))?;
    let protocol = match protocol {
        None => None,
        Some(read) => Some(read_evidence::<DB>(inner, &mut tx, plan, read, evidence).await?),
    };
    tx.commit().await?;
    Ok(Executed { value, protocol })
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
async fn read_evidence<DB>(
    inner: &EngineInner,
    connection: &mut DB::Connection,
    plan: &SqlPlan,
    read: ProtocolRead,
    evidence: ExtractedQueryEvidence,
) -> Result<ProtocolEvidence, ExecuteError>
where
    DB: GraphqlBackend,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
    DB::Arguments: sqlx::IntoArguments<DB>,
    for<'q> i64: sqlx::Encode<'q, DB> + sqlx::Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: sqlx::Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: sqlx::Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: sqlx::Encode<'q, DB> + sqlx::Type<DB>,
    for<'q> &'q [u8]: sqlx::Encode<'q, DB> + sqlx::Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let (snapshot, live) = super::query_protocol::finish_protocol_query::<DB>(
        connection,
        &inner.query_protocol,
        &read.role_surface,
        &read.accumulator,
        plan,
        evidence,
        read.live_resume,
    )
    .await?;
    #[cfg(feature = "gateway")]
    if read.accumulator.gateway_enabled() {
        if let Some(store) = &inner.gateway_versions {
            if store.covers(&plan.tables_touched) {
                let versions = DB::gateway_versions(store, connection, &plan.tables_touched)
                    .await
                    .map_err(|error| ExecuteError::Failed(error.to_string()))?;
                read.accumulator
                    .record_gateway_versions(&versions)
                    .map_err(|error| ExecuteError::Failed(error.to_string()))?;
            }
        }
    }
    Ok(ProtocolEvidence { snapshot, live })
}

/// Apply [`BindValue`]s to a sqlx query builder.
///
/// Shared by SQLite and Postgres executors so bind-order and type mapping stay
/// in one place. Macro because sqlx `Query` is database-generic.
#[cfg(any(feature = "sqlite", feature = "postgres"))]
macro_rules! apply_binds {
    ($qb:expr, $binds:expr) => {{
        let mut __qb = $qb;
        for __bind in $binds {
            __qb = match __bind {
                BindValue::Null => __qb.bind(None::<String>),
                BindValue::Bool(b) => __qb.bind(*b),
                BindValue::I64(i) => __qb.bind(*i),
                BindValue::F64(f) => __qb.bind(*f),
                BindValue::Text(s) => __qb.bind(s.as_str()),
                BindValue::Bytes(b) => __qb.bind(b.as_slice()),
                // No sqlx `json` feature — bind as text; compiler adds casts.
                BindValue::Json(j) => __qb.bind(j.to_string()),
            };
        }
        __qb
    }};
}

#[cfg(feature = "sqlite")]
impl GraphqlBackend for sqlx::Sqlite {
    fn begin_read(_timeout: Duration) -> String {
        // SQLite has no statement timeout; `apply_statement_timeout` bounds
        // the whole execution instead.
        <Self as crate::sqlx_repo::repo::SqlxRepoBackend>::BEGIN_READ_SNAPSHOT.to_owned()
    }

    async fn fetch(
        connection: &mut sqlx::SqliteConnection,
        plan: &SqlPlan,
        _primary: bool,
    ) -> Result<JsonValue, ExecuteError> {
        use sqlx::Row;

        let query = apply_binds!(
            sqlx::query(sqlx::AssertSqlSafe(Arc::clone(&plan.sql))),
            &plan.binds
        );
        // by_pk and filtered lookups may return zero rows → GraphQL null.
        let text = match query.fetch_optional(&mut *connection).await? {
            Some(row) => row.try_get::<Option<String>, _>(0)?,
            None => None,
        };
        let mut json = match text {
            Some(text) => serde_json::from_str(&text)
                .map_err(|error| ExecuteError::failed(format!("json decode: {error}")))?,
            None => JsonValue::Null,
        };
        rewrite_hex_bytes(&mut json, &plan.bytes_hex_paths);
        Ok(json)
    }

    #[cfg(feature = "gateway")]
    fn gateway_versions<'c>(
        store: &'c GatewayVersionStore,
        connection: &'c mut sqlx::SqliteConnection,
        tables: &'c [String],
    ) -> impl Future<Output = Result<VersionVector, VersionError>> + Send + 'c {
        store.sqlite(connection, tables)
    }
}

#[cfg(feature = "postgres")]
impl GraphqlBackend for sqlx::Postgres {
    fn begin_read(timeout: Duration) -> String {
        let timeout_ms = i64::try_from(timeout.as_millis()).unwrap_or(i64::MAX);
        // One simple-query message: both statements cost one round trip.
        format!(
            "{}; SET LOCAL statement_timeout = '{timeout_ms}ms'",
            <Self as crate::sqlx_repo::repo::SqlxRepoBackend>::BEGIN_READ_SNAPSHOT
        )
    }

    async fn fetch(
        connection: &mut sqlx::PgConnection,
        plan: &SqlPlan,
        primary: bool,
    ) -> Result<JsonValue, ExecuteError> {
        use sqlx::Row;

        // Only gateway builds route reads to replicas, so only they check
        // that the "primary" connection is not a standby. The check rides
        // in the plan's statement, on the same connection and snapshot.
        let check_primary = cfg!(feature = "gateway") && primary;
        let sql: Arc<str> = if check_primary {
            format!("SELECT pg_is_in_recovery(), ({})", plan.sql).into()
        } else {
            Arc::clone(&plan.sql)
        };
        let column = usize::from(check_primary);
        let query = apply_binds!(sqlx::query(sqlx::AssertSqlSafe(sql)), &plan.binds);
        let Some(row) = query.fetch_optional(&mut *connection).await? else {
            return Ok(JsonValue::Null);
        };
        if check_primary && row.try_get::<bool, _>(0)? {
            return Err(ExecuteError::failed("authoritative primary is unavailable"));
        }
        // Plans select `json`; accept text too, as a JSON document.
        if let Ok(json) = row.try_get::<Option<sqlx::types::Json<JsonValue>>, _>(column) {
            return Ok(json.map_or(JsonValue::Null, |json| json.0));
        }
        match row.try_get::<Option<String>, _>(column)? {
            Some(text) => serde_json::from_str(&text)
                .map_err(|error| ExecuteError::failed(format!("json decode: {error}"))),
            None => Ok(JsonValue::Null),
        }
    }

    #[cfg(feature = "gateway")]
    fn gateway_versions<'c>(
        store: &'c GatewayVersionStore,
        connection: &'c mut sqlx::PgConnection,
        tables: &'c [String],
    ) -> impl Future<Output = Result<VersionVector, VersionError>> + Send + 'c {
        store.postgres(connection, tables)
    }
}

/// Rewrite hex-encoded Bytes paths to base64 (SQLite executor path).
pub fn rewrite_hex_bytes(json: &mut serde_json::Value, paths: &[String]) {
    for path in paths {
        let parts: Vec<&str> = path.split('.').collect();
        rewrite_path(json, &parts);
    }
}

fn rewrite_path(json: &mut serde_json::Value, parts: &[&str]) {
    use serde_json::Value as JsonValue;
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

/// Check the actual transaction connection, not another pool checkout. This
/// detects a physical standby misbound/switched into the primary slot; it does
/// not claim a general timeline or WAL replay routing implementation.
#[cfg(all(feature = "postgres", feature = "gateway"))]
pub(crate) async fn ensure_primary_backend(
    connection: &mut sqlx::PgConnection,
    primary: bool,
) -> Result<(), String> {
    #[cfg(feature = "gateway")]
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

#[cfg(test)]
mod statement_timeout_tests {
    use super::{apply_statement_timeout, ExecuteError};
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn elapses_to_statement_timeout_error() {
        let run = async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok::<String, ExecuteError>("never".into())
        };
        let handle =
            tokio::spawn(
                async move { apply_statement_timeout(Duration::from_millis(1), run).await },
            );
        tokio::time::advance(Duration::from_millis(5)).await;
        let err = handle.await.expect("join").expect_err("budget must elapse");
        assert!(matches!(err, ExecuteError::Timeout), "{err}");
    }

    #[tokio::test(start_paused = true)]
    async fn completes_when_under_budget() {
        let run = async { Ok::<String, ExecuteError>("ok".into()) };
        let handle =
            tokio::spawn(async move { apply_statement_timeout(Duration::from_secs(5), run).await });
        tokio::time::advance(Duration::from_millis(1)).await;
        let v: String = handle.await.expect("join").expect("under budget");
        assert_eq!(v, "ok");
    }

    #[tokio::test(start_paused = true)]
    async fn propagates_inner_error() {
        let run = async { Err::<String, _>(ExecuteError::Failed("sqlite execute: boom".into())) };
        let handle =
            tokio::spawn(async move { apply_statement_timeout(Duration::from_secs(5), run).await });
        tokio::time::advance(Duration::from_millis(1)).await;
        let err = handle.await.expect("join").expect_err("inner err");
        assert!(matches!(&err, ExecuteError::Failed(detail) if detail.contains("boom")));
    }
}
