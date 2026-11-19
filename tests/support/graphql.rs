//! Shared GraphQL test helpers for integration crates via
//! `#[path = "../support/graphql.rs"]`.
//!
//! Prefer driving shipped `GraphqlEngine` / HTTP only — no SQL reimplementation.
#![allow(dead_code)] // each including target uses a subset

use async_graphql::Request;
use distributed::graphql::{read, GraphqlEngine, GraphqlPoolSource, ModelPermissions};
use distributed::microsvc::{Session, ROLE_KEY, USER_ID_KEY};
use distributed::read_model::ReadModelWritePlanBuilder;
use distributed::{ReadModel, RelationalReadModel, SqliteRepository, TableSchemaRegistry};
use serde::{Deserialize, Serialize};

#[cfg(feature = "postgres")]
#[path = "postgres.rs"]
mod postgres;

/// The database a GraphQL scenario runs against. Scenarios take a `Db` and
/// are instantiated once per dialect, so they prove the same behaviour on
/// SQLite and Postgres.
pub enum Db {
    Sqlite(SqliteRepository),
    #[cfg(feature = "postgres")]
    Postgres(distributed::PostgresRepository),
}

impl Db {
    pub async fn sqlite() -> Self {
        Self::sqlite_at("sqlite::memory:").await
    }

    /// SQLite at `url`, for scenarios that need a file shared by connections.
    pub async fn sqlite_at(url: &str) -> Self {
        Db::Sqlite(
            SqliteRepository::connect_and_migrate(url)
                .await
                .expect("SQLite repository"),
        )
    }

    /// A repository in a fresh schema of `DATABASE_URL`; `None` when the
    /// service is absent and not required (see `env.rs`).
    #[cfg(feature = "postgres")]
    pub async fn postgres(what: &str) -> Option<Self> {
        let schema = postgres::PostgresTestSchema::create_from_env("graphql", what).await?;
        Some(Db::Postgres(schema.repository().await))
    }

    pub fn is_postgres(&self) -> bool {
        !matches!(self, Db::Sqlite(_))
    }

    /// What `GraphqlEngine::builder` and `from_schema_catalog` read from.
    pub fn pool(&self) -> GraphqlPoolSource {
        match self {
            Db::Sqlite(repository) => repository.into(),
            #[cfg(feature = "postgres")]
            Db::Postgres(repository) => repository.into(),
        }
    }

    /// Raw SQL for fault injection (dropping a table, holding a lock); rows
    /// and tables come from [`Db::seed`].
    pub async fn execute(&self, sql: &'static str) {
        match self {
            Db::Sqlite(repository) => {
                sqlx::query(sql).execute(repository.pool()).await.expect(sql);
            }
            #[cfg(feature = "postgres")]
            Db::Postgres(repository) => {
                sqlx::query(sql).execute(repository.pool()).await.expect(sql);
            }
        }
    }

    /// Create `M`'s table from its read-model declaration and write `rows`
    /// through a read-model write plan: the table GraphQL reads is the one
    /// the framework generates, not hand-written DDL.
    pub async fn seed<M: RelationalReadModel>(&self, rows: impl IntoIterator<Item = M>) {
        let mut registry = TableSchemaRegistry::new();
        registry.register::<M>().expect("read-model schema");
        let mut plan = ReadModelWritePlanBuilder::new();
        for row in rows {
            plan.upsert(&row).expect("read-model row");
        }
        match self {
            Db::Sqlite(repository) => {
                repository
                    .bootstrap_table_schema_for_dev(&registry)
                    .await
                    .expect("bootstrap read-model table");
                plan.commit(repository).await.expect("seed read-model rows");
            }
            #[cfg(feature = "postgres")]
            Db::Postgres(repository) => {
                repository
                    .bootstrap_table_schema_for_dev(&registry)
                    .await
                    .expect("bootstrap read-model table");
                plan.commit(repository).await.expect("seed read-model rows");
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("orders")]
pub struct OrderView {
    #[id("order_id")]
    pub order_id: String,
    pub customer_id: String,
    pub status: String,
    pub total_cents: i64,
    /// May look like JSON; must remain a GraphQL String.
    pub note: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("parents")]
pub struct ParentView {
    #[id("parent_id")]
    pub parent_id: String,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("children")]
pub struct ChildView {
    #[id("child_id")]
    pub child_id: String,
    pub parent_id: String,
    pub name: String,
}

pub fn session(role: &str, user: &str) -> Session {
    let mut s = Session::new();
    s.set(ROLE_KEY, role);
    s.set(USER_ID_KEY, user);
    s
}

pub fn order(order_id: &str, customer_id: &str, status: &str, total_cents: i64, note: &str) -> OrderView {
    OrderView {
        order_id: order_id.into(),
        customer_id: customer_id.into(),
        status: status.into(),
        total_cents,
        note: note.into(),
    }
}

pub fn parent(parent_id: &str, name: &str) -> ParentView {
    ParentView {
        parent_id: parent_id.into(),
        name: name.into(),
    }
}

pub fn child(child_id: &str, parent_id: &str, name: &str) -> ChildView {
    ChildView {
        child_id: child_id.into(),
        parent_id: parent_id.into(),
        name: name.into(),
    }
}

/// Three orders across two tenants; `o1`'s note looks like JSON.
pub async fn seed_orders(db: &Db) -> GraphqlPoolSource {
    db.seed([
        order("o1", "tenant-a", "open", 100, r#"{"looks":"json"}"#),
        order("o2", "tenant-a", "shipped", 200, "plain"),
        order("o3", "tenant-b", "open", 50, "x"),
    ])
    .await;
    db.pool()
}

pub fn engine_all_columns(pool: impl Into<GraphqlPoolSource>) -> GraphqlEngine {
    GraphqlEngine::builder(pool)
        .roles(&["user"])
        .model::<OrderView>(ModelPermissions::new().grant("user", read().all_columns()))
        .build()
        .unwrap()
}

pub fn assert_no_sql_leak(resp: &async_graphql::Response) {
    for e in &resp.errors {
        let m = e.message.to_ascii_lowercase();
        assert!(
            !m.contains("select ") && !m.contains(" from ") && !m.contains(" where "),
            "must not leak SQL: {}",
            e.message
        );
    }
}

pub fn error_messages(resp: &async_graphql::Response) -> String {
    resp.errors
        .iter()
        .map(|e| e.message.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn extension_code(err: &async_graphql::ServerError) -> Option<String> {
    err.extensions
        .as_ref()
        .and_then(|ext| ext.get("code"))
        .map(|v| format!("{v:?}"))
}

/// Helper: execute and return JSON data (panics if GraphQL errors).
pub async fn exec_json(
    engine: &GraphqlEngine,
    session: &Session,
    query: &str,
) -> serde_json::Value {
    let resp = engine.execute(session, Request::new(query)).await;
    assert!(
        resp.errors.is_empty(),
        "unexpected errors: {:?}",
        resp.errors
    );
    serde_json::to_value(&resp.data).unwrap()
}
