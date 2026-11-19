//! Shared GraphQL test helpers for integration crates via
//! `#[path = "../support/graphql.rs"]`.
//!
//! Prefer driving shipped `GraphqlEngine` / HTTP only — no SQL reimplementation.
#![allow(dead_code)] // each including target uses a subset

use async_graphql::Request;
use distributed::graphql::{read, GraphqlEngine, GraphqlPool, GraphqlPoolSource, ModelPermissions};
use distributed::microsvc::{Session, ROLE_KEY, USER_ID_KEY};
use distributed::projection_protocol::ProjectionChangeRetention;
use distributed::read_model::ReadModelWritePlanBuilder;
use distributed::{
    ReadModel, ReadModelChange, RelationalReadModel, SqliteRepository, TableSchemaRegistry,
};
use serde::{Deserialize, Serialize};

#[cfg(feature = "postgres")]
#[path = "postgres.rs"]
mod postgres;

/// Run `$body` with `$repository` bound to the concrete repository in `$db`.
macro_rules! with_repository {
    ($db:expr, $repository:ident => $body:expr) => {
        match $db {
            Db::Sqlite($repository) => $body,
            #[cfg(feature = "postgres")]
            Db::Postgres($repository) => $body,
        }
    };
}
#[allow(unused_imports)]
pub(crate) use with_repository;

/// One case list, instantiated per dialect as `sqlite::<case>` and
/// `postgres::<case>`. A case is an `async fn(Db)`; `module::{..}` groups
/// name cases in `crate::module`, bare names are cases at the crate root.
macro_rules! on_each_dialect {
    (@cases $($(#[$meta:meta])* $case:ident),* $(,)?) => {
        $($(#[$meta])*
        #[tokio::test]
        async fn $case() {
            let Some(db) = super::db(stringify!($case)).await else {
                return;
            };
            scenarios::$case(db).await;
        })*
    };
    (@dialect $($module:ident::{ $($cases:tt)* })* ; $($root:tt)*) => {
        $(mod $module {
            #[allow(unused_imports)]
            use super::db;
            use crate::$module as scenarios;
            $crate::common::on_each_dialect!(@cases $($cases)*);
        })*
        #[allow(unused_imports)]
        mod root {
            use super::db;
            use crate as scenarios;
            $crate::common::on_each_dialect!(@cases $($root)*);
        }
    };
    ($($module:ident::{ $($cases:tt)* })* $(; $($root:tt)*)?) => {
        mod sqlite {
            async fn db(_case: &str) -> Option<$crate::common::Db> {
                Some($crate::common::Db::sqlite().await)
            }
            $crate::common::on_each_dialect!(@dialect $($module::{ $($cases)* })* ; $($($root)*)?);
        }

        #[cfg(feature = "postgres")]
        mod postgres {
            async fn db(case: &str) -> Option<$crate::common::Db> {
                $crate::common::Db::postgres(case).await
            }
            $crate::common::on_each_dialect!(@dialect $($module::{ $($cases)* })* ; $($($root)*)?);
        }
    };
}
pub(crate) use on_each_dialect;

/// The database a GraphQL scenario runs against. Scenarios take a `Db` and
/// are instantiated once per dialect, so they prove the same behaviour on
/// SQLite and Postgres.
#[derive(Clone)]
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
        with_repository!(self, repository => repository.into())
    }

    pub fn graphql_pool(&self) -> GraphqlPool {
        with_repository!(self, repository => repository.pool().clone().into())
    }

    pub fn read_model_changes(&self) -> tokio::sync::broadcast::Receiver<ReadModelChange> {
        with_repository!(self, repository => repository.read_model_changes())
    }

    pub fn publish_read_model_change(&self, change: ReadModelChange) {
        with_repository!(self, repository => repository.publish_read_model_change(change))
    }

    pub fn with_projection_change_retention(self, retention: ProjectionChangeRetention) -> Self {
        match self {
            Db::Sqlite(repository) => {
                Db::Sqlite(repository.with_projection_change_retention(retention))
            }
            #[cfg(feature = "postgres")]
            Db::Postgres(repository) => {
                Db::Postgres(repository.with_projection_change_retention(retention))
            }
        }
    }

    /// Create the tables `registry` declares.
    pub async fn bootstrap(&self, registry: &TableSchemaRegistry) {
        with_repository!(self, repository => {
            repository
                .bootstrap_table_schema_for_dev(registry)
                .await
                .expect("bootstrap read-model tables");
        })
    }

    /// Raw SQL for fault injection and for standing in for an external
    /// writer; fixture rows and tables come from [`Db::seed`].
    pub async fn execute(&self, sql: impl Into<String>) {
        let sql = sql.into();
        with_repository!(self, repository => {
            sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                .execute(repository.pool())
                .await
                .unwrap_or_else(|error| panic!("{sql}: {error}"));
        })
    }

    /// Run `sql` in a transaction that is rolled back.
    pub async fn execute_rolled_back(&self, sql: &'static str) {
        with_repository!(self, repository => {
            let mut transaction = repository.pool().begin().await.expect("begin");
            sqlx::query(sql).execute(&mut *transaction).await.expect(sql);
            transaction.rollback().await.expect("rollback");
        })
    }

    pub async fn scalar_i64(&self, sql: &'static str) -> i64 {
        with_repository!(self, repository => {
            sqlx::query_scalar(sql)
                .fetch_one(repository.pool())
                .await
                .expect(sql)
        })
    }

    /// Create `M`'s table from its read-model declaration and write `rows`
    /// through a read-model write plan: the table GraphQL reads is the one
    /// the framework generates, not hand-written DDL.
    pub async fn seed<M: RelationalReadModel>(&self, rows: impl IntoIterator<Item = M>) {
        let mut registry = TableSchemaRegistry::new();
        registry.register::<M>().expect("read-model schema");
        self.bootstrap(&registry).await;
        let mut plan = ReadModelWritePlanBuilder::new();
        let mut staged = false;
        for row in rows {
            plan.upsert(&row).expect("read-model row");
            staged = true;
        }
        if !staged {
            return;
        }
        with_repository!(self, repository => {
            plan.commit(repository).await.expect("seed read-model rows");
        })
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
