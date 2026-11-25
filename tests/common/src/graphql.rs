//! GraphQL fixtures: the [`Db`] a scenario runs against on each dialect, and
//! engines over the shared read models.
//!
//! Prefer driving shipped `GraphqlEngine` / HTTP only — no SQL reimplementation.

use async_graphql::Request;
use distributed::graphql::IdentityConfig;
use distributed::graphql::{read, GraphqlEngine, GraphqlPool, GraphqlPoolSource, ModelPermissions};
use distributed::microsvc::Session;
use distributed::projection_protocol::ProjectionChangeRetention;
use distributed::read_model::ReadModelWritePlanBuilder;
use distributed::{
    ReadModelChange, RelationalReadModel, SqliteRepository, TableSchema, TableSchemaRegistry,
};

pub use crate::read_models::{child, order, parent, ChildView, OrderView, ParentView};
pub use crate::session::session;

/// Run `$body` with `$repository` bound to the concrete repository in `$db`.
#[macro_export]
macro_rules! with_repository {
    ($db:expr, $repository:ident => $body:expr) => {
        match $db {
            $crate::graphql::Db::Sqlite($repository) => $body,
            #[cfg(feature = "postgres")]
            $crate::graphql::Db::Postgres($repository, _) => $body,
        }
    };
}

/// One case list, instantiated per dialect as `sqlite::<case>` and
/// `postgres::<case>`. A case is an `async fn(Db)`; `module::{..}` groups
/// name cases in `<invoking module>::module`, bare names are cases of the
/// invoking module itself (each test binary groups former targets as modules,
/// so this is not the crate root).
#[macro_export]
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
            use super::super::$module as scenarios;
            $crate::on_each_dialect!(@cases $($cases)*);
        })*
        #[allow(unused_imports)]
        mod root {
            use super::db;
            use super::super as scenarios;
            $crate::on_each_dialect!(@cases $($root)*);
        }
    };
    ($($module:ident::{ $($cases:tt)* })* $(; $($root:tt)*)?) => {
        mod sqlite {
            async fn db(_case: &str) -> Option<$crate::graphql::Db> {
                Some($crate::graphql::Db::sqlite().await)
            }
            $crate::on_each_dialect!(@dialect $($module::{ $($cases)* })* ; $($($root)*)?);
        }

        #[cfg(feature = "postgres")]
        mod postgres {
            async fn db(case: &str) -> Option<$crate::graphql::Db> {
                $crate::graphql::Db::postgres(case).await
            }
            $crate::on_each_dialect!(@dialect $($module::{ $($cases)* })* ; $($($root)*)?);
        }
    };
}

/// The database a GraphQL scenario runs against. Scenarios take a `Db` and
/// are instantiated once per dialect, so they prove the same behaviour on
/// SQLite and Postgres.
#[derive(Clone)]
pub enum Db {
    Sqlite(SqliteRepository),
    /// The schema is shared by every clone and dropped with the last one.
    #[cfg(feature = "postgres")]
    Postgres(
        distributed::PostgresRepository,
        std::sync::Arc<crate::postgres::PostgresTestSchema>,
    ),
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
        let schema = crate::postgres::PostgresTestSchema::create_from_env("graphql", what).await?;
        let repository = schema.repository().await;
        Some(Db::Postgres(repository, std::sync::Arc::new(schema)))
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
            Db::Postgres(repository, schema) => Db::Postgres(
                repository.with_projection_change_retention(retention),
                schema,
            ),
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

    /// Create the tables `schemas` declare, together so foreign keys between
    /// them resolve. Relationships are dropped first: they are read-side
    /// metadata that adds no DDL and may name models outside this set.
    pub async fn create_tables<'a>(&self, schemas: impl IntoIterator<Item = &'a TableSchema>) {
        let mut registry = TableSchemaRegistry::new();
        for schema in schemas {
            let mut schema = schema.clone();
            schema.relationships.clear();
            registry.register_schema(schema).expect("table schema");
        }
        self.bootstrap(&registry).await;
    }

    /// Create the table `schema` declares and insert `rows` (see [`Db::rows`]).
    /// For fixtures that are a hand-built `TableSchema` with no model type;
    /// prefer [`Db::seed`] when there is one.
    pub async fn table(&self, schema: &TableSchema, rows: serde_json::Value) {
        self.create_tables([schema]).await;
        self.rows(schema, rows).await;
    }

    /// Insert `rows` into an existing table: a JSON array of arrays giving
    /// values in declared column order (objects and arrays bind as JSON).
    pub async fn rows(&self, schema: &TableSchema, rows: serde_json::Value) {
        let columns: Vec<&str> = schema
            .columns
            .iter()
            .filter(|column| !column.skipped)
            .map(|column| column.column_name.as_str())
            .collect();
        let placeholders: Vec<String> = (1..=columns.len())
            .map(|n| {
                if self.is_postgres() {
                    format!("${n}")
                } else {
                    "?".into()
                }
            })
            .collect();
        let sql = format!(
            "INSERT INTO \"{}\" ({}) VALUES ({})",
            schema.table_name,
            columns
                .iter()
                .map(|column| format!("\"{column}\""))
                .collect::<Vec<_>>()
                .join(", "),
            placeholders.join(", ")
        );
        for row in rows.as_array().expect("rows is an array of arrays") {
            let values = row.as_array().expect("a row is an array");
            assert_eq!(values.len(), columns.len(), "{}: {row}", schema.table_name);
            with_repository!(self, repository => {
                let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.clone()));
                for value in values {
                    query = match value {
                        serde_json::Value::String(text) => query.bind(text.clone()),
                        serde_json::Value::Bool(flag) => query.bind(*flag),
                        serde_json::Value::Number(number) => {
                            query.bind(number.as_i64().expect("integer"))
                        }
                        serde_json::Value::Null => query.bind(None::<String>),
                        json => query.bind(sqlx::types::Json(json.clone())),
                    };
                }
                query
                    .execute(repository.pool())
                    .await
                    .unwrap_or_else(|error| panic!("{sql}: {error}"));
            })
        }
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
        self.create_tables([M::schema()]).await;
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
        .identity(IdentityConfig::dev_headers())
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
